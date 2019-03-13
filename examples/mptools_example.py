import logging
import random
import socket
import sys
import time

from mptools import (
    init_signals,
    default_signal_handler,
    MainContext,
    EventMessage,
    ProcWorker,
    TimerProcWorker,
    QueueProcWorker,
)


# ----  Example Specific Code starts here

class StatusWorker(TimerProcWorker):
    def startup(self):
        self.last_status = None

    def get_status(self):
        return "OKAY" if random.randrange(10) else "NOT-OKAY"

    def main_func(self):
        # -- Do the things to check current status, only send the status message
        # if status has changed
        curr_status = self.get_status()
        if curr_status != self.last_status:
            self.event_q.put(EventMessage(self.name, "STATUS", curr_status))
            self.last_status = curr_status


class ObservationWorker(TimerProcWorker):
    def main_func(self):
        # -- Do the things to obtain a current observation
        self.event_q.put(EventMessage(self.name, "OBSERVATION", "SOME DATA"))


class SendWorker(QueueProcWorker):
    def startup(self):
        self.send_file = open("send_file.txt", "a")

    def shutdown(self):
        self.send_file.close()

    def main_func(self, data):
        # -- Write the messages to the log file.
        self.send_file.write(f'{data.msg_type}::{data.msg}\n')
        self.send_file.flush()


class ListenWorker(ProcWorker):
    SOCKET_TIMEOUT = 1.0

    def init_args(self, args):
        self.reply_q, = args

    def startup(self):
        # -- Called during worker process start up sequence
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('127.0.0.1', 9999))
        self.socket.settimeout(self.SOCKET_TIMEOUT)
        self.socket.listen(1)

    def shutdown(self):
        # -- Called when worker process is shutting down
        self.socket.close()

    def _test_hook(self):
        # -- method intended to be overriden during testing, allowing testing
        # to interact with the main_func
        pass

    def main_func(self):
        # -- Handle one connection from a client.  Each connection will
        # handle exactly ONE request and its reply
        try:
            (clientsocket, address) = self.socket.accept()
        except socket.timeout:
            return

        # TODO: handle timeout conditions better
        self.log(logging.INFO, f"Accepted connection from {address}")
        try:
            clientsocket.settimeout(self.SOCKET_TIMEOUT)
            buffer = clientsocket.recv(1500).decode()
            self.log(logging.DEBUG, f"Received {buffer}")
            self.event_q.put(EventMessage("LISTEN", "REQUEST", buffer))
            self._test_hook()
            reply = self.reply_q.safe_get(timeout=self.SOCKET_TIMEOUT)
            self.log(logging.DEBUG, f"Sending Reply {reply}")
            clientsocket.send(reply.encode("utf-8"))
        finally:
            clientsocket.close()


def request_handler(event, reply_q, main_ctx):
    main_ctx.log(logging.DEBUG, f"request_handler - '{event.msg}'")
    if event.msg == "REQUEST END":
        main_ctx.log(logging.DEBUG, "request_handler - queued END event")
        main_ctx.event_queue.safe_put(EventMessage("request_handler", "END", "END"))

    reply = f'REPLY {event.id} {event.msg}'
    reply_q.safe_put(reply)


def main(die_in_secs):
    with MainContext() as main_ctx:
        if die_in_secs:
            die_time = time.time() + die_in_secs
            main_ctx.log(logging.DEBUG, f"Application die time is in {die_in_secs} seconds")
        else:
            die_time = None

        init_signals(main_ctx.shutdown_event, default_signal_handler, default_signal_handler)

        send_q = main_ctx.MPQueue()
        reply_q = main_ctx.MPQueue()

        main_ctx.Proc("SEND", SendWorker, send_q)
        main_ctx.Proc("LISTEN", ListenWorker, reply_q)
        main_ctx.Proc("STATUS", StatusWorker)
        main_ctx.Proc("OBSERVATION", ObservationWorker)

        while not main_ctx.shutdown_event.is_set():
            if die_time and time.time() > die_time:
                raise RuntimeError("Application has run too long.")
            event = main_ctx.event_queue.safe_get()
            if not event:
                continue
            elif event.msg_type == "STATUS":
                send_q.put(event)
            elif event.msg_type == "OBSERVATION":
                send_q.put(event)
            elif event.msg_type == "ERROR":
                send_q.put(event)
            elif event.msg_type == "REQUEST":
                request_handler(event, reply_q, main_ctx)
            elif event.msg_type == "FATAL":
                main_ctx.log(logging.INFO, f"Fatal Event received: {event.msg}")
                break
            elif event.msg_type == "END":
                main_ctx.log(logging.INFO, f"Shutdown Event received: {event.msg}")
                break
            else:
                main_ctx.log(logging.ERROR, f"Unknown Event: {event}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    die_in_secs = float(sys.argv[1]) if sys.argv[1:] else 0
    main(die_in_secs)
