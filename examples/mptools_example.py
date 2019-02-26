import logging
import random
import socket
import multiprocessing as mp

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
        curr_status = self.get_status()
        if curr_status != self.last_status:
            self.event_q.put(EventMessage(self.name, "STATUS", curr_status))
            self.last_status = curr_status


class ObservationWorker(TimerProcWorker):
    def main_func(self):
        self.event_q.put(EventMessage(self.name, "OBSERVATION", "SOME DATA"))


class SendWorker(QueueProcWorker):
    def startup(self):
        self.send_file = open("send_file.txt", "a")

    def shutdown(self):
        self.send_file.close()

    def main_func(self, data):
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
        pass

    def main_func(self):
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


def request_handler(event, reply_q):
    reply = f'REPLY {event.id} {event.msg}'
    reply_q.put(reply)


def main():
    main_ctx = MainContext()
    shutdown_evt = mp.Event()

    init_signals(shutdown_evt, default_signal_handler, default_signal_handler)

    # -- This queue gets the terminating "END" message, so create it _first_,
    # and if it raises an exception, nothing will need to get cleaned up
    send_q = main_ctx.MPQueue()
    try:
        event_q = main_ctx.MPQueue()
        reply_q = main_ctx.MPQueue()

        main_ctx.Proc("SEND", SendWorker, shutdown_evt, event_q, send_q)
        main_ctx.Proc("LISTEN", ListenWorker, shutdown_evt, event_q, reply_q)
        main_ctx.Proc("STATUS", StatusWorker, shutdown_evt, event_q)
        main_ctx.Proc("OBSERVATION", ObservationWorker, shutdown_evt, event_q)

        while not shutdown_evt.is_set():
            event = event_q.safe_get()
            if not event:
                continue
            elif event.msg_type == "STATUS":
                send_q.put(event)
            elif event.msg_type == "OBSERVATION":
                send_q.put(event)
            elif event.msg_type == "ERROR":
                send_q.put(event)
            elif event.msg_type == "REQUEST":
                request_handler(event, reply_q)
            elif event.msg_type == "FATAL":
                main_ctx.log(logging.INFO, f"Fatal Event received: {event.msg}")
                break
            elif event.msg_type == "END":
                main_ctx.log(logging.INFO, f"Shutdown Event received: {event.msg}")
                break
            else:
                main_ctx.log(logging.ERROR, f"Unknown Event: {event}")

    except Exception as exc:
        # -- This is the main thread, so this will shut down the whole thing
        main_ctx.log(logging.ERROR, f"Exception: {exc}")
        raise

    finally:
        shutdown_evt.set()
        send_q.put("END")
        main_ctx.stop_procs()
        main_ctx.stop_queues()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
