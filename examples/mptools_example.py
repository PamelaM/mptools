import functools
import logging
import random
import socket
import time
from multiprocessing import Event

from mptools._mptools import (
    MPQueue,
    _sleep_secs,
    init_signals,
    default_signal_handler,
    Proc,
    logger,
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
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('127.0.0.1', 9999))
        self.socket.settimeout(self.SOCKET_TIMEOUT)
        self.socket.listen(1)

    def shutdown(self):
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
    log = functools.partial(logger, "MAIN")
    shutdown_evt = Event()

    init_signals(shutdown_evt, default_signal_handler, default_signal_handler)

    event_q = MPQueue()
    reply_q = MPQueue()
    send_q = MPQueue()

    queues = [event_q, reply_q, send_q]
    procs = []
    try:
        procs = [
            Proc("SEND", SendWorker, shutdown_evt, event_q, send_q),
            Proc("LISTEN", ListenWorker, shutdown_evt, event_q, reply_q),
            Proc("STATUS", StatusWorker, shutdown_evt, event_q),
            Proc("OBSERVATION", ObservationWorker, shutdown_evt, event_q),
        ]

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
                log(logging.INFO, f"Fatal Event received: {event.msg}")
                break
            elif event.msg_type == "END":
                log(logging.INFO, f"Shutdown Event received: {event.msg}")
                break
            else:
                log(logging.ERROR, f"Unknown Event: {event}")

    except Exception as exc:
        # -- This is the main thread, so this will shut down the whole thing
        log(logging.ERROR, f"Exception: {exc}")
        raise

    finally:
        shutdown_evt.set()
        send_q.put("END")

        STOP_WAIT_SECS = 3.0
        end_time = time.time() + STOP_WAIT_SECS
        procs.reverse()
        for proc in procs:
            join_secs = _sleep_secs(STOP_WAIT_SECS, end_time)
            proc.proc.join(join_secs)

        for proc in procs:
            if proc.proc.is_alive():
                log(logging.ERROR, f"Terminating process {proc.name}")
                proc.proc.terminate()
            else:
                exitcode = proc.proc.exitcode
                log(
                    logging.ERROR if exitcode else logging.DEBUG,
                    f"Process {proc.name} ended with exitcode {exitcode}"
                )

        for q in queues:
            q.safe_close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
