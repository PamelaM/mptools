import functools
import logging
import random
import signal
import socket
import time
from mptoolsessing import Process, Queue, Event
from queue import Empty, Full

DEFAULT_POLLING_TIMEOUT = 0.02
MAX_SLEEP_SECS = 0.02

start_time = time.time()


def logger(name, level, msg):
    elapsed = time.time() - start_time
    hours = int(elapsed // 60)
    seconds = elapsed - (hours * 60)
    logging.log(level, f'{hours:3}:{seconds:06.3f} {name:20} {msg}')


def _queue_get(q, timeout=DEFAULT_POLLING_TIMEOUT):
    try:
        if timeout is None:
            return q.get(block=False)
        else:
            return q.get(block=True, timeout=timeout)
    except Empty:
        return None


def _queue_try_put(q, item, timeout=DEFAULT_POLLING_TIMEOUT):
    try:
        q.put(item, block=False, timeout=timeout)
        return True
    except Full:
        return False


def _drain_queue(q):
    got = _queue_get(q)
    while got:
        yield got
        got = _queue_get(q)


def _close_queue(q):
    num_left = sum(1 for ignored in _drain_queue(q))
    q.close()
    q.join_thread()
    return num_left


def _sleep_secs(max_sleep, end_time=999999999999999.9):
    # Calculate time left to sleep, no less than 0
    return max(0.0, min(end_time - time.time(), max_sleep))


class EventMessage:
    def __init__(self, msg_src, msg_type, msg):
        self.id = time.time()
        self.msg_src = msg_src
        self.msg_type = msg_type
        self.msg = msg

    def __str__(self):
        return f"{self.msg_src:10} - {self.msg_type:10} : {self.msg}"


class TerminateInterrupt(BaseException):
    pass


class SignalObject:
    MAX_TERMINATE_CALLED = 3

    def __init__(self, shutdown_event):
        self.terminate_called = 0
        self.shutdown_event = shutdown_event


def default_signal_handler(signal_object, exception_class, signal_num, current_stack_frame):
    signal_object.terminate_called += 1
    signal_object.shutdown_event.set()
    if signal_object.terminate_called >= signal_object.MAX_TERMINATE_CALLED:
        raise exception_class()


def init_signal(signal_num, signal_object, exception_class, handler):
    handler = functools.partial(handler, signal_object, exception_class)
    signal.signal(signal_num, handler)
    signal.siginterrupt(signal_num, False)


def init_signals(shutdown_event, int_handler, term_handler):
    signal_object = SignalObject(shutdown_event)
    init_signal(signal.SIGINT, signal_object, KeyboardInterrupt, int_handler)
    init_signal(signal.SIGTERM, signal_object, TerminateInterrupt, term_handler)
    return signal_object


class ProcWorker:
    MAX_TERMINATE_CALLED = 3
    int_handler = staticmethod(default_signal_handler)
    term_handler = staticmethod(default_signal_handler)

    def __init__(self, name, startup_event, shutdown_event, event_q, *args):
        self.name = name
        self.log = functools.partial(logger, f'{self.name} Worker')
        self.startup_event = startup_event
        self.shutdown_event = shutdown_event
        self.event_q = event_q
        self.terminate_called = 0
        self.init_args(args)

    def init_args(self, args):
        if args:
            raise ValueError(f"Unexpected arguments to ProcWorker.init_args: {args}")

    def init_signals(self):
        self.log(logging.DEBUG, "Entering init_signals")
        signal_object = init_signals(self.shutdown_event, self.int_handler, self.term_handler)
        return signal_object

    def main_loop(self):
        self.log(logging.DEBUG, "Entering main_loop")
        while not self.shutdown_event.is_set():
            self.main_func()

    def startup(self):
        self.log(logging.DEBUG, "Entering startup")
        pass

    def shutdown(self):
        self.log(logging.DEBUG, "Entering shutdown")
        pass

    def main_func(self):
        self.log(logging.DEBUG, "Entering main_func")
        raise NotImplementedError("ProcWorker.main_func is not implemented")

    def run(self):
        self.init_signals()
        try:
            self.startup()
            self.startup_event.set()
            self.main_loop()
            self.log(logging.INFO, "Normal Shutdown")
            _queue_try_put(self.event_q, EventMessage(self.name, "SHUTDOWN", "Normal"))
        except Exception as exc:
            self.log(logging.ERROR, f"Exception Shutdown: {exc}")
            _queue_try_put(self.event_q, EventMessage(self.name, "FATAL", f"{exc}"))
            raise
        finally:
            self.shutdown()


def proc_worker_wrapper(proc_worker_class, name, startup_evt, shutdown_evt, event_q, *args):
    proc_worker = proc_worker_class(name, startup_evt, shutdown_evt, event_q, *args)
    proc_worker.run()


class TimerProcWorker(ProcWorker):
    INTERVAL_SECS = 10
    MAX_SLEEP_SECS = 0.02

    def main_loop(self):
        self.log(logging.DEBUG, "Entering TimerProcWorker.main_loop")
        next_time = time.time() + self.INTERVAL_SECS
        while not self.shutdown_event.is_set():
            sleep_secs = _sleep_secs(self.MAX_SLEEP_SECS, next_time)
            time.sleep(sleep_secs)
            if time.time() >= next_time:
                self.log(logging.DEBUG, f"TimerProcWorker.main_loop : calling main_func")
                self.main_func()
                next_time = time.time() + self.INTERVAL_SECS

    def main_func(self):
        raise NotImplementedError("TimerProcWorker.main_func is not implemented")


class QueueProcWorker(ProcWorker):
    def init_args(self, args):
        self.log(logging.DEBUG, f"Entering QueueProcWorker.init_args : {args}")
        self.work_q, = args

    def main_loop(self):
        self.log(logging.DEBUG, "Entering QueueProcWorker.main_loop")
        while not self.shutdown_event.is_set():
            item = _queue_get(self.work_q)
            if not item:
                continue
            self.log(logging.DEBUG, f"QueueProcWorker.main_loop received '{item}' message")
            if item == "END":
                break
            else:
                self.main_func(item)

    def main_func(self, item):
        raise NotImplementedError("QueueProcWorker.main_func is not implemented")


class Proc:
    STARTUP_WAIT_SECS = 3.0

    def __init__(self, name, worker_class, shutdown_event, event_q, *args):
        self.log = functools.partial(logger, f'{name} Worker')
        self.name = name
        self.shutdown_event = shutdown_event
        self.startup_event = Event()
        self.proc = Process(target=proc_worker_wrapper,
                            args=(worker_class, name, self.startup_event, shutdown_event, event_q, *args))
        self.log(logging.DEBUG, f"Proc.__init__ starting : {name}")
        self.proc.start()
        started = self.startup_event.wait(timeout=Proc.STARTUP_WAIT_SECS)
        self.log(logging.DEBUG, f"Proc.__init__ starting : {name} got {started}")
        if not started:
            raise RuntimeError(f"Process {name} failed to startup after {Proc.STARTUP_WAIT_SECS} seconds")

    def stop(self, wait_time):
        self.log(logging.DEBUG, f"Proc.__init__ stoping : {self.name}")
        self.shutdown_event.set()
        self.proc.join(wait_time)
        if self.proc.is_alive():
            self.log(logging.DEBUG, f"Proc.__init__ terminating : {self.name} after waiting {wait_time:7.4f} seconds")
            self.proc.terminate()


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
            reply = _queue_get(self.reply_q, timeout=self.SOCKET_TIMEOUT)
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

    event_q = Queue()
    reply_q = Queue()
    send_q = Queue()

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
            event = _queue_get(event_q)
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
            _close_queue(q)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
