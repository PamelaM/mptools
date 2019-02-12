import functools
import logging
import signal
import time
import multiprocessing as mp
import multiprocessing.queues as mpq
from queue import Empty, Full

DEFAULT_POLLING_TIMEOUT = 0.02
MAX_SLEEP_SECS = 0.02

start_time = time.time()

def logger(name, level, msg):
    elapsed = time.time() - start_time
    hours = int(elapsed // 60)
    seconds = elapsed - (hours * 60)
    logging.log(level, f'{hours:3}:{seconds:06.3f} {name:20} {msg}')


# -- Queue handling support

class MPQueue(mpq.Queue):

    # -- See StackOverflow Article :
    #   https://stackoverflow.com/questions/39496554/cannot-subclass-multiprocessing-queue-in-python-3-5
    #
    # -- tldr; mp.Queue is a _method_ that returns an mpq.Queue object.  That object
    # requires a context for proper operation, so this __init__ does that work as well.
    def __init__(self,*args,**kwargs):
        ctx = mp.get_context()
        super().__init__(*args, **kwargs, ctx=ctx)

    def safe_get(self, timeout=DEFAULT_POLLING_TIMEOUT):
        try:
            if timeout is None:
                return self.get(block=False)
            else:
                return self.get(block=True, timeout=timeout)
        except Empty:
            return None

    def safe_put(self, item, timeout=DEFAULT_POLLING_TIMEOUT):
        try:
            self.put(item, block=False, timeout=timeout)
            return True
        except Full:
            return False

    def drain(self):
        item = self.safe_get()
        while item:
            yield item
            item = self.safe_get()

    def safe_close(self):
        num_left = sum(1 for __ in self.drain())
        self.close()
        self.join_thread()
        return num_left


# -- useful function
def _sleep_secs(max_sleep, end_time=999999999999999.9):
    # Calculate time left to sleep, no less than 0
    return max(0.0, min(end_time - time.time(), max_sleep))

# -- Standard Event Queue manager
class EventMessage:
    def __init__(self, msg_src, msg_type, msg):
        self.id = time.time()
        self.msg_src = msg_src
        self.msg_type = msg_type
        self.msg = msg

    def __str__(self):
        return f"{self.msg_src:10} - {self.msg_type:10} : {self.msg}"


# -- Signal Handling
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

# -- Worker Process classes

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
            self.event_q.safe_put(EventMessage(self.name, "SHUTDOWN", "Normal"))
        except Exception as exc:
            self.log(logging.ERROR, f"Exception Shutdown: {exc}")
            self.event_q.safe_put(EventMessage(self.name, "FATAL", f"{exc}"))
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
            if time.time() > next_time:
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
            item = self.work_q.safe_get()
            if not item:
                continue
            self.log(logging.DEBUG, f"QueueProcWorker.main_loop received '{item}' message")
            if item == "END":
                break
            else:
                self.main_func(item)

    def main_func(self, item):
        raise NotImplementedError("QueueProcWorker.main_func is not implemented")


# -- Process Wrapper

class Proc:
    STARTUP_WAIT_SECS = 3.0

    def __init__(self, name, worker_class, shutdown_event, event_q, *args):
        self.log = functools.partial(logger, f'{name} Worker')
        self.name = name
        self.shutdown_event = shutdown_event
        self.startup_event = mp.Event()
        self.proc = mp.Process(target=proc_worker_wrapper,
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
