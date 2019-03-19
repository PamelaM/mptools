# -*- coding: utf-8 -*-

"""Top-level package for Multiprocessing Tools."""

__author__ = """Pamela D McA'Nulty"""
__email__ = 'pamela@mcanulty.org'
__version__ = '0.2.0'

from mptools._mptools import (
    _logger,
    MPQueue,
    _sleep_secs,
    SignalObject,
    init_signal,
    init_signals,
    default_signal_handler,
    ProcWorker,
    proc_worker_wrapper,
    TimerProcWorker,
    QueueProcWorker,
    Proc,
    EventMessage,
    MainContext,
    TerminateInterrupt,
)
__all__ = [
    '_logger',
    'MPQueue',
    '_sleep_secs',
    'SignalObject',
    'init_signal',
    'init_signals',
    'default_signal_handler',
    'ProcWorker',
    'proc_worker_wrapper',
    'TimerProcWorker',
    'QueueProcWorker',
    'Proc',
    'EventMessage',
    'MainContext',
    'TerminateInterrupt',
]
