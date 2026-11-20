"""
Ambient context for log records.

Two sources feed the log filter in app.log_handlers: the HTTP request being
served, and the background job being run. Both are stored in thread-locals,
which is exactly right now that jobs run on worker pool threads — under
celery's prefork pool this module was relying on each task having a process to
itself, and would have broken under any threaded pool.
"""
import threading
import time
from contextlib import contextmanager

from .middleware import get_django_context


def get_django_request_context():
    store = get_django_context()
    return {
        'request_id': getattr(store, 'request_id', None),
        'user': getattr(store, 'user', None),
        'client_ip': getattr(store, 'client_ip', None),
        'request_start_time': getattr(store, 'request_start_time', None),
        'request_path': getattr(store, 'request_path', None),
        'request_method': getattr(store, 'request_method', None),
    }


_job_context = threading.local()


def get_job_context():
    """Context for log records emitted from inside a background job."""
    return {
        'task_start_time': getattr(_job_context, 'task_start_time', None),
        'request_id_from_caller': getattr(_job_context, 'request_id_from_caller', None),
        'user_from_caller': getattr(_job_context, 'user_from_caller', None),
        'client_ip_from_caller': getattr(_job_context, 'client_ip_from_caller', None),
        'request_path_from_caller': getattr(_job_context, 'request_path_from_caller', None),
        'job_id': getattr(_job_context, 'job_id', None),
    }


def _set_job_context(job):
    _job_context.task_start_time = time.time()
    _job_context.job_id = str(job.pk)
    _job_context.request_id_from_caller = job.request_id or None
    _job_context.user_from_caller = job.username or None
    _job_context.client_ip_from_caller = job.client_ip or None
    _job_context.request_path_from_caller = job.request_path or None


def _clear_job_context():
    for var_name in (
        'task_start_time', 'job_id', 'request_id_from_caller', 'user_from_caller',
        'client_ip_from_caller', 'request_path_from_caller',
    ):
        if hasattr(_job_context, var_name):
            delattr(_job_context, var_name)


@contextmanager
def job_logging_context(job):
    """
    Attribute log records written while a job runs to the request that queued it.

    The context is carried on the Job row rather than smuggled through task
    kwargs as _request_id/_user/_client_ip/_request_path, which is how the
    celery version did it and why every task signature had to accept and pop
    four arguments it never used.
    """
    _set_job_context(job)
    try:
        yield
    finally:
        _clear_job_context()
