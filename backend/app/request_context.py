import threading
import time
from functools import wraps

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

_celery_task_context = threading.local()

def get_celery_task_context():
    """Gets context relevant for Celery task logging."""
    return {
        'task_start_time': getattr(_celery_task_context, 'task_start_time', None),
        'request_id_from_caller': getattr(_celery_task_context, 'request_id_from_caller', None),
        'user_from_caller': getattr(_celery_task_context, 'user_from_caller', None),
        'client_ip_from_caller': getattr(_celery_task_context, 'client_ip_from_caller', None),
        'request_path_from_caller': getattr(_celery_task_context, 'request_path_from_caller', None),
    }

def _set_celery_task_context(request_id=None, username=None, client_ip=None, request_path=None):
    _celery_task_context.task_start_time = time.time()
    _celery_task_context.request_id_from_caller = request_id
    _celery_task_context.user_from_caller = username
    _celery_task_context.client_ip_from_caller = client_ip
    _celery_task_context.request_path_from_caller = request_path

def _clear_celery_task_context():
    vars_to_clear = [
        'task_start_time', 'request_id_from_caller', 'user_from_caller',
        'client_ip_from_caller', 'request_path_from_caller'
    ]
    for var_name in vars_to_clear:
        if hasattr(_celery_task_context, var_name):
            delattr(_celery_task_context, var_name)

def celery_logging_context(func=None):
    """
    Decorator for Celery tasks to set up basic logging context.
    Primarily sets task_start_time and handles passed-through HTTP request context.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        # Context from the caller (e.g., original HTTP request context)
        ctx_request_id = kwargs.pop('_request_id', None)
        ctx_user_id = kwargs.pop('_user', None)
        ctx_client_ip = kwargs.pop('_client_ip', None)
        ctx_request_path = kwargs.pop('_request_path', None)

        _set_celery_task_context(
            request_id=ctx_request_id,
            username=ctx_user_id,
            client_ip=ctx_client_ip,
            request_path=ctx_request_path
        )

        try:
            result = func(self, *args, **kwargs)
        finally:
            _clear_celery_task_context()
        return result

    return wrapper
