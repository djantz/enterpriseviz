import threading
import uuid
import time

# Thread-local storage specifically for Django HTTP request context
_request_context = threading.local()

def _get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip

def _set_django_request_context(request):
    """Sets Django request context in thread-local storage."""
    _request_context.request_id = uuid.uuid4()
    _request_context.request_start_time = time.time()
    _request_context.client_ip = _get_client_ip(request)
    _request_context.request_path = request.path
    _request_context.request_method = request.method

    if hasattr(request, 'user') and request.user.is_authenticated:
        _request_context.user = request.user
    else:
        _request_context.user = None

def _clear_django_request_context():
    """Clears Django request context from thread-local storage."""
    vars_to_clear = [
        'request_id', 'user', 'client_ip',
        'request_start_time', 'request_path', 'request_method'
    ]
    for var_name in vars_to_clear:
        if hasattr(_request_context, var_name):
            delattr(_request_context, var_name)


class RequestContextLogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        _set_django_request_context(request)
        response = self.get_response(request)
        _clear_django_request_context()
        return response

def get_django_context():
    return _request_context
