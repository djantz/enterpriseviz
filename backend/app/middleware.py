import ipaddress
import threading
import uuid
import time

# Thread-local storage specifically for Django HTTP request context
_request_context = threading.local()

def _forwarded_for_addresses(request):
    """Every address in X-Forwarded-For, left to right, as written by the client."""
    forwarded = request.META.get('HTTP_X_FORWARDED_FOR') or ''
    return [part.strip() for part in forwarded.split(',') if part.strip()]


def client_ip(request):
    """
    The client's address, trusting X-Forwarded-For only as far as configured.

    X-Forwarded-For is *appended* to by each proxy, so the rightmost entries are
    the ones written by infrastructure and the leftmost is whatever the original
    client sent — which is to say, whatever an attacker chose. Reading entry [0]
    therefore hands the client full control of the value: it bypasses the IP half
    of the sign-in throttle and writes an address of the caller's choosing into
    LogEntry.client_ip, where it is read later as evidence.

    settings.TRUSTED_PROXY_COUNT says how many proxies actually sit in front of
    this deployment, and only that many entries are believed:

    * ``0`` (the default) — nothing in front adds the header, so it is ignored
      entirely and REMOTE_ADDR is the answer. This is right for the IIS layout,
      where HttpPlatformHandler proxies from loopback and sets no XFF, and for
      any container reached directly.
    * ``1`` — one reverse proxy, which appends the peer it saw. The last entry
      is that address; earlier ones came from the client.
    * ``n`` — the nth entry from the right, for a chain of n proxies you run.

    A value that is not an address is discarded rather than passed on. It ends up
    in a GenericIPAddressField over a PostgreSQL inet column, where a bad value
    makes the INSERT fail; DatabaseLogHandler swallows that, the row is lost, and
    — because ATOMIC_REQUESTS wraps the view — the failed save also marks the
    request's transaction for rollback, so the next query raises
    TransactionManagementError. One junk header used to be enough to do that.

    :return: A valid IP address string, or None when there is no usable one.
    """
    from django.conf import settings

    trusted = getattr(settings, 'TRUSTED_PROXY_COUNT', 0)
    candidates = []

    if trusted > 0:
        forwarded = _forwarded_for_addresses(request)
        if len(forwarded) >= trusted:
            candidates.append(forwarded[-trusted])

    # REMOTE_ADDR is the one value the client cannot choose, so it is both the
    # fallback and the answer whenever no proxy is trusted.
    candidates.append(request.META.get('REMOTE_ADDR') or '')

    for candidate in candidates:
        try:
            return str(ipaddress.ip_address(candidate.strip()))
        except ValueError:
            continue
    return None


def _set_django_request_context(request):
    """Sets Django request context in thread-local storage."""
    _request_context.request_id = uuid.uuid4()
    _request_context.request_start_time = time.time()
    _request_context.client_ip = client_ip(request)
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
    """
    Attribute log records to the request that caused them.

    The thread-local this sets is what CombinedContextFilter reads to stamp
    every LogEntry with a request id, username and client IP.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        _set_django_request_context(request)
        try:
            return self.get_response(request)
        finally:
            _clear_django_request_context()

def get_django_context():
    return _request_context
