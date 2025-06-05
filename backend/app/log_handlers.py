import logging
import traceback
import math
from django.apps import apps

from .request_context import get_django_request_context, get_celery_task_context

_log_entry_model_cache = None

def get_log_entry_model():
    global _log_entry_model_cache
    if _log_entry_model_cache is None:
        _log_entry_model_cache = apps.get_model(app_label='app', model_name='LogEntry')
    return _log_entry_model_cache

class CombinedContextFilter(logging.Filter):
    def filter(self, record):
        django_context = get_django_request_context()
        celery_context = get_celery_task_context()

        is_django_request = bool(django_context.get('request_id'))
        # Celery task if has task start time
        is_celery_task = bool(celery_context.get('task_start_time'))

        record.request_id = None
        record.user = None
        record.client_ip = None
        record.request_path = None
        record.request_method = None
        record.request_duration = None

        if is_django_request:
            record.request_id = django_context.get('request_id')
            record.user = django_context.get('user')
            record.client_ip = django_context.get('client_ip')
            record.request_path = django_context.get('request_path')
            record.request_method = django_context.get('request_method')
            start_time = django_context.get('request_start_time')
            if start_time:
                duration_ms = (record.created - start_time) * 1000
                record.request_duration = round(duration_ms, 2)

        elif is_celery_task:
            # Use context passed from the original request
            record.request_id = celery_context.get('request_id_from_caller')
            record.user = celery_context.get('user_from_caller')
            record.client_ip = celery_context.get('client_ip_from_caller')
            record.request_path = celery_context.get('request_path_from_caller')
            record.request_method = None

            start_time = celery_context.get('task_start_time')
            if start_time:
                duration_ms = (record.created - start_time) * 1000
                record.request_duration = round(duration_ms, 2)
        return True


class DatabaseLogHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level=level)

    def emit(self, record: logging.LogRecord):
        LogEntryModel = get_log_entry_model()
        try:
            msg = self.format(record)
            tb_text = None
            if record.exc_info:
                if not record.exc_text:
                    record.exc_text = self.formatException(record.exc_info)
                tb_text = record.exc_text

            log_entry = LogEntryModel(
                level=record.levelname,
                logger_name=record.name,
                message=msg,
                pathname=record.pathname,
                funcName=record.funcName,
                lineno=record.lineno,
                traceback=tb_text,

                # Context fields from CombinedContextFilter
                request_id=getattr(record, 'request_id', None),
                request_username=getattr(record, 'user', None),
                client_ip=getattr(record, 'client_ip', None),
                request_path=getattr(record, 'request_path', None),
                request_method=getattr(record, 'request_method', None),
                request_duration=getattr(record, 'request_duration', None),
            )
            log_entry.save()
        except Exception:
            import sys
            print(f"--- Logging Error (DatabaseLogHandler) ---", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
