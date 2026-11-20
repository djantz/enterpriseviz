import logging
import traceback
import math
from django.apps import apps
from django.db import connection, transaction

from .request_context import get_django_request_context, get_job_context

_log_entry_model_cache = None

def get_log_entry_model():
    global _log_entry_model_cache
    if _log_entry_model_cache is None:
        _log_entry_model_cache = apps.get_model(app_label='app', model_name='LogEntry')
    return _log_entry_model_cache

class CombinedContextFilter(logging.Filter):
    def filter(self, record):
        django_context = get_django_request_context()
        job_context = get_job_context()

        is_django_request = bool(django_context.get('request_id'))
        # A background job is running on this thread if it recorded a start time.
        is_job = bool(job_context.get('task_start_time'))

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

        elif is_job:
            # Context carried on the Job row from the request that queued it.
            record.request_id = job_context.get('request_id_from_caller')
            record.user = job_context.get('user_from_caller')
            record.client_ip = job_context.get('client_ip_from_caller')
            record.request_path = job_context.get('request_path_from_caller')
            record.request_method = None

            start_time = job_context.get('task_start_time')
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

            base = dict(
                level=record.levelname,
                logger_name=record.name,
                message=msg,
                pathname=record.pathname,
                funcName=record.funcName,
                lineno=record.lineno,
                traceback=tb_text,
            )
            context = dict(
                request_id=getattr(record, 'request_id', None),
                request_username=getattr(record, 'user', None),
                client_ip=getattr(record, 'client_ip', None),
                request_path=getattr(record, 'request_path', None),
                request_method=getattr(record, 'request_method', None),
                request_duration=getattr(record, 'request_duration', None),
            )
        except Exception:
            self._report_failure()
            return

        try:
            self._save(LogEntryModel, base, context)
        except Exception:
            # The context fields are the ones that can carry an unstorable
            # value (a malformed client IP reaching an inet column). Keep the
            # record itself rather than losing the line entirely.
            try:
                self._save(LogEntryModel, base)
            except Exception:
                self._report_failure()

    @staticmethod
    def _save(LogEntryModel, *field_groups):
        """
        Write one log row, isolating the INSERT when something else owns the
        transaction.

        Under ATOMIC_REQUESTS the whole request is one transaction, so a failed
        INSERT here marks it for rollback and PostgreSQL then rejects every
        later statement on that connection: the retry above could not have
        succeeded, and the view that emitted the record would die with
        TransactionManagementError on its next query. A savepoint contains the
        failure and leaves both the transaction and the request usable.

        Only when there is a surrounding transaction, though. The worker runs in
        autocommit, where each INSERT is already isolated and wrapping it would
        add a BEGIN and a COMMIT to every line a job logs.
        """
        fields = {}
        for group in field_groups:
            fields.update(group)
        if connection.in_atomic_block:
            with transaction.atomic():
                LogEntryModel(**fields).save()
        else:
            LogEntryModel(**fields).save()

    @staticmethod
    def _report_failure():
        import sys
        print(f"--- Logging Error (DatabaseLogHandler) ---", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
