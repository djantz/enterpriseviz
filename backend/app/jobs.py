# ----------------------------------------------------------------------
# Enterpriseviz
# Copyright (C) 2025 David C Jantz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# ----------------------------------------------------------------------
"""
Background jobs: the registry, the enqueue API and the execution context.

This replaces celery. Work is queued as a row in the Job table and run by the
``run_worker`` management command, so nothing outside PostgreSQL is needed —
which is the requirement on the Windows/IIS host, where there is no broker to
run and celery's prefork pool, SIGUSR1 soft time limits and SIGKILL revokes do
not exist.

The vocabulary is deliberately Django's own Tasks framework (``@task``,
``.enqueue()``, a result with ``.id`` and ``.status``): django.tasks ships no
worker and none of the progress, cron or cancellation this application needs,
so the dependency would buy nothing — but matching its shape keeps the door
open and makes the code read the way a Django developer expects.

Two things a task author needs:

    @task(name="Update webmaps", portal_arg="instance_alias", time_limit=6000)
    def update_webmaps(self, instance_alias, full_refresh=False):
        self.progress.set_progress(0, total, "Connecting…")
        for item in items:
            self.checkpoint()      # raises if canceled or past the deadline
            ...

and, from a view::

    job = update_webmaps.enqueue(portal.alias, False)
    job.id   # -> the progress-polling URL
"""
import functools
import importlib
import inspect
import logging
import re
import threading
import time
import traceback as traceback_module
from datetime import timedelta

from django.db import connections, transaction
from django.utils import timezone

from .models import Job
from .request_context import (get_django_request_context, get_job_context,
                              job_logging_context)

logger = logging.getLogger("enterpriseviz.jobs")

#: Registry key -> _RegisteredTask. Populated by @task at import time; the
#: worker resolves Job.func through this, so a job can only ever run a
#: function that was explicitly registered.
_REGISTRY = {}

#: How often set_progress() is allowed to write to the database. The refresh
#: tasks call it once per item, which is thousands of times per run, and the
#: progress bar only polls every 3 seconds.
PROGRESS_WRITE_INTERVAL = 1.0

#: Counters inside a progress description, stripped out before one description
#: is compared with the next. See Progress._phase.
_COUNTER_RUN = re.compile(r"[\d,]+")

#: How often checkpoint() re-reads cancel_requested. Same reasoning: the flag
#: is set by a human clicking cancel, so seconds of latency are irrelevant.
CANCEL_POLL_INTERVAL = 2.0


class JobCanceled(Exception):
    """Raised inside a job when someone requested cancellation."""


class JobDeadlineExceeded(Exception):
    """Raised inside a job when it ran past its time limit."""


class UnknownJobFunction(Exception):
    """Job.func names something that is not in the registry."""


def close_thread_connections():
    """
    Hand back this thread's database connections.

    Jobs and their batch helpers run on pool threads, and Django opens a
    connection per thread. With CONN_MAX_AGE set, a connection opened on a
    pool thread would otherwise be held until the worker exits, so a long run
    steadily consumes the server's connection budget.

    Skipped on the main thread, whose connection belongs to Django — the
    request/response cycle, the management command, or the test runner. A job
    invoked inline there must not pull the connection out from under its
    caller.
    """
    if threading.current_thread() is threading.main_thread():
        return
    try:
        connections.close_all()
    except Exception:  # pragma: no cover - closing must never mask a result
        logger.warning("Failed to close thread database connections", exc_info=True)


def discard_connections_after_error():
    """
    Drop this thread's database connections after a failure, on any thread.

    Deliberately not close_thread_connections(): that one skips the main thread
    on purpose, so a job called inline does not pull the connection out from
    under its caller. Recovery has the opposite requirement, and the worker's
    own loop *is* the main thread.

    Django only heals a dead connection at a request boundary — close_old_
    connections() runs off the request_started signal — and a management
    command has no such boundary. A connection killed by a database restart,
    a failover or an idle timeout stays in place: ensure_connection() reconnects
    only when self.connection is None, so every later query raises
    "the connection is closed" and the worker stops doing any work at all until
    the process is restarted. On the Windows host that is a Scheduled Task
    nothing supervises.

    Closing it explicitly sets it back to None, and the next query connects.
    """
    try:
        connections.close_all()
    except Exception:  # pragma: no cover - recovery must never raise
        logger.warning("Failed to discard database connections after an error",
                       exc_info=True)


def worker_thread(func):
    """
    Run ``func`` on a pool thread, then release that thread's connections.

    Also re-establishes the job's logging context. Log records are attributed
    through a thread-local, and the batch helpers run on a nested pool's
    threads rather than the one ``run_job`` set that thread-local on — so
    without this every line written by the code doing the actual per-item work
    would arrive with no job id, no request id and no user. Under celery each
    batch was a task of its own and carried those as arguments.

    The job comes from the JobContext the helpers already take as their first
    argument, so no call site has to pass anything extra.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        context = args[0] if args and isinstance(args[0], JobContext) else None
        try:
            if context is None or context.job._state.adding:
                # No job to attribute to: called directly, or detached.
                return func(*args, **kwargs)
            with job_logging_context(context.job):
                return func(*args, **kwargs)
        finally:
            close_thread_connections()

    return wrapper


class Progress:
    """
    Progress sink for a running job.

    ``set_progress`` keeps the signature celery_progress.ProgressRecorder had,
    so the ~25 call sites in tasks.py did not have to change when the recorder
    behind them did.

    Writes are throttled to PROGRESS_WRITE_INTERVAL. Two things bypass the
    interval: reaching the total, and a change of *phase* — the description
    with its counters and its per-item suffix stripped, see _phase. A short job
    that only reports a handful of phases therefore still shows every one of
    them, while a per-item label ticking over thousands of times does not cost
    a write each.
    """

    def __init__(self, job):
        self._job = job
        self._last_write = 0.0
        self._last_phase = None

    def set_progress(self, current, total, description=""):
        current = max(0, int(current or 0))
        total = max(0, int(total or 0))

        now = time.monotonic()
        phase_changed = self._phase(description) != self._last_phase
        due = (now - self._last_write) >= PROGRESS_WRITE_INTERVAL

        # Always let the final tick through, so a bar that reached its total
        # is not left one throttled write short of complete.
        complete = total and current >= total

        if not (due or phase_changed or complete):
            return

        self._last_write = now
        self._last_phase = self._phase(description)
        self._write(current, total, description)

    @staticmethod
    def _phase(description):
        """
        The part of a description that says *what* the job is doing, with the
        counters stripped out.

        Comparing raw descriptions defeated the throttle entirely on the paths
        it was written for: the refresh tasks report "1 of 8,942 services",
        "2 of 8,942 services", and so on, so the text differed on every call
        and every one of those thousands of calls wrote to the database. Only a
        genuine phase change - "Searching for services..." to "Removing
        outdated records..." - should bypass the interval; a counter ticking
        over should not.

        Two things are dropped: any run of digits, which covers the "N of M"
        labels, and anything after a colon, which is the convention the
        per-item labels follow ("Checking: {username}", "Analyzing: {title}").
        Over-normalizing is safe - the worst case is that a real phase change
        waits out the interval, which is at most one second - whereas
        under-normalizing costs a database write per item.
        """
        return _COUNTER_RUN.sub("", (description or "").split(":", 1)[0])

    def _write(self, current, total, description):
        Job.objects.filter(pk=self._job.pk).update(
            progress_current=current,
            progress_total=total,
            progress_description=description or "",
        )
        # Keep the in-memory copy honest; the worker reads it when finalizing.
        self._job.progress_current = current
        self._job.progress_total = total
        self._job.progress_description = description or ""


class NullProgress(Progress):
    """
    Progress sink for a helper running underneath someone else's job.

    The batch helpers (process_batch_maps and friends) used to be tasks with a
    progress bar of their own. They now run on threads inside their parent, and
    several of them run at once, so letting each one report would have them
    fight over a single bar. The parent counts completed batches instead.
    """

    def __init__(self, job=None):
        super().__init__(job)

    def set_progress(self, current, total, description=""):
        return None


class JobContext:
    """
    The ``self`` a task function receives.

    Carries the Job row, the progress sink, and the two cooperative checks that
    replaced celery's SIGKILL revoke and SIGUSR1 soft time limit. Both are
    cooperative because jobs run on threads, which cannot be killed from the
    outside — and because stopping at a loop boundary cannot tear a write in
    half the way a signal delivered mid-``update_or_create`` could.
    """

    def __init__(self, job, progress=None):
        self.job = job
        self.progress = progress if progress is not None else Progress(job)
        self._last_cancel_poll = 0.0
        self._canceled = False

    @property
    def id(self):
        return self.job.id

    def is_canceled(self, force=False):
        """True once someone has asked this job to stop. Throttled."""
        if self._canceled:
            return True
        if self.job._state.adding:
            # Detached context: no row to carry the flag.
            return False
        now = time.monotonic()
        if not force and (now - self._last_cancel_poll) < CANCEL_POLL_INTERVAL:
            return False
        self._last_cancel_poll = now
        self._canceled = bool(
            Job.objects.filter(pk=self.job.pk, cancel_requested=True).exists()
        )
        return self._canceled

    def is_past_deadline(self):
        deadline = self.job.deadline_at
        return bool(deadline and timezone.now() >= deadline)

    def checkpoint(self):
        """
        Stop the job if it has been canceled or has run out of time.

        Call this at loop boundaries — between items, between batches — where
        the database is in a consistent state.
        """
        if self.is_canceled():
            raise JobCanceled(f"Job {self.job.pk} was canceled")
        if self.is_past_deadline():
            raise JobDeadlineExceeded(
                f"Job {self.job.pk} exceeded its {self.job.name} time limit"
            )

    def child_context(self):
        """A context for a helper running under this job: no progress of its own."""
        child = JobContext(self.job, progress=NullProgress(self.job))
        return child


def detached_context(registered=None):
    """
    A context for a task running outside the queue.

    Backs onto an unsaved Job, so progress goes nowhere, nothing can cancel it
    and there is no deadline. For tests, management shells, and anywhere a task
    is called as a plain function.
    """
    job = Job(
        name=getattr(registered, "name", "detached"),
        func=getattr(registered, "key", ""),
    )
    return JobContext(job, progress=NullProgress(job))


def _calling_context():
    """
    Who a newly queued job should be attributed to.

    Normally the HTTP request that queued it, from the thread-local that
    RequestContextLogMiddleware populates. A job queued from inside another job
    — "Update All" fanning out to the four refresh tasks, or a tool queueing a
    resync — has no request behind it, so it inherits its parent's. Without
    that fallback every schedule-driven run produced children whose log lines
    could not be joined back to the run that caused them.

    :return: (request_id, username, client_ip, request_path), all strings.
    """
    ctx = get_django_request_context()
    if ctx.get("request_id"):
        user = ctx.get("user")
        return (
            str(ctx["request_id"]),
            getattr(user, "username", "") or "",
            ctx.get("client_ip") or "",
            ctx.get("request_path") or "",
        )

    job_ctx = get_job_context()
    return (
        str(job_ctx.get("request_id_from_caller") or ""),
        job_ctx.get("user_from_caller") or "",
        job_ctx.get("client_ip_from_caller") or "",
        job_ctx.get("request_path_from_caller") or "",
    )


class _RegisteredTask:
    """A registered callable plus how to queue it."""

    def __init__(self, func, name, portal_arg=None, time_limit=None,
                 rerunnable=True):
        self.func = func
        self.name = name or func.__name__
        self.portal_arg = portal_arg
        self.time_limit = time_limit
        self.rerunnable = rerunnable
        self.key = f"{func.__module__}.{func.__name__}"
        self._signature = inspect.signature(func)
        functools.update_wrapper(self, func)

    @property
    def signature(self):
        """The task function's signature, including the JobContext at position 0."""
        return self._signature

    @property
    def parameters(self):
        """Parameter names the task accepts."""
        return self._signature.parameters

    def __call__(self, *args, **kwargs):
        """
        Run the task here and now, without queueing it.

        A context is supplied automatically unless the caller passed one, so a
        task can be called straight from a test or a shell the way a celery
        bound task could. The worker does not come through here — it calls
        ``.func`` with the real job's context.
        """
        if args and isinstance(args[0], JobContext):
            return self.func(*args, **kwargs)
        return self.func(detached_context(self), *args, **kwargs)

    def _resolve_portal_alias(self, args, kwargs):
        """
        Work out which portal this job is for, so the run-history tables can
        filter on a column instead of matching a substring inside a serialized
        argument list the way the celery version did.
        """
        if not self.portal_arg:
            return ""
        try:
            # `self` is supplied by the worker, not the caller, so bind against
            # the signature with a placeholder standing in for it.
            bound = self._signature.bind(None, *args, **kwargs)
        except TypeError:
            return ""
        value = bound.arguments.get(self.portal_arg, "")
        return str(value) if value else ""

    def enqueue(self, *args, **kwargs):
        """
        Queue this task and return the Job row.

        The calling context is picked up ambiently, so callers no longer thread
        _request_id/_user/_client_ip/_request_path through as task kwargs.

        ATOMIC_REQUESTS is on, so the row commits with the request. The worker
        therefore cannot observe a half-written job — the opposite of celery's
        .delay(), which published the message before the transaction committed
        and could have a worker acting on rows that did not exist yet.
        """
        request_id, username, client_ip, request_path = _calling_context()

        job = Job.objects.create(
            name=self.name,
            func=self.key,
            args=list(args),
            kwargs=dict(kwargs),
            portal_alias=self._resolve_portal_alias(args, kwargs),
            request_id=request_id,
            username=username,
            client_ip=client_ip,
            request_path=request_path,
        )
        logger.info(f"Queued job '{self.name}' ({job.pk})")
        return job


def task(_func=None, *, name=None, portal_arg=None, time_limit=None,
         rerunnable=True):
    """
    Register a function as a background task.

    :param name: what the UI shows. Kept identical to the celery task names so
        existing run history and the progress bar read the same.
    :param portal_arg: the parameter naming the portal, recorded on the Job so
        run-history views can filter by it.
    :param time_limit: seconds before checkpoint() raises JobDeadlineExceeded.
        Cooperative, unlike celery's soft_time_limit, which needed SIGUSR1 and
        was therefore never going to work on Windows.
    :param rerunnable: whether the worker may requeue this job after the worker
        running it died. True for the refresh tasks, which re-derive everything
        from the portal and converge on the same rows. False for anything that
        acts on the portal — disabling accounts, unsharing items, replacing a
        service — where a partial run repeated is not the same as one run.
        Celery never re-ran a lost task at all, so this is where that guarantee
        moved to.
    """

    def decorator(func):
        registered = _RegisteredTask(
            func, name=name, portal_arg=portal_arg, time_limit=time_limit,
            rerunnable=rerunnable,
        )
        if registered.key in _REGISTRY:
            raise RuntimeError(f"Duplicate task registration for {registered.key}")
        _REGISTRY[registered.key] = registered
        return registered

    if _func is not None:
        return decorator(_func)
    return decorator


def get_task(key):
    try:
        return _REGISTRY[key]
    except KeyError:
        raise UnknownJobFunction(f"No task registered under '{key}'")


def get_task_by_name(name):
    """
    Look a task up by its display name.

    PeriodicTask.task holds the display name — "Update All", "Pro License
    Tool" — because that is what celery's task name= was set to, and the
    scheduling forms write those strings. Keeping the names identical is what
    let every existing schedule row keep working untouched.
    """
    for registered in _REGISTRY.values():
        if registered.name == name:
            return registered
    raise UnknownJobFunction(f"No task registered with the name '{name}'")


def registered_tasks():
    """Every registered task, keyed by registry key. For the worker and tests."""
    return dict(_REGISTRY)


def autodiscover_tasks():
    """
    Import every installed app's ``tasks`` module so its @task decorators run.

    The registry is only populated as a side effect of importing the module
    that defines the tasks, so something has to do that import deliberately.
    This is celery's autodiscover_tasks() by another name, and AppConfig.ready
    calls it — without it the registry was populated only incidentally, by
    views importing tasks in the web process and by the worker importing utils
    on its way to reading the log level. Either could be refactored away
    without anyone noticing until a job failed to resolve at runtime.
    """
    from django.apps import apps as django_apps

    for config in django_apps.get_app_configs():
        module = f"{config.name}.tasks"
        try:
            importlib.import_module(module)
        except ModuleNotFoundError as exc:
            # The app simply has no tasks module. A ModuleNotFoundError naming
            # anything else came from an import *inside* tasks.py and is a real
            # error worth surfacing.
            if exc.name != module:
                raise
    return registered_tasks()


def request_cancel(job_id):
    """
    Ask a job to stop.

    Only sets the flag; the job notices at its next checkpoint(). A job that
    has already finished is left alone.
    """
    return Job.objects.filter(pk=job_id).exclude(status__in=Job.TERMINAL).update(
        cancel_requested=True
    )


@worker_thread
def run_job(job, worker_id=""):
    """
    Execute one claimed Job to completion and record the outcome.

    Called on a worker pool thread. Never raises: a job that blows up is
    recorded as FAILURE, because the worker loop must survive any task.
    """
    try:
        registered = get_task(job.func)

        deadline = None
        if registered.time_limit:
            deadline = timezone.now() + timedelta(seconds=registered.time_limit)

        Job.objects.filter(pk=job.pk).update(
            status=Job.RUNNING,
            started_at=timezone.now(),
            deadline_at=deadline,
            worker_id=worker_id,
            heartbeat_at=timezone.now(),
        )
        job.status = Job.RUNNING
        job.deadline_at = deadline

        context = JobContext(job)
    except Exception as exc:
        logger.error(
            f"Job '{job.name}' ({job.pk}) could not be started: {exc}", exc_info=True
        )
        _finalize(job, Job.FAILURE, error=str(exc),
                  tb=traceback_module.format_exc())
        return

    logger.info(f"Running job '{job.name}' ({job.pk})")

    with job_logging_context(job):
        try:
            result = registered.func(context, *job.args, **job.kwargs)
        except JobCanceled:
            logger.info(f"Job '{job.name}' ({job.pk}) canceled")
            _finalize(job, Job.CANCELED, error="Canceled")
            return
        except JobDeadlineExceeded as exc:
            logger.warning(f"Job '{job.name}' ({job.pk}) hit its time limit")
            _finalize(job, Job.FAILURE, error=str(exc))
            return
        except Exception as exc:
            logger.error(
                f"Job '{job.name}' ({job.pk}) failed: {exc}", exc_info=True
            )
            _finalize(
                job,
                Job.FAILURE,
                error=str(exc),
                tb=traceback_module.format_exc(),
            )
            return

    # A task that reports success=False finished, but not cleanly. The progress
    # bar renders that as WARNING, which is what the celery version did by
    # inspecting the same key in the result payload.
    status = Job.SUCCESS
    if isinstance(result, dict) and result.get("success") is False:
        status = Job.WARNING

    _finalize(job, status, result=result)
    logger.info(f"Job '{job.name}' ({job.pk}) finished with {status}")


# Name of the task parameter carrying a temporary credential handle. Tasks that
# connect to a portal on someone's behalf take it under this name.
CREDENTIAL_ARG = "credential_token"

# What replaces it on a finished job.
CREDENTIAL_REDACTED = "[released]"


def release_job_credentials(job):
    """
    Drop the temporary credentials a finished job was given, and forget the handle.

    The handle is a bearer capability: whoever holds it can decrypt the username
    and password behind it, and it is not bound to the portal it was entered for.
    It arrives as a task argument, so it is stored in Job.args — plain text in a
    row that JobAdmin renders to any staff user and that the retention window
    keeps for a month after the job is over.

    Two separate things, both needed. Deleting the cache entry stops the handle
    working the moment the job that needed it is done, instead of letting it ride
    out a TTL that every read extends. Redacting the column stops the row itself
    from being a place to find one.

    Never raises: a job's outcome must be recorded whatever happens here.
    """
    from .utils import CredentialManager

    try:
        registered = get_task(job.func)
    except UnknownJobFunction:
        return

    if CREDENTIAL_ARG not in registered.parameters:
        return

    args, kwargs = list(job.args or []), dict(job.kwargs or {})
    try:
        bound = registered.signature.bind(None, *args, **kwargs)
    except TypeError:
        return

    token = bound.arguments.get(CREDENTIAL_ARG)
    if not token or token == CREDENTIAL_REDACTED:
        return

    try:
        CredentialManager.delete_credentials(token)
    except Exception:  # pragma: no cover - releasing must not mask the outcome
        logger.warning(f"Could not release the credentials held by job {job.pk}",
                       exc_info=True)

    bound.arguments[CREDENTIAL_ARG] = CREDENTIAL_REDACTED
    redacted_args = list(bound.args)[1:]
    Job.objects.filter(pk=job.pk).update(args=redacted_args, kwargs=dict(bound.kwargs))
    job.args, job.kwargs = redacted_args, dict(bound.kwargs)


def _finalize(job, status, result=None, error="", tb=""):
    """Write the terminal state, and top the progress bar up to complete."""
    fields = {
        "status": status,
        "finished_at": timezone.now(),
        "error": error or "",
        "traceback": tb or "",
        "heartbeat_at": None,
    }
    if result is not None:
        fields["result"] = result if isinstance(result, dict) else {"result": result}
    if status in (Job.SUCCESS, Job.WARNING) and job.progress_total:
        fields["progress_current"] = job.progress_total

    with transaction.atomic():
        Job.objects.filter(pk=job.pk).update(**fields)

    release_job_credentials(job)
