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
The background worker: replaces `celery worker` and `celery beat` with one
process that needs no broker.

Each tick it

  1. enqueues any PeriodicTask that has come due,
  2. claims queued jobs with SELECT ... FOR UPDATE SKIP LOCKED,
  3. runs them on a thread pool,
  4. heartbeats what it is running and reclaims what a dead worker left behind,
  5. trims finished jobs past the retention window.

On Windows this runs as a Scheduled Task set to trigger at startup, because
the deployment may only install Microsoft-signed IIS modules — no NSSM. It is
deliberately independent of IIS: an app-pool recycle must not interrupt a
running portal refresh.

    python manage.py run_worker
    python manage.py run_worker --once        # one tick, for tests and cron
    python manage.py run_worker --concurrency 4
"""
import json
import logging
import os
import signal
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection, transaction
from django.utils import timezone
from django_celery_beat.models import PeriodicTask

from app import jobs as jobs_module
from app.jobs import (close_thread_connections, discard_connections_after_error,
                      get_task, get_task_by_name, release_job_credentials,
                      run_job)
from app.models import Job, SiteSettings

logger = logging.getLogger("enterpriseviz.worker")


class Command(BaseCommand):
    help = "Run background jobs and due schedules from the database queue."

    def add_arguments(self, parser):
        parser.add_argument(
            "--concurrency", type=int, default=None,
            help="Jobs to run at once (default: settings.JOB_CONCURRENCY).",
        )
        parser.add_argument(
            "--poll-interval", type=float, default=None,
            help="Seconds between queue polls (default: settings.JOB_POLL_INTERVAL).",
        )
        parser.add_argument(
            "--once", action="store_true",
            help="Run a single tick and exit, waiting for any claimed jobs.",
        )
        parser.add_argument(
            "--no-schedule", action="store_true",
            help="Do not enqueue due PeriodicTasks; only drain the queue.",
        )

    def handle(self, *args, **options):
        self.concurrency = options["concurrency"] or settings.JOB_CONCURRENCY
        self.poll_interval = options["poll_interval"] or settings.JOB_POLL_INTERVAL
        self.run_schedules = not options["no_schedule"]
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()
        self._running = {}
        self._running_lock = threading.Lock()
        self._last_purge = 0.0

        self._install_signal_handlers()
        self._apply_configured_log_level()

        logger.info(
            f"Worker {self.worker_id} starting: concurrency={self.concurrency}, "
            f"poll={self.poll_interval}s, tasks={len(jobs_module.registered_tasks())}"
        )

        with ThreadPoolExecutor(max_workers=self.concurrency,
                                thread_name_prefix="job") as pool:
            self.pool = pool
            try:
                if options["once"]:
                    self._tick()
                else:
                    self._loop()
            finally:
                logger.info("Worker draining in-flight jobs before exit")

        logger.info(f"Worker {self.worker_id} stopped")

    # -- lifecycle ---------------------------------------------------------

    def _install_signal_handlers(self):
        """
        Stop cleanly on the signals each platform actually sends.

        SIGBREAK exists only on Windows and is what a Scheduled Task's "End
        task" delivers; SIGTERM does not exist there at all, so it is attached
        conditionally rather than assumed.
        """
        def _request_stop(signum, _frame):
            logger.info(f"Signal {signum} received; finishing in-flight jobs")
            self._stop.set()

        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            sig = getattr(signal, name, None)
            if sig is None:
                continue
            try:
                signal.signal(sig, _request_stop)
            except (ValueError, OSError):
                # Not the main thread, or the platform refuses this signal.
                logger.debug(f"Could not install a handler for {name}")

    def _apply_configured_log_level(self):
        """
        Apply the log level configured in SiteSettings.

        Celery did this from worker_process_init, which fired per forked child
        and so would never have run under a thread pool. Changing the level in
        the UI reaches a running worker by way of the "Apply log level" job the
        settings view queues, so this only has to cover startup.
        """
        try:
            from app.utils import apply_global_log_level
            apply_global_log_level()
        except Exception as exc:
            logger.warning(f"Could not apply the configured log level: {exc}")
        finally:
            close_thread_connections()

    def _loop(self):
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                self._tick()
            except Exception as exc:
                # The loop has to outlive anything a tick can do to it,
                # otherwise one bad row stops all background work.
                logger.error(f"Worker tick failed: {exc}", exc_info=True)
                # The usual cause is the database going away — a restart, a
                # failover, an idle timeout. Django will not reconnect on its
                # own here, so drop the connection and let the next tick open
                # a fresh one. See discard_connections_after_error().
                discard_connections_after_error()

            elapsed = time.monotonic() - started
            self._stop.wait(max(0.0, self.poll_interval - elapsed))

    def _tick(self):
        # Heartbeat first. Anything later in the tick can raise — a schedule row
        # the scheduler cannot evaluate, or the database going away — and a tick
        # that dies before this point leaves jobs this worker is still running
        # looking abandoned to _reclaim_orphaned in another worker.
        self._heartbeat()
        if self.run_schedules:
            self._enqueue_due_schedules()
        self._reclaim_orphaned()
        self._cancel_queued()
        self._purge_old_jobs()
        self._claim_and_dispatch()

    # -- queue -------------------------------------------------------------

    def _free_slots(self):
        with self._running_lock:
            return max(0, self.concurrency - len(self._running))

    def _claim_and_dispatch(self):
        slots = self._free_slots()
        if not slots:
            return

        for job in self._claim(slots):
            with self._running_lock:
                self._running[job.pk] = True
            self.pool.submit(self._run, job)

    def _claim(self, limit):
        """
        Take up to ``limit`` queued jobs, atomically.

        FOR UPDATE SKIP LOCKED lets a second worker pass over rows this one has
        already locked instead of blocking on them, so the queue stays correct
        if the deployment ever runs more than the single worker it does today.

        Rows already asked to stop are passed over rather than started and then
        interrupted at the first checkpoint; _cancel_queued finalizes those.
        """
        sql = """
            UPDATE app_job
               SET status = %s, worker_id = %s, heartbeat_at = %s,
                   attempts = attempts + 1
             WHERE id IN (
                   SELECT id FROM app_job
                    WHERE status = %s
                      AND cancel_requested = FALSE
                    ORDER BY queued_at
                    LIMIT %s
                      FOR UPDATE SKIP LOCKED
             )
            RETURNING id
        """
        now = timezone.now()
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(
                    sql, [Job.RUNNING, self.worker_id, now, Job.QUEUED, limit]
                )
                claimed = [row[0] for row in cursor.fetchall()]

        if not claimed:
            return []
        # Preserve queue order; the UPDATE ... RETURNING order is unspecified.
        return list(Job.objects.filter(pk__in=claimed).order_by("queued_at"))

    def _run(self, job):
        try:
            run_job(job, worker_id=self.worker_id)
        except Exception as exc:
            logger.error(f"Job {job.pk} could not be finalised: {exc}", exc_info=True)
            try:
                Job.objects.filter(pk=job.pk, status=Job.RUNNING).update(
                    status=Job.FAILURE,
                    finished_at=timezone.now(),
                    heartbeat_at=None,
                    error=str(exc),
                )
            except Exception:
                logger.error(f"Job {job.pk} left in {Job.RUNNING}", exc_info=True)
        finally:
            with self._running_lock:
                self._running.pop(job.pk, None)

    # -- housekeeping ------------------------------------------------------

    def _heartbeat(self):
        """
        Say that this worker is alive, for every job it holds.

        Deliberately per worker rather than per job: it is meant to detect a
        worker that died, not a job that is merely slow. A portal refresh can
        legitimately spend many minutes inside one ArcGIS call.
        """
        with self._running_lock:
            running = list(self._running)
        if running:
            Job.objects.filter(pk__in=running).update(heartbeat_at=timezone.now())

    def _reclaim_orphaned(self):
        """
        Deal with jobs whose worker stopped reporting.
        """
        cutoff = timezone.now() - timedelta(seconds=settings.JOB_HEARTBEAT_TIMEOUT)
        orphaned = list(
            Job.objects.filter(status=Job.RUNNING, heartbeat_at__lt=cutoff)
            .exclude(worker_id=self.worker_id)
        )
        if not orphaned:
            return

        requeue, abandon = [], {}
        for job in orphaned:
            reason = self._abandon_reason(job)
            if reason:
                abandon[job.pk] = reason
            else:
                requeue.append(job.pk)

        if requeue:
            Job.objects.filter(pk__in=requeue).update(
                status=Job.QUEUED,
                worker_id="",
                heartbeat_at=None,
                started_at=None,
                progress_description="Requeued after the previous worker stopped",
            )
            logger.warning(f"Requeued {len(requeue)} job(s) orphaned by a stopped worker")

        for pk, reason in abandon.items():
            Job.objects.filter(pk=pk).update(
                status=Job.FAILURE,
                worker_id="",
                heartbeat_at=None,
                finished_at=timezone.now(),
                error=reason,
                progress_description="Abandoned",
            )
        for job in orphaned:
            # These never reach _finalize, so release their credentials here.
            if job.pk in abandon:
                release_job_credentials(job)
        if abandon:
            logger.error(
                f"Failed {len(abandon)} orphaned job(s) rather than rerunning them: "
                + "; ".join(f"{pk} ({reason})" for pk, reason in abandon.items())
            )

    @staticmethod
    def _abandon_reason(job):
        """Why this orphan must not be requeued, or "" if it may be."""
        try:
            registered = get_task(job.func)
        except jobs_module.UnknownJobFunction:
            return f"No task is registered under '{job.func}'"

        if not registered.rerunnable:
            return ("The worker running this job stopped. It is not safe to "
                    "rerun automatically; start it again if you still want it.")

        if job.attempts >= settings.JOB_MAX_ATTEMPTS:
            return (f"The worker stopped while running this job "
                    f"{job.attempts} times; not retrying again.")

        return ""

    def _cancel_queued(self):
        """
        Finalize jobs canceled before they ever started.

        _claim passes over these, so without this they would sit in the queue
        as QUEUED forever with the progress bar still polling them.
        """
        pending = list(Job.objects.filter(status=Job.QUEUED, cancel_requested=True))
        if not pending:
            return

        canceled = Job.objects.filter(
            pk__in=[job.pk for job in pending]
        ).update(
            status=Job.CANCELED,
            finished_at=timezone.now(),
            error="Canceled",
            progress_description="Canceled before it started",
            heartbeat_at=None,
        )
        for job in pending:
            release_job_credentials(job)
        if canceled:
            logger.info(f"Canceled {canceled} job(s) that had not started")

    def _purge_old_jobs(self):
        """
        Drop finished jobs past the retention window, at most hourly.

        The window is SiteSettings.job_retention_days, so changing it in the
        UI takes effect at the next purge without restarting the worker.

        The hourly guard comes first, and stamps before reading the window: the
        settings row was being fetched every tick — every few seconds, forever —
        to decide not to purge. Stamping before the `days` check matters too,
        or a deployment with retention switched off falls through without
        stamping and is straight back to a query per tick.
        """
        now = time.monotonic()
        if now - self._last_purge < 3600:
            return
        self._last_purge = now

        days = SiteSettings.load().job_retention_days
        if not days:
            return

        cutoff = timezone.now() - timedelta(days=days)
        deleted, _ = Job.objects.filter(
            status__in=Job.TERMINAL, finished_at__lt=cutoff
        ).delete()
        if deleted:
            logger.info(f"Purged {deleted} job(s) finished before {cutoff}")

    # -- scheduler ---------------------------------------------------------

    def _enqueue_due_schedules(self):
        """
        Queue a Job for every PeriodicTask that has come due.

        This is what celery beat did. The schedules stay in django_celery_beat's
        PeriodicTask/CrontabSchedule tables, so the per-portal scheduling UI and
        Portal.task carried over untouched — only the thing reading them changed.
        """
        now = timezone.now()

        for periodic in PeriodicTask.objects.filter(enabled=True).select_related("crontab"):
            try:
                if not self._is_due(periodic, now):
                    continue
                self._enqueue_periodic(periodic, now)
            except Exception as exc:
                logger.error(
                    f"Could not evaluate schedule '{periodic.name}': {exc}", exc_info=True
                )

    def _is_due(self, periodic, now):
        if periodic.start_time and now < periodic.start_time:
            return False
        if periodic.expires and now >= periodic.expires:
            return False
        if periodic.one_off and periodic.total_run_count:
            return False

        schedule = self._schedule_for(periodic)
        if schedule is None:
            return False

        last_run = periodic.last_run_at
        if last_run is None:
            # Never run: start the window now rather than firing immediately,
            # so adding a schedule does not also trigger it.
            PeriodicTask.objects.filter(pk=periodic.pk).update(last_run_at=now)
            return False

        if last_run > now:
            # A last run in the future means the schedule can never come due
            # again, and it would fail silently — the schedule simply stops.
            # celery beat wrote these timestamps against its own clock, which
            # was UTC while this project runs USE_TZ=False, so every schedule
            # carried over from the celery deployment can land here once.
            logger.warning(
                f"Schedule '{periodic.name}' has a last run in the future "
                f"({last_run}); resetting it to now so it can fire again."
            )
            PeriodicTask.objects.filter(pk=periodic.pk).update(last_run_at=now)
            return False

        due, _next = schedule.is_due(last_run)
        return bool(due)

    @staticmethod
    def _schedule_for(periodic):
        """
        The celery schedule object behind this PeriodicTask.

        celery is still installed as a library purely for this: its crontab
        implements the cron arithmetic that django_celery_beat's CrontabSchedule
        defers to. No celery app, worker or broker is involved.

        nowfun is overridden deliberately. A celery schedule reads the clock
        through its app, and with no app configured that is the default one,
        which reports aware UTC. This project runs USE_TZ=False, so the
        last_run_at it is compared against is naive local time — seven hours
        adrift here. The mismatch does not error, it just makes is_due() answer
        True for every schedule on every tick, which would have re-fired every
        portal refresh every few seconds. Pinning both sides to
        django.utils.timezone.now keeps the comparison in one clock.
        """
        for attr in ("crontab", "interval", "solar", "clocked"):
            model = getattr(periodic, attr, None)
            if model is None:
                continue
            schedule = model.schedule
            schedule.nowfun = timezone.now
            return schedule
        return None

    def _enqueue_periodic(self, periodic, now):
        try:
            registered = get_task_by_name(periodic.task)
        except jobs_module.UnknownJobFunction:
            logger.error(
                f"Schedule '{periodic.name}' names unknown task '{periodic.task}'; "
                f"disabling it so it stops being retried every tick."
            )
            PeriodicTask.objects.filter(pk=periodic.pk).update(enabled=False)
            return

        args = json.loads(periodic.args or "[]")
        kwargs = json.loads(periodic.kwargs or "{}")

        # Claim the tick before queueing. Two workers evaluating the same
        # schedule in the same second would otherwise both queue it; whichever
        # updates last_run_at first wins and the other sees no rows.
        claimed = PeriodicTask.objects.filter(
            pk=periodic.pk, last_run_at=periodic.last_run_at
        ).update(last_run_at=now, total_run_count=periodic.total_run_count + 1)
        if not claimed:
            return

        job = registered.enqueue(*args, **kwargs)
        Job.objects.filter(pk=job.pk).update(periodic_task_name=periodic.name)
        logger.info(f"Schedule '{periodic.name}' queued job {job.pk}")
