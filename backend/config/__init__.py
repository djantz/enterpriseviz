# The celery app instance that used to be imported here is gone along with the
# celery worker and its broker. Background work is queued to the database and
# run by the run_worker management command; see app/jobs.py.
#
# celery remains an installed library because django_celery_beat's
# CrontabSchedule hands back a celery.schedules.crontab, whose is_due() the
# scheduler uses — but no celery app, worker or broker is configured.

# ---------------------------------------------------------------------------
# Windows: stop a version string from shelling out to git during startup.
#
# django-cryptography-django5 declares VERSION = (2, 2, 0, 'alpha', 0), and
# Django's get_version() treats "alpha" with a serial of 0 as an unreleased
# build — so importing the package runs `git log` through subprocess purely to
# append a .devNNNN suffix to a version string nothing reads.
#
# On a console that is merely wasteful. Under IIS, or under a Task Scheduler
# task, the process has no console and therefore no valid stdin handle, and
# subprocess dies before it can even launch git:
#
#     File "subprocess.py", line 1348, in _get_handles
#       p2cread = _winapi.GetStdHandle(_winapi.STD_INPUT_HANDLE)
#     OSError: [WinError 6] The handle is invalid
#
# Django's get_git_changeset() passes capture_output=True, which redirects
# stdout and stderr but leaves stdin inherited — hence the failure on a handle
# it never wanted in the first place.
#
# Neutralizing it returns the plain version ("2.2" instead of "2.2.devNNNN"),
# which is what a checkout without git metadata already produces. This module
# is imported when DJANGO_SETTINGS_MODULE is loaded, before the app registry
# imports any models, so it covers wsgi, asgi and manage.py alike.
#
# The real defect is upstream: a released package should not ship an alpha
# version tuple. Removing django-cryptography-django5 — which also blocks
# Django 6 — removes the need for this.
def _silence_git_version_lookup():
    from django.utils import version

    if getattr(version, "_enterpriseviz_patched", False):
        return
    version.get_git_changeset = lambda: None
    version._enterpriseviz_patched = True


_silence_git_version_lookup()
