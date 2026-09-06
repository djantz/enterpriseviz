"""
Move the two nightly purges into PeriodicTask rows.

They used to live in settings.CELERY_BEAT_SCHEDULE, which celery beat merged
into the PeriodicTask table on startup. Nothing does that merge any more — the
run_worker scheduler reads the table and only the table — so the entries are
created here instead. Putting them in the same place as the schedules the UI
creates also means an operator can see and adjust them.
"""
import json

from django.db import migrations

PURGES = [
    {
        "name": "purge-expired-replacement-backups",
        "task": "Purge expired replacement backups",
        "hour": "3",
        "minute": "0",
        "description": "Delete replacement backups past their retention window.",
    },
    {
        "name": "purge-old-log-entries",
        "task": "Purge old log entries",
        "hour": "3",
        "minute": "30",
        "description": "Delete application log rows past the retention window "
                       "set under Settings > Retention.",
    },
]


def create_purge_schedules(apps, schema_editor):
    CrontabSchedule = apps.get_model("django_celery_beat", "CrontabSchedule")
    PeriodicTask = apps.get_model("django_celery_beat", "PeriodicTask")

    # django_celery_beat seeds a celery.backend_cleanup entry to expire rows in
    # the celery result backend. Nothing writes that backend any more — the
    # worker trims the Job table itself on SiteSettings.job_retention_days — and the
    # scheduler would only find the task unresolvable and disable it, with a
    # misleading error every time a fresh database is set up.
    PeriodicTask.objects.filter(task="celery.backend_cleanup").delete()

    for purge in PURGES:
        schedule, _ = CrontabSchedule.objects.get_or_create(
            minute=purge["minute"],
            hour=purge["hour"],
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        )
        PeriodicTask.objects.update_or_create(
            name=purge["name"],
            defaults={
                "task": purge["task"],
                "crontab": schedule,
                "args": json.dumps([]),
                "kwargs": json.dumps({}),
                "enabled": True,
                "description": purge["description"],
            },
        )


def remove_purge_schedules(apps, schema_editor):
    PeriodicTask = apps.get_model("django_celery_beat", "PeriodicTask")
    PeriodicTask.objects.filter(name__in=[p["name"] for p in PURGES]).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("app", "0025_job"),
        # __latest__, not 0001_initial. The historical PeriodicTask this
        # migration writes through is built from whatever django_celery_beat
        # migrations are guaranteed to have run first, and fields added after
        # 0001_initial - one_off and headers among them - are NOT NULL with no
        # database-level default once Django has applied them. Pinned to
        # 0001_initial, an INSERT here omits those columns and fails on a fresh
        # database whenever the plan happens to order this migration first.
        ("django_celery_beat", "__latest__"),
    ]

    operations = [
        migrations.RunPython(create_purge_schedules, remove_purge_schedules),
    ]
