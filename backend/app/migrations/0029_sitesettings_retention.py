"""
Give the two retention windows a home in SiteSettings.

Neither was configurable before — log entries and finished jobs were never
trimmed — so there is nothing to carry across and no deployment sees a value
change. The column defaults here are the windows that now apply, and the row is
the only source.

Also normalizes logging_level, whose default was the lowercase 'warning' while
LOG_LEVEL_CHOICES only ever contained uppercase names. Existing rows keep the
value they were saved with, so they need correcting or the level dropdown
renders with nothing selected.
"""

from django.db import migrations, models


def normalize_logging_level(apps, schema_editor):
    SiteSettings = apps.get_model("app", "SiteSettings")

    for row in SiteSettings.objects.exclude(logging_level=""):
        upper = row.logging_level.upper()
        if upper != row.logging_level:
            row.logging_level = upper
            row.save(update_fields=["logging_level"])

    # The purge schedule's description, which an operator reads in the schedule
    # list, named the environment variable. Point it at where the window is now.
    PeriodicTask = apps.get_model("django_celery_beat", "PeriodicTask")
    PeriodicTask.objects.filter(name="purge-old-log-entries").update(
        description="Delete application log rows past the retention window set in Settings > Logs."
    )


def denormalize(apps, schema_editor):
    """Nothing to undo: an uppercase level is valid either way."""


class Migration(migrations.Migration):
    dependencies = [
        ("app", "0028_job_attempts"),
        ("django_celery_beat", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="sitesettings",
            name="job_retention_days",
            field=models.PositiveIntegerField(
                default=30,
                help_text="Delete finished background jobs older than this. 0 keeps them indefinitely.",
                verbose_name="Job history retention (days)",
            ),
        ),
        migrations.AddField(
            model_name="sitesettings",
            name="log_retention_days",
            field=models.PositiveIntegerField(
                default=90,
                help_text="Delete application log entries older than this. 0 keeps them indefinitely.",
                verbose_name="Log retention (days)",
            ),
        ),
        migrations.AlterField(
            model_name="sitesettings",
            name="logging_level",
            field=models.CharField(
                choices=[
                    ("INFO", "Info"),
                    ("WARNING", "Warning"),
                    ("ERROR", "Error"),
                    ("DEBUG", "Debug"),
                    ("CRITICAL", "Critical"),
                ],
                default="WARNING",
                max_length=255,
            ),
        ),
        migrations.RunPython(normalize_logging_level, denormalize),
    ]
