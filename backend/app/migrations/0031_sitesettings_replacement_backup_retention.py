"""
Make the replacement backup retention window configurable.

The third and last retention window, and the only one that was never reachable
at all: purge_expired_replacement_backups read REPLACEMENT_BACKUP_RETENTION_DAYS
through getattr with a default of 90, nothing ever defined it, and the README
documented it as a Django setting. The column default is that same 90, so no
deployment changes behaviour — it just becomes possible to change.
"""

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("app", "0030_inactive_user_action_limits"),
    ]

    operations = [
        migrations.AddField(
            model_name="sitesettings",
            name="replacement_backup_retention_days",
            field=models.PositiveIntegerField(
                default=90,
                help_text="Delete Replace Service item backups older than this, after which those replacements can no longer be reverted. 0 keeps them indefinitely.",
                verbose_name="Replacement backup retention (days)",
            ),
        ),
    ]
