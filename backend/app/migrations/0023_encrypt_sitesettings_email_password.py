"""
Encrypt SiteSettings.email_password at rest.

The column was plain varchar while webhook_secret next to it was already
encrypted, so anyone with read access to the database — a backup, a replica, a
support query — could read the SMTP password.

encrypt() stores bytea, so the value cannot be converted in place by altering
the column. The password is copied through a temporary encrypted field, which
preserves the configured value instead of forcing an operator to re-enter it.
"""
import django_cryptography.fields
from django.db import migrations, models


def copy_password_to_encrypted(apps, schema_editor):
    """Read the plaintext column and write it through the encrypted field."""
    SiteSettings = apps.get_model("app", "SiteSettings")
    for row in SiteSettings.objects.exclude(email_password__isnull=True).exclude(email_password=""):
        row.email_password_encrypted = row.email_password
        row.save(update_fields=["email_password_encrypted"])


def copy_password_to_plaintext(apps, schema_editor):
    """Reverse: decrypt back into the plaintext column."""
    SiteSettings = apps.get_model("app", "SiteSettings")
    for row in SiteSettings.objects.all():
        if row.email_password_encrypted:
            row.email_password = row.email_password_encrypted
            row.save(update_fields=["email_password"])


class Migration(migrations.Migration):

    dependencies = [
        ("app", "0022_remove_layer_layer_used_by_apps_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="sitesettings",
            name="email_password_encrypted",
            field=django_cryptography.fields.encrypt(
                models.CharField(blank=True, max_length=255, null=True)
            ),
        ),
        migrations.RunPython(copy_password_to_encrypted, copy_password_to_plaintext),
        migrations.RemoveField(
            model_name="sitesettings",
            name="email_password",
        ),
        migrations.RenameField(
            model_name="sitesettings",
            old_name="email_password_encrypted",
            new_name="email_password",
        ),
    ]
