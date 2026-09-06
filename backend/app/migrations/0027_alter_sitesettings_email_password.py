"""
Bring SiteSettings.email_password into line with the model.

READ BEFORE DEPLOYING. This migration was not hand-authored: the development
container's start script ran makemigrations on boot and wrote it, then applied
it. That call has since been removed, but the migration is kept because the
change it makes is the correct one and it is already applied in development.

What it does:

    ALTER TABLE app_sitesettings
        ALTER COLUMN email_password TYPE bytea USING email_password::bytea;

Why the column is wrong in the first place: the model declares

    email_password = encrypt(models.CharField(...))

exactly as webhook_secret does beside it, and webhook_secret is bytea. Migration
0023 was written to convert email_password the same way — add an encrypted
column, copy the value across, drop the plaintext one, rename — and it is
recorded as applied, yet the column was still `character varying`. Whatever
happened there, the model and the database disagreed, and the autodetector had
been asking for this AlterField on every run.

The risk, and the check to run first: `varchar::bytea` reinterprets the bytes
of whatever text is in the column. Where the column is already bytea this is a
no-op. Where it holds a real, populated SMTP password the cast may not round
trip through django-cryptography's decryption. Before deploying, check:

    SELECT data_type FROM information_schema.columns
     WHERE table_name = 'app_sitesettings' AND column_name = 'email_password';

    SELECT id, email_password IS NULL AS is_null, length(email_password::text)
      FROM app_sitesettings;

If it is already bytea, this migration is inert. If it is varchar and every row
is null or empty — which was the case in development — it is equally safe. If it
is varchar and holds a value, re-enter the SMTP password in the UI after
migrating and confirm a test email sends, or convert the value deliberately
rather than letting this cast do it.
"""

import django_cryptography.fields
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("app", "0026_purge_schedules"),
    ]

    operations = [
        migrations.AlterField(
            model_name="sitesettings",
            name="email_password",
            field=django_cryptography.fields.encrypt(
                models.CharField(blank=True, max_length=255, null=True)
            ),
        ),
    ]
