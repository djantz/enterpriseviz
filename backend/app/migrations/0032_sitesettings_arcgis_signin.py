"""
Move ArcGIS sign-in configuration out of the environment and into SiteSettings.

These four are carried across rather than left at their defaults, which the
retention windows and the action limits were not. The difference is that those
had a default worth keeping — 90 days is 90 days on any deployment — while an
OAuth client id, secret and organization URL are specific to one portal and
have no default at all. Dropping them would sign every ArcGIS user out of every
existing installation until an administrator re-entered them, and the only way
back in would be a local Django account.

Read from os.environ rather than django.conf.settings: the settings this
replaces are already gone from base.py, but django-environ's read_env() puts
.env values into the process environment, which is where a deployment that set
them can still be found.
"""

import os

import django_cryptography.fields
from django.db import migrations, models


def _env(name):
    """An environment value with the quoting the .env files allow stripped."""
    return (os.environ.get(name, "") or "").strip().strip("'\" ")


def seed_from_environment(apps, schema_editor):
    SiteSettings = apps.get_model("app", "SiteSettings")

    org_url = _env("SOCIAL_AUTH_ARCGIS_URL").rstrip("/")
    client_id = _env("SOCIAL_AUTH_ARCGIS_KEY")
    client_secret = _env("SOCIAL_AUTH_ARCGIS_SECRET")
    # Blank refuses every ArcGIS sign-in, so an unset variable keeps the
    # column default rather than writing an empty string over it.
    user_role = _env("ARCGIS_USER_ROLE")

    if not any([org_url, client_id, client_secret, user_role]):
        return

    row, _ = SiteSettings.objects.get_or_create(pk=1)
    row.arcgis_org_url = org_url
    row.arcgis_client_id = client_id
    row.arcgis_client_secret = client_secret
    if user_role:
        row.arcgis_user_role = user_role
    row.save(update_fields=["arcgis_org_url", "arcgis_client_id",
                            "arcgis_client_secret", "arcgis_user_role"])


def unseed(apps, schema_editor):
    """Nothing to undo: removing the columns takes the values with them."""


class Migration(migrations.Migration):
    dependencies = [
        ("app", "0031_sitesettings_replacement_backup_retention"),
    ]

    operations = [
        migrations.AddField(
            model_name="sitesettings",
            name="arcgis_client_id",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Client ID of the OAuth application registered in the portal.",
                max_length=128,
                verbose_name="App ID",
            ),
        ),
        migrations.AddField(
            model_name="sitesettings",
            name="arcgis_client_secret",
            field=django_cryptography.fields.encrypt(
                models.CharField(
                    blank=True,
                    default="",
                    help_text="Client secret of that OAuth application.",
                    max_length=255,
                    verbose_name="App Secret",
                )
            ),
        ),
        migrations.AddField(
            model_name="sitesettings",
            name="arcgis_org_url",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Enterprise portal or ArcGIS Online organization users sign in against, e.g. https://example.maps.arcgis.com.",
                max_length=512,
                verbose_name="Organization URL",
            ),
        ),
        migrations.AddField(
            model_name="sitesettings",
            name="arcgis_user_role",
            field=models.CharField(
                blank=True,
                default="org_admin",
                help_text="Portal role a user must hold to sign in, e.g. org_admin, org_publisher, or a custom role ID. Blank refuses every ArcGIS sign-in.",
                max_length=128,
                verbose_name="Required role",
            ),
        ),
        migrations.RunPython(seed_from_environment, unseed),
    ]
