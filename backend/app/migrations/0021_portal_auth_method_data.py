from django.db import migrations


def set_auth_method(apps, schema_editor):
    """Derive auth_method from the legacy store_password flag."""
    Portal = apps.get_model("app", "Portal")
    Portal.objects.filter(store_password=True).update(auth_method="password")
    Portal.objects.filter(store_password=False).update(auth_method="prompt")


def unset_auth_method(apps, schema_editor):
    """Restore store_password from auth_method."""
    Portal = apps.get_model("app", "Portal")
    Portal.objects.filter(auth_method="password").update(store_password=True)
    Portal.objects.exclude(auth_method="password").update(store_password=False)


class Migration(migrations.Migration):
    dependencies = [
        ("app", "0020_portal_oauth_fields"),
    ]

    operations = [
        migrations.RunPython(set_auth_method, unset_auth_method),
    ]
