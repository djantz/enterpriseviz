"""
Say that a custom role id works, now that it does.

config.customArcGIS.user_role compared only the portal's ``role``, which is
"org_user" for anyone holding a custom role — so a deployment that took the old
help text at its word and entered a custom role id refused every sign-in with
nothing to say why. The gate now matches ``roleId`` as well; this is the wording
catching up.
"""

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("app", "0033_layer_service_unique_layer_id"),
    ]

    operations = [
        migrations.AlterField(
            model_name="sitesettings",
            name="arcgis_user_role",
            field=models.CharField(
                blank=True,
                default="org_admin",
                help_text="Portal role a user must hold to sign in: a built-in role such as org_admin or org_publisher, or the ID of a custom role. Blank refuses every ArcGIS sign-in.",
                max_length=128,
                verbose_name="Required role",
            ),
        ),
    ]
