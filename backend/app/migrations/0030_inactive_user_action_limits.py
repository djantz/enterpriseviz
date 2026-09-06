"""
Move the inactive-user safety limits onto each portal's tool settings.

They were INACTIVE_USER_MAX_ACTIONS and INACTIVE_USER_MAX_ACTION_FRACTION,
whose defaults (25, and 0.25 as a percentage) are the column defaults here.
The settings are gone rather than carried across: these columns are the only
source, and the limit is now visible on the page that sets the action it
bounds.
"""

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("app", "0029_sitesettings_retention"),
    ]

    operations = [
        migrations.AddField(
            model_name="portaltoolsettings",
            name="tool_inactive_user_max_actions",
            field=models.PositiveIntegerField(
                default=25,
                help_text="Most accounts one run may disable, delete or demote. 0 for no limit.",
                verbose_name="Maximum accounts per run",
            ),
        ),
        migrations.AddField(
            model_name="portaltoolsettings",
            name="tool_inactive_user_max_percent",
            field=models.PositiveIntegerField(
                default=25,
                help_text="Same limit as a percentage of the organization's accounts. 0 for no limit.",
                validators=[
                    django.core.validators.MinValueValidator(0),
                    django.core.validators.MaxValueValidator(100),
                ],
                verbose_name="Maximum share of the organization per run",
            ),
        ),
    ]
