import django_cryptography.fields
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("app", "0019_layer_layer_used_by_apps_layer_layer_used_by_count_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="portal",
            name="auth_method",
            field=models.CharField(
                choices=[
                    ("prompt", "Prompt for credentials"),
                    ("password", "Stored username and password"),
                    ("oauth", "OAuth 2.0 (ArcGIS sign in)"),
                ],
                default="prompt",
                max_length=16,
                verbose_name="Authentication method",
            ),
        ),
        migrations.AddField(
            model_name="portal",
            name="oauth_client_id",
            field=models.CharField(blank=True, max_length=128, verbose_name="Client ID"),
        ),
        migrations.AddField(
            model_name="portal",
            name="oauth_client_secret",
            field=django_cryptography.fields.encrypt(
                models.TextField(blank=True, verbose_name="Client Secret")
            ),
        ),
        migrations.AddField(
            model_name="portal",
            name="oauth_refresh_token",
            field=django_cryptography.fields.encrypt(models.TextField(blank=True)),
        ),
        migrations.AddField(
            model_name="portal",
            name="oauth_refresh_expiration",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
