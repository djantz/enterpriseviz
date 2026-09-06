"""
Add the Job table: the queue, progress and run history for background work.

Replaces the celery broker, the celery result backend and the celery-progress
side channel with one row per job, so background work needs no service beyond
PostgreSQL. See app/jobs.py and the run_worker management command.

The autodetector also wanted an AlterField on SiteSettings.email_password,
which is unrelated to this change and is kept out of it. It lives in 0027 —
read that file's notes before deploying, as it rewrites an encrypted column.
"""

import django.utils.timezone
import uuid
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("app", "0024_layer_layer_used_by_apps_layer_layer_used_by_count_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="Job",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4,
                        editable=False,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                ("name", models.CharField(max_length=150)),
                ("func", models.CharField(max_length=255)),
                ("args", models.JSONField(blank=True, default=list)),
                ("kwargs", models.JSONField(blank=True, default=dict)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("QUEUED", "Queued"),
                            ("RUNNING", "Running"),
                            ("SUCCESS", "Success"),
                            ("WARNING", "Completed with errors"),
                            ("FAILURE", "Failed"),
                            ("CANCELED", "Canceled"),
                        ],
                        db_index=True,
                        default="QUEUED",
                        max_length=10,
                    ),
                ),
                ("progress_current", models.PositiveIntegerField(default=0)),
                ("progress_total", models.PositiveIntegerField(default=0)),
                ("progress_description", models.TextField(blank=True)),
                ("result", models.JSONField(blank=True, null=True)),
                ("error", models.TextField(blank=True)),
                ("traceback", models.TextField(blank=True)),
                (
                    "portal_alias",
                    models.CharField(blank=True, db_index=True, max_length=20),
                ),
                ("periodic_task_name", models.CharField(blank=True, max_length=200)),
                ("queued_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                ("cancel_requested", models.BooleanField(default=False)),
                ("deadline_at", models.DateTimeField(blank=True, null=True)),
                ("worker_id", models.CharField(blank=True, max_length=100)),
                ("heartbeat_at", models.DateTimeField(blank=True, null=True)),
                ("request_id", models.CharField(blank=True, max_length=64)),
                ("username", models.CharField(blank=True, max_length=150)),
                ("client_ip", models.CharField(blank=True, max_length=45)),
                ("request_path", models.TextField(blank=True)),
            ],
            options={
                "verbose_name": "Background Job",
                "verbose_name_plural": "Background Jobs",
                "ordering": ["-queued_at"],
                "indexes": [
                    models.Index(fields=["status", "queued_at"], name="job_claim_idx"),
                    models.Index(
                        fields=["portal_alias", "-finished_at"], name="job_history_idx"
                    ),
                    models.Index(
                        fields=["status", "heartbeat_at"], name="job_heartbeat_idx"
                    ),
                ],
            },
        ),
    ]
