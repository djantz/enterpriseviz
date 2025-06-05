# Licensed under GPLv3 - See LICENSE file for details.
from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver
import logging as py_logging # Alias to avoid conflict if you have a 'logging' variable
from celery.signals import worker_process_init

from .models import UserProfile
from .utils import apply_global_log_level


# Signal to create/update user profile when a User instance is created/updated
@receiver(post_save, sender=User)
def create_or_update_user_profile(sender, instance, created, **kwargs):
    user_profile, created = UserProfile.objects.get_or_create(user=instance)
    user_profile.save()


signal_handler_logger = py_logging.getLogger(__name__)

@worker_process_init.connect(weak=False)
def setup_dynamic_logging_on_worker_process_start(sender=None, **kwargs):
    worker_pid = getattr(sender, 'pid', 'N/A')
    signal_handler_logger.debug(
        f"Celery worker_process_init signal received (PID: {worker_pid}). Applying dynamic log levels from SiteSettings."
    )
    try:
        apply_global_log_level(logger_name='app.tasks')
        signal_handler_logger.debug(f"Dynamic log levels applied successfully on worker process start (PID: {worker_pid}).")
    except Exception as e:
        signal_handler_logger.error(
            f"Failed to apply dynamic log levels on worker process start (PID: {worker_pid}): {e}",
            exc_info=True
        )
