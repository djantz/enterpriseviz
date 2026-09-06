# Licensed under GPLv3 - See LICENSE file for details.
from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)


class AppConfig(AppConfig):
    name = "app"

    def ready(self):
        import app.signals
        from app.jobs import autodiscover_tasks

        discovered = autodiscover_tasks()
        logger.debug(f"Registered {len(discovered)} background tasks.")
