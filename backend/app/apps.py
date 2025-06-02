# Licensed under GPLv3 - See LICENSE file for details.
from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)


class AppConfig(AppConfig):
    name = "app"

    def ready(self):
        logger.info("AppConfig ready: Importing signals...")
        try:
            import app.signals  # This will execute the @worker_process_init.connect
            logger.info("Successfully imported app.signals.")
        except ImportError:
            logger.warning("app.signals module not found.")
        except Exception as e:
            logger.error(f"Error importing app.signals: {e}", exc_info=True)
