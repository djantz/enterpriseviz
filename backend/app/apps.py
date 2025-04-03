# Licensed under GPLv3 - See LICENSE file for details.
from __future__ import unicode_literals

from django.apps import AppConfig


class AppConfig(AppConfig):
    name = "app"

    def ready(self):
        pass
