# ----------------------------------------------------------------------
# Enterpriseviz
# Copyright (C) 2025 David C Jantz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# ----------------------------------------------------------------------
from app.views import *
from django.urls import path

app_name = "enterpriseviz"

urlpatterns = [
    path(r"/table/<name>/", Table.as_view(), name="table"),
    path(r"/table/<instance>/<name>/", Table.as_view(), name="table"),
    path(r"/portal/add/", portal_create_view, name="add_portal"),
    path(r"/portal/delete/<instance>/", delete_portal_view, name="delete_portal"),
    path(r"/portal/update/<instance>/", update_portal_view, name="update_portal"),
    path(r"/portal/refresh/", refresh_portal_view, name="refresh_portal"),
    path(r"/portal/refresh/progress/<instance>/<task_id>/", progress_view, name="refresh_progress"),
    path(r"/login/", login_view, name="login"),
    path(r"/logout/", logout_view, name="logout"),
    path(r"/layer/<name>/", portal_layer_view, name="layer"),
    path(r"/portal/<instance>/service/<path:url>/", portal_service_view, name="service"),
    path(r"/portal/<instance>/map/<id>", portal_map_view, name="map"),
    path(r"/portal/schedule/<instance>/", schedule_task_view, name="schedule"),
    path(r"/portal/<instance>/duplicates/", duplicates_view, name="duplicate"),
    path(r"/portal/<instance>/layerid/", layerid_view, name="layerid"),
    path(r"/portal/<instance>/metadata/", metadata_view, name="metadata"),
    path(r"/portal/<instance>/", index_view, name="viz"),
    path(r"/theme", mode_toggle_view, name="mode_toggle"),
    path(r"/usage", usage_toggle_view, name="usage_toggle"),
    path(r"/webhook", webhook_view, name="webhook"),
    path(r"/settings/email/", email_settings, name="email_settings"),
    path(r"/settings/logs/", log_settings_view, name='log_settings'),
    path(r"/logs/", logs_page, name="log"),
    path(r"/logs/table/", LogTable.as_view(), name="log_table"),
    path(r"/notify/", notify_view, name="notify"),

    # The home page
    path(r"/", index_view, name="index"),
]
