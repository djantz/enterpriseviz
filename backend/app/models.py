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
from __future__ import unicode_literals

import json

from django import forms
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import URLValidator
from django.db import models
from django_celery_beat.models import PeriodicTask
from django_celery_results.models import TaskResult
from django_cryptography.fields import encrypt
from django.contrib.postgres.fields import ArrayField



class Portal(models.Model):
    """
    Stores an ArcGIS portal entry, either Portal or ArcGIS Online.
    """
    alias = models.CharField(verbose_name="Alias", primary_key=True, unique=True, max_length=20)
    url = models.TextField(verbose_name="URL", blank=False, null=False)
    store_password = models.BooleanField(default=False)
    types = (
        ("agol", "ArcGIS Online"),
        ("portal", "Enterprise Portal"),
    )
    portal_type = models.CharField(max_length=32, choices=types, null=True)
    username = models.TextField(blank=True, null=False)
    password = encrypt(models.TextField(blank=True, null=False))
    token = encrypt(models.TextField(blank=True))
    token_expiration = models.DateTimeField(blank=True, null=True)
    webmap_updated = models.DateTimeField(blank=True, null=True)
    service_updated = models.DateTimeField(blank=True, null=True)
    webapp_updated = models.DateTimeField(blank=True, null=True)
    user_updated = models.DateTimeField(blank=True, null=True)
    task = models.OneToOneField(
        PeriodicTask, null=True, blank=True, on_delete=models.SET_NULL
    )
    org_id = models.CharField(verbose_name="Org Id", max_length=50, blank=True, null=True)

    def __str__(self):
        return self.alias


class User(models.Model):
    """
    Stores a user from an ArcGIS portal or ArcGIS Online.
    """
    portal_instance = models.ForeignKey(Portal, on_delete=models.CASCADE)
    user_username = models.CharField(verbose_name="Username", max_length=50)
    user_first_name = models.CharField(verbose_name="First Name", blank=False, max_length=50)
    user_last_name = models.CharField(verbose_name="Last Name", blank=False, max_length=50)
    user_email = models.CharField(verbose_name="Email", blank=False, max_length=50)
    user_created = models.DateTimeField(verbose_name="Created", blank=True)
    user_last_login = models.DateTimeField(verbose_name="Last Login", blank=True, null=True)
    user_role = models.CharField(verbose_name="Role", blank=False, max_length=50)
    user_level = models.CharField(verbose_name="Level", blank=False, max_length=50)
    user_disabled = models.BooleanField(blank=False)
    user_provider = models.CharField(verbose_name="Provider", blank=False, max_length=10)
    types = (
        ("desktopAdvN", "Advanced"),
        ("desktopBasicN", "Basic"),
        ("desktopStdN", "Standard"),
    )
    user_pro_license = models.CharField(verbose_name="Pro License", max_length=32, choices=types, blank=True, null=True)
    user_pro_last = models.DateField(verbose_name="Pro Login", blank=True, null=True)
    user_items = models.IntegerField(verbose_name="Items", blank=True, null=True)
    updated_date = models.DateTimeField(blank=True, null=True)

    def __str__(self):
        return self.user_username


class Webmap(models.Model):
    """
    Stores an ArcGIS webmap entry.
    """
    portal_instance = models.ForeignKey(Portal, on_delete=models.CASCADE)
    webmap_id = models.CharField(verbose_name="Webmap ID", max_length=50)
    webmap_title = models.TextField(verbose_name="Title", blank=False, null=True)
    webmap_url = models.TextField(verbose_name="URL", blank=False, null=True)
    webmap_owner = models.ForeignKey(User, verbose_name="Owner", null=True, on_delete=models.CASCADE)
    webmap_created = models.DateTimeField(verbose_name="Created", blank=True)
    webmap_modified = models.DateTimeField(verbose_name="Modified", blank=True)
    webmap_access = models.CharField(verbose_name="Access", blank=True, max_length=200)
    webmap_extent = models.CharField(verbose_name="Extent", blank=True, max_length=100)
    webmap_description = models.TextField(verbose_name="Description", blank=True, null=True)
    webmap_snippet = models.TextField(verbose_name="Snippet", blank=True, null=True)
    webmap_views = models.IntegerField(verbose_name="Views", blank=True, null=True)
    webmap_layers = models.JSONField(verbose_name="Layers", default=dict)
    webmap_services = models.TextField(verbose_name="Services", blank=True, null=True)
    webmap_dependency = models.JSONField(verbose_name="Dependency", default=list, blank=True, null=True)
    webmap_usage = models.JSONField(verbose_name="Usage", blank=True, null=True)
    updated_date = models.DateTimeField(blank=True, null=True)
    webmap_last_viewed = models.DateTimeField(verbose_name="Last Viewed", blank=True, null=True)

    class Meta:
        ordering = ["webmap_title"]

    def __str__(self):
        return "%s" % self.webmap_title

    def webmap_usage_values(self):
        if self.webmap_usage:
            return list(self.webmap_usage.values())
        else:
            return []


class Service(models.Model):
    """
    Stores an ArcGIS service entry.
    """
    portal_instance = models.ForeignKey(Portal, on_delete=models.CASCADE)
    service_name = models.TextField(verbose_name="Name", blank=True, null=True)
    service_url = ArrayField(
        models.URLField(max_length=1024),
        blank=True,
        null=True,
        help_text="List of URLs for this service (e.g., MapServer, FeatureServer)."
    )
    service_layers = models.JSONField(verbose_name="Layers", default=dict)
    service_mxd_server = models.TextField(verbose_name="Publish Server", blank=True, null=True)
    service_mxd = models.TextField(verbose_name="Publish Map", blank=True, null=True)
    service_type = models.TextField(verbose_name="Type", blank=False, null=False)
    service_owner = models.ForeignKey(User, verbose_name="Owner", null=True, on_delete=models.CASCADE)
    service_access = models.TextField(verbose_name="Access", blank=True, null=True)
    service_description = models.TextField(verbose_name="Description", blank=True, null=True)
    service_snippet = models.TextField(verbose_name="Snippet", blank=True, null=True)
    service_usage = models.JSONField(verbose_name="Usage", blank=True, null=True)
    service_usage_trend = models.IntegerField(verbose_name="Trend", blank=True, null=True)
    service_view = models.ForeignKey("Service", verbose_name="View", blank=True, null=True,
                                     on_delete=models.CASCADE)  # TODO make constraint to same portal
    portal_id = models.JSONField(verbose_name="Portal Id", default=dict)
    service_last_viewed = models.DateTimeField(verbose_name="Last Viewed", blank=True, null=True)
    updated_date = models.DateTimeField(blank=True, null=True)
    apps = models.ManyToManyField(
        "App",
        through="App_Service"
    )
    maps = models.ManyToManyField(
        Webmap,
        through="Map_Service"
    )

    def __str__(self):
        return "%s" % self.service_name

    def service_url_as_list(self):
        return self.service_url

    def service_owner_as_list(self):
        return self.service_owner.split(",")

    def service_usage_as_list(self):
        return self.service_usage.split(",")


class Layer(models.Model):
    """
    Stores an ArcGIS layer entry.
    """
    portal_instance = models.ForeignKey(Portal, on_delete=models.CASCADE)
    layer_server = models.TextField(verbose_name="Server", blank=False, null=True)
    layer_version = models.TextField(verbose_name="Version", blank=False, null=True)
    layer_database = models.TextField(verbose_name="Database", blank=False, null=True)
    layer_name = models.TextField(verbose_name="Name", blank=False, null=True)
    updated_date = models.DateTimeField(blank=True, null=True)
    layer_last_viewed = models.DateTimeField(verbose_name="Last Viewed", blank=True, null=True)
    services = models.ManyToManyField(Service, through="Layer_Service")

    def __str__(self):
        return '%s.%s.%s.%s' % (self.layer_name, self.layer_server, self.layer_database, self.layer_version)


class PortalCreateForm(forms.ModelForm):
    class Meta:
        model = Portal
        fields = ("alias", "url", "portal_type", "store_password", "username", "password")
        widgets = {"password": forms.PasswordInput(render_value=False), }

    def clean_url(self):
        """Ensure the URL is valid."""
        url = self.cleaned_data.get("url")
        validator = URLValidator()
        try:
            validator(url)
        except ValidationError:
            raise ValidationError("Enter a valid URL.")
        return url

    def clean(self):
        """Ensure store_password requires username and password."""
        cleaned_data = super(PortalCreateForm, self).clean()
        store_password = cleaned_data.get("store_password")
        username = cleaned_data.get("username")
        password = cleaned_data.get("password")
        if store_password:
            if not username:
                self.add_error("username", "Username is required when storing credentials.")
            if not password:
                self.add_error("password", "Password is required when storing credentials.")
        return cleaned_data

    def save(self, commit=True):
        user = super().save(commit=False)
        new_password = self.cleaned_data.get("password")
        if new_password:
            user.password = new_password
        if commit:
            user.save()
        return user

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["password"].required = False  # Make password optional
        self.fields["password"].widget.attrs["placeholder"] = "Leave blank to keep current password"


class PortalScheduleForm(forms.ModelForm):
    class Meta:
        model = Portal
        fields = ("alias", "task")

    def clean(self):
        cleaned_data = super(PortalScheduleForm, self).clean()

        return cleaned_data


class App(models.Model):
    """
    Stores an ArcGIS application entry.
    """
    portal_instance = models.ForeignKey(Portal, on_delete=models.CASCADE)
    app_id = models.CharField(verbose_name="App Id", max_length=50)
    # webmap_id = models.ForeignKey(Webmap, on_delete=models.CASCADE)
    app_title = models.TextField(verbose_name="Title", blank=False, null=True)
    app_url = models.TextField(verbose_name="URL", blank=False, null=True)
    types = (
        ("Web Mapping Application", "Web Mapping Application"),
        ("Story Map", "Story Map"),
        ("Experience Builder", "Experience Builder"),
        ("Dashboard", "Dashboard"),
        ("Web AppBuilder Apps", "Web AppBuilder Apps"),
        ("Form", "Form"),
    )
    app_type = models.CharField(verbose_name="Type", blank=True, null=True, choices=types)
    app_owner = models.ForeignKey(User, verbose_name="Owner", null=True, on_delete=models.CASCADE)
    app_created = models.DateTimeField(verbose_name="Created", blank=True)
    app_modified = models.DateTimeField(verbose_name="Modified", blank=True)
    app_access = models.CharField(verbose_name="Access", blank=True, max_length=1000)
    app_extent = models.CharField(verbose_name="Extent", blank=True, max_length=1000)
    app_description = models.TextField(verbose_name="Description", blank=True, null=True)
    app_snippet = models.TextField(verbose_name="Snippet", blank=True, null=True)
    app_views = models.IntegerField(verbose_name="Views", blank=True, null=True)
    app_dependent = models.JSONField(default=dict, null=True, blank=True)
    app_usage = models.JSONField(verbose_name="Usage", blank=True, null=True)
    updated_date = models.DateTimeField(blank=True, null=True)
    app_last_viewed = models.DateTimeField(verbose_name="Last Viewed", blank=True, null=True)
    maps = models.ManyToManyField(
        Webmap,
        through="App_Map"
    )
    services = models.ManyToManyField(
        Service,
        through="App_Service"
    )

    def __str__(self):
        return self.app_title

    def app_usage_values(self):
        if self.app_usage:
            return list(self.app_usage.values())
        else:
            return []


class Map_Layer(models.Model):
    """
    Stores the relationship between :model:`Webmap` and :model:`Layer`.
    """
    portal_instance = models.ForeignKey(Portal, on_delete=models.CASCADE)
    webmap_id = models.ForeignKey(Webmap, on_delete=models.CASCADE)
    layer_id = models.ForeignKey(Layer, on_delete=models.CASCADE)
    updated_date = models.DateTimeField(blank=True, null=True)


class Map_Service(models.Model):
    """
    Stores the relationship between :model:`Webmap` and :model:`Service`.
    """
    portal_instance = models.ForeignKey(Portal, on_delete=models.CASCADE)
    webmap_id = models.ForeignKey(Webmap, on_delete=models.CASCADE)
    service_id = models.ForeignKey(Service, on_delete=models.CASCADE)
    updated_date = models.DateTimeField(blank=True, null=True)

    def __str__(self):
        return f"{self.webmap_id.webmap_title} - {self.service_id.service_name}"


class Layer_Service(models.Model):
    """
    Stores the relationship between :model:`Layer` and :model:`Service`.
    """
    portal_instance = models.ForeignKey(Portal, on_delete=models.CASCADE)
    layer_id = models.ForeignKey(Layer, on_delete=models.CASCADE)
    service_id = models.ForeignKey(Service, on_delete=models.CASCADE)
    updated_date = models.DateTimeField(blank=True, null=True)

    def __str__(self):
        return f"{self.layer_id.layer_name} - {self.service_id.service_name}"


class App_Map(models.Model):
    """
    Stores the relationship between :model:`app.App` and :model:`app.Webmap`.
    """
    portal_instance = models.ForeignKey(Portal, on_delete=models.CASCADE)
    app_id = models.ForeignKey(App, on_delete=models.CASCADE)
    webmap_id = models.ForeignKey(Webmap, on_delete=models.CASCADE)
    types = (
        ("widget", "widget"),
        ("search", "search"),
        ("filter", "filter"),
        ("map", "map"),
        ("other", "other")
    )
    rel_type = models.CharField(choices=types, null=True)
    updated_date = models.DateTimeField(blank=True, null=True)

    def __str__(self):
        return f"{self.app_id.app_title} - {self.webmap_id.webmap_title}"


class App_Service(models.Model):
    """
    Stores the relationship between :model:`app.App` and a :model:`app.Layer`
    """
    portal_instance = models.ForeignKey(Portal, on_delete=models.CASCADE)
    app_id = models.ForeignKey(App, on_delete=models.CASCADE)
    service_id = models.ForeignKey(Service, on_delete=models.CASCADE)
    types = (
        ("widget", "widget"),
        ("search", "search"),
        ("filter", "filter"),
        ("other", "other")
    )
    rel_type = models.CharField(choices=types, null=True)
    updated_date = models.DateTimeField(blank=True, null=True)

    def __str__(self):
        return f"{self.app_id.app_title} - {self.service_id.service_name}"


class UserProfile(models.Model):
    user = models.OneToOneField(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="profile")
    types = (
        ("light", "light"),
        ("dark", "dark")
    )
    mode = models.CharField(choices=types, max_length=10, default="light")
    service_usage = models.BooleanField(default=True)


def result_as_dict(self):
    """Convert the result field from a JSON string to a dictionary."""
    try:
        return json.loads(self.result) if self.result else {}
    except (ValueError, TypeError):
        return {}


TaskResult.result_as_dict = result_as_dict


class SiteSettings(models.Model):
    admin_email = models.EmailField(null=True, blank=True)
    email_host = models.CharField(max_length=255, null=True, blank=True)
    email_port = models.PositiveIntegerField(default=25, null=True)
    types = (
        ('plain_text', 'Plain Text'),
        ('starttls', 'StartTLS'),
        ('ssl', 'SSL')
    )
    email_encryption = models.CharField(max_length=255, choices=types, default="plain_text")
    email_username = models.CharField(max_length=255, null=True, blank=True)
    email_password = models.CharField(max_length=255, null=True, blank=True)
    from_email = models.EmailField(null=True, blank=True)
    reply_to = models.EmailField(null=True, blank=True)
    LOG_LEVEL_CHOICES = [
        ('INFO', 'Info'),
        ('WARNING', 'Warning'),
        ('ERROR', 'Error'),
        ('DEBUG', 'Debug'),
        ('CRITICAL', 'Critical'),
    ]
    logging_level = models.CharField(max_length=255, choices=LOG_LEVEL_CHOICES, default='warning', null=False, blank=False)

    def has_module_permission(self, request):
        return request.user.is_superuser

    def has_view_permission(self, request, obj=None):
        return request.user.is_superuser

    def has_change_permission(self, request, obj=None):
        return request.user.is_superuser

    def has_add_permission(self, request):
        return request.user.is_superuser

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser


class LogEntry(models.Model):
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True)
    request_id = models.UUIDField(
        null=True, blank=True, db_index=True,
        help_text="Django request ID or ID passed from Celery task caller",
        verbose_name="Request ID"
    )
    LOG_LEVEL_CHOICES = [
        ('DEBUG', 'Debug'),
        ('INFO', 'Info'),
        ('WARNING', 'Warning'),
        ('ERROR', 'Error'),
        ('CRITICAL', 'Critical'),
    ]
    level = models.CharField(max_length=10, choices=LOG_LEVEL_CHOICES, db_index=True)
    request_username = models.CharField(max_length=150, null=True, blank=True, help_text="Username", verbose_name="Username")
    client_ip = models.GenericIPAddressField(null=True, blank=True, verbose_name="Client IP")
    request_path = models.CharField(max_length=1024, null=True, blank=True)
    request_method = models.CharField(max_length=10, null=True, blank=True)
    request_duration = models.FloatField(
        null=True, blank=True,
        help_text="Time from request/task start to log in ms"
    )
    logger_name = models.CharField(max_length=255, db_index=True)
    message = models.TextField()
    pathname = models.CharField(max_length=512, blank=True, null=True)
    funcName = models.CharField(max_length=100, blank=True, null=True, verbose_name="Function Name")
    lineno = models.PositiveIntegerField(blank=True, null=True, verbose_name="Line Number")
    traceback = models.TextField(blank=True, null=True)

    def __str__(self):
        return f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] [{self.level}] {self.message[:50]}"

    @property
    def formatted_timestamp(self):
        if self.timestamp:
            return self.timestamp.strftime('%Y-%m-%d %H:%M:%S')
        return "-"

    class Meta:
        verbose_name = "Log Entry"
        verbose_name_plural = "Log Entries"
        ordering = ['-timestamp']
