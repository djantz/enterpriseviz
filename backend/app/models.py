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

import uuid
from datetime import timedelta

from django import forms
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import (MaxValueValidator, MinValueValidator,
                                    URLValidator, validate_email)
from django.db import models
from django.utils import timezone
from django_celery_beat.models import PeriodicTask
from django_cryptography.fields import encrypt
from django.contrib.postgres.fields import ArrayField



class Portal(models.Model):
    """
    Stores an ArcGIS portal entry, either Portal or ArcGIS Online.
    """
    alias = models.CharField(verbose_name="Alias", primary_key=True, unique=True, max_length=20)
    url = models.TextField(verbose_name="URL", blank=False, null=False)
    store_password = models.BooleanField(default=False)
    auth_methods = (
        ("prompt", "Prompt for credentials"),
        ("password", "Stored username and password"),
        ("oauth", "OAuth 2.0 (ArcGIS sign in)"),
    )
    auth_method = models.CharField(
        verbose_name="Authentication method", max_length=16, choices=auth_methods, default="prompt"
    )
    types = (
        ("agol", "ArcGIS Online"),
        ("portal", "Enterprise Portal"),
    )
    portal_type = models.CharField(max_length=32, choices=types, null=True)
    username = models.TextField(blank=True, null=False)
    password = encrypt(models.TextField(blank=True, null=False))
    token = encrypt(models.TextField(blank=True))
    token_expiration = models.DateTimeField(blank=True, null=True)
    oauth_client_id = models.CharField(verbose_name="Client ID", max_length=128, blank=True)
    oauth_client_secret = encrypt(models.TextField(verbose_name="Client Secret", blank=True))
    oauth_refresh_token = encrypt(models.TextField(blank=True))
    oauth_refresh_expiration = models.DateTimeField(blank=True, null=True)
    webmap_updated = models.DateTimeField(blank=True, null=True)
    service_updated = models.DateTimeField(blank=True, null=True)
    webapp_updated = models.DateTimeField(blank=True, null=True)
    user_updated = models.DateTimeField(blank=True, null=True)
    task = models.OneToOneField(
        PeriodicTask, null=True, blank=True, on_delete=models.SET_NULL
    )
    org_id = models.CharField(verbose_name="Org Id", max_length=50, blank=True, null=True)
    admin_emails = models.TextField(
        blank=True,
        help_text="Comma-separated list of admin email addresses for tool notifications"
    )
    enable_admin_notifications = models.BooleanField(
        default=True,
        help_text="Enable/disable admin notifications for this portal"
    )

    def __str__(self):
        return self.alias

    def save(self, *args, **kwargs):
        # store_password is retained for one release; keep it derived from auth_method.
        update_fields = kwargs.get("update_fields")
        if update_fields is None or "auth_method" in update_fields:
            self.store_password = self.auth_method == "password"
            if update_fields is not None and "store_password" not in update_fields:
                kwargs["update_fields"] = list(update_fields) + ["store_password"]
        super().save(*args, **kwargs)

    @property
    def requires_interactive_credentials(self):
        """True when an operator must supply credentials for each connection."""
        return self.auth_method == "prompt"

    @property
    def oauth_is_configured(self):
        """True when this portal has a usable OAuth refresh token."""
        return bool(self.auth_method == "oauth" and self.oauth_client_id and self.oauth_refresh_token)

    @property
    def oauth_needs_reconsent(self):
        """True when the authorization is incomplete, missing, or past its expiration."""
        if self.auth_method != "oauth":
            return False
        if not self.oauth_client_id or not self.oauth_refresh_token:
            return True
        return bool(self.oauth_refresh_expiration and self.oauth_refresh_expiration <= timezone.now())


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
    service_created = models.DateTimeField(verbose_name="Created", blank=True, null=True)
    service_modified = models.DateTimeField(verbose_name="Modified", blank=True, null=True)
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
    layers = models.ManyToManyField(
        'Layer',
        through="Layer_Service"
    )

    def __str__(self):
        return "%s" % self.service_name

    def service_url_as_list(self):
        if not self.service_url:
            return []
        return list(self.service_url)

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
    # Dependency counts, grouped by layer_name.
    # Stored because per-row aggregation over the name joins is too slow to sort on.
    layer_used_by_services = models.IntegerField(default=0, editable=False)
    layer_used_by_maps = models.IntegerField(default=0, editable=False)
    layer_used_by_apps = models.IntegerField(default=0, editable=False)
    layer_used_by_count = models.IntegerField(verbose_name="Used By", default=0, editable=False, db_index=True)
    services = models.ManyToManyField(Service, through="Layer_Service")

    def __str__(self):
        return '%s.%s.%s.%s' % (self.layer_name, self.layer_server, self.layer_database, self.layer_version)


class PortalCreateForm(forms.ModelForm):
    class Meta:
        model = Portal
        fields = ("alias", "url", "portal_type", "auth_method", "username", "password", "oauth_client_id",
                  "oauth_client_secret", "admin_emails", "enable_admin_notifications")
        widgets = {"password": forms.PasswordInput(render_value=False),
                   "oauth_client_secret": forms.PasswordInput(render_value=False), }

    def clean_url(self):
        """Ensure the URL is valid."""
        url = self.cleaned_data.get("url")
        validator = URLValidator()
        try:
            validator(url)
        except ValidationError:
            raise ValidationError("Enter a valid URL.")
        return url

    def clean_admin_emails(self):
        """Validate admin email addresses."""
        admin_emails = self.cleaned_data.get("admin_emails")

        if not admin_emails:
            return admin_emails

        # Split emails by comma and strip whitespace
        email_list = [email.strip() for email in admin_emails.split(',')]

        # Remove empty strings
        email_list = [email for email in email_list if email]

        # Validate each email address
        for email in email_list:
            try:
                validate_email(email)
            except ValidationError:
                raise ValidationError(f"'{email}' is not a valid email address.")

        # Return cleaned emails (comma-separated, no extra whitespace)
        return ', '.join(email_list)

    def clean_password(self):
        """Keep the stored password when the field is submitted blank."""
        password = self.cleaned_data.get("password")
        self._password_submitted = bool(password)
        if not password and self._is_update:
            return self.instance.password
        return password

    def clean_oauth_client_secret(self):
        """Keep the stored client secret when the field is submitted blank."""
        secret = self.cleaned_data.get("oauth_client_secret")
        if not secret and self._is_update:
            return self.instance.oauth_client_secret
        return secret

    def clean(self):
        """Ensure the selected authentication method has the fields it needs."""
        cleaned_data = super(PortalCreateForm, self).clean()
        auth_method = cleaned_data.get("auth_method")
        username = cleaned_data.get("username")
        password = cleaned_data.get("password")
        client_id = cleaned_data.get("oauth_client_id")

        if auth_method == "password":
            if not username:
                self.add_error("username", "Username is required when storing credentials.")
            if not password:
                self.add_error("password", "Password is required when storing credentials.")
        elif auth_method == "oauth":
            if not client_id:
                self.add_error("oauth_client_id", "Client ID is required for OAuth authentication.")
        return cleaned_data

    def stored_credentials_need_check(self):
        """
        True when the stored-credential login should be re-tested before it is saved.

        :return: Whether :func:`utils.try_connection` should be run for this submission
        :rtype: bool
        """
        if self.cleaned_data.get("auth_method") != "password":
            return False
        if not self._is_update:
            return True  # New portal
        return (self._password_submitted
                or self.cleaned_data.get("auth_method") != self._original_auth_method
                or self.cleaned_data.get("url") != self._original_url
                or self.cleaned_data.get("username") != self._original_username)

    def _clear_unused_credentials(self, portal):
        """
        Erase every credential the selected authentication method does not use.

        A secret that nothing reads is pure liability, and this form is the only place an
        operator can remove one short of the Django admin. So rather than clearing what
        the previous method left behind, this states the invariant positively: a portal
        stores credentials for its current method and nothing else. Applied on every save
        it is self-healing, and it does not depend on the browser having disabled the
        inputs that belong to the other method.

        :param portal: The unsaved Portal instance carrying the pending changes
        """
        if portal.auth_method != "password":
            portal.username = ""
            portal.password = ""

        if portal.auth_method != "oauth":
            portal.oauth_client_id = ""
            portal.oauth_client_secret = ""
            portal.oauth_refresh_token = ""
            portal.oauth_refresh_expiration = None

    def _drop_invalidated_credentials(self, portal):
        """
        Clear stored credentials that the pending changes make unusable.

        Tokens are issued by one portal to one application, so changing the URL or the
        client credentials invalidates them. Left in place they are retried and rejected
        with a misleading error instead of prompting to reconnect. Credentials stranded by
        an authentication method change are handled by :meth:`_clear_unused_credentials`.

        :param portal: The unsaved Portal instance carrying the pending changes
        """
        if not self._is_update:
            return  # New portal; there is nothing stored yet.

        portal_changed = portal.url != self._original_url
        method_changed = portal.auth_method != self._original_auth_method
        oauth_app_changed = (portal.oauth_client_id != self._original_oauth_client_id
                             or portal.oauth_client_secret != self._original_oauth_client_secret)

        # The cached access token is only valid for the portal and method that issued it.
        if portal_changed or method_changed:
            portal.token = ""
            portal.token_expiration = None

        # The refresh token belongs to one application on one portal.
        if portal_changed or oauth_app_changed:
            portal.oauth_refresh_token = ""
            portal.oauth_refresh_expiration = None

    def save(self, commit=True):
        user = super().save(commit=False)
        self._drop_invalidated_credentials(user)
        # Last, so it also sweeps up anything the invalidation above chose to keep.
        self._clear_unused_credentials(user)
        if commit:
            user.save()
        return user

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["password"].required = False  # Make password optional
        self.fields["password"].widget.attrs["placeholder"] = "Leave blank to keep current password"
        self.fields["oauth_client_id"].required = False
        self.fields["oauth_client_secret"].required = False
        self.fields["oauth_client_secret"].widget.attrs["placeholder"] = "Leave blank to keep current secret"
        stored = self.instance if self.instance.pk else None
        self._is_update = stored is not None
        self._password_submitted = False  # Set by clean_password()
        self._original_url = stored.url if stored else ""
        self._original_username = stored.username if stored else ""
        self._original_auth_method = stored.auth_method if stored else ""
        self._original_oauth_client_id = stored.oauth_client_id if stored else ""
        self._original_oauth_client_secret = stored.oauth_client_secret if stored else ""
        if self._is_update:
            self.initial["password"] = ""
            self.initial["oauth_client_secret"] = ""


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
    service_layer_id = models.IntegerField(
        null=True,
        blank=True,
        help_text="Specific layer ID from this service used in the webmap"
    )
    webmap_layer_id = models.CharField(max_length=100, null=True, blank=True)
    updated_date = models.DateTimeField(blank=True, null=True)

    def __str__(self):
        return f"{self.webmap_id.webmap_title} - {self.service_id.service_name}"

    class Meta:
        unique_together = ('portal_instance', 'webmap_id', 'service_id', 'service_layer_id')
        indexes = [
            models.Index(fields=['webmap_id', 'webmap_layer_id']),
        ]


class Layer_Service(models.Model):
    """
    Stores the relationship between :model:`Layer` and :model:`Service`.
    """
    portal_instance = models.ForeignKey(Portal, on_delete=models.CASCADE)
    layer_id = models.ForeignKey(Layer, on_delete=models.CASCADE)
    service_id = models.ForeignKey(Service, on_delete=models.CASCADE)
    service_layer_id = models.IntegerField(
        null=True,
        blank=True,
        help_text="The layer ID within the service (e.g., 0, 1, 2 from /MapServer/0)"
    )

    service_layer_name = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="The display name of the layer in the service"
    )
    updated_date = models.DateTimeField(blank=True, null=True)


    class Meta:
        db_table = 'layer_service'
        unique_together = ['portal_instance', 'layer_id', 'service_id', 'service_layer_id']
        indexes = [
            models.Index(fields=['service_id', 'service_layer_id'], name='idx_service_layer')
        ]
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
        ("search", "Search"),
        ("filter", "Filter"),
        ('map', 'Primary Map Reference'),  # Web Mapping Application, Dashboard
        ('datasource', 'Data Source Map'),  # Web AppBuilder, Experience Builder
        ('widget', 'Widget Map'),  # Experience Builder
        ('storymap', 'StoryMap Embed'),  # StoryMap
        ('other', 'Other'),
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
        ('search', 'Search Layer'),  # Web AppBuilder
        ('filter', 'Filter'),  # Web AppBuilder
        ('widget', 'Widget'),  # Web AppBuilder
        ('datasource', 'Data Source'),  # Web AppBuilder, Experience Builder
        ('dataset', 'Dashboard Dataset'),  # Dashboard
        ('arcade', 'Arcade Expression'),  # Dashboard
        ('survey', 'Survey123 Form'),  # Experience Builder
        ('embed', 'StoryMap Embed'),  # StoryMap
        ('other', 'Other'),
    )
    rel_type = models.CharField(choices=types, null=True)
    service_layer_id = models.IntegerField(null=True, blank=True)
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


class SiteSettings(models.Model):
    arcgis_org_url = models.CharField(
        max_length=512, blank=True, default="",
        verbose_name="Organization URL",
        help_text="Enterprise portal or ArcGIS Online organization users sign in against, "
                  "e.g. https://example.maps.arcgis.com."
    )
    arcgis_client_id = models.CharField(
        max_length=128, blank=True, default="",
        verbose_name="App ID",
        help_text="Client ID of the OAuth application registered in the portal."
    )
    arcgis_client_secret = encrypt(models.CharField(
        max_length=255, blank=True, default="",
        verbose_name="App Secret",
        help_text="Client secret of that OAuth application."
    ))
    arcgis_user_role = models.CharField(
        max_length=128, blank=True, default="org_admin",
        verbose_name="Required role",
        help_text="Portal role a user must hold to sign in: a built-in role such as org_admin "
                  "or org_publisher, or the ID of a custom role. Blank refuses every ArcGIS "
                  "sign-in."
    )

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
    email_password = encrypt(models.CharField(max_length=255, null=True, blank=True))
    from_email = models.EmailField(null=True, blank=True)
    reply_to = models.EmailField(null=True, blank=True)
    LOG_LEVEL_CHOICES = [
        ('INFO', 'Info'),
        ('WARNING', 'Warning'),
        ('ERROR', 'Error'),
        ('DEBUG', 'Debug'),
        ('CRITICAL', 'Critical'),
    ]
    logging_level = models.CharField(max_length=255, choices=LOG_LEVEL_CHOICES, default='WARNING', null=False, blank=False)
    webhook_secret = encrypt(models.CharField(max_length=255, null=True, blank=True))
    log_retention_days = models.PositiveIntegerField(
        default=90,
        verbose_name="Log retention (days)",
        help_text="Delete application log entries older than this. 0 keeps them indefinitely."
    )
    job_retention_days = models.PositiveIntegerField(
        default=30,
        verbose_name="Job history retention (days)",
        help_text="Delete finished background jobs older than this. 0 keeps them indefinitely."
    )
    replacement_backup_retention_days = models.PositiveIntegerField(
        default=90,
        verbose_name="Replacement backup retention (days)",
        help_text="Delete Replace Service item backups older than this, after which those "
                  "replacements can no longer be reverted. 0 keeps them indefinitely."
    )

    @classmethod
    def load(cls):
        """
        The settings row, created on first use.

        There is only ever one, at pk=1. Call sites used to be split between
        ``objects.first()`` and ``get_or_create(pk=1)``, which differ on an
        empty table: the first returns None and the caller falls back to
        something, the second decides what the defaults are.

        The existing row is reused whatever its pk. A row deleted and re-added
        in the admin comes back at pk=2, and keying on pk=1 alone would answer
        with a second row full of defaults while the configured one sat unread.
        """
        obj = cls.objects.order_by("pk").first()
        if obj is not None:
            return obj
        obj, _ = cls.objects.get_or_create(pk=1)
        return obj


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


class PortalToolSettings(models.Model):
    """Stores configuration settings for automation tools related to a specific Portal."""
    portal = models.OneToOneField(
        Portal,
        on_delete=models.CASCADE,
        primary_key=True,
        related_name='tool_settings',
        help_text="The Portal these tool settings apply to."
    )

    tool_pro_license_enabled = models.BooleanField(default=False, help_text="Enable ArcGIS Pro license removal tool.")
    TOOL_DURATION_CHOICES = [
        (30, '30 days'), (60, '60 days'), (90, '90 days'), (180, '180 days')
    ]
    tool_pro_duration = models.PositiveIntegerField(
        choices=TOOL_DURATION_CHOICES,
        default=30,
        help_text="Duration of inactivity before Pro license is considered for removal."
    )
    TOOL_WARNING_CHOICES = [
        (3, '3 days'), (5, '5 days'), (7, '7 days'), (14, '14 days')
    ]
    tool_pro_warning = models.PositiveIntegerField(
        choices=TOOL_WARNING_CHOICES,
        default=3,
        help_text="Days before removal to send a warning notification."
    )

    tool_public_unshare_enabled = models.BooleanField(default=False, help_text="Enable public item unsharing tool.")
    TOOL_SCORE_CHOICES = [
        (50, '50%'), (75, '75%'), (90, '90%'), (100, '100%')
    ]
    tool_public_unshare_score = models.PositiveIntegerField(
        choices=TOOL_SCORE_CHOICES,
        default=50,
        help_text="Minimum metadata score required for items to remain public."
    )
    tool_public_unshare_notify_limit = models.PositiveIntegerField(
        default=24,
        help_text="Hours to wait before sending another notification email for public item unsharing (prevents spam)."
    )

    TOOL_PUBLIC_UNSHARE_TRIGGER_CHOICES = [
        ('webhook', 'Webhook'),
        ('daily', 'Daily Schedule')
    ]

    tool_public_unshare_trigger = models.CharField(
        max_length=10,
        choices=TOOL_PUBLIC_UNSHARE_TRIGGER_CHOICES,
        default='daily',
        verbose_name="Public Item Unsharing Trigger",
        help_text="Choose how the unsharing process is triggered."
    )

    tool_inactive_user_enabled = models.BooleanField(default=False, help_text="Enable inactive user management tool.")
    TOOL_USER_DURATION_CHOICES = [
        (30, '30 days'), (60, '60 days'), (90, '90 days'),
        (180, '180 days'), (365, '365 days')
    ]
    tool_inactive_user_duration = models.PositiveIntegerField(
        choices=TOOL_USER_DURATION_CHOICES,
        default=30,
        help_text="Duration of inactivity before user is considered for action."
    )
    TOOL_USER_WARNING_CHOICES = [
        (3, '3 days'), (5, '5 days'), (7, '7 days'),
        (14, '14 days'), (30, '30 days')
    ]
    tool_inactive_user_warning = models.PositiveIntegerField(
        choices=TOOL_USER_WARNING_CHOICES,
        default=3,
        help_text="Days before action to send a warning notification."
    )
    TOOL_USER_ACTION_CHOICES = [
        ('notify', 'Notify Only'),
        ('disable', 'Disable User'),
        ('delete', 'Delete User'),
        ('transfer', 'Transfer User Content')
    ]
    tool_inactive_user_action = models.CharField(
        max_length=10,
        default='disable',
        choices=TOOL_USER_ACTION_CHOICES,
        help_text="Action to take for inactive users."
    )
    tool_inactive_user_max_actions = models.PositiveIntegerField(
        default=25,
        verbose_name="Maximum accounts per run",
        help_text="Most accounts one run may disable, delete or demote. 0 for no limit."
    )
    tool_inactive_user_max_percent = models.PositiveIntegerField(
        default=25,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        verbose_name="Maximum share of the organization per run",
        help_text="Same limit as a percentage of the organization's accounts. 0 for no limit."
    )


    def __str__(self):
        return f"Tool Settings for Portal: {self.portal.alias}"

    class Meta:
        verbose_name = "Portal Tool Settings"
        verbose_name_plural = "Portal Tool Settings"


class WebhookNotificationLog(models.Model):
    """
    Tracks webhook notifications sent to users to implement grace periods
    and prevent notification spam.
    """
    portal = models.ForeignKey(
        Portal,
        on_delete=models.CASCADE,
        related_name='webhook_notifications',
        help_text="The Portal this notification belongs to."
    )

    item_id = models.CharField(
        max_length=100,
        help_text="ID of the item that triggered the notification."
    )

    owner = models.CharField(
        max_length=100,
        help_text="Username of the item owner who received the notification."
    )

    notification_type = models.CharField(
        max_length=50,
        choices=[
            ('public_unshare_webhook', 'Public Item Unshare Webhook'),
            # Add other notification types as needed
        ],
        help_text="Type of notification sent."
    )

    sent_at = models.DateTimeField(
        auto_now_add=True,
        help_text="When the notification was sent."
    )

    item_title = models.CharField(
        max_length=200,
        blank=True,
        help_text="Title of the item (for reference)."
    )

    item_type = models.CharField(
        max_length=100,
        blank=True,
        help_text="Type of the item (for reference)."
    )

    class Meta:
        verbose_name = "Webhook Notification Log"
        verbose_name_plural = "Webhook Notification Logs"
        indexes = [
            models.Index(fields=['portal', 'item_id', 'sent_at']),
            models.Index(fields=['portal', 'owner', 'sent_at']),
            models.Index(fields=['sent_at']),  # For cleanup tasks
        ]

    def __str__(self):
        return f"{self.notification_type} - {self.owner} - {self.item_title} ({self.sent_at})"


class ReplacementJob(models.Model):
    """
    Tracks a service replacement operation: repointing consuming webmaps and
    apps from a source service to one or more replacement services.
    """
    STATUS_CHOICES = [
        ("analyzing", "Analyzing"),
        ("dry_run", "Dry Run"),
        ("pending", "Pending"),
        ("running", "Running"),
        ("completed", "Completed"),
        ("completed_errors", "Completed with errors"),
        ("failed", "Failed"),
        ("reverting", "Reverting"),
        ("reverted", "Reverted"),
    ]

    # Statuses during which no other replacement job may start for the portal
    ACTIVE_STATUSES = ("analyzing", "pending", "running", "reverting")

    # Statuses from which a job (with surviving applied backups) can be
    # reverted; "failed" is included because an execute that dies mid-run
    # leaves already-updated items behind
    REVERTABLE_STATUSES = ("completed", "completed_errors", "failed")

    portal_instance = models.ForeignKey(
        Portal,
        on_delete=models.CASCADE,
        related_name="replacement_jobs"
    )
    source_service = models.ForeignKey(
        "Service",
        null=True,
        on_delete=models.SET_NULL,
        related_name="replacement_jobs"
    )
    source_service_name = models.TextField(
        help_text="Denormalized service name; survives Service deletion."
    )
    replacement_config = models.JSONField(
        default=dict,
        help_text="Raw UI input: {'mode': 'simple', 'target_service_id': n} or "
                  "{'mode': 'advanced', 'layer_mappings': [...]}."
    )
    replacement_pairs = models.JSONField(
        default=list,
        help_text="Ordered [[old, new], ...] string pairs computed at dry-run."
    )
    sublayer_renumber = models.JSONField(
        default=dict, blank=True,
        help_text="{old_layer_id: new_layer_id} for whole-service map image "
                  "layers, computed at dry-run alongside replacement_pairs. "
                  "These sublayer numbers are bare integers in the web map "
                  "JSON, so URL replacement cannot reach them."
    )
    selected_map_ids = models.JSONField(default=list)
    selected_app_ids = models.JSONField(default=list)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="dry_run")
    dry_run_summary = models.JSONField(
        default=dict,
        help_text="{'items': [...], 'warnings': [...], 'totals': {...}}"
    )
    initiated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        on_delete=models.SET_NULL
    )
    celery_task_id = models.CharField(max_length=100, blank=True)
    revert_task_id = models.CharField(max_length=100, blank=True)
    error_message = models.TextField(blank=True)
    created = models.DateTimeField(auto_now_add=True)
    status_updated = models.DateTimeField(
        default=timezone.now,
        help_text="When status last changed; anchors abandoned-job cleanup."
    )
    executed_at = models.DateTimeField(null=True, blank=True)
    reverted_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = "Replacement Job"
        verbose_name_plural = "Replacement Jobs"
        ordering = ["-created"]
        indexes = [
            models.Index(fields=["portal_instance", "status"]),
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._loaded_status = self.status

    def save(self, *args, **kwargs):
        # Keep status_updated in step with status transitions, including
        # saves restricted by update_fields
        if self.status != self._loaded_status:
            self.status_updated = timezone.now()
            update_fields = kwargs.get("update_fields")
            if update_fields is not None and "status_updated" not in update_fields:
                kwargs["update_fields"] = [*update_fields, "status_updated"]
        super().save(*args, **kwargs)
        self._loaded_status = self.status

    def __str__(self):
        return f"Replace {self.source_service_name} ({self.get_status_display()}, {self.created:%Y-%m-%d %H:%M})"


class ReplacementItemBackup(models.Model):
    """
    Snapshot of a portal item's pre-change state taken by a ReplacementJob so
    the change can be reverted. Only content that will change is stored, but
    the url property is always snapshotted because revert restores it.
    """
    STATUS_CHOICES = [
        ("analyzed", "Analyzed"),
        ("applied", "Applied"),
        ("failed", "Failed"),
        ("apply_failed", "Partially Applied (Failed)"),
        ("skipped", "Skipped"),
        ("reverted", "Reverted"),
        ("revert_failed", "Revert Failed"),
    ]

    # Backup statuses eligible for (re-)revert
    REVERTABLE_STATUSES = ("applied", "revert_failed", "apply_failed")

    # How long a revert claim (revert_claimed_at) is honored before it's
    # treated as abandoned (worker crashed/broker redelivery) and the row
    # becomes claimable again
    REVERT_CLAIM_LEASE = timedelta(hours=2)

    job = models.ForeignKey(
        ReplacementJob,
        on_delete=models.CASCADE,
        related_name="item_backups"
    )
    item_id = models.CharField(max_length=50)
    item_type = models.CharField(max_length=100, blank=True)
    item_title = models.TextField(blank=True)
    item_owner = models.CharField(max_length=100, blank=True)
    url_property = models.TextField(null=True, blank=True)
    data_text = models.TextField(null=True, blank=True)
    resources = models.JSONField(
        default=dict,
        help_text="{resource_name: pre-change text} for changing resources only."
    )
    item_modified_at = models.BigIntegerField(
        null=True,
        help_text="item.modified epoch ms at backup time."
    )
    applied_modified_at = models.BigIntegerField(
        null=True,
        help_text="item.modified epoch ms right after this job updated the item; "
                  "lets revert detect (and flag) edits made after the replacement."
    )
    counts = models.JSONField(
        default=dict,
        help_text="{'url': n, 'data': n, 'resources': n} replacements applied."
    )
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="analyzed")
    error = models.TextField(blank=True)
    created = models.DateTimeField(auto_now_add=True)
    revert_claimed_at = models.DateTimeField(
        null=True, blank=True,
        help_text="Set when a revert claims this backup, so a concurrent duplicate "
                  "request or redelivered task cannot revert it twice. Cleared when "
                  "the revert finishes; a claim older than REVERT_CLAIM_LEASE is "
                  "treated as abandoned and becomes claimable again."
    )

    class Meta:
        verbose_name = "Replacement Item Backup"
        verbose_name_plural = "Replacement Item Backups"
        unique_together = ("job", "item_id")

    def __str__(self):
        return f"{self.item_title or self.item_id} ({self.get_status_display()})"


class Job(models.Model):
    """
    One unit of background work: the queue, the progress bar and the run
    history, in a single row.

    This replaces the three separate things celery needed — a broker to hold
    the message, a result backend to hold the outcome, and a side channel for
    progress. Keeping them together is what lets the whole system run with no
    service beyond PostgreSQL, which is the requirement on the Windows host.

    Rows are claimed by ``run_worker`` with SELECT ... FOR UPDATE SKIP LOCKED,
    so more than one worker is safe even though only one is deployed.

    The properties at the bottom deliberately mirror django_celery_results'
    TaskResult field names, so the run-history templates that render this did
    not have to change.
    """

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    FAILURE = "FAILURE"
    CANCELED = "CANCELED"

    STATUS_CHOICES = [
        (QUEUED, "Queued"),
        (RUNNING, "Running"),
        (SUCCESS, "Success"),
        (WARNING, "Completed with errors"),
        (FAILURE, "Failed"),
        (CANCELED, "Canceled"),
    ]

    #: Statuses that mean the worker is finished with the row.
    TERMINAL = frozenset({SUCCESS, WARNING, FAILURE, CANCELED})

    # The primary key is a UUID because it is handed straight to the browser as
    # the progress-polling URL, and it kept those URLs the same shape as the
    # celery task ids they replaced.
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    #: Human-readable, shown in the UI: "Update webmaps", "Pro License Tool".
    name = models.CharField(max_length=150)
    #: Registry key from app.jobs — how the worker finds the callable.
    func = models.CharField(max_length=255)
    args = models.JSONField(default=list, blank=True)
    kwargs = models.JSONField(default=dict, blank=True)

    status = models.CharField(
        max_length=10, choices=STATUS_CHOICES, default=QUEUED, db_index=True
    )

    progress_current = models.PositiveIntegerField(default=0)
    progress_total = models.PositiveIntegerField(default=0)
    progress_description = models.TextField(blank=True)

    result = models.JSONField(null=True, blank=True)
    error = models.TextField(blank=True)
    traceback = models.TextField(blank=True)

    portal_alias = models.CharField(max_length=20, blank=True, db_index=True)

    #: Set when a PeriodicTask produced this job; blank for user-initiated runs.
    periodic_task_name = models.CharField(max_length=200, blank=True)

    queued_at = models.DateTimeField(default=timezone.now)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    cancel_requested = models.BooleanField(default=False)
    #: Wall-clock limit, checked at the same boundaries. Replaces soft_time_limit.
    deadline_at = models.DateTimeField(null=True, blank=True)

    # Written while a job runs so a job orphaned by a worker crash can be
    # spotted and requeued rather than sitting in RUNNING forever.
    worker_id = models.CharField(max_length=100, blank=True)
    heartbeat_at = models.DateTimeField(null=True, blank=True)

    attempts = models.PositiveSmallIntegerField(default=0)

    # Carried from the HTTP request that queued the job, so log lines written
    # by the job join up with the request that caused it.
    request_id = models.CharField(max_length=64, blank=True)
    username = models.CharField(max_length=150, blank=True)
    client_ip = models.CharField(max_length=45, blank=True)
    request_path = models.TextField(blank=True)

    class Meta:
        verbose_name = "Background Job"
        verbose_name_plural = "Background Jobs"
        ordering = ["-queued_at"]
        indexes = [
            # The claim query: oldest queued job first.
            models.Index(fields=["status", "queued_at"], name="job_claim_idx"),
            # The run-history tables: latest finished runs for one portal.
            models.Index(fields=["portal_alias", "-finished_at"], name="job_history_idx"),
            # Finding running jobs whose worker has stopped reporting.
            models.Index(fields=["status", "heartbeat_at"], name="job_heartbeat_idx"),
        ]

    def __str__(self):
        return f"{self.name} ({self.status})"

    @property
    def is_terminal(self):
        return self.status in self.TERMINAL

    @property
    def progress_percent(self):
        """Whole-percent progress, clamped to 0-100."""
        if self.is_terminal:
            return 100
        if not self.progress_total:
            return 0
        pct = (self.progress_current / self.progress_total) * 100
        return max(0, min(100, int(pct)))

    @property
    def is_active(self):
        """Queued or running: the worker is not finished with this row."""
        return self.status in (self.QUEUED, self.RUNNING)

    def idle_description(self):
        """
        What to show under the bar before a job describes itself, and after.

        Names the state rather than a phase, and only while there is still one
        to come: an empty line next to a bar that is not moving yet reads as a
        stall. A finished job says nothing here — see bar_description.
        """
        return {
            self.QUEUED: "Waiting for a worker…",
            self.RUNNING: "Working…",
        }.get(self.status, "")

    @property
    def bar_description(self):
        """
        The line under the progress bar, or "" once the job is over.

        progress_description holds the last phase the job announced, and
        nothing rewrites it when the job ends — so a completed refresh sat
        there reading "Removing outdated records…", describing work that had
        finished several seconds earlier.

        Rather than replace it with "Finished", say nothing. The bar has
        already turned green, amber or red, calcite-progress carries the same
        outcome in its accessible label, and an alert has just named it. A
        fourth copy adds nothing, and a stale one actively misleads.

        The column keeps its last value either way: on a failed job, knowing
        which phase it died in is worth having in the admin.
        """
        if self.is_terminal:
            return ""
        return self.progress_description or self.idle_description()

    @property
    def progress_info(self):
        """
        The dict partials/progress_bar.html renders from.

        On the model rather than in the view because two places need it: the
        progress endpoint htmx polls, and the page itself, which renders a live
        bar for a job that was already running when it loaded.
        """
        return {
            "state": self.status,
            "complete": self.is_terminal,
            "success": self.status == self.SUCCESS,
            "percent": self.progress_percent,
            "current": self.progress_current,
            "total": self.progress_total,
            "description": self.bar_description,
            "cancel_requested": self.cancel_requested,
        }


    @property
    def task_name(self):
        return self.name

    @property
    def date_created(self):
        return self.started_at or self.queued_at

    @property
    def date_done(self):
        return self.finished_at

    def result_as_dict(self):
        """
        The result payload as a dict.

        A method rather than a property to match the TaskResult helper the
        templates already call, which parsed a JSON string. Here the column is
        already JSON, so this only has to guarantee the type.
        """
        return self.result if isinstance(self.result, dict) else {}
