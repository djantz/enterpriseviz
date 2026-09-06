# Licensed under GPLv3 - See LICENSE file for details.
from django.contrib import admin

from .models import Webmap, Portal, Service, Layer, App, User, Map_Service, Map_Layer, App_Service, App_Map, \
    Layer_Service, UserProfile, SiteSettings, LogEntry, PortalToolSettings, WebhookNotificationLog, \
    ReplacementJob, ReplacementItemBackup, Job


# Register your models here.

class SecretFreeAdminMixin:
    """
    Keep stored secrets out of the admin.

    django_cryptography's encrypt() fields decrypt transparently on load, so a
    default ModelAdmin renders a portal's password, OAuth client secret and
    refresh token as readable text in the change form — bypassing the
    PasswordInput(render_value=False) widgets the app's own forms use, for any
    staff user who can reach the admin.

    Subclasses list those fields in `secret_fields`; they are excluded from the
    form entirely and replaced with a read-only indicator, so an administrator
    can still tell whether a secret is set. Secrets are set and cleared through
    the app's own forms, which is also where the logic for invalidating tokens
    when a portal's URL or auth method changes lives.
    """

    secret_fields = ()

    def get_exclude(self, request, obj=None):
        return tuple(super().get_exclude(request, obj) or ()) + tuple(self.secret_fields)

    def get_readonly_fields(self, request, obj=None):
        return tuple(super().get_readonly_fields(request, obj)) + ("stored_secrets",)

    @admin.display(description="Stored secrets")
    def stored_secrets(self, obj=None):
        if obj is None:
            return "-"
        states = ("%s: %s" % (self.model._meta.get_field(name).verbose_name,
                             "set" if getattr(obj, name, None) else "not set")
                  for name in self.secret_fields)
        return "; ".join(states)


class PortalAdmin(SecretFreeAdminMixin, admin.ModelAdmin):
    secret_fields = ("password", "token", "oauth_client_secret", "oauth_refresh_token")
    list_display = ("alias", "url", "portal_type", "auth_method")
    list_filter = ("portal_type", "auth_method")


class SiteSettingsAdmin(SecretFreeAdminMixin, admin.ModelAdmin):
    """
    Superuser only. This is one row holding the SMTP password and the webhook
    secret — the credential that authenticates every inbound webhook — so it is
    not something to hand out with a per-model permission grant. The secrets
    themselves are excluded from the form by SecretFreeAdminMixin regardless;
    these checks keep the rest of the row from being read or edited too.

    These lived on the SiteSettings model, where Django never called them.
    """

    secret_fields = ("email_password", "webhook_secret", "arcgis_client_secret")

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

class LayerAdmin(admin.ModelAdmin):
    list_per_page = 1000
    search_fields = ["layer_name", "layer_database"]


class WebmapAdmin(admin.ModelAdmin):
    search_fields = ["webmap_title"]


class ServiceAdmin(admin.ModelAdmin):
    search_fields = ["service_name"]


class AppAdmin(admin.ModelAdmin):
    search_fields = ["app_title"]

class MapServiceAdmin(admin.ModelAdmin):
    search_fields = ["webmap_id__webmap_title", "service_id__service_name"]

class LayerServiceAdmin(admin.ModelAdmin):
    search_fields = ["layer_id__layer_name", "service_id__service_name"]


class WebhookNotificationLogAdmin(admin.ModelAdmin):
    list_display = (
        'sent_at',
        'notification_type',
        'owner',
        'item_title',
        'item_type',
        'portal'
    )

    # Make all fields read-only
    readonly_fields = (
        'portal',
        'item_id',
        'owner',
        'notification_type',
        'sent_at',
        'item_title',
        'item_type'
    )

    list_filter = (
        'notification_type',
        'sent_at',
        'portal',
        'item_type'
    )

    search_fields = (
        'owner',
        'item_title',
        'item_id',
        'notification_type'
    )

    # Order by most recent first
    ordering = ('-sent_at',)
    date_hierarchy = 'sent_at'

    # Disable all modification permissions
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class ReplacementJobAdmin(admin.ModelAdmin):
    list_display = ("created", "source_service_name", "portal_instance", "status", "initiated_by", "executed_at")
    list_filter = ("status", "portal_instance")
    search_fields = ("source_service_name",)
    ordering = ("-created",)
    readonly_fields = ("replacement_config", "replacement_pairs", "selected_map_ids",
                       "selected_app_ids", "dry_run_summary", "celery_task_id",
                       "revert_task_id", "created", "executed_at", "reverted_at")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class ReplacementItemBackupAdmin(admin.ModelAdmin):
    list_display = ("created", "item_title", "item_type", "item_owner", "status", "job")
    list_filter = ("status", "item_type")
    search_fields = ("item_title", "item_id", "item_owner")
    ordering = ("-created",)
    readonly_fields = ("job", "item_id", "item_type", "item_title", "item_owner",
                       "url_property", "data_text", "resources", "item_modified_at",
                       "applied_modified_at", "counts", "created")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class JobAdmin(admin.ModelAdmin):
    """
    Read-only view of the background queue.

    The application's own pages show a portal's recent runs; this is the whole
    queue across every portal, which is what you want when a job is stuck, a
    schedule has not fired, or a worker has died. The only thing it can change
    is asking a job to stop — the worker does the stopping, at the job's next
    checkpoint.
    """

    list_display = ("name", "status", "portal_alias", "queued_at", "started_at",
                    "finished_at", "attempts", "worker_id")
    list_filter = ("status", "name", "portal_alias")
    search_fields = ("name", "portal_alias", "username", "request_id", "periodic_task_name")
    ordering = ("-queued_at",)
    date_hierarchy = "queued_at"
    actions = ("request_cancel",)

    def get_readonly_fields(self, request, obj=None):
        return [field.name for field in self.model._meta.fields]

    def has_add_permission(self, request):
        return False

    @admin.action(description="Request cancellation of the selected jobs")
    def request_cancel(self, request, queryset):
        from .jobs import request_cancel

        canceled = sum(request_cancel(job.pk) for job in queryset)
        self.message_user(
            request,
            f"Asked {canceled} job(s) to stop. A running job stops at its next "
            f"checkpoint; a queued one never starts."
        )


admin.site.register(Job, JobAdmin)
admin.site.register(Webmap, WebmapAdmin)
admin.site.register(Portal, PortalAdmin)
admin.site.register(Service, ServiceAdmin)
admin.site.register(Layer, LayerAdmin)
admin.site.register(App, AppAdmin)
admin.site.register(Map_Service, MapServiceAdmin)
admin.site.register(Map_Layer)
admin.site.register(App_Service)
admin.site.register(App_Map)
admin.site.register(Layer_Service, LayerServiceAdmin)
admin.site.register(User)
admin.site.register(UserProfile)
admin.site.register(SiteSettings, SiteSettingsAdmin)
admin.site.register(LogEntry)
admin.site.register(PortalToolSettings)
admin.site.register(WebhookNotificationLog, WebhookNotificationLogAdmin)
admin.site.register(ReplacementJob, ReplacementJobAdmin)
admin.site.register(ReplacementItemBackup, ReplacementItemBackupAdmin)
