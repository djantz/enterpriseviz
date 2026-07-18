# Licensed under GPLv3 - See LICENSE file for details.
from django.contrib import admin

from .models import Webmap, Portal, Service, Layer, App, User, Map_Service, Map_Layer, App_Service, App_Map, \
    Layer_Service, UserProfile, SiteSettings, LogEntry, PortalToolSettings, WebhookNotificationLog, \
    ReplacementJob, ReplacementItemBackup


# Register your models here.

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


admin.site.register(Webmap, WebmapAdmin)
admin.site.register(Portal)
admin.site.register(Service, ServiceAdmin)
admin.site.register(Layer, LayerAdmin)
admin.site.register(App, AppAdmin)
admin.site.register(Map_Service, MapServiceAdmin)
admin.site.register(Map_Layer)
admin.site.register(App_Service)
admin.site.register(App_Map)
admin.site.register(Layer_Service)
admin.site.register(User)
admin.site.register(UserProfile)
admin.site.register(SiteSettings)
admin.site.register(LogEntry)
admin.site.register(PortalToolSettings)
admin.site.register(WebhookNotificationLog, WebhookNotificationLogAdmin)
admin.site.register(ReplacementJob, ReplacementJobAdmin)
admin.site.register(ReplacementItemBackup, ReplacementItemBackupAdmin)
