# Licensed under GPLv3 - See LICENSE file for details.
from django.contrib import admin

from .models import Webmap, Portal, Service, Layer, App, User, Map_Service, Map_Layer, App_Service, App_Map, \
    Layer_Service, UserProfile, SiteSettings, LogEntry, PortalToolSettings, WebhookNotificationLog


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


admin.site.register(Webmap, WebmapAdmin)
admin.site.register(Portal)
admin.site.register(Service, ServiceAdmin)
admin.site.register(Layer, LayerAdmin)
admin.site.register(App, AppAdmin)
admin.site.register(Map_Service)
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
