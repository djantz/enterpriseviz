# Licensed under GPLv3 - See LICENSE file for details.
from django.contrib import admin

from .models import Webmap, Portal, Service, Layer, App, User, Map_Service, Map_Layer, App_Service, App_Map, \
    Layer_Service, UserProfile, SiteSettings, LogEntry


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
