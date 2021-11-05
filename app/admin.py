from django.contrib import admin
from .models import Webmap, Portal, Service, Layer, App
# Register your models here.



class LayerAdmin(admin.ModelAdmin):
    list_per_page = 1000

admin.site.register(Webmap)
admin.site.register(Portal)
admin.site.register(Service)
admin.site.register(Layer,LayerAdmin)
admin.site.register(App)