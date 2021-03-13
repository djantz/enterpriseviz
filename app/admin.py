from django.contrib import admin
from .models import Webmap, Portal, Service, Layer, App
# Register your models here.

admin.site.register(Webmap)
admin.site.register(Portal)
admin.site.register(Service)
admin.site.register(Layer)
admin.site.register(App)
