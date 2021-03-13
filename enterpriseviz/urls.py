from django.conf.urls import url, include
from django.contrib import admin


urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^', include('app.urls')),
    url(r'^oauth/', include('social_django.urls', namespace='social')),

]
