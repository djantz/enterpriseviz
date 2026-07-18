# Licensed under GPLv3 - See LICENSE file for details.
from django.conf import settings
from django.contrib import admin
from django.urls import path, include

admin.site.site_url = r"/enterpriseviz"

handler400 = "app.views.error_400_view"
handler403 = "app.views.error_403_view"
handler404 = "app.views.error_404_view"
handler500 = "app.views.error_500_view"

urlpatterns = [
    path(settings.ADMIN_URL, admin.site.urls),
    path(r'enterpriseviz/admin/', admin.site.urls),
    path('enterpriseviz', include("app.urls")),
    path(r'enterpriseviz/oauth/', include("social_django.urls", namespace="social")),

]

if settings.DEBUG:
    # This allows the error pages to be debugged during development, just visit
    # these url in browser to see how these error pages look like.
    from app import views as app_views

    urlpatterns += [
        path("400/", app_views.error_400_view),
        path("403/", app_views.error_403_view),
        path("404/", app_views.error_404_view),
        path("500/", app_views.error_500_view),
    ]
    if "debug_toolbar" in settings.INSTALLED_APPS:
        import debug_toolbar

        urlpatterns = [path("__debug__/", include(debug_toolbar.urls))] + urlpatterns

# Add 'prefix' to all urlpatterns
if settings.URL_PREFIX:
    urlpatterns = [path(f"{settings.URL_PREFIX}/", include(urlpatterns))]
