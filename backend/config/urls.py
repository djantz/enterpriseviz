# Licensed under GPLv3 - See LICENSE file for details.
from django.conf import settings
from django.contrib import admin
from django.urls import path, include
from django.views import defaults as default_views

admin.site.site_url = r"/enterpriseviz"

urlpatterns = [
    path(settings.ADMIN_URL, admin.site.urls),
    path(r'enterpriseviz/admin/', admin.site.urls),
    path('enterpriseviz', include("app.urls")),
    path(r'enterpriseviz/oauth/', include("social_django.urls", namespace="social")),

]

if settings.DEBUG:
    # This allows the error pages to be debugged during development, just visit
    # these url in browser to see how these error pages look like.
    urlpatterns += [
        path(
            "400/",
            default_views.bad_request,
            kwargs={"exception": Exception("Bad Request!")},
        ),
        path(
            "403/",
            default_views.permission_denied,
            kwargs={"exception": Exception("Permission Denied")},
        ),
        path(
            "404/",
            default_views.page_not_found,
            kwargs={"exception": Exception("Page not Found")},
        ),
        path("500/", default_views.server_error),
    ]
    if "debug_toolbar" in settings.INSTALLED_APPS:
        import debug_toolbar

        urlpatterns = [path("__debug__/", include(debug_toolbar.urls))] + urlpatterns

# Add 'prefix' to all urlpatterns
if settings.URL_PREFIX:
    urlpatterns = [path(f"{settings.URL_PREFIX}/", include(urlpatterns))]
