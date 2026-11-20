"""
ArcGIS OAuth2 backend
"""
import logging
from urllib.parse import urlparse

from arcgis.gis import GIS
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from social_core.backends.arcgis import ArcGISOAuth2


logger = logging.getLogger("enterpriseviz")


def arcgis_org_url():
    """
    The organization this application signs users in against.

    Returns the configured URL with no trailing slash, or "" when nothing is
    configured. Callers that are about to build a request out of it must use
    :func:`require_arcgis_org_url` instead; this one is for the login page,
    which needs to render whatever is configured without failing when nothing
    is — including when the database cannot be reached to find out, since the
    page still has local sign-in to offer.
    """
    from app.models import SiteSettings

    try:
        return (SiteSettings.load().arcgis_org_url or "").strip().rstrip("/")
    except Exception as e:
        logger.error(f"Could not read the ArcGIS organization URL from SiteSettings: {e}")
        return ""


def require_arcgis_org_url():
    """
    The organization URL, or a loud failure.

    The URLs below used to be class attributes built by concatenation at import
    time, so an unset organization produced "/sharing/rest/oauth2/authorize": a
    relative URL, which resolves against this application's own host. The
    sign-in redirect then pointed at a path this app does not serve, and the
    token exchange POSTed the client credentials back to this application
    instead of to a portal.

    Refusing to build a URL at all is the only safe answer. It is raised when
    the OAuth flow is used rather than at import, so a deployment with no ArcGIS
    organization configured still starts and still offers local sign-in.
    """
    url = arcgis_org_url()
    if not url:
        raise ImproperlyConfigured(
            "No ArcGIS organization URL is configured, so ArcGIS sign-in cannot be used. "
            "Set it under Settings > ArcGIS Sign-In to the Enterprise portal or ArcGIS "
            "Online organization URL, e.g. https://example.maps.arcgis.com."
        )
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ImproperlyConfigured(
            f"The ArcGIS organization URL must be an absolute http(s) URL; got {url!r}."
        )
    return url


class PortalOAuth2(ArcGISOAuth2):
    name = "arcgis"
    ID_KEY = "username"
    ACCESS_TOKEN_METHOD = "POST"
    REFRESH_TOKEN_METHOD = "POST"
    REDIRECT_STATE = False

    EXTRA_DATA = [
        ("expires_in", "expires_in")
    ]

    def authorization_url(self):
        return f"{require_arcgis_org_url()}/sharing/rest/oauth2/authorize"

    def access_token_url(self):
        return f"{require_arcgis_org_url()}/sharing/rest/oauth2/token"

    def refresh_token_url(self):
        return self.access_token_url()

    def get_key_and_secret(self):
        """
        The OAuth client credentials, from SiteSettings rather than Django
        settings.

        This is the only place that has to know where they live: social-core
        routes every use of the client id and secret through
        BaseAuth.get_key_and_secret() — the authorization redirect, the token
        exchange and the refresh all call it — and its default implementation
        is what reads SOCIAL_AUTH_ARCGIS_KEY and _SECRET off django.conf.
        """
        from app.models import SiteSettings

        site_settings = SiteSettings.load()
        return site_settings.arcgis_client_id, site_settings.arcgis_client_secret

    def get_user_details(self, response):
        """Return user details from ArcGIS account"""
        return {"username": response.get("username"),
                "email": response.get("email"),
                "fullname": response.get("fullName"),
                "first_name": response.get("firstName"),
                "last_name": response.get("lastName")}

    def user_data(self, access_token, *args, **kwargs):
        """Loads user data from service"""
        client_id, _secret = self.get_key_and_secret()
        return self.get_json(
            self.access_token_url(),
            params={
                "client_id": client_id,
                "refresh_token": kwargs["response"]["refresh_token"],
                "grant_type": "refresh_token"

            }
        )


def user_role(backend, user, response, *args, **kwargs):
    """
    Admit only users holding the configured portal role.

    This is the gate on who may sign in, so it fails closed: an unconfigured
    role refuses everybody rather than admitting the whole organization while
    an administrator is midway through setting ArcGIS sign-in up.

    The role is matched against both ``role`` and ``roleId``. A portal reports
    a built-in role in ``role`` ("org_admin", "org_publisher", "org_user"), but
    a user holding a *custom* role is reported as ``org_user`` with the role's
    id in ``roleId``. Comparing only ``role`` would mean a deployment that
    configured a custom role id — which the field's help text offers — refused
    every sign-in, with nothing to say why.

    Staff status is not decided here. An ArcGIS account arrives without it and
    is promoted by hand in the Django admin, because staff can edit the site
    settings row that holds the OAuth client secret, the webhook secret and the
    SMTP password.
    """
    from app.models import SiteSettings
    from social_core.exceptions import AuthForbidden

    site_settings = SiteSettings.load()
    required_role = (site_settings.arcgis_user_role or "").strip()
    if not required_role:
        logger.error("ArcGIS sign-in refused: no required role is configured "
                     "under Settings > ArcGIS Sign-In.")
        raise AuthForbidden(backend)

    token = response.get("access_token")
    url = require_arcgis_org_url()
    target = GIS(url=url, token=token, verify_cert=getattr(settings, "ARCGIS_VERIFY_TLS", True))

    portal_user = target.properties.user
    held = {str(getattr(portal_user, attr, "") or "") for attr in ("role", "roleId")}
    if required_role not in held:
        logger.warning(
            f"ArcGIS sign-in refused for '{getattr(portal_user, 'username', 'unknown')}': "
            f"holds {sorted(r for r in held if r)}, requires '{required_role}'.")
        raise AuthForbidden(backend)

    return {"is_new": True}


from social_core.exceptions import AuthForbidden
from social_django.middleware import SocialAuthExceptionMiddleware
from django.http import HttpResponse


class MySocialAuthExceptionMiddleware(SocialAuthExceptionMiddleware):
    def process_exception(self, request, exception):
        from django.template import loader
        if isinstance(exception, AuthForbidden):
            context = {"details": "Not Authorized - User does not have the required ArcGIS user role"}
            template = loader.get_template("403.html")
            return HttpResponse(template.render(context, request), status=403)
