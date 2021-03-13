"""
ArcGIS OAuth2 backend
"""
from social_core.backends.arcgis import ArcGISOAuth2
from arcgis.gis import GIS
from django.conf import settings


class PortalOAuth2(ArcGISOAuth2):
    name = 'arcgis'
    ID_KEY = 'username'
    AUTHORIZATION_URL = settings.SOCIAL_AUTH_ARCGIS_URL+'/sharing/rest/oauth2/authorize'
    ACCESS_TOKEN_URL = settings.SOCIAL_AUTH_ARCGIS_URL+'/sharing/rest/oauth2/token'
    ACCESS_TOKEN_METHOD = 'POST'
    REFRESH_TOKEN_URL = settings.SOCIAL_AUTH_ARCGIS_URL+'/sharing/rest/oauth2/token'
    REFRESH_TOKEN_METHOD = 'POST'
    STATE_PARAMETER = False
    REDIRECT_STATE = False
    EXTRA_DATA = [
        ('expires_in', 'expires_in')
    ]

    def get_user_details(self, response):
        """Return user details from ArcGIS account"""
        return {'username': response.get('username'),
                'email': response.get('email'),
                'fullname': response.get('fullName'),
                'first_name': response.get('firstName'),
                'last_name': response.get('lastName')}

    def user_data(self, access_token, *args, **kwargs):
        """Loads user data from service"""
        return self.get_json(
            self.ACCESS_TOKEN_URL,
            params={
                'client_id': settings.SOCIAL_AUTH_ARCGIS_KEY,
                'refresh_token': kwargs['response']['refresh_token'],
                'grant_type': 'refresh_token'

            }
        )


def user_role(backend, user, response, *args, **kwargs):
    from social_core.exceptions import AuthForbidden
    token = response.get('access_token')
    url = settings.SOCIAL_AUTH_ARCGIS_URL
    target = GIS(url=url, token=token)
    if target.properties.user.role == settings.ARCGIS_USER_ROLE:
        return {'is_new': True}
    else:
        raise AuthForbidden(backend)


from social_core.exceptions import AuthForbidden
from social_django.middleware import SocialAuthExceptionMiddleware
from django.http import HttpResponse

class MySocialAuthExceptionMiddleware(SocialAuthExceptionMiddleware):
    def process_exception(self, request, exception):
        from django.template import loader
        if isinstance(exception, AuthForbidden):
            context = {'details': 'Not Authorized - User does not have the required ArcGIS user role'}
            template = loader.get_template('app/page_403.html')
            return HttpResponse(template.render(context, request), status=403)

