from django.urls import path, re_path
from app.views import *
app_name = 'enterpriseviz'

urlpatterns = [
    path('portal/add/', PortalCreateView.as_view(), name='create'),
    # path('auth/', auth_view, name='auth'),
    path('login/', login_view, name='login'),
    path('logout/', logout_view, name='logout'),
    path('layer/<name>/', PortalLayerView.as_view(), name='layer'),
    path('portal/<instance>/', viz_html, name='viz'),
    path('portal/<instance>/service/<path:url>/', PortalServiceView.as_view(), name='service'),
    path('portal/<instance>/map/<id>', PortalMapView.as_view(), name='map'),


    # The home page
    path('', index, name='index'),
]
