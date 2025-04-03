# Licensed under GPLv3 - See LICENSE file for details.
import django_filters
from django.db.models import Q

from .models import Webmap, Service, Layer, App, User


class WebmapFilter(django_filters.FilterSet):
    search = django_filters.CharFilter(method="filter_by_search", label="Search")

    class Meta:
        model = Webmap
        fields = ["search"]

    def filter_by_search(self, queryset, name, value):
        if value:
            return queryset.filter(
                Q(webmap_id__icontains=value) |
                Q(webmap_title__icontains=value) |
                Q(webmap_owner__user_username__icontains=value) |
                Q(webmap_access__icontains=value)
                # Add more fields as needed
            )
        return queryset


class ServiceFilter(django_filters.FilterSet):
    search = django_filters.CharFilter(method="filter_by_search", label="Search")

    class Meta:
        model = Service
        fields = ["search"]

    def filter_by_search(self, queryset, name, value):
        if value:
            return queryset.filter(
                Q(service_name__icontains=value) |
                Q(service_url__icontains=value) |
                Q(service_mxd__icontains=value) |
                Q(service_mxd_server__icontains=value) |
                Q(service_owner__user_username__icontains=value) |
                Q(service_access__icontains=value)
                # Add more fields as needed
            )
        return queryset


class LayerFilter(django_filters.FilterSet):
    search = django_filters.CharFilter(method="filter_by_search", label="Search")

    class Meta:
        model = Layer
        fields = ["search"]

    def filter_by_search(self, queryset, name, value):
        if value:
            return queryset.filter(
                Q(layer_name__icontains=value) |
                Q(layer_server__icontains=value) |
                Q(layer_database__icontains=value) |
                Q(layer_version__icontains=value)
                # Add more fields as needed
            )
        return queryset


class AppFilter(django_filters.FilterSet):
    search = django_filters.CharFilter(method="filter_by_search", label="Search")

    class Meta:
        model = App
        fields = ["search"]

    def filter_by_search(self, queryset, name, value):
        if value:
            return queryset.filter(
                Q(app_id__icontains=value) |
                Q(app_title__icontains=value) |
                Q(app_type__icontains=value) |
                Q(app_owner__user_username__icontains=value) |
                Q(app_access__icontains=value)
                # Add more fields as needed
            )
        return queryset


class UserFilter(django_filters.FilterSet):
    search = django_filters.CharFilter(method="filter_by_search", label="Search")

    class Meta:
        model = User
        fields = ["search"]

    def filter_by_search(self, queryset, name, value):
        if value:
            return queryset.filter(
                Q(user_username__icontains=value) |
                Q(user_first_name__icontains=value) |
                Q(user_last_name__icontains=value) |
                Q(user_email__icontains=value) |
                Q(user_role__icontains=value) |
                Q(user_level__icontains=value) |
                Q(user_provider__icontains=value) |
                Q(user_pro_license__icontains=value)
                # Add more fields as needed
            )
        return queryset
