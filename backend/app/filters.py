# Licensed under GPLv3 - See LICENSE file for details.
import django_filters
from django.db.models import Q
from django.utils import timezone
from datetime import timedelta


from .models import Webmap, Service, Layer, App, User, LogEntry


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


TIME_FILTER_CHOICES = [
    ('', 'All Time'),
    ('hour', 'Last Hour'),
    ('day', 'Last 24 Hours'),
    ('week', 'Last 7 Days'),
    ('month', 'Last 30 Days'),
]

class LogEntryFilter(django_filters.FilterSet):
    level = django_filters.MultipleChoiceFilter(
        choices=LogEntry.LOG_LEVEL_CHOICES,
        widget=django_filters.widgets.CSVWidget,
        label="Level"
    )
    time_range = django_filters.ChoiceFilter(
        label="Time Range",
        method='filter_by_time_range',
        choices=(
            ('', 'Any Time'),
            ('hour', 'Last Hour'),
            ('day', 'Last Day'),
            ('week', 'Last Week'),
            ('month', 'Last Month'),
        ),
        empty_label=None,
    )

    search_term = django_filters.CharFilter(method='filter_search_term', label="Search")

    class Meta:
        model = LogEntry
        fields = ['level', 'time_range', 'search_term']

    def filter_search_term(self, queryset, name, value):
        if value:
            return queryset.filter(
                Q(message__icontains=value) | Q(logger_name__icontains=value) | Q(funcName__icontains=value)
            )
        return queryset

    def filter_by_time_range(self, queryset, name, value):
        now = timezone.now()
        if value == 'hour':
            return queryset.filter(timestamp__gte=now - timedelta(hours=1))
        elif value == 'day':
            return queryset.filter(timestamp__gte=now - timedelta(days=1))
        elif value == 'week':
            return queryset.filter(timestamp__gte=now - timedelta(weeks=1))
        elif value == 'month':
            return queryset.filter(timestamp__gte=now - timedelta(weeks=4))
        return queryset
