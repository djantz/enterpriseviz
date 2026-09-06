# ----------------------------------------------------------------------
# Enterpriseviz
# Copyright (C) 2025 David C Jantz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# ----------------------------------------------------------------------
import functools
import io
import secrets
from collections import defaultdict
from urllib.parse import urlencode

import cron_descriptor
from arcgis import gis
from django.conf import settings
from django.contrib import messages
from django.contrib.admin.utils import unquote
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required, login_not_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.views import redirect_to_login
from django.urls import reverse
from django.contrib.auth.forms import AuthenticationForm
from django.core.cache import cache
from django.core.exceptions import FieldError, ValidationError
from django.core.mail import get_connection, EmailMessage
from django.db import transaction, DatabaseError
from django.db.models import Count, F, IntegerField, OuterRef, Q, Subquery, Value
from django.db.models.functions import Coalesce
from django.http import Http404
from django.http import HttpResponse, JsonResponse, HttpResponseForbidden, HttpResponseServerError
from django.shortcuts import render, redirect, get_object_or_404
from django.template import loader
from django.utils import timezone
from django.utils.crypto import constant_time_compare
from django.utils.decorators import method_decorator
from django.utils.html import escape
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django_filters.views import FilterView
from django_tables2 import SingleTableMixin, RequestConfig
from django_tables2.export.views import ExportMixin

from config.customArcGIS import arcgis_org_url
from .export import SafeTableExport
from . import reports
from .filters import WebmapFilter, ServiceFilter, LayerFilter, AppFilter, UserFilter, LogEntryFilter
from .forms import (ArcGISSignInForm, LoggingAndRetentionForm, ScheduleForm,
                    SiteSettingsForm, ToolsForm, WebhookSettingsForm,
                    PortalCredentialsForm)
from .models import Portal, User, Webmap, Service, Layer, App, PortalCreateForm, UserProfile, LogEntry, SiteSettings, \
    PortalToolSettings, ReplacementJob, Map_Service, App_Map, App_Service, Layer_Service, Job
from .request_context import get_django_request_context
from .tables import WebmapTable, ServiceTable, LayerTable, AppTable, UserTable, LogEntryTable
from .throttling import client_ip, login_limiter, webhook_limiter
from .tasks import *
from .jobs import request_cancel

logger = logging.getLogger('enterpriseviz')


def superuser_required(view_func):
    """
    Restrict a view to superusers.

    For the pages that edit the SiteSettings row. SiteSettingsAdmin already
    refuses everyone but a superuser, on the grounds that one row holds the SMTP
    password, the webhook secret that authenticates every inbound webhook, and
    the ArcGIS OAuth client secret — not something to hand out with a per-model
    permission grant. Those pages edit the same row, so leaving them at
    @staff_member_required made the admin's restriction decorative: a staff user
    could read and rewrite through the application what the admin would not show
    them.

    Anonymous users are sent to sign in rather than refused, so a bookmarked
    settings URL behaves the way every other protected page does.
    """

    @functools.wraps(view_func)
    def wrapper(request, *args, **kwargs):
        user = request.user
        if not user.is_authenticated:
            return redirect_to_login(request.get_full_path(), settings.LOGIN_URL)
        if not user.is_superuser:
            logger.warning(
                f"User '{user.username}' was refused {request.path}: superuser only.")
            return render_error(
                request, 403,
                "Only a superuser can change these settings. They hold credentials "
                "shared by the whole deployment.")
        return view_func(request, *args, **kwargs)

    return wrapper


def _related_count(through_model, owner_field, counted_field):
    """COUNT(DISTINCT counted_field) of through-table rows referencing the outer row."""
    return Coalesce(
        Subquery(
            through_model.objects.filter(**{owner_field: OuterRef("pk")})
            .order_by()
            .values(owner_field)
            .annotate(c=Count(counted_field, distinct=True))
            .values("c")[:1],
            output_field=IntegerField(),
        ),
        Value(0),
    )


def _service_used_by_apps():
    """Distinct apps consuming a service directly or via a webmap (matches service detail page)."""
    apps = (
        App.objects.filter(
            Q(app_service__service_id=OuterRef("pk"))
            | Q(app_map__webmap_id__map_service__service_id=OuterRef("pk"))
        )
        .order_by()
        .annotate(_g=Value(1))
        .values("_g")
        .annotate(c=Count("id", distinct=True))
        .values("c")[:1]
    )
    return Coalesce(Subquery(apps, output_field=IntegerField()), Value(0))


def _annotate_webmap(qs):
    return qs.annotate(
        depends_on_count=_related_count(Map_Service, "webmap_id", "service_id"),
        used_by_count=_related_count(App_Map, "webmap_id", "app_id"),
    )


def _annotate_service(qs):
    return qs.annotate(
        depends_on_count=_related_count(Layer_Service, "service_id", "layer_id"),
        used_by_maps=_related_count(Map_Service, "service_id", "webmap_id"),
        used_by_apps=_service_used_by_apps(),
    ).annotate(used_by_count=F("used_by_maps") + F("used_by_apps"))


def _annotate_app(qs):
    return qs.annotate(
        depends_on_maps=_related_count(App_Map, "app_id", "webmap_id"),
        depends_on_services=_related_count(App_Service, "app_id", "service_id"),
    ).annotate(depends_on_count=F("depends_on_maps") + F("depends_on_services"))


# Configuration for the Table view
TABLE_VIEW_MODEL_CONFIG = {
    "webmap": {"model": Webmap, "table_class": WebmapTable, "filterset_class": WebmapFilter,
               "annotate": _annotate_webmap},
    "service": {"model": Service, "table_class": ServiceTable, "filterset_class": ServiceFilter,
                "annotate": _annotate_service},
    "layer": {"model": Layer, "table_class": LayerTable, "filterset_class": LayerFilter},
    "app": {"model": App, "table_class": AppTable, "filterset_class": AppFilter,
            "annotate": _annotate_app},
    "user": {"model": User, "table_class": UserTable, "filterset_class": UserFilter},
}


class ColumnVisibilityViewMixin:
    """
    'visible_cols' GET-param handling for SingleTableMixin views.

    Accepts repeated params (?visible_cols=a&visible_cols=b) and/or CSV
    (?visible_cols=a,b). Absent param => table DEFAULT_VISIBLE_COLUMNS;
    present-but-empty => hide all columns. Pinned columns (e.g. details) are
    always kept, whatever the param says.
    """
    visible_cols_param = "visible_cols"

    def _requested_visible_columns(self):
        if self.visible_cols_param not in self.request.GET:
            return None  # absent => defaults
        raw = self.request.GET.getlist(self.visible_cols_param)
        return [c for chunk in raw for c in chunk.split(",") if c]

    def get_table_kwargs(self):
        kwargs = super().get_table_kwargs()
        table_class = self.get_table_class()
        requested = self._requested_visible_columns()
        visible = (list(table_class.default_visible_columns())
                   if requested is None else requested)
        visible += [c for c in table_class.pinned_columns() if c not in visible]
        kwargs["exclude"] = tuple(c for c in table_class.base_columns if c not in visible)
        return kwargs

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        table_class = self.get_table_class()
        params = self.request.GET.copy()
        params.pop("page", None)
        context["query_params_urlencode"] = params.urlencode()
        context["all_columns"] = table_class.get_column_labels()
        requested = self._requested_visible_columns()
        pinned = table_class.pinned_columns()
        context["current_visible_columns"] = (
            list(table_class.selectable_default_columns()) if requested is None
            else [c for c in requested if c not in pinned]
        )
        return context

class Table(LoginRequiredMixin, ColumnVisibilityViewMixin, ExportMixin, SingleTableMixin, FilterView):
    """
    Dynamically renders tables, filters, and data based on a model type from URL.

    This class provides a unified interface for generating paginated Django tables
    and filtersets, configurable per model type specified in the URL's 'name' kwarg.
    It can also filter data by a portal 'instance' kwarg if provided.

    :ivar template_name: The template used for rendering the table.
    :type template_name: str
    :ivar paginate_by: Number of items per page.
    :type paginate_by: int
    :ivar model: The Django model, dynamically set.
    :type model: django.db.models.Model
    :ivar table_class: The django-tables2 Table class, dynamically set.
    :type table_class: django_tables2.tables.Table
    :ivar filterset_class: The django-filters FilterSet class, dynamically set.
    :type filterset_class: django_filters.FilterSet
    """
    template_name = "partials/table.html"
    paginate_by = 10
    export_class = SafeTableExport
    exclude_columns = ("details",)

    def _configure_for_model_type(self):
        """
        Sets model, table_class, and filterset_class based on 'name' URL kwarg.

        Raises Http404 if the model_type is not found in `TABLE_VIEW_MODEL_CONFIG`
        and no default model is configured for the class.
        """
        model_type = self.kwargs.get("name")
        config = TABLE_VIEW_MODEL_CONFIG.get(model_type)
        if config:
            self.model = config["model"]
            self.table_class = config["table_class"]
            self.filterset_class = config["filterset_class"]
            self.annotate_fn = config.get("annotate")
        else:
            self.annotate_fn = None
            if not hasattr(self, 'model') or self.model is None:
                raise Http404(f"Unsupported table type: {model_type}")

    def get_filterset_class(self):
        """
        Returns the appropriate filterset class after configuring for model type.

        :return: The filterset class for the current model type.
        :rtype: django_filters.FilterSet
        """
        self._configure_for_model_type()
        return self.filterset_class

    def get_table_class(self):
        """
        Returns the table class after configuring for model type.

        :return: The table class for the current model type.
        :rtype: type[django_tables2.tables.Table]
        """
        self._configure_for_model_type()
        return super().get_table_class()

    def get_queryset(self):
        """
        Returns the queryset for the current model type, optionally filtered by instance.

        :return: The queryset to be displayed.
        :rtype: django.db.models.QuerySet
        """
        self._configure_for_model_type()
        qs = super().get_queryset()
        if getattr(self, "annotate_fn", None):
            qs = self.annotate_fn(qs)

        instance_alias = self.kwargs.get("instance")
        if instance_alias:
            get_object_or_404(Portal, alias=instance_alias)
            try:
                return qs.filter(portal_instance__alias=instance_alias)
            except FieldError:
                logger.warning(
                    f"Model {self.model.__name__} does not support filtering by 'portal_instance__alias'. Ignoring instance filter for '{instance_alias}'.")
                return qs
        return qs

    def get_context_data(self, **kwargs):
        """
        Adds the filter, table, and pagination information to the context.

        :param kwargs: Additional keyword arguments for context.
        :return: The context dictionary for template rendering.
        :rtype: dict
        """
        context = super().get_context_data(**kwargs)
        page = context["page_obj"]
        context["paginator_range"] = page.paginator.get_elided_page_range(page.number, on_each_side=1, on_ends=1)

        self._configure_for_model_type()

        table = self.table_class(context["filter"].qs, **self.get_table_kwargs())
        RequestConfig(self.request, paginate={"per_page": self.paginate_by}).configure(table)

        context.update({
            "table": table,
            "table_name": self.kwargs.get("name"),
        })
        if "instance" in self.kwargs:
            context.update({"instance": self.kwargs["instance"]})

        return context


@method_decorator(staff_member_required, name="dispatch")
class LogTable(ColumnVisibilityViewMixin, ExportMixin, SingleTableMixin, FilterView):
    """
    Displays a paginated and filterable table of LogEntry records.

    Supports dynamic column visibility based on 'visible_cols' GET parameter.

    Staff only, and exported as well as rendered: a log row carries the client
    IP, username, request path and full traceback of whoever generated it, so
    this reads other people's activity rather than the viewer's own. Every other
    view over this data is staff-gated — including log_settings_view, which only
    changes the level — and this is the one that shows the data itself.

    :ivar model: The LogEntry Django model.
    :type model: enterpriseviz.models.LogEntry
    :ivar table_class: The LogEntryTable for rendering.
    :type table_class: enterpriseviz.tables.LogEntryTable
    :ivar filterset_class: The LogEntryFilter for querying.
    :type filterset_class: enterpriseviz.filters.LogEntryFilter
    :ivar template_name: Template for rendering the log table.
    :type template_name: str
    :ivar paginate_by: Number of log entries per page.
    :type paginate_by: int
    """
    model = LogEntry
    table_class = LogEntryTable
    filterset_class = LogEntryFilter
    template_name = "partials/log_table.html"
    paginate_by = 15
    export_class = SafeTableExport

    def get_queryset(self):
        """
        Returns LogEntry queryset ordered by timestamp descending.

        :return: Ordered queryset of log entries.
        :rtype: django.db.models.QuerySet
        """
        return super().get_queryset().order_by("-timestamp")

    def get_context_data(self, **kwargs):
        """
        Adds log-specific pagination context; column visibility comes from the mixin.

        :param kwargs: Additional keyword arguments for context.
        :return: The context dictionary for template rendering.
        :rtype: dict
        """
        context = super().get_context_data(**kwargs)

        page_obj = context.get('page_obj')
        if page_obj:
            context['paginator_range'] = page_obj.paginator.get_elided_page_range(
                page_obj.number, on_each_side=1, on_ends=1
            )

        return context


@staff_member_required
def logs_view(request):
    """
    Renders the main application logs page or a partial for HTMX requests.

    Provides context for filtering logs and selecting visible columns.

    Staff only, for the reason given on :class:`LogTable`: this is the page that
    page's rows are rendered into.

    :param request: The HTTP request object.
    :type request: django.http.HttpRequest
    :return: Rendered HTML response.
    :rtype: django.http.HttpResponse
    """
    template = "app/logs.html" if request.htmx else "app/logs_full.html"
    filterset = LogEntryFilter(request.GET, queryset=LogEntry.objects.all().order_by('-timestamp'))
    portals = Portal.objects.values_list("alias", "portal_type", "url")

    # Use LogEntryTable's definition for all columns
    all_table_columns = LogEntryTable.get_column_labels()

    default_cols = list(LogEntryTable.selectable_default_columns())
    pinned_cols = LogEntryTable.pinned_columns()

    if 'visible_cols' in request.GET:
        visible_columns_param = request.GET.getlist('visible_cols')
        if len(visible_columns_param) == 1 and visible_columns_param[0] == '':  # User deselected all
            initial_visible_columns = []
        else:
            initial_visible_columns = [col for col in visible_columns_param
                                       if col and col not in pinned_cols]
    else:
        initial_visible_columns = default_cols

    context = {
        'title': 'Application Logs',
        'filter': filterset,
        'all_columns': all_table_columns,
        'current_visible_columns': initial_visible_columns,
        'default_visible_columns_json': json.dumps(default_cols),  # For JS reset
        "portal": portals,
    }
    return render(request, template, context)


@login_required
def portal_map_view(request, instance=None, id=None):
    """
    Display the portal map details, including associated layers, services, and apps.

    This view retrieves necessary map details and portal data to render the context dynamically.

    :param request: HTTP request object
    :type request: HttpRequest
    :param id: Item id of the map
    :type id: str

    :return: Rendered template with map details or error response
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, id={id}, user={request.user.username}")
    template = "portals/portal_detail_map.html" if request.htmx else "portals/portal_detail_map_full.html"

    if not id:
        logger.warning("Missing 'id' parameter.")
        return render_error(request, 400, "Error retrieving map details: ID missing.")

    try:
        logger.debug(f"Fetching map details for ID: {id}")
        details = utils.map_details(id)
        if "error" in details:
            logger.warning(f"Error in map details response for ID {id}: {details['error']}")
            return render_error(request, 500, details["error"])

        portal_data = Portal.objects.values_list("alias", "portal_type", "url")
        details["portal"] = portal_data
        logger.debug(f"Successfully retrieved and rendering map details for ID: {id}")
        return render(request, template, details)

    except Exception as e:
        logger.error(f"Unexpected error for ID {id}: {e}", exc_info=True)
        return render_error(request, 500, "An unexpected error occurred while retrieving map details.")


@login_required
def portal_service_view(request, instance=None, url=None):
    """
    Display the portal service details, including associated layers, maps, and apps.

    This view retrieves service details for a given portal and renders the appropriate template
    based on whether the request is made via HTMX.

    :param request: The HTTP request object
    :type request: HttpRequest
    :param instance: Portal alias
    :type instance: str
    :param url: Service URL
    :type url: str

    :return: A rendered HTML response with service details
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, url_provided={bool(url)}, user={request.user.username}")
    template = "portals/portal_detail_service.html" if request.htmx else "portals/portal_detail_service_full.html"

    if not instance or not url:
        logger.warning("Missing 'instance' or 'url' parameter.")
        return render_error(request, 400, "Missing 'instance' or 'url' parameter.")

    try:
        unquoted_url = unquote(url)
        logger.debug(f"Fetching service details for instance: {instance}, url: {unquoted_url}")
        details = utils.service_details(instance, unquoted_url)

        if "error" in details:
            logger.warning(f"Error in service details for {instance}: {details['error']}")
            return render_error(request, 500, details["error"])

        portal_data = Portal.objects.values_list("alias", "portal_type", "url")
        details["portal"] = portal_data
        details["service_usage"] = None

        if hasattr(request.user, "profile") and getattr(request.user.profile, "service_usage", False):
            logger.debug(f"Fetching usage report for service {details.get('item')}")
            item = details.get("item")
            service_qs = Service.objects.filter(pk=item.pk) if item else Service.objects.none()
            if service_qs.exists():
                usage = utils.get_usage_report(service_qs)
                if "error" in usage:
                    logger.warning(f"Error in usage report: {usage['error']}")
                else:
                    details["service_usage"] = usage.get("usage")
                    logger.debug("Successfully retrieved service usage report")
            else:
                logger.debug("No item found in details to fetch usage report for.")

        logger.debug(f"Successfully retrieved and rendering service details for {instance}.")
        return render(request, template, details)

    except Exception as e:
        logger.error(f"Unexpected error for instance {instance}: {e}", exc_info=True)
        return render_error(request, 500, "An unexpected error occurred while retrieving service details.")


@login_required
def portal_layer_view(request, name=None):
    """
    Display the portal layer details, including associated services, maps, and apps.

    This view retrieves layer details for a given portal and renders the appropriate template
    based on whether the request is made via HTMX.

    :param request: The HTTP request object containing:
                   * ``request.htmx``: Boolean indicating if this is an HTMX request
                   * ``request.GET['version']``: Optional version parameter
                   * ``request.GET['server']``: Optional server parameter
                   * ``request.GET['database']``: Optional database parameter
                   * ``request.user.profile.service_usage``: User preference for showing usage data
    :type request: HttpRequest
    :param name: The layer name to fetch details
    :type name: str

    :return: A rendered HTML response with layer details
    :rtype: HttpResponse
    """
    logger.debug(f"name={name}, user={request.user.username}")
    template = "portals/portal_detail_layer.html" if request.htmx else "portals/portal_detail_layer_full.html"

    version = request.GET.get("version", None)
    dbserver = request.GET.get("server", None)
    database = request.GET.get("database", None)
    logger.debug(f"Params - version: {version}, server: {dbserver}, database: {database}")

    if not name:
        logger.warning("Layer name missing.")
        return render_error(request, 400, "Layer name is missing.")

    try:
        logger.debug(f"Fetching layer details for name: {name}")
        details = utils.layer_details(dbserver, database, version, name)
        if "error" in details:
            logger.warning(f"Error in layer details for {name}: {details['error']}")
            return render_error(request, 500, details["error"])

        portal_data = list(Portal.objects.values_list("alias", "portal_type", "url"))
        details["portal"] = portal_data

        query = urlencode([(key, value) for key, value in
                           (("server", dbserver), ("database", database), ("version", version))
                           if value])
        details["report_url"] = (reverse("enterpriseviz:layer_report", kwargs={"name": name})
                                 + (f"?{query}" if query else ""))
        details["report_label"] = name
        details["service_usage"] = None
        services_for_usage = details.get("services", [])

        if hasattr(request.user, "profile") and getattr(request.user.profile, "service_usage",
                                                        False) and services_for_usage:
            logger.debug(f"Fetching usage report for {len(services_for_usage)} services.")
            usage = utils.get_usage_report(services_for_usage)
            if "error" in usage:
                logger.warning(f"Error in usage report: {usage['error']}")
            else:
                details["service_usage"] = usage.get("usage")
                logger.debug("Successfully retrieved service usage report")

        logger.debug(f"Successfully retrieved and rendering layer details for {name}.")
        return render(request, template, details)

    except Exception as e:
        logger.error(f"Unexpected error for layer {name}: {e}", exc_info=True)
        return render_error(request, 500, "An unexpected error occurred while retrieving layer details.")


def _report_response(request, kind, details, filename_stem, fallback_label=None):
    """
    Turn a details dict into a downloadable workbook.

    Every report view calls the same ``utils.*_details()`` its page calls and
    hands the result straight here, so the workbook and the page always agree.
    """
    if isinstance(details, dict) and "error" in details:
        logger.warning(f"Cannot build {kind} report: {details['error']}")
        return render_error(request, 404, details["error"])

    try:
        workbook = reports.build_dependency_workbook(
            kind, details, user=request.user, fallback_label=fallback_label)
        buffer = io.BytesIO()
        workbook.save(buffer)
    except Exception as e:
        logger.error(f"Error building {kind} report: {e}", exc_info=True)
        return render_error(request, 500, "An unexpected error occurred while building the report.")

    payload = buffer.getvalue()
    response = HttpResponse(payload, content_type=reports.XLSX_CONTENT_TYPE)
    response["Content-Disposition"] = reports.content_disposition(filename_stem)
    response["Content-Length"] = str(len(payload))
    logger.debug(f"Built {kind} report ({len(payload)} bytes) for user={request.user.username}")
    return response


@login_required
def map_report_view(request, instance=None, id=None):
    """
    Download the web map's dependencies as a formatted workbook: one sheet each
    for services, layers and apps.

    :param request: HTTP request object
    :type request: HttpRequest
    :param instance: Portal alias
    :type instance: str
    :param id: Item id of the map
    :type id: str

    :return: An .xlsx attachment, or an error response
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, id={id}, user={request.user.username}")
    if not id:
        return render_error(request, 400, "Error building report: ID missing.")

    details = utils.map_details(id)
    title = getattr(details.get("item"), "webmap_title", None) or id
    return _report_response(request, "map", details, f"webmap-{title}", fallback_label=id)


@login_required
def service_report_view(request, instance=None, url=None):
    """
    Download the service's dependencies as a formatted workbook: one sheet each
    for web maps, layers and apps.

    The Layers sheet has no counterpart on the page itself, which only shows
    the layer count.

    :param request: HTTP request object
    :type request: HttpRequest
    :param instance: Portal alias
    :type instance: str
    :param url: Service name
    :type url: str

    :return: An .xlsx attachment, or an error response
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, url_provided={bool(url)}, user={request.user.username}")
    if not instance or not url:
        return render_error(request, 400, "Missing 'instance' or 'url' parameter.")

    service_name = unquote(url)
    details = utils.service_details(instance, service_name)
    return _report_response(request, "service", details, f"service-{service_name}",
                            fallback_label=service_name)


@login_required
def layer_report_view(request, name=None):
    """
    Download the layer's dependencies as a formatted workbook: one sheet each
    for services, web maps and apps.

    :param request: HTTP request object, optionally carrying ``server``,
                    ``database`` and ``version`` query parameters
    :type request: HttpRequest
    :param name: The layer name
    :type name: str

    :return: An .xlsx attachment, or an error response
    :rtype: HttpResponse
    """
    logger.debug(f"name={name}, user={request.user.username}")
    if not name:
        return render_error(request, 400, "Layer name is missing.")

    details = utils.layer_details(request.GET.get("server"), request.GET.get("database"),
                                  request.GET.get("version"), name)
    return _report_response(request, "layer", details, f"layer-{name}", fallback_label=name)


@login_required
def layerid_report_view(request, instance):
    """
    Download the web maps and apps that use one service layer URL.

    The page this mirrors is POST-only, but its results come from a plain
    database query, so the search is simply re-run from the ``url`` query
    parameter.

    :param request: HTTP request object carrying a ``url`` query parameter
    :type request: HttpRequest
    :param instance: Portal alias
    :type instance: str

    :return: An .xlsx attachment, or an error response
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, user={request.user.username}")
    layer_url = unquote(request.GET.get("url", "").strip())
    if not layer_url:
        return render_error(request, 400, "Provide a layer URL.")

    try:
        portal = get_object_or_404(Portal, alias=instance)
    except Http404:
        logger.warning(f"Portal instance '{instance}' not found.")
        return render_error(request, 404, f"Portal '{instance}' not found.")

    details = utils.service_layer_details(portal, layer_url)
    if isinstance(details, dict) and "error" not in details:
        details["instance_alias"] = instance
        details["url_searched"] = layer_url
        details["item"] = None
    return _report_response(request, "layerid", details, f"layer-usage-{instance}",
                            fallback_label=layer_url)


@login_required
def metadata_report_view(request, instance):
    """
    Download the metadata completeness report the user last ran for this portal.

    ``metadata_view`` is POST-only and its data comes from a live authenticated
    crawl of the portal, so a GET link cannot re-derive it without re-prompting
    for credentials and re-crawling. The page caches its result instead, and
    this reads that back.

    :param request: HTTP request object
    :type request: HttpRequest
    :param instance: Portal alias
    :type instance: str

    :return: An .xlsx attachment, or an error response
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, user={request.user.username}")
    metadata = cache.get(reports.metadata_cache_key(request.user.pk, instance))
    if metadata is None:
        logger.debug(f"No cached metadata report for '{instance}' and user {request.user.pk}.")
        return render_error(request, 404,
                            "This metadata report is no longer available. Run it again, "
                            "then download it.")

    details = {"metadata": metadata, "instance_alias": instance, "item": None}
    return _report_response(request, "metadata", details, f"metadata-{instance}",
                            fallback_label=instance)


@staff_member_required
def portal_create_view(request):
    """
    Handle creation of a portal instance.

    Processes form submissions to create new portal instances with optional authentication,
    validating connections when credentials are stored.

    :param request: The HTTP request object
    :type request: HttpRequest

    :return: Rendered response with form, success message, or error message
    :rtype: HttpResponse
    """
    logger.debug(f"Method={request.method}, user={request.user.username}")

    if request.method == "POST":
        form = PortalCreateForm(request.POST)
        if form.is_valid():
            auth_method = form.cleaned_data["auth_method"]
            alias = form.cleaned_data["alias"]
            url = form.cleaned_data["url"]
            username = form.cleaned_data.get("username", "")
            logger.debug(f"Form valid. alias={alias}, url={url}, auth_method={auth_method}")

            try:
                if auth_method == "password":
                    logger.debug(f"Attempting connection for {alias} with stored credentials.")
                    auth_result = utils.try_connection(form.cleaned_data)
                    if auth_result.get("authenticated", False):
                        form.save()
                        logger.info(
                            f"Portal {alias} ({url}) created and authenticated as {username}.")
                        context = {"portal": Portal.objects.values_list("alias", "portal_type", "url")}
                        response = render(request, "partials/portal_updates.html", context)
                        triggers = {"closeModal": True}
                        absent = auth_result.get("missing_privileges") or []
                        if absent:
                            triggers["showWarningAlert"] = _privilege_warning(username, absent)
                        else:
                            triggers["showSuccessAlert"] = (
                                f"Successfully added {url} as {alias} and authenticated.")
                        response["HX-Trigger-After-Settle"] = json.dumps(triggers)
                        return response
                    else:
                        logger.warning(f"Authentication failed for {alias} ({url}) as {username}.")
                        context = {"form": form, "oauth_redirect_uri": _oauth_redirect_uri(request)}
                        response = render(request, "partials/portal_add_form.html", context,
                                          status=401)
                        response["HX-Trigger-After-Settle"] = json.dumps(
                            {"showDangerAlert": f"Unable to connect to {url} as {username}. Please verify credentials."}
                        )
                        return response
                else:
                    form.save()
                    logger.info(f"Portal {alias} ({url}) created with auth method '{auth_method}'.")
                    if auth_method == "oauth":
                        next_step = "Connect it with ArcGIS to authorize unattended refreshes."
                    else:
                        next_step = "Authentication will be required when refreshing data."
                    context = {"portal": Portal.objects.values_list("alias", "portal_type", "url")}
                    response = render(request, "partials/portal_updates.html", context)
                    response["HX-Trigger-After-Settle"] = json.dumps(
                        {
                            "showSuccessAlert": f"Successfully added {url} as {alias}. {next_step}",
                            "closeModal": True}
                    )
                    return response
            except Exception as e:
                logger.error(f"Unexpected error creating portal {alias}: {e}",
                             exc_info=True)
                context = {"form": form, "oauth_redirect_uri": _oauth_redirect_uri(request)}
                response = render(request, "partials/portal_add_form.html", context, status=500)
                response["HX-Trigger-After-Settle"] = json.dumps(
                    {"showDangerAlert": f"An unexpected error occurred"}
                )
                return response
        else:
            logger.warning(f"Form invalid. Errors: {form.errors}")
            response = render(request, "partials/portal_add_form.html",
                              {"form": form, "oauth_redirect_uri": _oauth_redirect_uri(request)})
            response["HX-Trigger-After-Settle"] = json.dumps(
                {"showDangerAlert": _form_error_alert(form)})
            return response

    logger.debug("Rendering initial form.")
    return render(request, "portals/portal_add.html",
                  {"form": PortalCreateForm(), "oauth_redirect_uri": _oauth_redirect_uri(request)})


@login_required
def index_view(request, instance=None):
    """
    Handles the main index view request for the application.

    Aggregates information from webmaps, services, layers, apps, and users.
    If a specific portal instance is provided, the data is filtered accordingly.

    :param request: The HTTP request object that contains metadata about the request.
    :type request: HttpRequest
    :param instance: The optional portal instance for filtering data. If not provided,
                     unfiltered data across all instances will be aggregated.
    :type instance: str
    :return: The rendered HTTP response object with the context.
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, user={request.user.username}")
    template = "app/index.html" if request.htmx else "app/index_full.html"

    try:
        base_query = Q(portal_instance__alias=instance) if instance else Q()

        webmaps_count = Webmap.objects.filter(base_query).count()
        services_count = Service.objects.filter(base_query).count()
        layers_count = Layer.objects.filter(base_query).count()
        apps_count = App.objects.filter(base_query).count()
        users_count = User.objects.filter(base_query).count()

        portals = Portal.objects.values_list("alias", "portal_type", "url")
        instance_item = None
        if instance:
            try:
                instance_item = Portal.objects.get(alias=instance)
                logger.debug(f"Retrieved data for portal instance: {instance}")
            except Portal.DoesNotExist:
                logger.warning(f"Portal instance '{instance}' not found.")
                return render_error(request, 404, f"Portal '{instance}' not found.")
        else:
            logger.debug("Retrieved aggregated data across all portal instances")

        table_columns = {}
        for name, cfg in TABLE_VIEW_MODEL_CONFIG.items():
            tc = cfg["table_class"]
            # Pinned columns are neither listed nor submitted; the view re-adds them.
            defaults = list(tc.selectable_default_columns())
            table_columns[name] = {
                "columns": [{"name": n, "label": label, "selected": n in defaults}
                            for n, label in tc.get_column_labels()],
                "default_csv": ",".join(defaults),
            }

        context = {
            "webmaps": webmaps_count,
            "services": services_count,
            "layers": layers_count,
            "apps": apps_count,
            "users": users_count,
            "portal": portals,
            "instance": instance_item,
            "table_columns": table_columns,
            "refresh_jobs": {items: active_refresh_job(instance, items)
                             for items in REFRESH_TASKS},
        }
        return render(request, template, context)
    except Exception as e:
        logger.error(f"Error generating dashboard: {e}", exc_info=True)
        context = {
            "error": "An error occurred while retrieving data.",
            "portal": Portal.objects.values_list("alias", "portal_type", "url")
        }
        return render(request, template, context, status=500)


def _oauth_redirect_uri(request):
    """Build the absolute callback URL registered with each portal's OAuth application."""
    return request.build_absolute_uri(reverse("enterpriseviz:portal_oauth_callback"))


def _privilege_warning(username, absent):
    """
    Build the warning shown when a validated account lacks update privileges.

    The credentials are saved anyway: they authenticate, and the account may be intended
    for partial use. The content updates enforce the same privileges and fail with the
    specifics, so this is a heads-up rather than a block.

    :param username: The account that was validated
    :param absent: Privilege strings the account lacks
    :return: Warning text
    :rtype: str
    """
    return (f"Saved, but '{username}' is missing privilege(s) needed for updates: "
            f"{', '.join(absent)}. Content updates will fail until the account's role grants them.")


def _needs_oauth_connect(portal):
    """True when a portal is set to OAuth but has no usable authorization."""
    return portal.auth_method == "oauth" and portal.oauth_needs_reconsent


def _render_oauth_connect_prompt(request, portal, reason=None):
    """
    Prompt the user to authorize an OAuth portal instead of failing the operation.

    OAuth cannot be completed from a form the way a username and password can; it needs
    a round trip to the portal. So the prompt offers to start that flow.

    :param request: The HTTP request object
    :param portal: Portal model instance needing authorization
    :param reason: Optional explanation shown in place of the default text
    :return: Rendered connect prompt
    :rtype: HttpResponse
    """
    logger.info(f"Portal '{portal.alias}' needs OAuth authorization; prompting to connect.")
    response = render(request, "portals/portal_connect.html",
                      {"instance": portal, "reason": reason})
    response["HX-Retarget"] = "body"
    response["HX-Reswap"] = "beforeend"
    response["HX-Push-Url"] = "false"
    return response


def _form_error_alert(form):
    """
    Summarize a form's validation errors for a danger alert.

    The alert complements the inline field errors, which sit in collapsed sections
    the user may not have open.

    :param form: A bound form that failed validation
    :return: Alert text
    :rtype: str
    """
    messages_found = [error for errors in form.errors.values() for error in errors]
    if not messages_found:
        return "Please correct the errors below."
    if len(messages_found) == 1:
        return messages_found[0]
    return f"{messages_found[0]} (and {len(messages_found) - 1} more)"


def _tag_job_portal(task, replacement_job):
    """
    Record which portal a queued replacement job acts on.

    @task resolves portal_alias from a named task argument, and the two
    replacement tasks take a ReplacementJob id rather than an alias - so their
    Job rows were the only portal-scoped ones left blank, invisible to the run
    history on the portal page and to JobAdmin's portal filter. That is the one
    class of job where knowing which portal was modified matters most.

    portal_instance_id is the alias itself; Portal's primary key is its alias.

    :param task: The Job row returned by enqueue()
    :param replacement_job: The ReplacementJob the task will act on
    """
    Job.objects.filter(pk=task.pk).update(
        portal_alias=replacement_job.portal_instance_id)


@staff_member_required
def portal_oauth_connect_view(request, instance):
    """
    Start the OAuth 2.0 authorization code flow for a portal.

    Redirects the administrator to the portal's sign in page. The resulting refresh token
    is stored by :func:`portal_oauth_callback_view` and used for unattended connections.

    :param request: The HTTP request object
    :type request: HttpRequest
    :param instance: The portal alias
    :type instance: str
    :return: Redirect to the portal authorization endpoint, or back with an error
    :rtype: HttpResponse
    """
    portal = get_object_or_404(Portal, alias=instance)

    if not portal.oauth_client_id:
        logger.warning(f"OAuth connect attempted for '{portal.alias}' without a client ID.")
        messages.error(request, f"Set an OAuth Client ID for {portal.alias} before connecting.")
        return redirect("enterpriseviz:viz", instance=portal.alias)

    state = secrets.token_urlsafe(32)
    request.session["portal_oauth_state"] = state
    request.session["portal_oauth_alias"] = portal.alias

    params = urlencode({
        "client_id": portal.oauth_client_id,
        "response_type": "code",
        "redirect_uri": _oauth_redirect_uri(request),
        "state": state,
        "expiration": -1,
    })
    logger.info(f"Starting OAuth authorization for portal '{portal.alias}'.")
    return redirect(f"{utils.oauth_authorize_url(portal.url)}?{params}")


@staff_member_required
def portal_oauth_callback_view(request):
    """
    Complete the OAuth 2.0 authorization code flow and store the refresh token.

    Verifies the anti-forgery state, exchanges the authorization code, and confirms the
    consenting user holds the administrative privileges the application requires before
    persisting any tokens.

    :param request: The HTTP request object
    :type request: HttpRequest
    :return: Redirect to the portal page with a success or error message
    :rtype: HttpResponse
    """
    expected_state = request.session.pop("portal_oauth_state", None)
    alias = request.session.pop("portal_oauth_alias", None)
    returned_state = request.GET.get("state")

    if not expected_state or not alias:
        logger.warning("OAuth callback received without an active authorization session.")
        messages.error(request, "Authorization session expired. Please try connecting again.")
        return redirect("enterpriseviz:index")

    if not returned_state or not constant_time_compare(expected_state, returned_state):
        logger.warning(f"OAuth callback state mismatch for portal '{alias}'.")
        messages.error(request, "Authorization failed a security check. Please try connecting again.")
        return redirect("enterpriseviz:index")

    portal = get_object_or_404(Portal, alias=alias)

    if request.GET.get("error"):
        description = request.GET.get("error_description", request.GET["error"])
        logger.warning(f"OAuth authorization denied for '{portal.alias}': {description}")
        messages.error(request, f"Authorization was denied for {portal.alias}.")
        return redirect("enterpriseviz:viz", instance=portal.alias)

    code = request.GET.get("code")
    if not code:
        logger.warning(f"OAuth callback for '{portal.alias}' returned no authorization code.")
        messages.error(request, "Authorization did not return a code. Please try connecting again.")
        return redirect("enterpriseviz:viz", instance=portal.alias)

    try:
        token_data = utils.exchange_oauth_authorization_code(portal, code, _oauth_redirect_uri(request))
    except ConnectionError as e:
        logger.error(f"OAuth code exchange failed for '{portal.alias}': {e}")
        messages.error(request, str(e))
        return redirect("enterpriseviz:viz", instance=portal.alias)

    # Confirm the account can actually do the work before storing its token.
    try:
        target = gis.GIS(portal.url, token=token_data["access_token"],
                         verify_cert=utils.portal_verify_tls())
        me = target.users.me
        if me is None:
            raise ValueError("the session is not signed in")
        portal_type = portal.portal_type or ("portal" if target.properties.isPortal else "agol")
        missing = missing_privileges(me, portal_update_privileges(portal_type))
    except Exception as e:
        logger.error(f"Unable to verify OAuth identity for '{portal.alias}': {e}", exc_info=True)
        messages.error(request, f"Could not verify the signed in account for {portal.alias}.")
        return redirect("enterpriseviz:viz", instance=portal.alias)

    if missing:
        logger.warning(f"OAuth user '{me.username}' lacks privileges for '{portal.alias}': {', '.join(missing)}")
        messages.error(
            request,
            f"{me.username} is missing required administrator privileges: {', '.join(missing)}. "
            f"Sign in with an administrator account.")
        return redirect("enterpriseviz:viz", instance=portal.alias)

    utils.store_oauth_tokens(portal, token_data)

    if portal.portal_type != portal_type:
        portal.portal_type = portal_type
        portal.save(update_fields=['portal_type'])

    logger.info(f"Portal '{portal.alias}' connected via OAuth as '{me.username}'.")
    messages.success(request, f"Connected {portal.alias} as {me.username}.")
    return redirect("enterpriseviz:viz", instance=portal.alias)


@staff_member_required
def refresh_portal_view(request):
    """
    Initiates a background task to refresh data for a specified portal instance.

    This view is triggered by a POST request. It validates the portal instance and
    the type of items to refresh. If credentials are provided in the POST, they are
    used for authentication. If not, and the portal doesn't store credentials,
    a credential form is rendered. Otherwise, stored credentials are used implicitly
    by the background task. A Celery task is then dispatched to perform the refresh.

    Expected POST parameters:
        - 'instance': Alias of the portal to refresh.
        - 'items': Type of items to refresh (e.g., 'webmaps', 'services').
        - 'url': Optional URL of the portal (alternative to 'instance').
        - 'full_refresh': Optional str ('true'/'false') to perform a full refresh instead of incremental
                          based on items modified since last refresh timestamp.
        - 'username': Optional username for portal authentication.
        - 'password': Optional password for portal authentication.

    :param request: The HTTP request object.
    :type request: django.http.HttpRequest
    :return: Rendered HTML response, typically a progress bar partial or a credential form.
    :rtype: django.http.HttpResponse
    """
    logger.debug(f"Method={request.method}, user={request.user.username}, POST data keys={list(request.POST.keys())}")

    if request.method != "POST":
        logger.warning("Method not allowed for refresh_portal_view.")
        return JsonResponse({"error": "Method not allowed"}, status=405)

    # Get basic parameters first
    instance_alias = request.POST.get("instance")
    items_to_refresh = request.POST.get("items")
    instance_url = request.POST.get("url")
    full_refresh = request.POST.get("full_refresh", "false").lower() == "true"

    # Validate portal exists
    try:
        portal_query = Q()
        if instance_alias:
            portal_query |= Q(alias=instance_alias)
        if instance_url:
            portal_query |= Q(url=instance_url)

        if not (instance_alias or instance_url):
            logger.error("Refresh portal called without instance alias or URL.")
            return HttpResponse(status=200, headers={
                "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Portal identifier missing."})
            })

        portal = Portal.objects.filter(portal_query).first()
        if not portal:
            logger.warning(f"Portal instance for '{instance_alias or instance_url}' not found.")
            return HttpResponse(status=200, headers={
                "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Invalid portal instance."})
            })

    except Exception as e:
        logger.error(f"Error fetching portal '{instance_alias or instance_url}': {e}", exc_info=True)
        return HttpResponse(status=200, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Error accessing portal data."})
        })

    # Validate task type
    task_func = REFRESH_TASKS.get(items_to_refresh)
    if not task_func:
        logger.warning(f"Invalid task type '{items_to_refresh}' for portal '{portal.alias}'.")
        return JsonResponse({"error": "Invalid task type selected"}, status=400)

    running = active_refresh_job(portal.alias, items_to_refresh)
    if running:
        logger.info(f"Refresh of '{items_to_refresh}' already {running.status.lower()} "
                    f"for '{portal.alias}' ({running.pk}); not queueing another.")
        response_data = {
            "instance": portal.alias,
            "task_id": running.id,
            "task_name": running.name,
            "value": running.progress_percent,
            "progress": running.progress_info,
            "refresh_control": _refresh_control_context(
                running, items=items_to_refresh, instance=portal.alias),
            "panel_statuses": _panel_status_contexts(
                running, items=items_to_refresh, instance=portal.alias),
        }
        response = render(request, "partials/progress_response.html", context=response_data)
        response["HX-Trigger-After-Settle"] = json.dumps({
            "showWarningAlert": f"A {items_to_refresh} refresh is already running for "
                                f"{portal.alias}."})
        return response

    # Determine if credentials are needed
    if _needs_oauth_connect(portal):
        return _render_oauth_connect_prompt(request, portal)

    credentials_required = portal.requires_interactive_credentials
    credential_token = None

    # Check if this is a credential form submission
    is_credential_submission = 'username' in request.POST or 'password' in request.POST

    if credentials_required:
        if is_credential_submission:
            # Validate credential form submission
            logger.debug(f"Processing credential form submission for portal '{portal.alias}'.")
            form = PortalCredentialsForm(
                request.POST,
                portal=portal,
                require_credentials=True
            )

            if form.is_valid():
                username = form.cleaned_data['username']
                password = form.cleaned_data['password']

                # Test connection
                logger.debug(f"Testing auth for {portal.alias} with user {username[0] if username else ''}...")
                auth_payload = {'username': username, 'password': password, 'url': portal.url}
                auth_result = utils.try_connection(auth_payload)

                # When credentials fail during form submission
                if not auth_result.get("authenticated", False):
                    logger.warning(f"Auth failed for {portal.alias} during form submission.")
                    form.add_error('password',
                                   auth_result.get("error", "Authentication failed. Please verify credentials."))
                    context = {
                        "form": form,
                        "instance": portal,
                        "items": items_to_refresh,
                        "full_refresh": full_refresh,
                        "error_message": "Authentication failed. Please verify credentials."
                    }
                    response = render(request, "partials/portal_credentials_form.html", context)
                    # Retarget to the credentials modal container
                    response["HX-Retarget"] = "#credentials-form-container"
                    response["HX-Reswap"] = "innerHTML"
                    return response

                # Store credentials
                logger.info(f"Authenticated to {portal.alias} as {username}")

                # Update portal type if needed
                if portal.portal_type is None and auth_result.get("is_agol") is not None:
                    portal.portal_type = "agol" if auth_result["is_agol"] else "portal"
                    portal.save(update_fields=['portal_type'])

                credential_token = utils.CredentialManager.store_credentials(username, password)
                if not credential_token:
                    logger.error(f"Failed to store temporary credentials for {portal.alias}.")
                    form.add_error(None, "System error: Failed to secure credentials.")
                    context = {
                        "form": form,
                        "instance": portal,
                        "items": items_to_refresh,
                        "full_refresh": full_refresh,
                        "error_message": "System error: Failed to secure credentials."
                    }
                    return render(request, "portals/portal_credentials.html", context, status=200)

                logger.debug(f"Credential token generated for {portal.alias}: {credential_token[:8]}...")

            else:
                # Form validation failed
                logger.warning(f"Credential form validation failed for {portal.alias}: {form.errors}")
                context = {
                    "form": form,
                    "instance": portal,
                    "items": items_to_refresh,
                    "full_refresh": full_refresh,
                    "error_message": "Please correct the errors below."
                }
                response = render(request, "portals/portal_credentials.html", context, status=400)
                response["X-Error-Page"] = "true"
                return response
        else:
            # Need to show credential form
            logger.debug(f"Credentials required for {portal.alias}; rendering credential form.")
            form = PortalCredentialsForm(
                initial={
                    'instance': portal.alias,
                    'items': items_to_refresh,
                    'full_refresh': full_refresh
                },
                portal=portal,
                require_credentials=True
            )
            context = {
                "form": form,
                "instance": portal,
                "items": items_to_refresh,
                "full_refresh": full_refresh,
            }
            return render(request, "portals/portal_credentials.html", context)
    else:
        # Portal has stored credentials, proceed directly
        logger.debug(f"Portal '{portal.alias}' has stored credentials. Proceeding to task.")

    # Start the background job
    logger.debug(f"Queueing job for '{portal.alias}', items: '{items_to_refresh}'")

    try:
        task = task_func.enqueue(portal.alias, full_refresh, credential_token)
        logger.info(f"Started job '{task.id}' for '{items_to_refresh}' on portal '{portal.alias}'.")

        response_data = {
            "instance": portal.alias,
            "task_id": task.id,
            "task_name": task.name,
            "value": 0,
            "progress": task.progress_info,
            "refresh_control": _refresh_control_context(
                task, items=items_to_refresh, instance=portal.alias),
            "panel_statuses": _panel_status_contexts(
                task, items=items_to_refresh, instance=portal.alias),
        }
        response = render(request, "partials/progress_response.html", context=response_data)
        response["HX-Trigger"] = json.dumps({"closeModal": True})
        return response

    except Exception as e:
        logger.error(f"Failed to submit Celery task for portal '{portal.alias}': {e}", exc_info=True)
        if credential_token:
            utils.CredentialManager.delete_credentials(credential_token)
            logger.info(f"Cleaned up credential token due to task submission failure.")
        return HttpResponse(status=500, headers={
            "HX-Trigger-After-Settle": json.dumps({
                "showDangerAlert": "System error: Failed to start background refresh task."
            })
        })


REFRESH_TASKS = {
    "webmaps": update_webmaps,
    "services": update_services,
    "webapps": update_webapps,
    "users": update_users,
}

REFRESH_PANEL_BY_TASK_NAME = {t.name: items for items, t in REFRESH_TASKS.items()}

PANEL_TIMESTAMP_FIELDS = {
    "webmaps": "webmap_updated",
    "services": "service_updated",
    "webapps": "webapp_updated",
    "users": "user_updated",
    "layers": "service_updated",
}

PANELS_ALSO_REFRESHED = {"services": ("layers",)}


def active_refresh_job(portal_alias, items):
    """
    The queued or running refresh for one portal and one panel, if any.

    This is what decides whether the panel offers Refresh or Cancel. Reading it
    from the Job table rather than from whatever the browser last saw means a
    reloaded page is right too — otherwise the tab that started a refresh shows
    Cancel and every other view of the same portal offers to start a second one.
    """
    registered = REFRESH_TASKS.get(items)
    if not portal_alias or registered is None:
        return None
    return (Job.objects
            .filter(portal_alias=str(portal_alias), name=registered.name,
                    status__in=(Job.QUEUED, Job.RUNNING))
            .order_by("-queued_at")
            .first())


def _panel_status_contexts(job=None, items=None, instance=None):
    """
    Contexts for every panel_status line this job should update.

    Usually one, its own. A services refresh returns two, because it rebuilds
    the layers panel as well.

    Timestamps are read fresh rather than taken from whatever the page was
    rendered with: the refresh writes that column as its last act, so the poll
    reporting the job finished is the one that can show the time it just wrote.
    """
    if items is None and job is not None:
        items = REFRESH_PANEL_BY_TASK_NAME.get(job.name)
    if items not in PANEL_TIMESTAMP_FIELDS:
        return []

    portal = Portal.objects.filter(alias=str(instance)).first()

    def status(panel, phase):
        return {
            "items": panel,
            "last_updated": getattr(portal, PANEL_TIMESTAMP_FIELDS[panel], None),
            "phase": phase,
            "oob": True,
        }

    # Empty phase once the job is terminal, so the line falls back to the
    # timestamp instead of freezing on the phase it stopped in.
    contexts = [status(items, job.bar_description if job is not None else "")]

    for also in PANELS_ALSO_REFRESHED.get(items, ()):
        contexts.append(status(also, ""))

    return contexts


def _refresh_control_context(job=None, items=None, instance=None):
    """
    Context for partials/portal_refresh_control.html, or None if there is none.

    Returns None when the job belongs to no panel — "Update All" and the tools
    have progress bars but no button of their own to flip.
    """
    if items is None and job is not None:
        items = REFRESH_PANEL_BY_TASK_NAME.get(job.name)
    if items is None:
        return None
    return {
        "items": items,
        "instance": instance if instance is not None else (job.portal_alias if job else ""),
        "active_job": job if (job is not None and job.is_active) else None,
        "with_modes": items != "users",
        "oob": True,
    }


@staff_member_required
def progress_view(request, instance, task_id):
    """
    Retrieve and display task progress information.

    Shows current status and progress of background tasks.

    :param request: The HTTP request object
    :type request: HttpRequest
    :param instance: Portal instance alias
    :type instance: str
    :param task_id: ID of the task to monitor
    :type task_id: str

    :return: JSON or HTML response with task progress data
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, task_id={task_id}")
    try:
        try:
            job = Job.objects.get(pk=task_id)
        except (Job.DoesNotExist, ValueError, ValidationError):
            logger.warning(f"No job found for '{task_id}'")
            return HttpResponse(status=404, headers={"HX-Trigger-After-Settle": json.dumps(
                {"showDangerAlert": "That background job is no longer available."})})

        task_name = job.name
        progress_percentage = job.progress_percent
        task_state = job.status

        response_data = {
            "instance": instance,
            "task_id": task_id,
            "task_name": task_name,
            "value": progress_percentage,
            "state": task_state,
            "progress": job.progress_info,
            "refresh_control": _refresh_control_context(job, instance=instance),
            "panel_statuses": _panel_status_contexts(job, instance=instance),
        }

        logger.debug(f"Task '{task_id}' state '{task_state}', progress {progress_percentage}%.")

        htmx_trigger = {}
        if task_state in (Job.SUCCESS, Job.WARNING):
            task_result = job.result
            has_errors = task_state == Job.WARNING

            logger.info(
                f"Task '{task_id}' ({task_name}) for {instance} completed.")
            success_details = "Completed."
            if isinstance(task_result, dict):
                # Check if task has errors
                if task_result.get('success') is False:
                    has_errors = True
                    error_messages = task_result.get('error_messages', [])
                    if error_messages:
                        success_details = f"Errors: {', '.join(error_messages)}"
                    else:
                        success_details = "Task completed with errors"

                    logger.warning(f"Task '{task_id}' completed with errors: {success_details}")

                # Handle data refresh task results
                elif 'num_updates' in task_result or 'num_inserts' in task_result:
                    success_details = (f"{task_result.get('num_updates', 0)} updates, "
                                       f"{task_result.get('num_inserts', 0)} inserts, "
                                       f"{task_result.get('num_deletes', 0)} deletes, "
                                       f"{task_result.get('num_errors', 0)} errors.")
                # Handle tool task results
                elif 'tool_name' in task_result:
                    details_parts = [f"Processed: {task_result.get('processed', 0)}"]

                    # Add actions taken
                    actions_taken = task_result.get('actions_taken', 0)
                    if actions_taken > 0:
                        details_parts.append(f"Actions Taken: {actions_taken}")

                    # Add warnings sent
                    warnings_sent = task_result.get('warnings_sent', 0)
                    if warnings_sent > 0:
                        details_parts.append(f"Warnings Sent: {warnings_sent}")

                    # Add tool-specific metrics from extra_metrics
                    extra_metrics = task_result.get('extra_metrics', {})
                    if 'inactive_found' in extra_metrics:
                        details_parts.append(f"Inactive Found: {extra_metrics['inactive_found']}")
                    if 'unshared' in extra_metrics:
                        details_parts.append(f"Items Unshared: {extra_metrics['unshared']}")

                    # Add errors last
                    errors = task_result.get('errors', 0)
                    details_parts.append(f"Errors: {errors}")

                    success_details = ", ".join(details_parts)
                if 'errors' in task_result and task_result['errors'] > 0:
                    has_errors = True

            if has_errors:
                htmx_trigger = {
                    "showWarningAlert": f"{task_name} for {instance} completed with errors.\n{success_details}",
                    "updateComplete": "true"
                }
            else:
                htmx_trigger = {
                    "showSuccessAlert": f"{task_name} for {instance} completed.\n{success_details}",
                    "updateComplete": "true"}
        elif task_state == Job.FAILURE:
            logger.warning(f"Task '{task_id}' ({task_name}) for {instance} failed")
            htmx_trigger = {
                "showDangerAlert": f"{task_name} for {instance} failed. Please check logs or results table for details.",
                "updateComplete": "true"}
        elif task_state == Job.CANCELED:
            logger.info(f"Task '{task_id}' ({task_name}) for {instance} was canceled")
            htmx_trigger = {
                "showWarningAlert": f"{task_name} for {instance} was canceled.",
                "updateComplete": "true"}

        response = render(request, "partials/progress_response.html", context=response_data)

        if htmx_trigger:
            response["HX-Trigger"] = json.dumps(htmx_trigger)
        return response

    except Exception as e:
        logger.error(f"Error retrieving progress for task '{task_id}': {e}", exc_info=True)
        return HttpResponse(status=500, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Failed to retrieve task progress."})})


@staff_member_required
@require_POST
def cancel_refresh_view(request, instance, task_id):
    """
    Ask a running refresh to stop, and hand back the panel's control.

    Only sets the flag. The job stops at its next checkpoint — between items or
    between batches, where the database is consistent — rather than being
    killed where it stands, so this returns "Cancelling…" and the progress bar's
    own poll reports the job as CANCELED a moment later.

    Nothing already written is undone. A refresh only ever writes what the
    portal currently says, so a half-finished one leaves the database further
    forward than it was, not inconsistent.

    :param request: The HTTP request object
    :type request: HttpRequest
    :param instance: Portal instance alias
    :type instance: str
    :param task_id: ID of the job to stop
    :type task_id: str
    :return: The re-rendered refresh control
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, task_id={task_id}, user={request.user.username}")
    try:
        job = Job.objects.get(pk=task_id)
    except (Job.DoesNotExist, ValueError, ValidationError):
        logger.warning(f"Cancel requested for unknown job '{task_id}'")
        return HttpResponse(status=404, headers={"HX-Trigger-After-Settle": json.dumps(
            {"showDangerAlert": "That background job is no longer available."})})

    context = _refresh_control_context(job, instance=instance)
    if context is None:
        logger.warning(f"Job '{task_id}' ({job.name}) has no refresh panel to cancel from")
        return HttpResponse(status=400, headers={"HX-Trigger-After-Settle": json.dumps(
            {"showDangerAlert": "That job cannot be canceled from here."})})

    # Rendered inline into the control's own slot, so no out-of-band swap.
    context["oob"] = False

    if request_cancel(job.pk):
        logger.info(f"User '{request.user.username}' canceled job '{job.name}' ({job.pk})")
        job.refresh_from_db()
        context["active_job"] = job if job.is_active else None
        trigger = {"showWarningAlert": f"Stopping {job.name} for {instance}. It will finish "
                                       f"the item it is on and stop."}
    else:
        logger.info(f"Job '{job.name}' ({job.pk}) had already finished; nothing to cancel")
        context["active_job"] = None
        trigger = {"showInfoAlert": f"{job.name} for {instance} had already finished."}

    response = render(request, "partials/portal_refresh_control.html", context=context)
    response["HX-Trigger-After-Settle"] = json.dumps(trigger)
    return response


@login_not_required
def login_view(request):
    """
    Handle user authentication.

    Processes login requests and authenticates users.

    :param request: The HTTP request object
    :type request: HttpRequest

    :return: Redirect to dashboard or login form with errors
    :rtype: HttpResponse
    """
    logger.debug(f"Method={request.method}")
    if request.user.is_authenticated:
        return redirect("/enterpriseviz/")

    form = AuthenticationForm(request, data=request.POST if request.method == "POST" else None)
    if request.method == "POST":
        attempted_username = (request.POST.get("username") or "").strip().lower()
        ip = client_ip(request)

        if login_limiter.is_blocked(attempted_username, ip):
            logger.warning(f"Login throttled for '{attempted_username}' from {ip}.")
            messages.error(request, "Too many failed sign-in attempts. Please try again later.")
            return render(request, "app/login.html",
                          {"form": form, "login_url": arcgis_org_url()},
                          status=429)

        if form.is_valid():
            username = form.cleaned_data["username"]
            user = form.get_user()

            if user is not None:
                login_limiter.reset(attempted_username, ip)
                login(request, user)
                messages.success(request, f"Welcome back, {username}!")
                logger.info(f"User '{username}' logged in successfully.")
                return redirect("/enterpriseviz/")
        else:
            login_limiter.record_failure(attempted_username, ip)
            logger.warning(f"Login form invalid: {form.errors.as_json()}")
            messages.error(request, "Invalid username or password.")

    return render(request, "app/login.html", {"form": form, "login_url": arcgis_org_url()})


@login_not_required
def logout_view(request):
    """
    Process user logout.

    Logs out the current user and redirects to login page.

    :param request: The HTTP request object
    :type request: HttpRequest

    :return: Redirect to login page
    :rtype: HttpResponseRedirect
    """
    username = request.user.username if request.user.is_authenticated else "Anonymous"
    logout(request)
    logger.info(f"User '{username}' logged out successfully.")
    return redirect("/enterpriseviz/")


@staff_member_required
@require_POST
def delete_portal_view(request, instance):
    """
    Handle deletion of a portal instance.

    Removes a portal and its associated data from the system.

    :param request: The HTTP request object
    :type request: HttpRequest
    :param instance: Portal instance alias
    :type instance: str

    :return: Success response or error message
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, user={request.user.username}")
    try:
        portal = Portal.objects.get(alias=instance)
        with transaction.atomic():
            portal.delete()
        logger.info(f"Deleted portal '{instance}' and related data successfully.")
        updated_portals = list(Portal.objects.values_list("alias", "portal_type", "url"))
        response = render(request, "partials/portal_updates.html", context={"portal": updated_portals})
        response["HX-Trigger-After-Settle"] = json.dumps(
            {"showSuccessAlert": f"Successfully deleted portal '{instance}' and related data."}
        )
        return response
    except Portal.DoesNotExist:
        logger.warning(f"Portal '{instance}' not found for deletion.")
        return HttpResponse(status=404, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Portal '{instance}' not found."})})
    except DatabaseError as e:
        logger.error(f"Database error deleting portal '{instance}': {e}", exc_info=True)
        return HttpResponse(status=500, headers={"HX-Trigger-After-Settle": json.dumps(
            {"showDangerAlert": f"Database error deleting portal '{instance}'."})})
    except Exception as e:
        logger.error(f"Unexpected error deleting portal '{instance}': {e}", exc_info=True)
        return HttpResponse(status=400, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Could not delete portal '{instance}'."})})


@staff_member_required
def update_portal_view(request, instance):
    """
    Update portal configuration.

    Processes form submissions to modify existing portal settings.

    :param request: The HTTP request object
    :type request: HttpRequest
    :param instance: Portal instance alias
    :type instance: str

    :return: Rendered response with form or success message
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, method={request.method}, user={request.user.username}")
    try:
        portal = Portal.objects.get(alias=instance)
    except Portal.DoesNotExist:
        logger.warning(f"Portal '{instance}' not found for update.")
        return HttpResponse(status=404, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Portal '{instance}' not found."})})

    if request.method == "POST":
        form = PortalCreateForm(request.POST, instance=portal)
        if form.is_valid():
            logger.debug("Form valid.")

            requires_auth_check = form.stored_credentials_need_check()

            missing_update_privileges = []
            if requires_auth_check:
                logger.debug(f"Checking connection for {portal.alias} with new credentials.")
                auth_result = utils.try_connection(form.cleaned_data)
                missing_update_privileges = auth_result.get("missing_privileges") or []
                if not auth_result.get("authenticated", False):
                    username = form.cleaned_data.get("username", "N/A")
                    url = form.cleaned_data.get("url", portal.url)
                    logger.warning(f"Auth failed for {instance} ({url}) as {username}.")
                    response = render(request, "partials/portal_update_form.html",
                                      {"form": form, "instance": instance, "portal": portal,
                                       "oauth_redirect_uri": _oauth_redirect_uri(request)}, status=401)
                    response["X-Error-Page"] = "true"
                    response["HX-Trigger-After-Settle"] = json.dumps(
                        {"showDangerAlert": f"Unable to connect to {url} as {username}."})
                    return response
                logger.info(f"Auth successful for {instance} with new credentials.")

            form.save()
            new_alias = form.cleaned_data['alias']
            logger.info(f"Portal '{instance}' (now '{new_alias}') updated successfully.")

            updated_portals = Portal.objects.values_list("alias", "portal_type", "url")
            response = render(request, "partials/portal_updates.html",
                              context={"portal": updated_portals, "instance": new_alias})
            if missing_update_privileges:
                triggers = {
                    "showWarningAlert": _privilege_warning(form.cleaned_data.get("username", "N/A"),
                                                          missing_update_privileges),
                    "closeModal": True}
            else:
                success_message = f"Successfully updated portal '{new_alias}'."
                if requires_auth_check: success_message += " Authentication updated."
                triggers = {"showSuccessAlert": success_message, "closeModal": True}

            response["HX-Trigger-After-Settle"] = json.dumps(triggers)
            return response
        else:
            logger.warning(f"Form invalid. Errors: {form.errors}")
            response = render(request, "partials/portal_update_form.html",
                              {"form": form, "instance": instance, "portal": portal,
                               "oauth_redirect_uri": _oauth_redirect_uri(request)}, status=400)
            response["X-Error-Page"] = "true"
            response["HX-Trigger-After-Settle"] = json.dumps(
                {"showDangerAlert": _form_error_alert(form)})
            return response

    form = PortalCreateForm(instance=portal)
    return render(request, "portals/portal_update.html",
                  {"form": form, "instance": instance, "portal": portal,
                   "oauth_redirect_uri": _oauth_redirect_uri(request)})


@staff_member_required
def schedule_task_view(request, instance):
    """
    Schedule recurring portal refresh tasks.

    Sets up periodic synchronization of portal data.

    :param request: The HTTP request object
    :type request: HttpRequest
    :param instance: Portal instance alias
    :type instance: str

    :return: Schedule configuration form or success message
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, method={request.method}, user={request.user.username}")

    try:
        portal = Portal.objects.get(alias=instance)
    except Portal.DoesNotExist:
        logger.warning(f"Portal '{instance}' not found.")
        return HttpResponse(status=404, headers={"HX-Trigger-After-Settle": json.dumps(
            {"showDangerAlert": f"Portal '{instance}' not found.", "closeModal": True})})

    results = Job.objects.filter(portal_alias=instance).order_by('-queued_at')[:10]

    if request.method == "DELETE":
        logger.debug(f"Processing DELETE for portal '{instance}' task.")
        try:
            if portal.task:
                portal.task.delete()
                portal.task = None
                portal.save(update_fields=['task'])
                logger.info(f"Deleted scheduled task for portal '{instance}'.")
                form = ScheduleForm(initial={"instance": portal.alias})
                response = render(request, "partials/portal_schedule_form.html",
                                  {"form": form, "enable": utils.portal_has_credentials(portal), "description": ""})
                response["HX-Trigger-After-Settle"] = json.dumps(
                    {"showSuccessAlert": "Scheduled task removed.", "closeModal": True})
                return response
            else:
                return HttpResponse(status=204)
        except Exception as e:
            logger.error(f"Error deleting task for '{instance}': {e}", exc_info=True)
            form = ScheduleForm(initial={"instance": portal.alias})
            response = render(request, "partials/portal_schedule_form.html",
                              {"form": form, "enable": utils.portal_has_credentials(portal),
                               "description": ""})
            response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Failed to delete scheduled task."})
            return response

    description = ""
    initial_form_data = {"instance": portal.alias}
    if portal.task:
        task = portal.task
        crontab = task.crontab
        cron_expression = f"{crontab.minute} {crontab.hour} {crontab.day_of_month} {crontab.month_of_year} {crontab.day_of_week}"
        try:
            description = cron_descriptor.get_description(cron_expression)
            if task.expires: description += f" until {task.expires.strftime('%Y-%m-%d')}"
        except Exception as e:
            logger.warning(f"Could not generate cron description for '{cron_expression}': {e}")
            description = "Custom schedule set."

        try:
            initial_form_data = json.loads(task.kwargs) if task.kwargs else {}
            initial_form_data["instance"] = portal.alias

            # If the form expects crontab parts separately and they are not in task.kwargs:
            if hasattr(task, 'crontab') and task.crontab:
                initial_form_data.update({
                    'minute': task.crontab.minute,
                    'hour': task.crontab.hour,
                    'day_of_month': task.crontab.day_of_month,
                    'month_of_year': task.crontab.month_of_year,
                    'day_of_week': task.crontab.day_of_week,
                })
            if hasattr(task, 'interval') and task.interval:
                initial_form_data['interval_schedule'] = task.interval
            if hasattr(task, 'start_time') and task.start_time:
                initial_form_data['start_time'] = task.start_time
            if hasattr(task, 'expires') and task.expires:
                initial_form_data['one_off'] = True
                initial_form_data['expires_on'] = task.expires

        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Invalid task.kwargs for portal '{instance}': {e}. Using defaults.")
            initial_form_data = {"instance": portal.alias}

    form = ScheduleForm(request.POST or None, initial=initial_form_data)

    if request.method == "POST":
        if form.is_valid():
            logger.debug("ScheduleForm is valid.")
            task_instance, error = form.save_task(portal=portal)
            if error:
                logger.warning(f"Error scheduling task for '{instance}': {error}")
                response = render(request, "partials/portal_schedule_form.html",
                                  {"form": form, "enable": utils.portal_has_credentials(portal), "description": description,
                                   "results": results, "instance_alias": portal.alias})
                response["HX-Trigger-After-Settle"] = json.dumps(
                    {"showDangerAlert": f"Error scheduling task: {error}"})
                return response

            new_crontab = task_instance.crontab
            new_cron_exp = f"{new_crontab.minute} {new_crontab.hour} {new_crontab.day_of_month} {new_crontab.month_of_year} {new_crontab.day_of_week}"
            try:
                description = cron_descriptor.get_description(new_cron_exp)
                if task_instance.expires: description += f" until {task_instance.expires.strftime('%Y-%m-%d')}"
            except Exception:
                description = "Custom schedule updated."

            logger.info(f"Task scheduled for portal '{instance}': {description}")
            response = render(request, "partials/portal_schedule_form.html",
                              {"form": form, "enable": utils.portal_has_credentials(portal), "description": description,
                               "results": results, "instance_alias": portal.alias})
            response["HX-Trigger-After-Settle"] = json.dumps(
                {"showSuccessAlert": f"Updates scheduled for '{instance}': {description}", "closeModal": True}
            )
            return response
        else:
            logger.warning(f"schedule_task_view: Form invalid for '{instance}'. Errors: {form.errors}")
            response = render(request, "partials/portal_schedule_form.html",
                              {"form": form, "enable": utils.portal_has_credentials(portal), "description": description,
                               "results": results, "instance_alias": portal.alias})
            return response

    context = {"form": form, "description": description, "results": results, "enable": utils.portal_has_credentials(portal),
               "instance_alias": portal.alias}
    return render(request, "portals/portal_schedule.html", context)


@login_required
def duplicates_view(request, instance):
    """
    Display duplicate item information.

    Returns lists of webmaps, services, and webapps with duplicate names based on fuzzywuzzy matching.

    :param request: The HTTP request object
    :type request: HttpRequest
    :param instance: Portal instance alias
    :type instance: str

    :return: Rendered template with duplicate layer data
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, user={request.user.username}")
    template_name = "portals/portal_duplicates.html" if request.htmx else "portals/portal_duplicates_full.html"

    try:
        portal_instance_obj = Portal.objects.get(alias=instance)
    except Portal.DoesNotExist:
        logger.warning(f"Portal '{instance}' not found.")
        return render_error(request, 404, "Portal not found.")

    similarity_threshold = 70
    logger.debug(f"Fetching duplicates for '{instance}' with threshold {similarity_threshold}%.")

    try:
        duplicates_data = utils.get_duplicates(portal_instance_obj, similarity_threshold=similarity_threshold)
        if "error" in duplicates_data:
            logger.warning(
                f"Error from get_duplicates for '{instance}': {duplicates_data['error']}")
            return render_error(request, 500, duplicates_data['error'])

        context = {
            "duplicate_webmaps": duplicates_data.get("webmaps", []),
            "duplicate_services": duplicates_data.get("services", []),
            "duplicate_layers": duplicates_data.get("layers", []),
            "duplicate_apps": duplicates_data.get("apps", []),
            "instance_alias": instance,
        }
        logger.debug(f"Rendering duplicates for '{instance}'.")
        return render(request, template_name, context)
    except Exception as e:
        logger.error(f"Error processing duplicates for '{instance}': {e}", exc_info=True)
        return render_error(request, 500, "Error processing duplicates.")


@login_required
@require_POST
def layerid_view(request, instance):
    """
    Display usage information for a specific layer id of a service.

    :param request: The HTTP request object
    :type request: HttpRequest
    :param instance: Portal instance alias
    :type instance: str

    :return: Rendered template with layer ID data
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, user={request.user.username}")
    template_name = "portals/portal_detail_layerid.html" if request.htmx else "portals/portal_detail_layerid_full.html"

    try:
        portal = get_object_or_404(Portal, alias=instance)
    except Http404:
        logger.warning(f"Portal instance '{instance}' not found.")
        return render_error(request, 404, f"Portal '{instance}' not found.")

    maps_found, apps_found, layer_url_searched = [], [], None

    if "layerId_input" in request.POST:
        layer_url_searched = unquote(request.POST.get("layerId_input", "").strip())
        logger.debug(f"Processing layer URL input: {layer_url_searched}")

        if not layer_url_searched:
            logger.debug("Empty layerId_input provided.")
            return HttpResponse(headers={"HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Provide a layer URL."})})

        try:
            layer_usage_result = utils.service_layer_details(portal, layer_url_searched)
            if "error" in layer_usage_result:
                error_msg = layer_usage_result["error"]
                logger.warning(f"Error from find_layer_usage for '{layer_url_searched}': {error_msg}")
                return HttpResponse(headers={"HX-Trigger-After-Settle": json.dumps(
                    {"showDangerAlert": error_msg})})

            maps_found = layer_usage_result.get("maps", [])
            apps_found = layer_usage_result.get("apps", [])
            logger.debug(
                f"Found {len(maps_found)} maps, {len(apps_found)} apps for layer '{layer_url_searched}'.")

        except Exception as e:
            logger.error(f"Error getting layer usage for '{layer_url_searched}': {e}",
                         exc_info=True)
            return HttpResponse(
                headers={"HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Error getting layer usage."})})
    else:
        logger.debug("'layerId_input' not in POST.")

    context = {
        "portal_list": Portal.objects.values("alias", "portal_type", "url"),
        "current_portal": portal,
        "instance_alias": instance,
        "maps": maps_found,
        "apps": apps_found,
        "url_searched": layer_url_searched
    }
    return render(request, template_name, context)


@login_required
@require_POST
def metadata_view(request, instance):
    """
    Display metadata completeness for Portal items (maps, services, layers, apps).

    Item description, snippet, access, tags, and thumbnail account for the completeness score.

    :param request: The HTTP request object
    :type request: HttpRequest
    :param instance: Portal instance alias
    :type instance: str

    :return: Rendered template with metadata
    :rtype: HttpResponse
    """
    logger.debug(f"instance={instance}, user={request.user.username}")
    template_name = "portals/portal_metadata.html" if request.htmx else "portals/portal_metadata_full.html"

    try:
        portal = get_object_or_404(Portal, alias=instance)
    except Http404:
        logger.warning(f"Portal '{instance}' not found.")
        return render_error(request, 404, "Portal instance not found.")

    # Determine if credentials are needed
    if _needs_oauth_connect(portal):
        return _render_oauth_connect_prompt(request, portal)

    credentials_required = portal.requires_interactive_credentials
    credential_token = None

    # Check if this is a credential form submission
    is_credential_submission = 'username' in request.POST or 'password' in request.POST

    if credentials_required:
        if is_credential_submission:
            # Validate credential form submission
            logger.debug(f"Processing credential form submission for portal '{portal.alias}'.")
            form = PortalCredentialsForm(
                request.POST,
                portal=portal,
                require_credentials=True
            )

            if form.is_valid():
                username = form.cleaned_data['username']
                password = form.cleaned_data['password']

                # Test connection
                logger.debug(f"Testing auth for {portal.alias} with user {username[0] if username else ''}...")
                auth_payload = {'username': username, 'password': password, 'url': portal.url}
                auth_result = utils.try_connection(auth_payload)

                if not auth_result.get("authenticated", False):
                    logger.warning(f"Auth failed for {portal.alias} during form submission.")
                    form.add_error('password',
                                   auth_result.get("error", "Authentication failed. Please verify credentials."))
                    context = {
                        "form": form,
                        "instance": portal,
                        "items": "metadata",
                        "error_message": "Authentication failed. Please verify credentials."
                    }
                    return render(request, "portals/portal_credentials.html", context, status=200)

                # Store credentials
                logger.info(f"Authenticated to {portal.alias} as {username}")

                # Update portal type if needed
                if portal.portal_type is None and auth_result.get("is_agol") is not None:
                    portal.portal_type = "agol" if auth_result["is_agol"] else "portal"
                    portal.save(update_fields=['portal_type'])

                credential_token = utils.CredentialManager.store_credentials(username, password)
                if not credential_token:
                    logger.error(f"Failed to store temporary credentials for {portal.alias}.")
                    form.add_error(None, "System error: Failed to secure credentials.")
                    context = {
                        "form": form,
                        "instance": portal,
                        "items": "metadata",
                        "error_message": "System error: Failed to secure credentials."
                    }
                    return render(request, "portals/portal_credentials.html", context, status=200)

                logger.debug(f"Credential token generated for {portal.alias}: {credential_token[:8]}...")

            else:
                # Form validation failed
                logger.warning(f"Credential form validation failed for {portal.alias}: {form.errors}")
                context = {
                    "form": form,
                    "instance": portal,
                    "items": "metadata",
                    "error_message": "Please correct the errors below."
                }
                return render(request, "portals/portal_credentials.html", context, status=200)
        else:
            # Need to show credential form
            logger.debug(f"Credentials required for {portal.alias}; rendering credential form.")
            form = PortalCredentialsForm(
                initial={
                    'instance': portal.alias,
                    'items': "metadata"
                },
                portal=portal,
                require_credentials=True
            )
            context = {
                "form": form,
                "instance": portal,
                "items": "metadata",
            }
            return render(request, "portals/portal_credentials.html", context)
    else:
        # Portal has stored credentials, proceed directly
        logger.debug(f"Portal '{portal.alias}' has stored credentials. Proceeding to fetch metadata.")

    # Fetch metadata report
    try:
        logger.debug(f"metadata_view: Fetching metadata for {instance}.")

        metadata_report_data = utils.get_metadata(portal, credential_token)

        if "error" in metadata_report_data:
            error_msg = metadata_report_data["error"]
            logger.warning(f"Error from get_metadata for '{instance}': {error_msg}")
            return HttpResponse(status=200, headers={
                "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": error_msg})
            })

        metadata_rows = metadata_report_data.get("metadata", [])
        context = {
            "portal_list": Portal.objects.values_list("alias", "portal_type", "url"),
            "current_portal": portal,
            "instance_alias": instance,
            "metadata": metadata_rows,
        }

        cache.set(reports.metadata_cache_key(request.user.pk, instance), metadata_rows,
                  timeout=reports.METADATA_REPORT_TTL_SECONDS)

        logger.debug(f"Rendering metadata for '{instance}'.")
        response = render(request, template_name, context)

        # Close modal if credentials were provided or form was submitted
        if credential_token or (credentials_required and is_credential_submission):
            response["HX-Trigger"] = json.dumps({"closeModal": True})

        return response

    except Exception as e:
        logger.error(f"Error processing metadata for '{instance}': {e}", exc_info=True)
        # Clean up credential token if an error occurred
        if credential_token:
            utils.CredentialManager.delete_credentials(credential_token)
            logger.info("Cleaned up credential token due to metadata processing error.")
        return HttpResponse(status=500, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Error getting metadata report."})
        })


@login_required
@require_POST
def mode_toggle_view(request):
    """
    Toggle between 'light' and 'dark' mode for the user's profile.

    Returns:
    - JsonResponse: JSON object containing the updated mode and rendered switch UI.
    """
    logger.debug(f"user={request.user.username}")
    try:
        user_profile = get_object_or_404(UserProfile, user=request.user)
        user_profile.mode = "dark" if user_profile.mode == "light" else "light"
        user_profile.save(update_fields=['mode'])
        logger.info(f"User '{request.user.username}' toggled display mode to '{user_profile.mode}'.")
        switch_html = loader.render_to_string("partials/mode_switch.html", {"mode": user_profile.mode}, request)
        return JsonResponse({"mode": user_profile.mode, "switch_html": switch_html})
    except Http404:
        logger.warning(f"UserProfile not found for {request.user.username}.")
        return JsonResponse({"error": "User profile not found"}, status=404)
    except Exception as e:
        logger.error(f"Error for {request.user.username}: {e}", exc_info=True)
        return JsonResponse({"error": "Failed to toggle mode."}, status=500)


@login_required
@require_POST
def usage_toggle_view(request):
    """
    Toggle the service usage setting for the logged-in user's profile.

    Returns:
    - JsonResponse: JSON object containing the updated state and rendered switch UI.
    """
    logger.debug(f"user={request.user.username}")
    try:
        user_profile = get_object_or_404(UserProfile, user=request.user)
        user_profile.service_usage = not user_profile.service_usage
        user_profile.save(update_fields=['service_usage'])
        logger.info(f"User '{request.user.username}' toggled service usage to '{user_profile.service_usage}'.")
        switch_html = loader.render_to_string("partials/usage_switch.html",
                                              {"service_usage": user_profile.service_usage}, request)
        return JsonResponse({"service_usage": user_profile.service_usage, "switch_html": switch_html})
    except Http404:
        logger.warning(f"UserProfile not found for {request.user.username}.")
        return JsonResponse({"error": "User profile not found"}, status=404)
    except Exception as e:
        logger.error(f"Error for {request.user.username}: {e}", exc_info=True)
        return JsonResponse({"error": "Failed to toggle usage setting."}, status=500)


@require_POST
@csrf_exempt
@login_not_required
def webhook_view(request):
    """
    Handle incoming organization webhook requests and process their events.

    Authenticates against the secret configured in SiteSettings, which
    organization webhooks send in a request header — see
    utils.validate_webhook_secret.
    https://enterprise.arcgis.com/en/portal/11.5/administer/windows/webhook-payloads.htm

    Expected Payload Structure:
    {
        "info": {
            "portalURL": "https://example.com"
        },
        "events": [
            {
                "source": "item",
                "operation": "update",
                "id": "12345"
            }
        ]
    }

    The events themselves are handled by the "Process webhook" job, not here.
    Everything that talks to the portal — connecting, and the per-event lookup
    that decides which item task to run — used to happen in this request, which
    held one of the web server's threads and its transaction for as long as the
    portal took to answer. This is the only endpoint reachable without a
    session, so it is the worst place to do that.

    A 200 therefore means the payload was authenticated and queued, not that
    every event succeeded. Processing failures show up in the job history and
    the logs.

    Note that webhook_limiter counts *failed* attempts, so a caller holding the
    secret is not rate limited.

    Returns:
    - `200 OK` if the webhook is authenticated and queued.
    - `403 Forbidden` if the secret header is missing or does not match.
    - `400 Bad Request` for malformed JSON payloads.
    - `404 Not Found` if the portal is unknown.
    - `429 Too Many Requests` after repeated authentication failures.
    """
    logger.debug("Received webhook request.")

    # Validate webhook secret
    ip = client_ip(request)
    if webhook_limiter.is_blocked(ip):
        logger.warning(f"Webhook rejected: too many failed attempts from {ip}.")
        return HttpResponse(status=429)

    if not utils.validate_webhook_secret(request):
        webhook_limiter.record_failure(ip)
        return HttpResponseForbidden()

    webhook_limiter.reset(ip)

    # Parse and validate payload
    try:
        payload = json.loads(request.body)
    except json.JSONDecodeError:
        logger.warning("Invalid JSON payload.")
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    webhook_registry = payload.get("WebhookRegistry", None)
    if webhook_registry:
        return JsonResponse({"message": "Webhook registry received."}, status=200)

    portal_url = payload.get("info", {}).get("portalURL")
    if not portal_url:
        logger.warning("Missing 'portalURL' in payload.")
        return JsonResponse({"error": "Missing portal URL"}, status=400)

    # Get portal instance
    portal_instance = utils.get_portal_instance(portal_url)
    if not portal_instance:
        logger.warning(f"Unknown portal URL received: {portal_url}")
        return JsonResponse({"error": "Unknown portal"}, status=404)

    events = payload.get("events", [])
    if not events:
        logger.debug(f"Webhook payload for {portal_url} carried no events.")
        return JsonResponse({"message": "Webhook received successfully"}, status=200)

    job = process_webhook.enqueue(portal_instance.alias, events)

    logger.info(f"Queued job {job.pk} for {len(events)} webhook event(s) from {portal_url}.")
    return JsonResponse({"message": "Webhook received successfully"}, status=200)


@staff_member_required
def log_settings_view(request):
    """
    Configures the application-wide logging level and the retention windows
    for log entries, finished background jobs and Replace Service backups.

    On GET, displays the current values. On POST, saves them, applies the new
    level to the current Django process, and queues a job so the worker
    process picks it up too. The retention windows need no such signal — the
    purges read them when they next run.

    :param request: The HTTP request object.
    :type request: django.http.HttpRequest
    :return: Rendered HTML response (log settings dialog) or an HX-Trigger response.
    :rtype: django.http.HttpResponse
    """
    logger.debug(f"Method={request.method}, user={request.user.username}")
    site_settings = SiteSettings.load()

    if request.method == "POST":
        form = LoggingAndRetentionForm(request.POST, instance=site_settings)
        if not form.is_valid():
            logger.warning(f"Invalid log settings submitted: {form.errors.as_json()}")
            return render(request, 'partials/log_settings.html', {'form': form})

        try:
            form.save()
            new_level_str = form.cleaned_data["logging_level"]
            logger.info(f"SiteSettings updated: logging level {new_level_str}, "
                        f"log retention {form.cleaned_data['log_retention_days']} day(s), "
                        f"job retention {form.cleaned_data['job_retention_days']} day(s), "
                        f"replacement backup retention "
                        f"{form.cleaned_data['replacement_backup_retention_days']} day(s).")

            utils.apply_global_log_level(level_name=new_level_str)
            logger.info(f"Log level '{new_level_str}' applied to current Django process.")

            apply_site_log_level_in_worker.enqueue()
            logger.info("Queued a job to apply the new log level in the worker.")

            return HttpResponse(headers={"HX-Trigger-After-Settle": json.dumps(
                {"showSuccessAlert": "Log settings saved.", "closeModal": True}
            )})
        except Exception as e:
            logger.error(f"Error updating log settings: {e}", exc_info=True)
            return HttpResponse(status=500, headers={
                "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Error: {str(e)}"})})

    return render(request, 'partials/log_settings.html',
                  {'form': LoggingAndRetentionForm(instance=site_settings)})


@superuser_required
def email_settings(request):
    """
    Manages application-wide email configuration settings.

    On GET, displays the SiteSettingsForm with current email settings.
    On POST, handles two actions:
        - 'save': Validates and saves the email configuration from the form.
        - 'test_email': Sends a test email using the (potentially unsaved)
          configuration data in the form to a specified test email address.

    Expected POST parameters for 'test_email' action:
        - 'test_email': The email address to send the test email to.
    All other POST data is expected to match SiteSettingsForm fields.

    :param request: The HTTP request object.
    :type request: django.http.HttpRequest
    :return: Rendered HTML response, typically the email settings form or a partial.
    :rtype: django.http.HttpResponse
    """
    logger.debug(f"Method={request.method}, user={request.user.username}")
    settings_instance = SiteSettings.load()

    if request.method == "POST":
        form = SiteSettingsForm(request.POST, instance=settings_instance)
        action = request.POST.get("action")
        logger.debug(f"POST action='{action}'")

        if form.is_valid():
            logger.debug("Form valid.")
            if action == "save":
                try:
                    form.save()
                    logger.info("Email configuration saved successfully.")
                    response = render(request, "partials/portal_email_form.html", {"form": form})
                    response["HX-Trigger-After-Settle"] = json.dumps(
                        {"showSuccessAlert": "Email configuration saved.", "closeModal": True}
                    )
                    return response
                except Exception as e:
                    logger.error(f"Error saving config: {e}", exc_info=True)
                    response = render(request, "partials/portal_email_form.html", {"form": form})
                    response["HX-Trigger-After-Settle"] = json.dumps(
                        {"showDangerAlert": f"Failed to save: {str(e)}"})
                    return response

            elif action == "test_email":
                test_email_addr = request.POST.get("test_email", "").strip()
                if not test_email_addr:
                    logger.warning("Test email address missing.")
                    return HttpResponse(status=200, headers={
                        "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Test email address required."})})

                config = form.cleaned_data
                if not config.get("email_host") or not config.get("email_port"):
                    return HttpResponse(status=200, headers={
                        "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Host and Port required."})})

                # Already resolved by clean_email_password when left blank.
                email_password = config.get("email_password")

                try:
                    logger.debug(
                        f"Sending test email to {test_email_addr} using host {config['email_host']}.")
                    connection_kwargs = {
                        "host": config["email_host"], "port": config["email_port"],
                        "username": config.get("email_username") or None,
                        "password": email_password or None,
                        "use_tls": (config.get("email_encryption") == "starttls"),
                        "use_ssl": (config.get("email_encryption") == "ssl"),
                        "fail_silently": False,
                    }
                    with get_connection(**connection_kwargs) as connection:
                        email = EmailMessage(
                            subject="Enterpriseviz Test Email",
                            body="This is a test email from Enterpriseviz. Your email configuration appears to be working.",
                            from_email=config["from_email"],
                            to=[test_email_addr],
                            reply_to=[config.get("reply_to") or config["from_email"]],
                            connection=connection
                        )
                        email.send()
                    logger.info(f"Test email sent successfully to {test_email_addr}.")
                    return HttpResponse(headers={"HX-Trigger-After-Settle": json.dumps(
                        {"showSuccessAlert": f"Test email sent to {test_email_addr}."})})
                except Exception as e:
                    logger.error(f"Failed to send test email: {e}", exc_info=True)
                    return HttpResponse(status=200, headers={
                        "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Test email failed: {str(e)}"})})
            else:
                logger.warning(f"Unknown POST action '{action}'.")
        else:
            logger.warning(f"Form invalid. Errors: {form.errors}")
            return render(request, "partials/portal_email_form.html", {"form": form})

    else:
        form = SiteSettingsForm(instance=settings_instance)

    template_to_render = "portals/portal_email.html" if request.method == "GET" else "partials/portal_email_form.html"
    return render(request, template_to_render, {"form": form})


@require_POST
@staff_member_required
def notify_view(request):
    """
    Sends email notifications to owners of selected maps and apps about an upcoming change.

    Accepts a POST request with the description of the change and comma-separated
    IDs of selected maps and apps. Groups items by owner and sends one email per owner.
    Requires email settings to be configured in SiteSettings.

    Expected POST parameters:
        - 'change': Description of the upcoming change.
        - 'selected_maps': Comma-separated string of Webmap IDs.
        - 'selected_apps': Comma-separated string of App IDs.

    :param request: The HTTP request object.
    :type request: django.http.HttpRequest
    :return: HTTP response, typically empty with HTMX triggers for alerts/modal closure.
    :rtype: django.http.HttpResponse
    """
    logger.debug(f"user={request.user.username}")
    change_item_description = request.POST.get("change", "related GIS resources")
    map_ids_str = request.POST.get("selected_maps", "")
    app_ids_str = request.POST.get("selected_apps", "")

    logger.debug(f"Change='{change_item_description}', Maps='{map_ids_str}', Apps='{app_ids_str}'")

    # Check email configuration
    site_settings = SiteSettings.load()
    if not site_settings.email_host:
        logger.error("Email settings not configured.")
        return HttpResponse(status=200, headers={
            "HX-Retarget": "#notification-form-container",
            "HX-Reswap": "innerHTML",
            "HX-Trigger-After-Settle": json.dumps({
                "showDangerAlert": "Email settings not configured. Please contact administrator."
            })
        })

    # Parse and validate selections
    selected_maps_qs = Webmap.objects.none()
    if map_ids_str:
        map_ids_list = [map_id.strip() for map_id in map_ids_str.split(',') if map_id.strip()]
        if map_ids_list:
            selected_maps_qs = Webmap.objects.filter(webmap_id__in=map_ids_list).select_related("webmap_owner")

    selected_apps_qs = App.objects.none()
    if app_ids_str:
        app_ids_list = [app_id.strip() for app_id in app_ids_str.split(',') if app_id.strip()]
        if app_ids_list:
            selected_apps_qs = App.objects.filter(app_id__in=app_ids_list).select_related("app_owner")

    if not selected_maps_qs.exists() and not selected_apps_qs.exists():
        logger.debug("No maps or apps selected for notification.")
        return HttpResponse(status=200, headers={
            "HX-Retarget": "#notification-form-container",
            "HX-Trigger-After-Settle": json.dumps({
                "showWarningAlert": "No items were selected for notification."
            })
        })

    # Group items by owner
    owner_items_map = defaultdict(lambda: {"maps": [], "apps": []})
    for webmap_obj in selected_maps_qs:
        if webmap_obj.webmap_owner and webmap_obj.webmap_owner.user_email:
            owner_items_map[webmap_obj.webmap_owner]["maps"].append(webmap_obj)
    for app_obj in selected_apps_qs:
        if app_obj.app_owner and app_obj.app_owner.user_email:
            owner_items_map[app_obj.app_owner]["apps"].append(app_obj)

    if not owner_items_map:
        logger.warning("Selected items have no valid owners with email addresses.")
        return HttpResponse(status=200, headers={
            "HX-Retarget": "#notification-form-container",
            "HX-Trigger-After-Settle": json.dumps({
                "showWarningAlert": "Selected items have no valid owners with email addresses."
            })
        })

    # Send emails
    logger.info(f"Preparing to send notifications to {len(owner_items_map)} owners.")
    emails_sent_count = 0
    emails_failed_count = 0

    for owner_obj, owner_items in owner_items_map.items():
        try:
            owner_maps = owner_items["maps"]
            owner_apps = owner_items["apps"]
            message, html, subject = utils.format_notification_email(
                owner_obj, change_item_description, owner_maps, owner_apps
            )
            success, status_msg = utils.send_email(owner_obj.user_email, subject, message, html)
            if success:
                emails_sent_count += 1
            else:
                emails_failed_count += 1
                logger.error(f"Failed to send email to {owner_obj.user_email}: {status_msg}")
        except Exception as e:
            logger.error(f"Error sending email to {owner_obj.user_email}: {e}", exc_info=True)
            emails_failed_count += 1

    if emails_sent_count > 0 and emails_failed_count == 0:
        # Complete success
        response = HttpResponse("")
        response["HX-Trigger"] = json.dumps({
            "closeModal": True,
            "showSuccessAlert": f"{emails_sent_count} notification email(s) sent successfully."
        })
        return response

    elif emails_sent_count > 0 and emails_failed_count > 0:
        # Partial success. close modal but show warning
        response = HttpResponse("")
        response["HX-Trigger"] = json.dumps({
            "closeModal": True,
            "showWarningAlert": f"{emails_sent_count} sent, {emails_failed_count} failed. Check logs for details."
        })
        return response

    else:
        # Complete failure. keep modal open, show error
        logger.error(f"All email notifications failed. Sent: {emails_sent_count}, Failed: {emails_failed_count}")
        return HttpResponse(status=200, headers={
            "HX-Retarget": "#notification-form-container",
            "HX-Trigger-After-Settle": json.dumps({
                "showDangerAlert": f"Failed to send all notifications. Please check logs or contact administrator."
            })
        })


@staff_member_required
def tool_settings(request, instance):
    """
    Manages Portal Tools settings for a specific portal instance.

    These settings might control scheduled maintenance tasks or other utilities
    related to a portal.
    On GET, displays the ToolsForm with current settings for the instance.
    On POST, validates and saves the tool settings using the form's `save` method.

    :param request: The HTTP request object.
                    POST data expected to match ToolsForm fields.
    :type request: django.http.HttpRequest
    :param instance: The alias of the portal instance for which to manage tool settings.
    :type instance: str
    :return: Rendered HTML response, typically the tools settings form or a partial.
    :rtype: django.http.HttpResponse
    """
    logger.debug(f"instance={instance}, method={request.method}, user={request.user.username}")
    try:
        portal_instance_obj = get_object_or_404(Portal, alias=instance)
        tool_settings_obj, _ = PortalToolSettings.objects.get_or_create(portal=portal_instance_obj)
    except Http404:
        logger.warning(f"Portal '{instance}' not found.")
        return render_error(request, 404, "Portal not found.")
    except Exception as e:
        logger.error(f"Error getting/creating settings for '{instance}': {e}", exc_info=True)
        return HttpResponse(status=500, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Error loading tool settings."})})

    # Check prerequisites
    site_settings = SiteSettings.load()
    webhook_configured = bool(site_settings.webhook_secret)
    email_configured = bool(site_settings.email_host)

    results = Job.objects.filter(
        portal_alias=instance, name__icontains="tool"
    ).order_by('-queued_at')[:10]

    if request.method == "POST":
        # Block submission if email not configured (required for all tools)
        if not email_configured:
            form = ToolsForm(request.POST, instance=tool_settings_obj)
            context = {
                'form': form,
                'instance_alias': instance,
                'webhook_configured': webhook_configured,
                'email_configured': False
            }
            response = render(request, "partials/portal_tools_form.html", context, status=400)
            response["HX-Trigger-After-Settle"] = json.dumps({
                "showDangerAlert": "Cannot save: Email settings must be configured"
            })
            return response

        form = ToolsForm(request.POST, instance=tool_settings_obj)
        if form.is_valid():
            try:
                form.save(portal=portal_instance_obj)

                logger.info(f"Tool settings updated for portal '{instance}'.")
                context = {
                    'form': form,
                    'instance_alias': instance,
                    'webhook_configured': webhook_configured,
                    'email_configured': email_configured
                }
                response = render(request, "partials/portal_tools_form.html", context)
                response["HX-Trigger-After-Settle"] = json.dumps(
                    {"showSuccessAlert": "Tool settings saved.", "closeModal": True})
                return response
            except Exception as e:
                logger.error(f"Error saving tool settings for '{instance}': {e}", exc_info=True)
                context = {
                    'form': form,
                    'instance_alias': instance,
                    'webhook_configured': webhook_configured,
                    'email_configured': email_configured
                }
                response = render(request, "partials/portal_tools_form.html", context, status=500)
                response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": f"Error saving: {str(e)}"})
                return response
        else:
            logger.warning(f"Form invalid for '{instance}'. Errors: {form.errors}")
            context = {
                'form': form,
                'instance_alias': instance,
                'webhook_configured': webhook_configured,
                'email_configured': email_configured
            }
            response = render(request, "partials/portal_tools_form.html", context, status=400)
            response["X-Error-Page"] = "true"
            return response
    else:
        form = ToolsForm(instance=tool_settings_obj)

    context = {
        "form": form,
        "instance_alias": instance,
        "results": results,
        "webhook_configured": webhook_configured,
        "email_configured": email_configured
    }
    template_name = "portals/portal_tools.html"
    return render(request, template_name, context)


@staff_member_required
@require_POST
def tool_run(request, instance, tool_name):
    """
    Handles an on-demand request to run a single portal automation tool.

    Validates the submitted tool parameters from the ToolsForm and queues a
    Celery task to execute the specified tool, returning a progress bar to
    monitor the execution.
    """
    logger.debug(f"Received request to run tool '{tool_name}' for instance '{instance}'.")

    portal = get_object_or_404(Portal, alias=instance)

    if _needs_oauth_connect(portal):
        logger.warning(f"Tool '{tool_name}' blocked: portal '{portal.alias}' is not authorized.")
        return HttpResponse(status=200, headers={
            "HX-Trigger-After-Settle": json.dumps({
                "showDangerAlert": f"{portal.alias} is not authorized. Open the portal settings "
                                   f"and use Connect with ArcGIS before running tools."
            })
        })

    tool_settings_obj, _ = PortalToolSettings.objects.get_or_create(portal=portal)

    form = ToolsForm(request.POST, instance=tool_settings_obj)
    if not form.is_valid():
        error_msg = ". ".join([f"{field}: {', '.join(errors)}" for field, errors in form.errors.items()])
        logger.warning(f"Invalid parameters for tool '{tool_name}': {form.errors.as_json()}")
        return HttpResponse(status=200, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Invalid tool parameters: {error_msg}"})
        })

    site_settings = SiteSettings.load()

    # Check email configuration
    if not bool(site_settings.email_host):
        return HttpResponse(status=400, headers={
            "HX-Trigger-After-Settle": json.dumps({
                "showDangerAlert": "Cannot run tool. Email settings must be configured first."
            })
        })

    TOOL_CONFIG_MAP = {
        'pro_license': {
            'task': process_pro_license_task,
            'params': {
                'duration_days': 'tool_pro_duration',
                'warning_days': 'tool_pro_warning'
            },
            'name': 'ArcGIS Pro License'
        },
        'public_unshare': {
            'task': process_public_unshare_task,
            'params': {
                'score_threshold': 'tool_public_unshare_score'
            },
            'name': 'Public Item Unsharing'
        },
        'inactive_user': {
            'task': process_inactive_user_task,
            'params': {
                'duration_days': 'tool_inactive_user_duration',
                'warning_days': 'tool_inactive_user_warning',
                'action': 'tool_inactive_user_action'
            },
            'name': 'Inactive User'
        }
    }

    if tool_name not in TOOL_CONFIG_MAP:
        logger.error(f"Attempted to run unknown tool: '{tool_name}'")
        return HttpResponse(status=200, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Unknown tool specified: {tool_name}"})
        })

    config = TOOL_CONFIG_MAP[tool_name]
    tool_display_name = config.get('name', tool_name.replace('_', ' ').title())

    # Build task parameters
    task_params = {'portal_alias': instance}
    for param_name, form_field in config['params'].items():
        task_params[param_name] = form.cleaned_data[form_field]

    try:
        # Call the specific task for this tool
        task = config['task'].enqueue(**task_params)
        logger.info(f"Successfully queued tool '{tool_display_name}' for instance '{instance}'. Task ID: {task.id}")

        response_data = {
            "instance": instance,
            "task_id": task.id,
            "value": task.progress_percent,
            "progress": task.progress_info,
            "task_name": tool_display_name,
        }

        response = render(request, "partials/progress_response.html", context=response_data)
        return response

    except Exception as e:
        logger.error(f"Failed to queue tool '{tool_name}' for '{instance}': {e}", exc_info=True)
        return HttpResponse(status=200, headers={
            "HX-Trigger-After-Settle": json.dumps(
                {"showDangerAlert": "Failed to queue the tool. See logs for details."})
        })


@superuser_required
def arcgis_signin_settings(request):
    """
    Configures the ArcGIS OAuth application users sign in through.

    On GET, renders the current configuration. On POST, saves it. Nothing has
    to be signalled to a running process: config.customArcGIS reads the row
    when a sign-in happens, and the login page reads it when it renders, so a
    change applies to the next sign-in attempt.

    :param request: The HTTP request object.
    :type request: django.http.HttpRequest
    :return: Rendered HTML response (dialog or form partial), or an HX-Trigger response.
    :rtype: django.http.HttpResponse
    """
    logger.debug(f"Method={request.method}, user={request.user.username}")
    site_settings = SiteSettings.load()

    if request.method == "POST":
        form = ArcGISSignInForm(request.POST, instance=site_settings)
        if not form.is_valid():
            logger.warning(f"Invalid ArcGIS sign-in settings submitted: {form.errors.as_json()}")
            return render(request, 'partials/arcgis_signin_form.html', {'form': form})

        try:
            form.save()
            configured = bool(form.cleaned_data["arcgis_org_url"])
            logger.info(f"ArcGIS sign-in settings saved: organization "
                        f"'{form.cleaned_data['arcgis_org_url'] or 'not set'}', "
                        f"required role '{form.cleaned_data['arcgis_user_role'] or 'not set'}'.")

            message = ("ArcGIS sign-in settings saved." if configured
                       else "ArcGIS sign-in settings cleared. Only local accounts can sign in.")
            return HttpResponse(headers={"HX-Trigger-After-Settle": json.dumps(
                {"showSuccessAlert": message, "closeModal": True}
            )})
        except Exception as e:
            logger.error(f"Error saving ArcGIS sign-in settings: {e}", exc_info=True)
            return HttpResponse(status=500, headers={
                "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Error: {str(e)}"})})

    return render(request, 'portals/arcgis_signin.html', {
        'form': ArcGISSignInForm(instance=site_settings),
        'current_secret': bool(site_settings.arcgis_client_secret),
    })


@superuser_required
def webhook_settings_view(request):
    """Configure webhook settings for the site."""
    site_settings = SiteSettings.load()

    if request.method == "POST":
        action = request.POST.get("action")

        if action == "save":
            form = WebhookSettingsForm(request.POST, instance=site_settings)
            if form.is_valid():
                form.save()
                return HttpResponse(headers={"HX-Trigger-After-Settle": json.dumps(
                    {"showSuccessAlert": "Webhook settings saved successfully.", "closeModal": True}
                )})
            else:
                return render(request, 'partials/webhook_settings_form.html', {'form': form})

    elif request.method == "DELETE":
        site_settings.webhook_secret = ""
        site_settings.save(update_fields=['webhook_secret'])
        return HttpResponse(headers={"HX-Trigger-After-Settle": json.dumps(
            {"showSuccessAlert": "Webhook secret cleared.", "closeModal": True}
        )})

    # GET request - show form
    form = WebhookSettingsForm(instance=site_settings)
    return render(request, 'portals/webhook_settings.html', {
        'form': form,
        'current_secret': bool(site_settings.webhook_secret)
    })


def _render_replace_credentials(request, portal, error, post_url, button_label, action_label):
    """
    Renders the inline credential prompt for a replacement action, targeting
    the same container the triggering request targeted.
    """
    hx_target = request.headers.get("HX-Target") or "replace-dryrun-container"
    return render(request, "partials/replace_credentials.html", {
        "portal_url": portal.url,
        "error": error,
        "post_url": post_url,
        "button_label": button_label,
        "action_label": action_label,
        "target_selector": f"#{hx_target}",
    })


@staff_member_required
def replace_modal_view(request, instance, service_pk):
    """
    Renders the Replace Service modal: replacement candidates, the source
    service's sublayer inventory, consuming maps/apps, and job history.

    The inventory is read live from the service where the portal can
    authenticate unattended, because layer ids reach the database only through
    MSD parsing and the hosted sync; it falls back to the database otherwise,
    so a portal that prompts for credentials still opens the dialog without
    asking for them.
    """
    portal = get_object_or_404(Portal, alias=instance)
    service = get_object_or_404(Service, pk=service_pk, portal_instance=portal)

    candidates = Service.objects.filter(portal_instance=portal) \
        .exclude(pk=service.pk).order_by("service_name") \
        .values("pk", "service_name", "service_type")
    maps = Webmap.objects.filter(map_service__service_id=service) \
        .select_related("portal_instance").distinct()
    apps = service.apps.select_related("app_owner", "portal_instance").distinct()

    # Consumers come from the local database, so surface how fresh it is
    sync_times = [t for t in (portal.webmap_updated, portal.webapp_updated) if t]

    return render(request, "portals/portal_replace.html", {
        "item": service,
        "instance_alias": instance,
        "candidates": candidates,
        "source_layers": utils.resolve_layer_inventory(service),
        "maps": maps,
        "apps": apps,
        "jobs": utils.get_service_replacement_jobs(service),
        "last_synced": min(sync_times) if sync_times else None,
    })


@staff_member_required
@require_POST
def replace_dry_run_view(request, instance, service_pk):
    """
    Validates the replacement configuration, creates a ReplacementJob, and
    queues the dry-run analysis task. Returns the progress bar partial.
    """
    portal = get_object_or_404(Portal, alias=instance)
    service = get_object_or_404(Service, pk=service_pk, portal_instance=portal)
    danger = utils.danger_alert_response

    if utils.active_replacement_job_exists(portal):
        return danger("Another replacement job is already running for this portal. "
                      "Wait for it to finish before starting a new one.")

    mode = request.POST.get("mode", "simple")
    selected_maps = [m for m in request.POST.get("selected_maps", "").split(",") if m]
    selected_apps = [a for a in request.POST.get("selected_apps", "").split(",") if a]
    if not selected_maps and not selected_apps:
        return danger("Select at least one map or app to update.")

    # Only known consumers of this service may be targeted
    known_map_ids = set(Webmap.objects.filter(map_service__service_id=service)
                        .values_list("webmap_id", flat=True))
    known_app_ids = set(service.apps.values_list("app_id", flat=True))
    if set(selected_maps) - known_map_ids or set(selected_apps) - known_app_ids:
        return danger("Some selected items are not known consumers of this service. "
                      "Close the dialog and reopen it to refresh the lists.")

    if mode == "advanced":
        try:
            layer_mappings = json.loads(request.POST.get("layer_mappings") or "[]")
        except json.JSONDecodeError:
            return danger("Invalid layer mapping data.")
        if not layer_mappings:
            return danger("Add at least one layer mapping or switch to simple mode.")

        try:
            layer_mappings = [
                dict(m, old_layer_id=int(m["old_layer_id"]),
                     new_layer_id=int(m["new_layer_id"]),
                     explicit=bool(m.get("explicit")))
                for m in layer_mappings
            ]
        except (KeyError, TypeError, ValueError):
            return danger("Layer mappings contain an invalid layer id.")
        target_pks = {m.get("target_service_id") for m in layer_mappings}
        valid_pks = set(Service.objects.filter(
            portal_instance=portal, pk__in=target_pks).values_list("pk", flat=True))
        if service.pk in valid_pks or target_pks - valid_pks:
            return danger("Layer mappings reference an invalid replacement service.")
        config = {"mode": "advanced", "layer_mappings": layer_mappings}
    else:
        try:
            target_pk = int(request.POST.get("target_service", ""))
        except ValueError:
            return danger("Select a replacement service.")
        if target_pk == service.pk or not Service.objects.filter(
                portal_instance=portal, pk=target_pk).exists():
            return danger("Invalid replacement service.")
        config = {"mode": "simple", "target_service_id": target_pk}

    try:
        rows, warnings, partial, sublayer_renumber = utils.build_mapping_rows(service, config)
        pairs = utils.build_replacements(rows)
    except Exception as e:
        logger.error(f"Failed to build replacement mapping for service {service.pk}: {e}",
                     exc_info=True)
        return danger("Failed to build the replacement mapping. See logs for details.")

    # A renumber-only mapping is real work even when no string pair changes:
    # whole-service web map layers carry their sublayer ids as bare integers
    if not pairs and not sublayer_renumber:
        return danger("The configuration produces no replacements - the source and "
                      "replacement service URLs and item IDs are identical.")

    cred = utils.resolve_replacement_credentials(request, portal)
    if cred["prompt"]:
        return _render_replace_credentials(
            request, portal, cred["error"],
            post_url=reverse("enterpriseviz:replace_dry_run",
                             kwargs={"instance": instance, "service_pk": service.pk}),
            button_label="Authenticate & run dry run",
            action_label="run the dry run")

    # In a partial replacement (layer split, or layers left unmapped),
    # whole-service references cannot be repointed; the dry run counts how
    # many references to these URLs and item IDs remain per item
    unreplaced_references = []
    if partial:
        unreplaced_references = [u.rstrip("/") for u in (service.service_url or []) if u]
        unreplaced_references += [v for v in (service.portal_id or {}).values() if v]

    with transaction.atomic():
        Portal.objects.select_for_update().get(pk=portal.pk)
        if utils.active_replacement_job_exists(portal):
            return danger("Another replacement job is already running for this portal. "
                          "Wait for it to finish before starting a new one.")
        job = ReplacementJob.objects.create(
            portal_instance=portal,
            source_service=service,
            source_service_name=service.service_name or "",
            replacement_config=config,
            replacement_pairs=[list(p) for p in pairs],
            sublayer_renumber=sublayer_renumber,
            selected_map_ids=selected_maps,
            selected_app_ids=selected_apps,
            status="analyzing",
            initiated_by=request.user,
            dry_run_summary={"items": [], "warnings": warnings,
                             "unreplaced_references": unreplaced_references, "totals": {}},
        )

    if cred["token"]:
        utils.cache_replacement_credentials(job.id, cred["token"], request.user.pk)

    try:
        task = process_replacement_task.enqueue(job.id, "dry_run", credential_token=cred["token"])
        _tag_job_portal(task, job)
        job.celery_task_id = str(task.id)
        job.save(update_fields=["celery_task_id"])
    except Exception as e:
        logger.error(f"Failed to queue replacement dry run for job {job.id}: {e}", exc_info=True)
        job.delete()
        return danger("Failed to queue the dry run. See logs for details.")

    logger.info(f"Queued replacement dry run job {job.id} for service "
                f"'{service.service_name}' ({instance}). Task ID: {task.id}")
    return render(request, "partials/replace_progress.html", {
        "instance": instance,
        "task_id": task.id,
        "value": 0,
        "progress": task.progress_info,
        "task_name": "Replace Service Dry Run",
        "job_id": job.id,
        "phase": "dry_run",
        "source_service_pk": service.pk,
        "results_target": f"#{request.headers.get('HX-Target') or 'replace-dryrun-container'}",
    })


@staff_member_required
def replace_target_layers_view(request, instance, target_pk):
    """
    Returns the sublayer options of a candidate replacement service for the
    advanced mapping table row selects, live from the service where possible.
    """
    target = get_object_or_404(Service, pk=target_pk, portal_instance__alias=instance)
    return render(request, "partials/replace_layer_options.html", {
        "layers": utils.resolve_layer_inventory(target),
    })


@staff_member_required
def replace_preview_view(request, instance, job_id):
    """
    Renders the dry-run preview (with Execute) or, for finished jobs, the
    execution results (with Revert).
    """
    job, error = utils.get_replacement_job(instance, job_id)
    if error:
        return error
    return render(request, "partials/replace_preview.html", {
        "job": job,
        "instance_alias": instance,
        "backups": job.item_backups.order_by("item_title") if job.status not in
                   ("analyzing", "dry_run", "pending") else None,
        "can_revert": job.status in ReplacementJob.REVERTABLE_STATUSES
                      and job.item_backups.filter(
                          status__in=["applied", "revert_failed"]).exists(),
    })


@staff_member_required
@require_POST
def replace_execute_view(request, instance, job_id):
    """
    Queues execution of a previously dry-run replacement job.
    """
    job, error = utils.get_replacement_job(instance, job_id)
    if error:
        return error
    danger = utils.danger_alert_response

    if job.status != "dry_run":
        return danger(f"This job is '{job.get_status_display()}' and can no longer be executed. "
                      f"Run a new dry run first.")
    if utils.active_replacement_job_exists(job.portal_instance):
        return danger("Another replacement job is already running for this portal.")

    cred = utils.resolve_replacement_credentials(request, job.portal_instance, job_id=job.id)
    if cred["prompt"]:
        return _render_replace_credentials(
            request, job.portal_instance, cred["error"],
            post_url=reverse("enterpriseviz:replace_execute",
                             kwargs={"instance": instance, "job_id": job.pk}),
            button_label="Authenticate & execute",
            action_label="execute the replacement")

    # Lock the portal row and re-check under the lock so two racing requests
    # cannot both mark the job pending or start concurrent jobs
    with transaction.atomic():
        Portal.objects.select_for_update().get(pk=job.portal_instance_id)
        job.refresh_from_db()
        if job.status != "dry_run":
            return danger(f"This job is '{job.get_status_display()}' and can no longer be "
                          f"executed. Run a new dry run first.")
        if utils.active_replacement_job_exists(job.portal_instance):
            return danger("Another replacement job is already running for this portal.")
        job.status = "pending"
        job.save(update_fields=["status"])

    try:
        task = process_replacement_task.enqueue(job.id, "execute", credential_token=cred["token"])
    except Exception as e:
        logger.error(f"Failed to queue replacement execution for job {job.id}: {e}", exc_info=True)
        job.status = "dry_run"
        job.save(update_fields=["status"])
        return danger("Failed to queue the replacement. See logs for details.")

    _tag_job_portal(task, job)
    job.celery_task_id = str(task.id)
    job.save(update_fields=["celery_task_id"])
    logger.info(f"Queued replacement execution job {job.id} ({instance}). Task ID: {task.id}")
    return render(request, "partials/replace_progress.html", {
        "instance": instance,
        "task_id": task.id,
        "value": 0,
        "progress": task.progress_info,
        "task_name": "Replace Service",
        "job_id": job.id,
        "phase": "execute",
        "source_service_pk": job.source_service_id,
        "results_target": f"#{request.headers.get('HX-Target') or 'replace-dryrun-container'}",
    })


@staff_member_required
@require_POST
def replace_revert_view(request, instance, job_id):
    """
    Queues a revert of a completed replacement job from its item backups.
    """
    job, error = utils.get_replacement_job(instance, job_id)
    if error:
        return error
    danger = utils.danger_alert_response

    # "failed" is revertable too: an execute that died mid-run may have
    # already updated (and backed up) some items
    if job.status not in ReplacementJob.REVERTABLE_STATUSES:
        return danger(f"This job is '{job.get_status_display()}' and cannot be reverted.")
    if not job.item_backups.filter(status__in=["applied", "revert_failed"]).exists():
        return danger("No backups remain for this job (they may have been removed by the "
                      "retention policy), so it cannot be reverted.")
    if utils.active_replacement_job_exists(job.portal_instance):
        return danger("Another replacement job is already running for this portal.")

    cred = utils.resolve_replacement_credentials(request, job.portal_instance, job_id=job.id)
    if cred["prompt"]:
        return _render_replace_credentials(
            request, job.portal_instance, cred["error"],
            post_url=reverse("enterpriseviz:replace_revert",
                             kwargs={"instance": instance, "job_id": job.pk}),
            button_label="Authenticate & revert",
            action_label="revert the replacement")

    # Lock the portal row and re-check under the lock so two racing requests
    # cannot both queue a revert or start concurrent jobs
    with transaction.atomic():
        Portal.objects.select_for_update().get(pk=job.portal_instance_id)
        job.refresh_from_db()
        if job.status not in ReplacementJob.REVERTABLE_STATUSES:
            return danger(f"This job is '{job.get_status_display()}' and cannot be reverted.")
        if utils.active_replacement_job_exists(job.portal_instance):
            return danger("Another replacement job is already running for this portal.")
        prior_status = job.status
        job.status = "reverting"
        job.save(update_fields=["status"])

    try:
        task = revert_replacement_task.enqueue(job.id, credential_token=cred["token"],
                                             prior_status=prior_status)
    except Exception as e:
        logger.error(f"Failed to queue revert for job {job.id}: {e}", exc_info=True)
        job.status = prior_status
        job.save(update_fields=["status"])
        return danger("Failed to queue the revert. See logs for details.")

    _tag_job_portal(task, job)
    job.revert_task_id = str(task.id)
    job.save(update_fields=["revert_task_id"])
    logger.info(f"Queued replacement revert job {job.id} ({instance}). Task ID: {task.id}")
    return render(request, "partials/replace_progress.html", {
        "instance": instance,
        "task_id": task.id,
        "value": 0,
        "progress": task.progress_info,
        "task_name": "Revert Replacement",
        "job_id": job.id,
        "phase": "revert",
        "source_service_pk": job.source_service_id,
        "results_target": f"#{request.headers.get('HX-Target') or 'replace-dryrun-container'}",
    })


@staff_member_required
def replace_history_view(request, instance, service_pk):
    """
    Renders the job history fragment for a service's Replace modal.
    """
    service = get_object_or_404(Service, pk=service_pk, portal_instance__alias=instance)
    return render(request, "partials/replace_history.html", {
        "jobs": utils.get_service_replacement_jobs(service),
        "instance_alias": instance,
        "item": service,
    })


@staff_member_required
def replace_report_view(request, instance):
    """
    Full replacement report for a portal: every job's source and target
    services with one row per affected map/app, exportable via the standard
    table export buttons. Renders the page or, with ?partial=table, just the
    table fragment so it can refresh itself when a task completes.
    """
    portal = get_object_or_404(Portal, alias=instance)
    rows = utils.get_replacement_report_rows(portal)
    context = {
        "instance_alias": instance,
        "rows": rows,
        "portal": Portal.objects.values_list("alias", "portal_type", "url"),
        "instance": portal,
    }

    if request.GET.get("partial") == "table":
        return render(request, "partials/replace_report_table.html", context)

    template = "portals/portal_replace_report.html" if request.htmx \
        else "portals/portal_replace_report_full.html"
    return render(request, template, context)


@staff_member_required
def replace_backup_export_view(request, instance, backup_id):
    """
    Downloads a single item's pre-change backup as a JSON file so it can be
    applied manually (e.g. via ArcGIS Online Assistant) instead of reverted.
    """
    from .models import ReplacementItemBackup

    backup = ReplacementItemBackup.objects.filter(
        pk=backup_id, job__portal_instance__alias=instance).first()
    if backup is None:
        return HttpResponse(status=404, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Backup not found."})})

    payload = utils.build_backup_export(backup)
    response = JsonResponse(payload, json_dumps_params={"indent": 2})
    filename = f"backup_{backup.item_id or backup.pk}.json"
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


@staff_member_required
@require_POST
def replace_revert_item_view(request, instance, backup_id):
    """
    Queues a revert of a single item from its backup row (partial revert of
    a replacement job).
    """
    from .models import ReplacementItemBackup

    backup = ReplacementItemBackup.objects.filter(
        pk=backup_id, job__portal_instance__alias=instance).select_related("job").first()
    danger = utils.danger_alert_response
    if backup is None:
        return danger("Backup not found.")

    job = backup.job
    if backup.status not in ReplacementItemBackup.REVERTABLE_STATUSES:
        return danger(f"This item is '{backup.get_status_display()}' and cannot be reverted.")
    if utils.active_replacement_job_exists(job.portal_instance):
        return danger("Another replacement job is already running for this portal.")

    cred = utils.resolve_replacement_credentials(request, job.portal_instance, job_id=job.id)
    if cred["prompt"]:
        return _render_replace_credentials(
            request, job.portal_instance, cred["error"],
            post_url=reverse("enterpriseviz:replace_revert_item",
                             kwargs={"instance": instance, "backup_id": backup.pk}),
            button_label="Authenticate & revert item",
            action_label=f"revert '{backup.item_title or backup.item_id}'")

    # If the item was edited in the portal after the replacement, reverting
    # would overwrite those edits - ask the user first, offering the backup
    # download as the non-destructive alternative
    force = request.POST.get("force") == "1"
    if not force and backup.applied_modified_at is not None:
        check = utils.check_backup_newer_edits(job.portal_instance, cred["token"], backup)
        if check["error"]:
            return danger(check["error"])
        if check["has_newer_edits"]:
            return render(request, "partials/replace_revert_confirm.html", {
                "backup": backup,
                "instance_alias": instance,
                "modified_at": check["modified_at"],
            })

    # Lock the portal row and re-check under the lock so an item revert
    # cannot start while another job is being queued. The backup is also
    # claimed here (revert_claimed_at) so a duplicate/concurrent request for
    # the same backup is rejected instead of queueing a second revert task;
    # a claim older than REVERT_CLAIM_LEASE is treated as abandoned (crashed
    # worker) and can be reclaimed.
    with transaction.atomic():
        Portal.objects.select_for_update().get(pk=job.portal_instance_id)
        backup.refresh_from_db()
        if backup.status not in ReplacementItemBackup.REVERTABLE_STATUSES:
            return danger(f"This item is '{backup.get_status_display()}' and cannot be reverted.")
        if utils.active_replacement_job_exists(job.portal_instance):
            return danger("Another replacement job is already running for this portal.")
        lease_cutoff = timezone.now() - ReplacementItemBackup.REVERT_CLAIM_LEASE
        claimed = ReplacementItemBackup.objects.filter(pk=backup.pk).filter(
            Q(revert_claimed_at__isnull=True) | Q(revert_claimed_at__lt=lease_cutoff)
        ).update(revert_claimed_at=timezone.now())
        if not claimed:
            return danger("This item's revert is already in progress.")

    try:
        task = revert_replacement_task.enqueue(job.id, credential_token=cred["token"],
                                             backup_ids=[backup.pk], force=force)
    except Exception as e:
        logger.error(f"Failed to queue item revert for backup {backup.pk}: {e}", exc_info=True)
        ReplacementItemBackup.objects.filter(pk=backup.pk).update(revert_claimed_at=None)
        return danger("Failed to queue the revert. See logs for details.")

    _tag_job_portal(task, job)
    job.revert_task_id = str(task.id)
    job.save(update_fields=["revert_task_id"])
    logger.info(f"Queued item revert for '{backup.item_title}' (job {job.id}, {instance}). "
                f"Task ID: {task.id}")
    return render(request, "partials/replace_item_progress.html", {
        "instance": instance,
        "task_id": task.id,
        "value": 0,
        "progress": task.progress_info,
        "task_name": f"Revert '{backup.item_title or backup.item_id}'",
    })


ERROR_PAGE_CONFIG = {
    400: ("400.html", "Bad request"),
    403: ("403.html", "Access denied"),
    404: ("404.html", "Page not found"),
    500: ("500.html", "Something went wrong"),
}


def render_error(request, status, message):
    """
    Render an error consistently for every kind of request.

    HTMX navigations (target #mainbodycontent) get a Calcite notice partial
    swapped into the page — the response carries an X-Error-Page header that
    custom.js uses to opt the error response in to swapping, since HTMX
    ignores 4xx/5xx bodies by default. Other HTMX requests (modals, polling,
    actions) keep the HX-Trigger-After-Settle alert pattern. Direct requests
    get the standard full error page.
    """
    template_name, title = ERROR_PAGE_CONFIG.get(status, ERROR_PAGE_CONFIG[500])
    htmx = getattr(request, "htmx", None)
    if htmx:
        if htmx.target == "mainbodycontent":
            response = render(request, "partials/error_panel.html",
                              {"code": status, "title": title, "message": message},
                              status=status)
            response["X-Error-Page"] = "true"
            return response
        return HttpResponse(status=status, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": message})})
    return render(request, template_name, status=status)


def error_400_view(request, exception=None):
    return render_error(request, 400, "The server couldn't process this request.")


def error_403_view(request, exception=None):
    return render_error(request, 403, "You don't have permission to perform this action.")


def error_404_view(request, exception=None):
    return render_error(request, 404, "The requested page or resource was not found.")


def error_500_view(request):
    try:
        if getattr(request, "htmx", None):
            return render_error(request, 500, "An unexpected server error occurred.")
        template = loader.get_template("500.html")
        return HttpResponseServerError(template.render())
    except Exception:
        return HttpResponseServerError(
            "<h1>Server Error (500)</h1>", content_type="text/html")
