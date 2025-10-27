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
from collections import defaultdict

import cron_descriptor
from celery import current_app as celery_app  # For signaling Celery
from celery.result import AsyncResult
from celery_progress.backend import Progress
from django.conf import settings
from django.contrib import messages
from django.contrib.admin.utils import unquote
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import AuthenticationForm
from django.core.exceptions import FieldError
from django.core.mail import get_connection, EmailMessage
from django.db import transaction, DatabaseError
from django.db.models import Q
from django.http import Http404
from django.http import HttpResponse, JsonResponse, HttpResponseForbidden
from django.shortcuts import render, redirect, get_object_or_404
from django.template import loader
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django_celery_results.models import TaskResult
from django_filters.views import FilterView
from django_tables2 import SingleTableMixin, RequestConfig
from django_tables2.export.views import ExportMixin

from .filters import WebmapFilter, ServiceFilter, LayerFilter, AppFilter, UserFilter, LogEntryFilter
from .forms import ScheduleForm, SiteSettingsForm, ToolsForm, WebhookSettingsForm, PortalCredentialsForm
from .models import (
    PortalCreateForm, UserProfile, LogEntry,
)
from .request_context import get_django_request_context
from .tables import WebmapTable, ServiceTable, LayerTable, AppTable, UserTable, LogEntryTable
from .tasks import *

logger = logging.getLogger('enterpriseviz')

# Configuration for the Table view
TABLE_VIEW_MODEL_CONFIG = {
    "webmap": {"model": Webmap, "table_class": WebmapTable, "filterset_class": WebmapFilter},
    "service": {"model": Service, "table_class": ServiceTable, "filterset_class": ServiceFilter},
    "layer": {"model": Layer, "table_class": LayerTable, "filterset_class": LayerFilter},
    "app": {"model": App, "table_class": AppTable, "filterset_class": AppFilter},
    "user": {"model": User, "table_class": UserTable, "filterset_class": UserFilter},
}


class Table(ExportMixin, SingleTableMixin, FilterView):
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
        else:
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

    def get_queryset(self):
        """
        Returns the queryset for the current model type, optionally filtered by instance.

        :return: The queryset to be displayed.
        :rtype: django.db.models.QuerySet
        """
        self._configure_for_model_type()
        qs = super().get_queryset()

        instance_alias = self.kwargs.get("instance")
        if instance_alias:
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

        table = self.table_class(context["filter"].qs)
        RequestConfig(self.request, paginate={"per_page": self.paginate_by}).configure(table)

        context.update({
            "table": table,
            "table_name": self.kwargs.get("name"),
        })
        if "instance" in self.kwargs:
            context.update({"instance": self.kwargs["instance"]})

        return context


class LogTable(ExportMixin, SingleTableMixin, FilterView):
    """
    Displays a paginated and filterable table of LogEntry records.

    Supports dynamic column visibility based on 'visible_cols' GET parameter.

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

    def get_queryset(self):
        """
        Returns LogEntry queryset ordered by timestamp descending.

        :return: Ordered queryset of log entries.
        :rtype: django.db.models.QuerySet
        """
        return super().get_queryset().order_by("-timestamp")

    def get_table_kwargs(self):
        """
        Dynamically sets 'exclude' kwarg for table based on 'visible_cols' GET params.

        :return: Keyword arguments for table instantiation.
        :rtype: dict
        """
        kwargs = super().get_table_kwargs()
        all_column_field_names = [name for name, _ in self.table_class.base_columns.items()]
        default_visible = list(self.table_class.DEFAULT_VISIBLE_COLUMNS)

        if 'visible_cols' in self.request.GET:
            visible_columns_param = self.request.GET.getlist('visible_cols')
            # Handle `visible_cols=` (empty string from deselected all) vs `visible_cols` not present.
            if len(visible_columns_param) == 1 and visible_columns_param[0] == '':
                actual_visible_columns = []
            else:
                actual_visible_columns = [col for col in visible_columns_param if col]

            # If 'visible_cols' was in GET and actual_visible_columns is empty, it means "show none"
            if not actual_visible_columns and 'visible_cols' in self.request.GET:
                kwargs['exclude'] = tuple(all_column_field_names)
            else:  # Some columns specified or 'visible_cols' was not in GET (covered by else below)
                excluded_columns = [
                    col_name for col_name in all_column_field_names if col_name not in actual_visible_columns
                ]
                # If actual_visible_columns is empty because 'visible_cols' was NOT in GET,
                # this branch shouldn't be hit due to outer 'if'.
                # This path is for when actual_visible_columns is populated.
                if actual_visible_columns:  # ensure it's not empty due to no param
                    kwargs['exclude'] = tuple(excluded_columns)
                else:  # 'visible_cols' in GET but empty list after filtering (e.g. ?visible_cols=&visible_cols=)
                    # This case should be rare, treat as show none.
                    kwargs['exclude'] = tuple(all_column_field_names)

        else:
            # 'visible_cols' not in request.GET, apply defaults
            excluded_columns = [
                col_name for col_name in all_column_field_names if col_name not in default_visible
            ]
            kwargs['exclude'] = tuple(excluded_columns)
        return kwargs

    def get_context_data(self, **kwargs):
        """
        Adds log-specific context data, including column visibility and pagination.

        :param kwargs: Additional keyword arguments for context.
        :return: The context dictionary for template rendering.
        :rtype: dict
        """
        context = super().get_context_data(**kwargs)
        query_params = self.request.GET.copy()
        if 'page' in query_params:
            del query_params['page']
        context['query_params_urlencode'] = query_params.urlencode()

        page_obj = context.get('page_obj')
        if page_obj:
            context['paginator_range'] = page_obj.paginator.get_elided_page_range(
                page_obj.number, on_each_side=1, on_ends=1
            )

        all_columns = [(name, column.header) for name, column in self.table_class.base_columns.items()]
        context['all_columns'] = all_columns

        # Determine current_visible_columns based on what will be excluded/included
        # This should reflect what the table is actually rendering.
        # It's simpler to derive this from the request or defaults, similar to get_table_kwargs.
        default_visible_list = list(self.table_class.DEFAULT_VISIBLE_COLUMNS)
        if 'visible_cols' in self.request.GET:
            visible_columns_param = self.request.GET.getlist('visible_cols')
            if len(visible_columns_param) == 1 and visible_columns_param[0] == '':
                context['current_visible_columns'] = []
            else:
                context['current_visible_columns'] = [col for col in visible_columns_param if col]
        else:
            context['current_visible_columns'] = default_visible_list

        return context


@login_required
def logs_view(request):
    """
    Renders the main application logs page or a partial for HTMX requests.

    Provides context for filtering logs and selecting visible columns.

    :param request: The HTTP request object.
    :type request: django.http.HttpRequest
    :return: Rendered HTML response.
    :rtype: django.http.HttpResponse
    """
    template = "app/logs.html" if request.htmx else "app/logs_full.html"
    filterset = LogEntryFilter(request.GET, queryset=LogEntry.objects.all().order_by('-timestamp'))
    portals = Portal.objects.values_list("alias", "portal_type", "url")

    # Use LogEntryTable's definition for all columns
    all_table_columns = [(name, column.header) for name, column in LogEntryTable.base_columns.items()]

    default_cols = list(LogEntryTable.DEFAULT_VISIBLE_COLUMNS)

    if 'visible_cols' in request.GET:
        visible_columns_param = request.GET.getlist('visible_cols')
        if len(visible_columns_param) == 1 and visible_columns_param[0] == '':  # User deselected all
            initial_visible_columns = []
        else:
            initial_visible_columns = [col for col in visible_columns_param if col]
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
        return HttpResponse(status=400, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Error retrieving map details: ID missing."})})

    try:
        logger.debug(f"Fetching map details for ID: {id}")
        details = utils.map_details(id)
        if "error" in details:
            logger.warning(f"Error in map details response for ID {id}: {details['error']}")
            return HttpResponse(status=500,
                                headers={"HX-Trigger-After-Settle": json.dumps({"showDangerAlert": details["error"]})})

        portal_data = Portal.objects.values_list("alias", "portal_type", "url")
        details["portal"] = portal_data
        logger.debug(f"Successfully retrieved and rendering map details for ID: {id}")
        return render(request, template, details)

    except Exception as e:
        logger.error(f"Unexpected error for ID {id}: {e}", exc_info=True)
        return HttpResponse(status=500, headers={"HX-Trigger-After-Settle": json.dumps(
            {"showDangerAlert": "An unexpected error occurred while retrieving map details."})})


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
        return HttpResponse(status=400, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Missing 'instance' or 'url' parameter."})})

    try:
        unquoted_url = unquote(url)
        logger.debug(f"Fetching service details for instance: {instance}, url: {unquoted_url}")
        details = utils.service_details(instance, unquoted_url)

        if "error" in details:
            logger.warning(f"Error in service details for {instance}: {details['error']}")
            return HttpResponse(status=500,
                                headers={"HX-Trigger-After-Settle": json.dumps({"showDangerAlert": details["error"]})})

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
        return HttpResponse(status=500, headers={"HX-Trigger-After-Settle": json.dumps(
            {"showDangerAlert": "An unexpected error occurred while retrieving service details."})})


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

    version = request.GET.get("version", "")
    dbserver = request.GET.get("server", "")
    database = request.GET.get("database", "")
    logger.debug(f"Params - version: {version}, server: {dbserver}, database: {database}")

    if not name:
        logger.warning("Layer name missing.")
        return HttpResponse(status=400, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Layer name is missing."})})

    try:
        logger.debug(f"Fetching layer details for name: {name}")
        details = utils.layer_details(dbserver, database, version, name)
        if "error" in details:
            logger.warning(f"Error in layer details for {name}: {details['error']}")
            return HttpResponse(status=500,
                                headers={"HX-Trigger-After-Settle": json.dumps({"showDangerAlert": details["error"]})})

        portal_data = list(Portal.objects.values_list("alias", "portal_type", "url"))
        details["portal"] = portal_data
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
        return HttpResponse(status=500, headers={"HX-Trigger-After-Settle": json.dumps(
            {"showDangerAlert": "An unexpected error occurred while retrieving layer details."})})


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
            store_password = form.cleaned_data["store_password"]
            alias = form.cleaned_data["alias"]
            url = form.cleaned_data["url"]
            username = form.cleaned_data.get("username", "")
            logger.debug(f"Form valid. alias={alias}, url={url}, store_password={store_password}")

            try:
                if store_password:
                    logger.debug(f"Attempting connection for {alias} with stored credentials.")
                    connection_result = utils.try_connection(form.cleaned_data).get("authenticated", False)
                    if connection_result:
                        form.save()
                        logger.info(
                            f"Portal {alias} ({url}) created and authenticated as {username}.")
                        context = {"portal": Portal.objects.values_list("alias", "portal_type", "url")}
                        response = render(request, "partials/portal_updates.html", context)
                        response["HX-Trigger-After-Settle"] = json.dumps(
                            {"closeModal": True,
                             "showSuccessAlert": f"Successfully added {url} as {alias} and authenticated."}
                        )
                        return response
                    else:
                        logger.warning(f"Authentication failed for {alias} ({url}) as {username}.")
                        context = {"form": form}
                        response = render(request, "partials/portal_add_form.html", context,
                                          status=401)
                        response["HX-Trigger-After-Settle"] = json.dumps(
                            {"showDangerAlert": f"Unable to connect to {url} as {username}. Please verify credentials."}
                        )
                        return response
                else:
                    form.save()
                    logger.info(f"Portal {alias} ({url}) created without stored credentials.")
                    context = {"portal": Portal.objects.values_list("alias", "portal_type", "url")}
                    response = render(request, "partials/portal_updates.html", context)
                    response["HX-Trigger-After-Settle"] = json.dumps(
                        {
                            "showSuccessAlert": f"Successfully added {url} as {alias}. Authentication will be required when refreshing data.",
                            "closeModal": True}
                    )
                    return response
            except Exception as e:
                logger.error(f"Unexpected error creating portal {alias}: {e}",
                             exc_info=True)
                context = {"form": form}
                response = render(request, "partials/portal_add_form.html", context, status=500)
                response["HX-Trigger-After-Settle"] = json.dumps(
                    {"showDangerAlert": f"An unexpected error occurred"}
                )
                return response
        else:
            logger.warning(f"Form invalid. Errors: {form.errors}")
            return render(request, "partials/portal_add_form.html", {"form": form})

    logger.debug("Rendering initial form.")
    return render(request, "portals/portal_add.html", {"form": PortalCreateForm()})


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
                logger.warning(
                    f"Portal instance '{instance}' not found, dashboard will show aggregate if instance_item is None.")
        else:
            logger.debug("Retrieved aggregated data across all portal instances")

        context = {
            "webmaps": webmaps_count,
            "services": services_count,
            "layers": layers_count,
            "apps": apps_count,
            "users": users_count,
            "portal": portals,
            "instance": instance_item
        }
        return render(request, template, context)
    except Exception as e:
        logger.error(f"Error generating dashboard: {e}", exc_info=True)
        context = {
            "error": "An error occurred while retrieving data.",
            "portal": Portal.objects.values_list("alias", "portal_type", "url")
        }
        return render(request, template, context, status=500)


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
        - 'delete': Optional boolean ('true'/'false') to delete existing items before refresh.
        - 'username': Optional username for portal authentication.
        - 'password': Optional password for portal authentication.

    :param request: The HTTP request object.
    :type request: django.http.HttpRequest
    :return: Rendered HTML response, typically a progress bar partial or a credential form.
    :rtype: django.http.HttpResponse
    """
    logger.debug(f"Method={request.method}, user={request.user.username}, POST data: {request.POST}")

    if request.method != "POST":
        logger.warning("Method not allowed for refresh_portal_view.")
        return JsonResponse({"error": "Method not allowed"}, status=405)

    # Get basic parameters first
    instance_alias = request.POST.get("instance")
    items_to_refresh = request.POST.get("items")
    instance_url = request.POST.get("url")
    delete_flag = request.POST.get("delete", "false").lower() == "true"

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
    task_map = {
        "webmaps": update_webmaps,
        "services": update_services,
        "webapps": update_webapps,
        "users": update_users
    }
    task_func = task_map.get(items_to_refresh)
    if not task_func:
        logger.warning(f"Invalid task type '{items_to_refresh}' for portal '{portal.alias}'.")
        return JsonResponse({"error": "Invalid task type selected"}, status=400)

    # Determine if credentials are needed
    credentials_required = not portal.store_password
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
                        "delete": delete_flag,
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

                credential_token = utils.CredentialManager.store_credentials(username, password, ttl_seconds=300)
                if not credential_token:
                    logger.error(f"Failed to store temporary credentials for {portal.alias}.")
                    form.add_error(None, "System error: Failed to secure credentials.")
                    context = {
                        "form": form,
                        "instance": portal,
                        "items": items_to_refresh,
                        "delete": delete_flag,
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
                    "delete": delete_flag,
                    "error_message": "Please correct the errors below."
                }
                return render(request, "portals/portal_credentials.html", context, status=400)
        else:
            # Need to show credential form
            logger.debug(f"Credentials required for {portal.alias}; rendering credential form.")
            form = PortalCredentialsForm(
                initial={
                    'instance': portal.alias,
                    'items': items_to_refresh,
                    'delete': delete_flag
                },
                portal=portal,
                require_credentials=True
            )
            context = {
                "form": form,
                "instance": portal,
                "items": items_to_refresh,
                "delete": delete_flag,
            }
            return render(request, "portals/portal_credentials.html", context)
    else:
        # Portal has stored credentials, proceed directly
        logger.debug(f"Portal '{portal.alias}' has stored credentials. Proceeding to task.")

    # Start the background task
    logger.debug(f"Starting Celery task for '{portal.alias}', items: '{items_to_refresh}'")

    django_ctx = get_django_request_context()
    user_id = request.user if request.user.is_authenticated else None

    task_args = [portal.alias, delete_flag, credential_token]
    task_kwargs = {
        '_request_id': str(django_ctx.get('request_id')),
        '_user': user_id.username,
        '_client_ip': django_ctx.get('client_ip'),
        '_request_path': django_ctx.get('request_path'),
    }

    try:
        task = task_func.apply_async(args=task_args, kwargs=task_kwargs)
        logger.info(f"Started task '{task.id}' for '{items_to_refresh}' on portal '{portal.alias}'.")

        response_data = {
            "instance": portal.alias,
            "task_id": task.id,
            "value": 0
        }
        response = render(request, "partials/progress_bar.html", context=response_data)
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
        result = AsyncResult(task_id)
        progress_info = Progress(result).get_info()
        task_name = result._get_task_meta().get('task_name')

        if not progress_info or "progress" not in progress_info:
            logger.warning(
                f"Invalid or missing progress data for task '{task_id}'. State: {result.state}")
            progress_percentage = 100 if result.successful() or result.failed() else 0
            task_state = result.state
            response_data = {"instance": instance, "task_id": task_id, "task_name": task_name,
                             "progress": {"percent": progress_percentage, "description": "Fetching status..."},
                             "value": progress_percentage, "state": task_state}
        else:
            progress_percentage = progress_info["progress"].get("percent", 0)
            task_state = progress_info.get("state", result.state)
            response_data = {"instance": instance, "task_id": task_id, "task_name": task_name,
                             "progress": progress_info,
                             "value": progress_percentage}

        logger.debug(f"Task '{task_id}' state '{task_state}', progress {progress_percentage}%.")
        response = render(request, "partials/progress_bar.html", context=response_data)

        htmx_trigger = {}
        task_result = result.info
        if task_state == "SUCCESS":
            has_errors = False

            logger.info(
                f"Task '{task_id}' ({task_name}) for {instance} completed successfully.")
            success_details = "Completed."
            if isinstance(task_result, dict):
                # Handle data refresh task results
                if 'num_updates' in task_result or 'num_inserts' in task_result:
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
                    "showWarningAlert": f"{task_name} for {instance} completed with errors. <br> <b>{success_details}</b>",
                    "updateComplete": "true"
                }
            else:
                htmx_trigger = {
                    "showSuccessAlert": f"{task_name} for {instance} completed. <br> <b>{success_details}</b>",
                    "updateComplete": "true"}
        elif task_state == "FAILURE":
            logger.warning(
                f"Task '{task_id}' ({task_name}) for {instance} failed. Info: {task_result}")
            error_message = str(task_result) if not isinstance(task_result, dict) else task_result.get('error',
                                                                                                       str(task_result))
            htmx_trigger = {
                "showDangerAlert": f"{task_name} for {instance} failed. See logs or results table for details. Error: {error_message[:100]}...",
                "updateComplete": "true"}

        if htmx_trigger:
            response["HX-Trigger"] = json.dumps(htmx_trigger)
        return response

    except Exception as e:
        logger.error(f"Error retrieving progress for task '{task_id}': {e}", exc_info=True)
        return HttpResponse(status=500, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Failed to retrieve task progress."})})


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
        time.sleep(0.5)
        if form.is_valid():
            username = form.cleaned_data["username"]
            user = form.get_user()

            if user is not None:
                login(request, user)
                messages.success(request, f"Welcome back, {username}!")
                logger.info(f"User '{username}' logged in successfully.")
                return redirect("/enterpriseviz/")
        else:
            logger.warning(f"Login form invalid: {form.errors.as_json()}")
            messages.error(request, "Invalid username or password.")

    return render(request, "app/login.html", {"form": form, "login_url": settings.SOCIAL_AUTH_ARCGIS_URL})


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
        time.sleep(0.5)
        form = PortalCreateForm(request.POST, instance=portal)
        if form.is_valid():
            logger.debug("Form valid.")
            store_password = form.cleaned_data["store_password"]
            requires_auth_check = store_password and form.cleaned_data.get("password")

            if requires_auth_check:
                logger.debug(f"Checking connection for {portal.alias} with new credentials.")
                connection_result = utils.try_connection(form.cleaned_data).get("authenticated", False)
                if not connection_result:
                    username = form.cleaned_data.get("username", "N/A")
                    url = form.cleaned_data.get("url", portal.url)
                    logger.warning(f"Auth failed for {instance} ({url}) as {username}.")
                    response = render(request, "portals/portal_update.html", {"form": form, "instance": instance},
                                      status=401)
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
            success_message = f"Successfully updated portal '{new_alias}'."
            if requires_auth_check: success_message += " Authentication updated."

            response["HX-Trigger-After-Settle"] = json.dumps({"showSuccessAlert": success_message, "closeModal": True})
            return response
        else:
            logger.warning(f"Form invalid. Errors: {form.errors}")
            return render(request, "portals/portal_update.html", {"form": form, "instance": instance},
                          status=400)

    form = PortalCreateForm(instance=portal)
    return render(request, "portals/portal_update.html", {"form": form, "instance": instance})


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

    results = TaskResult.objects.filter(task_args__icontains=f"'{instance}'").exclude(
        task_name__icontains="batch").order_by('-date_done')[:10]

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
                                  {"form": form, "enable": portal.store_password, "description": ""})
                response["HX-Trigger-After-Settle"] = json.dumps(
                    {"showSuccessAlert": "Scheduled task removed.", "closeModal": True})
                return response
            else:
                return HttpResponse(status=204)
        except Exception as e:
            logger.error(f"Error deleting task for '{instance}': {e}", exc_info=True)
            form = ScheduleForm(initial={"instance": portal.alias})
            response = render(request, "partials/portal_schedule_form.html",
                              {"form": form, "enable": portal.store_password,
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
                                  {"form": form, "enable": portal.store_password, "description": description,
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
                              {"form": form, "enable": portal.store_password, "description": description,
                               "results": results, "instance_alias": portal.alias})
            response["HX-Trigger-After-Settle"] = json.dumps(
                {"showSuccessAlert": f"Updates scheduled for '{instance}': {description}", "closeModal": True}
            )
            return response
        else:
            logger.warning(f"schedule_task_view: Form invalid for '{instance}'. Errors: {form.errors}")
            response = render(request, "partials/portal_schedule_form.html",
                              {"form": form, "enable": portal.store_password, "description": description,
                               "results": results, "instance_alias": portal.alias})
            return response

    context = {"form": form, "description": description, "results": results, "enable": portal.store_password,
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
        return HttpResponse(status=404,
                            headers={
                                "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Portal not found."})})

    similarity_threshold = 70
    logger.debug(f"Fetching duplicates for '{instance}' with threshold {similarity_threshold}%.")

    try:
        duplicates_data = utils.get_duplicates(portal_instance_obj, similarity_threshold=similarity_threshold)
        if "error" in duplicates_data:
            logger.warning(
                f"Error from get_duplicates for '{instance}': {duplicates_data['error']}")
            return HttpResponse(status=500, headers={
                "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": duplicates_data['error']})})

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
        return HttpResponse(status=500, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Error processing duplicates."})})


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
        return HttpResponse(status=404, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Portal '{instance}' not found."})})

    maps_found, apps_found, layer_url_searched = [], [], None

    if "layerId_input" in request.POST:
        layer_url_searched = unquote(request.POST.get("layerId_input", "").strip())
        logger.debug(f"Processing layer URL input: {layer_url_searched}")

        if not layer_url_searched:
            logger.debug("Empty layerId_input provided.")
            return HttpResponse(headers={"HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Provide a layer URL."})})

        try:
            layer_usage_result = utils.find_layer_usage(portal, layer_url_searched)
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
        return HttpResponse(status=200, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Portal instance not found."})
        })

    # Determine if credentials are needed
    credentials_required = not portal.store_password
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

                credential_token = utils.CredentialManager.store_credentials(username, password, ttl_seconds=300)
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

        context = {
            "portal_list": Portal.objects.values_list("alias", "portal_type", "url"),
            "current_portal": portal,
            "instance_alias": instance,
            "metadata": metadata_report_data.get("metadata", []),
        }

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
def webhook_view(request):
    """
    Handle incoming webhook requests by validating signatures and processing events.

    This view receives and verifies webhook payloads using ArcGIS configured secret. If the
    signature is valid, it parses the JSON payload and processes relevant events.
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

    Returns:
    - `200 OK` if the webhook is processed successfully.
    - `403 Forbidden` if the signature is invalid.
    - `400 Bad Request` for malformed JSON payloads.
    - `404 Not Found` if the portal is unknown.
    """
    logger.debug("Received webhook request.")

    # Validate webhook secret
    if not utils.validate_webhook_secret(request):
        return HttpResponseForbidden()

    # Parse and validate payload
    try:
        payload = json.loads(request.body)
        portal_url = payload.get("info", {}).get("portalURL")
        if not portal_url:
            logger.warning("Missing 'portalURL' in payload.")
            return JsonResponse({"error": "Missing portal URL"}, status=400)
    except json.JSONDecodeError:
        logger.warning("Invalid JSON payload.")
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    # Get portal instance
    portal_instance = utils.get_portal_instance(portal_url)
    if not portal_instance:
        logger.warning(f"Unknown portal URL received: {portal_url}")
        return JsonResponse({"error": "Unknown portal"}, status=404)

    # Connect to portal
    try:
        target = utils.connect(portal_instance)
    except Exception as e:
        logger.error(f"Connection failed for portal '{portal_instance.alias}': {e}", exc_info=True)
        return JsonResponse({"error": "Failed to connect to portal"}, status=500)

    # Process events
    events = payload.get("events", [])
    logger.debug(f"Webhook: Processing {len(events)} events.")

    utils.process_webhook_events(events, target, portal_instance)

    logger.info(f"Successfully processed payload for {portal_url}.")
    return JsonResponse({"message": "Webhook received successfully"}, status=200)


VALID_LOG_LEVELS = {
    "CRITICAL": logging.CRITICAL, "ERROR": logging.ERROR, "WARNING": logging.WARNING,
    "INFO": logging.INFO, "DEBUG": logging.DEBUG
}
LOG_LEVEL_NAMES = list(VALID_LOG_LEVELS.keys())


@staff_member_required
def log_settings_view(request):
    """
    Configures application-wide logging levels.

    On GET, displays the current logging level and a form to change it.
    On POST, updates the logging level in SiteSettings, applies it to the
    current Django process, and signals active Celery workers to update their
    log levels as well.

    Expected POST parameter:
        - 'level': The desired logging level string (e.g., 'INFO', 'DEBUG').

    :param request: The HTTP request object.
    :type request: django.http.HttpRequest
    :return: Rendered HTML response (log settings form partial) or an error response.
    :rtype: django.http.HttpResponse
    """
    logger.debug(f"Method={request.method}, user={request.user.username}")
    site_settings, _ = SiteSettings.objects.get_or_create(pk=1)

    if request.method == "POST":
        new_level_str = request.POST.get("level", "").upper()
        logger.debug(f"Requested log level: {new_level_str}")

        if new_level_str in VALID_LOG_LEVELS:
            try:
                site_settings.logging_level = new_level_str
                site_settings.save(update_fields=['logging_level'])
                logger.info(f"SiteSettings database updated to logging level: {new_level_str}.")

                utils.apply_global_log_level(level_name=new_level_str)
                logger.info(f"Log level '{new_level_str}' applied to current Django process.")

                active_workers = celery_app.control.inspect().active()
                if active_workers:
                    tasks_sent_count = 0
                    for worker_name in active_workers.keys():
                        # Task should fetch the level from SiteSettings when it runs in the worker
                        apply_site_log_level_in_worker.delay()
                        tasks_sent_count += 1
                    logger.info(f"Task to update log levels sent to {tasks_sent_count} active Celery worker(s).")
                else:
                    logger.info("No active Celery workers found to send log level update task.")

                return HttpResponse(headers={"HX-Trigger-After-Settle": json.dumps(
                    {"showSuccessAlert": f"Log level updated to {new_level_str}.", "closeModal": True}
                )})
            except Exception as e:
                logger.error(f"Error updating log levels: {e}", exc_info=True)
                return HttpResponse(status=500, headers={
                    "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Error: {str(e)}"})})
        else:
            logger.warning(f"Invalid log level '{new_level_str}' provided.")
            return HttpResponse("Invalid log level.", status=400)

    context = {
        'current_log_level': site_settings.logging_level,
        'log_levels': LOG_LEVEL_NAMES,
    }
    return render(request, 'partials/log_settings.html', context)


@staff_member_required
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
    settings_instance, _ = SiteSettings.objects.get_or_create(pk=1)

    if request.method == "POST":
        form = SiteSettingsForm(request.POST, instance=settings_instance)
        action = request.POST.get("action")
        logger.debug(f"POST action='{action}'")

        if form.is_valid():
            logger.debug("Form valid.")
            if action == "save":
                try:
                    # Handle empty password - keep existing if not changed
                    if not form.cleaned_data.get("email_password"):
                        form.cleaned_data["email_password"] = settings_instance.email_password
                        logger.debug("Password field empty - keeping existing password.")

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

                # Use existing password if current submission is empty
                email_password = config.get("email_password") or settings_instance.email_password

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
    site_settings = SiteSettings.objects.first()
    if not site_settings or not site_settings.email_host:
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
        return HttpResponse(status=404,
                            headers={
                                "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Portal not found."})})
    except Exception as e:
        logger.error(f"Error getting/creating settings for '{instance}': {e}", exc_info=True)
        return HttpResponse(status=500, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Error loading tool settings."})})

    # Check prerequisites
    site_settings, _ = SiteSettings.objects.get_or_create(pk=1)
    webhook_configured = bool(site_settings.webhook_secret)
    email_configured = bool(site_settings.email_host)

    results = TaskResult.objects.filter(task_kwargs__icontains=f"'{instance}'", task_name__icontains="tool").order_by(
        '-date_done')[:10]

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
            return render(request, "partials/portal_tools_form.html", context, status=400)
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
    tool_settings_obj, _ = PortalToolSettings.objects.get_or_create(portal=portal)

    form = ToolsForm(request.POST, instance=tool_settings_obj)
    if not form.is_valid():
        error_msg = ". ".join([f"{field}: {', '.join(errors)}" for field, errors in form.errors.items()])
        logger.warning(f"Invalid parameters for tool '{tool_name}': {form.errors.as_json()}")
        return HttpResponse(status=200, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Invalid tool parameters: {error_msg}"})
        })

    site_settings, _ = SiteSettings.objects.get_or_create(pk=1)

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
        task = config['task'].delay(**task_params)
        logger.info(f"Successfully queued tool '{tool_display_name}' for instance '{instance}'. Task ID: {task.id}")

        response_data = {
            "instance": instance,
            "task_id": task.id,
            "value": 0,
            "task_name": tool_display_name,
        }

        response = render(request, "partials/progress_bar.html", context=response_data)
        return response

    except Exception as e:
        logger.error(f"Failed to queue tool '{tool_name}' for '{instance}': {e}", exc_info=True)
        return HttpResponse(status=200, headers={
            "HX-Trigger-After-Settle": json.dumps(
                {"showDangerAlert": "Failed to queue the tool. See logs for details."})
        })


@staff_member_required
def webhook_settings_view(request):
    """Configure webhook settings for the site."""
    site_settings, _ = SiteSettings.objects.get_or_create(pk=1)

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
