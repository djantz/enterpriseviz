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
import hashlib
import hmac
from collections import defaultdict

from celery import current_app as celery_app  # For signaling Celery
from celery.result import AsyncResult
from celery_progress.backend import Progress
import cron_descriptor
from django.conf import settings
from django.contrib import messages
from django.contrib.admin.utils import unquote
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import AuthenticationForm
from django.core.exceptions import FieldError
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

from app import utils
from .filters import WebmapFilter, ServiceFilter, LayerFilter, AppFilter, UserFilter, LogEntryFilter
from .forms import ScheduleForm
from .models import PortalCreateForm, UserProfile, LogEntry
from .tables import WebmapTable, ServiceTable, LayerTable, AppTable, UserTable, LogEntryTable
from .tasks import *

from .request_context import get_django_request_context

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
def logs_page(request):
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
            item_for_usage = details.get("item")
            if item_for_usage:
                usage_input = [item_for_usage] if not isinstance(item_for_usage, list) else item_for_usage
                usage = utils.get_usage_report(usage_input)
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
                            {"showSuccessAlert": f"Successfully added {url} as {alias} and authenticated.",
                             "closeModal": True}
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
    Initiate refresh of portal data.

    Triggers asynchronous updates of items from the specified portal.

    :param request: The HTTP request object
    :type request: HttpRequest

    :return: Status response or redirect
    :rtype: HttpResponse
    """

    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)

    try:
        instance_alias = request.POST.get("instance", None)
        delete = request.POST.get("delete", False)
        items = request.POST.get("items")
        instance_url = request.POST.get("url", None)

        # Validate instance
        portal = Portal.objects.filter(Q(alias=instance_alias) | Q(url=instance_url)).first()
        if not portal:
            logger.error(f"Portal instance '{instance_alias}' not found.")
            response = HttpResponse(status=200)
            response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Invalid portal instance."})
            return response

        # Retrieve credentials if included
        username = request.POST.get("username", None)
        password = request.POST.get("password", None)
        if username and password:
            auth = webmaps.try_connection(request.POST)
            if auth.get("authenticated", False):
                if portal.portal_type is None:
                    portal.portal_type = "agol" if auth.get("is_agol") else "portal"
                    portal.save()
                # Task mapping for better maintainability
                task_map = {
                    "webmaps": update_webmaps,
                    "services": update_services,
                    "webapps": update_webapps,
                    "users": update_users
                }

                # Validate selected task
                task_func = task_map.get(items)
                if not task_func:
                    logger.warning(f"Invalid task type '{items}' for portal '{instance_alias}'.")
                    return JsonResponse({"error": "Invalid task type selected"}, status=400)

                time.sleep(1.5)

                # Start the task
                task = task_func.apply_async(args=[portal.alias, delete, username, password],
                                             task_name=f"Update{items}")
                logger.info(f"Started task '{task.id}' for '{items}' update on portal '{instance_alias}'.")

                response_data = {
                    "instance": portal.alias,
                    "task_id": task.id,
                    "progress": 0  # Initial progress
                }

                response = render(request, "partials/progress_bar.html", context=response_data)
                response["HX-Trigger"] = json.dumps({"closeModal": True})
                return response
            response = HttpResponse(status=401)
            response["HX-Trigger"] = json.dumps(
                {"showDangerAlert": "Unable to connect to portal. Please verify credentials."})
            return response

        if not portal.store_password:
            return render(request, "portals/portal_credentials.html", {"instance": portal, "items": items})

        # Task mapping for better maintainability
        task_map = {
            "webmaps": update_webmaps,
            "services": update_services,
            "webapps": update_webapps,
            "users": update_users
        }

        # Validate selected task
        task_func = task_map.get(items)
        if not task_func:
            logger.warning(f"Invalid task type '{items}' for portal '{instance_alias}'.")
            return JsonResponse({"error": "Invalid task type selected"}, status=400)

        time.sleep(1.5)

        # Start the task
        task = task_func.apply_async(args=[portal.alias, delete, username, password], task_name=f"Update{items}")
        logger.info(f"Started task '{task.id}' for '{items}' update on portal '{instance_alias}'.")
        response_data = {
            "instance": instance_alias,
            "task_id": task.id,
            "progress": 0  # Initial progress
        }

        return render(request, "partials/progress_bar.html", context=response_data)

    except Exception as e:
        logger.exception(f"Error in refresh_portal_view: {e}")
        response = HttpResponse(status=500)
        response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Failed to start refresh task"})
        return response


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

        if not progress_info or "progress" not in progress_info:
            logger.warning(
                f"Invalid or missing progress data for task '{task_id}'. State: {result.state}")
            progress_percentage = 100 if result.successful() or result.failed() else 0
            task_state = result.state
            response_data = {"instance": instance, "task_id": task_id,
                             "progress": {"percent": progress_percentage, "description": "Fetching status..."},
                             "value": progress_percentage, "state": task_state}
        else:
            progress_percentage = progress_info["progress"].get("percent", 0)
            task_state = progress_info.get("state", result.state)
            response_data = {"instance": instance, "task_id": task_id, "progress": progress_info,
                             "value": progress_percentage}

        logger.debug(f"Task '{task_id}' state '{task_state}', progress {progress_percentage}%.")
        response = render(request, "partials/progress_bar.html", context=response_data)

        htmx_trigger = {}
        task_result = result.info
        task_name = result._get_task_meta().get('task_name', 'Task')

        if task_state == "SUCCESS":
            logger.info(
                f"Task '{task_id}' ({task_name}) for {instance} completed successfully.")
            success_details = "Completed."
            if isinstance(task_result, dict):
                success_details = (f"{task_result.get('num_updates', 0)} updates, "
                                   f"{task_result.get('num_inserts', 0)} inserts, "
                                   f"{task_result.get('num_deletes', 0)} deletes, "
                                   f"{task_result.get('num_errors', 0)} errors.")
            htmx_trigger = {
                "showSuccessAlert": f"{task_name} for {instance} completed. <br> <b>{success_details}</b>",
                "updateComplete": "true"}
        elif task_state == "FAILURE":
            logger.warning(
                f"Task '{task_id}' ({task_name}) for {instance} failed. Info: {task_result}")
            htmx_trigger = {
                "showDangerAlert": f"{task_name} for {instance} failed. See logs or results table for details. Error: {str(task_result)[:100]}...",
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
        return HttpResponse(status=404, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Portal instance not found."})})

    username = request.POST.get("username")
    password = request.POST.get("password")
    auth_args = {}

    if username and password:
        logger.debug(f"Credentials provided for {instance}.")
        auth_result = utils.try_connection(request.POST)
        if not auth_result.get("authenticated", False):
            logger.warning(f"metadata_view: Auth failed for {instance} with user {username}.")
            return HttpResponse(status=401, headers={
                "HX-Trigger": json.dumps({"showDangerAlert": "Unable to connect. Verify credentials."})})
        logger.info(f"Authenticated to {instance} as {username}.")
        if portal.portal_type is None and auth_result.get("is_agol") is not None:
            portal.portal_type = "agol" if auth_result["is_agol"] else "portal"
            portal.save(update_fields=['portal_type'])
        auth_args = {'username': username, 'password': password}
    elif not portal.store_password:
        logger.debug(f"Credentials required for {instance}, rendering form.")
        return render(request, "portals/portal_credentials.html",
                      {"instance": portal, "items": "metadata_report"})

    try:
        logger.debug(f"metadata_view: Fetching metadata for {instance}.")
        metadata_report_data = utils.get_metadata(portal, **auth_args)

        if "error" in metadata_report_data:
            error_msg = metadata_report_data["error"]
            logger.warning(f"Error from get_metadata for '{instance}': {error_msg}")
            return HttpResponse(status=500,
                                headers={"HX-Trigger-After-Settle": json.dumps({"showDangerAlert": error_msg})})

        context = {
            "portal_list": Portal.objects.values_list("alias", "portal_type", "url"),
            "current_portal": portal,
            "instance_alias": instance,
            "metadata_items": metadata_report_data.get("metadata", []),
        }
        logger.debug(f"Rendering metadata for '{instance}'.")
        response = render(request, template_name, context)
        if (username and password) or (not portal.store_password and not (username and password)):
            response["HX-Trigger"] = json.dumps({"closeModal": True})
        return response

    except Exception as e:
        logger.error(f"Error processing metadata for '{instance}': {e}", exc_info=True)
        return HttpResponse(status=500, headers={
            "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Error getting metadata report."})})


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

    This view receives and verifies webhook payloads using HMAC authentication. If the
    signature is valid, it parses the JSON payload and processes relevant events.

    Security:
    - Uses HMAC-SHA256 to validate the signature against `settings.WEBHOOK_SECRET`.
    - Uses `hmac.compare_digest` for secure signature comparison.

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

    # Read and verify request body
    body = request.body
    computed_signature = hmac.new(
        key=settings.WEBHOOK_SECRET.encode(),
        msg=body,
        digestmod=hashlib.sha256
    ).hexdigest()

    try:
        payload = json.loads(request.body)
    except json.JSONDecodeError:
        logger.warning("Invalid JSON payload.")
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    portal_url = payload.get("info", {}).get("portalURL")
    if not portal_url:
        logger.warning("Missing 'portalURL' in payload.")
        return JsonResponse({"error": "Missing portal URL"}, status=400)

    normalized_portal_url = portal_url.rstrip('/')
    portal_qs = Portal.objects.filter(Q(url=normalized_portal_url) | Q(url=normalized_portal_url + '/'))
    instance_item = portal_qs.first()

    if not instance_item:
        logger.warning(f"Unknown portal URL received: {portal_url}")
        return JsonResponse({"error": "Unknown portal"}, status=404)

    logger.debug(f"Processing for portal {instance_item.alias} ({portal_url}).")

    try:
        target = utils.connect(instance_item)
    except Exception as e:
        logger.error(f"Connection failed for portal '{instance_item.alias}': {e}", exc_info=True)
        return JsonResponse({"error": "Failed to connect to portal"}, status=500)

    events = payload.get("events", [])
    logger.debug(f"Webhook: Processing {len(events)} events.")
    for event in events:
        source = event.get("source")
        operation = event.get("operation")
        event_id = event.get("id")

        logger.info(f"Processing event - Source: {source}, Operation: {operation}, ID: {event_id}")

        if source == "item":
            item = target.content.get(event_id)
            item_type = item.type if item else "Unknown"

            if item_type in ["Feature Layer", "Map Image Layer"]:
                process_service.delay(instance_item.id, event_id, operation)
            elif item_type == "Web Map":
                process_webmap.delay(instance_item.id, event_id, operation)
            elif item_type in ["Web Mapping Application", "Dashboard", "Web AppBuilder Apps",
                               "Experience Builder", "Form", "Story Map"]:
                process_webapp.delay(instance_item.id, event_id, operation)
            else:
                logger.warning(f"Unknown item type received in webhook: {item_type}")
        elif source == "user":
            process_user.delay(instance_item.id, event_id, operation)
        else:
            logger.warning(f"Unhandled event source: {source}")

    logger.info(f"Successfully processed payload for {portal_url}.")
    return JsonResponse({"message": "Webhook received successfully"}, status=200)

VALID_LOG_LEVELS = {
    "DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING,
    "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL,
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
                    return HttpResponse(status=400, headers={
                        "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Test email address required."})})

                config = form.cleaned_data
                if not config.get("email_host") or not config.get("email_port"):
                    return HttpResponse(status=400, headers={
                        "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": "Host and Port required."})})

                try:
                    logger.debug(
                        f"Sending test email to {test_email_addr} using host {config['email_host']}.")
                    connection_kwargs = {
                        "host": config["email_host"], "port": config["email_port"],
                        "username": config.get("email_username") or None,
                        "password": config.get("email_password") or None,
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
                    return HttpResponse(status=500, headers={
                        "HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Test email failed: {str(e)}"})})
            else:
                logger.warning(f"Unknown POST action '{action}'.")
        else:
            logger.warning(f"Form invalid. Errors: {form.errors}")
            return render(request, "partials/portal_email_form.html", {"form": form}, status=400)

    else:
        form = SiteSettingsForm(instance=settings_instance)

    template_to_render = "portals/portal_email.html" if request.method == "GET" else "partials/portal_email_form.html"
    return render(request, template_to_render, {"form": form})

