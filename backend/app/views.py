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

from app import webmaps
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
from django.db import transaction, DatabaseError
from django.db.models import Q
from django.http import HttpResponse, JsonResponse, HttpResponseForbidden
from django.shortcuts import render, redirect, get_object_or_404
from django.template import loader
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django_celery_results.models import TaskResult
from django_filters.views import FilterView
from django_tables2 import SingleTableMixin, RequestConfig
from django_tables2.export.views import ExportMixin

from .filters import WebmapFilter, ServiceFilter, LayerFilter, AppFilter, UserFilter
from .forms import ScheduleForm
from .models import PortalCreateForm, UserProfile
from .tables import WebmapTable, ServiceTable, LayerTable, AppTable, UserTable
from .tasks import *

logger = logging.getLogger(__name__)


class Table(ExportMixin, SingleTableMixin, FilterView):
    """
    Handles the dynamic rendering of tables, filters, and data based on the type of model. This class is
    intended to provide a unified interface for generating Django tables and filtersets with paginated
    output, configurable per model type.

    The class dynamically adjusts its model, table class, and filterset class based on a model type
    passed in the URL. The corresponding data can also be filtered based on a specific instance when
    provided. This flexibility allows the reuse of the same view for multiple types of data objects.

    :ivar model: The Django model associated with the view. Dynamically determined based on the URL.
    :type model: Model
    :ivar table_class: The Django table class used to render the data for the model.
    :type table_class: Table
    :ivar filterset_class: The filterset class used for querying and filtering data for the model.
    :type filterset_class: FilterSet
    :ivar template_name: The name of the template used to render tables.
    :type template_name: str
    :ivar paginate_by: Defines the number of items to display per page in the generated table.
    :type paginate_by: int
    """
    model = User
    table_class = UserTable
    filterset_class = UserFilter
    template_name = "partials/table.html"
    paginate_by = 10

    def get_filterset_class(self):
        model_type = self.kwargs["name"]
        if model_type == "webmap":
            self.model = Webmap
            self.table_class = WebmapTable
            self.filterset_class = WebmapFilter
            return WebmapFilter
        if model_type == "service":
            self.model = Service
            self.table_class = ServiceTable
            self.filterset_class = ServiceFilter
            return ServiceFilter
        if model_type == "layer":
            self.model = Layer
            self.table_class = LayerTable
            self.filterset_class = LayerFilter
            return LayerFilter
        if model_type == "app":
            self.model = App
            self.table_class = AppTable
            self.filterset_class = AppFilter
            return AppFilter
        if model_type == "user":
            self.model = User
            self.table_class = UserTable
            self.filterset_class = UserFilter
            return UserFilter

    def get_queryset(self):
        """
        Returns the queryset to be displayed based on the model type in the URL.
        """
        model_type = self.kwargs["name"]
        instance = None
        if "instance" in self.kwargs:
            instance = self.kwargs["instance"]
        if model_type == "webmap":
            self.model = Webmap
            self.table_class = WebmapTable
            self.filterset_class = WebmapFilter
            if instance:
                return Webmap.objects.filter(portal_instance__alias=instance)
            return Webmap.objects.all()
        if model_type == "service":
            self.model = Service
            self.table_class = ServiceTable
            self.filterset_class = ServiceFilter
            if instance:
                return Service.objects.filter(portal_instance__alias=instance)
            return Service.objects.all()
        if model_type == "layer":
            self.model = Layer
            self.table_class = LayerTable
            self.filterset_class = LayerFilter
            if instance:
                return Layer.objects.filter(portal_instance__alias=instance)
            return Layer.objects.all()
        if model_type == "app":
            self.model = App
            self.table_class = AppTable
            self.filterset_class = AppFilter
            if instance:
                return App.objects.filter(portal_instance__alias=instance)
            return App.objects.all()
        if model_type == "user":
            self.model = User
            self.table_class = UserTable
            self.filterset_class = UserFilter
            if instance:
                return User.objects.filter(portal_instance__alias=instance)
            return User.objects.all()

    def get_context_data(self, **kwargs):
        """
        Adds the filter and table to the context.
        """
        model_type = self.kwargs["name"]
        if model_type == "webmap":
            self.model = Webmap
            self.table_class = WebmapTable
            self.filterset_class = WebmapFilter
        if model_type == "service":
            self.model = Service
            self.table_class = ServiceTable
            self.filterset_class = ServiceFilter
        if model_type == "layer":
            self.model = Layer
            self.table_class = LayerTable
            self.filterset_class = LayerFilter
        if model_type == "app":
            self.model = App
            self.table_class = AppTable
            self.filterset_class = AppFilter
        if model_type == "user":
            self.model = User
            self.table_class = UserTable
            self.filterset_class = UserFilter
        # Apply filtering to the queryset
        context = super().get_context_data(**kwargs)
        page = context["page_obj"]
        context["paginator_range"] = page.paginator.get_elided_page_range(page.number, on_each_side=1, on_ends=1)
        filterset = self.filterset_class(self.request.GET, queryset=self.get_queryset())
        context["filter"] = filterset
        table = self.table_class(filterset.qs)
        RequestConfig(self.request, paginate={
            "per_page": 10,
        }).configure(table)
        context.update({"table": table})
        context.update({"table_name": model_type})
        if "instance" in self.kwargs:
            context.update({"instance": self.kwargs["instance"]})

        return context


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
    # Determine the correct template
    template = (
        "portals/portal_detail_map.html"
        if request.htmx
        else "portals/portal_detail_map_full.html"
    )

    # Validate map ID
    if not id:
        logger.error("Missing 'id' parameter in request.")
        response = HttpResponse(status=400)
        response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Error retrieving map details"})
        return response

    try:
        # Fetch map details
        details = webmaps.map_details(id)
        if "error" in details:
            response = HttpResponse(status=500)
            response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": details["error"]})
            return response

    except Exception as e:
        logger.exception(f"Error retrieving map details for ID {id}: {e}")
        response = HttpResponse(status=500)
        response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Error retrieving map details"})
        return response

    # Optimize portal data retrieval
    portal_data = Portal.objects.values_list("alias", "portal_type", "url")

    # Construct context dynamically
    details["portal"] = portal_data
    context = details

    return render(request, template, context)


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
    # Determine the correct template
    template = (
        "portals/portal_detail_service.html"
        if request.htmx
        else "portals/portal_detail_service_full.html"
    )

    # Validate instance and URL
    if not instance or not url:
        logger.error("Missing 'instance' or 'url' parameter in request.")
        response = HttpResponse(status=400)
        response["HX-Trigger-After-Settle"] = json.dumps(
            {"showDangerAlert": "Missing 'instance' or 'url' parameter in request."})
        return response

    try:
        # Fetch service details
        details = webmaps.service_details(instance, unquote(url))
        if "error" in details:
            response = HttpResponse(status=500)
            response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": details["error"]})
            return response

    except Exception as e:
        logger.exception(f"Error retrieving service details for {instance}: {e}")
        response = HttpResponse(status=500)
        response["HX-Trigger-After-Settle"] = json.dumps(
            {"showDangerAlert": f"Error retrieving service details for {instance}."})
        return response

    # Optimize portal data retrieval
    portal_data = Portal.objects.values_list("alias", "portal_type", "url")

    # Determine service usage
    service_usage = None
    if hasattr(request.user, "profile") and getattr(request.user.profile, "service_usage", False):
        try:
            usage = webmaps.get_usage_report([details.get("item")])
            if "error" in usage:
                response = HttpResponse(status=500)
                response["HX-Trigger-After-Settle"] = json.dumps(
                    {"showDangerAlert": usage["error"]})
                return response
            service_usage = usage["usage"]
        except Exception as e:
            logger.exception(f"Error retrieving service usage report: {e}")

    # Construct context dynamically
    details["portal"] = portal_data
    details["service_usage"] = service_usage
    context = details

    return render(request, template, context)


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
    template = (
        "portals/portal_detail_layer.html"
        if request.htmx
        else "portals/portal_detail_layer_full.html"
    )

    # Retrieve parameters safely
    version = request.GET.get("version", "")
    dbserver = request.GET.get("server", "")
    database = request.GET.get("database", "")

    if not name:
        logger.error("Layer name is missing in request.")
        response = HttpResponse(status=400)
        response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Layer name is missing in request."})
        return response

    try:
        # Fetch layer details
        details = webmaps.layer_details(dbserver, database, version, name)
        if "error" in details:
            response = HttpResponse(status=500)
            response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": details["error"]})
            return response
    except Exception as e:
        logger.exception(f"Error retrieving layer details for {name}: {e}")
        response = HttpResponse(status=500)
        response["HX-Trigger-After-Settle"] = json.dumps(
            {"showDangerAlert": f"Error retrieving layer details for {name}."})
        return response

    # Optimize portal data retrieval
    portal_data = list(Portal.objects.values_list("alias", "portal_type", "url"))

    # Determine service usage
    service_usage = None
    services = details["services"]
    if hasattr(request.user, "profile") and getattr(request.user.profile, "service_usage", False):
        try:
            usage = webmaps.get_usage_report(services)
            if "error" in usage:
                response = HttpResponse(status=500)
                response["HX-Trigger-After-Settle"] = json.dumps(
                    {"showDangerAlert": usage["error"]})
                return response
            service_usage = usage["usage"]
        except Exception as e:
            logger.exception(f"Error retrieving service usage report: {e}")

    # Construct context dynamically
    details["portal"] = portal_data
    details["service_usage"] = service_usage
    context = details

    return render(request, template, context)


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

    if request.method == "POST":
        form = PortalCreateForm(request.POST)

        if form.is_valid():
            store_password = form.cleaned_data["store_password"]
            alias = form.cleaned_data["alias"]
            url = form.cleaned_data["url"]

            if store_password:
                logger.info(f"Attempting connection for {alias} ({url})")
                if webmaps.try_connection(form.cleaned_data).get("authenticated", False):
                    form.save()
                    context = {"portal": Portal.objects.values_list("alias", "portal_type", "url")}
                    response = render(request,
                                      "partials/portal_updates.html",
                                      context)
                    response["HX-Trigger-After-Settle"] = json.dumps(
                        {"showSuccessAlert": f"Successfully added {url} as {alias} and authenticated.",
                         "closeModal": True})
                    return response
                else:
                    logger.warning(f"Connection failed for {alias} ({url})")
                    context = {"form": form}
                    response = render(request,
                                      "partials/portal_add_form.html",
                                      context, status=401)
                    response["HX-Trigger-After-Settle"] = json.dumps({
                        "showDangerAlert": f"Unable to connect to {form.cleaned_data['url']} as {form.cleaned_data['username']}. Please verify credentials."})
                    return response
            else:
                form.save()
                context = {"portal": Portal.objects.values_list("alias", "portal_type", "url")}
                response = render(request, "partials/portal_updates.html",
                                  context)
                response["HX-Trigger-After-Settle"] = json.dumps({
                    "showSuccessAlert": f"Successfully added {url} as {alias}. Authentication will be required when refreshing data.",
                    "closeModal": True})
                return response

        logger.error("Form validation failed for portal creation.")
        context = {"form": form}
        response = render(request, "partials/portal_add_form.html",
                          context)
        return response

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
    # Select the template based on HTMX request
    template = "app/index.html" if request.htmx else "app/index_full.html"
    webmaps = Webmap.objects.filter(Q(portal_instance=instance) if instance else Q()).count()
    portals = Portal.objects.values_list("alias", "portal_type", "url")
    services = Service.objects.filter(Q(portal_instance=instance) if instance else Q()).count()
    layers = Layer.objects.filter(Q(portal_instance=instance) if instance else Q()).count()
    apps = App.objects.filter(Q(portal_instance=instance) if instance else Q()).count()
    users = User.objects.filter(Q(portal_instance=instance) if instance else Q()).count()
    instance_item = Portal.objects.get(alias=instance) if instance else None

    context = {
        "webmaps": webmaps,
        "services": services,
        "layers": layers,
        "apps": apps,
        "users": users,
        "portal": portals,
        "instance": instance_item
    }

    return render(request, template, context)


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

    try:
        result = AsyncResult(task_id)
        if not result:
            logger.error(f"Task ID '{task_id}' not found for instance '{instance}'.")
            return JsonResponse({"error": "Task not found"}, status=404)

        progress = Progress(result).get_info()

        if not progress or "progress" not in progress:
            logger.warning(f"Invalid progress data for task '{task_id}'.")
            return JsonResponse({"error": "Invalid task progress data"}, status=500)

        progress_percentage = progress["progress"].get("percent", 0)
        task_state = progress.get("state", "PENDING")

        logger.info(
            f"Task '{task_id}' for instance '{instance}' is in state '{task_state}' at {progress_percentage}%.")

        response_data = {
            "instance": instance,
            "task_id": task_id,
            "progress": progress,
            "value": progress_percentage
        }
        response = render(request, "partials/progress_bar.html", context=response_data)
        # Check task completion and trigger UI updates
        htmx_trigger = {}
        if task_state == "SUCCESS":
            htmx_trigger = {
                "showSuccessAlert": f"{result._get_task_meta().get('task_name')} for {instance} completed. <br> <b>{result.result['num_updates']}</b> updates, <b>{result.result['num_inserts']}</b> inserts, <b>{result.result['num_deletes']}</b> deletes, <b>{result.result['num_errors']}</b> errors",
                "updateComplete": "true"}
        elif task_state == "FAILURE":
            htmx_trigger = {
                "showDangerAlert": f"{result._get_task_meta().get('task_name')} for {instance} failed. See errors in the results table.",
                "updateComplete": "true"}

        if htmx_trigger:
            response["HX-Trigger"] = json.dumps(htmx_trigger)

        return response

    except Exception as e:
        logger.exception(f"Error retrieving progress for task '{task_id}': {e}")
        response = HttpResponse(status=500)
        response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Failed to retrieve task progress"})
        return response


def login_view(request):
    """
    Handle user authentication.

    Processes login requests and authenticates users.

    :param request: The HTTP request object
    :type request: HttpRequest

    :return: Redirect to dashboard or login form with errors
    :rtype: HttpResponse
    """

    form = AuthenticationForm(request, data=request.POST)

    if request.method == "POST":
        # Introduce a small delay to mitigate brute-force attacks
        time.sleep(1.5)

        if form.is_valid():
            username = form.cleaned_data.get("username")
            password = form.cleaned_data.get("password")
            user = authenticate(request, username=username, password=password)

            if user is not None:
                login(request, user)
                messages.success(request, f"Welcome back, {username}!")

                # Log successful login
                logger.info(f"User '{username}' logged in successfully.")

                return redirect("/enterpriseviz/")

            else:
                messages.error(request, "Invalid credentials.")
                logger.warning(f"Failed login attempt for username: {username}")
        else:
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

    logout(request)
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

    try:
        portal = Portal.objects.get(alias=instance)

        with transaction.atomic():
            portal.delete()

        updated_portals = list(Portal.objects.values_list("alias", "portal_type", "url"))

        response = render(
            request,
            "partials/portal_updates.html",
            context={"portal": updated_portals}
        )
        response["HX-Trigger-After-Settle"] = json.dumps(
            {"showSuccessAlert": f"Successfully deleted portal '{instance}' and related data."})
        return response

    except Portal.DoesNotExist:
        logger.warning(f"Attempted to delete non-existent portal: {instance}")
        return HttpResponse(
            status=404,
            headers={"HX-Trigger-After-Settle": json.dumps({"showDangerAlert": f"Portal '{instance}' not found."})}
        )

    except DatabaseError as e:
        logger.error(f"Database error while deleting portal '{instance}': {e}")
        return HttpResponse(
            status=500,
            headers={"HX-Trigger-After-Settle": json.dumps(
                {"showDangerAlert": f"Database error occurred while deleting portal '{instance}'."})}
        )

    except Exception as e:
        logger.error(f"Unexpected error deleting portal '{instance}': {e}")
        return HttpResponse(
            status=400,
            headers={"HX-Trigger-After-Settle": json.dumps(
                {"showDangerAlert": f"Could not delete portal '{instance}'. Please try again later."})}
        )


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

    try:
        portal = Portal.objects.get(alias=instance)
    except Portal.DoesNotExist:
        return HttpResponse(status=404,
                            headers={"HX-Trigger-After-Settle": json.dumps(
                                {"showDangerAlert": f"Portal {instance} not found."})})

    form = PortalCreateForm(request.POST or None, instance=portal)

    if request.method == "POST":
        time.sleep(1.5)

        if form.is_valid():
            store_password = form.cleaned_data["store_password"]
            requires_auth = store_password and form.cleaned_data.get("password")

            if requires_auth and not webmaps.try_connection(form.cleaned_data).get("authenticated", False):
                response = render(
                    request, "portals/portal_update.html",
                    {"form": form, "instance": instance}, status=401)
                response["HX-Trigger-After-Settle"] = json.dumps({
                    "showDangerAlert": f"Unable to connect to {form.cleaned_data['url']} as {form.cleaned_data['username']}. Please verify credentials."})
                return response

            form.save()

            response = render(request, "partials/portal_updates.html",
                              context={"portal": Portal.objects.values_list("alias", "portal_type", "url"),
                                       "instance": instance})
            success_message = f"Successfully updated {form.cleaned_data['url']} as {form.cleaned_data['alias']}."
            if requires_auth:
                success_message += f" Authentication as {form.cleaned_data['username']} updated."

            response["HX-Trigger-After-Settle"] = json.dumps({"showSuccessAlert": success_message, "closeModal": True})
            return response
        response = render(request, "portals/portal_update.html",
                          {"form": form, "instance": instance}, status=200)
        response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Form not valid"})
        return response

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

    filter_instance = f"'{instance}'"
    results = TaskResult.objects.filter(task_args__icontains=filter_instance).exclude(
        task_name__icontains="batch")[:10]

    # Retrieve the portal instance or return an error response
    try:
        portal = Portal.objects.get(alias=instance)
    except Portal.DoesNotExist:
        logger.error(f"Portal '{instance}' not found.")
        response = render(request, "partials/portal_schedule_form.html",
                          context={"form": ScheduleForm(), "closeModal": True})
        response["HX-Trigger-After-Settle"] = json.dumps({"showDanger": f"Portal '{instance}' not found."})
        return response
    store_password = portal.store_password
    # If deleting a scheduled task
    if request.method == "DELETE":
        try:
            if portal.task:
                portal.task.delete()  # Remove the scheduled task
                portal.task = None
                portal.save()
            form = ScheduleForm(initial={"instance": portal})
            response = render(request, "partials/portal_schedule_form.html", {"form": form})
            response["HX-Trigger-After-Settle"] = json.dumps({"closeModal": True})
            return response

        except Exception as e:
            logger.exception(f"Error deleting scheduled task for portal '{instance}': {e}")
            response = render(request, "partials/portal_schedule_form.html", {"form": form})
            return response

    # Extract task scheduling details if available
    description = ""
    initial = {"instance": instance}

    if portal.task:
        task = portal.task
        crontab = task.crontab
        description = cron_descriptor.get_description(f"{crontab.minute} {crontab.hour} {crontab.day_of_month} "
                                      f"{crontab.month_of_year} {crontab.day_of_week}")

        if task.expires:
            description += f" until {task.expires.strftime('%Y-%m-%d')}"

        # Extract stored task items
        try:
            initial = json.loads(task.kwargs)
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning(f"Invalid task kwargs for portal '{instance}'. Resetting items.")
            initial = {"instance": instance}

    # Initialize the form with either submitted data or existing task details
    form = ScheduleForm(request.POST or None, initial=initial)
    if request.method == "POST":
        if form.is_valid():
            # Save the new or updated task
            task_instance, error = form.save_task()

            if error:
                logger.error(f"Task scheduling error: {error}")
                response = render(request, "partials/portal_schedule_form.html", {"form": form})
                return response

            crontab = task_instance.crontab
            description = cron_descriptor.get_description(f"{crontab.minute} {crontab.hour} {crontab.day_of_month} "
                                          f"{crontab.month_of_year} {crontab.day_of_week}")
            response = render(request, "partials/portal_schedule_form.html", {"form": form})
            response["HX-Trigger-After-Settle"] = json.dumps(
                {"showSuccessAlert": f"Updates scheduled for portal '{instance}': {description}",
                 "closeModal": True})
            return response

        # Handle form validation errors
        response = render(request, "partials/portal_schedule_form.html", {"form": form})
        return response

    # Render the page with existing schedule data
    context = {"form": form, "description": description, "results": results, "enable": store_password}
    return render(request, "portals/portal_schedule.html", context)


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

    try:
        instance_item = Portal.objects.get(alias=instance)
    except Portal.DoesNotExist:
        response = HttpResponse(status=200)
        response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Portal not found."})
        return response
    similarity_threshold = 70

    # Call the get_duplicates function and retrieve results
    duplicates_data = webmaps.get_duplicates(instance_item, similarity_threshold=similarity_threshold)

    # Check if an error was returned
    if "error" in duplicates_data:
        response = HttpResponse(status=200)
        response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": duplicates_data["error"]})
        return response

    # Extract duplicate lists from the returned dictionary
    duplicate_webmaps = duplicates_data.get("webmaps", [])
    duplicate_services = duplicates_data.get("services", [])
    duplicate_layers = duplicates_data.get("layers", [])
    duplicate_apps = duplicates_data.get("apps", [])

    response = render(request, "portals/portal_duplicates.html", {
        "duplicate_webmaps": duplicate_webmaps,
        "duplicate_services": duplicate_services,
        "duplicate_layers": duplicate_layers,
        "duplicate_apps": duplicate_apps,
    })

    return response


@login_required
def unused_view(request, instance):
    """
    Display unused services.

    Lists services or layers that are not configured in any webmaps or apps.

    :param request: The HTTP request object
    :type request: HttpRequest
    :param instance: Portal instance alias
    :type instance: str

    :return: Rendered template with unused component data
    :rtype: HttpResponse
    """

    # Choose template based on HTMX request
    template_name = "portals/portal_unused.html" if request.htmx else "portals/portal_unused_full.html"

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
                try:
                    unused_services = webmaps.get_unused(portal, username, password)
                    if "error" in unused_services:
                        response = HttpResponse(status=500)
                        response["HX-Trigger-After-Settle"] = json.dumps(
                            {"showDangerAlert": "Failed to retrieve unused services."})
                        return response
                except Exception as e:
                    logger.error(f"Error fetching unused services for {instance}: {e}")

                # Construct context
                context = {
                    "portal": Portal.objects.values("alias", "portal_type", "url"),
                    "instance": instance,
                    "services": unused_services.get("unused_services", [])
                }
                response = render(request, template_name, context)
                response["HX-Trigger"] = json.dumps({"closeModal": True})
                return response
            response = HttpResponse(status=401)
            response["HX-Trigger"] = json.dumps(
                {"showDangerAlert": "Unable to connect to portal. Please verify credentials."})
            return response

        if not portal.store_password:
            return render(request, "portals/portal_credentials.html", {"instance": portal, "items": items})
    except Exception as e:
        logger.exception(f"Error in refresh_portal_view: {e}")
        response = HttpResponse(status=500)
        response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Failed to start refresh task"})
        return response
    # Fetch unused services
    try:
        unused_services = webmaps.get_unused(portal)
        if "error" in unused_services:
            response = HttpResponse(status=500)
            response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Failed to retrieve unused services."})
            return response
    except Exception as e:
        logger.error(f"Error fetching unused services for {instance}: {e}")

    # Construct context
    context = {
        "portal": Portal.objects.values("alias", "portal_type", "url"),
        "instance": instance,
        "services": unused_services.get("unused_services", [])
    }

    return render(request, template_name, context)


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

    # Choose template based on HTMX request
    template_name = "portals/portal_detail_layerid.html" if request.htmx else "portals/portal_detail_layerid_full.html"

    # Retrieve portal instance or return 404 if not found
    portal = get_object_or_404(Portal, alias=instance)

    # Default values
    maps, apps, l_url = [], [], None

    # Process form submission
    if "layerId_input" in request.POST:
        l_url = unquote(request.POST.get("layerId_input"))
        try:
            layer_usage = webmaps.find_layer_usage(portal, l_url)
            if "error" in layer_usage:
                response = HttpResponse()
                response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": layer_usage["error"]})
                return response
            maps = layer_usage["maps"]
            apps = layer_usage["apps"]
        except Exception as e:
            logger.error(f"Error fetching layer usage for {l_url} in portal {instance}: {e}")
            response = HttpResponse()
            response["HX-Trigger-After-Settle"] = json.dumps(
                {"showDangerAlert": f"Error fetching layer usage for {l_url} in portal {instance}"})
            return response

    # Construct context
    context = {
        "portal": Portal.objects.values("alias", "portal_type", "url"),
        "updated": portal,
        "instance": instance,
        "maps": maps,
        "apps": apps,
        "url": l_url
    }

    return render(request, template_name, context)


@login_required
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

    # Select template based on whether the request is made via HTMX
    # Choose template based on HTMX request
    template_name = "portals/portal_metadata.html" if request.htmx else "portals/portal_metadata_full.html"

    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)

    try:
        instance_alias = request.POST.get("instance", None)
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
                try:
                    metadata_report = webmaps.get_metadata(portal, username, password)
                    if "error" in metadata_report:
                        response = HttpResponse(status=500)
                        response["HX-Trigger-After-Settle"] = json.dumps(
                            {"showDangerAlert": "Failed to retrieve metadata report."})
                        return response
                except Exception as e:
                    logger.error(f"Error fetching metadata report for {instance}: {e}")

                # Construct context
                context = {
                    "portal": Portal.objects.values_list("alias", "portal_type", "url"),
                    "updated": Portal.objects.get(alias=instance),
                    "instance": instance,
                    "items": items,
                    "metadata": metadata_report.get("metadata", []),
                }
                response = render(request, template_name, context)
                response["HX-Trigger"] = json.dumps({"closeModal": True})
                return response
            response = HttpResponse(status=401)
            response["HX-Trigger"] = json.dumps(
                {"showDangerAlert": "Unable to connect to portal. Please verify credentials."})
            return response

        if not portal.store_password:
            return render(request, "portals/portal_credentials.html", {"instance": portal, "items": items})
    except Exception as e:
        logger.exception(f"Error in metadata_view: {e}")
        response = HttpResponse(status=500)
        response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": "Failed to start metadata task"})
        return response

    # Fetch metadata compliance report
    metadata_report = webmaps.get_metadata(portal)

    # If an error occurred, send it via HX-Trigger-After-Settle for client-side display
    if "error" in metadata_report:
        response = HttpResponse(status=500)
        response["HX-Trigger-After-Settle"] = json.dumps({"showDangerAlert": metadata_report["error"]})
        return response

    # Context for rendering the template
    context = {
        "portal": Portal.objects.values_list("alias", "portal_type", "url"),
        "updated": Portal.objects.get(alias=instance),
        "instance": instance,
        "metadata": metadata_report.get("metadata", []),
    }

    return render(request, template_name, context)


@login_required
@require_POST
def mode_toggle_view(request):
    """
    Toggle between 'light' and 'dark' mode for the user's profile.

    Returns:
    - JsonResponse: JSON object containing the updated mode and rendered switch UI.
    """
    # Ensure the user profile exists
    user_profile = get_object_or_404(UserProfile, user=request.user)

    # Toggle mode
    user_profile.mode = "dark" if user_profile.mode == "light" else "light"
    user_profile.save()

    # Render switch component dynamically
    switch_html = loader.render_to_string("partials/mode_switch.html", {"mode": user_profile.mode})

    return JsonResponse({"mode": user_profile.mode, "switch_html": switch_html})


@login_required
@require_POST
def usage_toggle_view(request):
    """
    Toggle the service usage setting for the logged-in user's profile.

    Returns:
    - JsonResponse: JSON object containing the updated state and rendered switch UI.
    """
    # Ensure the user profile exists
    user_profile = get_object_or_404(UserProfile, user=request.user)

    # Toggle service usage
    user_profile.service_usage = not user_profile.service_usage
    user_profile.save()

    # Render switch component dynamically
    switch_html = loader.render_to_string("partials/usage_switch.html", {"service_usage": user_profile.service_usage})

    return JsonResponse({"service_usage": user_profile.service_usage, "switch_html": switch_html})


@require_POST
@csrf_exempt  # Only exempt CSRF for webhook requests
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
    # TODO rate limiting
    # Extract the signature from headers
    signature = request.headers.get("X-Signature", "")
    if not signature:
        logger.warning("Webhook request missing 'X-Signature' header.")
        return HttpResponseForbidden("Missing signature")

    # Read and verify request body
    body = request.body
    computed_signature = hmac.new(
        key=settings.WEBHOOK_SECRET.encode(),
        msg=body,
        digestmod=hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(computed_signature, signature):
        logger.warning("Webhook signature mismatch. Possible tampering attempt.")
        return HttpResponseForbidden("Invalid signature")

    # Parse JSON payload
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        logger.error("Received malformed JSON in webhook payload.")
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    # Extract portal URL and check if it's known
    portal_url = payload.get("info", {}).get("portalURL")
    if not portal_url:
        logger.warning("Webhook payload missing 'portalURL'.")
        return JsonResponse({"error": "Missing portal URL"}, status=400)

    # Validate portal existence
    if not Portal.objects.filter(url=portal_url).exists():
        logger.warning(f"Unknown portal received in webhook: {portal_url}")
        return JsonResponse({"error": "Unknown portal"}, status=404)

    instance_item = get_object_or_404(Portal, url=portal_url)

    # Attempt to establish a connection
    try:
        target = webmaps.connect(instance_item)
    except Exception as e:
        logger.error(f"Failed to connect to portal: {portal_url}. Error: {e}")
        return JsonResponse({"error": "Failed to connect to portal"}, status=500)

    # Process events in the webhook payload
    for event in payload.get("events", []):
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

    # Respond with success
    return JsonResponse({"message": "Webhook received successfully"}, status=200)
