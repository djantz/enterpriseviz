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
import datetime
import logging
import re
from dataclasses import dataclass, field, asdict
from itertools import combinations, groupby
from operator import itemgetter
import os
import time
import hashlib
from cryptography.fernet import Fernet
from django.conf import settings
from django.core.cache import cache

import requests
from arcgis import gis
from arcgis.mapping import WebMap
from django.db.models import F, QuerySet, Q
from django.utils import timezone
from fuzzywuzzy import fuzz

from .models import Webmap, Service, Layer, App, Portal

logger = logging.getLogger('enterpriseviz.utils')


@dataclass
class UpdateResult:
    """
    Tracks and summarizes the results of an update operation.

    :ivar success: Indicates if the overall operation was successful.
    :type success: bool
    :ivar num_updates: Count of updated records.
    :type num_updates: int
    :ivar num_inserts: Count of inserted records.
    :type num_inserts: int
    :ivar num_deletes: Count of deleted records.
    :type num_deletes: int
    :ivar num_errors: Count of errors encountered.
    :type num_errors: int
    :ivar error_messages: List of error messages.
    :type error_messages: list
    """
    success: bool = False
    num_updates: int = 0
    num_inserts: int = 0
    num_deletes: int = 0
    num_errors: int = 0
    error_messages: list = field(default_factory=list)

    def set_success(self, status: bool = True):
        """Marks the operation as successful or unsuccessful."""
        self.success = status

    def add_update(self, count: int = 1):
        """Increments the update count."""
        self.num_updates += count

    def add_insert(self, count: int = 1):
        """Increments the insert count."""
        self.num_inserts += count

    def add_delete(self, count: int = 1):
        """Increments the delete count."""
        self.num_deletes += count

    def add_error(self, error_message: str):
        """Logs an error message and increments the error count."""
        self.num_errors += 1
        self.error_messages.append(error_message)
        self.success = False

    def to_json(self):
        """Converts the instance to a JSON-serializable dictionary.

        :return: Dictionary representation of the UpdateResult.
        :rtype: dict
        """
        return asdict(self)


def try_connection(connection_details):
    """
    Attempts to connect to an ArcGIS instance using provided credentials.

    :param connection_details: Dictionary with 'url', 'username', and 'password'.
    :type connection_details: dict
    :return: Dictionary with 'authenticated' (bool), 'is_agol' (bool or None),
             and optionally 'error' (str).
    :rtype: dict
    """
    url = connection_details.get("url")
    username = connection_details.get("username")
    password = connection_details.get("password")

    if not all([url, username, password]):
        logger.warning("Missing required connection details (url, username, or password).")
        return {"authenticated": False, "is_agol": None, "error": "Missing connection details."}

    logger.debug(f"Attempting connection to {url} with user {username}.")
    try:
        target_gis = gis.GIS(url, username, password)
        is_agol = target_gis.properties.isPortal is False

        # Verify successful authentication by checking if user property is accessible
        if hasattr(target_gis.properties, 'user') and target_gis.properties.user.username.lower() == username.lower():
            logger.info(f"Successfully connected to {url} as {username} (AGOL: {is_agol}).")
            return {"authenticated": True, "is_agol": is_agol}
        else:
            logger.warning(f"Authentication to {url} as {username} appears to have failed or user mismatch.")
            return {"authenticated": False, "is_agol": None, "error": "Authentication failed or username mismatch."}

    except requests.exceptions.SSLError as ssl_e:
        logger.error(
            f"SSL Error connecting to {url}. Ensure certs are valid or try verify_cert=False if appropriate. Error: {ssl_e}",
            exc_info=True)
        return {"authenticated": False, "is_agol": None, "error": f"SSL Error connecting to {url}."}
    except Exception as e:
        logger.error(f"Connection attempt to {url} with user {username} failed: {e}", exc_info=True)
        return {"authenticated": False, "is_agol": None, "error": f"Connection to {url} failed."}


def connect(portal_model_instance, username=None, password=None):
    """
    Establishes a connection to an ArcGIS instance defined by a Portal model.

    Uses provided credentials if given, otherwise attempts to use stored credentials
    from the portal_model_instance.

    :param portal_model_instance: The Portal model instance.
    :type portal_model_instance: enterpriseviz.models.Portal
    :param username: Optional username for authentication.
    :type username: str, optional
    :param password: Optional password for authentication.
    :type password: str, optional
    :return: An authenticated `arcgis.gis.GIS` object.
    :rtype: arcgis.gis.GIS
    :raises ValueError: If portal URL is missing.
    :raises ConnectionError: If connection or authentication fails.
    """
    if not portal_model_instance.url:
        logger.error(f"Portal URL missing for instance {portal_model_instance.alias}.")
        raise ValueError("Missing URL in portal instance configuration.")

    url = portal_model_instance.url
    logger.debug(f"Preparing to connect to {url} for instance {portal_model_instance.alias}.")

    gis_kwargs = {'verify_cert': True}

    try:
        if username and password:
            logger.debug(f"Using provided credentials for {url}.")
            target_gis = gis.GIS(url, username, password, **gis_kwargs)
        elif portal_model_instance.token and portal_model_instance.token_expiration and portal_model_instance.token_expiration > timezone.now():
            logger.debug(f"Using valid stored token for {url}.")
            target_gis = gis.GIS(url, token=portal_model_instance.token, **gis_kwargs)
            if target_gis.properties.isPortal:
                _update_portal_token_info(portal_model_instance, target_gis)
        elif portal_model_instance.store_password and portal_model_instance.username and portal_model_instance.password:
            logger.debug(f"Using stored credentials for {url}.")
            target_gis = gis.GIS(url, portal_model_instance.username, portal_model_instance.password, **gis_kwargs)
        else:  # Expired or no token, and no stored user/pass, or store_password is false
            logger.warning(f"No valid credentials or token for {url}. Authentication may fail or be anonymous.")
            target_gis = gis.GIS(url, **gis_kwargs)

        if not target_gis.properties.isPortal and not portal_model_instance.org_id:  # is AGOL
            _fetch_and_save_org_id(portal_model_instance, target_gis)

        # Ensure connection is actually established (GIS() can sometimes not raise error on init)
        if not target_gis._con.is_logged_in and (
            username or portal_model_instance.store_password):
            logger.error(f"Failed to establish an authenticated session for {url}.")
            raise ConnectionError(f"Failed to establish an authenticated session for {url}.")

        logger.info(f"Successfully connected to {url} (Instance: {portal_model_instance.alias}).")
        return target_gis

    except Exception as e:
        logger.error(f"Failed to connect to {url} for instance {portal_model_instance.alias}: {e}", exc_info=True)
        raise ConnectionError(f"Connection to {url} failed: {e}")


def _update_portal_token_info(portal_model_instance, target_gis_connection):
    """Helper to update token info on the Portal model instance."""
    try:
        if hasattr(target_gis_connection._con, 'token') and hasattr(target_gis_connection._con, '_expiration'):
            new_token = target_gis_connection._con.token
            minutes_to_expiry = getattr(target_gis_connection._con, '_expiration', 60)
            if isinstance(minutes_to_expiry, (int, float)):
                new_expiration = timezone.now() + datetime.timedelta(minutes=minutes_to_expiry)
            else:
                new_expiration = None
                logger.warning(
                    f"Unexpected _expiration type for {portal_model_instance.alias}: {type(minutes_to_expiry)}")

            if new_token and new_expiration and \
                (new_token != portal_model_instance.token or new_expiration != portal_model_instance.token_expiration):
                portal_model_instance.token = new_token
                portal_model_instance.token_expiration = new_expiration
                portal_model_instance.save(update_fields=['token', 'token_expiration'])
                logger.debug(f"Token refreshed for {portal_model_instance.alias}. New expiration: {new_expiration}")
    except Exception as e:
        logger.error(f"Error updating token for {portal_model_instance.alias}: {e}", exc_info=True)


def _fetch_and_save_org_id(portal_model_instance, target_gis_connection):
    """Helper to fetch and save org_id for AGOL portals."""
    try:
        org_id_from_props = getattr(target_gis_connection.properties, 'id', None)
        if org_id_from_props:
            portal_model_instance.org_id = org_id_from_props
        else:
            response = requests.get(f"{target_gis_connection.url}/sharing/rest/portals/self?culture=en&f=pjson",
                                    headers={
                                        'Authorization': f'Bearer {target_gis_connection._con.token}'} if target_gis_connection._con.token else None)
            if response.status_code == 200:
                data = response.json()
                portal_model_instance.org_id = data.get("id")
            else:
                logger.warning(
                    f"Failed to fetch portal self details for {portal_model_instance.alias}. Status: {response.status_code}")

        if portal_model_instance.org_id:
            portal_model_instance.save(update_fields=['org_id'])
            logger.debug(f"Org ID {portal_model_instance.org_id} saved for {portal_model_instance.alias}.")
    except Exception as e:
        logger.error(f"Error fetching/saving org_id for {portal_model_instance.alias}: {e}", exc_info=True)


def map_details(item_id):
    """
    Retrieves details of a Webmap, its services, applications, and layers.

    :param item_id: Unique ID of the Webmap.
    :type item_id: str
    :return: Dictionary with 'tree' (Webmap details), 'services' (QuerySet),
             'apps' (QuerySet with usage_type), 'layers' (list),
             and 'item' (Webmap model instance). Returns {'error': msg} on failure.
    :rtype: dict
    """
    try:
        webmap_item = Webmap.objects.select_related('portal_instance').get(webmap_id=item_id)
        logger.debug(f"Retrieved Webmap '{webmap_item.webmap_title}' (ID: {item_id}).")

        tree = {
            "name": webmap_item.webmap_title, "url": webmap_item.webmap_url,
            "instance": webmap_item.portal_instance.alias if webmap_item.portal_instance else None,
            "description": webmap_item.webmap_description, "access": webmap_item.webmap_access,
            "children": []
        }

        services = webmap_item.service_set.select_related('portal_instance').distinct()
        apps = webmap_item.app_set.select_related('portal_instance', 'app_owner') \
            .annotate(usage_type=F("app_map__rel_type")).distinct()

        layers = webmap_item.webmap_layers or []
        logger.debug(
            f"map_details: Found {services.count()} services, {apps.count()} apps, {len(layers)} layers for Webmap {item_id}.")

        for service in services:
            if service.service_name and service.service_url:
                tree["children"].append({
                    "name": service.service_name,
                    "url": service.service_url_as_list()[0] if service.service_url_as_list() else service.service_url,
                    "instance": service.portal_instance.alias if service.portal_instance else None,
                    "children": []
                })

        return {"tree": tree, "services": services, "apps": apps, "layers": layers, "item": webmap_item}

    except Webmap.DoesNotExist:
        logger.warning(f"Webmap with ID {item_id} not found.")
        return {"error": f"Webmap with ID {item_id} not found."}
    except Exception as e:
        logger.error(f"Error retrieving details for Webmap ID {item_id}: {e}", exc_info=True)
        return {"error": f"Error retrieving Webmap {item_id} details."}


def service_details(portal_alias, service_name):
    """
    Retrieves details of a Service, its layers, related maps, and applications.

    :param portal_alias: Alias of the Portal instance.
    :type portal_alias: str
    :param service_name: Name of the Service.
    :type service_name: str
    :return: Dictionary with 'tree' (Service details), 'maps' (QuerySet),
             'apps' (list of combined app dicts), 'layers' (QuerySet),
             and 'item' (Service model instance). Returns {'error': msg} on failure.
    :rtype: dict
    """
    try:
        portal_obj = Portal.objects.get(alias=portal_alias)

        service_item = Service.objects.select_related('portal_instance') \
            .get(portal_instance=portal_obj,
                 service_name=service_name)

        logger.debug(f"Retrieved Service '{service_item.service_name}' from '{portal_alias}'.")

        tree = {
            "name": service_item.service_name,
            "url": service_item.service_url_as_list()[0] if service_item.service_url_as_list() else None,
            "instance": portal_alias,
            "children": []
        }

        layers = Layer.objects.filter(layer_service__service_id=service_item)
        maps = Webmap.objects.filter(map_service__service_id=service_item).select_related(
            'portal_instance').distinct()

        # Apps related to this service via maps that use this service
        apps_via_maps = App.objects.filter(app_map__webmap_id__map_service__service_id=service_item) \
            .annotate(usage_type=F("app_map__rel_type")) \
            .select_related('app_owner', 'portal_instance').distinct()

        # Apps directly related to this service
        apps_direct = service_item.apps.annotate(usage_type=F("app_service__rel_type")) \
            .select_related('app_owner', 'portal_instance').distinct()

        apps_combined = combine_apps(apps_direct.union(apps_via_maps))
        logger.debug(
            f"Found {layers.count()} layers, {maps.count()} maps, {len(apps_combined)} apps for Service '{service_item.service_name}'.")

        for map_item in maps:
            map_data = {
                "name": map_item.webmap_title,
                "url": map_item.webmap_url,
                "instance": map_item.portal_instance.alias if map_item.portal_instance else None,
                "children": []
            }
            for app_item in map_item.app_map_set.all():
                if app_item.app_id:
                    app_data = {
                        "name": app_item.app_id.app_title,
                        "url": app_item.app_id.app_url,
                        "instance": app_item.app_id.portal_instance.alias if app_item.app_id.portal_instance else None,
                        "children": []
                    }
                    map_data["children"].append(app_data)
            tree["children"].append(map_data)

        return {"tree": tree, "maps": maps, "apps": apps_combined, "layers": layers, "item": service_item}

    except Portal.DoesNotExist:
        logger.warning(f"Portal with alias '{portal_alias}' not found.")
        return {"error": f"Portal '{portal_alias}' not found."}
    except Service.DoesNotExist:
        logger.warning(f"Service '{service_name}' not found in Portal '{portal_alias}'.")
        return {"error": f"Service '{service_name}' not found in Portal '{portal_alias}'."}
    except Exception as e:
        logger.error(f"Error for Service '{service_name}' in Portal '{portal_alias}': {e}", exc_info=True)
        return {"error": f"Error retrieving details for Service '{service_name}'."}


def layer_details(dbserver, database, version, name):
    """
    Retrieves details of a Layer, its parent services, and related maps/apps.

    :param dbserver: Database server of the layer.
    :type dbserver: str
    :param database: Database name.
    :type database: str
    :param version: Layer version (optional).
    :type version: str
    :param name: Name of the Layer.
    :type name: str
    :return: Dictionary with 'tree' (Layer details), 'services' (QuerySet),
             'maps' (QuerySet), 'apps' (list of combined app dicts),
             and 'item' (Layer name). Returns {'error': msg} on failure.
    :rtype: dict
    """

    logger.debug(
        f"layer_details: Fetching layer '{name}' from server '{dbserver}', db '{database}', version '{version}'.")
    try:
        layer_filter_q = Q(layer_name=name)

        layer_items = Layer.objects.filter(layer_filter_q).select_related('portal_instance')
        if not layer_items.exists():
            logger.warning(f"Layer '{name}' not found with specified criteria.")
            return {"error": f"Layer '{name}' not found."}

        tree = {
            "name": name,
            "url": None,
            "instance": None,
            "children": []
        }

        # Services directly associated with any of the found layer_items
        services = Service.objects.filter(layer__in=layer_items).select_related('portal_instance').distinct()
        if not services.exists():
            logger.warning(f"No services found directly associated with layer(s) '{name}'.")

        # Maps that use services which contain any of the found layer_items
        maps = Webmap.objects.filter(map_service__service_id__layer__in=layer_items).select_related(
            'portal_instance').distinct()

        # Apps related via maps that use services which contain the layer(s)
        apps_via_maps = App.objects.filter(app_map__webmap_id__map_service__service_id__layer__in=layer_items) \
            .annotate(usage_type=F("app_map__rel_type")) \
            .select_related('app_owner', 'portal_instance').distinct()

        # Apps directly related to services which contain the layer(s)
        apps_direct = App.objects.filter(app_service__service_id__layer__in=layer_items) \
            .annotate(usage_type=F("app_service__rel_type")) \
            .select_related('app_owner', 'portal_instance').distinct()
        apps_combined = combine_apps(apps_direct.union(apps_via_maps))
        logger.debug(
            f"Found {services.count()} services, {maps.count()} maps, {len(apps_combined)} apps for Layer '{name}'.")

        for service_item in services:
            tree["children"].append({
                "name": service_item.service_name, "type": "Service",
                "url": service_item.service_url_as_list()[0] if service_item.service_url_as_list() else None,
                "instance": service_item.portal_instance.alias if service_item.portal_instance else "N/A"
            })

        return {
            "tree": tree, "services": services, "maps": maps, "apps": apps_combined,
            "item": name
        }
    except Exception as e:
        logger.error(f"Error for Layer '{name}' in server '{dbserver}', db '{database}', version '{version}': {e}",
                     exc_info=True)
        return {"error": f"Error retrieving details for Layer '{name}'."}


def epoch_to_datetime(epoch_ms):
    """
    Converts an epoch timestamp (milliseconds) to a timezone-aware datetime object.

    :param epoch_ms: Epoch timestamp in milliseconds.
    :type epoch_ms: int or float or None
    :return: Localized datetime object, or None if input is invalid.
    :rtype: datetime.datetime or None
    """
    if epoch_ms is None or epoch_ms == -1:  # Common ArcGIS placeholder for no date
        return None
    try:
        if not isinstance(epoch_ms, (int, float)):
            raise TypeError("Timestamp must be a number.")
        dt_utc = datetime.datetime.fromtimestamp(epoch_ms / 1000.0, tz=datetime.timezone.utc)
        return dt_utc.astimezone()
    except (TypeError, ValueError) as e:
        logger.warning(f"Invalid timestamp value {epoch_ms}: {e}")
        return None


def get_usage_report(service_queryset):
    """
    Retrieves and processes a usage report for a queryset of Service objects.

    Queries GIS server usage reports for the last month for specified services.
    Requires services to have a portal_instance with stored credentials.

    :param service_queryset: QuerySet of Service model instances.
    :type service_queryset: django.db.models.QuerySet
    :return: Dictionary with 'usage' (chart data) on success, or 'error' on failure.
    :rtype: dict
    """
    chart_data = {"labels": [], "datasets": []}
    processed_portals = set()

    for service_obj in service_queryset.select_related('portal_instance'):
        portal_instance = service_obj.portal_instance
        if not portal_instance or portal_instance.alias in processed_portals:
            continue

        if not portal_instance.store_password:
            logger.debug(f"Skipping portal {portal_instance.alias}, no stored password.")
            processed_portals.add(portal_instance.alias)
            continue

        try:
            target_gis = connect(portal_instance)
            gis_servers = target_gis.admin.servers.list()
            if not gis_servers:
                logger.warning(f"No GIS servers found for portal instance: {portal_instance.alias}")
                processed_portals.add(portal_instance.alias)
                continue

            # Filter services for the current portal to batch requests
            current_portal_services = service_queryset.filter(portal_instance=portal_instance)
            service_url_paths = []
            for srv in current_portal_services:
                for url_part in srv.service_url_as_list():
                    if 'services/' in url_part:
                        url_path = url_part.split('services/', 1)[1]
                        service_url_paths.append(
                            f"services/{url_path.replace('/MapServer', '.MapServer').replace('/FeatureServer', '.FeatureServer')}")

            if not service_url_paths:
                processed_portals.add(portal_instance.alias)
                continue

            query_list_str = ",".join(set(service_url_paths))
            logger.debug(f"Querying usage for services on {portal_instance.alias}: {query_list_str}")

            usage_api = gis_servers[0].usage
            quick_report = usage_api.quick_report(since="LAST_MONTH", queries=query_list_str, metrics="RequestCount")

            if not chart_data["labels"] and quick_report.get("report", {}).get("time-slices"):
                chart_data["labels"] = [epoch_to_date(ts) for ts in quick_report["report"]["time-slices"]]

            for report_item in quick_report.get("report", {}).get("report-data", [])[0]:
                dataset_label = report_item["resourceURI"].replace("services", portal_instance.alias)
                chart_data["datasets"].append({
                    "label": dataset_label,
                    "data": [val if val is not None else 0 for val in report_item["data"]],
                    "fill": False
                })
            processed_portals.add(portal_instance.alias)

        except ConnectionError:
            logger.error(f"Connection failed for portal {portal_instance.alias}.")
            return {"error": "No service usage data could be retrieved."}
        except Exception as e:
            logger.error(f"Error processing portal {portal_instance.alias}: {e}", exc_info=True)
            return {"error": "No service usage data could be retrieved."}

    if not chart_data["datasets"]:
        logger.debug("No usage data generated.")
        return {"error": "No service usage data could be retrieved."}

    return {"usage": chart_data}


def epoch_to_date(epoch_ms):
    """
    Converts an epoch timestamp (milliseconds) to a date string 'YYYY-MM-DD'.

    :param epoch_ms: Epoch timestamp in milliseconds.
    :type epoch_ms: int or float or None
    :return: Date string 'YYYY-MM-DD', or None if input is invalid.
    :rtype: str or None
    """
    if epoch_ms is None: return None
    try:
        if not isinstance(epoch_ms, (int, float)) or epoch_ms < 0:
            raise ValueError("Timestamp must be a non-negative number.")
        return datetime.datetime.fromtimestamp(epoch_ms / 1000.0).strftime("%Y-%m-%d")
    except (TypeError, ValueError) as e:
        logger.warning(f"Invalid timestamp {epoch_ms}: {e}")
        return None


def find_layer_usage(portal_model_instance, layer_url_to_find):
    """
    Identifies web maps and apps in a portal that reference a specific layer URL.

    :param portal_model_instance: Portal model instance to search.
    :type portal_model_instance: enterpriseviz.models.Portal
    :param layer_url_to_find: URL of the layer to search for.
    :type layer_url_to_find: str
    :return: Dictionary with 'maps' (list of map dicts) and 'apps' (list of app dicts).
             Returns {'error': msg} on failure.
    :rtype: dict
    """
    if not layer_url_to_find:
        return {"error": "Layer URL to find cannot be empty."}

    normalized_layer_url = layer_url_to_find.lower().strip().rstrip('/')

    logger.debug(
        f"Searching for layer '{normalized_layer_url}' in portal '{portal_model_instance.alias}'.")

    try:
        target_gis = connect(portal_model_instance)
    except ConnectionError as e:
        logger.error(f"Connection failed for portal {portal_model_instance.alias}: {e}")
        return {"error": f"Connection failed for portal {portal_model_instance.alias}."}

    found_maps = []
    found_apps = []

    try:
        search_query = "NOT owner:esri*"
        all_relevant_items = target_gis.content.search(search_query,
                                                       item_type="Web Map, Web Mapping Application, Dashboard, Web AppBuilder, Experience Builder, Form, StoryMap",
                                                       max_items=2000, outside_org=False)
        logger.debug(
            f"find_layer_usage: Found {len(all_relevant_items)} candidate items in '{portal_model_instance.alias}'.")

        for item in all_relevant_items:
            try:
                item_data_str = str(item.get_data()).lower()
                if normalized_layer_url in item_data_str:
                    item_details = {
                        "portal_instance": portal_model_instance.alias, "id": item.id,
                        "title": item.title, "url": item.homepage, "owner": item.owner,
                        "created": epoch_to_datetime(item.created), "modified": epoch_to_datetime(item.modified),
                        "access": item.access, "type": item.type, "views": item.numViews,
                    }
                    if item.type == "Web Map":
                        found_maps.append(item_details)
                    else:  # App types
                        found_apps.append(item_details)
            except Exception as item_e:
                logger.warning(f"Error processing item '{item.id}' ({item.title}): {item_e}", exc_info=False)

        logger.debug(
            f"Found {len(found_maps)} maps and {len(found_apps)} apps using layer '{normalized_layer_url}'.")
        return {"maps": found_maps, "apps": found_apps}

    except Exception as e:
        logger.error(f"Error searching content in '{portal_model_instance.alias}': {e}", exc_info=True)
        return {"error": f"Error searching content in '{portal_model_instance.alias}'."}


def get_duplicates(portal_model_instance, similarity_threshold=70):
    """
    Identifies duplicate items within a portal based on name similarity.

    Checks Webmaps, Services, Layers, and Apps associated with the portal.

    :param portal_model_instance: Portal model instance to check.
    :type portal_model_instance: enterpriseviz.models.Portal
    :param similarity_threshold: Min similarity score (0-100) for duplicates.
    :type similarity_threshold: int, optional
    :return: Dict with 'webmaps', 'services', 'layers', 'apps' (lists of duplicate tuples).
             Returns {'error': msg} on failure.
    :rtype: dict
    """
    logger.debug(
        f"Finding duplicates in portal '{portal_model_instance.alias}' with threshold {similarity_threshold}%.")

    def _find_duplicates_for_model(items_qs, name_attr, score_threshold):
        """Helper to find duplicates in a queryset."""
        duplicates_found = []
        items_list = list(items_qs)
        if len(items_list) < 2: return []

        for item1, item2 in combinations(items_list, 2):
            name1 = getattr(item1, name_attr, "")
            name2 = getattr(item2, name_attr, "")
            if not name1 or not name2: continue

            if name_attr == 'service_name':
                name1 = name1.split('/')[-1].split('\\')[-1]
                name2 = name2.split('/')[-1].split('\\')[-1]

            similarity = fuzz.token_sort_ratio(name1.lower(), name2.lower())
            if similarity >= score_threshold:
                item1_disp = {"id": item1.pk, "title": name1,
                              "url": getattr(item1, 'url_field_name', '#')}
                item2_disp = {"id": item2.pk, "title": name2, "url": getattr(item2, 'url_field_name', '#')}
                duplicates_found.append((item1_disp, item2_disp, similarity))
        return sorted(duplicates_found, key=lambda x: x[2], reverse=True)

    try:
        webmaps = Webmap.objects.filter(portal_instance=portal_model_instance)
        services = Service.objects.filter(portal_instance=portal_model_instance)
        layers = Layer.objects.filter(portal_instance=portal_model_instance)
        apps = App.objects.filter(portal_instance=portal_model_instance)

        return {
            "webmaps": _find_duplicates_for_model(webmaps, 'webmap_title', similarity_threshold),
            "services": _find_duplicates_for_model(services, 'service_name', similarity_threshold),
            "layers": _find_duplicates_for_model(layers, 'layer_name', similarity_threshold),
            "apps": _find_duplicates_for_model(apps, 'app_title', similarity_threshold)
        }
    except Exception as e:
        logger.error(f"Error finding duplicates for '{portal_model_instance.alias}': {e}", exc_info=True)
        return {"error": f"Error finding duplicates in '{portal_model_instance.alias}'."}


def get_missing_item_attrs(arcgis_item):
    """
    Checks an ArcGIS item for metadata completeness and returns a compliance report.

    :param arcgis_item: An `arcgis.gis.Item` object.
    :type arcgis_item: arcgis.gis.Item
    :return: Dictionary with compliance status for description, thumbnail, snippet, etc.,
             plus item ID, title, URL, type, owner, and completeness score.
    :rtype: dict
    """

    def is_text_sufficient(text_value, min_words):
        if not text_value: return False
        return "Too short" if len(str(text_value).split()) < min_words else True

    description = getattr(arcgis_item, 'description', None)
    snippet = getattr(arcgis_item, 'snippet', None)
    access_info = getattr(arcgis_item, 'accessInformation', None)
    license_info = getattr(arcgis_item, 'licenseInfo', None)
    thumbnail_name = getattr(arcgis_item, 'thumbnail', None)

    has_thumbnail = bool(thumbnail_name)

    status = {
        "description": is_text_sufficient(description, 100),
        "snippet": is_text_sufficient(snippet, 10),
        "accessInformation": bool(access_info),
        "licenseInfo": bool(license_info),
        "thumbnail": has_thumbnail,
        "id": arcgis_item.id, "title": arcgis_item.title, "url": arcgis_item.homepage,
        "type": arcgis_item.type, "owner": arcgis_item.owner,
        "scoreCompleteness": getattr(arcgis_item, 'scoreCompleteness', None),
    }
    return status


def get_metadata(portal_model_instance, username=None, password=None):
    """
    Retrieves metadata compliance for content items in a portal.

    Connects to the portal, searches for non-Esri items, and checks each
    for metadata completeness using `get_missing_item_attrs`.

    :param portal_model_instance: Portal model instance.
    :type portal_model_instance: enterpriseviz.models.Portal
    :param username: Optional username for authentication.
    :type username: str, optional
    :param password: Optional password for authentication.
    :type password: str, optional
    :return: Dictionary with 'metadata' (list of compliance dicts).
             Returns {'error': msg} on failure.
    :rtype: dict
    """
    logger.debug(f"Fetching metadata for portal '{portal_model_instance.alias}'.")
    try:
        target_gis = connect(portal_model_instance, username, password)
    except ConnectionError as e:
        logger.error(f"Connection failed for '{portal_model_instance.alias}': {e}")
        return {"error": f"Connection failed for portal '{portal_model_instance.alias}'."}

    try:
        content_items = target_gis.content.search("NOT owner:esri*", max_items=10000,
                                                  outside_org=False)
        logger.debug(f"Found {len(content_items)} items in '{portal_model_instance.alias}'.")

        metadata_results = [get_missing_item_attrs(item) for item in content_items]
        return {"metadata": metadata_results}

    except Exception as e:
        logger.error(f"Error retrieving metadata from '{portal_model_instance.alias}': {e}",
                     exc_info=True)
        return {"error": f"Error retrieving metadata from '{portal_model_instance.alias}'."}


URL_REGEX = re.compile(r"https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[-\w./?=&%#~:]+")


def _extract_urls_from_string(text_string):
    """Extracts all URLs found in a string using regex."""
    if isinstance(text_string, str):
        return URL_REGEX.findall(text_string)
    return []


def extract_webappbuilder(data_structure, current_path="", parent_key_for_url=None):
    """
    Extracts URLs from ArcGIS Web AppBuilder JSON data.

    Recursively traverses the data (dicts, lists, strings) to find URLs.

    :param data_structure: The Web AppBuilder JSON data (as Python dict/list).
    :type data_structure: dict or list or str
    :param current_path: Internal use for recursion: current dot-separated path.
    :type current_path: str, optional
    :param parent_key_for_url: Internal use for recursion: key of the parent dict containing a URL string.
    :type parent_key_for_url: str, optional
    :return: List of [url, path_to_url_string, key_of_url_string] lists.
    :rtype: list
    """
    extracted_urls = []
    if isinstance(data_structure, dict):
        for key, value in data_structure.items():
            new_path = f"{current_path}.{key}" if current_path else key
            extracted_urls.extend(extract_webappbuilder(value, new_path, key))
    elif isinstance(data_structure, list):
        for idx, item in enumerate(data_structure):
            new_path = f"{current_path}[{idx}]"
            extracted_urls.extend(extract_webappbuilder(item, new_path, parent_key_for_url))
    elif isinstance(data_structure, str):
        found_in_string = _extract_urls_from_string(data_structure)
        for url in found_in_string:
            extracted_urls.append([url, current_path, parent_key_for_url])
    return extracted_urls


def _recursive_extract_by_key(data, target_key, current_path_list=None, results_list=None):
    """Generic helper to recursively find values for a specific key."""
    if current_path_list is None: current_path_list = []
    if results_list is None: results_list = []

    if isinstance(data, dict):
        type_at_level = data.get("type")  # For Experience Builder specific logic
        for key, value in data.items():
            new_path = current_path_list + [key]
            if key == target_key:
                # For Experience Builder, 'type' is at the same level as 'itemId' or URL-like value.
                results_list.append(
                    ("/".join(current_path_list), key, value, type_at_level if target_key != "type" else None))
            elif isinstance(value, str) and target_key == "_URL_" and value.startswith("http"):
                results_list.append(("/".join(current_path_list), key, value, type_at_level))

            _recursive_extract_by_key(value, target_key, new_path, results_list)
    elif isinstance(data, list):
        for idx, item in enumerate(data):
            new_path = current_path_list + [str(idx)]
            _recursive_extract_by_key(item, target_key, new_path, results_list)
    return results_list


def extract_dashboard(dashboard_data):
    """
    Extracts all 'itemId' values from ArcGIS Dashboard JSON data.

    :param dashboard_data: The Dashboard JSON data (as Python dict/list).
    :type dashboard_data: dict or list
    :return: List of (path_string, key_of_itemId, itemId_value, type_value_None) tuples.
    :rtype: list
    """
    return _recursive_extract_by_key(dashboard_data, "itemId")


def extract_experiencebuilder(exb_data):
    """
    Extracts 'itemId' and URLs from ArcGIS Experience Builder JSON data.

    Also captures the 'type' field at the same level if present.

    :param exb_data: The Experience Builder JSON data (as Python dict/list).
    :type exb_data: dict or list
    :return: List of (path_string, key, value, type_at_level) tuples.
             'key' will be 'itemId' or the key for the URL.
             'value' will be the itemId or URL.
    :rtype: list
    """
    item_ids = _recursive_extract_by_key(exb_data, "itemId")
    urls = _recursive_extract_by_key(exb_data, "_URL_")
    return item_ids + urls


def extract_storymap(storymap_data, current_path_list=None, results_list=None):
    """
    Extracts 'itemType' and 'itemId' pairs from ArcGIS StoryMap JSON data.

    :param storymap_data: The StoryMap JSON data (as Python dict/list).
    :type storymap_data: dict or list
    :param current_path_list: Internal use for recursion.
    :type current_path_list: list, optional
    :param results_list: Internal use for recursion.
    :type results_list: list, optional
    :return: List of (path_string, itemType_value, itemId_value) tuples.
    :rtype: list
    """
    if current_path_list is None: current_path_list = []
    if results_list is None: results_list = []

    if isinstance(storymap_data, dict):
        # StoryMaps often have 'itemType' and 'itemId' at the same level describing a resource.
        if "itemType" in storymap_data and "itemId" in storymap_data:
            item_type = storymap_data["itemType"]
            item_id = storymap_data["itemId"]
            path_str = "/".join(current_path_list)
            results_list.append((path_str, item_type, item_id))

        for key, value in storymap_data.items():
            new_path = current_path_list + [key]
            extract_storymap(value, new_path, results_list)
    elif isinstance(storymap_data, list):
        for idx, item in enumerate(storymap_data):
            new_path = current_path_list + [str(idx)]
            extract_storymap(item, new_path, results_list)
    return results_list


def combine_apps(app_queryset):
    """
    Aggregates app data from a queryset, combining 'usage_type' for unique apps.

    Groups apps by core attributes and concatenates their 'usage_type' values.
    Assumes 'app_queryset' is annotated with 'usage_type'.

    :param app_queryset: QuerySet of App model instances, annotated with 'usage_type'.
                         The queryset should ideally already have `values()` called if specific fields are needed,
                         or be prepared for `itemgetter` to access model attributes.
    :type app_queryset: django.db.models.QuerySet
    :return: List of dictionaries, each representing a unique app with combined usage types.
    :rtype: list
    """

    if isinstance(app_queryset, QuerySet) and not app_queryset._result_cache:
        app_list = list(app_queryset.values(
            "app_id", "portal_instance_id", "app_title", "app_url","app_type", "app_owner__user_username",
            "app_created", "app_modified", "app_access", "app_views", "usage_type"
        ))
    else:
        app_list = list(app_queryset)

    if not app_list: return []

    group_key_fields = ("app_id", "portal_instance_id", "app_title", "app_url",
                        "app_type", "app_owner__user_username", "app_created", "app_modified",
                        "app_access", "app_views")

    try:
        apps_sorted = sorted(app_list, key=itemgetter(*group_key_fields))
    except KeyError as e:
        logger.error(f"Missing key {e} in app data for sorting/grouping. App list sample: {app_list[:1]}")
        return []

    combined_results = []
    for key_tuple, group in groupby(apps_sorted, key=itemgetter(*group_key_fields)):
        group_list = list(group)
        usage_types = sorted(list(set(item['usage_type'] for item in group_list if item.get('usage_type'))))

        combined_app_data = dict(zip(group_key_fields, key_tuple))
        combined_app_data['usage_type'] = ", ".join(usage_types) if usage_types else None
        combined_app_data['app_owner'] = combined_app_data['app_owner__user_username']
        combined_results.append(combined_app_data)

    return combined_results


TARGET_LOGGER_NAMES = [
    'enterpriseviz',
    'celery',
    'app.tasks',
    __name__,
]

from .models import SiteSettings


def apply_global_log_level(level_name=None, logger_name=None):
    """
    Applies a log level to predefined target loggers and their handlers.

    If `specific_level_name` is provided, that level is applied. Otherwise,
    the level is fetched from `SiteSettings.logging_level`.

    :param level_name: Optional specific log level string (e.g., 'DEBUG').
    :type level_name: str, optional
    :param logger_name: Optional specific logger name (e.g., 'enterpriseviz').
    :type logger_name: str, optional
    """
    util_logger = logging.getLogger(__name__)
    util_logger.debug("Attempting to apply global log levels...")

    level_to_apply_str = level_name
    source_msg = f"specific parameter '{level_name}'"

    if not level_name:
        try:
            site_settings = SiteSettings.objects.first()
            if site_settings and site_settings.logging_level:
                level_name = site_settings.logging_level
                source_msg = f"SiteSettings ({level_name})"
            else:
                util_logger.warning("SiteSettings not found or logging_level not set. No dynamic level applied.")
                return
        except Exception as e:
            util_logger.error(f"Error fetching SiteSettings: {e}", exc_info=True)
            return

    level_name = level_name.upper()
    log_level_int = getattr(logging, level_name, None)

    if log_level_int is None:
        util_logger.error(f"Invalid log level '{level_name}' from {source_msg}. Cannot apply.")
        return

    util_logger.info(f"Applying log level {level_name} (from {source_msg}) to target loggers.")

    changed_loggers = 0
    if logger_name:
        target_logger_obj = logging.getLogger(logger_name)
        if target_logger_obj:
            target_logger_obj.setLevel(log_level_int)
            changed_loggers += 1
        else:
            util_logger.warning(f"Logger '{logger_name}' not found. No dynamic level applied.")
    else:
        for logger_name_str in TARGET_LOGGER_NAMES:
            try:
                target_logger_obj = logging.getLogger(logger_name_str)
                original_logger_level_int = target_logger_obj.level

                target_logger_obj.setLevel(log_level_int)
                if original_logger_level_int != log_level_int:
                    util_logger.debug(
                        f"Logger '{logger_name_str or 'root'}': Level changed from {logging.getLevelName(original_logger_level_int)} to {level_to_apply_str}.")
                    changed_loggers += 1

                for handler_obj in target_logger_obj.handlers:
                    original_handler_level_int = handler_obj.level

                    if original_handler_level_int != log_level_int:
                        handler_obj.setLevel(log_level_int)
                        util_logger.debug(
                            f"Handler '{type(handler_obj).__name__}' on logger '{logger_name_str or 'root'}': Level changed from {logging.getLevelName(original_handler_level_int)} to {level_to_apply_str}.")

            except Exception as e:
                util_logger.error(f"Error applying level to logger '{logger_name_str}': {e}", exc_info=False)

    if changed_loggers > 0:
        util_logger.info(f"Finished applying log level {level_to_apply_str}. {changed_loggers} loggers updated.")
    else:
        util_logger.debug(f"Log level {level_to_apply_str} was already effectively set or no target loggers changed.")
