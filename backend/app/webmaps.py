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
from dataclasses import dataclass, field, asdict
from itertools import combinations
from itertools import groupby
from operator import itemgetter

from arcgis import gis
from arcgis.mapping import WebMap
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import F, Subquery, OuterRef, QuerySet
from django.utils import timezone
from fuzzywuzzy import fuzz
import requests

from .models import Webmap, Service, Layer, App, User, Portal

logger = logging.getLogger(__name__)


@dataclass
class UpdateResult:
    """
    A class to track and summarize the results of an update operation.
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
        """Increments the update count by the specified amount (default is 1)."""
        self.num_updates += count

    def add_insert(self, count: int = 1):
        """Increments the insert count by the specified amount (default is 1)."""
        self.num_inserts += count

    def add_delete(self, count: int = 1):
        """Increments the delete count by the specified amount (default is 1)."""
        self.num_deletes += count

    def add_error(self, error_message: str):
        """Logs an error message and increments the error count."""
        self.num_errors += 1
        self.error_messages.append(error_message)

    def to_json(self):
        """Converts the instance to a JSON-serializable dictionary."""
        return asdict(self)


def try_connection(instance):
    """
    Attempts to establish a connection to an ArcGIS instance using provided credentials.

    :param instance: A dictionary containing connection details:
                     - ``url`` (*str*): The URL of the ArcGIS instance.
                     - ``username`` (*str*): The username for authentication.
                     - ``password`` (*str*): The password for authentication.
    :type instance: dict

    :return: A tuple (``success``, ``is_agol``), where:
             - ``success`` (*bool*): ``True`` if the connection is successful, otherwise ``False``.
             - ``is_agol`` (*bool or None*): ``True`` if connected to ArcGIS Online, ``False`` if Enterprise,
               or ``None`` if the connection fails.
    :rtype: tuple(bool, bool or None)
    """

    # Extract credentials safely
    url = instance.get("url")
    username = instance.get("username")
    password = instance.get("password")

    # Validate required parameters
    if not all([url, username, password]):
        logger.error("Missing required connection details (url, username, or password).")
        return False, None

    logger.info(f"Attempting connection to ArcGIS instance: {url} with user {username}")

    try:
        # Establish connection
        target = gis.GIS(url, username, password)
        is_agol = target._is_agol
        # Verify successful authentication
        if target.properties.user.username.lower() == username.lower():
            logger.info(f"Successfully connected to {url} as {username} (AGOL: {is_agol})")
            return {"authenticated": True, "is_agol": is_agol}
        else:
            logger.warning(f"Authentication failed: Retrieved username does not match provided credentials.")
            return {"authenticated": False, "is_agol": None}

    except Exception as e:
        logger.exception(f"Connection attempt to {url} failed: {e}")
        return {"authenticated": False, "is_agol": None, "error": f"Connection attempt to {url} failed."}


def connect(instance_item, username=None, password=None):
    """
    Establishes a connection to an ArcGIS instance, handling authentication based on provided credentials
    or stored tokens.

    :param instance_item: An instance of the Portal model containing details about the ArcGIS instance,
                          including URL, stored credentials, and token expiration time.
    :type instance_item: object
    :param username: Optional username for authentication. If provided, it overrides stored credentials.
    :type username: str, optional
    :param password: Optional password for authentication. Used with `username` if provided.
    :type password: str, optional

    :return: A `GIS` object representing the authenticated connection to the ArcGIS instance.
    :rtype: arcgis.gis.GIS

    :raises ValueError: If essential details (e.g., URL) are missing.
    :raises ConnectionError: If authentication fails due to invalid credentials or expired tokens.
    """
    try:
        # Extract instance details
        url = instance_item.url
        token_expiration = instance_item.token_expiration
        portal_type = instance_item.portal_type.lower() if instance_item.portal_type else ""

        if not url:
            raise ValueError("Missing URL in portal instance configuration.")

        logger.info(f"Connecting to ArcGIS instance: {url}")

        # Case 1: Use provided credentials (overrides stored credentials)
        if username and password:
            logger.info("Using provided username and password for authentication.")
            target = gis.GIS(url, username, password)

            if portal_type == "agol" and (instance_item.org_id is None or instance_item.org_id == ""):
                response = requests.get(f"{target.url}/sharing/rest/portals/self?culture=en&f=pjson")
                if response.status_code == 200:
                    data = response.json()
                    org_id = data.get("id")
                else:
                    org_id = None
                instance_item.org_id = org_id
                instance_item.save()
            return target

        # Case 2: Use stored credentials if token is expired or for ArcGIS Online (AGOL)
        if not token_expiration or token_expiration < timezone.now() or portal_type == "agol":
            instance_username = instance_item.username
            instance_password = instance_item.password

            if not instance_username or not instance_password:
                raise ValueError("Missing stored username or password for portal authentication.")

            logger.info("Stored credentials detected, attempting authentication...")
            target = gis.GIS(url, instance_username, instance_password)

            if portal_type == "agol" and (instance_item.org_id is None or instance_item.org_id == ""):
                response = requests.get(f"{target.url}/sharing/rest/portals/self?culture=en&f=pjson")
                if response.status_code == 200:
                    data = response.json()
                    org_id = data.get("id")
                else:
                    org_id = None
                instance_item.org_id = org_id
                instance_item.save()

            # Refresh token and update expiration time
            new_token = target._con.token
            new_expiration = timezone.now() + datetime.timedelta(minutes=target._con._expiration)

            if new_token != instance_item.token or new_expiration != instance_item.token_expiration:
                instance_item.token = new_token
                instance_item.token_expiration = new_expiration
                instance_item.save()
                logger.info(f"Token refreshed and stored. New expiration: {new_expiration}")

            return target

        # Case 3: Use existing valid token
        logger.info("Using stored token for authentication.")
        return gis.GIS(url, token=instance_item.token)

    except ValueError as ve:
        logger.error(f"Configuration error: {ve}")
        raise
    except Exception as e:
        logger.exception(f"Failed to connect to {url}: {e}")
        raise ConnectionError(f"Failed to connect to {url}: {e}")


def map_details(item_id):
    """
    Retrieves details of a Webmap instance, including its associated services, applications, and layers.

    :param item_id: The unique identifier of the Webmap instance.
    :type item_id: str

    :return: A tuple containing:
        - **tree** (*dict*): A hierarchical dictionary with Webmap details including name, URL, instance,
          description, access, and child services.
        - **services** (*QuerySet*): A queryset of related service instances.
        - **apps2** (*QuerySet*): A queryset of related applications with an additional `usage_type` annotation.
        - **layers** (*list*): A list of layers associated with the Webmap.
        - **webmap_item** (*Webmap* or None): The Webmap model instance if found, otherwise `None`.
    :rtype: tuple
    """
    try:
        # Retrieve the Webmap instance
        webmap_item = Webmap.objects.get(webmap_id=item_id)
        logger.info(f"Retrieved Webmap: {webmap_item.webmap_title} (ID: {item_id})")

        # Construct the tree structure for Webmap details
        tree = {
            "name": webmap_item.webmap_title,
            "url": webmap_item.webmap_url,
            "instance": None,
            "description": webmap_item.webmap_description,
            "access": webmap_item.webmap_access,
            "children": []
        }

        # Fetch related services and applications
        services = webmap_item.service_set.all().distinct()
        apps = webmap_item.app_set.all().distinct()
        apps2 = apps.annotate(usage_type=F("app_map__rel_type"))

        # Log retrieved services and applications
        logger.debug(f"Retrieved {services.count()} services for Webmap ID: {item_id}")
        logger.debug(f"Retrieved {apps2.count()} applications for Webmap ID: {item_id}")

        # Retrieve associated layers
        layers = webmap_item.webmap_layers or []
        logger.debug(f"Retrieved {len(layers)} layers for Webmap ID: {item_id}")

        # Populate service details into the tree structure
        for service in services:
            if service.service_name and service.service_url:
                service_data = {
                    "name": service.service_name,
                    "url": service.service_url,
                    "instance": service.portal_instance.alias if service.portal_instance else None,
                    "children": []
                }
                tree["children"].append(service_data)

        return {"tree": tree,
                "services": services,
                "apps": apps2,
                "layers": layers,
                "item": webmap_item}

    except ObjectDoesNotExist:
        logger.error(f"Webmap with ID {item_id} not found.")
        return {"error": f"Webmap with ID {item_id} not found."}

    except Exception as e:
        logger.exception(f"Error retrieving Webmap details for ID {item_id}: {e}")
        return {"error": f"Error retrieving Webmap with ID {item_id}"}


def service_details(instance, item):
    """
    Retrieves details of a specific service within an ArcGIS instance, including associated layers, maps, and applications.

    :param instance: The Portal instance associated with the service.
    :type instance: models.Model
    :param item: The name of the service to retrieve details for.
    :type item: str

    :return: A tuple containing:
        - **tree** (*dict*): A hierarchical dictionary representing the service, including name, URL, instance, and related maps and apps.
        - **maps** (*QuerySet*): A queryset of Webmap instances associated with the service.
        - **apps_combined** (*list*): A processed list of applications linked to the service, combining different relationships.
        - **layers** (*QuerySet*): A queryset of Layer instances associated with the service.
        - **service_item** (*Service* or None): The Service model instance if found, otherwise `None`.

    :rtype: tuple
    """
    try:
        # Retrieve the Service instance
        service_item = Service.objects.get(portal_instance=instance, service_name=item)
        logger.info(f"Retrieved Service: {service_item.service_name} (Instance: {instance})")

        # Construct tree structure for service details
        tree = {
            "name": service_item.service_name,
            "url": service_item.service_url_as_list()[0] if service_item.service_url_as_list() else None,
            "instance": service_item.portal_instance.alias if service_item.portal_instance else None,
            "children": []
        }

        # Fetch related layers, maps, and applications
        layers = Layer.objects.filter(layer_service__service_id=service_item)

        maps = Webmap.objects.filter(map_service__service_id=service_item).distinct()

        # Applications linked via Webmaps
        app_map = App.objects.filter(app_map__webmap_id__map_service__service_id=service_item) \
            .annotate(usage_type=F("app_map__rel_type")) \
            .annotate(owner=Subquery(User.objects.filter(id=OuterRef("app_owner_id")).values("user_username"))) \
            .distinct()

        # Applications directly linked to the service
        app_service = service_item.apps.all() \
            .annotate(usage_type=F("app_service__rel_type")) \
            .annotate(owner=Subquery(User.objects.filter(id=OuterRef("app_owner_id")).values("user_username"))) \
            .distinct()

        # Combine both sets of applications
        apps_combined = combine_apps(app_service.union(app_map))

        # Log retrieved data
        logger.debug(f"Retrieved {layers.count()} layers for Service: {service_item.service_name}")
        logger.debug(f"Retrieved {maps.count()} maps for Service: {service_item.service_name}")
        logger.debug(f"Retrieved {len(apps_combined)} apps for Service: {service_item.service_name}")

        # Add related maps and apps to the tree structure
        for map_instance in maps:
            map_data = {
                "name": map_instance.webmap_title,
                "url": map_instance.webmap_url,
                "instance": map_instance.portal_instance.alias if map_instance.portal_instance else None,
                "children": []
            }
            for app in map_instance.app_map_set.all():
                if app.app_id:
                    app_data = {
                        "name": app.app_id.app_title,
                        "url": app.app_id.app_url,
                        "instance": app.app_id.portal_instance.alias if app.app_id.portal_instance else None,
                        "children": []
                    }
                    map_data["children"].append(app_data)
            tree["children"].append(map_data)

        return {"tree": tree,
                "maps": maps,
                "apps": apps_combined,
                "layers": layers,
                "item": service_item}

    except ObjectDoesNotExist:
        logger.error(f"Service '{item}' not found for Instance: {instance}")
        return {"error": f"Service '{item}' not found for Instance: {instance}"}

    except Exception as e:
        logger.exception(f"Error retrieving details for Service '{item}' in Instance: {instance}: {e}")
        return {"error": f"Error retrieving details for Service '{item}' in Instance: {instance}"}


def layer_details(dbserver, database, version, name):
    """
    Retrieves details of a specific layer within an ArcGIS instance, including associated services, maps, and applications.

    :param dbserver: The database server where the layer is located.
    :type dbserver: str
    :param database: The database name to query.
    :type database: str
    :param version: The version of the layer.
    :type version: str
    :param name: The name of the layer to retrieve details for.
    :type name: str

    :return: A tuple containing:
        - **tree** (*dict*): A hierarchical dictionary representing the layer, including name, URL, instance, and related services, maps, and apps.
        - **services** (*QuerySet* or None*): A QuerySet of distinct services associated with the layer, or `None` if not found.
        - **maps** (*QuerySet* or None*): A QuerySet of distinct maps related to the layer’s services, or `None` if not found.
        - **apps_combined** (*list* or None*): A processed list of applications linked to the layer, or `None` if not found.

    :rtype: tuple
    """
    try:
        # Initialize tree structure
        tree = {
            "name": name,
            "url": None,
            "instance": None,
            "children": []
        }

        # Retrieve layer instances matching the given name
        layer_items = Layer.objects.filter(layer_name=name)
        if not layer_items.exists():
            logger.warning(f"No layers found with name '{name}' in database '{database}' on server '{dbserver}'")
            return {"error": f"Error retrieving Layer with name {name}"}

        # Retrieve related services
        services = Service.objects.filter(layer__in=layer_items).distinct()
        if not services.exists():
            logger.warning(f"No services found for layer '{name}'")
            return {"error": f"Error retrieving Layer with name {name}"}

        # Retrieve related maps
        maps = Webmap.objects.filter(map_service__service_id__layer__in=layer_items).distinct()

        # Retrieve applications linked via Webmaps
        app_map = App.objects.filter(app_map__webmap_id__map_service__service_id__layer__in=layer_items) \
            .annotate(usage_type=F("app_map__rel_type")) \
            .annotate(owner=Subquery(User.objects.filter(id=OuterRef("app_owner_id")).values("user_username"))) \
            .distinct()

        # Retrieve applications directly linked to services
        app_service = App.objects.filter(app_service__service_id__layer__in=layer_items) \
            .annotate(usage_type=F("app_service__rel_type")) \
            .annotate(owner=Subquery(User.objects.filter(id=OuterRef("app_owner_id")).values("user_username"))) \
            .distinct()

        # Combine applications from both sources
        apps_combined = combine_apps(app_service.union(app_map))

        # Log retrieved data
        logger.debug(f"Retrieved {services.count()} services for Layer: {name}")
        logger.debug(f"Retrieved {maps.count()} maps for Layer: {name}")
        logger.debug(f"Retrieved {len(apps_combined)} apps for Layer: {name}")

        # Build tree structure
        for service in services:
            service_data = {
                "name": service.service_name,
                "url": service.service_url,
                "instance": service.portal_instance.alias if service.portal_instance else None,
                "children": []
            }

            # Process maps under the service
            for map_instance in service.maps.all():
                map_data = {
                    "name": map_instance.webmap_title,
                    "url": map_instance.webmap_url,
                    "instance": map_instance.portal_instance.alias if map_instance.portal_instance else None,
                    "children": []
                }

                # Process applications linked to the map
                for app in map_instance.app_map_set.all():
                    if app.app_id:
                        app_data = {
                            "name": app.app_id.app_title,
                            "url": app.app_id.app_url,
                            "instance": app.app_id.portal_instance.alias if app.app_id.portal_instance else None,
                            "children": []
                        }
                        map_data["children"].append(app_data)

                service_data["children"].append(map_data)

            tree["children"].append(service_data)

        return {"tree": tree,
                "services": services,
                "maps": maps,
                "apps": apps_combined,
                "item": layer_items.first()}

    except Exception as e:
        logger.exception(
            f"Error retrieving details for Layer '{name}' in Database '{database}' on Server '{dbserver}': {e}")
        return {"error": f"Error retrieving Layer with name {name}"}


def epoch_to_datetime(timestamp):
    """
    Convert an epoch timestamp (in milliseconds) to a localized datetime object.

    :param timestamp: The epoch timestamp to convert, in milliseconds.
    :type timestamp: int or None

    :returns: A timezone-aware datetime object representing the local time of the given timestamp,
              or `None` if the input timestamp is `None` or `-1`.
    :rtype: datetime.datetime or None

    :note:
        - The function converts the timestamp to local time using the system's time zone.
        - It ensures precise millisecond handling.
    """
    if timestamp is None or timestamp == -1:
        return None

    # Convert to datetime object with timezone awareness
    local_tz = datetime.datetime.now().astimezone().tzinfo  # Get system's local timezone
    return datetime.datetime.fromtimestamp(timestamp / 1000.0, tz=local_tz)


def get_usage_report(services):
    """
    Processes and retrieves a usage report based on given service instances.

    The code processes a collection of service instances, extracts relevant service
    URLs, and queries back-end GIS server resources to retrieve a usage report for
    the last month. The function builds a chart data structure containing time-
    slices, resource utilization, and metrics for requests from the services.
    The report captures and organizes the data per individual service hosted
    by the portal instances.

    :param services: Queryset of Service objects, where each service has a collection
        of URLs and a reference to its portal instance.
    :type services: queryset
    :return: A dictionary containing usage data with a "usage" key on success or an
        "error" message in case of failure.
    :rtype: dict
    """

    def process_service_urls(services):
        """Processes input services into a structured query dictionary."""
        query = {}
        for service in services:
            urls = service.service_url_as_list()
            instance = service.portal_instance.alias

            if instance not in query:
                query[instance] = []

            for url in urls:
                new_url = url.split("services/")[1].replace("/MapServer", ".MapServer").replace("/FeatureServer",
                                                                                                ".FeatureServer")
                query[instance].append(new_url)

        return query

    try:
        chartdata = {"datasets": []}

        query = process_service_urls(services)
        for instance, urls in query.items():
            try:
                instance_item = Portal.objects.get(alias=instance)
            except Portal.DoesNotExist:
                logger.warning(f"Instance '{instance}' is not available.")
                continue
            if not instance_item.store_password:
                continue
            try:
                target = connect(instance_item, instance_item.username, instance_item.password)
                gis_servers = target.admin.servers.list()

                if not gis_servers:
                    raise ValueError(f"No GIS servers found for instance: {instance}")

                query_list = ",".join(f"services/{service_url}" for service_url in urls)

                quick_report = gis_servers[0].usage.quick_report(
                    since="LAST_MONTH",
                    queries=query_list,
                    metrics="RequestCount"
                )

                chartdata["labels"] = [epoch_to_date(time_slice) for time_slice in quick_report["report"]["time-slices"]]

                for service_data in quick_report["report"]["report-data"][0]:
                    dataset = {
                        "label": service_data["resourceURI"].replace("services", instance),
                        "data": [0 if usage_data is None else usage_data for usage_data in service_data["data"]],
                        "fill": False
                    }
                    chartdata["datasets"].append(dataset)

            except Exception as e:
                logger.exception(f"Error retrieving usage report for {instance}: {e}")
                return {"error": "Error retrieving usage report."}

        return {"usage": chartdata}

    except Exception as e:
        logger.exception(f"Error retrieving usage report: {e}")
        return {"error": "Error retrieving usage report"}


def epoch_to_date(timestamp):
    """
    Convert a timestamp in milliseconds to a date string in 'YYYY-MM-DD' format.

    :param timestamp: The timestamp to convert, in milliseconds.
    :type timestamp: int or None

    :returns: A string representing the date in 'YYYY-MM-DD' format, or `None` if the input is invalid.
    :rtype: str or None
    """
    if isinstance(timestamp, (int, float)) and timestamp >= 0:
        return datetime.datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d")
    return None


def find_layer_usage(instance_item, layer_url, username=None, password=None):
    """
    Identify web maps and web applications that reference a specific layer URL in an ArcGIS portal.

    :param instance_item: The ArcGIS portal instance to search.
    :type instance_item: str
    :param layer_url: The URL of the layer to search for.
    :type layer_url: str
    :param username: The username for authentication (optional).
    :type username: str, optional
    :param password: The password for authentication (optional).
    :type password: str, optional

    :returns: A dictionary containing:
        - **maps** (*list*): Web maps referencing the specified layer.
        - **apps** (*list*): Web applications referencing the specified layer.
    :rtype: dict

    :raises Exception: If there is an issue connecting to the portal or retrieving data.
    """
    # Normalize layer URL (remove layer ID if present)
    layer_url_base = "/".join(layer_url.split("/")[:-1]) if layer_url.split("/")[-1].isdigit() else layer_url

    try:
        # Connect to ArcGIS portal
        target = connect(instance_item, username, password)
    except Exception as e:
        logger.exception(f"Error connecting to {instance_item}: {e}")
    try:
        # Search for web maps and web applications
        webmap_list = target.content.search("NOT owner:esri*", "Web Map", max_items=2000)
        app_types = ["Web Mapping Application", "Dashboard", "Web AppBuilder Apps",
                     "Experience Builder", "Form", "Story Map"]
        app_list = [app for app_type in app_types for app in
                    target.content.search("NOT owner:esri*", app_type, max_items=2000)]

        logger.debug(f"Found {len(webmap_list)} web maps and {len(app_list)} applications.")

    except Exception as e:
        logger.exception(f"Error retrieving content in {instance_item}: {e}")
        return {"error": f"Error retrieving content in {instance_item}"}

    # Helper function to process items
    def extract_usage_data(item):
        """Extract relevant metadata for maps/apps referencing the layer URL."""
        try:
            if layer_url in str(item.get_data()):
                return {
                    "portal_instance": instance_item,
                    "id": item.id,
                    "title": item.title,
                    "url": item.homepage,
                    "owner": item.owner,
                    "created": epoch_to_datetime(item.created),
                    "modified": epoch_to_datetime(item.modified),
                    "access": item.access,
                    "extent": item.extent,
                    "description": item.description,
                    "type": item.type,
                    "views": item.numViews
                }
        except Exception as e:
            logger.exception(f"Error processing {item.title}: {e}")
            return {"error": f"Error processing {item.title}"}
        return None

    # Extract relevant usage details
    maps = list(filter(None, [extract_usage_data(webmap) for webmap in webmap_list]))
    apps = list(filter(None, [extract_usage_data(app) for app in app_list]))

    return {"maps": maps, "apps": apps}


def get_duplicates(instance_item, username=None, password=None, similarity_threshold=70):
    """
    Identify duplicate web maps, services, layers, and applications based on name similarity.

    :param instance_item: ArcGIS portal instance to search for duplicates.
    :type instance_item: str
    :param username: Username for authentication (optional).
    :type username: str, optional
    :param password: Password for authentication (optional).
    :type password: str, optional
    :param similarity_threshold: Minimum similarity score (0-100) for considering items as duplicates.
    :type similarity_threshold: int, optional

    :returns: A dictionary containing duplicate web maps, services, layers, and apps.
    :rtype: dict
    """
    try:
        # Helper function to find duplicates
        def find_duplicates(items, key_func, additional_filter=None):
            """Find duplicates based on fuzzy name similarity."""
            duplicates = []
            seen = set()

            for item1, item2 in combinations(items, 2):  # Avoid redundant comparisons
                name1, name2 = key_func(item1), key_func(item2)
                similarity_score = fuzz.token_sort_ratio(name1, name2)

                if similarity_score >= similarity_threshold and (item1, item2) not in seen:
                    if additional_filter and not additional_filter(item1, item2):
                        continue  # Skip pairs that don't meet extra conditions

                    duplicates.append((item1, item2, similarity_score))
                    seen.add((item1, item2))
                    seen.add((item2, item1))  # Ensure bidirectional uniqueness

            return duplicates

        # Retrieve data from database
        webmaps = Webmap.objects.filter(portal_instance=instance_item)
        services = Service.objects.filter(portal_instance=instance_item)
        layers = Layer.objects.filter(portal_instance=instance_item)
        apps = App.objects.filter(portal_instance=instance_item)

        # Find duplicates using optimized search
        duplicate_webmaps = find_duplicates(webmaps, lambda wm: wm.webmap_title)
        duplicate_services = find_duplicates(
            services,
            lambda s: s.service_name.split("\\")[-1],
            lambda s1, s2: s1.service_mxd_server != s2.service_mxd_server
        )
        duplicate_layers = find_duplicates(
            layers,
            lambda l: l.layer_name,
            lambda l1, l2: "Hosted" in {l1.layer_server, l2.layer_server}
        )
        duplicate_apps = find_duplicates(apps, lambda a: a.app_title)

        return {
            "webmaps": sorted(duplicate_webmaps, reverse=True, key=lambda x: x[2]),
            "services": sorted(duplicate_services, reverse=True, key=lambda x: x[2]),
            "layers": sorted(duplicate_layers, reverse=True, key=lambda x: x[2]),
            "apps": sorted(duplicate_apps, reverse=True, key=lambda x: x[2])
        }

    except Exception as e:
        logger.exception(f"Error finding duplicates: {e}")
        return {"error": "Error finding duplicates"}


def get_missing_item_attrs(item):
    """
    Returns a dictionary of True/False values for specific attributes and metadata
    for a portal item, along with the item's ID, title, URL, and other relevant details.

    The function checks the compliance of key metadata fields (description, thumbnail,
    snippet, access information, and license information) to determine if they are
    properly filled. The result includes additional metadata about the item.

    :param item: The portal item to check for missing attributes.
    :type item: object

    :returns: A dictionary containing:
        - 'description': True if it exists and is sufficiently long, False if missing or too short.
        - 'thumbnail': True if valid, False if missing or non-compliant.
        - 'snippet': True if sufficiently long, False if missing or too short.
        - 'accessInformation': True if provided, False if missing.
        - 'licenseInfo': True if provided, False if missing.
        - 'id': The item's ID.
        - 'title': The item's title.
        - 'url': The item's URL.
        - 'type': The item's type.
        - 'owner': The item's owner.
        - 'scoreCompleteness': The item's completeness score.
    :rtype: dict
    """

    def is_valid_text(attr_value, min_length=50):
        """Returns True if the text exists and meets the length requirement, else False or 'Too short'."""
        if not attr_value:
            return False
        return True if len(attr_value) >= min_length else "Too short"

    # Check compliance for each attribute
    compliance_status = {
        "description": is_valid_text(getattr(item, "description", None)),
        "snippet": is_valid_text(getattr(item, "snippet", None)),
        "accessInformation": bool(getattr(item, "accessInformation", None)),
        "licenseInfo": bool(getattr(item, "licenseInfo", None)),
        "thumbnail": bool(getattr(item, "thumbnail", None) and "ago_downloaded" in getattr(item, "thumbnail", {})),
    }

    # Add metadata details
    compliance_status.update({
        "id": item.id,
        "title": item.title,
        "url": item.homepage,
        "type": item.type,
        "owner": item.owner,
        "scoreCompleteness": item.scoreCompleteness,
    })

    return compliance_status


def get_metadata(instance_item, username=None, password=None):
    """
    Retrieves metadata compliance for all content items in a portal instance.

    The function connects to the specified portal instance, retrieves content that is not
    owned by 'esri', and checks each item for missing or non-compliant attributes.
    The compliance status for each item is compiled into a structured list.

    :param instance_item: The portal instance to connect to and retrieve content from.
    :type instance_item: object
    :param username: The username to authenticate with the portal. (optional)
    :type username: str, optional
    :param password: The password to authenticate with the portal. (optional)
    :type password: str, optional

    :returns: A list of dictionaries, each containing the compliance status and metadata of an item.
    :rtype: list of dict
    """
    content_metadata = []
    try:
        # Establish a connection to the portal
        target = connect(instance_item, username, password)
    except Exception as e:
        # If connection fails, log the error and return a structured response
        logging.exception(f"Unable to connect to {instance_item}: {e}")
        return {"error": "Error finding duplicates"}

    try:
        # Retrieve up to 10,000 non-Esri content items
        content_items = target.content.search("NOT owner:esri*", max_items=10000)

        # Process each content item for missing attributes
        content_metadata = [get_missing_item_attrs(item) for item in content_items]

    except Exception as e:
        # Catch and log any errors that occur while fetching metadata
        logging.exception(f"Error retrieving metadata from {instance_item}: {e}")
        return {"error": "Error retrieving metadata"}

    return {"metadata": content_metadata}


def extract_webappbuilder(data, path="", parent_key=None):
    """
    Recursively extracts all URLs from a given data structure (ArcGIS Web App Builder data),
    along with the corresponding paths and parent keys, and returns them as a list of URL-path pairs.

    This function traverses through a nested structure (dicts and lists) and searches for
    URLs within any string values. It collects the URLs along with the path to their location
    and the parent key from which they were found. The URLs are returned as a list of
    `[url, path, parent_key]` tuples.

    :param data: The data structure (ArcGIS Web App Builder) to extract URLs from.
    :type data: dict | list | str
    :param path: The current path to the data in the structure (used for recursion). Defaults to an empty string.
    :type path: str, optional
    :param parent_key: The key of the parent item (used for recursion). Defaults to None.
    :type parent_key: str, optional

    :returns: A list of tuples containing URLs, the paths where they are located, and their parent keys.
    :rtype: list of lists
    """
    urls = []
    import re
    if isinstance(data, dict):
        for key, value in data.items():
            if parent_key is not None:
                current_path = f"{path}.{parent_key}.{key}"
            else:
                current_path = f"{path}.{key}" if path else key
            urls.extend(extract_webappbuilder(value, current_path, key))
    elif isinstance(data, list):
        for idx, item in enumerate(data):
            current_path = f"{path}.{parent_key}[{idx}]" if parent_key is not None else f"{path}[{idx}]"
            urls.extend(extract_webappbuilder(item, current_path, None))
    elif isinstance(data, str):
        # Use regular expression to find URLs in strings
        for url in re.findall(r"https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[-\w./?=&]+", data):
            urls.append([url, path, parent_key])

    return urls


def extract_dashboard(data, path=[], keys=[]):
    """
    Recursively extracts all occurrences of 'itemId' from a nested data structure
    (Dashboard data), along with their corresponding paths and keys.

    This function traverses through dictionaries and lists to find keys named 'itemId'.
    It returns a list of tuples containing:
    - The full path to the 'itemId' within the data structure.
    - The sequence of keys leading to the 'itemId'.
    - The corresponding 'itemId' value.

    :param data: The data structure (ArcGIS Dashboard) to search for 'itemId'.
    :type data: dict | list
    :param path: The current hierarchical path of keys leading to the current data level (used for recursion).
    :type path: list, optional
    :param keys: The sequence of keys leading to the current data level (used for recursion).
    :type keys: list, optional

    :returns: A list of tuples, where each tuple contains:
              - The full path as a string (slash-separated).
              - The list of keys leading to the 'itemId'.
              - The extracted 'itemId' value.
    :rtype: list of tuples (str, list, str)
    """
    item_ids = []

    if isinstance(data, dict):
        for key, value in data.items():
            # Update path and keys for the current level
            current_path = path + [key]
            current_keys = keys + [key]

            # Check if the current key is 'itemId' and add to item_ids list
            if key == "itemId":
                current_keys.append("itemId")
                # Construct the full path
                full_path = "/".join(path)
                item_ids.append((full_path, current_keys, value))

            item_ids.extend(
                extract_dashboard(value, current_path, current_keys))  # Recursive call for nested dictionaries
    elif isinstance(data, list):
        for i, item in enumerate(data):
            # Update path for the current level
            current_path = path + [str(i)]
            item_ids.extend(extract_dashboard(item, current_path, keys))  # Recursive call for nested lists

    return item_ids


def extract_experiencebuilder(data, path=[], keys=[]):
    """
    Recursively extracts all occurrences of 'itemId' and URLs from a nested data structure
    (ArcGIS Experience Builder data), along with their corresponding paths, keys, and type values.

    This function traverses through dictionaries and lists to find:
    - Keys named 'itemId'
    - String values that start with 'http' (indicating URLs)

    It returns a list of tuples containing:
    - The full path to the 'itemId' or URL within the data structure.
    - The sequence of keys leading to the 'itemId' or URL.
    - The extracted 'itemId' or URL value.
    - The associated 'type' value (if present at the same dictionary level, otherwise None).

    :param data: The data structure (ArcGIS Experience Builder) to search for 'itemId' and URLs.
    :type data: dict | list
    :param path: The current hierarchical path of keys leading to the current data level (used for recursion).
    :type path: list, optional
    :param keys: The sequence of keys leading to the current data level (used for recursion).
    :type keys: list, optional

    :returns: A list of tuples, where each tuple contains:
              - The full path as a string (slash-separated).
              - The list of keys leading to the 'itemId' or URL.
              - The extracted 'itemId' or URL value.
              - The associated 'type' value if present, otherwise None.
    :rtype: list of tuples (str, list, str, str | None)
    """
    results = []

    if isinstance(data, dict):
        for key, value in data.items():
            # Update path and keys for the current level
            current_path = path + [key]
            current_keys = keys + [key]

            # Check if the current key is 'itemId' or a URL and add to results list
            if key == "itemId" or (isinstance(value, str) and value.startswith("http")):
                current_keys.append(key)
                # Construct the full path
                full_path = "/".join(path)
                # Find the 'type' key at the same level
                if "type" in data:
                    results.append((full_path, current_keys, value, data["type"]))
                else:
                    results.append((full_path, current_keys, value, None))

            results.extend(extract_experiencebuilder(value, current_path,
                                                     current_keys))  # Recursive call for nested dictionaries
    elif isinstance(data, list):
        for i, item in enumerate(data):
            # Update path for the current level
            current_path = path + [str(i)]
            results.extend(extract_experiencebuilder(item, current_path, keys))  # Recursive call for nested lists

    return results


def extract_storymap(data, path=[], keys=[]):
    """
    Recursively extracts 'itemType' and 'itemId' values from a nested data structure (ArcGIS StoryMap data),
    returning their corresponding paths and keys.

    This function searches through dictionaries and lists, identifying occurrences of:
    - 'itemType'
    - 'itemId'

    It returns a list of tuples, where each tuple contains:
    - The full hierarchical path to the item.
    - The key list leading to 'itemType'.
    - The extracted 'itemType' value.
    - The key list leading to 'itemId'.
    - The extracted 'itemId' value.

    :param data: The data structure (ArcGIS StoryMap) to search for 'itemType' and 'itemId'.
    :type data: dict | list
    :param path: The current hierarchical path of keys leading to the current data level (used for recursion).
    :type path: list, optional
    :param keys: The sequence of keys leading to the current data level (used for recursion).
    :type keys: list, optional

    :returns: A list of tuples, each containing:
              - The full path as a string (slash-separated).
              - The list of keys leading to 'itemType'.
              - The extracted 'itemType' value.
              - The list of keys leading to 'itemId'.
              - The extracted 'itemId' value.
    :rtype: list of tuples (str, list, str, list, str)
    """
    results = []

    if isinstance(data, dict):
        if "itemType" in data:
            item_type = data.get("itemType")
            item_id = data.get("itemId")
            full_path = "/".join(path)
            results.append((full_path, ["itemType"], item_type, ["itemId"], item_id))

        for key, value in data.items():
            current_path = path + [key]
            current_keys = keys + [key]
            results.extend(extract_storymap(value, current_path, current_keys))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            current_path = path + [str(i)]
            results.extend(extract_storymap(item, current_path, keys))

    return results


def combine_apps(queryset):
    """
    Aggregates and combines application data from a queryset by grouping based on
    common attributes and merging usage types.

    This function:
    - Converts the queryset into a list of dictionaries.
    - Sorts the list based on 'app_id' and 'portal_instance_id'.
    - Groups the sorted list by a predefined set of application attributes.
    - Combines the 'usage_type' values within each group into a single string.

    :param queryset: A Django QuerySet containing application data.
    :type queryset: QuerySet

    :returns: A list of dictionaries, where each dictionary represents a unique
              application with aggregated 'usage_type' values.
    :rtype: list of dict

    :note:
        - If an application has multiple usage types, they will be concatenated
          into a comma-separated string.
    """
    # Convert the annotated queryset to a list of dictionaries
    queryset_list = list(queryset.values())
    queryset_sorted = sorted(queryset_list, key=itemgetter("app_id", "portal_instance_id"))

    # Group the queryset list by the grouping field
    grouped_results = groupby(queryset_sorted, key=itemgetter("app_id", "portal_instance_id", "app_title", "app_url",
                                                              "app_type", "owner", "app_created", "app_modified",
                                                              "app_access", "app_views"))

    # Combine the results for each group
    combined_results = []
    for group_key, group_data in grouped_results:
        # Initialize variables to store combined values
        other_field_values = []

        # Add all usage types to list
        other_field_values.extend([item["usage_type"] for item in group_data if item not in other_field_values])


        # Join unique usage types
        if len(other_field_values) > 1:
            combined_value = ", ".join(other_field_values)
        else:
            combined_value = other_field_values[0]

        # Add combined result to the list
        combined_results.append({
            "app_id": group_key[0],
            "portal_instance": group_key[1],
            "app_title": group_key[2],
            "app_url": group_key[3],
            "app_type": group_key[4],
            "app_owner": group_key[5],
            "app_created": group_key[6],
            "app_modified": group_key[7],
            "app_access": group_key[8],
            "app_views": group_key[9],
            "usage_type": combined_value
        })
    return combined_results
