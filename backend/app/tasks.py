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
import json
import logging
import re
import time

import arcgis.gis.server
import requests
from bs4 import BeautifulSoup
from celery import shared_task, group, chord, current_app
from celery.exceptions import Ignore
from celery_progress.backend import ProgressRecorder, Progress
from config.settings.base import USE_SERVICE_USAGE_REPORT
from django.core.exceptions import MultipleObjectsReturned

from .models import Webmap, Service, Layer, App, User, Map_Service, Layer_Service, App_Map, App_Service, Portal
from .webmaps import (connect, epoch_to_datetime, epoch_to_date, UpdateResult, extract_storymap,
                      extract_dashboard, extract_experiencebuilder, extract_webappbuilder)

logger = logging.getLogger(__name__)


@shared_task(bind=True)
def update_all(self, instance, items):
    for item in items:
        if item == "webmaps":
            update_webmaps.enqueue(instance, False)
        if item == "services":
            update_services.enqueue(instance, False)
        if item == "webapps":
            update_webapps.enqueue(instance, False)
        if item == "users":
            update_users.enqueue(instance, False)


@shared_task(bind=True, name="Update webmaps", time_limit=6000, soft_time_limit=3000)
def update_webmaps(self, instance_alias, overwrite=False, username=None, password=None):
    """
    Update web maps for a given portal instance.

    This task connects to a target portal instance, retrieves web maps,
    processes each map's details (including layers, usage statistics, and service relationships),
    and updates or creates corresponding database records. It also removes any records that were not updated,
    ensuring the database reflects the current state of the portal.

    :param instance_item: Portal instance object with connection and configuration details.
    :type instance_item: PortalInstance
    :param overwrite: Flag indicating whether existing web maps should be overwritten ('true' for overwrite).
    :type overwrite: bool or str
    :param username: Username for portal authentication (optional).
    :type username: str, optional
    :param password: Password for portal authentication (optional).
    :type password: str, optional
    :return: JSON-serialized update result containing counts of inserts, updates, deletions, and any error messages.
    :rtype: str
    """
    # Initialize progress recorder and result container for tracking task progress and outcome.
    progress_recorder = ProgressRecorder(self)
    result = UpdateResult()

    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        # Connect to the target portal using provided credentials.
        target = connect(instance_item, username, password)
    except Exception as e:
        logger.exception(f"Unable to connect to {instance_alias}...{e}")
        # Record connection error and return error result.
        result.add_error(f"Unable to connect to {instance_alias}")
        self.update_state(state="FAILURE", meta={result.to_json()})
        raise Ignore()

    try:
        if instance_item.portal_type == "agol":
            org_id = instance_item.org_id
            # Retrieve web maps from the portal, filtering out those owned by 'esri'
            total_webmaps = target.content.advanced_search(query=f'orgid:{org_id} AND NOT owner:esri*', max_items=-1,
                                                           return_count=True, filter='type:"Web Map"')
        else:
            org_id = None
            total_webmaps = target.content.advanced_search(query='NOT owner:esri*', max_items=-1,
                                                           return_count=True, filter='type:"Web Map"')
        logger.info(f"Total: {total_webmaps}")
        # Capture the current time to mark the update.
        update_time = datetime.datetime.now()

        # If overwrite flag is set to true, delete all existing web maps for the portal instance.
        if overwrite:
            Webmap.objects.filter(portal_instance=instance_item).delete()

        batch_size = 100
        batch_tasks = []
        # Iterate through each retrieved web map.
        for batch in range(0, total_webmaps, batch_size):
            batch_tasks.append(process_batch_maps.s(instance_alias, username, password, batch, batch_size, update_time))

        # Parallel processing by folder
        task_group = group(batch_tasks)
        batch_results = task_group.apply_async()
        # batch_results.save()
        while not batch_results.ready():
            completed_tasks = sum(
                task.result.get("current", 0) if "current" in task.result else batch_size
                for task in batch_results.children
                if task.result
            )
            logger.info(f"Completed {completed_tasks/total_webmaps}")
            progress_recorder.set_progress(completed_tasks, total_webmaps)
            time.sleep(1.5)
        logger.info(f"Aggregating batch results: {batch_results}")
        for batch in batch_results.get(disable_sync_subtasks=False):
            batch_result = UpdateResult(**batch["result"])
            if batch_result.success is False:
                result.success = False
            result.num_updates += batch_result.num_updates
            result.num_inserts += batch_result.num_inserts
            result.num_deletes += batch_result.num_deletes
            result.num_errors += batch_result.num_errors
            result.error_messages.extend(batch_result.error_messages)

        # Delete web maps not updated in this run, implying they have been removed.
        delete_outdated_records(instance_item, update_time, [Webmap, Map_Service], result)

        # Update the portal instance's last updated timestamp.
        instance_item.webmap_updated = datetime.datetime.now()
        instance_item.save()

        # Mark the overall update result as successful.
        result.set_success()
        return result.to_json()

    except Exception as e:
        # Log any unexpected exceptions during the update process and record the error.
        logger.exception(f"Unable to update webmaps for {instance_alias}...{e}")
        result.add_error(f"Unable to update webmaps for {instance_alias}")
        # Get child tasks
        if batch_results and batch_results.children:
            for child in batch_results.children:
                if child.id:
                    current_app.control.revoke(child.id, terminate=True, signal="SIGKILL")

        # Also revoke the current task
        # current_app.control.revoke(self.request.id, terminate=True, signal="SIGKILL")

        self.update_state(state="FAILURE", meta=result.to_json())
        raise Ignore()


@shared_task(bind=True, time_limit=6000, soft_time_limit=3000)
def process_batch_maps(self, instance_alias, username, password, batch, batch_size, update_time):
    result = UpdateResult()
    progress_recorder = ProgressRecorder(self)
    instance_item = Portal.objects.get(alias=instance_alias)
    target = connect(instance_item, username, password)
    if instance_item.portal_type == "agol":
        org_id = instance_item.org_id
        webmap_item_list = target.content.advanced_search(query=f'orgid:{org_id} AND NOT owner:esri*', max_items=batch_size,
                                                      start=batch, sort_field="title", sort_order="asc",
                                                      filter='type:"Web Map"')
    else:
        webmap_item_list = target.content.advanced_search(query='NOT owner:esri*', max_items=batch_size,
                                                          start=batch, sort_field="title", sort_order="asc",
                                                          filter='type:"Web Map"')
    total_webmaps = len(webmap_item_list.get("results"))
    for counter, webmap_item in enumerate(webmap_item_list.get("results")):
        try:
            # logger.info(webmap_item)
            webmap_data = extract_webmap_data(webmap_item, instance_item, update_time)
            obj, created = Webmap.objects.update_or_create(
                portal_instance=instance_item, webmap_id=webmap_item.id, defaults=webmap_data
            )
            result.add_insert() if created else result.add_update()

            link_services_to_webmap(instance_item, obj, webmap_data["webmap_services"])

            # Update task progress after processing each web map.
            progress_recorder.set_progress(counter, total_webmaps)
        except Exception as e:
            # Log and record any errors encountered during processing of this web map.
            logger.exception(f"Unable to update webmap {webmap_item.id}...{e}")
            result.add_error(f"Unable to update {webmap_item.id}")
    result.set_success()
    return {"result": result.to_json()}


def extract_webmap_data(item, instance_item, update_time):
    """Extracts relevant information from a web map."""
    description = get_description(item)
    access = get_access(item)

    layers, services = process_layers(item)
    usage = calculate_usage(item, instance_item)

    return {
        "webmap_title": item.title,
        "webmap_url": item.homepage,
        "webmap_owner": get_owner(instance_item, item.owner),
        "webmap_created": epoch_to_datetime(item.created),
        "webmap_modified": epoch_to_datetime(item.modified),
        "webmap_access": access,
        "webmap_extent": item.extent,
        "webmap_description": description,
        "webmap_views": item.numViews,
        "webmap_layers": layers,
        "webmap_services": services,
        "webmap_usage": usage,
        "updated_date": update_time,
        "webmap_last_viewed": epoch_to_datetime(item.lastViewed) if instance_item.portal_type == "agol" else None
    }


def process_layers(item):
    """Processes web map layers and returns a dictionary of layers and services."""
    from arcgis.mapping import WebMap

    wm = WebMap(item)
    layers = {}
    services = set()

    for layer in (l for l in wm.layers if l.get("layerType", None) != "GroupLayer"):
        if not getattr(layer, "url", None):
            continue
        s_url = "/".join(layer.url.split("/")[:-1]) if layer.url.split("/")[-1].isdigit() else layer.url
        services.add(s_url)

        layers[layer.title] = [layer.get("url", None), layer.get("layerType", None), layer.id]

    return layers, list(services)


def calculate_usage(item, instance_item):
    """Computes usage statistics for a web map."""
    # as_df = False seems to be broken as of 2.3.1 and 4/1/2025
    # if instance_item.portal_type == "agol":
    #     try:
    #         report = item.usage(date_range="60D", as_df=False)
    #         return {epoch_to_date(int(row[0])): int(row[1]) for row in report["data"][0]["num"]}
    #     except IndexError:
    #         return {}
    return {}
    # Attempt at gathering usage data for Enterprise items
    # Requires retrieving data every day which increases view count
    # try:
    #     obj = Webmap.objects.get(portal_instance=instance_item, webmap_id=item.id)
    #     usage = obj.webmap_usage or {}
    # except Webmap.DoesNotExist:
    #     usage = {}
    #
    # if len(usage) >= 60:
    #     del usage[min(usage.keys())]
    #
    # prev_views = list(usage.values())[-1] if usage else 0
    # usage[datetime.date.today().strftime('%Y-%m-%d')] = item.numViews - prev_views
    # return usage


def get_owner(instance_item, owner_username):
    """Retrieves the owner of a web map, if available."""
    return User.objects.filter(portal_instance=instance_item, user_username=owner_username).first()


def link_services_to_webmap(instance_item, webmap_obj, services):
    """Links web maps to services if they exist in the database."""
    update_time = datetime.datetime.now()
    for service_url in services:
        try:
            s_obj = Service.objects.get(service_url__iregex=rf"^{service_url}$")
        except Service.DoesNotExist:
            continue
        except MultipleObjectsReturned:
            s_obj = Service.objects.filter(service_url__iregex=rf"{service_url}").first()
        Map_Service.objects.update_or_create(
            portal_instance=instance_item, webmap_id=webmap_obj, service_id=s_obj,
            defaults={"updated_date": update_time}
        )
    deletes = Map_Service.objects.filter(portal_instance=instance_item, webmap_id=webmap_obj,
                                         updated_date__lt=update_time)
    deletes.delete()


def get_access(item):
    """
    Format an item's access information.
    :param item: arcgis Item
    :type item: arcgis.gis.content.Item
    :return:
    """
    if item.access == "shared":
        return f"Groups: {', '.join(group.title for group in item.sharing.shared_with['groups'])}"
    else:
        return item.access.title()


def get_description(item):
    """
    Extracts and returns the text content from the description of the given item.
    If the `description` attribute of the item is not `None`, it will parse the HTML
    content to extract plain text. Otherwise, it will return the string "None".

    :param item: arcgis Item
    :type item: arcgis.gis.content.Item
    :return: The plain text extracted from the item's description. If no description
        is present, the function returns the string "None".
    :rtype: str
    """
    if item.description:
        soup = BeautifulSoup(item.description, "html.parser")
        description = soup.get_text()
        return description
    return "None"


def delete_outdated_records(instance_item, update_time, models, result):
    """
    Deletes outdated records from the specified models where `updated_date` is older than `update_time`.

    :param instance_item: The portal instance associated with the records.
    :type instance_item: model
    :param update_time: The timestamp of the current update cycle.
    :type update_time: datetime.datetime.datetime
    :param models: A list of Django model classes to delete outdated records from.
    :type models: list
    :param result:
    :type result:
    """
    for model in models:
        deleted_records = model.objects.filter(portal_instance=instance_item, updated_date__lt=update_time)
        result.add_delete(deleted_records.count())
        deleted_records.delete()


# Utility functions
def compile_regex_patterns():
    """Compile and return regex patterns for connection string extraction."""
    patterns = {
        "server": re.compile(r"(?<=SERVER=)([^;]*)"),
        "version": re.compile(r"(?<=VERSION=)([^;]*)"),
        "branch": re.compile(r"(?<=BRANCH=)([^;]*)"),
        "database": re.compile(r"(?<=DATABASE=)([^;]*)"),
    }
    return patterns


def fetch_portal_item_details(target, item_id, instance_item):
    """
    Retrieve details from the portal item.

    Returns a tuple in the format:
      (owner, access, description)
    If the item or any property is not available, defaults are returned.
    """
    owner, access, description = None, None, None
    portal_item = target.content.get(item_id)
    if portal_item:
        owner = get_owner(instance_item, portal_item.owner)

        # Validate portal_item.access and shared_with exist.
        if hasattr(portal_item, "access"):
            access = get_access(portal_item)
        description = get_description(portal_item)
    return owner, access, description


def process_databases(manifest, regex_patterns, instance_item, s_obj, update_time):
    """
    Process database details from the manifest and update the database records.
    """
    if "databases" not in manifest:
        return
    for db_obj in manifest["databases"]:
        db_server, db_version, db_database = "", "", ""
        connection_string = db_obj.get("onServerConnectionString", "")
        if "Sde" in db_obj.get("onServerWorkspaceFactoryProgID", ""):
            match_server = regex_patterns["server"].search(connection_string)
            if match_server:
                db_server = match_server.group(0).upper()
            match_version = regex_patterns["version"].search(connection_string) or \
                            regex_patterns["branch"].search(connection_string)
            if match_version:
                db_version = match_version.group(0).upper()
            match_db = regex_patterns["database"].search(connection_string)
            if match_db:
                db_database = match_db.group(0).upper()
            db = f"{db_server}@{db_database}@{db_version}"
        elif "FileGDB" in db_obj.get("onServerWorkspaceFactoryProgID", ""):
            parts = connection_string.split("DATABASE=")
            if len(parts) > 1:
                db_database = parts[1].replace(r"\\", "\\")
            db = db_database
        else:
            continue

        for dataset in db_obj.get("datasets", []):
            # Only process if expected names exist.
            dataset_name = dataset.get("onServerName")
            if not dataset_name:
                continue
            layer_obj, _ = Layer.objects.update_or_create(
                portal_instance=instance_item,
                layer_server=db_server,
                layer_version=db_version,
                layer_database=db_database,
                layer_name=dataset_name,
                defaults={"updated_date": update_time},
            )
            Layer_Service.objects.update_or_create(
                portal_instance=instance_item,
                layer_id=layer_obj,
                service_id=s_obj,
                defaults={"updated_date": update_time},
            )


def get_map_name(service_url, token):
    """
    Fetch the map name from the provided service URL.

    The base service URL seems to be the only place where map name can be retrieved
    as it does not exist in admin endpoints or available in the Python API.

    :param service_url: The URL of the service where the map data will be fetched.
    :type service_url: str
    :param token: The authorization token required for accessing the service.
    :type token: str
    :return: The name of the map retrieved from the service, or None if an error
        occurs or the 'mapName' data is unavailable.
    :rtype: str or None
    """
    try:
        headers = {
            "X-Esri-Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        params = {
            "f": "json"
        }
        response = requests.get(service_url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            return data.get("mapName", None)
    except Exception as e:
        pass
    return None


@shared_task(bind=True, time_limit=6000, soft_time_limit=3000, name="Update services")
def update_services(self, instance_alias, overwrite=False, username=None, password=None):
    """
    Update service and layer records for a given portal instance.

    This task synchronizes service and layer data from a portal instance with the local database.
    Depending on the portal type (AGOL or non-AGOL), it retrieves service items, processes usage statistics,
    descriptions, ownership, and related layer information, then updates or creates corresponding records.
    Outdated records are removed to maintain data consistency.

    :param instance_alias: The portal instance alias.
    :type instance_alias: str
    :param overwrite: Flag indicating whether to delete existing service and layer records before updating.
    :type overwrite: bool
    :param username: Username for portal authentication (optional).
    :type username: str, optional
    :param password: Password for portal authentication (optional).
    :type password: str, optional
    :return: JSON serialized result of the update process including counts of inserts, updates, deletions, and errors.
    :rtype: str
    """
    # Initialize result container and progress tracker.
    result = UpdateResult()
    progress_recorder = ProgressRecorder(self)
    update_time = datetime.datetime.now()  # Timestamp for the current update cycle

    def fetch_usage_report(server, service_list):
        query_list = ",".join(service_list)
        quick_report = server.usage.quick_report(since="LAST_MONTH",
                                                 queries=query_list,
                                                 metrics="RequestCount")
        for s in quick_report["report"]["report-data"][0]:
            name = s["resourceURI"].replace("services/", "").split(".")[0]
            service_type = s["resourceURI"].split('.')[-1]
            data = [0 if d is None else d for d in s["data"]]
            baseline = sum(data[0:15])
            compare = sum(data[15:])
            try:
                trend = ((compare - baseline) / baseline) * 100
            except ZeroDivisionError:
                trend = 0 if (baseline == 0 and compare == 0) else 999
            try:
                s_obj = Service.objects.get(portal_instance=instance_item,
                                            service_name=name,
                                            service_type=service_type)
            except Service.DoesNotExist:
                continue
            except MultipleObjectsReturned:
                s_obj = Service.objects.filter(portal_instance=instance_item,
                                               service_name=name,
                                               service_type=service_type).first()
            s_obj.service_usage = data
            s_obj.service_usage_trend = trend
            s_obj.save()

    def process_views(view_list):
        for view_item in view_list:
            try:
                service_views = view_item.related_items(rel_type="Service2Service", direction="reverse")
                for parent in service_views:
                    logger.debug(f"{view_item.url} related to {parent.url}")
                    try:
                        view_obj = Service.objects.get(portal_instance=instance_item,
                                                       service_url__iregex=rf'^{view_item.url}$')
                    except Service.DoesNotExist:
                        continue
                    except MultipleObjectsReturned:
                        view_obj = Service.objects.filter(portal_instance=instance_item,
                                                          service_url__iregex=rf'{view_item.url}').first()
                    try:
                        service_obj = Service.objects.get(portal_instance=instance_item,
                                                          service_url__iregex=rf'^{parent.url}$')
                    except Service.DoesNotExist:
                        continue
                    except MultipleObjectsReturned:
                        service_obj = Service.objects.filter(portal_instance=instance_item,
                                                             service_url__iregex=rf'{parent.url}').first()
                    logger.debug(f"{service_obj} related to {view_obj}")

                    view_obj.service_view = service_obj
                    service_obj.service_view = view_obj
                    view_obj.save()
                    service_obj.save()
            except Exception as e:
                logger.exception(f"Error with view: {e}")

    instance_item = Portal.objects.get(alias=instance_alias)
    if instance_item.portal_type == "agol":
        # Connect to the AGOL portal.
        target = connect(instance_item, username, password)
        response = requests.get(f"{target.url}/sharing/rest/portals/self?culture=en&f=pjson")
        if response.status_code == 200:
            data = response.json()
            org_id = data.get("id")
        else:
            org_id = None
        try:
            if overwrite:
                # Remove existing Service and Layer records if overwrite is enabled.
                Service.objects.filter(portal_instance=instance_item).delete()
                Layer.objects.filter(portal_instance=instance_item).delete()

            # Retrieve layers by combining searches for "Map Image Layer" and "Feature Layer",
            # excluding items owned by 'esri'.
            services = (target.content.search("NOT owner:esri*", "Map Image Layer", max_items=10000) +
                        target.content.search("NOT owner:esri*", "Feature Layer", max_items=10000))
            total_services = len(services)  # Total number of layers to process
            logger.info(f"Total number of services to process: {total_services}")
            # services = target.content.advanced_search(query=f"orgid:{org_id} AND NOT owner:esri*", max_items=-1, return_count=True,
            #                                filter='type:"Map Service" OR type:"Feature Service"')
            # logger.info(f"Total number of services to process: {services}")
            for counter, service in enumerate(services):
                logger.debug(service)  # Log the current service for debugging
                try:
                    access = get_access(service)

                    url = []  # List to hold unique service URLs
                    service_layers = {}  # Dictionary to map layer names to service designations

                    # Attempt to retrieve the owner for the current service.
                    owner = get_owner(instance_item, service.owner)

                    usage = []  # Placeholder for usage statistics
                    trend = 0  # Initialize usage trend percentage
                    description = get_description(service)

                    # If service usage reporting is enabled, process the usage data.
                    if USE_SERVICE_USAGE_REPORT and org_id in service.url:
                        try:
                            trend = 0
                            report = service.usage(date_range="30D", as_df=False)
                            # Convert usage data entries to integers, defaulting to 0 when necessary.
                            usage = [0 if d is None else int(d[1]) for d in report["data"][0]["num"]]
                            baseline = sum(usage[0:15])
                            compare = sum(usage[15:])
                            try:
                                trend = ((compare - baseline) / baseline) * 100
                            except ZeroDivisionError:
                                trend = 0 if (baseline == 0 and compare == 0) else 999
                        except:
                            # Skip usage processing if data is not available.
                            pass

                    # Create a new Service record with the aggregated data.
                    s_obj, created = Service.objects.update_or_create(
                        portal_instance=instance_item,
                        service_name=service.title,
                        defaults={"service_url": service.url,
                                  "service_layers": service_layers,
                                  "service_mxd_server": service.get("sourceUrl", None),
                                  "service_mxd": None,
                                  "portal_id": service.homepage,
                                  "service_type": service.type,
                                  "service_description": description,
                                  "service_owner": owner,
                                  "service_access": access,
                                  "service_usage": usage,
                                  "service_usage_trend": trend,
                                  "service_last_viewed": epoch_to_datetime(service.lastViewed)}
                    )
                    result.add_insert() if created else result.add_update()

                    # Process associated layers if available.
                    if service.layers:
                        for layer in service.layers:
                            # Normalize URL by removing a trailing numeric segment if present.
                            s_url = layer.url if not layer.url.split("/")[-1].isdigit() else "/".join(
                                layer.url.split("/")[:-1])
                            if s_url not in url:
                                url.append(s_url)  # Append unique service URL
                            # Check for a specific hosted layer identifier and update service_layers accordingly.
                            if org_id in s_url:
                                service_layers[layer.properties.name] = "Hosted"
                                # Create a new Layer record if one does not exist.
                                obj, created = Layer.objects.update_or_create(
                                    portal_instance=instance_item,
                                    layer_server="Hosted",
                                    layer_version=None,
                                    layer_database=None,
                                    layer_name=layer.properties.name,
                                    defaults={"updated_date": update_time}
                                )
                                # Create or update the relationship between the layer and service.
                                obj, created = Layer_Service.objects.update_or_create(
                                    portal_instance=instance_item,
                                    layer_id=obj,
                                    service_id=s_obj,
                                    defaults={"updated_date": update_time}
                                )

                    progress_recorder.set_progress(counter, total_services)  # Update progress.
                except Exception as e:
                    logger.exception(f"Unable to update {service.title}: {e}")
                    result.add_error(f"Unable to update {service.title}")  # Log error for the current layer.

            # Remove outdated Service, Layer, and Layer_Service records.
            delete_outdated_records(instance_item, update_time, [Service, Layer, Layer_Service], result)

            # Search for view services and associate them with their parent services.
            view_items = target.content.search("NOT owner:esri* AND typekeywords:View Service", "Feature Layer",
                                               max_items=2000)
            process_views(view_items)

            # Update the portal instance's timestamp for service updates.
            instance_item.service_updated = datetime.datetime.now()
            instance_item.save()
            result.set_success()  # Mark the update process as successful.
            return result.to_json()
        except Exception as e:
            logger.exception(f"Unable to update services for {instance_alias}: {e}")
            result.add_error(f"Unable to update Services for {instance_alias}")
            self.update_state(state="FAILURE", meta={result.to_json()})
            raise Ignore()
    else:
        try:
            import re
            # Connect to an Enterprise portal.
            target = connect(instance_item, username, password)
            gis_servers = target.admin.servers.list()
        except Exception as e:
            logger.exception(f"Unable to connect to {instance_alias}: {e}")
            result.add_error(f"Unable to connect to {instance_item}")
            self.update_state(state="FAILURE", meta={result.to_json()})
            raise Ignore()

        try:
            # If overwrite is enabled, delete existing Service and Layer records.
            if overwrite:
                Service.objects.filter(portal_instance=instance_item).delete()
                Layer.objects.filter(portal_instance=instance_item).delete()

            service_list = []  # Track processed services for usage reporting.
            batch_tasks = []
            for gis_server in gis_servers:
                folders = gis_server.services.folders
                folders.remove("System")
                for folder in folders:
                    # Synchronous processing
                    # services = gis_server.services.list(folder=folder)
                    # for service in services:
                    #     logger.debug(service)
                    #     process_single_service(target, instance_item, service, folder, update_time, regex_patterns, result)
                    #     counter += 1
                    #     progress_recorder.set_progress(counter, total_services)
                    batch_tasks.append(
                        process_batch_services.s(instance_alias, username, password, folder, update_time))

            # Parallel processing by folder
            task_group = group(batch_tasks)
            batch_results = task_group.apply_async()
            while not batch_results.ready():
                completed_tasks = sum(1 for task in batch_results.children if task.ready())
                progress_recorder.set_progress(completed_tasks, len(folders))
                time.sleep(1.5)
            logger.info(f"Aggregating batch results: {batch_results}")
            for batch in batch_results.get(disable_sync_subtasks=False):
                batch_result = UpdateResult(**batch["result"])
                if batch_result.success is False:
                    result.success = False
                result.num_updates += batch_result.num_updates
                result.num_inserts += batch_result.num_inserts
                result.num_deletes += batch_result.num_deletes
                result.num_errors += batch_result.num_errors
                result.error_messages.extend(batch_result.error_messages)
                service_list.extend(batch["service_usage"])

            # Process service usage reports if enabled.
            if USE_SERVICE_USAGE_REPORT:
                fetch_usage_report(gis_server, service_list)

            # Remove outdated Service, Layer, and Layer_Service records.
            delete_outdated_records(instance_item, update_time, [Service, Layer, Layer_Service], result)

            # Search for view services and associate them with their parent services.
            view_items = target.content.search("NOT owner:esri* AND typekeywords:View Service", "Feature Layer",
                                               max_items=2000)
            process_views(view_items)

            # Update the portal instance with the latest service update timestamp.
            instance_item.service_updated = update_time
            instance_item.save()
            result.set_success()
            return result.to_json()

        except Exception as e:
            logger.exception(f"Unable to update services for {instance_alias}: {e}")
            result.add_error(f"Unable to update Services for {instance_alias}")
            # Get child tasks
            if batch_results and batch_results.children:
                for child in batch_results.children:
                    if child.id:
                        current_app.control.revoke(child.id, terminate=True, signal="SIGKILL")

            # Also revoke the current task
            # current_app.control.revoke(self.request.id, terminate=True, signal="SIGKILL")

            self.update_state(state="FAILURE", meta=result.to_json())
            raise Ignore()


@shared_task(bind=True, time_limit=6000, soft_time_limit=3000)
def process_batch_services(self, instance_alias, username, password, folder, update_time):
    result = UpdateResult()
    regex_patterns = compile_regex_patterns()
    service_usage_list = []
    instance_item = Portal.objects.get(alias=instance_alias)
    target = connect(instance_item, username, password)
    gis_servers = target.admin.servers.list()

    for server in gis_servers:
        for service in server.services.list(folder=folder):
            service_result = process_single_service(target, instance_item, service, folder, update_time, regex_patterns,
                                                    result)
            if service_result is not None:
                service_usage_list.append(service_result)
    result.set_success()
    return {"result": result.to_json(), "service_usage": service_usage_list}


def process_single_service(target, instance_item, service, folder, update_time, regex_patterns, result):
    try:
        logger.debug(service)
        portal_ids = {}  # Map portal item types to their IDs.
        service_usage_str = None
        access, owner, description = None, None, None

        # Construct the service name and corresponding URL based on folder structure.
        base_url = target.hosting_servers[0].url
        if not hasattr(service, "type"):
            result.add_error(f"Unable to determine service type for {service}")
            return service_usage_str
        service_type = service.properties.type
        if not hasattr(service, "serviceName"):
            result.add_error(f"Unable to determine service name for {service}")
            return service_usage_str
        service_name = service.properties.serviceName
        name = service_name if folder == "/" else f"{folder}/{service_name}"
        service_url = f"{base_url}/{name}/{service_type}"
        urls = [service_url]
        service_usage_str = f"services/{name}.{service_type}"

        # Attempt to fetch additional service details via an HTTP GET request.
        map_name = get_map_name(service_url, target._con.token)

        # Process portal items related to the service.
        try:
            portal_items = service.properties.portalProperties.portalItems
            # portal_items = service._json_dict.get("portalProperties", {}).get("portalItems", [])
            for item in portal_items:
                portal_ids[item.get("type")] = item.get("itemID")
                feature_url = f"{base_url}/{name}/FeatureServer"
                if item.get("type") == "FeatureServer" and feature_url not in urls:
                    urls.append(feature_url)
                owner, access, description = fetch_portal_item_details(target,
                                                                       item.get("itemID"),
                                                                       instance_item)
        except Exception as e:
            logger.exception(f"Unable to retrieve portal items for {name}: {e}")
            result.add_error(f"Error processing portal items for {name}")

        # Parse the service manifest to extract additional configuration.
        service_manifest = json.loads(service.service_manifest())
        service_data = {
            "service_url": ','.join(urls),
            "service_mxd_server": None,
            "service_mxd": map_name,
            "portal_id": portal_ids,
            "service_type": service_type,
            "service_description": description,
            "service_owner": owner,
            "service_access": access,
            "updated_date": update_time
        }
        # Service manifest not available for all hosted feature layers
        if service_manifest.get("code") == 500 and service_manifest.get(
            "status") == "error" and "FeatureServer" in portal_ids:
            result.add_error(f"{name}: No Service Manifest")
            s_obj, created = Service.objects.update_or_create(
                portal_instance=instance_item,
                service_name=name,
                defaults=service_data
            )
            result.add_insert() if created else result.add_update()

            # Process FeatureServer layers if available.
            feature_service_id = portal_ids["FeatureServer"]
            service_item = target.content.get(feature_service_id)
            if service_item.layers:
                for layer in service_item.layers:
                    obj, created = Layer.objects.update_or_create(
                        portal_instance=instance_item,
                        layer_server="Hosted",
                        layer_version=None,
                        layer_database=None,
                        layer_name=layer.properties.name,
                        defaults={"updated_date": update_time}
                    )
                    # Create or update the relationship between the layer and service.
                    obj, created = Layer_Service.objects.update_or_create(
                        portal_instance=instance_item,
                        layer_id=obj,
                        service_id=s_obj,
                        defaults={"updated_date": update_time}
                    )
        else:
            # Process service resources from the manifest if available.
            if "resources" in service_manifest.keys():
                res_obj = service_manifest["resources"]
                for obj in res_obj:
                    mxd = f"{obj['onPremisePath']}\\{map_name}" if map_name else f"{obj['onPremisePath']}"
                    mxd_server = obj["clientName"]
            else:
                mxd = None
                mxd_server = None
            service_data["service_mxd"] = mxd
            service_data["service_mxd_server"] = mxd_server
            s_obj, created = Service.objects.update_or_create(
                portal_instance=instance_item,
                service_name=name,
                defaults=service_data
            )
            result.add_insert() if created else result.add_update()

            # Process database details from the service manifest if available.
            process_databases(service_manifest, regex_patterns, instance_item, s_obj, update_time)

    except Exception as e:
        logger.exception(f"Unable to update {service.properties.serviceName}: {e}")
        result.add_error(f"Unable to update {service_name}")
    return service_usage_str


@shared_task(bind=True, name="Update apps", time_limit=6000, soft_time_limit=3000)
def update_webapps(self, instance_alias, overwrite=False, username=None, password=None):
    """
    Update web applications for the given portal instance.

    This task connects to the specified portal instance, retrieves various types of web applications
    (such as Web Mapping Applications, Dashboards, Web AppBuilder Apps, Experience Builder apps, Forms,
    and Story Maps), and synchronizes their data with the local database. For each application, it processes
    usage statistics, access levels, ownership, and related resources (like maps and services). It also
    manages relationships between applications and their corresponding maps or services, and cleans up
    outdated records to ensure data consistency.

    :param instance_item: The portal instance containing connection and configuration details.
    :type instance_item: PortalInstance
    :param overwrite: If True, delete existing application records before updating.
    :type overwrite: bool
    :param username: Username for portal authentication (optional).
    :type username: str, optional
    :param password: Password for portal authentication (optional).
    :type password: str, optional
    :return: JSON string summarizing the update results (inserts, updates, deletions, errors).
    :rtype: str
    """
    # Initialize the update result container and progress recorder.
    result = UpdateResult()
    progress_recorder = ProgressRecorder(self)

    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        # Attempt to connect to the portal instance using provided credentials.
        target = connect(instance_item, username, password)
    except Exception as e:
        # Log and return connection error.
        logger.exception(f"Unable to connect to {instance_alias}: {e}")
        result.add_error(f"Unable to connect to {instance_alias}")
        self.update_state(state="FAILURE", meta={result.to_json()})
        raise Ignore()

    try:
        update_time = datetime.datetime.now()  # Timestamp for this update cycle

        # Retrieve web application items by combining multiple searches for various app types.
        # items = [
        #     [target.content.search("NOT owner:esri*", "Web Mapping Application", max_items=2000)] +
        #     [target.content.search("NOT owner:esri*", "Dashboard", max_items=2000)] +
        #     [target.content.search("NOT owner:esri*", "Web AppBuilder Apps", max_items=2000)] +
        #     [target.content.search("NOT owner:esri*", "Experience Builder", max_items=2000)] +
        #     [target.content.search("NOT owner:esri*", "Form", max_items=2000)] +
        #     [target.content.search("NOT owner:esri*", "Story Map", max_items=2000)]
        # ]
        if instance_item.portal_type == "agol":
            org_id = instance_item.org_id
            total_apps = target.content.advanced_search(query=f"orgid:{org_id} AND NOT owner:esri*", max_items=-1, return_count=True,
                                                        filter='type:"Web Mapping Application" OR type:"Dashboard" OR type:"Web AppBuilder Apps" OR type:"Experience Builder" OR type:"Form" OR type:"Story Map"')
        else:
            total_apps = target.content.advanced_search(query="NOT owner:esri*", max_items=-1, return_count=True,
                                                    filter='type:"Web Mapping Application" OR type:"Dashboard" OR type:"Web AppBuilder Apps" OR type:"Experience Builder" OR type:"Form" OR type:"Story Map"')
        logger.info(f"Total apps: {total_apps}")
        # If overwrite flag is set to true, delete all existing web maps for the portal instance.
        if overwrite:
            App.objects.filter(portal_instance=instance_item).delete()

        batch_size = 20
        batch_tasks = []
        # Iterate through each retrieved app. Split into batches for parallel processing
        for batch in range(0, total_apps, batch_size):
            batch_tasks.append(process_batch_apps.s(instance_alias, username, password, batch, batch_size, update_time))

        task_group = group(batch_tasks)
        batch_results = task_group.apply_async()
        # batch_results.save()
        while not batch_results.ready():
            completed_tasks = sum(
                task.result.get("current", 0) if "current" in task.result else batch_size
                for task in batch_results.children
                if task.result
            )
            progress_recorder.set_progress(completed_tasks, total_apps)
            time.sleep(1.5)
        logger.info(f"Aggregating batch results: {batch_results}")
        for batch in batch_results.get(disable_sync_subtasks=False):
            batch_result = UpdateResult(**batch["result"])
            if batch_result.success is False:
                result.success = False
            result.num_updates += batch_result.num_updates
            result.num_inserts += batch_result.num_inserts
            result.num_deletes += batch_result.num_deletes
            result.num_errors += batch_result.num_errors
            result.error_messages.extend(batch_result.error_messages)

        # Remove outdated application records that were not updated in this run.
        delete_outdated_records(instance_item, update_time, [App, App_Map, App_Service], result)

        # Update the portal instance with the current webapp update timestamp.
        instance_item.webapp_updated = datetime.datetime.now()
        instance_item.save()

        result.set_success()
        return result.to_json()

    except Exception as e:
        logger.exception(f"Unable to update webapps for {instance_alias}: {e}")
        result.add_error(f"Unable to update Apps for {instance_alias}")
        # Get child tasks
        if batch_results and batch_results.children:
            for child in batch_results.children:
                if child.id:
                    current_app.control.revoke(child.id, terminate=True, signal="SIGKILL")

        # Also revoke the current task
        # current_app.control.revoke(self.request.id, terminate=True, signal="SIGKILL")

        self.update_state(state="FAILURE", meta=result.to_json())
        raise Ignore()


@shared_task(bind=True)
def process_batch_apps(self, instance_alias, username, password, batch, batch_size, update_time):
    result = UpdateResult()
    progress_recorder = ProgressRecorder(self)
    instance_item = Portal.objects.get(alias=instance_alias)
    target = connect(instance_item, username, password)
    if instance_item.portal_type == "agol":
        org_id = instance_item.org_id
        app_list = target.content.advanced_search(query=f"orgid:{org_id} AND NOT owner:esri*", max_items=batch_size, start=batch,
                                              sort_field="title", sort_order="asc",
                                              filter='type:"Web Mapping Application" OR type:"Dashboard" OR type:"Web AppBuilder Apps" OR type:"Experience Builder" OR type:"Form" OR type:"Story Map"')
    else:
        app_list = target.content.advanced_search(query="NOT owner:esri*", max_items=batch_size, start=batch,
                                                  sort_field="title", sort_order="asc",
                                                  filter='type:"Web Mapping Application" OR type:"Dashboard" OR type:"Web AppBuilder Apps" OR type:"Experience Builder" OR type:"Form" OR type:"Story Map"')
    total_apps = len(app_list.get("results"))
    for counter, app in enumerate(app_list.get("results")):
        try:
            process_single_app(app, target, instance_item, update_time, result)

            progress_recorder.set_progress(counter, total_apps)
        except Exception as e:
            # Log and record any errors encountered during processing of this web map.
            logger.exception(f"Unable to update {app.id}: {e}")
            result.add_error(f"Unable to update {app.id}")
    result.set_success()
    return {"result": result.to_json()}


def process_single_app(item, target, instance_item, update_time, result):
    try:
        logger.debug(item)  # Log the current item for debugging

        # Determine access level: if shared, format group names; otherwise use title case.
        access = get_access(item)

        # Process usage statistics based on portal type.
        usage = calculate_usage(item, instance_item)

        # Attempt to retrieve the owner of the item.
        owner = get_owner(instance_item, item.owner)

        app_data = {
            "app_title": item.title,
            "app_url": item.homepage,
            "app_type": item.type,
            "app_owner": owner,
            "app_created": epoch_to_datetime(item.created),
            "app_modified": epoch_to_datetime(item.modified),
            "app_access": access,
            "app_extent": item.extent,
            "app_description": item.description,
            "app_views": item.numViews,
            "app_usage": usage,
            "updated_date": update_time,
            "app_last_viewed": epoch_to_datetime(item.lastViewed) if instance_item.portal_type == "agol" else None
        }
        # Update or create the application record with the latest information.
        app_obj, created = App.objects.update_or_create(
            portal_instance=instance_item,
            app_id=item.id,
            defaults=app_data
        )
        # Record insert or update operation.
        result.add_insert() if created else result.add_update()

        # Retrieve detailed data from the item for further processing.
        data = item.get_data()
        logger.debug(item.type)

        # Process different app types and extract related resources.
        if item.type == "Web Mapping Application":
            logger.debug(data)
            # If the application has an associated map.
            if "map" in data:
                logger.debug("WAB has map")
                try:
                    map_id = data["map"]["itemId"]
                    map_obj = Webmap.objects.get(portal_instance=instance_item, webmap_id=map_id)
                    logger.debug(map_obj)
                    # Create or update the relationship between the app and its web map.
                    am_obj, created = App_Map.objects.update_or_create(
                        portal_instance=instance_item,
                        app_id=app_obj,
                        webmap_id=map_obj,
                        rel_type="map",
                        defaults={"updated_date": update_time}
                    )
                except Webmap.DoesNotExist:
                    result.add_error(f"{item.title}: map does not exist")
            # Process additional resources extracted from URLs.
            for resource in extract_webappbuilder(data):
                try:
                    if resource[2] == "url":
                        logger.debug(f"WAB has url {resource[0]}")
                        # Normalize URL by removing trailing numeric segments if present.
                        url = resource[0] if not resource[0].split("/")[-1].isdigit() else "/".join(
                            resource[0].split("/")[:-1])
                        try:
                            service_obj = Service.objects.get(service_url__iregex=rf'^{url}$')
                        except Service.DoesNotExist:
                            continue
                        except MultipleObjectsReturned:
                            service_obj = Service.objects.filter(service_url__iregex=rf'^{url}').first()
                        logger.debug(service_obj)
                        # Determine relationship type based on resource context.
                        rel_type = lambda t: "search" if "searchLayers" in resource[
                            1] else "filter" if "filters" in resource[1] else "widget" if "widgets" in resource[
                            1] else "other"
                        t = rel_type(resource[1])
                        # Update or create the relationship between the app and the service.
                        as_obj, created = App_Service.objects.update_or_create(
                            portal_instance=instance_item,
                            app_id=app_obj,
                            service_id=service_obj,
                            rel_type=t,
                            defaults={"updated_date": update_time}
                        )
                except Exception:
                    pass

        if item.type == "StoryMap":
            # Process StoryMap resources to link with web maps.
            for resource in extract_storymap(data["resources"]):
                try:
                    if resource[2] == "Web Map":
                        logger.debug(f"StoryMap has map {resource[4]}")
                        map_obj = Webmap.objects.get(webmap_id=resource[4])
                        logger.debug(map_obj)
                        am_obj, created = App_Map.objects.update_or_create(
                            app_id=app_obj,
                            webmap_id=map_obj,
                            rel_type="map",
                            defaults={"item_updated": update_time}
                        )
                except Webmap.DoesNotExist:
                    result.add_error(f"{item.title}: map does not exist")

        if item.type == "Dashboard":
            # Process Dashboard resources to link with services or web maps.
            for resource in extract_dashboard(data):
                try:
                    dashboard_id = target.content.get(resource[2])
                    logger.debug(f"Dashboard has resource {resource}")
                    if dashboard_id.type in ["Map Image Layer", "Feature Layer"]:
                        try:
                            service_obj = Service.objects.get(service_url__iregex=rf"^{dashboard_id.url}$")
                        except Service.DoesNotExist:
                            continue
                        except MultipleObjectsReturned:
                            service_obj = Service.objects.filter(service_url__iregex=rf"^{dashboard_id.url}").first()
                        logger.debug(service_obj)
                        sa_obj, created = App_Service.objects.update_or_create(
                            portal_instance=instance_item,
                            app_id=app_obj,
                            service_id=service_obj,
                            rel_type="other",
                            defaults={"updated_date": update_time}
                        )
                    if dashboard_id.type == "Web Map":
                        map_obj = Webmap.objects.get(webmap_id=dashboard_id.id)
                        logger.debug(map_obj)
                        am_obj, created = App_Map.objects.update_or_create(
                            portal_instance=instance_item,
                            app_id=app_obj,
                            webmap_id=map_obj,
                            rel_type="map",
                            defaults={"updated_date": update_time}
                        )
                except:
                    pass

        if item.type == "Form":
            # For Form items, process related Survey2Service items.
            for resource in item.related_items("Survey2Service", "forward"):
                try:
                    logger.debug(f"Form has service {resource}")
                    try:
                        service_obj = Service.objects.get(service_url__iregex=rf"^{resource.url}$")
                    except Service.DoesNotExist:
                        continue
                    except MultipleObjectsReturned:
                        service_obj = Service.objects.filter(service_url__iregex=rf"^{resource.url}").first()
                    logger.debug(service_obj)
                    sa_obj, created = App_Service.objects.update_or_create(
                        portal_instance=instance_item,
                        app_id=app_obj,
                        service_id=service_obj,
                        defaults={"updated_date": update_time}
                    )
                except:
                    pass

        if item.type == "Web Experience":
            # Process Experience Builder resources based on type.
            for resource in extract_experiencebuilder(data):
                try:
                    logger.debug(f"ExB has resource {resource[3]}")
                    if resource[3] == "WEB_MAP":
                        map_obj = Webmap.objects.get(webmap_id=resource[2])
                        logger.debug(map_obj)
                        am_obj, created = App_Map.objects.update_or_create(
                            portal_instance=instance_item,
                            webmap_id=map_obj,
                            app_id=app_obj,
                            rel_type="map",
                            defaults={"updated_date": update_time}
                        )
                    if resource[3] == "FEATURE_LAYER":
                        try:
                            service_obj = Service.objects.get(service_url__iregex=rf"^{resource[2]}$")
                        except Service.DoesNotExist:
                            continue
                        except MultipleObjectsReturned:
                            service_obj = Service.objects.filter(service_url__iregex=rf"^{resource[2]}").first()
                        logger.debug(service_obj)
                        as_obj, created = App_Service.objects.update_or_create(
                            portal_instance=instance_item,
                            service_id=service_obj,
                            app_id=app_obj,
                            defaults={"updated_date": update_time}
                        )
                    if resource[3] == "GEOCODING":
                        try:
                            service_obj = Service.objects.get(service_url__iregex=rf"^{resource[2]}$")
                        except Service.DoesNotExist:
                            continue
                        except MultipleObjectsReturned:
                            service_obj = Service.objects.filter(service_url__iregex=rf"^{resource[2]}").first()
                        logger.debug(service_obj)
                        as_obj, created = App_Service.objects.update_or_create(
                            portal_instance=instance_item,
                            service_id=service_obj,
                            app_id=app_obj,
                            defaults={"updated_date": update_time}
                        )
                except:
                    pass
        # Update the progress recorder after processing each item.
        # progress_recorder.set_progress(counter, total_apps)

    except Exception as e:
        logger.exception(f"Unable to update {item.id}: {e}")
        result.add_error(f"Unable to update {item.title}")


@shared_task(bind=True, name="Update users", time_limit=6000, soft_time_limit=3000)
def update_users(self, instance_alias, overwrite=False, username=None, password=None):
    """
    Update user records for the specified portal instance.

    This task connects to the portal instance and synchronizes the user information
    with the local database. Retrieves user data via the portal's API, processes user details including
    role, license type, and activity timestamps, and then updates or creates the corresponding
    database records.

    Additionally, the function retrieves ArcGIS Pro license usage information from the portal's
    administration interface and updates the user's license attributes (e.g., license type and
    last used date). Outdated user records are removed at the end of the process.

    :param instance_alias: The portal instance alias.
    :type instance_alias: str
    :param overwrite: If True, delete existing user records before updating.
    :type overwrite: bool
    :param username: Username for portal authentication (optional).
    :type username: str, optional
    :param password: Password for portal authentication (optional).
    :type password: str, optional
    :return: A JSON string summarizing the update results, including counts of inserts, updates,
             deletions, and any error messages.
    :rtype: str
    """
    # Initialize the result container and progress tracker for the task.
    result = UpdateResult()
    progress_recorder = ProgressRecorder(self)
    update_time = datetime.datetime.now()  # Mark the update timestamp for the current run

    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        # Attempt to establish a connection to the portal instance.
        target = connect(instance_item, username, password)
    except Exception as e:
        # If connection fails, record the error and return the result.
        logger.exception(f"Unable to connect to {instance_alias}: {e}")
        result.add_error(f"Unable to connect to {instance_alias}")
        self.update_state(state="FAILURE", meta={result.to_json()})
        raise Ignore()

    try:
        # If overwrite flag is set, delete existing user records for this portal instance.
        if overwrite:
            User.objects.filter(portal_instance=instance_item).delete()

        roles = dict([(role.role_id, role.name) for role in target.users.roles.all()])
        # Search and retrieve a list of users from the portal. Query is required and no good query exists for all
        total_users = target.users.advanced_search(query="!_esri", max_users=-1, return_count=True)
        logger.info(f"Total: {total_users}")

        batch_size = 100
        batch_tasks = []
        # Iterate through each retrieved web map.
        for batch in range(0, total_users, batch_size):
            batch_tasks.append(
                process_batch_users.s(instance_alias, username, password, batch, batch_size, roles, update_time))

        # Parallel processing by folder
        task_group = group(batch_tasks)
        batch_results = task_group.apply_async()
        # batch_results.save()
        while not batch_results.ready():
            completed_tasks = sum(
                task.result.get("current", 0) for task in batch_results.children if task.result is not None)
            progress_recorder.set_progress(completed_tasks, total_users)
            time.sleep(1.5)
        logger.info(f"Aggregating batch results: {batch_results}")
        for batch in batch_results.get(disable_sync_subtasks=False):
            batch_result = UpdateResult(**batch["result"])
            if batch_result.success is False:
                result.success = False
            result.num_updates += batch_result.num_updates
            result.num_inserts += batch_result.num_inserts
            result.num_deletes += batch_result.num_deletes
            result.num_errors += batch_result.num_errors
            result.error_messages.extend(batch_result.error_messages)

        # Retrieve ArcGIS Pro license information from the portal's admin interface.
        licenses = target.admin.license.get("ArcGIS Pro")
        if licenses:
            # Extract license usage details from the report for different license types.
            desktopAdvN = licenses.report.to_numpy()[2][4]
            desktopBasicN = licenses.report.to_numpy()[3][4]
            desktopStdN = licenses.report.to_numpy()[4][4]
            # Update user records with Advanced license information.
            for user_license in desktopAdvN:
                user = user_license["user"]
                last_used = user_license["lastUsed"]
                if last_used:
                    last_used = datetime.datetime.strptime(last_used, "%B %d, %Y").date()
                try:
                    update_entry = User.objects.get(portal_instance=instance_item, user_username__exact=user)
                    update_entry.user_pro_license = "desktopAdvN"
                    update_entry.user_pro_last = last_used
                    update_entry.save()
                except Exception as e:
                    result.add_error(f"Error with licenses: {e}")
            # Update user records with Basic license information.
            for user_license in desktopBasicN:
                user = user_license["user"]
                last_used = user_license["lastUsed"]
                if last_used:
                    last_used = datetime.datetime.strptime(last_used, "%B %d, %Y").date()
                try:
                    update_entry = User.objects.get(portal_instance=instance_item, user_username__exact=user)
                    update_entry.user_pro_license = "desktopBasicN"
                    update_entry.user_pro_last = last_used
                    update_entry.save()
                except Exception as e:
                    result.add_error(f"Error with licenses: {e}")
            # Update user records with Standard license information.
            for user_license in desktopStdN:
                user = user_license["user"]
                last_used = user_license["lastUsed"]
                if last_used:
                    last_used = datetime.datetime.strptime(last_used, "%B %d, %Y").date()
                try:
                    update_entry = User.objects.get(portal_instance=instance_item, user_username__exact=user)
                    update_entry.user_pro_license = "desktopStdN"
                    update_entry.user_pro_last = last_used
                    update_entry.save()
                except Exception as e:
                    result.add_error(f"Error with licenses: {e}")

        # Identify and delete outdated user records.
        delete_outdated_records(instance_item, update_time, [User], result)

        # Update the portal instance's last user update timestamp.
        instance_item.user_updated = datetime.datetime.now()
        instance_item.save()

        result.set_success()
        return result.to_json()

    except Exception as e:
        # Capture and log any exceptions during the update process, then return the result.
        logger.exception(f"Unable to update users for {instance_alias}: {e}")
        result.add_error(f"Unable to update users for {instance_alias}")
        # Get child tasks
        if batch_results and batch_results.children:
            for child in batch_results.children:
                if child.id:
                    current_app.control.revoke(child.id, terminate=True, signal="SIGKILL")

        # Also revoke the current task
        current_app.control.revoke(self.request.id, terminate=True, signal="SIGKILL")

        self.update_state(state="FAILURE", meta=result.to_json())
        raise Ignore()


@shared_task(bind=True)
def process_batch_users(self, instance_alias, username, password, batch, batch_size, roles, update_time):
    result = UpdateResult()
    progress_recorder = ProgressRecorder(self)
    instance_item = Portal.objects.get(alias=instance_alias)
    target = connect(instance_item, username, password)
    user_list = target.users.advanced_search(query="!_esri", max_users=batch_size,
                                             start=batch, sort_field="username", sort_order="asc")
    for counter, user in enumerate(user_list.get("results")):
        try:
            if not (user.get("userType", None) and user.get("role", None)):
                # Likely a system user. Can't exclude in advanced_search
                continue
            items = target.content.advanced_search(query=f"owner:{user.username}", max_items=-1, return_count=True)
            obj, created = User.objects.update_or_create(
                portal_instance=instance_item,
                user_username=user.username,
                defaults={
                    "user_first_name": user.fullName.split(" ")[0] if user.get("firstName",
                                                                               None) is None else user.firstName,
                    "user_last_name": user.fullName.split(" ")[-1] if user.get("lastName",
                                                                               None) is None else user.lastName,
                    "user_email": user.email,
                    "user_created": epoch_to_datetime(user.created),
                    "user_last_login": epoch_to_datetime(user.lastLogin),
                    "user_role": roles.get(user.roleId, user.roleId),
                    "user_level": user.userLicenseTypeId,
                    "user_disabled": user.disabled,
                    "user_provider": user.provider,
                    "user_items": items,
                    "updated_date": update_time
                }
            )
            result.add_insert() if created else result.add_update()
            progress_recorder.set_progress(counter, len(user_list.get("results")))
        except Exception as e:
            # Log and record any errors encountered during processing of this web map.
            logger.exception(f"Unable to update user {user.username}: {e}")
            result.add_error(f"Unable to update user {user.username}")
    result.set_success()
    return {"result": result.to_json()}


# TODO create logging model
@shared_task(bind=True)
def process_user(self, instance_alias, username, operation):
    # Operations: delete, update, add(?)
    # Event triggers: add, delete, disable, enable, updateUserRole, updateUserLicenseType
    logger.debug("updating users")
    result = UpdateResult()
    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        # Connect to the target portal using provided credentials.
        target = connect(instance_item)
    except Exception as e:
        # Record connection error and return error result.
        logger.exception(f"Unable to connect to {instance_alias}: {e}")
        result.add_error(f"Unable to connect to {instance_alias}")
        self.update_state(state="FAILURE", meta={result.to_json()})
        raise Ignore()
    if operation == 'delete':
        try:
            deletes = User.objects.filter(portal_instance=instance_item, user_username=username)
            result.add_delete(len(deletes))
            deletes.delete()

            instance_item.user_updated = datetime.datetime.now()
            instance_item.save()
            result.set_success()
            return result.to_json()
        except Exception as e:
            logger.exception(f"Unable to delete user {username} for {instance_alias}: {e}")
            result.add_error(f"Unable to delete user {username} for {instance_alias}")
            self.update_state(state="FAILURE", meta={result.to_json()})
            raise Ignore()

    else:
        try:
            roles = dict([(role.role_id, role.name) for role in target.users.roles.all()])
            user = target.users.get(username)

            user_license = target.admin.license.get("ArcGIS Pro").user_entitlement(username)
            target_entitlements = ['desktopAdvN', 'desktopStdN', 'desktopBasicN']
            pro_license = next((e for e in target_entitlements if e in user_license['entitlements']), None)

            items = target.content.search(query=f'owner:{user.username}', max_items=10000)
            obj, created = User.objects.update_or_create(portal_instance=instance_item, user_username=user.username,
                                                         defaults={
                                                             'user_first_name': user.fullName.split(" ")[
                                                                 0] if user.firstName is None else user.firstName,
                                                             'user_last_name': user.fullName.split(" ")[
                                                                 -1] if user.lastName is None else user.lastName,
                                                             'user_email': user.email,
                                                             'user_created': epoch_to_datetime(user.created),
                                                             'user_last_login': epoch_to_datetime(user.lastLogin),
                                                             'user_role': roles.get(user.roleId, user.roleId),
                                                             'user_level': user.userLicenseTypeId,
                                                             'user_disabled': user.disabled,
                                                             'user_provider': user.provider,
                                                             'user_items': len(items),
                                                             'user_pro_license': pro_license,
                                                             'user_pro_last': user_license.get('lastLogin', None),
                                                         })
            result.add_insert() if created else result.add_update()

            instance_item.user_updated = datetime.datetime.now()
            instance_item.save()
            result.set_success()
            return result.to_json()
        except Exception as e:
            logger.exception(f"Unable to update user {username} for {instance_alias}: {e}")
            result.add_error(f"Unable to update user {username} for {instance_alias}")
            self.update_state(state="FAILURE", meta={result.to_json()})
            raise Ignore()


@shared_task(bind=True)
def process_webmap(self, instance_alias, item_id, operation):
    # Operation: add, delete, publish, share, unshare, update
    logger.debug("updating webmap")
    result = UpdateResult()
    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        # Connect to the target portal using provided credentials.
        target = connect(instance_item)
    except Exception as e:
        # Record connection error and return error result.
        logger.exception(f"Unable to connect to {instance_alias}: {e}")
        result.add_error(f"Unable to connect to {instance_alias}")
        self.update_state(state="FAILURE", meta={result.to_json()})
        raise Ignore()
    if operation == 'delete':
        try:
            deletes = Webmap.objects.filter(portal_instance=instance_item, webmap_id=item_id)
            result.add_delete(len(deletes))
            deletes.delete()

            instance_item.webmap_updated = datetime.datetime.now()
            instance_item.save()
            result.set_success()
            return result.to_json()
        except Exception as e:
            logger.exception(f"Unable to delete webmap {item_id} for {instance_alias}: {e}")
            result.add_error(f"Unable to delete webmap {item_id} for {instance_alias}")
            self.update_state(state="FAILURE", meta={result.to_json()})
            raise Ignore()
    else:
        wm_item = target.content.get(item_id)
        try:
            logger.debug(wm_item)
            webmap_data = extract_webmap_data(wm_item, instance_item)
            obj, created = Webmap.objects.update_or_create(
                portal_instance=instance_item, webmap_id=wm_item.id, defaults=webmap_data
            )
            result.add_insert() if created else result.add_update()

            link_services_to_webmap(instance_item, obj, webmap_data["webmap_services"])

            # Update the portal instance's last updated timestamp.
            instance_item.webmap_updated = datetime.datetime.now()
            instance_item.save()

            # Mark the overall update result as successful.
            result.set_success()
            return result.to_json()

        except Exception as e:
            # Log any unexpected exceptions during the update process and record the error.
            logger.exception(f"Unable to update webmap {item_id} for {instance_alias}: {e}")
            result.add_error(f"Unable to update webmap {item_id} for {instance_alias}")
            self.update_state(state="FAILURE", meta={result.to_json()})
            raise Ignore()


@shared_task(bind=True)
def process_service(self, instance_alias, item, operation):
    return
    import re
    # Operation: add, delete, publish, share, unshare, update
    logger.debug("updating service")
    result = UpdateResult()
    update_time = datetime.datetime.now()
    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        # Connect to the target portal using provided credentials.
        target = connect(instance_item)
    except Exception as e:
        # Record connection error and return error result.
        logger.exception(f"Unable to connect to {instance_alias}: {e}")
        result.add_error(f"Unable to connect to {instance_alias}")
        self.update_state(state="FAILURE", meta={result.to_json()})
        raise Ignore()
    service = arcgis.gis.server.Service(item, target)

    if service is None:
        result.add_error(f"Unable to create service {item}. Service not found.")
    if operation == 'delete':
        try:
            deletes = Service.objects.filter(portal_instance=instance_item, service_url__icontains=service.url.url)
            result.add_delete(len(deletes))
            deletes.delete()

            instance_item.webmap_updated = datetime.datetime.now()
            instance_item.save()
            result.set_success()
            return result.to_json()
        except Exception as e:
            logger.exception(f"Unable to delete service {item} for {instance_alias}: {e}")
            result.add_error(f"Unable to delete service {item} for {instance_item}")
            self.update_state(state="FAILURE", meta={result.to_json()})
            raise Ignore()
    else:
        try:
            match = re.search(r"/services(?:/([^/]+))?/([^/]+)/[^/]+(?:/\d+)?$", service.url.url)
            if match:
                folder_name = match.group(1)
                if folder_name is None:
                    folder_name = ""
                service_name = match.group(2)
            else:
                result.add_error(f"Unable to determine service folder and name {item}")
                return result.to_json()
            service_admin = target.admin.servers.list()[0].services.get(service_name, folder_name)
            services_list = []
            regexp_server = re.compile("(?<=SERVER=)([^;]*)")
            regexp_version = re.compile("(?<=VERSION=)([^;]*)")
            regexp_branch = re.compile("(?<=BRANCH=)([^;]*)")
            regexp_database = re.compile("(?<=DATABASE=)([^;]*)")
            url = []
            portal_ids = {}
            service_layers = {}
            owner, access, description = None, None, None
            mxd_map = service.properties['mapName']
            service_type = service_admin.properties["type"]
            service.url
            if folder_name == '':
                name = "{}".format(service_admin.properties["serviceName"])
                url.append(target.hosting_servers[0].url + "/{}/{}".format(name, service_type))
            else:
                name = "{}/{}".format(folder_name, service_admin.properties["serviceName"])
                url.append(target.hosting_servers[0].url + "/{}/{}".format(name, service_type))
            services_list.append(f"services/{name}.{service_type}")

            try:
                portal_items = service_admin._json_dict['portalProperties']['portalItems']
                for item in portal_items:
                    portal_ids[item['type']] = item['itemID']
                    if item['type'] == "FeatureServer" and not target.hosting_servers[
                                                                   0].url + "/{}/{}".format(name,
                                                                                            "FeatureServer") in url:
                        url.append(
                            target.hosting_servers[0].url + "/{}/{}".format(name, "FeatureServer"))
                    portal_item = target.content.get(item['itemID'])
                    if portal_item:
                        try:
                            owner = User.objects.get(portal_instance=instance_item,
                                                     user_username=portal_item.owner)
                        except User.DoesNotExist:
                            owner = None
                        if portal_item.access == 'shared':  # or used .shared_with https://developers.arcgis.com/python/api-reference/arcgis.gis.toc.html#arcgis.gis.Item.shared_with
                            access = "Groups: " + ", ".join(
                                x.title for x in portal_item.shared_with['groups'])
                        else:
                            access = portal_item.access.title()
                        soup = BeautifulSoup(portal_item.description, 'html.parser')
                        description = soup.get_text()
                    else:
                        owner = None
            except Exception as e:
                logger.exception(f"Unable to get portal items for {item}: {e}")
                result.add_error(f"No portal item for {item}")
            sm = json.loads(service_admin.service_manifest())
            if sm.get('code') == 500 and sm.get(
                'status') == 'error' and 'FeatureServer' in portal_ids.keys():
                result.add_error(f"Unable to update {name}: No Service Manifest")
                s_obj, created = Service.objects.update_or_create(portal_instance=instance_item,
                                                                  service_name=name,
                                                                  defaults={
                                                                      'service_url': ','.join(url),
                                                                      'service_mxd_server': None,
                                                                      'service_mxd': None,
                                                                      'portal_id': portal_ids,
                                                                      'service_type': service_type,
                                                                      'service_description': description,
                                                                      'service_owner': owner,
                                                                      'service_access': access,
                                                                      'updated_date': update_time})
                if created:
                    result.add_insert()
                else:
                    result.add_update()
                id = portal_ids['FeatureServer']
                l = target.content.get(id)
                if l.layers:
                    for layer in l.layers:
                        obj, created = Layer.objects.update_or_create(portal_instance=instance_item,
                                                                      layer_server='Hosted',
                                                                      layer_version=None,
                                                                      layer_database=None,
                                                                      layer_name=layer.properties.name,
                                                                      defaults={
                                                                          'updated_date': update_time})
                        # TODO track layer CRUD
                        obj, created = Layer_Service.objects.update_or_create(
                            portal_instance=instance_item,
                            layer_id=obj, service_id=s_obj,
                            defaults={'updated_date': update_time})
            else:
                if 'resources' in sm.keys():
                    res_obj = sm["resources"]
                    for obj in res_obj:
                        mxd = f"{obj['onPremisePath']}/{mxd_map}"
                        mxd_server = obj["clientName"]
                else:
                    mxd = None
                    mxd_server = None

                s_obj, created = Service.objects.update_or_create(portal_instance=instance_item,
                                                                  service_name=name,
                                                                  defaults={'service_url': ','.join(url),
                                                                            'service_mxd_server': mxd_server,
                                                                            'service_mxd': mxd,
                                                                            'portal_id': portal_ids,
                                                                            'service_type': service_type,
                                                                            'service_description': description,
                                                                            'service_owner': owner,
                                                                            'service_access': access,
                                                                            'updated_date': update_time})
                if created:
                    result.add_insert()
                else:
                    result.add_update()

                if 'databases' in sm.keys():
                    db_obj = sm["databases"]
                    for obj in db_obj:
                        db_server = ""
                        db_version = ""
                        db_database = ""
                        if "Sde" in obj["onServerWorkspaceFactoryProgID"]:
                            db_server = str(
                                regexp_server.search(obj["onServerConnectionString"]).group(0)).upper()
                            try:
                                db_version = str(
                                    regexp_version.search(obj["onServerConnectionString"]).group(
                                        0)).upper()
                            except:
                                try:
                                    db_version = str(
                                        regexp_branch.search(obj["onServerConnectionString"]).group(
                                            0)).upper()
                                except:
                                    db_version = ""
                            db_database = str(
                                regexp_database.search(obj["onServerConnectionString"]).group(
                                    0)).upper()
                            db = "{}@{}@{}".format(db_server, db_database, db_version)
                        elif "FileGDB" in obj["onServerWorkspaceFactoryProgID"]:
                            db_database = obj["onServerConnectionString"].split("DATABASE=")[1].replace(
                                '\\\\',
                                '\\')
                            db = db_database
                        datasets = obj["datasets"]
                        for dataset in datasets:
                            service_layers[dataset["onServerName"]] = db
                            if db_database:
                                obj, created = Layer.objects.update_or_create(portal_instance=instance_item,
                                                                              layer_server=db_server,
                                                                              layer_version=db_version,
                                                                              layer_database=db_database,
                                                                              layer_name=dataset[
                                                                                  "onServerName"],
                                                                              defaults={
                                                                                  'updated_date': update_time})
                                # TODO track layer CRUD
                                obj, created = Layer_Service.objects.update_or_create(
                                    portal_instance=instance_item,
                                    layer_id=obj, service_id=s_obj,
                                    defaults={'updated_date': update_time})

            # Search all views, then associate with the parent service
            service_views = service.related_items(rel_type="Service2Service", direction="reverse")
            for parent in service_views:
                try:
                    view_obj = Service.objects.get(portal_instance=instance_item,
                                                   service_url__iregex=rf'^{service.url}$')
                except Service.DoesNotExist:
                    continue
                except MultipleObjectsReturned:
                    view_obj = Service.objects.filter(portal_instance=instance_item,
                                                      service_url__iregex=rf'{service.url}').first()
                try:
                    service_obj = Service.objects.get(portal_instance=instance_item,
                                                      service_url__iregex=rf'^{parent.url}$')
                except Service.DoesNotExist:
                    continue
                except MultipleObjectsReturned:
                    service_obj = Service.objects.filter(portal_instance=instance_item,
                                                         service_url__iregex=rf'{parent.url}').first()
                logger.debug(f"{service_obj} related to {view_obj}")

                view_obj.service_view = service_obj
                service_obj.service_view = view_obj
                view_obj.save()
                service_obj.save()
            instance_item.service_updated = update_time
            instance_item.save()
            result.set_success()
            return result.to_json()
        except Exception as e:
            logger.exception(f"Unable to update service {item} for {instance_alias}: {e}")
            result.add_error(f"Unable to update service {item} for {instance_alias}")
            self.update_state(state="FAILURE", meta={result.to_json()})
            raise Ignore()


@shared_task(bind=True)
def process_webapp(self, instance_alias, item_id, operation):
    # Operation: add, delete, publish, share, unshare, update
    logger.debug("updating app")
    result = UpdateResult()
    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        # Connect to the target portal using provided credentials.
        target = connect(instance_item)
    except Exception as e:
        # Record connection error and return error result.
        logger.exception(f"Unable to connect to {instance_alias}: {e}")
        result.add_error(f"Unable to connect to {instance_alias}")
        self.update_state(state="FAILURE", meta={result.to_json()})
        raise Ignore()
    if operation == 'delete':
        try:
            deletes = App.objects.filter(portal_instance=instance_item, app_id=item_id)
            result.add_delete(len(deletes))
            deletes.delete()

            instance_item.webapp_updated = datetime.datetime.now()
            instance_item.save()
            result.set_success()
            return result.to_json()
        except Exception as e:
            logger.exception(f"Unable to delete app {item_id} for {instance_alias}: {e}")
            result.add_error(f"Unable to delete app {item_id}")
            self.update_state(state="FAILURE", meta={result.to_json()})
            raise Ignore()
    else:
        try:
            item = target.content.get(item_id)
            update_time = datetime.datetime.now()
            logger.debug(item)  # Log the current item for debugging

            # Determine access level: if shared, format group names; otherwise use title case.
            access = get_access(item)
            # Process usage statistics based on portal type.
            usage = calculate_usage(item, instance_item)
            # Attempt to retrieve the owner of the item.
            owner = get_owner(instance_item, item.owner)

            app_data = {
                "app_title": item.title,
                "app_url": item.homepage,
                "app_type": item.type,
                "app_owner": owner,
                "app_created": epoch_to_datetime(item.created),
                "app_modified": epoch_to_datetime(item.modified),
                "app_access": access,
                "app_extent": item.extent,
                "app_description": item.description,
                "app_views": item.numViews,
                "app_usage": usage,
                "updated_date": update_time,
                "app_last_viewed": epoch_to_datetime(
                    item.lastViewed) if instance_item.portal_type == 'agol' else None
            }
            # Update or create the application record with the latest information.
            app_obj, created = App.objects.update_or_create(
                portal_instance=instance_item,
                app_id=item.id,
                defaults=app_data
            )
            # Record insert or update operation.
            result.add_insert() if created else result.add_update()

            # Retrieve detailed data from the item for further processing.
            data = item.get_data()
            logger.debug(item.type)

            # Process different app types and extract related resources.
            if item.type == 'Web Mapping Application':
                logger.debug(data)
                # If the application has an associated map.
                if 'map' in data:
                    logger.debug("WAB has map")
                    try:
                        map_id = data['map']['itemId']
                        map_obj = Webmap.objects.get(portal_instance=instance_item, webmap_id=map_id)
                        logger.debug(map_obj)
                        # Create or update the relationship between the app and its web map.
                        am_obj, created = App_Map.objects.update_or_create(
                            portal_instance=instance_item,
                            app_id=app_obj,
                            webmap_id=map_obj,
                            rel_type='map',
                            defaults={'updated_date': update_time}
                        )
                    except Webmap.DoesNotExist:
                        result.add_error(f"{item.title}: map does not exist")
                # Process additional resources extracted from URLs.
                for resource in extract_webappbuilder(data):
                    try:
                        if resource[2] == 'url':
                            logger.debug(f"WAB has url {resource[0]}")
                            # Normalize URL by removing trailing numeric segments if present.
                            url = resource[0] if not resource[0].split("/")[-1].isdigit() else "/".join(
                                resource[0].split("/")[:-1])
                            try:
                                service_obj = Service.objects.get(service_url__iregex=rf'^{url}$')
                            except Service.DoesNotExist:
                                continue
                            except MultipleObjectsReturned:
                                service_obj = Service.objects.filter(service_url__iregex=rf'{url}').first()
                            logger.debug(service_obj)
                            # Determine relationship type based on resource context.
                            rel_type = lambda t: 'search' if 'searchLayers' in resource[
                                1] else 'filter' if 'filters' in resource[1] else 'widget' if 'widgets' in resource[
                                1] else 'other'
                            t = rel_type(resource[1])
                            # Update or create the relationship between the app and the service.
                            as_obj, created = App_Service.objects.update_or_create(
                                portal_instance=instance_item,
                                app_id=app_obj,
                                service_id=service_obj,
                                rel_type=t,
                                defaults={'updated_date': update_time}
                            )
                    except Exception:
                        pass

            if item.type == 'StoryMap':
                # Process StoryMap resources to link with web maps.
                for resource in extract_storymap(data['resources']):
                    try:
                        if resource[2] == 'Web Map':
                            logger.debug(f"StoryMap has map {resource[4]}")
                            map_obj = Webmap.objects.get(webmap_id=resource[4])
                            logger.debug(map_obj)
                            am_obj, created = App_Map.objects.update_or_create(
                                app_id=app_obj,
                                webmap_id=map_obj,
                                rel_type='map',
                                defaults={"updated_date": update_time}
                            )
                    except Webmap.DoesNotExist:
                        result.add_error(f"{item.title}: map does not exist")

            if item.type == 'Dashboard':
                # Process Dashboard resources to link with services or web maps.
                for resource in extract_dashboard(data):
                    try:
                        dashboard_id = target.content.get(resource[2])
                        logger.debug(f"Dashboard has resource {resource}")
                        if dashboard_id.type in ['Map Image Layer', 'Feature Layer']:

                            try:
                                service_obj = Service.objects.get(service_url__iregex=rf'^{dashboard_id.url}$')
                            except Service.DoesNotExist:
                                continue
                            except MultipleObjectsReturned:
                                service_obj = Service.objects.filter(
                                    service_url__iregex=rf'^{dashboard_id.url}').first()
                            logger.debug(service_obj)
                            sa_obj, created = App_Service.objects.update_or_create(
                                portal_instance=instance_item,
                                app_id=app_obj,
                                service_id=service_obj,
                                rel_type='other',
                                defaults={'updated_date': update_time}
                            )
                        if dashboard_id.type == 'Web Map':
                            map_obj = Webmap.objects.get(webmap_id=dashboard_id.id)
                            logger.debug(map_obj)
                            am_obj, created = App_Map.objects.update_or_create(
                                portal_instance=instance_item,
                                app_id=app_obj,
                                webmap_id=map_obj,
                                rel_type='map',
                                defaults={'updated_date': update_time}
                            )
                    except:
                        pass

            if item.type == 'Form':
                # For Form items, process related Survey2Service items.
                for resource in item.related_items('Survey2Service', 'forward'):
                    try:
                        logger.debug(f"Form has service {resource}")
                        try:
                            service_obj = Service.objects.get(service_url__iregex=rf'^{resource.url}$')
                        except Service.DoesNotExist:
                            continue
                        except MultipleObjectsReturned:
                            service_obj = Service.objects.filter(service_url__iregex=rf'{resource.url}').first()
                        logger.debug(service_obj)
                        sa_obj, created = App_Service.objects.update_or_create(
                            portal_instance=instance_item,
                            app_id=app_obj,
                            service_id=service_obj,
                            rel_type='survey',
                            defaults={"updated_date": update_time}
                        )
                    except:
                        pass

            if item.type == 'Web Experience':
                # Process Experience Builder resources based on type.
                for resource in extract_experiencebuilder(data):
                    try:
                        logger.debug(f"ExB has resource {resource[3]}")
                        if resource[3] == 'WEB_MAP':
                            map_obj = Webmap.objects.get(webmap_id=resource[2])
                            logger.debug(map_obj)
                            am_obj, created = App_Map.objects.update_or_create(
                                portal_instance=instance_item,
                                webmap_id=map_obj,
                                app_id=app_obj,
                                rel_type='map',
                                defaults={'updated_date': update_time}
                            )
                        if resource[3] == 'FEATURE_LAYER':
                            try:
                                service_obj = Service.objects.get(service_url__iregex=rf'^{resource[2]}$')
                            except Service.DoesNotExist:
                                continue
                            except MultipleObjectsReturned:
                                service_obj = Service.objects.filter(service_url__iregex=rf'{resource[2]}').first()
                            logger.debug(service_obj)
                            as_obj, created = App_Service.objects.update_or_create(
                                portal_instance=instance_item,
                                service_id=service_obj,
                                app_id=app_obj,
                                rel_type='other',
                                defaults={'updated_date': update_time}
                            )
                        if resource[3] == 'GEOCODING':
                            try:
                                service_obj = Service.objects.get(service_url__iregex=rf'^{resource[2]}$')
                            except Service.DoesNotExist:
                                continue
                            except MultipleObjectsReturned:
                                service_obj = Service.objects.filter(service_url__iregex=rf'{resource[2]}').first()
                            service_obj = Service.objects.get(service_url__icontains=resource[2])
                            logger.debug(service_obj)
                            as_obj, created = App_Service.objects.update_or_create(
                                portal_instance=instance_item,
                                service_id=service_obj,
                                app_id=app_obj,
                                rel_type='other',
                                defaults={'updated_date': update_time}
                            )
                    except:
                        pass
            deletes = App_Map.objects.filter(portal_instance=instance_item, app_id=item.id,
                                             updated_date__lt=update_time)
            deletes.delete()
            deletes = App_Service.objects.filter(portal_instance=instance_item, app_id=item.id,
                                                 updated_date__lt=update_time)
            deletes.delete()

            # Update the portal instance with the current webapp update timestamp.
            instance_item.webapp_updated = datetime.datetime.now()
            instance_item.save()

            result.set_success()
            return result.to_json()

        except Exception as e:
            logger.exception(f"Unable to update app {item_id} for {instance_alias}: {e}")
            result.add_error(f"Unable to update app {item_id} for {instance_item}")
            self.update_state(state="FAILURE", meta={result.to_json()})
            raise Ignore()
