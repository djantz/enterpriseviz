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
import json
import logging
import re
import time
from datetime import datetime, timedelta

import arcgis.gis.server
import requests
from bs4 import BeautifulSoup
from celery import shared_task, group, current_app
from celery.exceptions import Ignore
from celery_progress.backend import ProgressRecorder, Progress
from django.conf import settings as django_settings
from django.core.exceptions import MultipleObjectsReturned
from django.utils import timezone

from . import utils
from .models import Webmap, Service, Layer, App, User, Map_Service, Layer_Service, App_Map, App_Service, Portal, \
    WebhookNotificationLog
from .request_context import celery_logging_context

logger = logging.getLogger('enterpriseviz.tasks')


@shared_task(name="apply_site_log_level_in_worker")
def apply_site_log_level_in_worker():
    utils.apply_global_log_level()


@shared_task(bind=True, name="Update All")
def update_all(self, instance, items):
    for item in items:
        if item == "webmaps":
            update_webmaps.delay(instance, False)
        if item == "services":
            update_services.delay(instance, False)
        if item == "webapps":
            update_webapps.delay(instance, False)
        if item == "users":
            update_users.delay(instance, False)


@shared_task(bind=True, name="Update webmaps", time_limit=6000, soft_time_limit=3000)
@celery_logging_context
def update_webmaps(self, instance_alias, full_refresh=False, credential_token=None):
    """
    Update web maps for a given portal instance.

    This task connects to a target portal instance, retrieves web maps,
    processes each map's details (including layers, usage statistics, and service relationships),
    and updates or creates corresponding database records. It also removes any records that were not updated,
    ensuring the database reflects the current state of the portal.

    :param instance_alias: Alias of the portal instance to update.
    :type instance_alias: str
    :param full_refresh: Flag indicating whether to perform a full refresh (delete all and re-fetch everything)
                         instead of incremental update based on items modified since last refresh timestamp.
    :type full_refresh: bool
    :param credential_token: Token for temporary credentials (optional)
    :type credential_token: str
    :return: JSON-serialized update result containing counts of inserts, updates, deletions, and any error messages.
    :rtype: str
    """
    logger.debug(f"Starting update_webmaps task for instance_alias={instance_alias}, full_refresh={full_refresh}")

    # Initialize progress recorder and result container for tracking task progress and outcome
    progress_recorder = ProgressRecorder(self)
    result = utils.UpdateResult()

    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        target = utils.connect(instance_item, credential_token)
    except Exception as e:
        logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
        result.add_error(f"Unable to connect to {instance_alias}")
        return {"result": result.to_json()}

    try:
        update_time = timezone.now()
        logger.debug(f"Update timestamp: {update_time}")

        # Build search query with optional date filter for incremental updates
        if instance_item.portal_type == "agol":
            org_id = instance_item.org_id
            # org_id required for AGOL search
            query = f"orgid:{org_id} AND NOT owner:esri*"
        else:
            query = "NOT owner:esri*"

        # Add date filter for incremental updates if last run time exists
        if instance_item.webmap_updated and not full_refresh:
            # Format: modified:[timestamp TO timestamp] in milliseconds since epoch
            last_update_ms = int(instance_item.webmap_updated.timestamp() * 1000)
            query += f" AND modified:[{last_update_ms} TO {int(update_time.timestamp() * 1000)}]"
            logger.info(f"Incremental update: searching for webmaps modified since {instance_item.webmap_updated}")
        else:
            logger.info("Full update: searching for all webmaps")

        total_webmaps = target.content.advanced_search(
            query=query,
            max_items=-1,
            return_count=True,
            filter='type:"Web Map"'
        )

        logger.info(f"Found {total_webmaps} web maps to process in portal '{instance_alias}'")


        if full_refresh:
            deleted_count = Webmap.objects.filter(portal_instance=instance_item).count()
            Webmap.objects.filter(portal_instance=instance_item).delete()
            logger.info(f"Deleted {deleted_count} existing web maps for portal '{instance_alias}'")

        # Set up batch processing
        batch_size = 100
        batch_tasks = []
        logger.debug(f"Setting up batch processing with batch size: {batch_size}")

        for batch in range(0, total_webmaps, batch_size):
            logger.debug(f"Creating batch task for items {batch} to {min(batch+batch_size, total_webmaps)}")
            batch_tasks.append(
                process_batch_maps.s(
                    instance_alias,
                    credential_token,
                    batch,
                    batch_size,
                    update_time,
                    query
                )
            )

        logger.info(f"Created {len(batch_tasks)} batch tasks for processing {total_webmaps} web maps")

        # Execute parallel processing by batch
        logger.debug("Starting parallel batch processing")
        task_group = group(batch_tasks)
        batch_results = task_group.apply_async()
        logger.debug(f"Batch processing started with group ID: {batch_results.id}")

        # Monitor progress
        logger.debug("Monitoring batch processing progress")
        while not batch_results.ready():
            try:
                completed_tasks = sum(
                    task.result.get("current", 0) if "current" in task.result else batch_size
                    for task in batch_results.children
                    if task.result
                )
                progress_percentage = (completed_tasks / total_webmaps) * 100
                logger.debug(f"Progress: {progress_percentage:.1f}% ({completed_tasks}/{total_webmaps} items)")
                progress_recorder.set_progress(completed_tasks, total_webmaps)
                time.sleep(1.5)
            except Exception as e:
                logger.warning(f"Error calculating progress: {e}")
                time.sleep(1.5)
                continue

        # Aggregate results
        logger.info(f"All batch tasks completed, aggregating results")
        logger.debug(f"Batch results: {batch_results}")

        success_count = 0
        failure_count = 0

        for batch in batch_results.get(disable_sync_subtasks=False):
            batch_result = utils.UpdateResult(**batch["result"])

            if batch_result.success is False:
                failure_count += 1
                result.success = False
                logger.warning(f"Batch task reported failure: {batch_result.error_messages}")
            else:
                success_count += 1

            # Aggregate counts
            result.num_updates += batch_result.num_updates
            result.num_inserts += batch_result.num_inserts
            result.num_deletes += batch_result.num_deletes
            result.num_errors += batch_result.num_errors
            result.error_messages.extend(batch_result.error_messages)

        logger.info(f"Batch processing summary: {success_count} successful batches, {failure_count} failed batches")

        # Delete web maps not updated in this run, implying they have been removed
        logger.debug(f"Cleaning up outdated records for portal '{instance_alias}'")
        delete_outdated_records(instance_item, update_time, [Webmap, Map_Service], result)
        logger.info(f"Outdated records cleanup completed with {result.num_deletes} deletions")

        instance_item.webmap_updated = timezone.now()
        instance_item.save()

        if not result.error_messages:
            result.set_success()
        logger.info(f"Web maps update for portal '{instance_alias}' completed.")
        logger.debug(f"Final result: {result.to_json()}")
        return result.to_json()

    except Exception as e:
        logger.critical(f"Webmaps update failed for portal '{instance_alias}': {e}", exc_info=True)
        result.add_error(f"Webmaps update failed")

        # Revoke child tasks if they exist
        if batch_results and batch_results.children:
            logger.warning(f"Revoking {len(batch_results.children)} child tasks due to failure")
            for child in batch_results.children:
                if child.id:
                    logger.debug(f"Revoking child task: {child.id}")
                    current_app.control.revoke(child.id, terminate=True, signal="SIGKILL")
            logger.info("All child tasks revoked")

        # Update task state and exit
        self.update_state(state="FAILURE", meta=result.to_json())
        raise Ignore()


@shared_task(bind=True, time_limit=6000, soft_time_limit=3000)
@celery_logging_context
def process_batch_maps(self, instance_alias, credential_token, batch, batch_size, update_time, query=None):
    result = utils.UpdateResult()
    progress_recorder = ProgressRecorder(self)

    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        target = utils.connect(instance_item, credential_token)
    except Exception as e:
        logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
        result.add_error(f"Unable to connect to {instance_alias}")
        return {"result": result.to_json()}

    try:
        logger.debug(f"Retrieving web maps for batch {batch} to {batch + batch_size}")

        # Use provided query or build default one
        if query is None:
            if instance_item.portal_type == "agol":
                org_id = instance_item.org_id
                logger.debug(f"Using AGOL-specific query with org_id: {org_id}")
                query = f'orgid:{org_id} AND NOT owner:esri*'
            else:
                logger.debug("Using standard Portal for ArcGIS query")
                query = 'NOT owner:esri*'

        webmap_item_list = target.content.advanced_search(
            query=query,
            max_items=batch_size,
            start=batch,
            sort_field="title",
            sort_order="asc",
            filter='type:"Web Map"'
        )

        results = webmap_item_list.get("results", [])
        total_webmaps = len(results)
        logger.debug(f"Retrieved {total_webmaps} web maps for batch {batch} to {batch + batch_size}")

        logger.debug(f"Starting to process {total_webmaps} web maps")

        for counter, webmap_item in enumerate(results):
            try:
                webmap_id = webmap_item.id
                webmap_title = webmap_item.title
                logger.debug(f"Processing web map {counter+1}/{total_webmaps}: {webmap_id} - '{webmap_title}'")

                logger.debug(f"Extracting data for web map: {webmap_id}")
                webmap_data = extract_webmap_data(webmap_item, instance_item, update_time)

                logger.debug(f"Updating or creating database record for web map: {webmap_id}")
                obj, created = Webmap.objects.update_or_create(
                    portal_instance=instance_item,
                    webmap_id=webmap_id,
                    defaults=webmap_data
                )

                if created:
                    logger.info(f"Created new record for web map: {webmap_id} - '{webmap_title}'")
                    result.add_insert()
                else:
                    logger.info(f"Updated existing record for web map: {webmap_id} - '{webmap_title}'")
                    result.add_update()

                # Link services to the web map
                service_count = len(webmap_data["webmap_services"])
                logger.debug(f"Linking {service_count} services to web map: {webmap_id}")
                link_services_to_webmap(instance_item, obj, webmap_data["webmap_services"])

                progress_recorder.set_progress(counter + 1, total_webmaps)

            except Exception as e:
                logger.error(f"Unable to process web map {webmap_item.id}: {e}", exc_info=True)
                result.add_error(f"Unable to process {webmap_item.id}")
                continue

        logger.info(f"Completed processing batch {batch} to {batch + batch_size}")
        logger.debug(f"Batch summary - Updates: {result.num_updates}, Inserts: {result.num_inserts}, Errors: {result.num_errors}")

        result.set_success()
        return {"result": result.to_json()}

    except Exception as e:
        logger.error(f"Webmaps in batch {batch} to {batch + batch_size}: {e}", exc_info=True)
        result.add_error(f"Error processing webmaps in batch {batch} to {batch + batch_size}")
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
        "webmap_created": utils.epoch_to_datetime(item.created),
        "webmap_modified": utils.epoch_to_datetime(item.modified),
        "webmap_access": access,
        "webmap_extent": item.extent,
        "webmap_description": description,
        "webmap_views": item.numViews,
        "webmap_layers": layers,
        "webmap_services": services,
        "webmap_usage": usage,
        "updated_date": update_time,
        "webmap_last_viewed": utils.epoch_to_datetime(item.lastViewed) if instance_item.portal_type == "agol" else None
    }


def process_layers(item):
    """
    Process web map layers and extract layer and service information.

    This function recursively processes operational layers from a web map, including handling
    nested group layers. It extracts layer metadata (name, type, URL, IDs) and identifies
    associated services. For each service URL, it attempts to extract the service layer ID
    (the numeric index at the end of the URL, e.g., MapServer/5) and the web map's internal
    layer ID used for references by applications.

    :param item: ArcGIS web map Item object to process.
    :type item: arcgis.gis.Item
    :return: Tuple containing:
             - layers (dict): Dictionary mapping layer names to their metadata including url, type,
               service_item_id, and webmap_layer_id
             - services (list): List of tuples (service_url, service_layer_id, webmap_layer_id)
               where service_layer_id and webmap_layer_id may be None
    :rtype: tuple[dict, list]
    """

    layers = {}
    services = []
    wm_content = item.get_data()

    def process_op_layer(layer):
        if layer.get("layerType") == "GroupLayer":
            for sublayer in layer.get("layers", []):
                process_op_layer(sublayer)
        else:
            # Web map's internal layer ID (used by apps for references)
            webmap_layer_id = layer.get("id", None)

            # Service item ID (the published service this layer comes from)
            service_item_id = layer.get("itemId", None)

            layer_name = layer.get("title", None)
            layer_type = layer.get("layerType", None)
            layer_url = layer.get("url", None)

            if layer_url:
                # Extract service_layer_id from URL if present (last segment is a digit)
                url_parts = layer_url.split("/")
                service_layer_id = None
                if url_parts and url_parts[-1].isdigit():
                    service_layer_id = int(url_parts[-1])
                    s_url = "/".join(url_parts[:-1])
                else:
                    s_url = layer_url

                services.append((s_url, service_layer_id, webmap_layer_id))

            # Store layer info including webmap_layer_id
            layers[layer_name] = {
                "url": layer_url,
                "type": layer_type,
                "service_item_id": service_item_id,
                "webmap_layer_id": webmap_layer_id  # NEW
            }

    for op_layer in wm_content.get("operationalLayers", []):
        process_op_layer(op_layer)

    return layers, services


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
    """
    Link web maps to services if they exist in the database.

    This function creates or updates Map_Service relationships between a web map and its
    associated services. It handles service layer IDs (for specific layer references within
    a service) and web map layer IDs (internal identifiers used by the web map). The function
    also cleans up outdated Map_Service records that are no longer present in the web map.

    :param instance_item: Portal instance containing the web map and services.
    :type instance_item: Portal
    :param webmap_obj: Web map model object to link services to.
    :type webmap_obj: Webmap
    :param services: List of service data as tuples (service_url, service_layer_id, webmap_layer_id).
                    service_layer_id and webmap_layer_id may be None.
    :type services: list[tuple[str, int | None, str | None]]
    :return: None
    :rtype: None
    """
    update_time = timezone.now()
    for service_data in services:
        service_url, service_layer_id, webmap_layer_id = service_data

        try:
            s_obj = Service.objects.get(service_url__overlap=[service_url])
        except Service.DoesNotExist:
            logger.warning(f"Service not found matching URL '{service_url}' from {webmap_obj}.")
            continue
        except MultipleObjectsReturned:
            s_obj = Service.objects.filter(service_url__overlap=[service_url]).first()

        Map_Service.objects.update_or_create(
            portal_instance=instance_item,
            webmap_id=webmap_obj,
            service_id=s_obj,
            service_layer_id=service_layer_id,
            defaults={
                "updated_date": update_time,
                "webmap_layer_id": webmap_layer_id
            }
        )

    deletes = Map_Service.objects.filter(
        portal_instance=instance_item,
        webmap_id=webmap_obj,
        updated_date__lt=update_time
    )
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


def delete_outdated_records(instance_item, update_time, models, result=None):
    """
    Deletes outdated records from the specified models where `updated_date` is older than `update_time`.

    :param instance_item: The portal instance associated with the records.
    :type instance_item: model
    :param update_time: The timestamp of the current update cycle.
    :type update_time: datetime.datetime
    :param models: A list of Django model classes to delete outdated records from.
    :type models: list
    :param result: Optional
    :type result:
    """
    for i ,model in enumerate(models):
        qs = model.objects.filter(portal_instance=instance_item, updated_date__lt=update_time)
        deleted_count, _ = qs.delete()

        logger.info(f"Deleted {deleted_count} records from {model}.")
        if result is not None:
            result.add_delete(deleted_count)
        else:
            logger.info(f"Deleted {deleted_records.count()} records from {model}")
            logger.info(f"Deleted {deleted_count} records from {model}")


@celery_logging_context
def check_deleted_items(instance_alias, credential_token, item_type, update_time):
    """
    Check if items that weren't modified since last update still exist in the portal.
    This detects deletions that occurred between updates by fetching all portal items
    in bulk and comparing against the database.

    :param instance_alias: Alias of the portal instance
    :type instance_alias: str
    :param credential_token: Token for temporary credentials (optional)
    :type credential_token: str
    :param item_type: Type of item to check ('webmap', 'service', 'webapp')
    :type item_type: str
    :param update_time: The timestamp of the current update cycle
    :type update_time: datetime
    :return: Dictionary with metrics (checked, still_exist, deleted)
    :rtype: dict
    :raises Exception: If the check fails, ensuring delete_outdated_records won't run
    """
    logger.info(f"Starting check_deleted_items for {item_type} in portal '{instance_alias}'")

    # Model mapping for bulk updates
    MODEL_MAP = {
        'webmap': Webmap,
        'service': Service,
        'webapp': App
    }

    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        target = utils.connect(instance_item, credential_token)
    except Exception as e:
        logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
        raise Exception(f"Cannot check deleted items - connection failed: {e}") from e

    try:
        # Determine item type configuration
        if item_type == 'webmap':
            last_update = instance_item.webmap_updated
            items_to_check = Webmap.objects.filter(
                portal_instance=instance_item,
                updated_date__lt=update_time
            )
            id_field = 'webmap_id'
            portal_filter = 'type:"Web Map"'
        elif item_type == 'service':
            last_update = instance_item.service_updated
            items_to_check = Service.objects.filter(
                portal_instance=instance_item,
                updated_date__lt=update_time
            )
            id_field = 'portal_id'
            portal_filter = 'type:"Feature Service" OR type:"Map Service" OR type:"Feature Layer" OR type:"Map Image Layer"'
        elif item_type == 'webapp':
            last_update = instance_item.webapp_updated
            items_to_check = App.objects.filter(
                portal_instance=instance_item,
                updated_date__lt=update_time
            )
            id_field = 'app_id'
            portal_filter = 'type:"Web Mapping Application" OR type:"Dashboard" OR type:"StoryMap" OR type:"Web Experience" OR type:"Form" OR type:"QuickCapture Project" OR type:"Hub Site Application" OR type:"Hub Page"'
        else:
            logger.error(f"Unknown item_type: {item_type}")
            raise ValueError(f"Unknown item_type: {item_type}")

        total_to_check = items_to_check.count()
        logger.info(f"Checking {total_to_check} {item_type} items that weren't modified since {last_update}")

        if total_to_check == 0:
            logger.info(f"No {item_type} items to check for deletions")
            return {'checked': 0, 'still_exist': 0, 'deleted': 0}

        # Build query to fetch ALL items of this type from portal
        if instance_item.portal_type == "agol":
            org_id = instance_item.org_id
            query = f'orgid:{org_id} AND NOT owner:esri*'
        else:
            query = "NOT owner:esri*"

        logger.debug(f"Fetching all {item_type} items from portal with query: {query}")

        # Fetch all portal items
        portal_items_result = target.content.advanced_search(
            query=query,
            max_items=-1,
            return_count=False,
            filter=portal_filter
        )

        # Extract portal item IDs into a set for fast lookup
        portal_item_ids = set()
        for portal_item in portal_items_result.get("results", []):
            portal_item_ids.add(portal_item.id)

        logger.info(f"Found {len(portal_item_ids)} {item_type} items currently in portal")

        # Compare database items against portal items
        updated_count = 0
        checked_count = 0

        # Process database items in batches
        batch_size = 500
        items_batch = []

        for item in items_to_check.iterator(chunk_size=batch_size):
            try:
                checked_count += 1

                # Get the portal ID(s) for this item
                if item_type == 'service':
                    portal_ids = getattr(item, id_field, {})
                    if not portal_ids:
                        continue

                    # Check if any of the service's portal IDs still exist in portal
                    if isinstance(portal_ids, dict):
                        # Extract the actual IDs from the dict values
                        if any(pid in portal_item_ids for pid in portal_ids.values()):
                            items_batch.append(item.pk)
                            updated_count += 1
                        else:
                            logger.info(f"Service '{item.service_name}' with portal_ids {portal_ids} no longer exists in portal")
                    else:
                        # Fallback for set or other iterables
                        if any(pid in portal_item_ids for pid in portal_ids):
                            items_batch.append(item.pk)
                            updated_count += 1
                        else:
                            logger.info(f"Service '{item.service_name}' with portal_ids {portal_ids} no longer exists in portal")
                else:
                    # For webmaps and webapps, portal_id is a simple string field
                    portal_id = getattr(item, id_field, None)
                    if not portal_id:
                        continue

                    if portal_id in portal_item_ids:
                        items_batch.append(item.pk)
                        updated_count += 1
                    else:
                        logger.info(f"{item_type.capitalize()} with ID '{portal_id}' no longer exists in portal")

                # Bulk update timestamps when batch is full
                if len(items_batch) >= batch_size:
                    model = MODEL_MAP[item_type]
                    model.objects.filter(pk__in=items_batch).update(updated_date=update_time)
                    logger.debug(f"Bulk updated {len(items_batch)} {item_type} items, processed {checked_count}/{total_to_check}")
                    items_batch = []

            except Exception as e:
                logger.warning(f"Error checking {item_type} item {getattr(item, 'pk', 'unknown')}: {e}")
                continue

        # Update remaining items in final batch
        if items_batch:
            model = MODEL_MAP[item_type]
            model.objects.filter(pk__in=items_batch).update(updated_date=update_time)
            logger.debug(f"Bulk updated final {len(items_batch)} {item_type} items")

        deleted_count = total_to_check - updated_count
        logger.info(f"Completed checking {checked_count} {item_type} items. "
                   f"Still exist: {updated_count}, Will be removed: {deleted_count}")

        return {
            'checked': checked_count,
            'still_exist': updated_count,
            'deleted': deleted_count
        }

    except Exception as e:
        logger.error(f"Error checking deleted items for {item_type}: {e}", exc_info=True)
        # Re-raise to prevent delete_outdated_records from running
        raise Exception(f"Failed to check deleted items for {item_type}") from e

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
        db_server, db_version, db_database = None, None, None
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
        elif "FileGDB" in db_obj.get("onServerWorkspaceFactoryProgID", ""):
            parts = connection_string.split("DATABASE=")
            if len(parts) > 1:
                db_database = parts[1].replace(r"\\", "\\")
        else:
            continue

        for dataset in db_obj.get("datasets", []):
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
                defaults={"updated_date": update_time}
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
        logger.warning(f"Unable to get publish map name for '{service_url}'", exc_info=True)
    return None


@shared_task(bind=True, time_limit=6000, soft_time_limit=3000, name="Update services")
@celery_logging_context
def update_services(self, instance_alias, full_refresh=False, credential_token=None):
    """
    Update service and layer records for a given portal instance.

    This task synchronizes service and layer data from a portal instance with the local database.
    Depending on the portal type (AGOL or non-AGOL), it retrieves service items, processes usage statistics,
    descriptions, ownership, and related layer information, then updates or creates corresponding records.
    Outdated records are removed to maintain data consistency.

    :param instance_alias: The portal instance alias.
    :type instance_alias: str
    :param full_refresh: Flag indicating whether to perform a full refresh (delete all and re-fetch everything)
                         instead of incremental update based on items modified since last refresh timestamp.
    :type full_refresh: bool
    :param credential_token: Token for temporary credentials (optional)
    :type credential_token: str
    :return: JSON serialized result of the update process including counts of inserts, updates, deletions, and errors.
    :rtype: str
    """
    logger.debug(f"Starting update_services task for instance_alias={instance_alias}, full_refresh={full_refresh}")

    result = utils.UpdateResult()
    progress_recorder = ProgressRecorder(self)
    update_time = timezone.now()  # Timestamp for the current update cycle

    def fetch_usage_report(server, service_list):
        query_list = ",".join(service_list)
        quick_report = server.usage.quick_report(since="LAST_MONTH",
                                                 queries=query_list,
                                                 metrics="RequestCount")

        dates = [utils.epoch_to_date(ts) for ts in quick_report["report"]["time-slices"]]

        for s in quick_report["report"]["report-data"][0]:
            name = s["resourceURI"].replace("services/", "").split(".")[0]
            service_type = s["resourceURI"].split('.')[-1]
            data = [0 if d is None else d for d in s["data"]]

            usage_data = {
                "dates": dates,
                "values": data,
            }

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

            s_obj.service_usage = usage_data
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
                                                       service_url__overlap=[view_item.url])
                    except Service.DoesNotExist:
                        logger.warning(f"Service not found matching URL {view_item.url} related to {parent.url}")
                        continue
                    except MultipleObjectsReturned:
                        view_obj = Service.objects.filter(portal_instance=instance_item,
                                                          service_url__overlap=[view_item.url]).first()
                    try:
                        service_obj = Service.objects.get(portal_instance=instance_item,
                                                          service_url__overlap=[parent.url])
                    except Service.DoesNotExist:
                        logger.warning(f"Service not found matching URL {parent.url} related to {view_item.url}")
                        continue
                    except MultipleObjectsReturned:
                        service_obj = Service.objects.filter(portal_instance=instance_item,
                                                             service_url__overlap=[parent.url]).first()
                    view_obj.service_view = service_obj
                    service_obj.service_view = view_obj
                    view_obj.save()
                    service_obj.save()
            except Exception as e:
                logger.error(f"Error processing view for {view_item}", exc_info=True)

    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        target = utils.connect(instance_item, credential_token)
    except Exception as e:
        logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
        result.add_error(f"Unable to connect to {instance_alias}")
        return {"result": result.to_json()}

    if instance_item.portal_type == "agol":
        logger.debug(f"Processing AGOL portal type for '{instance_alias}'")
        try:
            # Get organization ID
            logger.debug("Retrieving organization ID from portal")
            response = requests.get(f"{target.url}/sharing/rest/portals/self?culture=en&f=pjson")
            if response.status_code == 200:
                data = response.json()
                org_id = data.get("id")
                logger.debug(f"Retrieved organization ID: {org_id}")
            else:
                logger.warning(f"Failed to retrieve organization ID, status code: {response.status_code}")
                org_id = None
        except Exception as e:
            logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
            result.add_error(f"Unable to connect to {instance_alias}")
            self.update_state(state="FAILURE", meta=result.to_json())
            raise Ignore()

        try:
            if full_refresh:
                service_count = Service.objects.filter(portal_instance=instance_item).count()
                layer_count = Layer.objects.filter(portal_instance=instance_item).count()
                Service.objects.filter(portal_instance=instance_item).delete()
                Layer.objects.filter(portal_instance=instance_item).delete()
                logger.info(f"Deleted {service_count} services and {layer_count} layers for portal '{instance_alias}'")
            else:
                logger.debug("Proceeding with incremental update")

            logger.debug("Searching for Map Image Layer and Feature Layer services")

            # Build search query with optional date filter for incremental updates
            search_query = "NOT owner:esri*"
            if instance_item.service_updated and not full_refresh:
                # Format: modified:[timestamp TO timestamp] in milliseconds since epoch
                last_update_ms = int(instance_item.service_updated.timestamp() * 1000)
                search_query += f" AND modified:[{last_update_ms} TO {int(update_time.timestamp() * 1000)}]"
                logger.info(f"Incremental update: searching for services modified since {instance_item.service_updated}")
            else:
                logger.info("Full update: searching for all services")

            services = (target.content.search(search_query, "Map Image Layer", max_items=10000) +
                        target.content.search(search_query, "Feature Layer", max_items=10000))
            total_services = len(services)
            logger.info(f"Found {total_services} services to process in portal '{instance_alias}'")
            # total_services = target.content.advanced_search(query=f"orgid:{org_id} AND NOT owner:esri*", max_items=-1, return_count=True,
            #                                filter='type:"Map Service" OR type:"Feature Service"')
            # logger.info(f"Found {total_services} services to process in portal '{instance_alias}'")
            # Process each service
            logger.debug(f"Starting to process {total_services} services")
            for counter, service in enumerate(services):
                service_id = getattr(service, 'id', 'unknown')
                service_title = getattr(service, 'title', 'unknown')
                logger.debug(f"Processing service {counter+1}/{total_services}: {service_id} - '{service_title}'")

                try:
                    logger.debug(f"Extracting access information for service: {service_id}")
                    access = get_access(service)

                    url = []  # List to hold unique service URLs
                    service_layers = {}  # Dictionary to map layer names to service designations

                    # Attempt to retrieve the owner for the current service
                    logger.debug(f"Retrieving owner for service: {service_id}")
                    owner = get_owner(instance_item, service.owner)
                    logger.debug(f"Owner for service {service_id}: {owner}")

                    usage = []  # Placeholder for usage statistics
                    trend = 0  # Initialize usage trend percentage

                    # Get service description
                    logger.debug(f"Extracting description for service: {service_id}")
                    description = get_description(service)

                    # If service usage reporting is enabled, process the usage data
                    if django_settings.USE_SERVICE_USAGE_REPORT and org_id in service.url:
                        logger.debug(f"Processing usage data for service: {service_id}")
                        try:
                            trend = 0
                            report = service.usage(date_range="30D", as_df=False)
                            # Convert usage data entries to integers, defaulting to 0 when necessary
                            usage = [0 if d is None else int(d[1]) for d in report["data"][0]["num"]]
                            baseline = sum(usage[0:15])
                            compare = sum(usage[15:])

                            try:
                                trend = ((compare - baseline) / baseline) * 100
                                logger.debug(f"Usage trend for service {service_id}: {trend:.2f}%")
                            except ZeroDivisionError:
                                trend = 0 if (baseline == 0 and compare == 0) else 999
                                logger.debug(f"Zero division in trend calculation for service {service_id}, using default: {trend}")
                        except Exception as usage_error:
                            logger.warning(f"Unable to process usage data for service {service_id}: {usage_error}")
                    else:
                        logger.debug(f"Skipping usage data processing for service {service_id} (not enabled or not AGOL)")

                    # Create a new Service record with the aggregated data
                    logger.debug(f"Creating or updating service record for: {service_id}")
                    s_obj, created = Service.objects.update_or_create(
                        portal_instance=instance_item,
                        service_name=service.title,
                        defaults={"service_url": [service.url],
                                  "service_layers": service_layers,
                                  "service_mxd_server": service.get("sourceUrl", None),
                                  "service_mxd": None,
                                  "portal_id": {service.type: service.id},
                                  "service_type": service.type,
                                  "service_description": description,
                                  "service_owner": owner,
                                  "service_access": access,
                                  "service_usage": usage,
                                  "service_usage_trend": trend,
                                  "service_last_viewed": utils.epoch_to_datetime(service.lastViewed)}
                    )

                    if created:
                        logger.info(f"Created new record for service: {service_id} - '{service_title}'")
                        result.add_insert()
                    else:
                        logger.info(f"Updated existing record for service: {service_id} - '{service_title}'")
                        result.add_update()

                    # Process associated layers if available
                    if service.layers:
                        layer_count = len(service.layers)
                        logger.debug(f"Processing {layer_count} layers for service: {service_id}")

                        for layer_index, layer in enumerate(service.layers):
                            layer_name = getattr(layer.properties, 'name', 'unknown')
                            logger.debug(f"Processing layer {layer_index+1}/{layer_count}: {layer_name}")

                            # Normalize URL by removing a trailing numeric segment if present
                            s_url = layer.url if not layer.url.split("/")[-1].isdigit() else "/".join(
                                layer.url.split("/")[:-1])

                            if s_url not in url:
                                url.append(s_url)  # Append unique service URL
                                logger.debug(f"Added unique service URL: {s_url}")

                            # Check for a specific hosted layer identifier and update service_layers accordingly
                            if org_id in s_url:
                                logger.debug(f"Layer {layer_name} identified as hosted layer")
                                service_layers[layer.properties.name] = "Hosted"

                                # Create a new Layer record if one does not exist
                                logger.debug(f"Creating or updating layer record for: {layer_name}")
                                obj, layer_created = Layer.objects.update_or_create(
                                    portal_instance=instance_item,
                                    layer_server="Hosted",
                                    layer_version=None,
                                    layer_database=None,
                                    layer_name=layer.properties.name,
                                    defaults={"updated_date": update_time}
                                )

                                if layer_created:
                                    logger.debug(f"Created new layer record: {layer_name}")
                                else:
                                    logger.debug(f"Updated existing layer record: {layer_name}")

                                # Create or update the relationship between the layer and service
                                logger.debug(f"Creating or updating layer-service relationship for: {layer_name}")
                                rel_obj, rel_created = Layer_Service.objects.update_or_create(
                                    portal_instance=instance_item,
                                    layer_id=obj,
                                    service_id=s_obj,
                                    defaults={"updated_date": update_time}
                                )

                                if rel_created:
                                    logger.debug(f"Created new layer-service relationship for: {layer_name}")
                                else:
                                    logger.debug(f"Updated existing layer-service relationship for: {layer_name}")
                    else:
                        logger.debug(f"No layers found for service: {service_id}")

                    # Update progress
                    progress_percentage = ((counter + 1) / total_services) * 100
                    logger.debug(f"Progress: {progress_percentage:.1f}% ({counter+1}/{total_services})")
                    progress_recorder.set_progress(counter + 1, total_services)

                except Exception as e:
                    logger.error(f"Unable to process service {service_id} - '{service_title}': {e}", exc_info=True)
                    result.add_error(f"Unable to update {service_title}")  # Log error for the current service

            # Remove outdated Service, Layer, and Layer_Service records
            logger.debug(f"Cleaning up outdated records for portal '{instance_alias}'")
            delete_outdated_records(instance_item, update_time, [Service, Layer, Layer_Service], result)
            logger.info(f"Outdated records cleanup completed with {result.num_deletes} deletions")

            # Search for view services and associate them with their parent services
            logger.debug("Searching for view services to associate with parent services")
            view_items = target.content.search("NOT owner:esri* AND typekeywords:View Service", "Feature Layer",
                                               max_items=2000)
            view_count = len(view_items)
            logger.info(f"Found {view_count} view services to process")

            if view_count > 0:
                logger.debug("Processing view services and their relationships")
                process_views(view_items)
                logger.info("View service processing completed")
            else:
                logger.debug("No view services found to process")

            # Update the portal instance's timestamp for service updates
            logger.debug(f"Updating last updated timestamp for portal '{instance_alias}'")
            instance_item.service_updated = timezone.now()
            instance_item.save()
            logger.debug(f"Portal '{instance_alias}' last updated timestamp set to {instance_item.service_updated}")

            # Mark the update process as successful
            result.set_success()
            logger.info(f"Services update for portal '{instance_alias}' completed successfully")
            logger.debug(f"Final result: {result.to_json()}")
            return result.to_json()

        except Exception as e:
            logger.critical(f"Services update failed for portal '{instance_alias}': {e}", exc_info=True)
            result.add_error(f"Services update failed")
            self.update_state(state="FAILURE", meta=result.to_json())
            raise Ignore()

    else:
        logger.debug(f"Processing Enterprise portal type for '{instance_alias}'")
        batch_results = None

        try:
            import re
            logger.debug("Retrieving GIS servers list")
            gis_servers = target.admin.servers.list()
            server_count = len(gis_servers)
            logger.info(f"Found {server_count} GIS servers in portal '{instance_alias}'")

        except Exception as e:
            logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
            result.add_error(f"Unable to connect to {instance_alias}")
            self.update_state(state="FAILURE", meta=result.to_json())
            raise Ignore()

        try:
            if overwrite:
                service_count = Service.objects.filter(portal_instance=instance_item).count()
                layer_count = Layer.objects.filter(portal_instance=instance_item).count()
                Service.objects.filter(portal_instance=instance_item).delete()
                Layer.objects.filter(portal_instance=instance_item).delete()
                logger.info(f"Deleted {service_count} services and {layer_count} layers for portal '{instance_alias}'")
            else:
                logger.debug("Proceeding with incremental update")

            service_list = []
            batch_tasks = []
            total_folders = 0

            # Process each GIS server
            for server_index, gis_server in enumerate(gis_servers):
                logger.debug(f"Processing GIS server {server_index+1}/{server_count}")

                # Get folders for this server
                logger.debug(f"Retrieving service folders for server {server_index+1}")
                if not hasattr(gis_server, "services") or gis_server.services is None:
                    logger.warning(f"Server {gis_server} has no services collection, skipping")
                    continue
                try:
                    folders = gis_server.services.folders
                except Exception as e:
                    logger.warning(f"Cannot access services on {gis_server.url}: {e}")
                    continue

                # Remove system folder
                if "System" in folders:
                    logger.debug("Removing 'System' folder from processing list")
                    folders.remove("System")

                folder_count = len(folders)
                total_folders += folder_count
                logger.info(f"Found {folder_count} service folders to process in server {server_index+1}")

                # Create batch tasks for each folder
                logger.debug(f"Creating batch tasks for {folder_count} folders")
                for folder_index, folder in enumerate(folders):
                    logger.debug(f"Creating batch task for folder {folder_index+1}/{folder_count}: {folder}")
                    batch_tasks.append(
                        process_batch_services.s(instance_alias,
                                                 credential_token,
                                                 folder,
                                                 update_time
                                                 )
                    )

            logger.info(f"Created {len(batch_tasks)} batch tasks for processing {total_folders} folders")

            # Parallel processing by folder
            if batch_tasks:
                logger.debug("Starting parallel batch processing")
                task_group = group(batch_tasks)
                batch_results = task_group.apply_async()
                logger.debug(f"Batch processing started with group ID: {batch_results.id}")

                # Monitor group progress
                logger.debug("Monitoring batch processing progress")
                while not batch_results.ready():
                    try:
                        completed_tasks = sum(1 for task in batch_results.children if task.ready())
                        progress_percentage = (completed_tasks / total_folders) * 100
                        logger.debug(f"Progress: {progress_percentage:.1f}% ({completed_tasks}/{total_folders} folders)")
                        progress_recorder.set_progress(completed_tasks, total_folders)
                        time.sleep(1.5)
                    except Exception as e:
                        logger.warning(f"Error calculating progress: {e}")
                        time.sleep(1.5)
                        continue

                # Aggregate results
                logger.info(f"All batch tasks completed, aggregating results")
                logger.debug(f"Batch results: {batch_results}")

                success_count = 0
                failure_count = 0

                for batch in batch_results.get(disable_sync_subtasks=False):
                    batch_result = utils.UpdateResult(**batch["result"])

                    if batch_result.success is False:
                        failure_count += 1
                        result.success = False
                        logger.warning(f"Batch task reported failure: {batch_result.error_messages}")
                    else:
                        success_count += 1

                    # Aggregate counts
                    result.num_updates += batch_result.num_updates
                    result.num_inserts += batch_result.num_inserts
                    result.num_deletes += batch_result.num_deletes
                    result.num_errors += batch_result.num_errors
                    result.error_messages.extend(batch_result.error_messages)
                    service_list.extend(batch["service_usage"])

                logger.info(f"Batch processing summary: {success_count} successful batches, {failure_count} failed batches")

            else:
                logger.warning("No batch tasks created, no folders to process")

            if django_settings.USE_SERVICE_USAGE_REPORT:
                logger.debug(f"Processing usage reports for {len(service_list)} services")
                fetch_usage_report(gis_server, service_list)
                logger.info("Usage report processing completed")
            else:
                logger.debug("Service usage reporting is disabled, skipping")

            logger.debug(f"Cleaning up outdated records for portal '{instance_alias}'")
            delete_outdated_records(instance_item, update_time, [Service, Layer, Layer_Service], result)
            logger.info(f"Outdated records cleanup completed with {result.num_deletes} deletions")

            logger.debug("Searching for view services to associate with parent services")
            view_items = target.content.search("NOT owner:esri* AND typekeywords:View Service", "Feature Layer",
                                               max_items=2000)
            view_count = len(view_items)
            logger.info(f"Found {view_count} view services to process")

            if view_count > 0:
                logger.debug("Processing view services and their relationships")
                process_views(view_items)
                logger.info("View service processing completed")
            else:
                logger.debug("No view services found to process")

            instance_item.service_updated = update_time
            instance_item.save()

            result.set_success()
            logger.info(f"Services update for portal '{instance_alias}' completed successfully")
            logger.debug(f"Final result: {result.to_json()}")
            return result.to_json()

        except Exception as e:
            logger.critical(f"Services update failed for portal '{instance_alias}': {e}", exc_info=True)
            result.add_error(f"Services update failed")

            # Revoke child tasks if they exist
            if batch_results and batch_results.children:
                logger.warning(f"Revoking {len(batch_results.children)} child tasks due to failure")
                for child in batch_results.children:
                    if child.id:
                        logger.debug(f"Revoking child task: {child.id}")
                        current_app.control.revoke(child.id, terminate=True, signal="SIGKILL")
                logger.info("All child tasks revoked")

            self.update_state(state="FAILURE", meta=result.to_json())
            raise Ignore()


@shared_task(bind=True, time_limit=6000, soft_time_limit=3000)
@celery_logging_context
def process_batch_services(self, instance_alias, credential_token, folder, update_time):
    result = utils.UpdateResult()
    service_usage_list = []

    try:
        logger.debug("Compiling regex patterns for service processing")
        regex_patterns = compile_regex_patterns()
        try:
            instance_item = Portal.objects.get(alias=instance_alias)
            target = utils.connect(instance_item, credential_token)
            logger.debug("Retrieving GIS servers list")
            gis_servers = target.admin.servers.list()
            server_count = len(gis_servers)
            logger.debug(f"Found {server_count} GIS servers")

        except Exception as e:
            logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
            result.add_error(f"Unable to connect to {instance_alias}")
            return {"result": result.to_json(), "service_usage": service_usage_list}

        total_services_processed = 0
        logger.info(f"Processing services in folder '{folder}' across {server_count} servers")

        for server_index, server in enumerate(gis_servers):
            logger.debug(f"Processing server {server_index+1}/{server_count}")

            try:
                logger.debug(f"Retrieving services in folder '{folder}' for server {server_index+1}")
                services = server.services.list(folder=folder)
                service_count = len(services)
                logger.info(f"Found {service_count} services in folder '{folder}' for server {server_index+1}")

                for service_index, service in enumerate(services):
                    service_name = getattr(service.properties, 'serviceName', 'unknown')
                    logger.debug(f"Processing service {service_index+1}/{service_count}: {service_name}")

                    try:
                        service_result = process_single_service(target, instance_item, service, folder, update_time, regex_patterns, result)

                        if service_result is not None:
                            logger.debug(f"Adding service '{service_name}' to usage list")
                            service_usage_list.append(service_result)

                        total_services_processed += 1

                    except Exception as e:
                        logger.error(f"Error processing service '{service_name}' in folder '{folder}': {e}", exc_info=True)
                        result.add_error(f"Error processing service '{service_name}' in folder '{folder}'")

            except Exception as e:
                logger.error(f"Error retrieving services for server {server_index+1} in folder '{folder}': {e}", exc_info=True)
                result.add_error(f"Error retrieving services for server {server_index+1} in folder '{folder}'")

        logger.info(f"Completed processing {total_services_processed} services in folder '{folder}'")
        logger.debug(f"Batch summary - Updates: {result.num_updates}, Inserts: {result.num_inserts}, Errors: {result.num_errors}")

        result.set_success()
        return {"result": result.to_json(), "service_usage": service_usage_list}

    except Exception as e:
        logger.error(f"Error processing services in folder {folder}: {e}", exc_info=True)
        result.add_error(f"Error processing services in folder {folder}")
        return {"result": result.to_json()}


def process_single_service(target, instance_item, service, folder, update_time, regex_patterns, result):
    """
    Process a single service from a portal instance.

    This function extracts service details, processes related portal items, parses the service manifest,
    and creates or updates corresponding database records.

    :param target: Portal connection object
    :type target: arcgis.gis.GIS
    :param instance_item: Portal instance database object
    :type instance_item: Portal
    :param service: Service object to process
    :type service: Service
    :param folder: Folder containing the service
    :type folder: str
    :param update_time: Timestamp for this update operation
    :type update_time: datetime
    :param regex_patterns: Compiled regex patterns for service processing
    :type regex_patterns: dict
    :param result: UpdateResult object to track processing results
    :type result: UpdateResult
    :return: Service usage string for usage reporting or None if not applicable
    :rtype: str or None
    """
    service_name = getattr(service.properties, 'serviceName', 'unknown')
    logger.debug(f"Starting process_single_service for service: {service_name}, folder: {folder}")

    try:
        logger.debug(f"Service details: {service}")
        portal_ids = {}  # Map portal item types to their IDs
        service_usage_str = None
        access, owner, description = None, None, None

        # Construct the service name and corresponding URL based on folder structure
        logger.debug("Constructing service URL")
        base_url = target.hosting_servers[0].url
        logger.debug(f"Base URL: {base_url}")

        # Validate service has required attributes
        if not hasattr(service, "type"):
            logger.warning(f"Unable to determine service type for {service_name}")
            result.add_error(f"Unable to determine service type for {service_name}")
            return service_usage_str

        service_type = service.properties.type
        logger.debug(f"Service type: {service_type}")

        if not hasattr(service, "serviceName"):
            logger.warning(f"Unable to determine service name for {service_name}")
            result.add_error(f"Unable to determine service name for {service_name}")
            return service_usage_str

        service_name = service.properties.serviceName
        name = service_name if folder == "/" else f"{folder}/{service_name}"
        service_url = f"{base_url}/{name}/{service_type}"
        urls = [service_url]
        service_usage_str = f"services/{name}.{service_type}"

        logger.debug(f"Constructed service URL: {service_url}")
        logger.debug(f"Service usage string: {service_usage_str}")

        logger.debug(f"Fetching map name for service: {service_name}")
        map_name = get_map_name(service_url, target._con.token)
        if map_name:
            logger.debug(f"Retrieved map name: {map_name}")
        else:
            logger.debug("No map name found for service")

        logger.debug(f"Processing portal items for service: {service_name}")
        try:
            if hasattr(service.properties, 'portalProperties') and hasattr(service.properties.portalProperties, 'portalItems'):
                portal_items = service.properties.portalProperties.portalItems
                portal_item_count = len(portal_items)
                logger.debug(f"Found {portal_item_count} portal items for service: {service_name}")

                for item_index, item in enumerate(portal_items):
                    item_type = item.get("type", "unknown")
                    item_id = item.get("itemID", "unknown")
                    logger.debug(f"Processing portal item {item_index+1}/{portal_item_count}: {item_type} - {item_id}")

                    portal_ids[item_type] = item_id
                    logger.debug(f"Added portal ID mapping: {item_type} -> {item_id}")

                    if item_type == "FeatureServer":
                        feature_url = f"{base_url}/{name}/FeatureServer"
                        if feature_url not in urls:
                            logger.debug(f"Adding feature server URL: {feature_url}")
                            urls.append(feature_url)

                    logger.debug(f"Fetching details for portal item: {item_id}")
                    owner, access, description = fetch_portal_item_details(target, item_id, instance_item)
                    logger.debug(f"Retrieved details - Owner: {owner}, Access: {access}, Description length: {len(description) if description else 0}")
            else:
                logger.debug(f"No portal properties or portal items found for service: {service_name}")
        except Exception as e:
            error_msg = f"Unable to retrieve portal items for {name}"
            logger.error(f"{error_msg}: {e}", exc_info=True)
            result.add_error(f"Error processing portal items for {name}")

        # Parse the service manifest to extract additional configuration
        logger.debug(f"Retrieving service manifest for: {service_name}")
        try:
            service_manifest_json = service.service_manifest()
            service_manifest = json.loads(service_manifest_json)
            logger.debug(f"Successfully retrieved service manifest for: {service_name}")
        except Exception as e:
            logger.error(f"Error retrieving service manifest for {service_name}: {e}", exc_info=True)
            service_manifest = {"error": str(e)}
            result.add_error(f"Error retrieving service manifest for {service_name}")

        logger.debug(f"Preparing service data for database update: {service_name}")
        service_data = {
            "service_url": urls,
            "service_mxd_server": None,
            "service_mxd": map_name,
            "portal_id": portal_ids,
            "service_type": service_type,
            "service_description": description,
            "service_owner": owner,
            "service_access": access,
            "updated_date": update_time
        }
        logger.debug(f"Service data prepared for: {service_name}")
        # Service manifest not available for all hosted feature layers
        if service_manifest.get("code") == 500 and service_manifest.get(
            "status") == "error" and "FeatureServer" in portal_ids:
            logger.warning(f"No service manifest available for hosted feature layer: {name}")
            result.add_error(f"{name}: No Service Manifest")

            logger.debug(f"Creating or updating service record for hosted feature layer: {name}")
            s_obj, created = Service.objects.update_or_create(
                portal_instance=instance_item,
                service_name=name,
                defaults=service_data
            )

            if created:
                logger.info(f"Created new record for service: {name}")
                result.add_insert()
            else:
                logger.info(f"Updated existing record for service: {name}")
                result.add_update()

            # Attempt to get layers from Feature Layer instead
            feature_service_id = portal_ids["FeatureServer"]
            logger.debug(f"Retrieving feature service item: {feature_service_id}")

            try:
                service_item = target.content.get(feature_service_id)

                if service_item and hasattr(service_item, 'layers') and service_item.layers:
                    layer_count = len(service_item.layers)
                    logger.debug(f"Processing {layer_count} layers for feature service: {feature_service_id}")

                    for layer_index, layer in enumerate(service_item.layers):
                        layer_name = getattr(layer.properties, 'name', f"unknown_layer_{layer_index}")
                        logger.debug(f"Processing layer {layer_index+1}/{layer_count}: {layer_name}")

                        logger.debug(f"Creating or updating layer record for: {layer_name}")
                        obj, layer_created = Layer.objects.update_or_create(
                            portal_instance=instance_item,
                            layer_server="Hosted",
                            layer_version=None,
                            layer_database=None,
                            layer_name=layer.properties.name,
                            defaults={"updated_date": update_time}
                        )

                        if layer_created:
                            logger.debug(f"Created new layer record: {layer_name}")
                        else:
                            logger.debug(f"Updated existing layer record: {layer_name}")

                        logger.debug(f"Creating or updating layer-service relationship for: {layer_name}")
                        rel_obj, rel_created = Layer_Service.objects.update_or_create(
                            portal_instance=instance_item,
                            layer_id=obj,
                            service_id=s_obj,
                            defaults={"updated_date": update_time}
                        )

                        if rel_created:
                            logger.debug(f"Created new layer-service relationship for: {layer_name}")
                        else:
                            logger.debug(f"Updated existing layer-service relationship for: {layer_name}")
                else:
                    logger.debug(f"No layers found for feature service: {feature_service_id}")
            except Exception as e:
                logger.error(f"Error processing layers for feature service {name}: {e}", exc_info=True)
                result.add_error(f"Error processing layers for feature service {name}")
        else:
            if "resources" in service_manifest.keys():
                logger.debug(f"Processing resources from service manifest for: {name}")
                res_obj = service_manifest["resources"]
                resource_count = len(res_obj)
                logger.debug(f"Found {resource_count} resources in service manifest")

                for resource_index, obj in enumerate(res_obj):
                    logger.debug(f"Processing resource {resource_index+1}/{resource_count}")
                    mxd = f"{obj['onPremisePath']}\\{map_name}" if map_name else f"{obj['onPremisePath']}"
                    mxd_server = obj["clientName"]
                    logger.debug(f"Resource details - MXD: {mxd}, Server: {mxd_server}")
            else:
                logger.debug("No resources found in service manifest")
                mxd = None
                mxd_server = None

            logger.debug("Updating service data with MXD information")
            service_data["service_mxd"] = mxd
            service_data["service_mxd_server"] = mxd_server

            logger.debug(f"Creating or updating service record for: {name}")
            s_obj, created = Service.objects.update_or_create(
                portal_instance=instance_item,
                service_name=name,
                defaults=service_data
            )

            if created:
                logger.info(f"Created new record for service: {name}")
                result.add_insert()
            else:
                logger.info(f"Updated existing record for service: {name}")
                result.add_update()

            # First, try to process using MSD parser for more detailed layer information
            logger.debug("Attempting to process layers using MSD parser")
            msd_processed = utils.process_msd_layers_for_service(
                service_manifest, name, instance_item, s_obj, update_time
            )

            if msd_processed:
                logger.info(f"Successfully processed service '{name}' using MSD parser with layer id information")
            else:
                # Fallback to legacy process_databases if MSD parsing fails or is not available
                logger.info(
                    f"Service '{name}' processed without layer id information (MSD parsing unavailable). "
                )
                process_databases(service_manifest, regex_patterns, instance_item, s_obj, update_time)
            logger.debug("Database details processing completed")

    except Exception as e:
        logger.error(f"Unable to process service '{service_name}': {e}", exc_info=True)
        result.add_error(f"Unable to update {service_name}")
        return None

    logger.info(f"Successfully processed service '{service_name}'")
    return service_usage_str


@shared_task(bind=True, name="Update apps", time_limit=6000, soft_time_limit=3000)
@celery_logging_context
def update_webapps(self, instance_alias, full_refresh=False, credential_token=None):
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
    :param full_refresh: Flag indicating whether to perform a full refresh (delete all and re-fetch everything)
                         instead of incremental update based on items modified since last refresh timestamp.
    :type full_refresh: bool
    :param credential_token: Token for temporary credentials (optional)
    :type credential_token: str
    :return: JSON string summarizing the update results (inserts, updates, deletions, errors).
    :rtype: str
    """
    logger.debug(f"Starting update_webapps task for instance_alias={instance_alias}, full_refresh={full_refresh}")

    result = utils.UpdateResult()
    progress_recorder = ProgressRecorder(self)

    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        target = utils.connect(instance_item, credential_token)

    except Exception as e:
        logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
        result.add_error(f"Unable to connect to {instance_alias}")
        return {"result": result.to_json()}

    try:
        update_time = timezone.now()
        logger.debug(f"Update timestamp: {update_time}")

        # Retrieve web application items by combining multiple searches for various app types
        logger.debug("Retrieving web applications from portal")

        # Determine search query based on portal type
        if instance_item.portal_type == "agol":
            org_id = instance_item.org_id
            logger.debug(f"Using AGOL-specific query with org_id: {org_id}")
            query = f'orgid:{org_id} AND NOT owner:esri*'
        else:
            logger.debug("Using standard Portal for ArcGIS query")
            query = "NOT owner:esri*"

        # Add date filter for incremental updates if last run time exists
        if instance_item.webapp_updated and not full_refresh:
            # Format: modified:[timestamp TO timestamp] in milliseconds since epoch
            last_update_ms = int(instance_item.webapp_updated.timestamp() * 1000)
            query += f" AND modified:[{last_update_ms} TO {int(update_time.timestamp() * 1000)}]"
            logger.info(f"Incremental update: searching for webapps modified since {instance_item.webapp_updated}")
        else:
            logger.info(f"Full update: searching for all webapps")

        logger.debug(f"Search query: {query}")

        total_apps = target.content.advanced_search(
            query=query,
            max_items=-1,
            return_count=True,
            filter='type:"Web Mapping Application" OR type:"Dashboard" OR type:"StoryMap" OR type:"Web Experience" OR type:"Form" OR type:"QuickCapture Project" OR type:"Hub Site Application" OR type:"Hub Page"'
        )

        logger.info(f"Found {total_apps} web applications to process in portal '{instance_alias}'")

        if full_refresh:
            app_count = App.objects.filter(portal_instance=instance_item).count()
            App.objects.filter(portal_instance=instance_item).delete()
            logger.info(f"Deleted {app_count} existing web applications for portal '{instance_alias}'")


        # Set up batch processing
        batch_size = 20
        batch_tasks = []
        logger.debug(f"Setting up batch processing with batch size: {batch_size}")

        # Create batch tasks
        for batch in range(0, total_apps, batch_size):
            logger.debug(f"Creating batch task for items {batch} to {min(batch+batch_size, total_apps)}")
            batch_tasks.append(
                process_batch_apps.s(
                    instance_alias,
                    credential_token,
                    batch,
                    batch_size,
                    update_time,
                    query
                )
            )

        logger.info(f"Created {len(batch_tasks)} batch tasks for processing {total_apps} web applications")

        # Execute parallel processing by batch TODO comment about https://docs.celeryq.dev/en/stable/userguide/tasks.html#task-synchronous-subtasks
        logger.debug("Starting parallel batch processing")
        task_group = group(batch_tasks)
        batch_results = task_group.apply_async()
        logger.debug(f"Batch processing started with group ID: {batch_results.id}")

        # Monitor group progress
        logger.debug("Monitoring batch processing progress")
        while not batch_results.ready():
            try:
                completed_tasks = sum(
                    task.result.get("current", 0) if "current" in task.result else batch_size
                    for task in batch_results.children
                    if task.result
                )
                progress_percentage = (completed_tasks / total_apps) * 100
                logger.debug(f"Progress: {progress_percentage:.1f}% ({completed_tasks}/{total_apps} items)")
                progress_recorder.set_progress(completed_tasks, total_apps)
                time.sleep(1.5)
            except Exception as e:
                logger.warning(f"Error calculating progress: {e}")
                time.sleep(1.5)
                continue

        # Aggregate results
        logger.info(f"All batch tasks completed, aggregating results")
        logger.debug(f"Batch results: {batch_results}")

        success_count = 0
        failure_count = 0

        for batch in batch_results.get(disable_sync_subtasks=False):
            batch_result = utils.UpdateResult(**batch["result"])

            if batch_result.success is False:
                failure_count += 1
                result.success = False
                logger.warning(f"Batch task reported failure: {batch_result.error_messages}")
            else:
                success_count += 1

            # Aggregate counts
            result.num_updates += batch_result.num_updates
            result.num_inserts += batch_result.num_inserts
            result.num_deletes += batch_result.num_deletes
            result.num_errors += batch_result.num_errors
            result.error_messages.extend(batch_result.error_messages)

        logger.info(f"Batch processing summary: {success_count} successful batches, {failure_count} failed batches")


        logger.debug(f"Cleaning up outdated records for portal '{instance_alias}'")
        delete_outdated_records(instance_item, update_time, [App, App_Map, App_Service], result)
        logger.info(f"Outdated records cleanup completed with {result.num_deletes} deletions")

        instance_item.webapp_updated = timezone.now()
        instance_item.save()

        result.set_success()
        logger.info(f"Web applications update for portal '{instance_alias}' completed successfully")
        logger.debug(f"Final result: {result.to_json()}")
        return result.to_json()

    except Exception as e:
        logger.critical(f"Web applications update failed for portal '{instance_alias}': {e}", exc_info=True)
        result.add_error("Web applications update failed")

        # Revoke child tasks if they exist
        if batch_results and batch_results.children:
            logger.warning(f"Revoking {len(batch_results.children)} child tasks due to failure")
            for child in batch_results.children:
                if child.id:
                    logger.debug(f"Revoking child task: {child.id}")
                    current_app.control.revoke(child.id, terminate=True, signal="SIGKILL")
            logger.info("All child tasks revoked")

        # Update task state and exit
        self.update_state(state="FAILURE", meta=result.to_json())
        raise Ignore()


@shared_task(bind=True)
@celery_logging_context
def process_batch_apps(self, instance_alias, credential_token, batch, batch_size, update_time, query=None):
    result = utils.UpdateResult()
    progress_recorder = ProgressRecorder(self)
    try:
        try:
            instance_item = Portal.objects.get(alias=instance_alias)
            target = utils.connect(instance_item, credential_token)

        except Exception as e:
            logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
            result.add_error(f"Unable to connect to {instance_alias}")
            return {"result": result.to_json()}

        # Retrieve web applications for this batch
        logger.debug(f"Retrieving web applications for batch {batch} to {batch + batch_size}")

        # Use provided query or build default one
        if query is None:
            if instance_item.portal_type == "agol":
                org_id = instance_item.org_id
                logger.debug(f"Using AGOL-specific query with org_id: {org_id}")
                query = f'orgid:{org_id} AND NOT owner:esri*'
            else:
                logger.debug("Using standard Portal for ArcGIS query")
                query = "NOT owner:esri*"

        app_list = target.content.advanced_search(
            query=query,
            max_items=batch_size,
            start=batch,
            sort_field="title",
            sort_order="asc",
            filter='type:"Web Mapping Application" OR type:"Dashboard" OR type:"StoryMap" OR type:"Web Experience" OR type:"Form" OR type:"QuickCapture Project" OR type:"Hub Site Application" OR type:"Hub Page"'
        )

        results = app_list.get("results", [])
        total_apps = len(results)
        logger.info(f"Retrieved {total_apps} web applications for batch {batch} to {batch + batch_size}")

        # Process each web application in the batch
        logger.debug(f"Starting to process {total_apps} web applications")

        for counter, app in enumerate(results):
            app_id = getattr(app, 'id', 'unknown')
            app_title = getattr(app, 'title', 'unknown')
            logger.debug(f"Processing web application {counter+1}/{total_apps}: {app_id} - '{app_title}'")

            try:
                process_single_app(app, target, instance_item, update_time, result)
                logger.debug(f"Successfully processed web application: {app_id}")

                progress_percentage = ((counter + 1) / total_apps) * 100
                logger.debug(f"Progress: {progress_percentage:.1f}% ({counter+1}/{total_apps})")
                progress_recorder.set_progress(counter + 1, total_apps)

            except Exception as e:
                error_msg = f"Unable to process web application {app_id} - '{app_title}'"
                logger.error(f"{error_msg}: {e}", exc_info=True)
                result.add_error(f"Unable to update {app_id}")

        logger.info(f"Completed processing {total_apps} web applications in batch {batch} to {batch + batch_size}")
        logger.debug(f"Batch summary - Updates: {result.num_updates}, Inserts: {result.num_inserts}, Errors: {result.num_errors}")

        result.set_success()
        return {"result": result.to_json()}

    except Exception as e:
        logger.error(f"Error processing web apps in batch {batch} to {batch + batch_size}: {e}", exc_info=True)
        result.add_error(f"Error processing web apps in batch {batch} to {batch + batch_size}")
        return {"result": result.to_json()}


def process_single_app(item, target, instance_item, update_time, result):
    if result is None:
        result = utils.UpdateResult()

    app_id = getattr(item, 'id', 'unknown')
    app_title = getattr(item, 'title', 'unknown')
    logger.debug(f"Starting process_single_app for application: {app_id} - '{app_title}'")
    try:
        logger.debug(f"Application details: {item}")

        logger.debug(f"Extracting access information for application: {app_id}")
        access = get_access(item)
        logger.debug(f"Access for application {app_id}: {access}")

        logger.debug(f"Calculating usage statistics for application: {app_id}")
        usage = calculate_usage(item, instance_item)

        logger.debug(f"Retrieving owner for application: {app_id}")
        owner = get_owner(instance_item, item.owner)
        logger.debug(f"Owner for application {app_id}: {owner}")

        logger.debug(f"Preparing application data for database update: {app_id}")
        app_data = {
            "app_title": item.title,
            "app_url": item.homepage,
            "app_type": item.type,
            "app_owner": owner,
            "app_created": utils.epoch_to_datetime(item.created),
            "app_modified": utils.epoch_to_datetime(item.modified),
            "app_access": access,
            "app_extent": item.extent,
            "app_description": item.description,
            "app_views": item.numViews,
            "app_usage": usage,
            "updated_date": update_time,
            "app_last_viewed": utils.epoch_to_datetime(item.lastViewed) if instance_item.portal_type == "agol" else None
        }
        logger.debug(f"Application data prepared for: {app_id}")

        logger.debug(f"Creating or updating application record for: {app_id}")
        app_obj, created = App.objects.update_or_create(
            portal_instance=instance_item,
            app_id=item.id,
            defaults=app_data
        )

        if created:
            logger.info(f"Created new application record for: {app_id} - '{app_title}'")
            result.add_insert()
        else:
            logger.info(f"Updated existing application record for: {app_id} - '{app_title}'")
            result.add_update()

        logger.debug(f"Retrieving detailed data for application: {app_id}")
        try:
            data = item.get_data()
            logger.debug(f"Successfully retrieved detailed data for application: {app_id}")
        except Exception as e:
            logger.warning(f"Error retrieving detailed data for application {app_id}: {e}")
            result.add_error(f"Error retrieving detailed data for application {app_id}")
            data = {}

        logger.debug(f"Application type: {item.type}")

        if item.type == "Web Mapping Application":
            logger.debug(f"Processing Web Mapping Application: {app_id}")

            # Initialize webmap_obj for dependency resolution
            webmap_obj = None

            # Extract both itemIds and URLs
            logger.debug(f"Extracting web app builder resources for application: {app_id}")
            resources = utils.extract_webappbuilder(data)
            resource_count = len(resources)
            logger.debug(f"Found {resource_count} resources in Web Mapping Application")

            for resource_index, resource in enumerate(resources):
                logger.debug(f"Processing resource {resource_index + 1}/{resource_count}: {resource}")

                try:
                    if len(resource) == 5:
                        resource_value, path, parent_key, value_type, context_type = resource
                    else:
                        resource_value, path, parent_key, value_type = resource
                        context_type = "other"

                    logger.debug(
                        f"Resource: value={resource_value[:100] if isinstance(resource_value, str) else resource_value}, "
                        f"path={path}, key={parent_key}, type={value_type}, context={context_type}")

                    # Handle map references (itemId-based)
                    if context_type == "map" or (value_type == "map" and not isinstance(resource_value, str)):
                        logger.debug(f"Processing map reference: {resource_value}")

                        try:
                            map_obj = Webmap.objects.get(portal_instance=instance_item, webmap_id=resource_value)
                            logger.debug(f"Found web map: {map_obj}")

                            # Store webmap_obj for use by dependent resources like filter_layer_ref
                            webmap_obj = map_obj

                            logger.debug("Creating or updating app-map relationship")
                            _, rel_created = App_Map.objects.update_or_create(
                                portal_instance=instance_item,
                                app_id=app_obj,
                                webmap_id=map_obj,
                                rel_type="map",
                                defaults={"updated_date": update_time}
                            )

                            if rel_created:
                                logger.debug("Created new app-map relationship")
                            else:
                                logger.debug("Updated existing app-map relationship")

                        except Webmap.DoesNotExist:
                            logger.warning(f"Map {resource_value} referenced by application {app_id} does not exist")
                            result.add_error(f"{item.title}: map {resource_value} does not exist")

                    # Handle data source references (itemId-based)
                    elif context_type == "datasource" and value_type != "url":
                        logger.debug(f"Processing data source reference: {resource_value}")

                        # Fetch the item to determine its type
                        try:
                            ds_item = target.content.get(resource_value)

                            if ds_item is None:
                                logger.warning(f"Data source item {resource_value} not found or inaccessible, skipping")
                                continue

                            logger.debug(f"Found data source item: {ds_item.title} (Type: {ds_item.type})")

                            if ds_item.type == "Web Map":
                                try:
                                    map_obj = Webmap.objects.get(portal_instance=instance_item,
                                                                 webmap_id=resource_value)
                                    logger.debug(f"Found web map: {map_obj}")

                                    # Store webmap_obj for use by dependent resources like filter_layer_ref
                                    if webmap_obj is None:
                                        webmap_obj = map_obj

                                    _, rel_created = App_Map.objects.update_or_create(
                                        portal_instance=instance_item,
                                        app_id=app_obj,
                                        webmap_id=map_obj,
                                        rel_type="datasource",
                                        defaults={"updated_date": update_time}
                                    )

                                    if rel_created:
                                        logger.debug("Created new app-map relationship for data source")
                                    else:
                                        logger.debug("Updated existing app-map relationship for data source")

                                except Webmap.DoesNotExist:
                                    logger.warning(f"Map {resource_value} referenced as data source does not exist")
                                    result.add_error(f"Map {resource_value} referenced as data source does not exist")

                            elif ds_item.type in ["Feature Service", "Map Service", "Feature Layer", "Map Image Layer"]:
                                url = ds_item.url
                                logger.debug(f"Data source is a service: {url}")

                                try:
                                    service_obj = Service.objects.get(service_url__overlap=[url])
                                    logger.debug(f"Found service: {service_obj}")

                                    _, rel_created = App_Service.objects.update_or_create(
                                        portal_instance=instance_item,
                                        app_id=app_obj,
                                        service_id=service_obj,
                                        rel_type="datasource",
                                        defaults={"updated_date": update_time}
                                    )

                                    if rel_created:
                                        logger.debug("Created new app-service relationship for data source")
                                    else:
                                        logger.debug("Updated existing app-service relationship for data source")

                                except Service.DoesNotExist:
                                    logger.warning(f"Service with URL {url} not found, skipping")
                                    continue
                                except MultipleObjectsReturned:
                                    logger.warning(f"Multiple services found with URL {url}, using first match")
                                    service_obj = Service.objects.filter(service_url__overlap=[url]).first()

                                    _, rel_created = App_Service.objects.update_or_create(
                                        portal_instance=instance_item,
                                        app_id=app_obj,
                                        service_id=service_obj,
                                        rel_type="datasource",
                                        defaults={"updated_date": update_time}
                                    )
                                    if rel_created:
                                        logger.debug("Created new app-service relationship for data source")
                                    else:
                                        logger.debug("Updated existing app-service relationship for data source")

                        except Exception as e:
                            logger.error(f"Error retrieving data source {resource_value}: {e}", exc_info=True)
                            result.add_error(f"Error retrieving data source {resource_value}")

                    # Handle URL-based references (services)
                    elif value_type == "url":
                        logger.debug(f"Processing URL reference: {resource_value}")

                        # Normalize URL (remove layer index if present)
                        url = resource_value if not resource_value.split("/")[-1].isdigit() else "/".join(
                            resource_value.split("/")[:-1])
                        logger.debug(f"Normalized URL: {url}")

                        # Use the context from parsing
                        rel_type = context_type if context_type in ["search", "filter", "widget",
                                                                    "datasource"] else "other"
                        logger.debug(f"Relationship type: {rel_type}")

                        try:
                            service_obj = Service.objects.get(service_url__overlap=[url])
                            logger.debug(f"Found service: {service_obj}")

                            logger.debug("Creating or updating app-service relationship")
                            _, rel_created = App_Service.objects.update_or_create(
                                portal_instance=instance_item,
                                app_id=app_obj,
                                service_id=service_obj,
                                rel_type=rel_type,
                                defaults={"updated_date": update_time}
                            )

                            if rel_created:
                                logger.debug(f"Created new app-service relationship with type '{rel_type}'")
                            else:
                                logger.debug("Updated existing app-service relationship")

                        except Service.DoesNotExist:
                            logger.warning(f"Service with URL {url} not found, skipping resource")
                            continue
                        except MultipleObjectsReturned:
                            logger.warning(f"Multiple services found with URL {url}, using first match")
                            service_obj = Service.objects.filter(service_url__overlap=[url]).first()
                            logger.debug(f"Selected service: {service_obj}")

                            _, rel_created = App_Service.objects.update_or_create(
                                portal_instance=instance_item,
                                app_id=app_obj,
                                service_id=service_obj,
                                rel_type=rel_type,
                                defaults={"updated_date": update_time}
                            )

                            if rel_created:
                                logger.debug(f"Created new app-service relationship with type '{rel_type}'")
                            else:
                                logger.debug("Updated existing app-service relationship")

                    # Handle filter layer references (layer ID from map)
                    elif value_type == "filter_layer_ref":
                        logger.debug(f"Processing filter layer reference: {resource_value}")

                        if not webmap_obj:
                            logger.warning(f"Cannot resolve filter layer {resource_value}: web map not yet loaded")
                            # Try to get the web map from the data
                            try:
                                webmap_id = data.get("values", {}).get("webmap")
                                if webmap_id:
                                    webmap_obj = Webmap.objects.get(portal_instance=instance_item, webmap_id=webmap_id)
                            except (Webmap.DoesNotExist, KeyError, AttributeError):
                                logger.error("Unable to find web map for filter layer resolution")
                                continue

                        # Look up the service using the webmap_layer_id
                        try:
                            # Find the Map_Service record that has this webmap_layer_id
                            map_service = Map_Service.objects.get(
                                portal_instance=instance_item,
                                webmap_id=webmap_obj,
                                webmap_layer_id=resource_value  # e.g., '191e2c1dee3-layer-3'
                            )

                            service_obj = map_service.service_id
                            service_layer_id = map_service.service_layer_id

                            logger.debug(
                                f"Resolved filter layer {resource_value} to service {service_obj} (layer {service_layer_id})")

                            # Now you can link to the specific Layer_Service if you want
                            if service_layer_id is not None:
                                try:
                                    # Link to the specific layer in the service
                                    layer_service = Layer_Service.objects.get(
                                        portal_instance=instance_item,
                                        service_id=service_obj,
                                        service_layer_id=service_layer_id
                                    )

                                    logger.debug(f"Found Layer_Service: {layer_service.service_layer_name}")

                                    # Create app-service relationship with layer detail
                                    _, rel_created = App_Service.objects.update_or_create(
                                        portal_instance=instance_item,
                                        app_id=app_obj,
                                        service_id=service_obj,
                                        rel_type="filter",
                                        defaults={
                                            "updated_date": update_time,
                                            "service_layer_id": service_layer_id  # Store which layer
                                        }
                                    )

                                    if rel_created:
                                        logger.info(
                                            f"Created app-service filter relationship: {app_obj.app_title} filters {layer_service.service_layer_name}")
                                    else:
                                        logger.debug("Updated app-service filter relationship")

                                except Layer_Service.DoesNotExist:
                                    logger.warning(
                                        f"Layer_Service not found for service {service_obj}, layer {service_layer_id}")

                                    # Still create the service relationship without layer detail
                                    _, rel_created = App_Service.objects.update_or_create(
                                        portal_instance=instance_item,
                                        app_id=app_obj,
                                        service_id=service_obj,
                                        rel_type="filter",
                                        defaults={"updated_date": update_time}
                                    )
                                    if rel_created:
                                        logger.debug(f"Created new app-service relationship with type 'filter'")
                                    else:
                                        logger.debug("Updated existing app-service relationship")
                            else:
                                # No specific layer ID, link to service only
                                _, rel_created = App_Service.objects.update_or_create(
                                    portal_instance=instance_item,
                                    app_id=app_obj,
                                    service_id=service_obj,
                                    rel_type="filter",
                                    defaults={"updated_date": update_time}
                                )
                                if rel_created:
                                    logger.debug(f"Created new app-service relationship with type 'filter'")
                                else:
                                    logger.debug("Updated existing app-service relationship")

                        except Map_Service.DoesNotExist:
                            logger.warning(
                                f"Filter layer {resource_value} not found in Map_Service for web map {webmap_obj.webmap_id}")
                            result.add_error(f"Filter layer {resource_value} not found in map")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing filter layer reference: {e}", exc_info=True)
                            result.add_error(f"Error processing filter layer {resource_value}")

                except Exception as e:
                    logger.error(f"Error processing resource {resource} for application {app_id}: {e}", exc_info=True)
                    result.add_error(f"Error processing resource {resource} for application {app_id}")

        if item.type == "StoryMap":
            logger.debug(f"Processing StoryMap application: {app_id}")

            logger.debug(f"Extracting StoryMap resources for application: {app_id}")
            # Pass the item object so we can access draft data
            resources = utils.extract_storymap(data, item=item)
            resource_count = len(resources)
            logger.debug(f"Found {resource_count} resources in StoryMap")

            for resource_index, resource in enumerate(resources):
                path, context_type, resource_id, item_type = resource
                logger.debug(f"Processing StoryMap resource {resource_index + 1}/{resource_count}: "
                             f"Type={item_type}, Context={context_type}, ID={resource_id}")

                try:
                    if context_type == "map" or item_type in ["Web Map", "webmap"]:
                        logger.debug(f"Processing web map resource: {resource_id}")

                        try:
                            map_obj = Webmap.objects.get(webmap_id=resource_id)
                            logger.debug(f"Found web map: {map_obj}")

                            logger.debug(f"Creating or updating app-map relationship for map: {resource_id}")
                            _, rel_created = App_Map.objects.update_or_create(
                                portal_instance=instance_item,
                                app_id=app_obj,
                                webmap_id=map_obj,
                                rel_type="storymap",  # Specific to StoryMap embeds
                                defaults={"updated_date": update_time}
                            )

                            if rel_created:
                                logger.debug("Created new app-map relationship for StoryMap")
                            else:
                                logger.debug("Updated existing app-map relationship for StoryMap")

                        except Webmap.DoesNotExist:
                            logger.warning(f"Map {resource_id} referenced by StoryMap {app_id} does not exist")
                            result.add_error(f"Map {resource_id} referenced by StoryMap {app_id} does not exist")

                    elif context_type == "theme":
                        logger.debug(f"Processing StoryMap theme resource: {resource_id}")
                        # StoryMap themes are typically styling/layout templates
                        # You might want to track these separately or log them
                        logger.info(f"StoryMap {app_id} uses theme {resource_id}")

                    else:
                        logger.debug(f"Processing embedded resource: {resource_id}, type={item_type}")

                        # Fetch the item to determine its actual type
                        try:
                            story_resource = target.content.get(resource_id)

                            if story_resource is None:
                                logger.warning(f"StoryMap resource {resource_id} not found or inaccessible, skipping")
                                continue

                            logger.debug(
                                f"Found embedded resource: {story_resource.title} (Type: {story_resource.type})")

                            # Handle services embedded in StoryMaps
                            if story_resource.type in ["Feature Service", "Map Service", "Feature Layer",
                                                       "Map Image Layer"]:
                                logger.debug(f"Processing service resource: {story_resource.url}")

                                try:
                                    service_obj = Service.objects.get(service_url__overlap=[story_resource.url])
                                    logger.debug(f"Found service: {service_obj}")

                                    logger.debug("Creating or updating app-service relationship")
                                    _, rel_created = App_Service.objects.update_or_create(
                                        portal_instance=instance_item,
                                        service_id=service_obj,
                                        app_id=app_obj,
                                        rel_type="embed",  # Embedded in StoryMap content
                                        defaults={"updated_date": update_time}
                                    )

                                    if rel_created:
                                        logger.debug("Created new app-service relationship for StoryMap embed")
                                    else:
                                        logger.debug("Updated existing app-service relationship for StoryMap embed")

                                except Service.DoesNotExist:
                                    logger.warning(
                                        f"Service with URL {story_resource.url} not found, skipping resource")
                                    continue
                                except MultipleObjectsReturned:
                                    logger.warning(
                                        f"Multiple services found with URL {story_resource.url}, using first match")
                                    service_obj = Service.objects.filter(
                                        service_url__overlap=[story_resource.url]).first()
                                    logger.debug(f"Selected service: {service_obj}")

                                    _, rel_created = App_Service.objects.update_or_create(
                                        portal_instance=instance_item,
                                        service_id=service_obj,
                                        app_id=app_obj,
                                        rel_type="embed",
                                        defaults={"updated_date": update_time}
                                    )

                                    if rel_created:
                                        logger.debug("Created new app-service relationship for StoryMap embed")
                                    else:
                                        logger.debug("Updated existing app-service relationship for StoryMap embed")

                            elif story_resource.type == "Web Map":
                                # Caught by fallback - handle as map
                                logger.debug(f"Processing fallback web map resource: {resource_id}")

                                try:
                                    map_obj = Webmap.objects.get(webmap_id=resource_id)
                                    logger.debug(f"Found web map: {map_obj}")

                                    _, rel_created = App_Map.objects.update_or_create(
                                        portal_instance=instance_item,
                                        app_id=app_obj,
                                        webmap_id=map_obj,
                                        rel_type="storymap",
                                        defaults={"updated_date": update_time}
                                    )

                                    if rel_created:
                                        logger.debug("Created new app-map relationship for StoryMap")
                                    else:
                                        logger.debug("Updated existing app-map relationship for StoryMap")

                                except Webmap.DoesNotExist:
                                    logger.warning(f"Map {resource_id} referenced by StoryMap {app_id} does not exist")
                                    result.add_error(
                                        f"Map {resource_id} referenced by StoryMap {app_id} does not exist")

                            else:
                                logger.debug(
                                    f"Resource type {story_resource.type} not specifically handled in StoryMap, skipping")

                        except Exception as e:
                            logger.error(f"Error retrieving embedded resource {resource_id} for StoryMap {app_id}: {e}",
                                         exc_info=True)
                            result.add_error(f"Error retrieving embedded resource {resource_id} for StoryMap {app_id}")

                except Exception as e:
                    logger.error(f"Error processing StoryMap resource {resource} for application {app_id}: {e}",
                                 exc_info=True)
                    result.add_error(f"Error processing StoryMap resource {resource} for application {app_id}")

        if item.type == "Dashboard":
            logger.debug(f"Processing Dashboard application: {app_id}")

            logger.debug(f"Extracting Dashboard resources for application: {app_id}")
            resources = utils.extract_dashboard(data)
            resource_count = len(resources)
            logger.debug(f"Found {resource_count} resources in Dashboard")

            for resource_index, resource in enumerate(resources):
                logger.debug(f"Processing Dashboard resource {resource_index + 1}/{resource_count}: {resource}")

                try:
                    path, context_type, resource_id, widget_type = resource
                    logger.debug(
                        f"Resource: path={path}, context={context_type}, id={resource_id}, widget={widget_type}")

                    try:
                        dashboard_resource = target.content.get(resource_id)

                        if dashboard_resource is None:
                            logger.warning(f"Dashboard resource {resource_id} not found or inaccessible, skipping")
                            continue

                        logger.debug(
                            f"Found resource item: {dashboard_resource.title} (Type: {dashboard_resource.type})")

                        if dashboard_resource.type in ["Map Image Layer", "Feature Layer", "Feature Service"]:
                            logger.debug(f"Processing service resource: {dashboard_resource.url}")

                            try:
                                service_obj = Service.objects.get(service_url__overlap=[dashboard_resource.url])
                                logger.debug(f"Found service: {service_obj}")

                                logger.debug("Creating or updating app-service relationship")
                                _, rel_created = App_Service.objects.update_or_create(
                                    portal_instance=instance_item,
                                    app_id=app_obj,
                                    service_id=service_obj,
                                    rel_type=context_type,  # Use extracted context: "dataset", "arcade", "other"
                                    defaults={"updated_date": update_time}
                                )

                                if rel_created:
                                    logger.debug(f"Created new app-service relationship with type '{context_type}'")
                                else:
                                    logger.debug("Updated existing app-service relationship")

                            except Service.DoesNotExist:
                                logger.warning(
                                    f"Service with URL {dashboard_resource.url} not found, skipping resource")
                                continue
                            except MultipleObjectsReturned:
                                logger.warning(
                                    f"Multiple services found with URL {dashboard_resource.url}, using first match")
                                service_obj = Service.objects.filter(
                                    service_url__overlap=[dashboard_resource.url]).first()
                                logger.debug(f"Selected service: {service_obj}")

                                _, rel_created = App_Service.objects.update_or_create(
                                    portal_instance=instance_item,
                                    app_id=app_obj,
                                    service_id=service_obj,
                                    rel_type=context_type,
                                    defaults={"updated_date": update_time}
                                )

                                if rel_created:
                                    logger.debug(f"Created new app-service relationship with type '{context_type}'")
                                else:
                                    logger.debug("Updated existing app-service relationship")

                        elif dashboard_resource.type == "Web Map":
                            map_id = dashboard_resource.id
                            logger.debug(f"Processing web map resource: {map_id}")

                            try:
                                map_obj = Webmap.objects.get(webmap_id=map_id)
                                logger.debug(f"Found web map: {map_obj}")

                                logger.debug(f"Creating or updating app-map relationship for map: {map_id}")
                                _, rel_created = App_Map.objects.update_or_create(
                                    portal_instance=instance_item,
                                    app_id=app_obj,
                                    webmap_id=map_obj,
                                    rel_type=context_type,  # Will be "map" for mapWidget
                                    defaults={"updated_date": update_time}
                                )

                                if rel_created:
                                    logger.debug(f"Created new app-map relationship with type '{context_type}'")
                                else:
                                    logger.debug("Updated existing app-map relationship")

                            except Webmap.DoesNotExist:
                                logger.warning(f"Map {map_id} referenced by Dashboard {app_id} does not exist")
                                result.add_error(f"Map {map_id} referenced by Dashboard {app_id} does not exist")
                        else:
                            logger.debug(f"Resource type {dashboard_resource.type} not supported, skipping")

                    except Exception as e:
                        logger.error(f"Error retrieving resource {resource_id} for Dashboard {app_id}: {e}",
                                     exc_info=True)
                        result.add_error(f"Error retrieving resource {resource_id} for Dashboard {app_id}")

                except Exception as e:
                    logger.error(f"Error processing Dashboard resource {resource} for application {app_id}: {e}",
                                 exc_info=True)
                    result.add_error(f"Error processing Dashboard resource {resource} for application {app_id}")

        if item.type == "Form":
            logger.debug(f"Processing Form application: {app_id}")

            logger.debug(f"Retrieving related Survey2Service items for Form: {app_id}")
            try:
                related_items = item.related_items("Survey2Service", "forward")
                resource_count = len(related_items)
                logger.debug(f"Found {resource_count} related Survey2Service items")

                for resource_index, resource in enumerate(related_items):
                    resource_url = getattr(resource, 'url', 'unknown')
                    logger.debug(f"Processing Survey2Service resource {resource_index+1}/{resource_count}: {resource_url}")

                    try:
                        logger.debug(f"Form has service {resource_url}")

                        try:
                            service_obj = Service.objects.get(service_url__overlap=[resource_url])
                            logger.debug(f"Found service: {service_obj}")

                            logger.debug("Creating or updating app-service relationship")
                            _, rel_created = App_Service.objects.update_or_create(
                                portal_instance=instance_item,
                                app_id=app_obj,
                                service_id=service_obj,
                                defaults={"updated_date": update_time}
                            )

                            if rel_created:
                                logger.debug("Created new app-service relationship")
                            else:
                                logger.debug("Updated existing app-service relationship")

                        except Service.DoesNotExist:
                            logger.warning(f"Service with URL {resource_url} not found, skipping resource")
                            continue
                        except MultipleObjectsReturned:
                            logger.warning(f"Multiple services found with URL {resource_url}, using first match")
                            service_obj = Service.objects.filter(service_url__overlap=[resource_url]).first()
                            logger.debug(f"Selected service: {service_obj}")

                            logger.debug("Creating or updating app-service relationship")
                            _, rel_created = App_Service.objects.update_or_create(
                                portal_instance=instance_item,
                                app_id=app_obj,
                                service_id=service_obj,
                                defaults={"updated_date": update_time}
                            )

                            if rel_created:
                                logger.debug("Created new app-service relationship")
                            else:
                                logger.debug("Updated existing app-service relationship")

                    except Exception as e:
                        logger.error(f"Error processing Survey2Service resource {resource_url} for Form {app_id}: {e}", exc_info=True)
                        result.add_error(f"Error processing Survey2Service resource {resource_url} for Form {app_id}")

            except Exception as e:
                logger.error(f"Error retrieving related Survey2Service items for Form {app_id}: {e}", exc_info=True)
                result.add_error(f"Error retrieving related Survey2Service items for Form {app_id}")

        if item.type == "Web Experience":
            logger.debug(f"Processing Web Experience (Experience Builder) application: {app_id}")

            logger.debug(f"Extracting Experience Builder resources for application: {app_id}")
            # Pass the item object so we can access draft data
            resources = utils.extract_experiencebuilder(data, item=item)
            resource_count = len(resources)
            logger.debug(f"Found {resource_count} resources in Experience Builder")

            for resource_index, resource in enumerate(resources):
                path, context_type, resource_value, resource_type = resource
                logger.debug(f"Processing Experience Builder resource {resource_index + 1}/{resource_count}: "
                             f"Type={resource_type}, Context={context_type}, Value={resource_value[:50]}...")

                try:
                    # Determine if this is an itemId or URL
                    is_url = isinstance(resource_value, str) and resource_value.startswith("http")

                    if is_url:
                        # Handle URL-based resources (services)
                        logger.debug(f"Processing URL-based resource: {resource_value}")

                        try:
                            service_obj = Service.objects.get(service_url__overlap=[resource_value])
                            logger.debug(f"Found service: {service_obj}")

                            logger.debug("Creating or updating app-service relationship")
                            _, rel_created = App_Service.objects.update_or_create(
                                portal_instance=instance_item,
                                service_id=service_obj,
                                app_id=app_obj,
                                rel_type=context_type,  # "datasource", "other"
                                defaults={"updated_date": update_time}
                            )

                            if rel_created:
                                logger.debug(f"Created new app-service relationship with type '{context_type}'")
                            else:
                                logger.debug("Updated existing app-service relationship")

                        except Service.DoesNotExist:
                            logger.warning(f"Service with URL {resource_value} not found, skipping resource")
                            continue
                        except MultipleObjectsReturned:
                            logger.warning(f"Multiple services found with URL {resource_value}, using first match")
                            service_obj = Service.objects.filter(service_url__overlap=[resource_value]).first()
                            logger.debug(f"Selected service: {service_obj}")

                            _, rel_created = App_Service.objects.update_or_create(
                                portal_instance=instance_item,
                                service_id=service_obj,
                                app_id=app_obj,
                                rel_type=context_type,
                                defaults={"updated_date": update_time}
                            )

                            if rel_created:
                                logger.debug(f"Created new app-service relationship with type '{context_type}'")
                            else:
                                logger.debug("Updated existing app-service relationship")

                    else:
                        # Handle itemId-based resources
                        resource_id = resource_value
                        logger.debug(f"Processing itemId-based resource: {resource_id}")

                        # Fetch the item to determine its type
                        try:
                            exb_resource = target.content.get(resource_id)

                            if exb_resource is None:
                                logger.warning(f"Experience Builder resource {resource_id} not found or inaccessible, skipping")
                                continue

                            logger.debug(f"Found resource item: {exb_resource.title} (Type: {exb_resource.type})")

                            # Handle different resource types
                            if exb_resource.type == "Web Map":
                                logger.debug(f"Processing web map resource: {resource_id}")

                                try:
                                    map_obj = Webmap.objects.get(webmap_id=resource_id)
                                    logger.debug(f"Found web map: {map_obj}")

                                    # Use context_type: "map", "datasource", "widget", etc.
                                    map_context = context_type if context_type in ["map", "datasource",
                                                                                   "widget"] else "map"

                                    logger.debug(f"Creating or updating app-map relationship for map: {resource_id}")
                                    _, rel_created = App_Map.objects.update_or_create(
                                        portal_instance=instance_item,
                                        webmap_id=map_obj,
                                        app_id=app_obj,
                                        rel_type=map_context,
                                        defaults={"updated_date": update_time}
                                    )

                                    if rel_created:
                                        logger.debug(f"Created new app-map relationship with type '{map_context}'")
                                    else:
                                        logger.debug("Updated existing app-map relationship")

                                except Webmap.DoesNotExist:
                                    logger.warning(
                                        f"Map {resource_id} referenced by Experience Builder {app_id} does not exist")
                                    result.add_error(
                                        f"Map {resource_id} referenced by Experience Builder {app_id} does not exist")

                            elif exb_resource.type in ["Feature Service", "Map Service", "Feature Layer",
                                                       "Map Image Layer"]:
                                logger.debug(f"Processing service resource: {exb_resource.url}")

                                try:
                                    service_obj = Service.objects.get(service_url__overlap=[exb_resource.url])
                                    logger.debug(f"Found service: {service_obj}")

                                    logger.debug("Creating or updating app-service relationship")
                                    _, rel_created = App_Service.objects.update_or_create(
                                        portal_instance=instance_item,
                                        service_id=service_obj,
                                        app_id=app_obj,
                                        rel_type=context_type,  # "datasource", "widget", "other"
                                        defaults={"updated_date": update_time}
                                    )

                                    if rel_created:
                                        logger.debug(f"Created new app-service relationship with type '{context_type}'")
                                    else:
                                        logger.debug("Updated existing app-service relationship")

                                except Service.DoesNotExist:
                                    logger.warning(f"Service with URL {exb_resource.url} not found, skipping resource")
                                    continue
                                except MultipleObjectsReturned:
                                    logger.warning(
                                        f"Multiple services found with URL {exb_resource.url}, using first match")
                                    service_obj = Service.objects.filter(
                                        service_url__overlap=[exb_resource.url]).first()
                                    logger.debug(f"Selected service: {service_obj}")

                                    _, rel_created = App_Service.objects.update_or_create(
                                        portal_instance=instance_item,
                                        service_id=service_obj,
                                        app_id=app_obj,
                                        rel_type=context_type,
                                        defaults={"updated_date": update_time}
                                    )

                                    if rel_created:
                                        logger.debug(f"Created new app-service relationship with type '{context_type}'")
                                    else:
                                        logger.debug("Updated existing app-service relationship")

                            elif exb_resource.type == "Form":
                                logger.debug(f"Processing Survey123 form resource: {resource_id}")

                                # Get the form's related services (same pattern as standalone Form processing)
                                logger.debug(
                                    f"Retrieving related Survey2Service items for embedded Form: {resource_id}")
                                try:
                                    related_items = exb_resource.related_items("Survey2Service", "forward")
                                    form_service_count = len(related_items)
                                    logger.debug(
                                        f"Found {form_service_count} related Survey2Service items for form {resource_id}")

                                    for svc_index, form_service in enumerate(related_items):
                                        form_service_url = getattr(form_service, 'url', 'unknown')
                                        logger.debug(
                                            f"Processing Survey2Service resource {svc_index + 1}/{form_service_count}: {form_service_url}")

                                        try:
                                            logger.debug(f"Embedded form has service {form_service_url}")

                                            try:
                                                service_obj = Service.objects.get(
                                                    service_url__overlap=[form_service_url])
                                                logger.debug(f"Found service: {service_obj}")

                                                # Use context_type "survey" to indicate this came from an embedded form
                                                logger.debug("Creating or updating app-service relationship")
                                                _, rel_created = App_Service.objects.update_or_create(
                                                    portal_instance=instance_item,
                                                    app_id=app_obj,
                                                    service_id=service_obj,
                                                    rel_type="survey",  # Use "survey" context for form-related services
                                                    defaults={"updated_date": update_time}
                                                )

                                                if rel_created:
                                                    logger.debug(
                                                        "Created new app-service relationship for embedded form service")
                                                else:
                                                    logger.debug(
                                                        "Updated existing app-service relationship for embedded form service")

                                            except Service.DoesNotExist:
                                                logger.warning(
                                                    f"Service with URL {form_service_url} not found, skipping resource")
                                                continue
                                            except MultipleObjectsReturned:
                                                logger.warning(
                                                    f"Multiple services found with URL {form_service_url}, using first match")
                                                service_obj = Service.objects.filter(
                                                    service_url__overlap=[form_service_url]).first()
                                                logger.debug(f"Selected service: {service_obj}")

                                                _, rel_created = App_Service.objects.update_or_create(
                                                    portal_instance=instance_item,
                                                    app_id=app_obj,
                                                    service_id=service_obj,
                                                    rel_type="survey",
                                                    defaults={"updated_date": update_time}
                                                )

                                                if rel_created:
                                                    logger.debug(
                                                        "Created new app-service relationship for embedded form service")
                                                else:
                                                    logger.debug(
                                                        "Updated existing app-service relationship for embedded form service")

                                        except Exception as e:
                                            logger.error(
                                                f"Error processing Survey2Service resource {form_service_url} for embedded Form {resource_id}: {e}",
                                                exc_info=True)
                                            result.add_error(
                                                f"Error processing Survey2Service resource {form_service_url} for embedded Form {resource_id}")

                                except Exception as e:
                                    logger.error(
                                        f"Error retrieving related Survey2Service items for embedded Form {resource_id}: {e}",
                                        exc_info=True)
                                    result.add_error(
                                        f"Error retrieving related Survey2Service items for embedded Form {resource_id}")

                            else:
                                logger.debug(f"Resource type {exb_resource.type} not specifically handled, skipping")

                        except Exception as e:
                            logger.error(
                                f"Error retrieving resource {resource_id} for Experience Builder {app_id}: {e}",
                                exc_info=True)
                            result.add_error(f"Error retrieving resource {resource_id} for Experience Builder {app_id}")

                except Exception as e:
                    logger.error(
                        f"Error processing Experience Builder resource {resource} for application {app_id}: {e}",
                        exc_info=True)
                    result.add_error(
                        f"Error processing Experience Builder resource {resource} for application {app_id}")

        if item.type == "QuickCapture Project":
            logger.debug(f"Processing QuickCapture Project: {app_id}")

            resources = utils.extract_quickcapture(data, item=item)
            resource_count = len(resources)
            logger.debug(f"Found {resource_count} resources in QuickCapture Project")

            for resource_index, resource in enumerate(resources):
                path, context_type, resource_id, resource_type = resource
                logger.debug(f"Processing QuickCapture resource {resource_index + 1}/{resource_count}: "
                             f"Type={resource_type}, Context={context_type}")

                try:
                    if context_type == "map":
                        # Handle basemap
                        try:
                            map_obj = Webmap.objects.get(webmap_id=resource_id)
                            _, rel_created = App_Map.objects.update_or_create(
                                portal_instance=instance_item,
                                app_id=app_obj,
                                webmap_id=map_obj,
                                rel_type="map",
                                defaults={"updated_date": update_time}
                            )
                            if rel_created:
                                logger.debug("Created new app-map relationship")
                            else:
                                logger.debug("Updated existing app-map relationship")

                        except Webmap.DoesNotExist:
                            logger.warning(f"Map {resource_id} referenced by QuickCapture does not exist")
                            result.add_error(f"Map {resource_id} referenced by QuickCapture does not exist")

                    elif context_type == "datasource":
                        # Handle feature service data sources
                        try:
                            qc_resource = target.content.get(resource_id)

                            if qc_resource is None:
                                logger.warning(f"QuickCapture resource {resource_id} not found or inaccessible, skipping")
                                continue

                            if qc_resource.type in ["Feature Service", "Feature Layer"]:
                                service_obj = Service.objects.get(service_url__overlap=[qc_resource.url])
                                _, rel_created = App_Service.objects.update_or_create(
                                    portal_instance=instance_item,
                                    app_id=app_obj,
                                    service_id=service_obj,
                                    rel_type="datasource",
                                    defaults={"updated_date": update_time}
                                )
                                if rel_created:
                                    logger.debug("Created new app-service relationship with type 'datasource'")
                                else:
                                    logger.debug("Updated existing app-service relationship")
                        except Service.DoesNotExist:
                            logger.warning("Service for QuickCapture data source not found")
                        except Exception as e:
                            logger.error(f"Error processing QuickCapture data source: {e}")

                except Exception as e:
                    logger.error(f"Error processing QuickCapture resource: {e}")

        if item.type in ["Hub Site Application", "Hub Page"]:
            logger.debug(f"Processing {item.type}: {app_id}")

            resources = utils.extract_hub(data, item=item)
            resource_count = len(resources)
            logger.debug(f"Found {resource_count} resources in Hub")

            for resource_index, resource in enumerate(resources):
                path, context_type, resource_id, resource_type = resource
                logger.debug(f"Processing Hub resource {resource_index + 1}/{resource_count}: "
                             f"Type={resource_type}, Context={context_type}")

                try:
                    if context_type in ["map"]:
                        # Handle web maps and web scenes
                        try:
                            map_obj = Webmap.objects.get(webmap_id=resource_id)
                            _, rel_created = App_Map.objects.update_or_create(
                                portal_instance=instance_item,
                                app_id=app_obj,
                                webmap_id=map_obj,
                                rel_type="embed",
                                defaults={"updated_date": update_time}
                            )
                            if rel_created:
                                logger.debug("Created new app-map relationship with type 'embed'")
                            else:
                                logger.debug("Updated existing app-map relationship")
                        except Webmap.DoesNotExist:
                            logger.warning(f"Map {resource_id} referenced by Hub does not exist")

                    elif context_type == "app":
                        # Hub references other apps - you might want to track this
                        logger.info(f"Hub {app_id} references app {resource_id}")
                        # Optional: Create App_App relationship if tracking app-to-app dependencies

                    elif context_type == "survey":
                        # Handle Survey123 forms (same as Experience Builder)
                        try:
                            survey_resource = target.content.get(resource_id)

                            if survey_resource is None:
                                logger.warning(f"Survey resource {resource_id} not found or inaccessible, skipping")
                                continue

                            related_items = survey_resource.related_items("Survey2Service", "forward")
                            for form_service in related_items:
                                service_obj = Service.objects.get(service_url__overlap=[form_service.url])
                                _, rel_created = App_Service.objects.update_or_create(
                                    portal_instance=instance_item,
                                    app_id=app_obj,
                                    service_id=service_obj,
                                    rel_type="survey",
                                    defaults={"updated_date": update_time}
                                )
                                if rel_created:
                                    logger.debug("Created new app-service relationship with type 'survey'")
                                else:
                                    logger.debug("Updated existing app-service relationship")
                        except Exception as e:
                            logger.error(f"Error processing Hub survey: {e}")

                except Exception as e:
                    logger.error(f"Error processing Hub resource: {e}")

    except Exception as e:
        logger.error(f"Unable to process application '{app_id}' - '{app_title}': {e}", exc_info=True)
        result.add_error(f"Unable to process application '{app_id}' - '{app_title}'")
        return

    logger.info(f"Successfully processed application '{app_id}' - '{app_title}'")


@shared_task(bind=True, name="Update users", time_limit=6000, soft_time_limit=3000)
@celery_logging_context
def update_users(self, instance_alias, full_refresh=False, credential_token=None):
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
    :param full_refresh: Flag indicating whether to perform a full refresh (delete all and re-fetch everything)
                         instead of incremental update based on items modified since last refresh timestamp.
    :type full_refresh: bool
    :param credential_token: Token for temporary credentials (optional)
    :type credential_token: str
    :return: A JSON string summarizing the update results, including counts of inserts, updates,
             deletions, and any error messages.
    :rtype: str
    """
    result = utils.UpdateResult()
    progress_recorder = ProgressRecorder(self)
    update_time = timezone.now()  # Mark the update timestamp for the current run

    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        target = utils.connect(instance_item, credential_token)

    except Exception as e:
        logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
        result.add_error(f"Unable to connect to {instance_alias}")
        return {"result": result.to_json()}

    try:
        if full_refresh:
            user_count = User.objects.filter(portal_instance=instance_item).count()
            User.objects.filter(portal_instance=instance_item).delete()
            logger.info(f"Deleted {user_count} existing users for portal '{instance_alias}'")

        logger.debug("Retrieving user roles from portal")
        try:
            roles = dict([(role.role_id, role.name) for role in target.users.roles.all()])
            role_count = len(roles)
            logger.debug(f"Retrieved {role_count} roles from portal")
        except Exception as e:
            logger.warning(f"Error retrieving roles from portal: {e}")
            roles = {}

        logger.debug("Retrieving users from portal")

        # orgId needed for AGOL
        if instance_item.portal_type == "agol":
            org_id = instance_item.org_id
            query = f"orgid:{org_id}"
        else:
            query = "!_esri"

        total_users = target.users.advanced_search(query=query, max_users=-1, return_count=True)

        logger.info(f"Found {total_users} users to process in portal '{instance_alias}'")

        batch_size = 100
        batch_tasks = []
        logger.debug(f"Setting up batch processing with batch size: {batch_size}")

        # Create batch tasks
        for batch in range(0, total_users, batch_size):
            logger.debug(f"Creating batch task for users {batch} to {min(batch+batch_size, total_users)}")
            batch_tasks.append(
                process_batch_users.s(
                    instance_alias,
                    credential_token,
                    batch,
                    batch_size,
                    roles,
                    update_time
                )
            )

        logger.info(f"Created {len(batch_tasks)} batch tasks for processing {total_users} users")

        logger.debug("Starting parallel batch processing")
        task_group = group(batch_tasks)
        batch_results = task_group.apply_async()
        logger.debug(f"Batch processing started with group ID: {batch_results.id}")

        # Monitor group progress
        logger.debug("Monitoring batch processing progress")
        while not batch_results.ready():
            try:
                completed_tasks = sum(
                    task.result.get("current", 0) if "current" in task.result else batch_size
                    for task in batch_results.children
                    if task.result
                )
                progress_percentage = (completed_tasks / total_users) * 100
                logger.debug(f"Progress: {progress_percentage:.1f}% ({completed_tasks}/{total_users} users)")
                progress_recorder.set_progress(completed_tasks, total_users)
                time.sleep(1.5)
            except Exception as e:
                logger.warning(f"Error calculating progress: {e}")
                time.sleep(1.5)
                continue

        # Aggregate results
        logger.info(f"All batch tasks completed, aggregating results")
        logger.debug(f"Batch results: {batch_results}")

        success_count = 0
        failure_count = 0

        for batch in batch_results.get(disable_sync_subtasks=False):
            batch_result = utils.UpdateResult(**batch["result"])

            if batch_result.success is False:
                failure_count += 1
                result.success = False
                logger.warning(f"Batch task reported failure: {batch_result.error_messages}")
            else:
                success_count += 1

            # Aggregate counts
            result.num_updates += batch_result.num_updates
            result.num_inserts += batch_result.num_inserts
            result.num_deletes += batch_result.num_deletes
            result.num_errors += batch_result.num_errors
            result.error_messages.extend(batch_result.error_messages)

        logger.info(f"Batch processing summary: {success_count} successful batches, {failure_count} failed batches")

        logger.debug("Retrieving ArcGIS Pro license information from portal")
        try:
            current_licensed_users = {}
            licenses = target.admin.license.get("ArcGIS Pro")
            if licenses:
                logger.info("Successfully retrieved ArcGIS Pro license information")

                try:
                    logger.debug("Extracting license usage details from report")
                    desktop_adv_n = licenses.report.to_numpy()[2][4]
                    desktop_basic_n = licenses.report.to_numpy()[3][4]
                    desktop_std_n = licenses.report.to_numpy()[4][4]

                    # Extract all licensed users in one go
                    license_data = [
                        (desktop_adv_n, 'desktopAdvN'),
                        (desktop_basic_n, 'desktopBasicN'),
                        (desktop_std_n, 'desktopStdN')
                    ]

                    all_licensed_users = [
                        {
                            'username': user_license["user"],
                            'last_used': user_license["lastUsed"],
                            'license_type': license_type
                        }
                        for license_list, license_type in license_data
                        for user_license in license_list
                    ]

                    current_licensed_users = {user['username'] for user in all_licensed_users}

                    # Process all users
                    success_count = 0
                    error_count = 0

                    for user_data in all_licensed_users:
                        username = user_data['username']
                        last_used = user_data['last_used']
                        license_type = user_data['license_type']

                        if last_used:
                            try:
                                last_used = datetime.strptime(last_used, "%B %d, %Y").date()
                            except Exception as e:
                                logger.warning(f"Error parsing last used date '{last_used}' for user {username}: {e}")
                                last_used = None

                        try:
                            update_entry = User.objects.get(portal_instance=instance_item, user_username__exact=username)
                            update_entry.user_pro_license = license_type
                            update_entry.user_pro_last = last_used
                            update_entry.save()
                            success_count += 1
                        except User.DoesNotExist:
                            logger.warning(f"User {username} not found in database")
                            result.add_error(f"User {username} not found in database")
                            error_count += 1
                        except Exception as e:
                            logger.error(f"Error updating license for user {username}: {e}", exc_info=True)
                            result.add_error(f"Error with licenses: {e}")
                            error_count += 1

                    logger.info(f"License processing completed: {success_count} successful, {error_count} errors")

                except Exception as e:
                    logger.error(f"Error extracting license information from report: {e}", exc_info=True)
                    result.add_error("Error extracting license information from report")
            else:
                logger.info("No ArcGIS Pro license information available")

            # Clear licenses for users not in current license report
            logger.debug("Clearing licenses for users no longer in license report")
            cleared_count = User.objects.filter(
                portal_instance=instance_item,
                user_pro_license__isnull=False
            ).exclude(user_username__in=current_licensed_users).update(
                user_pro_license=None,
                user_pro_last=None
            )

            logger.info(f"Cleared licenses for {cleared_count} users no longer in license report")
        except Exception as e:
            logger.error(f"Error retrieving ArcGIS Pro license information: {e}", exc_info=True)
            result.add_error("Error retrieving ArcGIS Pro license information")

        logger.debug(f"Cleaning up outdated user records for portal '{instance_alias}'")
        delete_outdated_records(instance_item, update_time, [User], result)
        logger.info(f"Outdated user records cleanup completed with {result.num_deletes} deletions")

        instance_item.user_updated = timezone.now()
        instance_item.save()

        result.set_success()
        logger.info(f"Users update for portal '{instance_alias}' completed successfully")
        return result.to_json()

    except Exception as e:
        logger.critical(f"Users update failed for portal '{instance_alias}': {e}", exc_info=True)
        result.add_error(f"Unable to update users")

        # Revoke child tasks if they exist
        if batch_results and batch_results.children:
            logger.warning(f"Revoking {len(batch_results.children)} child tasks due to failure")
            for child in batch_results.children:
                if child.id:
                    logger.debug(f"Revoking child task: {child.id}")
                    current_app.control.revoke(child.id, terminate=True, signal="SIGKILL")
            logger.info("All child tasks revoked")

        # Also revoke the current task
        logger.debug(f"Revoking current task: {self.request.id}")
        current_app.control.revoke(self.request.id, terminate=True, signal="SIGKILL")
        logger.debug("Current task revoked")

        self.update_state(state="FAILURE", meta=result.to_json())
        raise Ignore()


@shared_task(bind=True)
@celery_logging_context
def process_batch_users(self, instance_alias, credential_token, batch, batch_size, roles, update_time):
    result = utils.UpdateResult()
    progress_recorder = ProgressRecorder(self)

    try:
        try:
            instance_item = Portal.objects.get(alias=instance_alias)
            target = utils.connect(instance_item, credential_token)

        except Exception as e:
            logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
            result.add_error(f"Unable to connect to {instance_alias}")
            return {"result": result.to_json()}

        logger.debug(f"Retrieving users for batch {batch} to {batch + batch_size}")

        if instance_item.portal_type == "agol":
            org_id = instance_item.org_id
            logger.debug(f"Using AGOL-specific query with org_id: {org_id}")
            query = f"orgid:{org_id} AND NOT username:_esri"

            user_list = target.users.advanced_search(
                query=query,
                max_users=batch_size,
                start=batch,
                sort_field="username",
                sort_order="asc"
            )
        else:
            logger.debug("Using standard Portal for ArcGIS query")
            query = "!_esri"

            user_list = target.users.advanced_search(
                query=query,
                max_users=batch_size,
                start=batch,
                sort_field="username",
                sort_order="asc"
            )

        results = user_list.get("results", [])
        total_users = len(results)
        logger.info(f"Retrieved {total_users} users for batch {batch} to {batch + batch_size}")

        logger.debug(f"Starting to process {total_users} users")

        for counter, user in enumerate(results):
            user_name = user.get("username", "unknown")
            logger.debug(f"Processing user {counter+1}/{total_users}: {user_name}")

            try:
                # Skip system users
                if not (user.get("userType", None) and user.get("role", None)):
                    logger.debug(f"Skipping system user: {user_name}")
                    continue

                logger.debug(f"Retrieving item count for user: {user_name}")
                items = target.content.advanced_search(query=f"owner:{user_name}", max_items=-1, return_count=True)
                logger.debug(f"User {user_name} has {items} items")

                first_name = user.fullName.split(" ")[0] if user.get("firstName", None) is None else user.firstName
                last_name = user.fullName.split(" ")[-1] if user.get("lastName", None) is None else user.lastName
                role = roles.get(user.roleId, user.roleId)

                logger.debug(f"Creating or updating user record for: {user_name}")
                obj, created = User.objects.update_or_create(
                    portal_instance=instance_item,
                    user_username=user_name,
                    defaults={
                        "user_first_name": first_name,
                        "user_last_name": last_name,
                        "user_email": user.email,
                        "user_created": utils.epoch_to_datetime(user.created),
                        "user_last_login": utils.epoch_to_datetime(user.lastLogin),
                        "user_role": role,
                        "user_level": user.userLicenseTypeId,
                        "user_disabled": user.disabled,
                        "user_provider": user.provider,
                        "user_items": items,
                        "updated_date": update_time
                    }
                )

                if created:
                    logger.info(f"Created new user record for: {user_name}")
                    result.add_insert()
                else:
                    logger.info(f"Updated existing user record for: {user_name}")
                    result.add_update()

                progress_percentage = ((counter + 1) / total_users) * 100
                logger.debug(f"Progress: {progress_percentage:.1f}% ({counter+1}/{total_users})")
                progress_recorder.set_progress(counter + 1, total_users)

            except Exception as e:
                logger.error(f"Unable to process user {user_name}: {e}", exc_info=True)
                result.add_error(f"Unable to update user {user_name}")

        logger.info(f"Completed processing {total_users} users in batch {batch} to {batch + batch_size}")
        logger.debug(f"Batch summary - Updates: {result.num_updates}, Inserts: {result.num_inserts}, Errors: {result.num_errors}")

        result.set_success()
        return {"result": result.to_json()}

    except Exception as e:
        logger.error(f"Error processing users in batch {batch} to {batch + batch_size}: {e}", exc_info=True)
        result.add_error(f"Error processing users in batch {batch} to {batch + batch_size}")
        return {"result": result.to_json()}


@shared_task(bind=True)
@celery_logging_context
def process_user(self, instance_alias, username, operation):
    # Operations: delete, update, add(?)
    # Event triggers: add, delete, disable, enable, updateUserRole, updateUserLicenseType
    logger.debug(
        f"Starting process_user for instance_alias={instance_alias}, username={username}, operation={operation}")

    update_time = timezone.now()
    result = utils.UpdateResult()
    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        target = utils.connect(instance_item)

    except Exception as e:
        logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
        self.update_state(state="FAILURE")
        raise Ignore()

    if operation == 'delete':
        logger.info(f"Processing delete operation for user '{username}' in portal '{instance_alias}'")
        try:
            logger.debug(f"Finding user records to delete with username: {username}")
            deletes = User.objects.filter(portal_instance=instance_item, user_username=username)
            delete_count = len(deletes)

            if delete_count > 0:
                logger.debug(f"Found {delete_count} user records to delete")
                deletes.delete()
                logger.info(f"Successfully deleted {delete_count} user records")
            else:
                logger.warning(f"No user records found to delete for username: {username}")

            instance_item.user_updated = update_time
            instance_item.save()

            logger.info(f"User '{username}' deletion completed")
            return

        except Exception as e:
            logger.error(f"Unable to delete user '{username}' for portal '{instance_alias}': {e}", exc_info=True)
            self.update_state(state="FAILURE")
            raise Ignore()

    else:
        logger.info(f"Processing {operation} operation for user '{username}' in portal '{instance_alias}'")
        try:
            logger.debug(f"Retrieving roles from portal for user '{username}'")
            roles = dict([(role.role_id, role.name) for role in target.users.roles.all()])
            logger.debug(f"Retrieved {len(roles)} roles from portal")

            logger.debug(f"Retrieving user details for '{username}'")
            user = target.users.get(username)
            if not user:
                logger.error(f"User '{username}' not found in portal '{instance_alias}'")
                return
            logger.debug(f"Successfully retrieved user details for '{username}'")

            logger.debug(f"Retrieving ArcGIS Pro license information for user '{username}'")
            try:
                user_license = target.admin.license.get("ArcGIS Pro").user_entitlement(username)
                target_entitlements = ['desktopAdvN', 'desktopStdN', 'desktopBasicN']
                pro_license = next((e for e in target_entitlements if e in user_license['entitlements']), None)
                logger.debug(f"User '{username}' has Pro license: {pro_license}")
            except Exception as e:
                logger.warning(f"Error retrieving Pro license for user '{username}': {e}")
                pro_license = None
                user_license = {}

            logger.debug(f"Retrieving items owned by user '{username}'")
            items = target.content.search(query=f'owner:{user.username}', max_items=10000)
            item_count = len(items)
            logger.debug(f"User '{username}' owns {item_count} items")

            first_name = user.fullName.split(" ")[0] if user.firstName is None else user.firstName
            last_name = user.fullName.split(" ")[-1] if user.lastName is None else user.lastName

            logger.debug(f"Creating or updating user record for '{username}'")
            obj, created = User.objects.update_or_create(
                portal_instance=instance_item,
                user_username=user.username,
                defaults={
                    'user_first_name': first_name,
                    'user_last_name': last_name,
                    'user_email': user.email,
                    'user_created': utils.epoch_to_datetime(user.created),
                    'user_last_login': utils.epoch_to_datetime(user.lastLogin),
                    'user_role': roles.get(user.roleId, user.roleId),
                    'user_level': user.userLicenseTypeId,
                    'user_disabled': user.disabled,
                    'user_provider': user.provider,
                    'user_items': item_count,
                    'user_pro_license': pro_license,
                    'user_pro_last': user_license.get('lastLogin', None),
                }
            )

            if created:
                logger.info(f"Created new user record for '{username}'")
            else:
                logger.info(f"Updated existing user record for '{username}'")

            instance_item.user_updated = timezone.now()
            instance_item.save()

            logger.info(f"User '{username}' {operation} operation completed successfully")
            return

        except Exception as e:
            logger.error(f"Unable to process {operation} operation for user '{username}' in portal '{instance_alias}': {e}", exc_info=True)
            self.update_state(state="FAILURE")
            raise Ignore()


@shared_task(bind=True)
@celery_logging_context
def process_webmap(self, instance_alias, item_id, operation):
    # Operation: add, delete, publish, share, unshare, update
    logger.debug(
        f"Starting process_webmap for instance_alias={instance_alias}, item_id={item_id}, operation={operation}")

    update_time = timezone.now()
    result = utils.UpdateResult()
    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        target = utils.connect(instance_item)

    except Exception as e:
        logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
        self.update_state(state="FAILURE")
        raise Ignore()

    logger.info(f"Processing web map {item_id} with operation: {operation}")
    try:
        logger.debug(f"Retrieving web map item with ID: {item_id}")
        wm_item = target.content.get(item_id)

        if wm_item is None:
            logger.error(f"Web map with ID {item_id} not found in portal '{instance_alias}'")
            self.update_state(state="FAILURE")
            raise Ignore()

        logger.debug(f"Successfully retrieved web map: {wm_item.title} (ID: {item_id})")

        logger.debug(f"Extracting data for web map: {item_id}")
        webmap_data = extract_webmap_data(wm_item, instance_item, update_time)

        logger.debug(f"Updating or creating database record for web map: {item_id}")
        obj, created = Webmap.objects.update_or_create(
            portal_instance=instance_item,
            webmap_id=wm_item.id,
            defaults=webmap_data
        )

        if created:
            logger.info(f"Created new record for web map: {item_id} - '{wm_item.title}'")
        else:
            logger.info(f"Updated existing record for web map: {item_id} - '{wm_item.title}'")

        service_count = len(webmap_data["webmap_services"])
        logger.debug(f"Linking {service_count} services to web map: {item_id}")
        link_services_to_webmap(instance_item, obj, webmap_data["webmap_services"])
        logger.debug(f"Successfully linked {service_count} services to web map: {item_id}")

        instance_item.webmap_updated = update_time
        instance_item.save()

        logger.info(f"Web map {item_id} processing completed successfully")
        return

    except Exception as e:
        logger.error(f"Unable to process web map {item_id} for portal '{instance_alias}': {e}", exc_info=True)
        self.update_state(state="FAILURE")
        raise Ignore()


@shared_task(bind=True)
@celery_logging_context
def process_service(self, instance_alias, item, operation):
    logger.debug(f"Starting process_service for instance_alias={instance_alias}, item={item}, operation={operation}")

    import re
    # Operation: add, delete, publish, share, unshare, update
    logger.info(f"Processing service {item} with operation: {operation}")

    update_time = timezone.now()

    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        target = utils.connect(instance_item)

    except Exception as e:
        logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
        self.update_state(state="FAILURE")
        raise Ignore()

    try:
        logger.debug(f"Creating service object for item: {item}")
        service = arcgis.gis.server.Service(item, target)

        if service is None:
            logger.error(f"Unable to create service {item}. Service not found.")
            self.update_state(state="FAILURE")
            raise Ignore()

        logger.debug(f"Successfully created service object for item: {item}")
        logger.debug(f"Service URL: {service.url.url if hasattr(service, 'url') else 'Unknown'}")

    except Exception as e:
        logger.error(f"Error creating service object for item: {item}: {e}", exc_info=True)
        self.update_state(state="FAILURE")
        raise Ignore()

    try:
        match = re.search(r"/services(?:/([^/]+))?/([^/]+)/[^/]+(?:/\d+)?$", service.url.url)
        if match:
            folder_name = match.group(1)
            if folder_name is None:
                folder_name = ""
            service_name = match.group(2)
        else:
            return
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
            logger.error(f"Unable to get portal items for {item}: {e}")
        sm = json.loads(service_admin.service_manifest())
        if sm.get('code') == 500 and sm.get(
            'status') == 'error' and 'FeatureServer' in portal_ids.keys():
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
                                               service_url__overlap=[service.url])
            except Service.DoesNotExist:
                continue
            except MultipleObjectsReturned:
                view_obj = Service.objects.filter(portal_instance=instance_item,
                                                  service_url__overlap=[service.url]).first()
            try:
                service_obj = Service.objects.get(portal_instance=instance_item,
                                                  service_url__overlap=[parent.url])
            except Service.DoesNotExist:
                continue
            except MultipleObjectsReturned:
                service_obj = Service.objects.filter(portal_instance=instance_item,
                                                     service_url__overlap=[parent.url]).first()
            logger.debug(f"{service_obj} related to {view_obj}")

            view_obj.service_view = service_obj
            service_obj.service_view = view_obj
            view_obj.save()
            service_obj.save()
        instance_item.service_updated = update_time
        instance_item.save()
        return
    except Exception as e:
        logger.exception(f"Unable to update service {item} for {instance_alias}: {e}")
        self.update_state(state="FAILURE")
        raise Ignore()


@shared_task(bind=True)
@celery_logging_context
def process_webapp(self, instance_alias, item_id, operation):
    # Operation: add, delete, publish, share, unshare, update
    logger.debug(
        f"Starting process_webapp for instance_alias={instance_alias}, item_id={item_id}, operation={operation}")

    update_time = timezone.now()
    result = utils.UpdateResult()
    try:
        instance_item = Portal.objects.get(alias=instance_alias)
        target = utils.connect(instance_item)

    except Exception as e:
        logger.critical(f"Connection failed for portal '{instance_alias}': {e}", exc_info=True)
        self.update_state(state="FAILURE")
        raise Ignore()

    logger.info(f"Processing web application {item_id} with operation: {operation}")
    try:
        logger.debug(f"Retrieving web application item with ID: {item_id}")
        item = target.content.get(item_id)

        if item is None:
            logger.error(f"Web application with ID {item_id} not found in portal '{instance_alias}'")
            self.update_state(state="FAILURE")
            raise Ignore()

        logger.debug(f"Successfully retrieved web application: {item.title} (ID: {item_id})")
        logger.debug(f"Web application type: {item.type}, Owner: {item.owner}")

        logger.debug(f"Processing web application details for: {item_id}")
        process_single_app(item, target, instance_item, update_time, result)

        instance_item.webapp_updated = update_time
        instance_item.save()

        logger.info(f"Web application {item_id} processing completed successfully")
        return

    except Exception as e:
        logger.error(f"Unable to process web application {item_id} for portal '{instance_alias}': {e}", exc_info=True)
        self.update_state(state="FAILURE")
        raise Ignore()


def _validate_params(params, required, tool_result):
    """Validate required parameters."""
    missing = [p for p in required if not params.get(p)]
    if missing:
        err = f"Missing parameters: {', '.join(missing)}"
        tool_result.add_error(err)
        return False
    return True


def _get_portal_info(target):
    """Get portal name and URL for emails."""
    info = target.properties
    name = info.get("name", "your ArcGIS organization")
    return name, target.url


def _get_all_users(target, tool_result):
    """Retrieve all users with error tracking."""
    try:
        users = target.users.search(query="*", max_users=10000)
        logger.info(f"Retrieved {len(users)} users")
        return users
    except Exception as e:
        tool_result.add_error("Failed to retrieve users from portal")
        logger.error(f"Failed to retrieve users from portal: {e}", exc_info=True)
        return None


def _calc_cutoffs(duration_days, warning_days):
    """Calculate cutoff dates."""
    now = timezone.now()
    action_cutoff = now - timedelta(days=duration_days)
    warning_cutoff = action_cutoff + timedelta(days=warning_days)
    return action_cutoff, warning_cutoff


def _check_skip_user(user, admin_username=None, check_email=True):
    """Check if user should be skipped."""
    if user.role == "org_admin":
        return True
    if admin_username and user.username == admin_username:
        return True
    if check_email and not user.email:
        return True
    return False


def _get_last_activity(user):
    """Get user's last activity date."""
    try:
        # Try lastLogin first (if it exists and is valid)
        last_login = utils.epoch_to_datetime(getattr(user, 'lastLogin', None))
        if last_login:
            return last_login

        # Fallback to creation date
        created = utils.epoch_to_datetime(getattr(user, 'created', None))
        if created:
            return created

        # If both are invalid, log warning and return None
        logger.warning(f"No valid timestamp found for user '{user.username}' - both lastLogin and created are invalid")
        return None

    except Exception as e:
        logger.error(f"Error getting last activity for user '{user.username}': {e}", exc_info=True)
        return None


def _log_completion(tool_result):
    """Log tool completion summary."""
    summary = tool_result.get_summary()
    logger.info(f"Tool completed: Processed={summary.get('processed', 0)}, "
                f"Actions={summary.get('actions_taken', 0)}, "
                f"Errors={summary.get('errors', 0)}")


def _build_warning_email(user, portal_name, portal_url, warning_text):
    """Build standard warning email."""
    return f"""Hello {user.firstName or user.fullName or user.username},

{warning_text}

Please take action to maintain your account access in {portal_name}.

Portal: {portal_url}

If you have questions, contact your portal administrator.
"""


def _verify_privileges(user):
    """Verify user has admin privileges."""
    required = ["portal:admin:viewUsers", "portal:admin:deleteUsers",
                "portal:admin:disableUsers", "portal:admin:manageLicenses",
                "portal:admin:reassignItems"]
    return all(priv in user.privileges for priv in required)


def _connect_portal_task(portal_alias, tool_result):
    """Establish portal connection with error tracking."""
    try:
        portal_instance = Portal.objects.get(alias=portal_alias)
        target = utils.connect(portal_instance)
        if not target:
            tool_result.add_error(f"Failed to connect to portal '{portal_alias}'")
            logger.error(f"Failed to connect to portal '{portal_alias}'")
            return portal_instance, None

        if not _verify_privileges(target.users.me):
            tool_result.add_error(f"User '{target.users.me.username}' lacks required admin privileges")
            logger.error(f"User '{target.users.me.username}' lacks required admin privileges")
            return portal_instance, None

        return portal_instance, target

    except Portal.DoesNotExist:
        tool_result.add_error(f"Portal '{portal_alias}' not found in database")
        logger.error(f"Portal '{portal_alias}' not found in database")
        return None, None
    except ConnectionError as e:
        tool_result.add_error(f"Connection error connecting to portal '{portal_alias}': {e}")
        logger.error(f"Connection error connecting to portal '{portal_alias}': {e}", exc_info=True)
        return None, None
    except Exception as e:
        tool_result.add_error(f"Unexpected error connecting to portal '{portal_alias}'")
        logger.error(f"Unexpected error connecting to portal '{portal_alias}': {e}", exc_info=True)
        return None, None


def _get_admin_email_list(portal_instance):
    """Get list of admin email addresses for notifications."""
    try:
        # Check if notifications are enabled for this portal
        if not getattr(portal_instance, 'enable_admin_notifications', True):
            logger.info(f"Admin notifications disabled for portal '{portal_instance.alias}'")
            return []

        # Get admin emails from portal configuration
        admin_emails = getattr(portal_instance, 'admin_emails', '')
        if admin_emails:
            emails = [email.strip() for email in admin_emails.split(',') if email.strip()]
            if emails:
                return emails

        logger.warning(f"No admin emails configured for portal '{portal_instance.alias}'")
        return []

    except Exception as e:
        logger.error(f"Error retrieving admin emails for portal '{portal_instance.alias}': {e}")
        return []


def _send_admin_notification(tool_result, portal_instance):
    """Send admin notification email with tool execution summary."""
    try:
        admin_emails = _get_admin_email_list(portal_instance)
        if not admin_emails:
            logger.warning(f"No admin emails configured for portal '{portal_instance.alias}' - skipping notification")
            return False

        # Format subject based on tool result
        status_indicator = "SUCCESS" if tool_result.success else "FAILURE"
        subject = f"ArcGIS Tool Report: {tool_result.tool_name.replace('_', ' ').title()} - {status_indicator}"

        # Use the ToolResult's built-in email formatting
        message = tool_result.format_for_email()

        # Add timestamp and portal info
        timestamp = timezone.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        header = f"ArcGIS Enterprise Tool Execution Report\nGenerated: {timestamp}\n" + "=" * 50 + "\n"
        message = header + message

        # Send to all admin emails
        notification_sent = False
        for admin_email in admin_emails:
            try:
                success, status_msg = utils.send_email(admin_email, subject, message)
                if success:
                    logger.info(f"Admin notification sent to {admin_email} for tool '{tool_result.tool_name}'")
                    notification_sent = True
                else:
                    logger.error(f"Failed to send admin notification to {admin_email}: {status_msg}")
            except Exception as e:
                logger.error(f"Error sending admin notification to {admin_email}: {e}")

        return notification_sent

    except Exception as e:
        logger.error(f"Error in admin notification process: {e}")
        return False


@shared_task(bind=True, name="Pro License Tool")
def process_pro_license_task(self, portal_alias: str, duration_days: int, warning_days: int):
    """Process Pro License management."""
    start_time = time.time()
    progress_recorder = ProgressRecorder(self)
    tool_result = utils.ToolResult(
        portal_alias=portal_alias,
        tool_name="pro_license"
    )
    portal_instance = None

    try:
        progress_recorder.set_progress(0, 100, "Connecting to portal...")
        portal_instance, target = _connect_portal_task(portal_alias, tool_result)
        if not target:
            progress_recorder.set_progress(100, 100, "Connection failed")
        else:
            logger.info(f"[{portal_alias}] Starting Pro License Tool")
            progress_recorder.set_progress(10, 100, "Connected")

            progress_recorder.set_progress(15, 100, "Getting users...")
            users = _get_all_users(target, tool_result)
            if users is None:
                progress_recorder.set_progress(100, 100, "Failed to get users")
            else:
                progress_recorder.set_progress(20, 100, f"Processing {len(users)} users")
                _run_pro_license(target, duration_days, warning_days, users, progress_recorder, tool_result)

                tool_result.set_success(tool_result.errors == 0)
                progress_recorder.set_progress(100, 100, f"Completed: {tool_result.actions_taken} actions")
                logger.info(f"[{portal_alias}] Pro License Tool completed in {tool_result.execution_time:.2f}s")

    except Exception as e:
        logger.error(f"Pro License Tool failed: {e}")
        tool_result.add_error("Pro License Tool failed")
        progress_recorder.set_progress(100, 100, "Failed")
        self.update_state(state="FAILURE")

    finally:
        tool_result.execution_time = time.time() - start_time
        _send_admin_notification(tool_result, portal_instance)

    return tool_result.to_json()


@shared_task(bind=True, name="Inactive User Tool")
def process_inactive_user_task(self, portal_alias: str, duration_days: int, warning_days: int, action: str):
    """Process inactive user management."""
    start_time = time.time()
    progress_recorder = ProgressRecorder(self)
    tool_result = utils.ToolResult(
        portal_alias=portal_alias,
        tool_name="inactive_user"
    )
    portal_instance = None

    try:
        progress_recorder.set_progress(0, 100, "Connecting to portal...")
        portal_instance, target = _connect_portal_task(portal_alias, tool_result)
        if not target:
            progress_recorder.set_progress(100, 100, "Connection failed")
        else:
            logger.info(f"[{portal_alias}] Starting Inactive User Tool")
            progress_recorder.set_progress(10, 100, "Connected")

            progress_recorder.set_progress(15, 100, "Getting users...")
            users = _get_all_users(target, tool_result)
            if users is None:
                progress_recorder.set_progress(100, 100, "Failed to get users")
            else:
                progress_recorder.set_progress(20, 100, f"Processing {len(users)} users")
                _run_inactive_user(target, duration_days, warning_days, action,
                                   portal_instance.username, users, progress_recorder, tool_result)

                tool_result.set_success(tool_result.errors == 0)
                progress_recorder.set_progress(100, 100, f"Completed: {tool_result.actions_taken} actions")
                logger.info(f"[{portal_alias}] Inactive User Tool completed in {tool_result.execution_time:.2f}s")

    except Exception as e:
        err = f"Inactive User Tool failed: {e}"
        logger.error(f"[{portal_alias}] {err}")
        tool_result.add_error(err)
        progress_recorder.set_progress(100, 100, f"Failed: {err}")
        self.update_state(state="FAILURE", meta={"error": err})

    finally:
        tool_result.execution_time = time.time() - start_time
        _send_admin_notification(tool_result, portal_instance)

    return tool_result.to_json()


@shared_task(bind=True, name="Public Sharing Tool")
def process_public_unshare_task(self, portal_alias: str, score_threshold: int = None, item_id: str = None):
    """Process public unshare task - either for a specific item or all public items."""
    start_time = time.time()
    progress_recorder = ProgressRecorder(self)
    tool_result = utils.ToolResult(
        portal_alias=portal_alias,
        tool_name="public_unshare"
    )
    portal_instance = None

    try:
        progress_recorder.set_progress(0, 100, "Connecting to portal...")
        portal_instance, target = _connect_portal_task(portal_alias, tool_result)
        if not target:
            progress_recorder.set_progress(100, 100, "Connection failed")
            return tool_result.to_json()

        # Determine if this is webhook or scheduled execution
        is_webhook = item_id is not None

        # Get score threshold
        if score_threshold is None:
            try:
                score_threshold = portal_instance.tool_settings.tool_public_unshare_score
                if not score_threshold:
                    tool_result.add_error("Score threshold not configured in portal settings")
                    return tool_result.to_json()
            except Exception as e:
                tool_result.add_error("Failed to retrieve portal tool settings")
                logger.error(f"Failed to retrieve portal tool settings: {e}", exc_info=True)
                return tool_result.to_json()

        progress_recorder.set_progress(10, 100, "Connected")
        logger.info(f"[{portal_alias}] Starting Public Unshare Tool ({'webhook' if is_webhook else 'scheduled'})")

        # Get items based on execution type
        progress_recorder.set_progress(15, 100, "Getting items...")
        items = _get_items_for_processing(target, item_id, tool_result)
        if items is None:
            progress_recorder.set_progress(100, 100, "Failed to get items")
            return tool_result.to_json()

        if not items:
            logger.info("No items to process")
            tool_result.set_success()
            progress_recorder.set_progress(100, 100, "No items to process")
            return tool_result.to_json()

        progress_recorder.set_progress(20, 100, f"Processing {len(items)} items")

        # Process items with different behavior for webhook vs scheduled
        portal_info = _get_portal_info(target)
        user_items = _process_public_items(
            items, score_threshold, portal_info, target,
            tool_result, progress_recorder, is_webhook, portal_instance
        )

        # Send notifications (scheduled only sends batch notifications)
        if not is_webhook and user_items:
            progress_recorder.set_progress(90, 100, "Sending notifications...")
            _send_batch_notifications(user_items, target, portal_info, score_threshold, tool_result)

        tool_result.set_success(tool_result.errors == 0)
        progress_recorder.set_progress(100, 100, f"Completed: {tool_result.actions_taken} actions")
        logger.info(f"[{portal_alias}] Public Unshare Tool completed in {tool_result.execution_time:.2f}s")

    except Exception as e:
        err = f"Public Unshare Tool failed: {e}"
        logger.error(f"[{portal_alias}] {err}", exc_info=True)
        tool_result.add_error(err)
        progress_recorder.set_progress(100, 100, f"Failed: {err}")
        self.update_state(state="FAILURE", meta={"error": err})

    finally:
        tool_result.execution_time = time.time() - start_time
        _send_admin_notification(tool_result, portal_instance)

    return tool_result.to_json()


def _run_pro_license(target, duration_days, warning_days, users, recorder, tool_result):
    """Pro License tool implementation."""
    params = {"duration_days": duration_days, "warning_days": warning_days}
    if not _validate_params(params, ["duration_days", "warning_days"], tool_result):
        return

    cutoffs = _calc_cutoffs(duration_days, warning_days)
    portal_info = _get_portal_info(target)

    _process_pro_users(target, users, cutoffs, portal_info, tool_result, recorder)
    _log_completion(tool_result)


def _process_pro_users(target, users, cutoffs, portal_info, tool_result, recorder):
    """Process users for Pro License management."""
    action_cutoff, warning_cutoff = cutoffs
    portal_name, portal_url = portal_info
    total = len(users)

    for i, user in enumerate(users):
        progress = 20 + int((i / total) * 75)
        recorder.set_progress(progress, 100, f"Checking: {user.username}")

        tool_result.processed += 1

        if _check_skip_user(user):
            continue

        license_info = _get_pro_license(target, user.username, tool_result)
        if not license_info:
            continue

        effective_date = _get_effective_date(user, license_info, tool_result)
        if not effective_date:
            continue

        if effective_date < action_cutoff:
            if _revoke_pro_license(user, license_info, effective_date, portal_name, portal_url, tool_result):
                tool_result.actions_taken += 1
                tool_result.add_extra_metric("licenses_revoked",
                                             tool_result.extra_metrics.get("licenses_revoked", 0) + 1)
        elif action_cutoff <= effective_date < warning_cutoff:
            success, status_msg = _send_pro_warning(user, effective_date, portal_name, portal_url)
            if success:
                tool_result.warnings_sent += 1
            else:
                tool_result.add_error(f"Failed to send warning email to {user.email}: {status_msg}")
                logger.error(f"Failed to send warning email to {user.email}: {status_msg}")

    recorder.set_progress(95, 100, "Finalizing...")


def _get_pro_license(target, username, tool_result):
    """Get Pro license for user."""
    try:
        license_info = target.admin.license.get("ArcGIS Pro").user_entitlement(username)
        if not license_info:
            return None
        entitlements = ["desktopAdvN", "desktopStdN", "desktopBasicN"]
        has_pro = next((e for e in entitlements if e in license_info.get("entitlements", None)), None)
        return license_info if has_pro else None
    except Exception as e:
        tool_result.add_error(f"Failed to check Pro license for user '{username}'")
        logger.error(f"Failed to check Pro license for user '{username}': {e}", exc_info=True)
        return None


def _get_effective_date(user, license_info, tool_result):
    """Get effective date for inactivity check."""
    try:
        pro_login = license_info.get("lastLogin", None)
        # User last login or created date if never logged in
        user_login = _get_last_activity(user)
        return pro_login or user_login
    except Exception as e:
        tool_result.add_error(f"Failed to calculate effective date for user '{user.username}'")
        logger.error(f"Failed to calculate effective date for user '{user.username}': {e}", exc_info=True)
        return None


def _revoke_pro_license(user, license_info, effective_date, portal_name, portal_url, tool_result):
    """Revoke Pro license for inactive user."""
    logger.warning(f"Revoking Pro license for inactive user '{user.username}' (inactive since {effective_date})")

    try:
        logger.info(f"Revoking Pro license for user '{user.username}'")
        revoked = license_info.revoke(username=user.username, entitlements='*', suppress_email=True)

        if revoked:
            logger.info(f"Successfully revoked Pro license for user '{user.username}'")
            subject = f"ArcGIS Pro License Revoked for {user.username}"
            message = f"""Hello {user.firstName or user.fullName},

Your ArcGIS Pro license in {portal_name} has been revoked due to inactivity (last activity: {effective_date.strftime('%Y-%m-%d')}).

If you need access restored, contact your portal administrator.

Portal: {portal_url}"""

            success, status_msg = utils.send_email(user.email, subject, message)
            if success:
                tool_result.warnings_sent += 1
                logger.info(f"Warning email sent to {user.email}")
            else:
                tool_result.add_error(f"Failed to send warning email to {user.email}: {status_msg}")
                logger.error(f"Failed to send warning email to {user.email}: {status_msg}")
            return True
        else:
            tool_result.add_error(f"Failed to revoke Pro license for user '{user.username}'")
            logger.error(f"Failed to revoke Pro license for user '{user.username}' - API returned failure")
            return False
    except Exception as e:
        tool_result.add_error(f"Error revoking Pro license for user '{user.username}'")
        logger.error(f"Error revoking Pro license for user '{user.username}': {e}", exc_info=True)
        return False


def _send_pro_warning(user, effective_date, portal_name, portal_url):
    """Send Pro license warning."""
    subject = "ArcGIS Pro License Inactivity Warning"
    warning_text = f"""Your ArcGIS Pro license in {portal_name} may be revoked due to inactivity.
Last activity: {effective_date.strftime('%Y-%m-%d')}

To keep your license, log in to {portal_name} or use ArcGIS Pro."""

    message = _build_warning_email(user, portal_name, portal_url, warning_text)

    success, status_msg = utils.send_email(user.email, subject, message)
    return success, status_msg


def _get_items_for_processing(target, item_id, tool_result):
    """Get items for processing - either specific item or all public items."""
    if item_id:
        # Webhook - get specific item
        try:
            item = target.content.get(item_id)
            if item and item.access == 'public':
                logger.info(f"Processing specific public item: {item_id}")
                return [item]
            else:
                logger.info(f"Item {item_id} is not public or not found, skipping")
                return []
        except Exception as e:
            tool_result.add_error(f"Failed to retrieve item {item_id}")
            logger.error(f"Failed to retrieve item {item_id}: {e}", exc_info=True)
            return None
    else:
        # Scheduled - get all public items
        try:
            items = target.content.search(query="access:public", max_items=1000)
            logger.info(f"Found {len(items)} public items")
            return items
        except Exception as e:
            tool_result.add_error("Failed to retrieve public items from portal")
            logger.error(f"Failed to retrieve public items from portal: {e}", exc_info=True)
            return None


def _process_public_items(items, threshold, portal_info, target, tool_result,
                          progress_recorder, is_webhook, portal_instance):
    """Process public items with different behavior for webhook vs scheduled."""
    user_items = {} if not is_webhook else None
    total = len(items)
    portal_name, portal_url = portal_info

    for i, item in enumerate(items):
        progress = 20 + int((i / total) * (65 if not is_webhook else 75))
        progress_recorder.set_progress(progress, 100, f"Checking: {item.title[:30]}...")

        try:
            tool_result.processed += 1
            score = getattr(item, "scoreCompleteness", 0)

            if score < threshold:
                logger.info(f"Unsharing item '{item.id}' (score: {score}%)")

                # Unshare the item
                success = _unshare_item(item, tool_result)
                if success:
                    tool_result.actions_taken += 1
                    tool_result.add_extra_metric("unshared", tool_result.extra_metrics.get("unshared", 0) + 1)

                    if is_webhook:
                        tool_result.add_extra_metric("validation_failed", 1)

                        # Send notification if no recent notification
                        if _should_notify(item, portal_instance):
                            if _send_webhook_notification(item, target, score, threshold, portal_name, portal_url,
                                                          tool_result):
                                tool_result.warnings_sent += 1
                            _record_webhook_notification(item, portal_instance)
                    else:
                        # For scheduled runs, collect items for batch notification
                        _add_to_user_items(user_items, item, score)
            else:
                if is_webhook:
                    tool_result.add_extra_metric("validation_passed", 1)
                    logger.info(f"Item '{item.id}' passed validation: {score}% >= {threshold}%")

        except Exception as e:
            tool_result.add_error(f"Error processing item '{item.id}'")
            logger.error(f"Error processing item '{item.id}': {e}", exc_info=True)

    progress_recorder.set_progress(85, 100, "Items processed")
    return user_items


def _unshare_item(item, tool_result):
    """Unshare a single item."""
    try:
        logger.debug(f"Unsharing item '{item.id}'")
        success = item.share(everyone=False)
        if success:
            logger.info(f"Successfully unshared item '{item.id}'")
            return True
        else:
            tool_result.add_error(f"Failed to unshare item '{item.id}'")
            logger.error(f"Failed to unshare item '{item.id}' - API returned failure")
            return False
    except Exception as e:
        tool_result.add_error(f"Error unsharing item '{item.id}'")
        logger.error(f"Error unsharing item '{item.id}': {e}", exc_info=True)
        return False


def _add_to_user_items(user_items, item, score):
    """Add item to user_items dictionary for batch notifications."""
    owner = item.owner
    if owner not in user_items:
        user_items[owner] = {"user": None, "items": []}

    user_items[owner]["items"].append({
        "id": item.id,
        "title": item.title,
        "score": score,
        "type": item.type
    })


def _send_batch_notifications(user_items, target, portal_info, threshold, tool_result):
    """Send batch notifications to users about unshared items (scheduled runs only)."""
    portal_name, portal_url = portal_info

    for username, data in user_items.items():
        items = data["items"]
        if not items:
            continue

        # Get user if not cached
        if not data["user"]:
            try:
                data["user"] = target.users.get(username)
            except Exception:
                continue

        user = data["user"]
        if not user or not user.email:
            continue

        avg_score = sum(item["score"] for item in items) / len(items)
        items_list = "\n".join([f"• {item['title']} ({item['type']}) - {item['score']}%"
                                for item in items[:10]])
        if len(items) > 10:
            items_list += f"\n... and {len(items) - 10} more"

        subject = "Public Items Unshared - Metadata Required"
        message = f"""Hello {user.firstName or user.fullName or user.username},

Your public items in {portal_name} were unshared due to low metadata completeness (below {threshold}%).

Items ({len(items)} total):
{items_list}

Average completeness: {avg_score:.1f}%

To improve and reshare:
1. Complete item summaries
2. Add detailed descriptions and tags
3. Include credits and limitations
4. Add thumbnail images

Portal: {portal_url}"""

        success, status_msg = utils.send_email(user.email, subject, message)
        if success:
            tool_result.warnings_sent += 1
        else:
            tool_result.add_error(f"Failed to send warning email to {user.email}: {status_msg}")
            logger.error(f"Failed to send warning email to {user.email}: {status_msg}")


def _run_inactive_user(target, duration_days, warning_days, action, admin_username, users, recorder, tool_result):
    """Inactive User tool implementation."""
    params = {"duration_days": duration_days, "warning_days": warning_days, "action": action}
    if not _validate_params(params, ["duration_days", "warning_days", "action"], tool_result):
        return

    cutoffs = _calc_cutoffs(duration_days, warning_days)
    _process_inactive_users(target, users, cutoffs, action, admin_username, tool_result, recorder)
    _log_completion(tool_result)


def _process_inactive_users(target, users, cutoffs, action, admin_username, tool_result, recorder):
    """Process users for inactivity."""
    action_cutoff, warning_cutoff = cutoffs
    portal_info = _get_portal_info(target)
    total = len(users)

    for i, user in enumerate(users):
        progress = 20 + int((i / total) * 75)
        recorder.set_progress(progress, 100, f"Checking: {user.username}")

        if _check_skip_user(user, admin_username):
            continue

        tool_result.processed += 1
        last_activity = _get_last_activity(user)
        if not last_activity:
            continue

        if last_activity < action_cutoff:
            success, msg = _take_inactive_action(user, last_activity, action, tool_result)
            if success:
                tool_result.actions_taken += 1
                tool_result.add_extra_metric("inactive_found",
                                             tool_result.extra_metrics.get("inactive_found", 0) + 1)
            else:
                tool_result.add_error(f"Failed to take action for user '{user.username}': {msg}")
                logger.error(f"Failed to take action for user '{user.username}': {msg}")
        elif action_cutoff <= last_activity < warning_cutoff:
            success, msg = _send_inactive_warning(user, last_activity, portal_info)
            if success:
                tool_result.warnings_sent += 1
            else:
                tool_result.add_error(f"Failed to send warning email to {user.email}: {msg}")
                logger.error(f"Failed to send warning email to {user.email}: {msg}")

    recorder.set_progress(95, 100, "Finalizing...")


def _take_inactive_action(user, last_activity, action, tool_result):
    """Take action on inactive user"""
    username = user.username
    logger.info(f"Taking action '{action}' for inactive user '{username}' (last activity: {last_activity})")

    try:
        if action == "disable":
            logger.info(f"Disabling user '{username}'")
            user.disable()
            logger.info(f"Successfully disabled user '{username}'")
            tool_result.add_extra_metric("users_disabled",
                                         tool_result.extra_metrics.get("users_disabled", 0) + 1)
            return True, "User disabled successfully"

        elif action == "remove_role":
            logger.info(f"Removing role from user '{username}'")
            user.update_role("viewer")
            logger.info(f"Successfully removed role from user '{username}'")
            tool_result.add_extra_metric("roles_removed",
                                         tool_result.extra_metrics.get("roles_removed", 0) + 1)
            return True, "User role removed successfully"

        elif action == "notify":
            logger.info(f"Sending admin notification for user '{username}'")
            subject = f"Inactive User: {username}"
            message = f"User {username} has been inactive since {last_activity.strftime('%Y-%m-%d')}."
            success, status_msg = utils.send_email(user.email, subject, message)
            if success:
                tool_result.add_extra_metric("admin_notifications_sent",
                                             tool_result.extra_metrics.get("admin_notifications_sent", 0) + 1)
            return success, status_msg

        elif action == "delete":
            logger.info(f"Deleting user '{username}'")
            user.delete()
            logger.info(f"Successfully deleted user '{username}'")
            tool_result.add_extra_metric("users_deleted", tool_result.extra_metrics.get("users_deleted", 0) + 1)

            return True, "User deleted successfully"

        elif action == "transfer":
            return False, "Transfer action requires destination parameters and is not implemented yet"

        else:
            return False, f"Unknown action: {action}"

    except Exception as e:
        error_msg = f"Failed to execute action '{action}' for user '{username}': {e}"
        tool_result.add_error(error_msg)
        logger.error(error_msg, exc_info=True)
        return False, str(e)


def _send_inactive_warning(user, last_activity, portal_info):
    """Send inactivity warning"""
    portal_name, portal_url = portal_info
    subject = "Account Inactivity Warning"
    warning_text = f"""Your account has been inactive since {last_activity.strftime('%Y-%m-%d')}.
If you remain inactive, your account may be subject to administrative action."""

    message = _build_warning_email(user, portal_name, portal_url, warning_text)
    return utils.send_email(user.email, subject, message)


def _send_webhook_notification(item, target, score, threshold, portal_name, portal_url, tool_result):
    """Send immediate notification to item owner (webhook only)."""
    try:
        user = target.users.get(item.owner)
        if not user or not user.email:
            logger.warning(f"Cannot send notification to item owner '{item.owner}': user not found or no email")
            return False

        subject = "Public Item Unshared - Metadata Required"
        message = f"""Hello {user.firstName or user.fullName or user.username},

Your attempt to share "{item.title}" publicly was blocked due to incomplete metadata.

Item Details:
• Title: {item.title}
• Type: {item.type}
• Completeness: {score}%
• Required: {threshold}%

To share publicly, complete missing metadata:
1. Complete item summary
2. Add detailed description and tags
3. Include credits and limitations
4. Add thumbnail image

Portal: {portal_url}"""

        success, status_msg = utils.send_email(user.email, subject, message)
        if success:
            logger.info(f"Sent validation notification to user '{user.username}' for item '{item.id}'")
            tool_result.add_extra_metric("owner_notifications_sent",
                                         tool_result.extra_metrics.get("owner_notifications_sent", 0) + 1)
            return True
        else:
            tool_result.add_error(f"Failed to send validation notification to user '{user.username}': {status_msg}")
            logger.warning(f"Failed to send validation notification to user '{user.username}': {status_msg}")
            return False

    except Exception as e:
        error_msg = f"Error sending validation notification for item '{item.id}': {e}"
        tool_result.add_error(error_msg)
        logger.error(error_msg, exc_info=True)
        return False


def _should_notify(item, portal_instance):
    """Check if notification should be sent (grace period check for webhooks)."""
    try:
        hours_limit = getattr(portal_instance.tool_settings, "tool_public_unshare_notify_limit", 24)
    except:
        hours_limit = 24

    hours_cutoff = timezone.now() - timedelta(hours=hours_limit)

    # Check recent notifications
    recent = WebhookNotificationLog.objects.filter(
        portal=portal_instance,
        owner=item.owner,
        notification_type="public_unshare_webhook",
        sent_at__gte=hours_cutoff
    ).exists()

    return not recent


def _record_webhook_notification(item, portal_instance):
    """Record notification in log (webhook only)."""
    try:
        WebhookNotificationLog.objects.create(
            portal=portal_instance,
            item_id=item.id,
            owner=item.owner,
            notification_type="public_unshare_webhook",
            sent_at=timezone.now(),
            item_title=item.title[:200],
            item_type=item.type
        )
    except Exception as e:
        logger.error(f"Failed to log notification for {item.id}: {e}")
