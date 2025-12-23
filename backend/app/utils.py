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
import base64
import hashlib
import json
import logging
import re
import secrets
import shutil
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone as dt_timezone
from itertools import combinations, groupby
from operator import itemgetter
from pathlib import Path
from typing import Dict, List, Optional

import requests
from arcgis import gis
from cryptography.fernet import Fernet
from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ValidationError as DjangoValidationError
from django.core.mail import get_connection, EmailMultiAlternatives, EmailMessage
from django.core.validators import validate_email
from django.db.models import F, Prefetch, QuerySet, Q
from django.utils import timezone
from django.utils.crypto import constant_time_compare
from django.utils.html import escape
from fuzzywuzzy import fuzz
from tabulate import tabulate

from . import tasks
from .models import Webmap, Service, Layer, App, Portal, SiteSettings, Layer_Service

logger = logging.getLogger('enterpriseviz.utils')


class CredentialManager:
    """Manages temporary credential storage with encryption and TTL."""

    @staticmethod
    def _get_encryption_key():
        """Get or create encryption key for credentials."""
        key = getattr(settings, 'CREDENTIAL_ENCRYPTION_KEY', None)
        if not key:
            if settings.DEBUG:
                logger.warning("CREDENTIAL_ENCRYPTION_KEY not set. Deriving a dev key from SECRET_KEY.")
                digest = hashlib.sha256(settings.SECRET_KEY.encode()).digest()
                key = base64.urlsafe_b64encode(digest).decode()
            else:
                logger.critical("CREDENTIAL_ENCRYPTION_KEY is not set in Django settings! Please create one.")
                raise ValueError("CREDENTIAL_ENCRYPTION_KEY must be set in production")
        return key.encode()

    @staticmethod
    def _validate_token(credential_token):
        """Validate token format."""
        if not credential_token or not isinstance(credential_token, str):
            return False
        if len(credential_token) != 64:  # SHA256 hex length
            return False
        try:
            int(credential_token, 16)  # Verify it's valid hex
            return True
        except ValueError:
            return False

    @staticmethod
    def _sanitize_credentials(username, password):
        """Basic credential validation."""
        if not username or not password:
            raise ValueError("Username and password cannot be empty")
        if not isinstance(username, str) or not isinstance(password, str):
            raise ValueError("Credentials must be strings")
        if len(username.strip()) == 0 or len(password.strip()) == 0:
            raise ValueError("Credentials cannot be just whitespace")
        return username.strip(), password

    @staticmethod
    def store_credentials(username, password, ttl_seconds=300):
        """
        Store credentials temporarily with encryption.
        :return: Unique token to retrieve credentials, or None on failure.
        """
        try:
            username, password = CredentialManager._sanitize_credentials(username, password)

            timestamp = str(datetime.now().timestamp())
            entropy = secrets.token_hex(16)
            token_data = f"{username}:{timestamp}:{entropy}"
            credential_token = hashlib.sha256(token_data.encode()).hexdigest()

            fernet = Fernet(CredentialManager._get_encryption_key())
            credentials_to_encrypt = f"{username}:{password}"
            encrypted_data = fernet.encrypt(credentials_to_encrypt.encode())

            cache_key = f"portal_creds:{credential_token}"
            cache.set(cache_key, encrypted_data, timeout=ttl_seconds)

            logger.debug(f"Stored credentials for user '{username[0]}***' with token {credential_token[:8]}...")
            return credential_token

        except Exception as e:
            logger.error(f"Failed to store credentials: {type(e).__name__} - {e}", exc_info=True)
            return None

    @staticmethod
    def retrieve_credentials(credential_token):
        """
        Retrieve and decrypt credentials using token.
        :return: dict {'username': str, 'password': str} or None if not found/expired/decryption error.
        """
        if not CredentialManager._validate_token(credential_token):
            logger.warning("Invalid credential token format")
            return None

        try:
            cache_key = f"portal_creds:{credential_token}"
            encrypted_data = cache.get(cache_key)

            if not encrypted_data:
                logger.warning(f"Credentials not found or expired for token {credential_token[:8]}...")
                return None

            fernet = Fernet(CredentialManager._get_encryption_key())
            decrypted_data_bytes = fernet.decrypt(encrypted_data)
            decrypted_data_str = decrypted_data_bytes.decode()

            # Split on first colon to handle passwords with colons
            username, password = decrypted_data_str.split(':', 1)

            return {'username': username, 'password': password}

        except Exception as e:
            logger.error(
                f"Failed to retrieve or decrypt credentials for token {credential_token[:8]}...: {type(e).__name__} - {e}",
                exc_info=True)
            return None

    @staticmethod
    def delete_credentials(credential_token):
        """Delete credentials immediately."""
        if not CredentialManager._validate_token(credential_token):
            return

        try:
            cache_key = f"portal_creds:{credential_token}"
            cache.delete(cache_key)
            logger.debug(f"Deleted credentials for token {credential_token[:8]}...")
        except Exception as e:
            logger.error(f"Failed to delete credentials for token {credential_token[:8]}...: {type(e).__name__} - {e}",
                         exc_info=True)


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


@dataclass
class ToolResult:
    """
    Tracks and summarizes the results of a tool.
    """
    success: bool = False
    processed: int = 0
    actions_taken: int = 0
    warnings_sent: int = 0
    errors: int = 0
    error_messages: list = field(default_factory=list)
    execution_time: float = 0.0
    portal_alias: str = ""
    tool_name: str = ""

    # Tool-specific metrics
    extra_metrics: dict = field(default_factory=dict)

    def set_success(self, status: bool = True):
        """Marks the tool execution as successful or unsuccessful."""
        self.success = status

    def add_error(self, error_message: str):
        """
        Logs an error message and increments the error count.

        :param error_message: The error message to log
        """
        self.errors += 1
        self.error_messages.append(error_message)
        self.success = False

    def add_extra_metric(self, key: str, value):
        """
        Adds a tool-specific metric (e.g., 'inactive_found', 'unshared').

        :param key: The metric name
        :param value: The metric value
        """
        self.extra_metrics[key] = value

    def get_summary(self):
        """Returns a summary of tool execution results for admin reporting."""
        return {
            'portal_alias': self.portal_alias,
            'tool_name': self.tool_name,
            'success': self.success,
            'execution_time_seconds': self.execution_time,
            'processed': self.processed,
            'actions_taken': self.actions_taken,
            'warnings_sent': self.warnings_sent,
            'errors': self.errors,
            'has_errors': self.errors > 0,
            'extra_metrics': self.extra_metrics
        }

    def get_detailed_results(self):
        """Returns detailed results including error messages for comprehensive logging."""
        return {
            'summary': self.get_summary(),
            'error_messages': self.error_messages
        }

    def format_for_email(self):
        """Formats the results for inclusion in admin notification emails."""
        email_content = f"""
Tool Execution Summary for {self.portal_alias}:

Tool: {self.tool_name.replace('_', ' ').title()}
Status: {'SUCCESS' if self.success else 'FAILED'}
Execution Time: {self.execution_time:.2f} seconds

Statistics:
- Items Processed: {self.processed}
- Actions Taken: {self.actions_taken}
- Warnings Sent: {self.warnings_sent}
- Errors: {self.errors}
"""

        # Add extra metrics if any
        if self.extra_metrics:
            email_content += "\nAdditional Metrics:\n"
            for key, value in self.extra_metrics.items():
                formatted_key = key.replace('_', ' ').title()
                email_content += f"- {formatted_key}: {value}\n"

        # Add error details if any
        if self.error_messages:
            email_content += f"\nErrors Encountered ({len(self.error_messages)}):\n"
            for i, error in enumerate(self.error_messages[:10], 1):  # Limit to first 10 errors
                email_content += f"{i}. {error}\n"
            if len(self.error_messages) > 10:
                email_content += f"... and {len(self.error_messages) - 10} more errors\n"

        return email_content

    def to_json(self):
        """Converts the instance to a JSON-serializable dictionary."""
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
        target_gis = gis.GIS(url, username, password, verify_cert=False)
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


def connect(portal_model_instance, credential_token=None):
    """
    Establishes a connection to an ArcGIS instance defined by a Portal model.

    Uses provided credentials if given, otherwise attempts to use stored credentials
    from the portal_model_instance.

    :param portal_model_instance: The Portal model instance.
    :type portal_model_instance: enterpriseviz.models.Portal
    :param credential_token:Token for temporary credentials.
    :type credential_token: str, optional
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

    gis_kwargs = {'verify_cert': False}
    target_gis = None

    try:
        # Determine credentials to use
        if credential_token:
            # Use temporary credentials from token
            creds = CredentialManager.retrieve_credentials(credential_token)
            if not creds:
                logger.error(f"Failed to retrieve credentials for portal {portal_model_instance.alias}")
                raise ConnectionError("Authentication failed. Credentials not found or expired")

            username = creds['username']
            password = creds['password']
            logger.info(f"Using temporary credentials for portal {portal_model_instance.alias}")
            target_gis = gis.GIS(url, username, password, **gis_kwargs)

        elif portal_model_instance.token and portal_model_instance.token_expiration and portal_model_instance.token_expiration > timezone.now():
            logger.debug(f"Using valid stored token for {url}.")
            target_gis = gis.GIS(url, token=portal_model_instance.token, **gis_kwargs)
            if target_gis.properties.isPortal:
                _update_portal_token_info(portal_model_instance, target_gis)

        elif portal_model_instance.store_password and portal_model_instance.username and portal_model_instance.password:
            logger.debug(f"Using stored credentials for {url}.")
            target_gis = gis.GIS(url, portal_model_instance.username, portal_model_instance.password, **gis_kwargs)

        else:
            logger.warning(f"No valid credentials or token for {url}. Authentication may fail or be anonymous.")
            raise ConnectionError(f"No valid credentials or token for {url}.")

    except Exception as e:
        logger.error(f"Failed to establish an authenticated session for {url}: {e}", exc_info=True)
        raise ConnectionError(f"Failed to establish an authenticated session for {url}.")

    if not target_gis.properties.isPortal and not portal_model_instance.org_id:  # is AGOL
        _fetch_and_save_org_id(portal_model_instance, target_gis)

    # Ensure connection is actually established (GIS() can sometimes not raise error on init)
    if not target_gis._con.is_logged_in and (
        credential_token or portal_model_instance.store_password):
        logger.error(f"Failed to establish an authenticated session for {url}.")
        raise ConnectionError(f"Failed to establish an authenticated session for {url}.")

    logger.info(f"Successfully connected to {url} (Instance: {portal_model_instance.alias}).")
    return target_gis


def _update_portal_token_info(portal_model_instance, target_gis_connection):
    """Helper to update token info on the Portal model instance."""
    try:
        if hasattr(target_gis_connection._con, 'token') and hasattr(target_gis_connection._con, '_expiration'):
            new_token = target_gis_connection._con.token
            minutes_to_expiry = getattr(target_gis_connection._con, '_expiration', 60)
            if isinstance(minutes_to_expiry, (int, float)):
                new_expiration = timezone.now() + timedelta(minutes=minutes_to_expiry)
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
    :return: Dictionary with 'graph_data' (JSON string), 'services' (QuerySet),
             'apps' (QuerySet with usage_type), 'layers' (list),
             and 'item' (Webmap model instance). Returns {'error': msg} on failure.
    :rtype: dict
    """
    try:
        webmap_item = Webmap.objects.select_related('portal_instance').get(webmap_id=item_id)
        logger.debug(f"Retrieved Webmap '{webmap_item.webmap_title}' (ID: {item_id}).")

        # Initialize flat structure
        nodes = []
        links = []
        added_node_ids = set()

        # Add the root map node
        map_node_id = f"map-{webmap_item.id}"
        nodes.append({
            "id": map_node_id,
            "name": webmap_item.webmap_title,
            "type": "map",
            "url": webmap_item.webmap_url,
            "instance": webmap_item.portal_instance.alias if webmap_item.portal_instance else None
        })
        added_node_ids.add(map_node_id)

        services = webmap_item.service_set.select_related('portal_instance').distinct()
        apps = webmap_item.app_set.select_related('portal_instance', 'app_owner') \
            .annotate(usage_type=F("app_map__rel_type")).distinct()

        layers = webmap_item.webmap_layers or []
        logger.debug(
            f"map_details: Found {services.count()} services, {apps.count()} apps, {len(layers)} layers for Webmap {item_id}.")

        # Add service nodes and links
        for service in services:
            service_id = f"service-{service.id}"

            # Add service node (if not already added)
            if service_id not in added_node_ids:
                nodes.append({
                    "id": service_id,
                    "name": service.service_name,
                    "type": "service",
                    "url": service.service_url_as_list()[0] if service.service_url_as_list() else service.service_url,
                    "instance": service.portal_instance.alias if service.portal_instance else None
                })
                added_node_ids.add(service_id)

            # Add link from map to service
            links.append({
                "source": map_node_id,
                "target": service_id
            })

        # Add app nodes and links
        for app in apps:
            app_id = f"app-{app.id}"

            # Add app node (if not already added)
            if app_id not in added_node_ids:
                nodes.append({
                    "id": app_id,
                    "name": app.app_title,
                    "type": app.app_type.title(),
                    "url": app.app_url,
                    "instance": app.portal_instance.alias if app.portal_instance else None
                })
                added_node_ids.add(app_id)

            # Add link from map to app
            links.append({
                "source": map_node_id,
                "target": app_id
            })

        return {
            'graph_data': json.dumps({
                'nodes': nodes,
                'links': links
            }),
            "services": services,
            "apps": apps,
            "layers": layers,
            "item": webmap_item
        }

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
    :return: Dictionary with 'graph_data' (JSON string), 'maps' (QuerySet),
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

        # Initialize flat structure
        nodes = []
        links = []
        added_node_ids = set()

        # Add the root service node
        service_node_id = f"service-{service_item.id}"
        nodes.append({
            "id": service_node_id,
            "name": service_item.service_name,
            "type": "service",
            "url": service_item.service_url_as_list()[0] if service_item.service_url_as_list() else None,
            "instance": portal_alias
        })
        added_node_ids.add(service_node_id)

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

        # Add layer nodes and links
        for layer in layers:
            layer_id = f"layer-{layer.layer_name}"

            # Add layer node (if not already added)
            if layer_id not in added_node_ids:
                nodes.append({
                    "id": layer_id,
                    "name": layer.layer_name,
                    "type": "layer",
                    "url": None,
                    "instance": None
                })
                added_node_ids.add(layer_id)

            # Add link from service to layer
            links.append({
                "source": service_node_id,
                "target": layer_id
            })

        # Add map nodes and links
        for map_item in maps:
            map_id = f"map-{map_item.id}"

            # Add map node (if not already added)
            if map_id not in added_node_ids:
                nodes.append({
                    "id": map_id,
                    "name": map_item.webmap_title,
                    "type": "map",
                    "url": map_item.webmap_url,
                    "instance": map_item.portal_instance.alias if map_item.portal_instance else None
                })
                added_node_ids.add(map_id)

            # Add link from service to map
            links.append({
                "source": service_node_id,
                "target": map_id
            })

            # Add app nodes and links (via maps)
            for app_rel in map_item.app_map_set.all():
                if app_rel.app_id:
                    app_id = f"app-{app_rel.app_id.id}"

                    # Add app node (if not already added)
                    if app_id not in added_node_ids:
                        nodes.append({
                            "id": app_id,
                            "name": app_rel.app_id.app_title,
                            "type": app_rel.app_id.app_type.title(),
                            "url": app_rel.app_id.app_url,
                            "instance": app_rel.app_id.portal_instance.alias if app_rel.app_id.portal_instance else None
                        })
                        added_node_ids.add(app_id)

                    # Add link from map to app
                    links.append({
                        "source": map_id,
                        "target": app_id
                    })

        # Add direct service → app links (bypassing maps)
        for app in apps_direct:
            app_id = f"app-{app.id}"

            # Add app node (if not already added)
            if app_id not in added_node_ids:
                nodes.append({
                    "id": app_id,
                    "name": app.app_title,
                    "type": app.app_type.title(),
                    "url": app.app_url,
                    "instance": app.portal_instance.alias if app.portal_instance else None
                })
                added_node_ids.add(app_id)

            # Add direct link from service to app
            links.append({
                "source": service_node_id,
                "target": app_id
            })

        return {
            'graph_data': json.dumps({
                'nodes': nodes,
                'links': links
            }),
            "maps": maps,
            "apps": apps_combined,
            "layers": layers,
            "item": service_item
        }

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
        layer_item = Layer.objects.filter(layer_name=name, layer_server=dbserver, layer_database=database,
                                          layer_version=version).first()
        layer_items = Layer.objects.filter(layer_filter_q).select_related('portal_instance')
        if not layer_items.exists():
            logger.warning(f"Layer '{name}' not found with specified criteria.")
            return {"error": f"Layer '{name}' not found."}

        # Initialize flat structure
        nodes = []
        links = []
        added_node_ids = set()

        # Add the root layer node
        layer_node_id = f"layer-{name}"
        nodes.append({
            "id": layer_node_id,
            "name": name,
            "type": "layer",
            "url": None,
            "instance": None
        })
        added_node_ids.add(layer_node_id)

        # Services directly associated with the layer
        services = (
            Service.objects
            .filter(layer__in=layer_items)
            .select_related('portal_instance')
            .prefetch_related(
                Prefetch(
                    'layers',
                    queryset=Layer.objects.filter(layer_name=name),
                    to_attr='filtered_layers'
                )
            )
            .distinct()
        )
        if not services.exists():
            logger.warning(f"No services found directly associated with layer(s) '{name}'.")

        # Maps that use services which contain the layer
        maps = Webmap.objects.filter(map_service__service_id__layer__in=layer_items).select_related(
            'portal_instance').distinct()

        # Apps related via maps
        apps_via_maps = App.objects.filter(app_map__webmap_id__map_service__service_id__layer__in=layer_items) \
            .annotate(usage_type=F("app_map__rel_type")) \
            .select_related('app_owner', 'portal_instance').distinct()

        # Apps directly related to services
        apps_direct = App.objects.filter(app_service__service_id__layer__in=layer_items) \
            .annotate(usage_type=F("app_service__rel_type")) \
            .select_related('app_owner', 'portal_instance').distinct()

        apps_combined = combine_apps(apps_direct.union(apps_via_maps))

        logger.debug(
            f"Found {services.count()} services, {maps.count()} maps, {len(apps_combined)} apps for Layer '{name}'."
        )

        # Add service nodes and links
        for service in services:
            service_id = f"service-{service.id}"

            # Add service node (if not already added)
            if service_id not in added_node_ids:
                nodes.append({
                    "id": service_id,
                    "name": service.service_name,
                    "type": "service",
                    "url": service.service_url_as_list()[0] if service.service_url_as_list() else None,
                    "instance": service.portal_instance.alias if service.portal_instance else None
                })
                added_node_ids.add(service_id)

            # Add link from layer to service
            links.append({
                "source": layer_node_id,
                "target": service_id
            })

            # Add map nodes and links
            for map_instance in service.maps.all():
                map_id = f"map-{map_instance.id}"

                # Add map node (if not already added)
                if map_id not in added_node_ids:
                    nodes.append({
                        "id": map_id,
                        "name": map_instance.webmap_title,
                        "type": "map",
                        "url": map_instance.webmap_url,
                        "instance": map_instance.portal_instance.alias if map_instance.portal_instance else None
                    })
                    added_node_ids.add(map_id)

                # Add link from service to map
                links.append({
                    "source": service_id,
                    "target": map_id
                })

                # Add app nodes and links (via maps)
                for app_rel in map_instance.app_map_set.select_related('app_id').all():
                    if app_rel.app_id:
                        app_id = f"app-{app_rel.app_id.id}"

                        # Add app node (if not already added)
                        if app_id not in added_node_ids:
                            nodes.append({
                                "id": app_id,
                                "name": app_rel.app_id.app_title,
                                "type": app_rel.app_id.app_type.title(),
                                "url": app_rel.app_id.app_url,
                                "instance": app_rel.app_id.portal_instance.alias if app_rel.app_id.portal_instance else None
                            })
                            added_node_ids.add(app_id)

                        # Add link from map to app
                        links.append({
                            "source": map_id,
                            "target": app_id
                        })

            # Add direct service → app links (bypassing maps)
            for app in apps_direct.filter(app_service__service_id=service):
                app_id = f"app-{app.id}"

                # Add app node (if not already added)
                if app_id not in added_node_ids:
                    nodes.append({
                        "id": app_id,
                        "name": app.app_title,
                        "type": app.app_type.title(),
                        "url": app.app_url,
                        "instance": app.portal_instance.alias if app.portal_instance else None
                    })
                    added_node_ids.add(app_id)

                # Add direct link from service to app
                links.append({
                    "source": service_id,
                    "target": app_id
                })

        return {
            'graph_data': json.dumps({
                'nodes': nodes,
                'links': links
            }),
            "services": services,
            "maps": maps,
            "apps": apps_combined,
            "item": layer_item
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
    :rtype: datetime or None
    """
    if epoch_ms is None or epoch_ms == -1:  # Common ArcGIS placeholder for no date
        return None
    try:
        if not isinstance(epoch_ms, (int, float)):
            raise TypeError("Timestamp must be a number.")
        dt_utc = datetime.fromtimestamp(epoch_ms / 1000.0, tz=dt_timezone.utc)
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
        return datetime.fromtimestamp(epoch_ms / 1000.0).strftime("%Y-%m-%d")
    except (TypeError, ValueError) as e:
        logger.warning(f"Invalid timestamp {epoch_ms}: {e}")
        return None


def service_layer_details(portal_model_instance, layer_url_to_find):
    """
    Identifies web maps and apps in a portal that reference a specific layer URL.

    This function checks for usage in two ways:
    1. Specific layer ID usage: When the Map_Service/App_Service has a service_layer_id matching the layer
    2. Full service usage: When the Map_Service/App_Service references the entire service (service_layer_id is NULL)


    :param portal_model_instance: Portal model instance to search.
    :type portal_model_instance: enterpriseviz.models.Portal
    :param layer_url_to_find: URL of the layer to search for (e.g., "https://server/service/MapServer/5").
    :type layer_url_to_find: str
    :return: Dictionary with 'maps' (list of map dicts) and 'apps' (list of app dicts).
             Returns {'error': msg} on failure.
    :rtype: dict
    """
    if not layer_url_to_find:
        return {"error": "Layer URL to find cannot be empty."}

    normalized_layer_url = layer_url_to_find.strip().rstrip('/')

    # Parse the URL to extract service URL and layer ID
    url_parts = normalized_layer_url.split("/")
    service_layer_id = None
    service_base_url = normalized_layer_url

    if url_parts and url_parts[-1].isdigit():
        service_layer_id = int(url_parts[-1])
        service_base_url = "/".join(url_parts[:-1])

    logger.debug(
        f"Searching for layer '{normalized_layer_url}' (service: '{service_base_url}', layer_id: {service_layer_id}) in portal '{portal_model_instance.alias}'.")

    found_maps = []
    found_apps = []

    try:
        # First, try to find usage via database relationships (faster and more reliable)
        from .models import Service, Map_Service, App_Service, Webmap, App

        # Find the service
        try:
            service_obj = Service.objects.get(
                portal_instance=portal_model_instance,
                service_url__overlap=[service_base_url]
            )

            logger.debug(f"Found service in database: {service_obj.service_name}")

            # Find webmaps using this service
            if service_layer_id is not None:
                # Look for webmaps using this specific layer OR the entire service
                map_services = Map_Service.objects.filter(
                    portal_instance=portal_model_instance,
                    service_id=service_obj
                ).filter(
                    Q(service_layer_id=service_layer_id) | Q(service_layer_id__isnull=True)
                ).select_related('webmap_id')
            else:
                # Looking for full service usage
                map_services = Map_Service.objects.filter(
                    portal_instance=portal_model_instance,
                    service_id=service_obj,
                    service_layer_id__isnull=True
                ).select_related('webmap_id')

            for map_service in map_services:
                webmap = map_service.webmap_id
                found_maps.append({
                    "portal_instance": portal_model_instance.alias,
                    "id": webmap.webmap_id,
                    "title": webmap.webmap_title,
                    "url": webmap.webmap_url,
                    "owner": webmap.webmap_owner.user_username if webmap.webmap_owner else None,
                    "created": webmap.webmap_created,
                    "modified": webmap.webmap_modified,
                    "access": webmap.webmap_access,
                    "type": "Web Map",
                    "views": webmap.webmap_views,
                    "usage_type": "specific_layer" if map_service.service_layer_id is not None else "full_service"
                })

            # Find apps using this service
            if service_layer_id is not None:
                # Look for apps using this specific layer OR the entire service
                app_services = App_Service.objects.filter(
                    portal_instance=portal_model_instance,
                    service_id=service_obj
                ).filter(
                    Q(service_layer_id=service_layer_id) | Q(service_layer_id__isnull=True)
                ).select_related('app_id')
            else:
                # Looking for full service usage
                app_services = App_Service.objects.filter(
                    portal_instance=portal_model_instance,
                    service_id=service_obj,
                    service_layer_id__isnull=True
                ).select_related('app_id')

            for app_service in app_services:
                app = app_service.app_id
                found_apps.append({
                    "portal_instance": portal_model_instance.alias,
                    "id": app.app_id,
                    "title": app.app_title,
                    "url": app.app_url,
                    "owner": app.app_owner.user_username if app.app_owner else None,
                    "created": app.app_created,
                    "modified": app.app_modified,
                    "access": app.app_access,
                    "type": app.app_type,
                    "views": app.app_views,
                    "usage_type": "specific_layer" if app_service.service_layer_id is not None else "full_service"
                })

            logger.debug(
                f"Found {len(found_maps)} maps and {len(found_apps)} apps using layer '{normalized_layer_url}' via database.")

        except Service.DoesNotExist:
            logger.debug(f"Service not found in database for URL: {service_base_url}, falling back to content search")
        except Exception as db_e:
            logger.warning(f"Error querying database relationships: {db_e}, falling back to content search")

        # If we didn't find anything in the database, or if we want to double-check, search via API
        # (This can be slower but catches items not yet synced to the database)
        if not found_maps and not found_apps:
            try:
                target_gis = connect(portal_model_instance)
            except ConnectionError as e:
                logger.error(f"Connection failed for portal {portal_model_instance.alias}: {e}")
                return {"error": f"Connection failed for portal {portal_model_instance.alias}."}

            search_query = "NOT owner:esri*"
            all_relevant_items = target_gis.content.search(
                search_query,
                item_type="Web Map, Web Mapping Application, Dashboard, Web AppBuilder, Experience Builder, Form, StoryMap",
                max_items=2000,
                outside_org=False
            )

            logger.debug(
                f"find_layer_usage: Found {len(all_relevant_items)} candidate items in '{portal_model_instance.alias}'.")

            normalized_search_url = normalized_layer_url.lower()
            normalized_service_url = service_base_url.lower()

            for item in all_relevant_items:
                try:
                    item_data_str = str(item.get_data()).lower()

                    # Check if this item uses the specific layer or the full service
                    has_specific_layer = normalized_search_url in item_data_str
                    has_full_service = service_layer_id is not None and normalized_service_url in item_data_str

                    if has_specific_layer or has_full_service:
                        usage_type = "specific_layer" if has_specific_layer else "full_service"

                        item_details = {
                            "portal_instance": portal_model_instance.alias,
                            "id": item.id,
                            "title": item.title,
                            "url": item.homepage,
                            "owner": item.owner,
                            "created": epoch_to_datetime(item.created),
                            "modified": epoch_to_datetime(item.modified),
                            "access": item.access,
                            "type": item.type,
                            "views": item.numViews,
                            "usage_type": usage_type
                        }

                        if item.type == "Web Map":
                            # Avoid duplicates from database search
                            if not any(m["id"] == item.id for m in found_maps):
                                found_maps.append(item_details)
                        else:  # App types
                            if not any(a["id"] == item.id for a in found_apps):
                                found_apps.append(item_details)

                except Exception as item_e:
                    logger.warning(f"Error processing item '{item.id}' ({item.title}): {item_e}", exc_info=False)

            logger.debug(
                f"Found {len(found_maps)} maps and {len(found_apps)} apps using layer '{normalized_layer_url}' via API search.")


        return {"maps": found_maps, "apps": found_apps}

    except Exception as e:
        logger.error(f"Error searching for layer usage in '{portal_model_instance.alias}': {e}", exc_info=True)
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
                duplicates_found.append((item1, item2, similarity))
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


def get_metadata(portal_model_instance, credential_token=None):
    """
    Retrieves metadata compliance for content items in a portal.

    Connects to the portal, searches for non-Esri items, and checks each
    for metadata completeness using `get_missing_item_attrs`.

    :param portal_model_instance: Portal model instance.
    :type portal_model_instance: enterpriseviz.models.Portal
    :param credential_token: Token for temporary credentials (optional)
    :type credential_token: str
    :return: Dictionary with 'metadata' (list of compliance dicts).
             Returns {'error': msg} on failure.
    :rtype: dict
    """
    logger.debug(f"Fetching metadata for portal '{portal_model_instance.alias}'.")
    try:
        target_gis = connect(portal_model_instance, credential_token)
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
    """
    Extracts all URLs found in a string using regex.

    :param text_string: String to search for URLs.
    :type text_string: str
    :return: List of URLs found in the string.
    :rtype: list
    """
    if isinstance(text_string, str):
        return URL_REGEX.findall(text_string)
    return []


def extract_webappbuilder(data_structure, current_path="", parent_key_for_url=None):
    """
    Extracts URLs and itemIds from ArcGIS Web AppBuilder and Instant Apps JSON data with context.

    :param data_structure: The Web AppBuilder/Instant App JSON data (as Python dict/list).
    :type data_structure: dict or list or str
    :param current_path: Internal use for recursion: current dot-separated path.
    :type current_path: str, optional
    :param parent_key_for_url: Internal use for recursion: key of the parent dict.
    :type parent_key_for_url: str, optional
    :return: List of [value, path_to_value, parent_key, value_type, context] lists.
             value_type is "url" or "itemId"
             context is "map", "datasource", "search", "filter", etc.
    :rtype: list
    """
    extracted_resources = []

    # First, check specific known structures for itemIds and URLs (only on initial call)
    if not current_path:
        itemids = []

        # Primary web map reference (Web AppBuilder & Instant Apps)
        if "map" in data_structure and isinstance(data_structure["map"], dict):
            try:
                map_item_id = data_structure["map"].get("itemId")
                if map_item_id:
                    extracted_resources.append([map_item_id, "map", "itemId", "map", "map"])
                    itemids.append(map_item_id)
            except:
                pass

        # Instant Apps - web map in values.webmap
        try:
            webmap_id = data_structure.get("values", {}).get("webmap")
            if webmap_id and webmap_id not in itemids:
                extracted_resources.append([webmap_id, "values.webmap", "webmap", "map", "map"])
                itemids.append(webmap_id)
        except:
            pass

        # Data sources (Web AppBuilder)
        if "dataSource" in data_structure:
            data_sources = data_structure.get("dataSource", {}).get("dataSources", {})
            if isinstance(data_sources, dict):
                for ds_key, ds_value in data_sources.items():
                    if isinstance(ds_value, dict):
                        try:
                            ds_item_id = ds_value.get("itemId")
                            if ds_item_id and ds_item_id not in itemids:
                                path = f"dataSource.dataSources.{ds_key}"
                                extracted_resources.append([ds_item_id, path, "itemId", "datasource", "datasource"])
                                itemids.append(ds_item_id)
                        except:
                            pass

        # Search (Instant Apps)
        try:
            search_config = data_structure.get("values", {}).get("searchConfiguration", {})
            sources = search_config.get("sources", [])

            for idx, source in enumerate(sources):
                if not isinstance(source, dict):
                    continue

                # Check for layer-based search (has a service URL)
                layer_info = source.get("layer", {})
                if isinstance(layer_info, dict) and "url" in layer_info:
                    search_url = layer_info["url"]
                    path = f"values.searchConfiguration.sources[{idx}].layer.url"
                    extracted_resources.append([search_url, path, "url", "url", "search"])

                # Check for geocoder URL (world geocoding service, etc.)
                elif "url" in source and source.get("url", "").startswith("http"):
                    geocoder_url = source["url"]
                    # Skip if it's a standard Esri geocoder (not a dependency we track)
                    if "GeocodeServer" in geocoder_url and "arcgis.com" not in geocoder_url:
                        path = f"values.searchConfiguration.sources[{idx}].url"
                        extracted_resources.append([geocoder_url, path, "url", "url", "search"])
        except Exception as e:
            pass

        # Also check draft search configuration
        try:
            draft_search_config = data_structure.get("values", {}).get("draft", {}).get("searchConfiguration", {})
            draft_sources = draft_search_config.get("sources", [])

            for idx, source in enumerate(draft_sources):
                if not isinstance(source, dict):
                    continue

                layer_info = source.get("layer", {})
                if isinstance(layer_info, dict) and "url" in layer_info:
                    search_url = layer_info["url"]
                    # Check if we already have this URL
                    if not any(r[0] == search_url and r[4] == "search" for r in extracted_resources):
                        path = f"values.draft.searchConfiguration.sources[{idx}].layer.url"
                        extracted_resources.append([search_url, path, "url", "url", "search"])
        except:
            pass

        # Filter (Instant Apps)
        try:
            filter_config = data_structure.get("values", {}).get("filterConfig", {})
            layer_expressions = filter_config.get("layerExpressions", [])

            for idx, layer_expr in enumerate(layer_expressions):
                if isinstance(layer_expr, dict):
                    layer_id = layer_expr.get("id")
                    if layer_id:
                        # This is a layer ID reference from the map
                        path = f"values.filterConfig.layerExpressions[{idx}]"
                        extracted_resources.append([layer_id, path, "id", "filter_layer_ref", "filter"])
        except:
            pass

        # Also check draft filter configuration
        try:
            draft_filter_config = data_structure.get("values", {}).get("draft", {}).get("filterConfig", {})
            draft_layer_expressions = draft_filter_config.get("layerExpressions", [])

            for idx, layer_expr in enumerate(draft_layer_expressions):
                if isinstance(layer_expr, dict):
                    layer_id = layer_expr.get("id")
                    if layer_id:
                        path = f"values.draft.filterConfig.layerExpressions[{idx}]"
                        # Check if we already have this layer_id
                        if not any(r[0] == layer_id and r[4] == "filter" for r in extracted_resources):
                            extracted_resources.append([layer_id, path, "id", "filter_layer_ref", "filter"])
        except:
            pass

    # URL extraction for Web AppBuilder
    url_results = _extract_urls_recursive(data_structure, current_path, parent_key_for_url)

    # Merge URL results, avoiding duplicates
    existing_urls = {r[0] for r in extracted_resources if r[3] == "url"}
    for url, path, parent_key in url_results:
        if url not in existing_urls:
            # Determine context from path
            if "searchLayers" in path or "search" in path.lower():
                context = "search"
            elif "filters" in path or "filter" in path.lower():
                context = "filter"
            elif "widgets" in path or "widget" in path.lower():
                context = "widget"
            else:
                context = "other"

            extracted_resources.append([url, path, parent_key, "url", context])
            existing_urls.add(url)

    return extracted_resources


def _extract_urls_recursive(data_structure, current_path="", parent_key_for_url=None):
    """
    Helper function for recursive URL extraction.

    Separated to avoid confusion with itemId extraction.

    :param data_structure: Data structure to recursively search (dict, list, or str).
    :type data_structure: dict or list or str
    :param current_path: Current path in the data structure.
    :type current_path: str
    :param parent_key_for_url: Parent key containing the URL.
    :type parent_key_for_url: str or None
    :return: List of [url, path, parent_key] lists.
    :rtype: list
    """
    extracted_urls = []

    if isinstance(data_structure, dict):
        for key, value in data_structure.items():
            new_path = f"{current_path}.{key}" if current_path else key
            extracted_urls.extend(_extract_urls_recursive(value, new_path, key))

    elif isinstance(data_structure, list):
        for idx, item in enumerate(data_structure):
            new_path = f"{current_path}[{idx}]"
            extracted_urls.extend(_extract_urls_recursive(item, new_path, parent_key_for_url))

    elif isinstance(data_structure, str):
        found_in_string = _extract_urls_from_string(data_structure)
        for url in found_in_string:
            extracted_urls.append([url, current_path, parent_key_for_url])

    return extracted_urls


def _recursive_extract_by_key(data, target_key, current_path_list=None, results_list=None):
    """
    Generic helper to recursively find values for a specific key.

    :param data: Data structure to search (dict or list).
    :type data: dict or list
    :param target_key: Key to search for.
    :type target_key: str
    :param current_path_list: Current path as list of keys.
    :type current_path_list: list or None
    :param results_list: Accumulated results list.
    :type results_list: list or None
    :return: List of (path_string, key, value, type_at_level) tuples.
    :rtype: list
    """
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
    Extracts itemIds from ArcGIS Dashboard JSON data with relationship context.

    :param dashboard_data: The Dashboard JSON data (as Python dict).
    :type dashboard_data: dict
    :return: List of (path_string, context_type, itemId_value, widget_type) tuples.
    :rtype: list
    """
    deps = []

    # Widgets from multiple widget sources
    widget_sources = [
        ("widgets", dashboard_data.get("widgets", [])),
        ("desktopView.widgets", dashboard_data.get("desktopView", {}).get("widgets", []))
    ]

    for source_path, widgets in widget_sources:
        for widget_idx, widget in enumerate(widgets):
            widget_type = widget.get("type", "unknown")
            path_prefix = f"{source_path}[{widget_idx}]"

            # Map widgets - direct web map reference
            if widget_type == "mapWidget":
                item_id = widget.get("itemId")
                if item_id:
                    deps.append((path_prefix, "map", item_id, widget_type))
                continue

            # Service datasets in other widget types
            try:
                datasets = widget.get("datasets", [])
                for dataset_idx, dataset in enumerate(datasets):
                    if dataset.get("type") == "serviceDataset":
                        data_source = dataset.get("dataSource", {})
                        data_source_type = data_source.get("type")
                        path = f"{path_prefix}.datasets[{dataset_idx}]"

                        # Item-based data source
                        if data_source_type == "itemDataSource":
                            item_id = data_source.get("itemId")
                            if item_id:
                                deps.append((path, "dataset", item_id, widget_type))

                        # Arcade expression data source
                        elif data_source_type == "arcadeDataSource":
                            script = data_source.get("script", "")
                            if script:
                                # Use regex to find GUIDs in Arcade expressions
                                guid_pattern = r"[0-9a-f]{8}[0-9a-f]{4}[1-5][0-9a-f]{3}[89ab][0-9a-f]{3}[0-9a-f]{12}"
                                guids = re.findall(guid_pattern, script, re.IGNORECASE)
                                for guid in guids:
                                    deps.append((f"{path}.script", "arcade", guid, widget_type))
            except Exception:
                pass

    # Fallback: recursively find any itemIds we might have missed
    # This catches edge cases and future dashboard features
    all_item_ids = _recursive_extract_by_key(dashboard_data, "itemId")
    existing_ids = {dep[2] for dep in deps}

    for path, key, item_id, _ in all_item_ids:
        if item_id not in existing_ids:
            deps.append((path, "other", item_id, "unknown"))

    return deps


def extract_experiencebuilder(exb_data, item=None):
    """
    Extracts itemIds and URLs from ArcGIS Experience Builder JSON data with relationship context.

    :param exb_data: The Experience Builder published JSON data (as Python dict).
    :type exb_data: dict
    :param item: Optional ArcGIS Item object to access draft data.
    :type item: arcgis.gis.Item
    :return: List of (path_string, context_type, value, resource_type) tuples.
    :rtype: list
    """

    def is_service_url(url):
        """Check if URL is a service URL (REST endpoint) rather than an app URL"""
        if not isinstance(url, str):
            return False
        url_lower = url.lower()
        service_patterns = [
            '/rest/services/',
            '/rest/admin/services/'
        ]
        return any(pattern in url_lower for pattern in service_patterns)

    deps = []
    processed_refs = set()  # Track (value, context) pairs to avoid duplicates

    # Gather both published and draft data
    data_to_process = [("published", exb_data)]

    if item:
        try:
            draft_data = item.resources.get("config/config.json")
            if draft_data:
                data_to_process.append(("draft", draft_data))
        except:
            pass

    for data_label, data in data_to_process:
        # Build a complete map of ALL dataSource IDs (including nested children) for widget resolution
        data_sources = data.get("dataSources", {})
        datasource_map = {}

        def process_datasource(ds_key, ds_value, parent_id=""):
            """Recursively process datasources including nested children with concatenated IDs"""
            if isinstance(ds_value, dict):
                ds_type = ds_value.get("type", "unknown")

                # Build the full concatenated ID as Experience Builder does
                full_id = f"{parent_id}-{ds_key}" if parent_id else ds_key

                # Store datasource info with both full ID and just the key
                ds_info = {
                    "type": ds_type,
                    "value": None,
                    "kind": None
                }

                if "itemId" in ds_value:
                    ds_info["value"] = ds_value["itemId"]
                    ds_info["kind"] = "itemId"
                elif "url" in ds_value:
                    ds_info["value"] = ds_value["url"]
                    ds_info["kind"] = "url"

                # Store under both full ID and just the key for flexible lookup
                if ds_info["value"]:
                    datasource_map[full_id] = ds_info
                    datasource_map[ds_key] = ds_info

                # Process nested child dataSources
                child_datasources = ds_value.get("childDataSourceJsons", {})
                if isinstance(child_datasources, dict):
                    for child_key, child_value in child_datasources.items():
                        process_datasource(child_key, child_value, full_id)

        for ds_key, ds_value in data_sources.items():
            process_datasource(ds_key, ds_value)

        # Extract from widgets with their specific context types
        widgets = data.get("widgets", {})
        if isinstance(widgets, dict):
            for widget_key, widget_value in widgets.items():
                if isinstance(widget_value, dict):
                    widget_uri = widget_value.get("uri", "")
                    config = widget_value.get("config") or {}

                    # Determine context type from widget URI
                    context_type = "widget"  # default
                    if "search" in widget_uri:
                        context_type = "search"
                    elif "filter" in widget_uri:
                        context_type = "filter"
                    elif "table" in widget_uri:
                        context_type = "table"
                    elif "arcgis-map" in widget_uri or "map" in widget_uri:
                        context_type = "map"
                    elif "query" in widget_uri:
                        context_type = "query"
                    elif "feature-info" in widget_uri:
                        context_type = "widget"

                    # Check for useDataSources at widget level
                    use_data_sources = widget_value.get("useDataSources", [])
                    if use_data_sources:
                        for use_ds in use_data_sources:
                            if isinstance(use_ds, dict):
                                # Get the specific dataSourceId (which may be a child layer)
                                # and the root dataSourceId (which is the web map)
                                ds_id = use_ds.get("dataSourceId")
                                root_ds_id = use_ds.get("rootDataSourceId")

                                # First, try to extract the specific layer reference (child dataSource)
                                # This gives us layer-level granularity for search, filter, table widgets
                                if ds_id and ds_id in datasource_map:
                                    ds_info = datasource_map[ds_id]

                                    # Only extract if it has a URL (i.e., it's a layer reference)
                                    # and it's not the same as the root (i.e., it's actually a child)
                                    if ds_info["kind"] == "url" and ds_id != root_ds_id:
                                        ref_key = (ds_info["value"], context_type)

                                        if ref_key not in processed_refs:
                                            path = f"{data_label}.widgets.{widget_key}.useDataSources[{ds_id}]"
                                            deps.append((path, context_type, ds_info["value"], ds_info["type"]))
                                            processed_refs.add(ref_key)

                                # Also extract the root dataSource if it's different and has an itemId
                                # This captures the web map dependency for map widgets
                                if root_ds_id and root_ds_id != ds_id and root_ds_id in datasource_map:
                                    root_info = datasource_map[root_ds_id]

                                    # Only extract root if it's an itemId (web map reference)
                                    if root_info["kind"] == "itemId":
                                        ref_key = (root_info["value"], context_type)

                                        if ref_key not in processed_refs:
                                            path = f"{data_label}.widgets.{widget_key}.useDataSources[root]"
                                            deps.append((path, context_type, root_info["value"], root_info["type"]))
                                            processed_refs.add(ref_key)

                    # Survey123 forms in widget config
                    if "surveyItemId" in config:
                        survey_id = config["surveyItemId"]
                        ref_key = (survey_id, "survey")
                        if ref_key not in processed_refs:
                            path = f"{data_label}.widgets.{widget_key}.config"
                            deps.append((path, "survey", survey_id, "SURVEY123"))
                            processed_refs.add(ref_key)

                    # Map widgets with itemId in config
                    if "itemId" in config:
                        item_id = config["itemId"]
                        # For map widgets, use "map" context
                        map_context = "map" if context_type == "map" else "widget"
                        ref_key = (item_id, map_context)
                        if ref_key not in processed_refs:
                            path = f"{data_label}.widgets.{widget_key}.config"
                            deps.append((path, map_context, item_id, "Web Map"))
                            processed_refs.add(ref_key)

        # Extract utilities (geocoding, routing, printing services, etc.)
        utilities = data.get("utilities", {})
        if isinstance(utilities, dict):
            for util_key, util_value in utilities.items():
                if isinstance(util_value, dict):
                    util_type = util_value.get("type", "unknown")

                    # Check for itemId in utilities
                    if "itemId" in util_value:
                        item_id = util_value["itemId"]
                        ref_key = (item_id, "utility")
                        if ref_key not in processed_refs:
                            path = f"{data_label}.utilities.{util_key}"
                            deps.append((path, "utility", item_id, util_type))
                            processed_refs.add(ref_key)

                    # Check for itemId in orgSetting (organization utilities)
                    org_setting = util_value.get("orgSetting", {})
                    if isinstance(org_setting, dict) and "itemId" in org_setting:
                        item_id = org_setting["itemId"]
                        ref_key = (item_id, "utility")
                        if ref_key not in processed_refs:
                            path = f"{data_label}.utilities.{util_key}.orgSetting"
                            deps.append((path, "utility", item_id, util_type))
                            processed_refs.add(ref_key)

                    # Check for URL in utilities
                    if "url" in util_value:
                        url = util_value["url"]
                        if url and url.startswith("http"):
                            ref_key = (url, "utility")
                            if ref_key not in processed_refs:
                                path = f"{data_label}.utilities.{util_key}"
                                deps.append((path, "utility", url, util_type))
                                processed_refs.add(ref_key)

        # Extract standalone dataSources that weren't referenced by widgets
        # These are truly just datasources without specific widget context
        def extract_standalone_datasource(ds_key, ds_value, parent_path=""):
            """Recursively extract standalone datasources including nested ones"""
            if isinstance(ds_value, dict):
                ds_type = ds_value.get("type", "unknown")
                full_key = f"{parent_path}.{ds_key}" if parent_path else ds_key

                # ItemId-based data sources
                if "itemId" in ds_value:
                    item_id = ds_value["itemId"]
                    ref_key = (item_id, "datasource")
                    if ref_key not in processed_refs:
                        path = f"{data_label}.dataSources.{full_key}"
                        deps.append((path, "datasource", item_id, ds_type))
                        processed_refs.add(ref_key)

                # URL-based data sources (feature layers, services)
                if "url" in ds_value:
                    url = ds_value["url"]
                    if url and url.startswith("http") and is_service_url(url):
                        ref_key = (url, "datasource")
                        if ref_key not in processed_refs:
                            path = f"{data_label}.dataSources.{full_key}"
                            deps.append((path, "datasource", url, ds_type))
                            processed_refs.add(ref_key)

                # Process nested child dataSources
                child_datasources = ds_value.get("childDataSourceJsons", {})
                if isinstance(child_datasources, dict):
                    for child_key, child_value in child_datasources.items():
                        extract_standalone_datasource(child_key, child_value, full_key)

        for ds_key, ds_value in data_sources.items():
            extract_standalone_datasource(ds_key, ds_value)

        # Find any itemIds or URLs we missed (fallback)
        all_item_ids = _recursive_extract_by_key(data, "itemId")
        all_urls = _recursive_extract_by_key(data, "_URL_")

        for path, key, item_id, type_at_level in all_item_ids:
            ref_key = (item_id, "other")
            if ref_key not in processed_refs:
                full_path = f"{data_label}.{path}"
                deps.append((full_path, "other", item_id, type_at_level or "unknown"))
                processed_refs.add(ref_key)

        for path, key, url, type_at_level in all_urls:
            if url and url.startswith("http") and is_service_url(url):
                ref_key = (url, "other")
                if ref_key not in processed_refs:
                    full_path = f"{data_label}.{path}"
                    deps.append((full_path, "other", url, type_at_level or "unknown"))
                    processed_refs.add(ref_key)

    return deps


def extract_storymap(storymap_data, item=None, current_path_list=None, results_list=None):
    """
    Extracts itemIds from ArcGIS StoryMap JSON data with relationship context.

    :param storymap_data: The StoryMap published JSON data (as Python dict).
    :type storymap_data: dict
    :param item: Optional ArcGIS Item object to access draft data.
    :type item: arcgis.gis.Item
    :param current_path_list: Internal use for recursion.
    :type current_path_list: list, optional
    :param results_list: Internal use for recursion.
    :type results_list: list, optional
    :return: List of (path_string, context_type, itemId_value, item_type) tuples.
    :rtype: list
    """
    if current_path_list is None:
        current_path_list = []
    if results_list is None:
        results_list = []

    # Gather both published and draft data (only on initial call)
    if not current_path_list and item:
        processed_ids = set()
        data_to_process = [("published", storymap_data)]

        # Get draft data
        try:
            for res in item.resources.list():
                if "draft" in res["resource"] and "express" not in res["resource"]:
                    draft_data = item.resources.get(res["resource"])
                    data_to_process.append(("draft", draft_data))
                    break  # Usually just one draft
        except:
            pass

        # Process each data source
        for data_label, data in data_to_process:
            if not isinstance(data, dict) or "resources" not in data:
                continue

            resources = data.get("resources", {})

            # Extract web maps
            for resource_key, resource_value in resources.items():
                if not isinstance(resource_value, dict):
                    continue

                resource_type = resource_value.get("type", "").lower()
                resource_data = resource_value.get("data", {})

                # Web Maps
                if "webmap" in resource_type or "web-map" in resource_type:
                    item_id = resource_data.get("itemId")
                    if item_id and item_id not in processed_ids:
                        path = f"{data_label}.resources.{resource_key}"
                        results_list.append((path, "map", item_id, "Web Map"))
                        processed_ids.add(item_id)

                # StoryMap Themes
                elif "story-theme" in resource_type or "theme" in resource_type:
                    theme_id = resource_data.get("themeItemId")
                    if theme_id and theme_id not in processed_ids:
                        path = f"{data_label}.resources.{resource_key}"
                        results_list.append((path, "theme", theme_id, "StoryMap Theme"))
                        processed_ids.add(theme_id)

            # Search for itemType/itemId pairs
            _extract_storymap_recursive(data, [data_label], results_list, processed_ids)

        return results_list

    return results_list


def _extract_storymap_recursive(data, current_path_list, results_list, processed_ids):
    """
    Recursively extracts itemType/itemId pairs from StoryMap data.

    Helper function for fallback extraction.

    :param data: Data structure to search (dict or list).
    :type data: dict or list
    :param current_path_list: Current path as list of keys.
    :type current_path_list: list
    :param results_list: Accumulated results list.
    :type results_list: list
    :param processed_ids: Set of already processed item IDs.
    :type processed_ids: set
    :return: None (modifies results_list in place)
    :rtype: None
    """
    if isinstance(data, dict):
        # StoryMaps often have 'itemType' and 'itemId' at the same level
        if "itemType" in data and "itemId" in data:
            item_type = data["itemType"]
            item_id = data["itemId"]
            if item_id not in processed_ids:
                path_str = ".".join(current_path_list)

                # Determine context from itemType
                if item_type in ["Web Map", "webmap"]:
                    context = "map"
                elif "theme" in item_type.lower():
                    context = "theme"
                else:
                    context = "embed"

                results_list.append((path_str, context, item_id, item_type))
                processed_ids.add(item_id)

        for key, value in data.items():
            new_path = current_path_list + [key]
            _extract_storymap_recursive(value, new_path, results_list, processed_ids)

    elif isinstance(data, list):
        for idx, item in enumerate(data):
            new_path = current_path_list + [str(idx)]
            _extract_storymap_recursive(item, new_path, results_list, processed_ids)


def extract_quickcapture(qc_data, item=None):
    """
    Extracts itemIds from ArcGIS QuickCapture Project JSON data.

    :param qc_data: The QuickCapture published JSON data (as Python dict).
    :type qc_data: dict
    :param item: Optional ArcGIS Item object to access project config.
    :type item: arcgis.gis.Item
    :return: List of (path_string, context_type, itemId_value, resource_type) tuples.
    :rtype: list
    """
    deps = []

    # Try to get the project configuration
    project_config = None
    if item:
        try:
            project_config = item.resources.get("qc.project.json")
        except:
            project_config = qc_data
    else:
        project_config = qc_data

    if not project_config:
        return deps

    # Extract basemap
    try:
        basemap = project_config.get("basemap", {})
        basemap_id = basemap.get("itemId")
        if basemap_id:
            deps.append(("basemap", "map", basemap_id, "Web Map"))
    except:
        pass

    # Extract data sources (feature services)
    try:
        data_sources = project_config.get("dataSources", [])
        for idx, ds in enumerate(data_sources):
            if isinstance(ds, dict):
                service_id = ds.get("featureServiceItemId")
                if service_id:
                    path = f"dataSources[{idx}]"
                    deps.append((path, "datasource", service_id, "Feature Service"))
    except:
        pass

    return deps


def extract_hub(hub_data, item=None):
    """
    Extracts itemIds from ArcGIS Hub Site or Hub Page JSON data.

    :param hub_data: The Hub published JSON data (as Python dict).
    :type hub_data: dict
    :param item: Optional ArcGIS Item object to access draft data.
    :type item: arcgis.gis.Item
    :return: List of (path_string, context_type, itemId_value, resource_type) tuples.
    :rtype: list
    """
    deps = []
    processed_ids = set()

    # Gather both published and draft data
    data_to_process = [("published", hub_data)]

    if item:
        try:
            for r in item.resources.list():
                if "draft" in r["resource"]:
                    draft_data = item.resources.get(r["resource"])
                    if isinstance(draft_data, dict) and "data" in draft_data:
                        data_to_process.append(("draft", draft_data["data"]))
                    else:
                        data_to_process.append(("draft", draft_data))
                    break
        except:
            pass

    for data_label, data in data_to_process:
        if not isinstance(data, dict):
            continue

        try:
            layout = data.get("values", {}).get("layout", {})
            sections = layout.get("sections", [])

            for section_idx, section in enumerate(sections):
                rows = section.get("rows", [])

                for row_idx, row in enumerate(rows):
                    cards = row.get("cards", [])

                    for card_idx, card in enumerate(cards):
                        component = card.get("component", {})
                        component_name = component.get("name", "")
                        settings = component.get("settings", {})

                        path_base = f"{data_label}.sections[{section_idx}].rows[{row_idx}].cards[{card_idx}]"

                        # Web Map cards
                        if component_name == "webmap-card":
                            for map_type in ["webmap", "webscene"]:
                                item_id = settings.get(map_type)
                                if item_id and item_id not in processed_ids:
                                    path = f"{path_base}.{map_type}"
                                    resource_type = "Web Scene" if map_type == "webscene" else "Web Map"
                                    deps.append((path, "map", item_id, resource_type))
                                    processed_ids.add(item_id)

                        # App cards (references other applications)
                        elif component_name == "app-card":
                            item_id = settings.get("itemId")
                            if item_id and item_id not in processed_ids:
                                path = f"{path_base}.itemId"
                                deps.append((path, "app", item_id, "Application"))
                                processed_ids.add(item_id)

                        # Chart cards (could be dashboards)
                        elif component_name == "chart-card":
                            item_id = settings.get("itemId")
                            if item_id and item_id not in processed_ids:
                                path = f"{path_base}.itemId"
                                deps.append((path, "chart", item_id, "Chart"))
                                processed_ids.add(item_id)

                        # Survey cards (Survey123)
                        elif component_name == "survey-card":
                            survey_id = settings.get("surveyId")
                            if survey_id and survey_id not in processed_ids:
                                path = f"{path_base}.surveyId"
                                deps.append((path, "survey", survey_id, "Form"))
                                processed_ids.add(survey_id)
        except Exception as e:
            pass

    return deps


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
            "app_id", "portal_instance_id", "app_title", "app_url", "app_type", "app_owner__user_username",
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


def generate_notification_tables(maps_qs, apps_qs):
    """
    Generates plain text and HTML table representations of maps and apps.

    :param maps_qs: QuerySet of Webmap model instances.
    :type maps_qs: django.db.models.QuerySet
    :param apps_qs: QuerySet of App model instances.
    :type apps_qs: django.db.models.QuerySet
    :return: Dictionary with 'plain' and 'html' table strings.
    :rtype: dict
    """
    # Extract data for tables - select only needed fields for efficiency
    map_rows = [(m.webmap_title, m.webmap_url, m.webmap_access,
                 m.webmap_created.strftime("%Y-%m-%d"), m.webmap_modified.strftime("%Y-%m-%d"), m.webmap_views)
                for m in maps_qs]
    app_rows = [(a.app_title, a.app_url, a.app_type, a.app_access,
                 a.app_created.strftime("%Y-%m-%d"), a.app_modified.strftime("%Y-%m-%d"), a.app_views)
                for a in apps_qs]

    plain_map_table = ""
    html_map_table = ""
    if map_rows:
        headers = ["Title", "URL", "Access", "Created", "Modified", "Views"]
        plain_map_table = tabulate(map_rows, headers=headers, tablefmt="grid")
        html_map_table = tabulate(map_rows, headers=headers, tablefmt="html")

    plain_app_table = ""
    html_app_table = ""
    if app_rows:
        headers = ["Title", "URL", "Type", "Access", "Created", "Modified", "Views"]
        plain_app_table = tabulate(app_rows, headers=headers, tablefmt="grid")
        html_app_table = tabulate(app_rows, headers=headers, tablefmt="html")

    plain_output = []
    if plain_map_table: plain_output.append(f"Maps:\n{plain_map_table}")
    if plain_app_table: plain_output.append(f"Apps:\n{plain_app_table}")

    html_output = []
    if html_map_table: html_output.append(f"<h2>Maps</h2>{html_map_table}")
    if html_app_table: html_output.append(f"<h2>Apps</h2>{html_app_table}")

    return {
        "plain": "\n\n".join(plain_output),
        "html": "<br><br>".join(html_output)
    }


def send_email(recipient_email, subject, message, html_message=None, bcc_emails=None):
    """
    Core email sending function that handles all email delivery.

    :param recipient_email: Email address of the recipient
    :type recipient_email: str
    :param subject: Subject line of the email
    :type subject: str
    :param message: Plain text body content of the email
    :type message: str
    :param html_message: Optional HTML version of the email body
    :type html_message: str or None
    :param bcc_emails: Optional list of BCC email addresses
    :type bcc_emails: list or None
    :return: Tuple containing success status (bool) and status message (str)
    :rtype: tuple(bool, str)
    """
    logger.debug(f"Starting send_email with recipient={recipient_email}, subject={subject}")

    # Validate recipient email
    if not recipient_email:
        logger.warning("Email failed: Recipient email address is missing")
        return False, "Recipient email address is missing."
    try:
        validate_email(recipient_email)
    except DjangoValidationError:
        logger.warning("Email failed: Invalid recipient email address")
        return False, "Invalid recipient email address."
    try:
        # Fetch email settings from SiteSettings
        logger.debug("Retrieving email configuration from SiteSettings")
        site_settings = SiteSettings.objects.first()

        # Check if email configuration is available
        if not site_settings:
            logger.warning("Email failed: No SiteSettings configuration found")
            return False, "Email configuration not found in SiteSettings."

        if not site_settings.from_email:
            logger.warning("Email failed: No 'From' address configured in SiteSettings")
            return False, "Email 'From' address not configured in SiteSettings."

        # Check if email host is configured
        if not site_settings.email_host:
            logger.warning("Email failed: No email host configured")
            return False, "Email host not configured in SiteSettings."

        from_addr = site_settings.from_email
        logger.debug(f"Using from address: {from_addr}")

        # Set up connection parameters
        connection_kwargs = {
            "host": site_settings.email_host,
            "port": site_settings.email_port,
            "username": site_settings.email_username or None,
            "password": site_settings.email_password or None,
            "use_tls": (site_settings.email_encryption == "starttls"),
            "use_ssl": (site_settings.email_encryption == "ssl"),
        }

        # Set up reply-to address
        reply_to_list = None
        if site_settings.reply_to:
            reply_to_list = [site_settings.reply_to]

        if reply_to_list:
            logger.debug(f"Using reply-to address: {reply_to_list[0]}")

        # Use connection context manager (like your original code)
        logger.debug(f"Setting up email connection to {site_settings.email_host}:{site_settings.email_port}")
        with get_connection(**connection_kwargs) as connection:
            logger.debug("Email connection established successfully")

            # Create email message
            logger.debug(f"Creating email message to {recipient_email}")

            if html_message:
                # Use EmailMultiAlternatives for HTML emails
                msg = EmailMultiAlternatives(
                    subject=subject,
                    body=message,
                    from_email=from_addr,
                    to=[recipient_email],
                    bcc=bcc_emails,
                    connection=connection,
                    reply_to=reply_to_list,
                )
                msg.attach_alternative(html_message, "text/html")
            else:
                # Use EmailMessage for plain text emails
                msg = EmailMessage(
                    subject=subject,
                    body=message,
                    from_email=from_addr,
                    to=[recipient_email],
                    bcc=bcc_emails,
                    connection=connection,
                    reply_to=reply_to_list,
                )

            # Send the email
            logger.debug(f"Sending email to {recipient_email}")
            msg.send(fail_silently=False)

        logger.info(f"Successfully sent email to {recipient_email}")
        return True, "Email sent successfully."

    except Exception as e:
        error_msg = f"Failed to send email to {recipient_email}: {e}"
        logger.error(error_msg, exc_info=True)
        return False, str(e)


def format_notification_email(owner, change_item_desc, maps_for_owner, apps_for_owner):
    """
    Format the email content for GIS change notifications.

    :param owner: The User object of the item owner
    :param change_item_desc: Description of the item/change causing the notification
    :param maps_for_owner: List of maps owned by this user
    :param apps_for_owner: List of apps owned by this user
    :return: Tuple of (plain_text_body, html_body, subject)
    :rtype: tuple(str, str, str)
    """
    subject = "Impacting GIS Changes Notification"

    greeting = f"Hello {owner.user_first_name or owner.user_username} {owner.user_last_name or ''}".strip()
    plain_body = f"{greeting},\n\n{change_item_desc}\n\n"

    # Generate email tables (assuming this function exists)
    email_tables = generate_notification_tables(maps_for_owner, apps_for_owner)
    plain_body += email_tables["plain"]

    # Build structured HTML and escape dynamic segments
    html_body = (
        f"<p>{escape(greeting)},</p>"
        f"<p>{escape(change_item_desc)}</p>"
        f"{email_tables['html']}"
    )

    return plain_body, html_body, subject


def validate_webhook_secret(request):
    """Validate webhook secret using secret header comparison."""
    signature = request.headers.get("Secret", "")
    if not signature:
        logger.warning("Received webhook request without signature.")
        return False

    # Get webhook secret from SiteSettings
    try:
        site_settings = SiteSettings.objects.first()
        webhook_secret = site_settings.webhook_secret if site_settings else None
    except Exception as e:
        logger.error(f"Failed to retrieve webhook secret from SiteSettings: {e}")
        return False

    if not webhook_secret:
        logger.warning("Webhook secret not configured in SiteSettings.")
        return False

    if not constant_time_compare(signature, webhook_secret):
        logger.warning("Webhook signature mismatch.")
        return False

    return True


def get_portal_instance(portal_url):
    """Get portal instance from URL with normalized matching."""

    normalized_url = portal_url.rstrip('/')
    portal_qs = Portal.objects.filter(
        Q(url=normalized_url) | Q(url=normalized_url + '/')
    )
    return portal_qs.first()


def process_webhook_events(events, target, portal_instance):
    """Process multiple webhook events by routing to appropriate handlers."""
    logger.debug(f"Processing {len(events)} webhook events")

    for event in events:
        source = event.get("source")
        operation = event.get("operation")
        event_id = event.get("id")

        logger.info(f"Processing event - Source: {source}, Operation: {operation}, ID: {event_id}")

        try:
            if source == "item":
                _process_item_event(target, portal_instance, event_id, operation, event)
            elif source == "user":
                _process_user_event(portal_instance, event_id, operation)
            else:
                logger.warning(f"Unhandled event source: {source}")
        except Exception as e:
            logger.error(f"Error processing event {event_id} ({source}/{operation}): {e}", exc_info=True)


def _process_item_event(target, portal_instance, event_id, operation, event):
    """Process item webhook events including public sharing validation and CRUD operations."""

    # Handle public sharing validation for share operations
    if operation == "share":
        _webhook_public_sharing_validation(portal_instance, event_id, event)

    # Handle item operations
    if operation == "delete":
        _webhook_item_deletion(portal_instance, event_id)
    elif operation in ("add", "update", "publish", "share", "unshare"):
        _webhook_item_crud(target, portal_instance, event_id, operation)
    else:
        logger.warning(f"Unhandled item operation: {operation}")

def _webhook_public_sharing_validation(portal_instance, event_id, event):
    """Handle public sharing validation when items are shared publicly."""
    try:
        tool_settings = portal_instance.tool_settings

        # Check if validation is enabled and configured for webhook triggers
        if not (tool_settings.tool_public_unshare_enabled and
                tool_settings.tool_public_unshare_trigger == 'webhook'):
            return

        # Check if item was shared to "Everyone" (public)
        shared_to_groups = event.get("properties", {}).get("sharedToGroups", [])
        if "Everyone" not in shared_to_groups:
            return

        logger.info(f"Processing public item share validation for item: {event_id}")

        tasks.process_public_unshare_task.delay(
            portal_alias=portal_instance.alias,
            score_threshold=tool_settings.tool_public_unshare_score,
            item_id=event_id
        )

    except Exception as e:
        logger.error(f"Error in public item share validation for {event_id}: {e}", exc_info=True)


def _webhook_item_deletion(portal_instance, event_id):
    """Handle item deletion by cleaning up local database records."""
    try:
        deleted_count = 0

        # Clean up records for the deleted item
        item_type_models = [
            (Service, {'portal_instance': portal_instance, 'portal_id__has_any_keys': [event_id]}),
            (Webmap, {'portal_instance': portal_instance, 'webmap_id': event_id}),
            (App, {'portal_instance': portal_instance, 'app_id': event_id})
        ]

        for model_class, filter_kwargs in item_type_models:
            deleted = model_class.objects.filter(**filter_kwargs).delete()
            if deleted[0] > 0:
                deleted_count += deleted[0]
                logger.info(f"Deleted {deleted[0]} {model_class.__name__.lower()} records for item {event_id}")

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} records for deleted item {event_id}")
        else:
            logger.debug(f"No records found to delete for item {event_id}")

    except Exception as e:
        logger.error(f"Error processing delete webhook for {event_id}: {e}", exc_info=True)


def _webhook_item_crud(target, portal_instance, event_id, operation):
    """Handle create/update/publish operations by dispatching to appropriate tasks."""
    try:
        item = target.content.get(event_id)
        if not item:
            logger.warning(f"Item {event_id} not found in portal for operation {operation}")
            return

        item_type = item.type
        logger.debug(f"Processing {operation} for {item_type} item {event_id}")

        # Task mapping for item types
        task_mapping = {
            "Feature Layer": "process_service",
            "Map Image Layer": "process_service",
            "Web Map": "process_webmap",
            "Web Mapping Application": "process_webapp",
            "Dashboard": "process_webapp",
            "Web AppBuilder Apps": "process_webapp",
            "Experience Builder": "process_webapp",
            "Form": "process_webapp",
            "Story Map": "process_webapp"
        }

        task_name = task_mapping.get(item_type)
        if task_name:
            task = getattr(tasks, task_name)
            task.delay(portal_instance.alias, event_id, operation)
        else:
            logger.info(f"No processing defined for item type: {item_type}")

    except Exception as e:
        logger.error(f"Error processing {operation} webhook for {event_id}: {e}", exc_info=True)


def _process_user_event(portal_instance, event_id, operation):
    """Process user webhook events."""
    logger.info(f"Processing user webhook - ID: {event_id}, Operation: {operation}")
    tasks.process_user.delay(portal_instance.alias, event_id, operation)


class MSDLayerInfo:
    """
    Container for parsed MSD layer information.

    :ivar service_layer_id: Layer index within the service (e.g., 0, 1, 2).
    :vartype service_layer_id: int or None
    :ivar layer_name: Display name of the layer.
    :vartype layer_name: str or None
    :ivar dataset: Dataset name (feature class name).
    :vartype dataset: str or None
    :ivar dataset_type: Type of dataset (e.g., 'esriDTFeatureClass').
    :vartype dataset_type: str or None
    :ivar feature_dataset: Parent feature dataset name if applicable.
    :vartype feature_dataset: str or None
    :ivar workspace_connection: Database connection string.
    :vartype workspace_connection: str or None
    :ivar workspace_factory: Workspace factory type.
    :vartype workspace_factory: str or None
    :ivar workspace_type: Type of workspace (e.g., 'esriDataSourcesGDB.SdeWorkspaceFactory').
    :vartype workspace_type: str or None
    """

    def __init__(self):
        self.service_layer_id = None
        self.layer_name = None
        self.dataset = None
        self.dataset_type = None
        self.feature_dataset = None
        self.workspace_connection = None
        self.workspace_factory = None
        self.workspace_type = None

    def to_dict(self):
        """
        Convert to dictionary.

        :return: Dictionary representation of layer info.
        :rtype: dict
        """
        return {
            'service_layer_id': self.service_layer_id,
            'layer_name': self.layer_name,
            'dataset': self.dataset,
            'dataset_type': self.dataset_type,
            'feature_dataset': self.feature_dataset,
            'workspace_connection': self.workspace_connection,
            'workspace_factory': self.workspace_factory,
            'workspace_type': self.workspace_type
        }


def extract_msd_from_manifest(service_manifest, service_name="service"):
    """
    Extract .msd file path from service manifest and copy it locally for processing.

    The .msd file is not accessible through REST API and must be accessed directly
    from the server file system. This function extracts the file path from the manifest,
    checks if it's accessible, and creates a local copy for processing.

    :param service_manifest: The service manifest dictionary (already retrieved).
    :type service_manifest: dict
    :param service_name: Name of the service for logging purposes.
    :type service_name: str
    :return: Path to local copy of .msd file or None if failed.
    :rtype: Optional[Path]
    """
    try:
        # Find the .msd resource in the manifest
        resources = service_manifest.get("resources", [])
        msd_resource = None

        for resource in resources:
            server_path = resource.get("serverPath", "")
            if server_path.endswith(".msd"):
                msd_resource = resource
                break

        if not msd_resource:
            logger.debug(f"No .msd file found in manifest for service: {service_name}")
            return None

        # Get the server path to the .msd file
        msd_server_path = Path(msd_resource.get("serverPath", ""))

        if not msd_server_path:
            logger.warning(f"Empty server path for .msd file in service: {service_name}")
            return None

        logger.debug(f"Found .msd file path: {msd_server_path}")

        # Check if the file is accessible
        if not msd_server_path.exists():
            logger.warning(f"MSD file not accessible at path: {msd_server_path}. "
                         f"This script must be run on a machine with access to the ArcGIS Server file system.")
            return None

        # Create a temporary copy to avoid modifying the original
        temp_dir = Path(tempfile.mkdtemp(prefix='msd_'))
        local_msd_path = temp_dir / msd_server_path.name

        logger.debug(f"Copying .msd file to temporary location: {local_msd_path}")
        shutil.copy2(msd_server_path, local_msd_path)

        logger.info(f"Successfully copied .msd file for service '{service_name}' to: {local_msd_path}")
        return local_msd_path

    except PermissionError as e:
        logger.error(f"Permission denied accessing .msd file for service '{service_name}': {e}")
        return None
    except Exception as e:
        logger.error(f"Error extracting .msd file for service '{service_name}': {e}", exc_info=True)
        return None


def parse_msd_layers(msd_path):
    """
    Parse layer information from an .msd file.

    Supports both ArcGIS Pro 2.x (p20) and 3.x (p30) formats:
    - p20: Traditional XML with namespaces
    - p30: JSON content in .xml files

    :param msd_path: Path to .msd file or extracted directory.
    :type msd_path: Path
    :return: List of MSDLayerInfo objects.
    :rtype: List[MSDLayerInfo]
    """
    temp_dir = None
    cleanup_required = False

    try:
        # Extract if it's a ZIP file
        if msd_path.is_file() and zipfile.is_zipfile(msd_path):
            logger.info("Extracting .msd ZIP file...")
            temp_dir = tempfile.mkdtemp(prefix='msd_')
            extract_dir = Path(temp_dir)
            cleanup_required = True

            with zipfile.ZipFile(msd_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

            logger.debug(f"Extracted .msd to: {extract_dir}")
        else:
            logger.info(".msd file is not a ZIP, using as-is.")
            extract_dir = msd_path

        # Detect format (p20 XML vs p30 JSON)
        is_json_format = _detect_json_format(extract_dir)
        if is_json_format:
            logger.info("Detected p30 JSON format")
            return _parse_msd_layers_json(extract_dir)
        else:
            logger.info("Detected p20 XML format")
            return _parse_msd_layers_xml(extract_dir)

    except Exception as e:
        logger.error(f"Error parsing .msd file: {e}", exc_info=True)
        return []

    finally:
        # Cleanup temp directory
        if cleanup_required and temp_dir and Path(temp_dir).exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


def _detect_json_format(directory):
    """
    Detect if MSD uses p30 JSON format or p20 XML format.

    p30 uses JSON content in .xml files and has GISProject.json instead of GISProject.xml.

    :param directory: Directory containing extracted MSD files.
    :type directory: Path
    :return: True if p30 JSON format, False if p20 XML format.
    :rtype: bool
    """
    # Check for GISProject.json (p30 indicator)
    if (directory / "GISProject.json").exists():
        return True

    # Check for Index.json (p30 indicator)
    if (directory / "Index.json").exists():
        return True

    # Check a sample XML file to see if it contains JSON
    xml_files = list(directory.rglob("*.xml"))[:3]  # Check first 3 XML files
    for xml_file in xml_files:
        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                content = f.read(100).strip()
                if content.startswith('{') or content.startswith('['):
                    return True
        except Exception:
            continue

    return False


def _parse_msd_layers_xml(extract_dir):
    """
    Parse layers from p20 XML format MSD.

    :param extract_dir: Directory containing extracted MSD files.
    :type extract_dir: Path
    :return: List of MSDLayerInfo objects.
    :rtype: List[MSDLayerInfo]
    """
    # Find the map XML
    map_xml = _find_map_xml(extract_dir)
    if not map_xml:
        logger.warning(f"Could not find map XML in {extract_dir}")
        return []

    logger.debug(f"Found map XML: {map_xml}")

    # Parse layer references
    layer_refs = _parse_layer_references(map_xml, extract_dir)
    logger.debug(f"Found {len(layer_refs)} layer references")

    # Parse each layer
    layers = []
    for layer_ref in layer_refs:
        layer_xml_path = extract_dir / layer_ref
        if not layer_xml_path.exists():
            logger.warning(f"Layer XML not found: {layer_ref}")
            continue

        layer_info = _parse_layer_xml(layer_xml_path)
        if layer_info:
            layers.append(layer_info)

    return layers


def _parse_msd_layers_json(extract_dir):
    """
    Parse layers from p30 JSON format MSD.

    :param extract_dir: Directory containing extracted MSD files.
    :type extract_dir: Path
    :return: List of MSDLayerInfo objects.
    :rtype: List[MSDLayerInfo]
    """
    # Find the map JSON/XML file (contains layer references)
    map_file = _find_map_json(extract_dir)
    if not map_file:
        logger.warning(f"Could not find map file in {extract_dir}")
        return []

    logger.debug(f"Found map file: {map_file}")

    # Parse layer references from JSON
    layer_refs = _parse_layer_references_json(map_file, extract_dir)
    logger.debug(f"Found {len(layer_refs)} layer references")

    # Parse each layer
    layers = []
    for layer_ref in layer_refs:
        layer_file_path = extract_dir / layer_ref
        if not layer_file_path.exists():
            logger.warning(f"Layer file not found: {layer_ref}")
            continue

        layer_info = _parse_layer_json(layer_file_path)
        if layer_info:
            layers.append(layer_info)

    return layers


def _find_map_xml(directory):
    """
    Find the main map XML file.

    :param directory: Directory to search in.
    :type directory: Path
    :return: Path to map XML file or None if not found.
    :rtype: Optional[Path]
    """
    # Search recursively
    for xml_file in directory.rglob("*.xml"):
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            if root.find('.//Layers') is not None:
                return xml_file
        except ET.ParseError:
            continue

    return None


def _get_namespace_from_root(root):
    """
    Extract namespace from root element.

    :param root: Root XML element.
    :type root: ET.Element
    :return: Dictionary with namespace mapping.
    :rtype: Dict[str, str]
    """
    if '}' in root.tag:
        ns_uri = root.tag.split('}')[0].strip('{')
        return {'typens': ns_uri}
    return {}


def _parse_layer_references(map_xml_path, base_dir):
    """
    Parse layer XML file references from map XML.

    :param map_xml_path: Path to map XML file.
    :type map_xml_path: Path
    :param base_dir: Base directory for resolving relative paths.
    :type base_dir: Path
    :return: List of layer file paths.
    :rtype: List[str]
    """
    tree = ET.parse(map_xml_path)
    root = tree.getroot()

    ns = _get_namespace_from_root(root)
    layer_refs = []

    # Try with namespace
    if ns and 'typens' in ns:
        layers_elem = root.find('.//typens:Layers', ns)
    else:
        layers_elem = None

    if layers_elem is None:
        layers_elem = root.find('.//Layers')

    if layers_elem is not None:
        for string_elem in layers_elem:
            layer_path = string_elem.text
            if layer_path and layer_path.startswith('CIMPATH='):
                layer_path = layer_path.replace('CIMPATH=', '')
                layer_refs.append(layer_path)

    return layer_refs


def _parse_layer_xml(layer_xml_path):
    """
    Parse a single layer XML file.

    :param layer_xml_path: Path to layer XML file.
    :type layer_xml_path: Path
    :return: MSDLayerInfo object or None if parsing fails.
    :rtype: Optional[MSDLayerInfo]
    """
    try:
        tree = ET.parse(layer_xml_path)
        root = tree.getroot()

        ns = _get_namespace_from_root(root)
        has_ns = ns and 'typens' in ns

        layer_info = MSDLayerInfo()

        # Helper to find elements with/without namespace
        def find_elem(xpath_with_ns, xpath_without_ns):
            if has_ns:
                elem = root.find(xpath_with_ns, ns)
                if elem is not None:
                    return elem
            return root.find(xpath_without_ns)

        # Extract layer name
        name_elem = find_elem('.//typens:Name', './/Name')
        if name_elem is not None:
            layer_info.layer_name = name_elem.text

        # Extract ServiceLayerID
        service_id_elem = find_elem('.//typens:ServiceLayerID', './/ServiceLayerID')
        if service_id_elem is not None:
            try:
                layer_info.service_layer_id = int(service_id_elem.text)
            except (ValueError, TypeError):
                pass

        # Find DataConnection element
        data_conn_elem = find_elem('.//typens:DataConnection', './/DataConnection')

        if data_conn_elem is not None:
            # Helper for finding within data connection
            def find_in_conn(xpath_with_ns, xpath_without_ns):
                if has_ns:
                    elem = data_conn_elem.find(xpath_with_ns, ns)
                    if elem is not None:
                        return elem
                return data_conn_elem.find(xpath_without_ns)

            # Workspace connection string
            ws_conn_elem = find_in_conn('.//typens:WorkspaceConnectionString', './/WorkspaceConnectionString')
            if ws_conn_elem is not None:
                layer_info.workspace_connection = ws_conn_elem.text
                layer_info.workspace_type = _parse_workspace_type(ws_conn_elem.text)

            # Workspace factory
            ws_factory_elem = find_in_conn('.//typens:WorkspaceFactory', './/WorkspaceFactory')
            if ws_factory_elem is not None:
                layer_info.workspace_factory = ws_factory_elem.text

            # Dataset name
            dataset_elem = find_in_conn('.//typens:Dataset', './/Dataset')
            if dataset_elem is not None:
                layer_info.dataset = dataset_elem.text

            # Dataset type
            dataset_type_elem = find_in_conn('.//typens:DatasetType', './/DatasetType')
            if dataset_type_elem is not None:
                layer_info.dataset_type = dataset_type_elem.text

            # Feature dataset
            feature_dataset_elem = find_in_conn('.//typens:FeatureDataset', './/FeatureDataset')
            if feature_dataset_elem is not None:
                layer_info.feature_dataset = feature_dataset_elem.text

        return layer_info

    except Exception as e:
        logger.error(f"Error parsing layer XML {layer_xml_path}: {e}")
        return None


def _find_map_json(directory):
    """
    Find the main map file in p30 JSON format.

    :param directory: Directory to search in.
    :type directory: Path
    :return: Path to map file or None if not found.
    :rtype: Optional[Path]
    """
    # Search for files that might contain layer references
    for json_xml_file in directory.rglob("*.xml"):
        try:
            with open(json_xml_file, 'r', encoding='utf-8') as f:
                content = f.read(1000)
                if content.strip().startswith('{') and '"layers":[' in content:
                    return json_xml_file
        except Exception:
            continue

    return None


def _parse_layer_references_json(map_file_path, base_dir):
    """
    Parse layer file references from p30 JSON format map file.

    :param map_file_path: Path to map file.
    :type map_file_path: Path
    :param base_dir: Base directory for resolving relative paths.
    :type base_dir: Path
    :return: List of layer file paths.
    :rtype: List[str]
    """
    try:
        with open(map_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        layers = data.get("layers", [])
        layer_refs = []

        for layer in layers:
            if isinstance(layer, str) and layer.startswith('CIMPATH='):
                layer_path = layer.replace('CIMPATH=', '')
                layer_refs.append(layer_path)

        return layer_refs

    except Exception as e:
        logger.error(f"Error parsing layer references from JSON {map_file_path}: {e}")
        return []


def _parse_layer_json(layer_file_path):
    """
    Parse a single layer file in p30 JSON format.

    :param layer_file_path: Path to layer JSON file.
    :type layer_file_path: Path
    :return: MSDLayerInfo object or None if parsing fails.
    :rtype: Optional[MSDLayerInfo]
    """
    try:
        with open(layer_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        layer_info = MSDLayerInfo()

        # Extract layer name
        layer_info.layer_name = data.get("name")

        # Extract ServiceLayerID
        service_layer_id = data.get("serviceLayerID")
        if service_layer_id is not None:
            try:
                layer_info.service_layer_id = int(service_layer_id)
            except (ValueError, TypeError):
                pass

        # Extract DataConnection
        feature_table = data.get("featureTable", {})
        data_connection = feature_table.get("dataConnection", {})

        if data_connection:
            # Workspace connection string
            ws_conn = data_connection.get("workspaceConnectionString")
            if ws_conn:
                layer_info.workspace_connection = ws_conn
                layer_info.workspace_type = _parse_workspace_type(ws_conn)

            # Workspace factory
            ws_factory = data_connection.get("workspaceFactory")
            if ws_factory:
                layer_info.workspace_factory = ws_factory

            # Dataset name
            dataset = data_connection.get("dataset")
            if dataset:
                layer_info.dataset = dataset

            # Dataset type
            dataset_type = data_connection.get("datasetType")
            if dataset_type:
                layer_info.dataset_type = dataset_type

            # Feature dataset
            feature_dataset = data_connection.get("featureDataset")
            if feature_dataset:
                layer_info.feature_dataset = feature_dataset

        return layer_info

    except Exception as e:
        logger.error(f"Error parsing layer JSON {layer_file_path}: {e}", exc_info=True)
        return None


def _parse_workspace_type(connection_string):
    """
    Determine workspace type from connection string.

    :param connection_string: Database connection string.
    :type connection_string: str
    :return: Workspace type description (e.g., "File Geodatabase", "Enterprise Geodatabase").
    :rtype: str
    """
    if not connection_string:
        return "Unknown"

    conn_lower = connection_string.lower()

    if 'database=' in conn_lower and '.gdb' in conn_lower:
        return "File Geodatabase"
    elif 'server=' in conn_lower and ('sqlserver' in conn_lower or 'postgresql' in conn_lower):
        return "Enterprise Geodatabase"
    elif 'dsid=' in conn_lower:
        return "Enterprise Geodatabase (Hosted)"
    else:
        return "Other"


def process_msd_layers_for_service(service_manifest, service_name, instance_item,
                                   service_obj, update_time):
    """
    Process MSD file for a service and update Layer and Layer_Service records.

    This function extracts the .msd file from the service manifest, parses layer information,
    and creates/updates Django model records for layers and their relationships to services.

    :param service_manifest: The service manifest dictionary.
    :type service_manifest: dict
    :param service_name: Name of the service.
    :type service_name: str
    :param instance_item: Portal instance model object.
    :type instance_item: Portal
    :param service_obj: Service model object.
    :type service_obj: Service
    :param update_time: Timestamp for this update.
    :type update_time: datetime
    :return: True if processing was successful, False otherwise.
    :rtype: bool
    """
    msd_path = None
    try:
        # Extract and copy the MSD file
        logger.debug(f"Attempting to extract MSD for service: {service_name}")
        msd_path = extract_msd_from_manifest(service_manifest, service_name)

        if not msd_path:
            logger.debug(f"No MSD file available for service: {service_name}")
            return False

        # Parse the MSD layers
        logger.debug(f"Parsing MSD layers for service: {service_name}")
        layers = parse_msd_layers(msd_path)

        if not layers:
            logger.warning(f"No layers found in MSD for service: {service_name}")
            return False

        logger.info(f"Found {len(layers)} layers in MSD for service: {service_name}")

        # Process each layer
        for layer_info in layers:
            try:
                # Extract database info from workspace connection
                db_server, db_version, db_database = _extract_database_info(layer_info.workspace_connection)

                # Dataset name
                dataset_name = layer_info.dataset
                if not dataset_name:
                    logger.warning(f"No dataset name for layer '{layer_info.layer_name}' in service '{service_name}'")
                    continue

                logger.debug(f"Processing layer: {layer_info.layer_name} (dataset: {dataset_name}, service_layer_id: {layer_info.service_layer_id})")

                # Create or update Layer record
                layer_obj, layer_created = Layer.objects.update_or_create(
                    portal_instance=instance_item,
                    layer_server=db_server,
                    layer_version=db_version,
                    layer_database=db_database,
                    layer_name=dataset_name,
                    defaults={"updated_date": update_time}
                )

                if layer_created:
                    logger.debug(f"Created new layer record: {dataset_name}")
                else:
                    logger.debug(f"Updated existing layer record: {dataset_name}")

                # Create or update Layer_Service relationship with service_layer_id
                _, rel_created = Layer_Service.objects.update_or_create(
                    portal_instance=instance_item,
                    layer_id=layer_obj,
                    service_id=service_obj,
                    defaults={
                        "updated_date": update_time,
                        "service_layer_id": layer_info.service_layer_id,
                        "service_layer_name": layer_info.layer_name
                    }
                )

                if rel_created:
                    logger.debug(f"Created new layer-service relationship for: {dataset_name} (service_layer_id: {layer_info.service_layer_id})")
                else:
                    logger.debug(f"Updated layer-service relationship for: {dataset_name} (service_layer_id: {layer_info.service_layer_id})")

            except Exception as e:
                logger.error(f"Error processing layer '{layer_info.layer_name}' in service '{service_name}': {e}", exc_info=True)
                continue

        logger.info(f"Successfully processed MSD layers for service: {service_name}")
        return True

    except Exception as e:
        logger.error(f"Error processing MSD for service '{service_name}': {e}", exc_info=True)
        return False

    finally:
        # Clean up temporary MSD file
        if msd_path and msd_path.exists():
            try:
                # Remove the temporary directory
                temp_dir = msd_path.parent
                if temp_dir.exists() and 'msd_' in temp_dir.name:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    logger.debug(f"Cleaned up temporary MSD directory: {temp_dir}")
            except Exception as e:
                logger.warning(f"Error cleaning up temporary MSD files: {e}")



def _extract_database_info(connection_string):
    """
    Extract database server, version, and database name from connection string.

    :param connection_string: Workspace connection string.
    :type connection_string: str
    :return: Tuple of (server, version, database). Returns (None, None, None) if parsing fails.
    :rtype: tuple
    """
    import re

    if not connection_string:
        return None, None, None

    conn_lower = connection_string.lower()

    # File Geodatabase
    if 'database=' in conn_lower and '.gdb' in conn_lower:
        parts = connection_string.split("DATABASE=")
        if len(parts) > 1:
            db_database = parts[1].replace(r"\\", "\\")
            return None, None, db_database

    # Enterprise Geodatabase
    elif 'server=' in conn_lower:
        server_match = re.search(r'SERVER=([^;]+)', connection_string, re.IGNORECASE)
        db_server = server_match.group(1).upper() if server_match else None

        version_match = re.search(r'VERSION=([^;]+)', connection_string, re.IGNORECASE) or \
                       re.search(r'BRANCH=([^;]+)', connection_string, re.IGNORECASE)
        db_version = version_match.group(1).upper() if version_match else None

        db_match = re.search(r'DATABASE=([^;]+)', connection_string, re.IGNORECASE)
        db_database = db_match.group(1).upper() if db_match else None

        return db_server, db_version, db_database

    return None, None, None
