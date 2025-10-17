# Licensed under GPLv3 - See LICENSE file for details.
import json
import logging
import zoneinfo
from datetime import datetime, time

from django import forms
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone
from django_celery_beat.models import PeriodicTask, CrontabSchedule

from .models import Portal, SiteSettings, PortalToolSettings

logger = logging.getLogger('enterpriseviz')


class ScheduleForm(forms.Form):
    """
    Form for scheduling periodic data update tasks for a portal instance.

    Allows users to define task frequency (minute, hour, day, week, month),
    start date, end conditions (never, specific date, or after a number of occurrences),
    and the types of items to update (webmaps, services, apps, users).

    The form's `save_task` method creates or updates a `PeriodicTask`
    with a `CrontabSchedule` based on the form's validated data.
    """

    DAY_CHOICES = [
        ("0", "Sunday"), ("1", "Monday"), ("2", "Tuesday"), ("3", "Wednesday"),
        ("4", "Thursday"), ("5", "Friday"), ("6", "Saturday")
    ]
    DEFAULT_DAYS = [day[0] for day in DAY_CHOICES]

    REPEAT_CHOICES = [
        ("minute", "Minute"), ("hour", "Hour"), ("day", "Day"),
        ("week", "Week"), ("month", "Month")
    ]

    ENDING_CHOICES = [
        ("never", "Never"), ("date", "Date"), ("count", "Count")
    ]

    ITEM_CHOICES = [
        ("webmaps", "Maps"), ("services", "Services/Layers"),
        ("webapps", "Apps"), ("users", "Users")
    ]

    instance = forms.ModelChoiceField(
        queryset=Portal.objects.all(),
        label="Portal URL"
    )

    beginning_on = forms.DateField(
        label="Beginning on",
        initial=timezone.now().date
    )

    repeat_type = forms.ChoiceField(
        choices=REPEAT_CHOICES,
        required=True,
        initial="minute"
    )

    # Repeat intervals with validation ranges
    repeat_interval_minute = forms.IntegerField(
        required=False, initial=15, min_value=1, max_value=59
    )
    repeat_interval_hour = forms.IntegerField(
        required=False, initial=1, min_value=1, max_value=23
    )
    repeat_interval_day = forms.IntegerField(
        required=False, initial=1, min_value=1, max_value=365
    )
    repeat_interval_week = forms.IntegerField(
        required=False, initial=1, min_value=1, max_value=52
    )
    repeat_interval_month = forms.IntegerField(
        required=False, initial=1, min_value=1, max_value=12
    )

    # Day of week
    day_of_week_minute = forms.MultipleChoiceField(
        choices=DAY_CHOICES,
        required=False,
        initial=DEFAULT_DAYS
    )
    day_of_week_hour = forms.MultipleChoiceField(
        choices=DAY_CHOICES,
        required=False,
        initial=DEFAULT_DAYS
    )
    day_of_week_week = forms.MultipleChoiceField(
        choices=DAY_CHOICES,
        required=False,
        initial=DEFAULT_DAYS
    )
    day_of_month = forms.IntegerField(
        required=False, initial=1, min_value=1, max_value=31
    )

    on_minute_hour = forms.ChoiceField(
        choices=[(str(m), f"{m} minutes past the hour") for m in [0, 5, 15, 30, 45]],
        required=False,
        initial="0"
    )

    # Time fields
    between_hours_start_minute = forms.TimeField(required=False, initial="00:00")
    between_hours_end_minute = forms.TimeField(required=False, initial="23:59")
    between_hours_start_hour = forms.TimeField(required=False, initial="00:00")
    between_hours_end_hour = forms.TimeField(required=False, initial="23:59")
    time_day = forms.TimeField(required=False, initial="00:00")
    time_week = forms.TimeField(required=False, initial="00:00")
    time_month = forms.TimeField(required=False, initial="00:00")

    # Ending conditions
    ending_on = forms.ChoiceField(
        choices=ENDING_CHOICES,
        required=False,
        initial="never"
    )
    ending_on_date = forms.DateField(required=False)
    ending_on_count = forms.IntegerField(required=False, min_value=1, max_value=9999)

    # Items to update
    items = forms.MultipleChoiceField(
        choices=ITEM_CHOICES,
        initial=[key for key, val in ITEM_CHOICES]
    )

    def _clean_day_of_week(self, field_name):
        """Helper to clean and format day_of_week fields."""
        days = self.cleaned_data.get(field_name)
        if not days:
            self.add_error(field_name, "At least one day must be selected.")
            return ""
        # Ensure consistent order for CrontabSchedule
        return ",".join(sorted(days))

    def _validate_time_range(self, start_field, end_field):
        """Validate that start time is before end time."""
        start_time = self.cleaned_data.get(start_field)
        end_time = self.cleaned_data.get(end_field)

        if start_time and end_time and start_time >= end_time:
            self.add_error(end_field, f"End time must be after start time.")

    def clean_beginning_on(self):
        """Ensure beginning_on is a timezone-aware datetime."""
        beginning_on = self.cleaned_data.get('beginning_on')
        if beginning_on:
            # Convert date to datetime if needed
            if not isinstance(beginning_on, datetime):
                beginning_on = datetime.combine(beginning_on, time.min)

            # Make timezone-aware
            if timezone.is_naive(beginning_on):
                beginning_on = timezone.make_aware(beginning_on)

        return beginning_on

    def clean_ending_on_date(self):
        """Ensure ending date is after beginning date."""
        ending_date = self.cleaned_data.get('ending_on_date')
        beginning_date = self.cleaned_data.get('beginning_on')

        if ending_date and beginning_date and ending_date <= beginning_date:
            raise ValidationError("End date must be after start date.")
        return ending_date

    def clean(self):
        """Comprehensive form validation."""
        cleaned_data = super().clean()
        repeat_type = cleaned_data.get("repeat_type")

        # Validate required fields based on repeat type
        if repeat_type == "minute":
            cleaned_data["day_of_week_minute"] = self._clean_day_of_week("day_of_week_minute")
            interval = cleaned_data.get("repeat_interval_minute")
            if not interval:
                self.add_error("repeat_interval_minute", "Repeat interval is required for minute schedules.")

            self._validate_time_range("between_hours_start_minute", "between_hours_end_minute")

            start_time = cleaned_data.get("between_hours_start_minute")
            end_time = cleaned_data.get("between_hours_end_minute")

            # Allow None for both (all day), otherwise both required
            if start_time is None and end_time is None:
                # All day - set cron to run every hour
                cleaned_data["time_range_hour_cron_minute"] = "*"
            elif start_time is None or end_time is None:
                # Only one is None - error
                self.add_error("between_hours_start_minute", "Both start and end times are required, or select all day.")
                self.add_error("between_hours_end_minute", "Both start and end times are required, or select all day.")
            elif start_time == end_time:
                cleaned_data["time_range_hour_cron_minute"] = "*"
            else:
                cleaned_data["time_range_hour_cron_minute"] = f"{start_time.hour}-{end_time.hour}"
        elif repeat_type == "hour":
            cleaned_data["day_of_week_hour"] = self._clean_day_of_week("day_of_week_hour")
            interval = cleaned_data.get("repeat_interval_hour")
            if not interval:
                self.add_error("repeat_interval_hour", "Repeat interval is required for hourly schedules.")
            self._validate_time_range("between_hours_start_hour", "between_hours_end_hour")

            start_time = cleaned_data.get("between_hours_start_hour")
            end_time = cleaned_data.get("between_hours_end_hour")

            # Allow None for both (all day), otherwise both required
            if start_time is None and end_time is None:
                # All day - set cron to run every hour
                cleaned_data["time_range_hour_cron_hour"] = "*"
            elif start_time is None or end_time is None:
                # Only one is None - error
                self.add_error("between_hours_start_hour", "Both start and end times are required, or select all day.")
                self.add_error("between_hours_end_hour", "Both start and end times are required, or select all day.")
            elif start_time == end_time:
                cleaned_data["time_range_hour_cron_hour"] = "*"
            else:
                cleaned_data["time_range_hour_cron_hour"] = f"{start_time.hour}-{end_time.hour}"
        elif repeat_type == "day":
            if not cleaned_data.get("repeat_interval_day"):
                self.add_error("repeat_interval_day", "Repeat interval is required for daily schedules.")
            if not cleaned_data.get("time_day"):
                self.add_error("time_day", "Time of day is required for daily schedules.")
        elif repeat_type == "week":
            cleaned_data["day_of_week_week"] = self._clean_day_of_week("day_of_week_week")

            if not cleaned_data.get("time_week"):
                self.add_error("time_week", "Time of day is required for weekly schedules.")
        elif repeat_type == "month":
            if not cleaned_data.get("repeat_interval_month"):
                self.add_error("repeat_interval_month", "Repeat interval is required for monthly schedules.")
            if not cleaned_data.get("time_month"):
                self.add_error("time_month", "Time of day is required for monthly schedules.")

        # Validate ending conditions
        ending_on = cleaned_data.get("ending_on")
        if ending_on == "date" and not cleaned_data.get("ending_on_date"):
            self.add_error("ending_on_date", "An end date is required if 'Ending on Date' is selected.")
        if ending_on == "count" and not cleaned_data.get("ending_on_count"):
            self.add_error("ending_on_count", "A count is required if 'Ending on Count' is selected.")

        # Validate items selection
        if not cleaned_data.get("items"):
            self.add_error("items", "At least one item type to update must be selected.")

        logger.debug(f"ScheduleForm cleaned_data: {cleaned_data}")
        return cleaned_data

    def _create_crontab_schedule(self, data):
        """Helper to create or get CrontabSchedule based on cleaned data."""
        cron_args = {
            "day_of_month": "*",
            "month_of_year": "*",
            "timezone": zoneinfo.ZoneInfo(settings.TIME_ZONE)
        }

        repeat_type = data["repeat_type"]

        if repeat_type == "minute":
            cron_args.update({
                "minute": f"*/{data['repeat_interval_minute']}",
                "hour": data["time_range_hour_cron_minute"],
                "day_of_week": data["day_of_week_minute"],
            })
        elif repeat_type == "hour":
            hour_spec = f"*/{data['repeat_interval_hour']}"
            if data["time_range_hour_cron_hour"] != "*":
                hour_spec = f"{data['time_range_hour_cron_hour']}/{data['repeat_interval_hour']}"

            cron_args.update({
                "minute": data['on_minute_hour'],
                "hour": hour_spec,
                "day_of_week": data["day_of_week_hour"],
            })
        elif repeat_type == "day":
            time_day = data['time_day']
            cron_args.update({
                "minute": str(time_day.minute),
                "hour": str(time_day.hour),
                "day_of_week": "*",
                "day_of_month": f"*/{data['repeat_interval_day']}",

            })
        elif repeat_type == "week":
            time_week = data['time_week']
            cron_args.update({
                "minute": str(time_week.minute),
                "hour": str(time_week.hour),
                "day_of_week": data['day_of_week_week'],
            })
        elif repeat_type == "month":
            time_month = data['time_month']
            cron_args.update({
                "minute": str(time_month.minute),
                "hour": str(time_month.hour),
                "day_of_week": "*",
                "day_of_month": data.get("day_of_month", "1"),
                "month_of_year": f"*/{data['repeat_interval_month']}"
            })
        else:
            raise ValidationError(f"Invalid repeat type: {repeat_type}")

        logger.debug(f"Crontab schedule arguments: {cron_args}")
        schedule, created = CrontabSchedule.objects.get_or_create(**cron_args)
        return schedule

    @transaction.atomic
    def save_task(self, portal=None):
        """
        Saves the schedule as a PeriodicTask within a database transaction.

        Args:
            portal: The Portal model instance for which this task is being saved.

        Returns:
            Tuple of (PeriodicTask instance or None, error message string or None).
        """
        if not self.is_valid():
            logger.error("ScheduleForm save_task called on invalid form.")
            return None, "Form is not valid."

        cleaned_data = self.cleaned_data
        portal_instance_obj = cleaned_data.get("instance")
        if not portal_instance_obj:
            return None, "Portal instance is required."

        # Use provided portal or fall back to form data
        target_portal = portal or portal_instance_obj
        task_name = f"portal-data-update-{target_portal.alias}"

        try:
            # Clean up existing task
            if hasattr(target_portal, 'task') and target_portal.task:
                logger.debug(
                    f"Deleting existing task '{target_portal.task.name}' "
                    f"for portal '{target_portal.alias}' before rescheduling."
                )
                target_portal.task.delete()
                target_portal.task = None

            schedule_obj = self._create_crontab_schedule(cleaned_data)

            task_kwargs = {
                'instance': target_portal.alias,
                'items': cleaned_data.get('items', [])
            }

            start_on = cleaned_data.get("beginning_on")
            expires_on = cleaned_data.get("ending_on_date") if cleaned_data.get("ending_on") == "date" else None
            start_dt = timezone.make_aware(datetime.combine(start_on, time.min)) if start_on else None
            expires_dt = timezone.make_aware(datetime.combine(expires_on, time.max)) if expires_on else None


            task_defaults = {
                "crontab": schedule_obj,
                "task": "Update All",
                "kwargs": json.dumps(task_kwargs),
                "enabled": True,
                "start_time": start_dt,
                "description": f"Scheduled data update for portal {target_portal.alias}"
            }

            # Set expiry date if specified
            if expires_dt:
                task_defaults["expires"] = expires_dt

            periodic_task, created = PeriodicTask.objects.update_or_create(
                name=task_name,
                defaults=task_defaults
            )

            # Link task to portal
            target_portal.task = periodic_task
            target_portal.save(update_fields=['task'])

            status_msg = "created" if created else "updated"
            logger.info(
                f"Periodic task '{periodic_task.name}' for portal "
                f"'{target_portal.alias}' {status_msg} successfully."
            )
            return periodic_task, None

        except ValidationError as ve:
            logger.error(f"Validation error creating schedule for {target_portal.alias}: {ve}")
            return None, str(ve)
        except Exception as e:
            logger.error(
                f"Failed to save periodic task for {target_portal.alias}: {e}",
                exc_info=True
            )
            return None, f"Failed to save task: {e}"


class SiteSettingsForm(forms.ModelForm):
    """
    Form for managing site-wide email configuration settings.

    Provides validation for email settings including port numbers,
    encryption types, and required field combinations.
    """

    # Standard port mappings for validation
    STANDARD_PORTS = {
        'ssl': 465,
        'starttls': 587,
        'plain_text': [25, 587]
    }

    # Override the password field to not show existing value
    email_password = forms.CharField(
        widget=forms.PasswordInput(render_value=False),
        required=False,
        help_text="Leave blank to keep existing password"
    )

    class Meta:
        model = SiteSettings
        fields = [
            "admin_email", "email_host", "email_port", "email_encryption",
            "email_username", "email_password", "from_email", "reply_to",
            "webhook_secret"
        ]

    def clean_email_encryption(self):
        """Validates the selected email encryption type."""
        encryption = self.cleaned_data.get("email_encryption")
        valid_types = ("plain_text", "starttls", "ssl")

        if encryption and encryption not in valid_types:
            raise ValidationError(f"Invalid encryption type. Must be one of: {', '.join(valid_types)}")
        return encryption

    def clean_email_port(self):
        """Validates the email port number is within the valid range."""
        port = self.cleaned_data.get("email_port")
        if port is not None:
            if not (1 <= port <= 65535):
                raise ValidationError("Enter a valid port number (1-65535).")
        return port

    def clean_webhook_secret(self):
        """Validate webhook secret strength."""
        secret = self.cleaned_data.get('webhook_secret')
        if secret and len(secret) < 16:
            raise ValidationError("Webhook secret must be at least 16 characters long.")
        return secret

    def clean(self):
        """Cross-field validation for email settings."""
        cleaned_data = super().clean()

        host = cleaned_data.get("email_host")
        port = cleaned_data.get("email_port")
        encryption = cleaned_data.get("email_encryption")
        from_email = cleaned_data.get("from_email")
        password = cleaned_data.get("email_password")

        # If any email field is set, require the core fields
        email_fields_set = any([host, port, encryption, from_email])

        if email_fields_set:
            required_fields = {
                "email_host": "SMTP host is required for email configuration",
                "email_port": "SMTP port is required for email configuration",
                "email_encryption": "Encryption type is required for email configuration",
                "from_email": "From email address is required for email configuration"
            }

            for field_name, error_msg in required_fields.items():
                if not cleaned_data.get(field_name):
                    self.add_error(field_name, error_msg)

            # Check if password is required (new configuration without existing password)
            if not password and self.instance and not self.instance.email_password:
                self.add_error("email_password", "Password is required for initial email configuration")

            # Port recommendations based on encryption type
            if host and port and encryption:
                self._validate_port_encryption_combination(port, encryption)

        logger.debug(f"SiteSettingsForm cleaned_data keys: {list(cleaned_data.keys())}")
        return cleaned_data

    def _validate_port_encryption_combination(self, port, encryption):
        """Validate port/encryption combinations and provide warnings."""
        if encryption == "ssl" and port != 465:
            self.add_error("email_port",
                           f"Port {port} is unusual for SSL encryption. Standard port is 465.")
        elif encryption == "starttls" and port != 587:
            self.add_error("email_port",
                           f"Port {port} is unusual for STARTTLS encryption. Standard port is 587.")
        elif encryption == "plain_text" and port not in [25, 587]:
            self.add_error("email_port",
                           f"Port {port} is unusual for plain text. Standard ports are 25 or 587.")


class ToolsForm(forms.ModelForm):
    """
    Form for configuring portal automation tools.

    Handles enabling/disabling various tools and their specific parameters.
    Creates separate Celery Beat tasks for each enabled tool.
    """

    class Meta:
        model = PortalToolSettings
        fields = [
            'tool_pro_license_enabled', 'tool_pro_duration', 'tool_pro_warning',
            'tool_public_unshare_enabled', 'tool_public_unshare_score',
            'tool_public_unshare_trigger', 'tool_public_unshare_notify_limit',
            'tool_inactive_user_enabled', 'tool_inactive_user_duration',
            'tool_inactive_user_warning', 'tool_inactive_user_action',
        ]

    def clean(self):
        """Validates tool-specific fields based on whether the tool is enabled."""
        cleaned_data = super().clean()

        # Tool configurations with their dependent fields
        tool_configs = [
            ('tool_pro_license_enabled',
             ['tool_pro_duration', 'tool_pro_warning'],
             'Pro License Management'),
            ('tool_public_unshare_enabled',
             ['tool_public_unshare_score', 'tool_public_unshare_notify_limit', 'tool_public_unshare_trigger'],
             'Public Item Unshare'),
            ('tool_inactive_user_enabled',
             ['tool_inactive_user_duration', 'tool_inactive_user_warning', 'tool_inactive_user_action'],
             'Inactive User Management'),
        ]

        for enabled_field, dependent_fields, tool_name in tool_configs:
            if cleaned_data.get(enabled_field):
                self._validate_tool_dependencies(cleaned_data, enabled_field, dependent_fields, tool_name)

        return cleaned_data

    def _validate_tool_dependencies(self, cleaned_data, enabled_field,
                                    dependent_fields, tool_name):
        """Validate that all required fields are set when a tool is enabled."""
        for dep_field in dependent_fields:
            value = cleaned_data.get(dep_field)
            # Check for None or empty string (but allow 0 as valid)
            if value is None or value == '':
                self.add_error(dep_field,f"This field is required when {tool_name} is enabled.")

    @transaction.atomic
    def save(self, commit: bool = True, portal=None):
        """
        Saves the PortalToolSettings and manages associated periodic tasks.

        :param commit: If True, saves the instance to the database.
        :type commit: bool
        :param portal: The Portal instance associated with these settings.
        :type portal: enterpriseviz.models.Portal
        :return: The saved PortalToolSettings instance.
        :rtype: enterpriseviz.models.PortalToolSettings
        """
        settings_instance = super().save(commit=commit)

        if not commit:
            return settings_instance

        # Get the target portal
        target_portal = portal or settings_instance.portal
        if not target_portal:
            logger.error(f"Portal not available for tool settings {settings_instance.pk}.")
            return settings_instance

        try:
            self._manage_tool_tasks(settings_instance, target_portal)
        except Exception as e:
            logger.error(f"Error managing tool tasks for portal {target_portal.alias}: {e}", exc_info=True)

        return settings_instance

    def _manage_tool_tasks(self, settings_instance: PortalToolSettings, target_portal: Portal) -> None:
        """Create, update, or delete periodic tasks for each tool."""
        # Create daily schedule (midnight)
        daily_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='0', hour='0', day_of_week='*',
            day_of_month='*', month_of_year='*',
            timezone=zoneinfo.ZoneInfo(settings.TIME_ZONE)
        )

        # Tool configurations for creating separate tasks
        tool_configs = [{
            'enabled': settings_instance.tool_pro_license_enabled,
            'task_name': f'process-pro-license-{target_portal.alias}',
            'task_function': 'Pro License Tool',
            'description': f'Pro License Management for {target_portal.alias}',
            'kwargs': {
                'portal_alias': target_portal.alias,
                'duration_days': settings_instance.tool_pro_duration,
                'warning_days': settings_instance.tool_pro_warning,
            }
        }, {
            'enabled': settings_instance.tool_inactive_user_enabled,
            'task_name': f'process-inactive-user-{target_portal.alias}',
            'task_function': 'Inactive User Tool',
            'description': f'Inactive User Management for {target_portal.alias}',
            'kwargs': {
                'portal_alias': target_portal.alias,
                'duration_days': settings_instance.tool_inactive_user_duration,
                'warning_days': settings_instance.tool_inactive_user_warning,
                'action': settings_instance.tool_inactive_user_action,
            }
        }, {
            'enabled': (settings_instance.tool_public_unshare_enabled and
                        settings_instance.tool_public_unshare_trigger == 'daily'),
            'task_name': f'process-public-unshare-{target_portal.alias}',
            'task_function': 'Public Sharing Tool',
            'description': f'Public Item Unshare Management for {target_portal.alias}',
            'kwargs': {
                'portal_alias': target_portal.alias,
                'score_threshold': settings_instance.tool_public_unshare_score,
            }
        }]

        # Create or update/delete tasks
        for config in tool_configs:
            self._manage_individual_task(config, daily_schedule)

    def _manage_individual_task(self, config, schedule):
        """Create, update, or delete a periodic task for a specific tool."""
        task_name = config['task_name']

        if config['enabled']:
            # Create or update the task
            task_defaults = {
                'task': config['task_function'],
                'crontab': schedule,
                'kwargs': json.dumps(config['kwargs']),
                'enabled': True,
                'description': config['description'],
            }

            try:
                periodic_task, created = PeriodicTask.objects.update_or_create(
                    name=task_name, defaults=task_defaults
                )
                status = "created" if created else "updated"
                logger.info(f"Periodic task '{task_name}' {status} successfully.")
            except Exception as e:
                logger.error(f"Failed to save periodic task '{task_name}': {e}", exc_info=True)
        else:
            # Delete the task if it exists
            try:
                existing_task = PeriodicTask.objects.get(name=task_name)
                existing_task.delete()
                logger.info(f"Disabled and removed periodic task: {task_name}")
            except PeriodicTask.DoesNotExist:
                pass  # Task doesn't exist, nothing to do


class PortalCredentialsForm(forms.Form):
    """
    Form for handling portal credential updates with context-aware validation.

    The form adapts its validation based on whether credentials are actually required
    for the portal (i.e., portal.store_password is False).
    """

    ITEM_CHOICES = [
        ('webmaps', 'Web Maps'),
        ('services', 'Services'),
        ('webapps', 'Web Apps'),
        ('users', 'Users'),
        ('metadata', 'Metadata'),
        ('unused', 'Unused Items'),
        ('duplicates', 'Duplicates')
    ]

    username = forms.CharField(max_length=150, required=False)
    password = forms.CharField(required=False, widget=forms.PasswordInput())
    instance = forms.CharField(max_length=100, widget=forms.HiddenInput())
    items = forms.ChoiceField(choices=ITEM_CHOICES, widget=forms.HiddenInput())
    delete = forms.BooleanField(required=False, widget=forms.HiddenInput())

    def __init__(self, *args, portal=None, require_credentials=False, **kwargs):
        """
        Initialize form with portal context.

        Args:
            portal: Portal instance for context
            require_credentials: Whether credentials are required for this portal
        """
        self.portal = portal
        self.require_credentials = require_credentials
        super().__init__(*args, **kwargs)

        # Adjust field requirements based on context
        if self.require_credentials:
            self.fields['username'].required = True
            self.fields['password'].required = True
            self.fields['username'].widget.attrs.update({
                'placeholder': 'Portal username',
                'class': 'form-control'
            })
            self.fields['password'].widget.attrs.update({
                'placeholder': 'Portal password',
                'class': 'form-control'
            })

    def clean_instance(self):
        """Validate that the portal instance exists."""
        instance_alias = self.cleaned_data.get('instance')

        if not instance_alias:
            raise ValidationError("Portal instance is required.")

        # Verify portal exists if not already provided
        if not self.portal:
            try:
                from .models import Portal
                self.portal = Portal.objects.get(alias=instance_alias)
            except Portal.DoesNotExist:
                raise ValidationError(f"Portal '{instance_alias}' does not exist.")

        return instance_alias

    def clean(self):
        """Context-aware credential validation."""
        cleaned_data = super().clean()
        username = cleaned_data.get('username')
        password = cleaned_data.get('password')

        # Only validate credentials if they're required
        if self.require_credentials:
            # Both must be provided when credentials are required
            if not username:
                raise ValidationError({'username': "Username is required for this portal."})
            if not password:
                raise ValidationError({'password': "Password is required for this portal."})
        else:
            # When not required, validate pairs if either is provided
            if username and not password:
                raise ValidationError({
                    'password': "Password is required when username is provided"
                })
            if password and not username:
                raise ValidationError({
                    'username': "Username is required when password is provided"
                })

        return cleaned_data

    def get_portal(self):
        """Return the portal instance."""
        return self.portal


class WebhookSettingsForm(forms.ModelForm):
    """
    Form for configuring webhook settings with enhanced security validation.
    """

    class Meta:
        model = SiteSettings
        fields = ('webhook_secret',)

    def clean_webhook_secret(self):
        """Validate webhook secret with enhanced security checks."""
        webhook_secret = self.cleaned_data.get('webhook_secret', '').strip()

        if not webhook_secret:
            return webhook_secret

        # Length validation
        if len(webhook_secret) < 16:
            raise ValidationError(
                "Webhook secret should be at least 16 characters long for security."
            )

        return webhook_secret
