# Licensed under GPLv3 - See LICENSE file for details.
import datetime
import json
import logging

from django import forms
from django.core.exceptions import ValidationError
from django_celery_beat.models import PeriodicTask, IntervalSchedule, CrontabSchedule

from .models import Portal, SiteSettings, PortalToolSettings

logger = logging.getLogger('enterpriseviz')


class ScheduleForm(forms.Form):
    """
    Handles the definition and validation of a form for scheduling periodic tasks with
    varied frequency and specific customization options.

    This class provides utilities for creating a Django form that allows users to
    specify task schedules. It includes diverse fields to set the behavior of the
    scheduling process such as repeat intervals, start and end dates, repeat types,
    days of the week, and additional task-specific configurations.

    :ivar instance: Selectable portal URL for the task.
    :type instance: django.db.models.ModelChoiceField
    :ivar beginning_on: The starting date of the schedule.
    :type beginning_on: django.forms.fields.DateField
    :ivar repeat_type: The type of repetition for the task.
    :type repeat_type: django.forms.fields.ChoiceField
    :ivar repeat_interval_minute: Interval in minutes for tasks to repeat.
    :type repeat_interval_minute: django.forms.fields.IntegerField
    :ivar repeat_interval_hour: Interval in hours for tasks to repeat.
    :type repeat_interval_hour: django.forms.fields.IntegerField
    :ivar repeat_interval_day: Interval in days for tasks to repeat.
    :type repeat_interval_day: django.forms.fields.IntegerField
    :ivar repeat_interval_week: Interval in weeks for tasks to repeat.
    :type repeat_interval_week: django.forms.fields.IntegerField
    :ivar repeat_interval_month: Interval in months for tasks to repeat.
    :type repeat_interval_month: django.forms.fields.IntegerField
    :ivar day_of_week_minute: Days of the week for minute-based tasks.
    :type day_of_week_minute: django.forms.fields.MultipleChoiceField
    :ivar day_of_week_hour: Days of the week for hour-based tasks.
    :type day_of_week_hour: django.forms.fields.MultipleChoiceField
    :ivar day_of_week_week: Days of the week for week-based tasks.
    :type day_of_week_week: django.forms.fields.MultipleChoiceField
    :ivar on_minute_hour: Specific minute(s) on which hour-based tasks run.
    :type on_minute_hour: django.forms.fields.ChoiceField
    :ivar between_hours_start_minute: Start time for active task hours in minute-based schedules.
    :type between_hours_start_minute: django.forms.fields.TimeField
    :ivar between_hours_end_minute: End time for active task hours in minute-based schedules.
    :type between_hours_end_minute: django.forms.fields.TimeField
    :ivar between_hours_start_hour: Start time for active task hours in hour-based schedules.
    :type between_hours_start_hour: django.forms.fields.TimeField
    :ivar between_hours_end_hour: End time for active task hours in hour-based schedules.
    :type between_hours_end_hour: django.forms.fields.TimeField
    :ivar ending_on: The type of ending condition for the task schedule.
    :type ending_on: django.forms.fields.ChoiceField
    :ivar ending_on_date: Specific date on which the task schedule ends.
    :type ending_on_date: django.forms.fields.DateField
    :ivar ending_on_count: The number of occurrences after which the task ends.
    :type ending_on_count: django.forms.fields.IntegerField
    :ivar time_day: Specific time of day for day-based tasks.
    :type time_day: django.forms.fields.TimeField
    :ivar time_week: Specific time of week for week-based tasks.
    :type time_week: django.forms.fields.TimeField
    :ivar time_month: Specific time of month for month-based tasks.
    :type time_month: django.forms.fields.TimeField
    :ivar items: Selected items related to the task, such as maps or services.
    :type items: django.forms.fields.MultipleChoiceField
    """
    instance = forms.ModelChoiceField(queryset=Portal.objects.all(), label="Portal URL")
    beginning_on = forms.DateField(label="Beginning on", initial=datetime.date.today())
    repeat_type = forms.ChoiceField(
        choices=[("minute", "Minute"), ("hour", "Hour"), ("day", "Day"), ("week", "Week"), ("month", "Month")],
        required=True, initial="minute"
    )
    repeat_interval_minute = forms.IntegerField(required=False, initial=15)
    repeat_interval_hour = forms.IntegerField(required=False, initial=1)
    repeat_interval_day = forms.IntegerField(required=False, initial=1)
    repeat_interval_week = forms.IntegerField(required=False, initial=1)
    repeat_interval_month = forms.IntegerField(required=False, initial=1)
    day_of_week_minute = forms.MultipleChoiceField(
        choices=[("0", "Sunday"), ("1", "Monday"), ("2", "Tuesday"), ("3", "Wednesday"),
                 ("4", "Thursday"), ("5", "Friday"), ("6", "Saturday")],
        required=False, initial=["0", "1", "2", "3", "4", "5", "6"]
    )
    day_of_week_hour = forms.MultipleChoiceField(
        choices=[("0", "Sunday"), ("1", "Monday"), ("2", "Tuesday"), ("3", "Wednesday"),
                 ("4", "Thursday"), ("5", "Friday"), ("6", "Saturday")],
        required=False, initial=["0", "1", "2", "3", "4", "5", "6"]
    )
    day_of_week_week = forms.MultipleChoiceField(
        choices=[("0", "Sunday"), ("1", "Monday"), ("2", "Tuesday"), ("3", "Wednesday"),
                 ("4", "Thursday"), ("5", "Friday"), ("6", "Saturday")],
        required=False, initial=["0", "1", "2", "3", "4", "5", "6"]
    )
    on_minute_hour = forms.ChoiceField(
        choices=[("0", "0"), ("5", "5"), ("15", "15"), ("30", "30"), ("45", "45")],
        required=False, initial=["0", "1", "2", "3", "4", "5", "6"]
    )
    between_hours_start_minute = forms.TimeField(required=False, initial="00:00", widget=forms.TimeInput(format='%H:%M'), input_formats=['%H:%M'])
    between_hours_end_minute = forms.TimeField(required=False, initial="00:00", widget=forms.TimeInput(format='%H:%M'), input_formats=['%H:%M'])
    between_hours_start_hour = forms.TimeField(required=False, initial="00:00", widget=forms.TimeInput(format='%H:%M'), input_formats=['%H:%M'])
    between_hours_end_hour = forms.TimeField(required=False, initial="00:00", widget=forms.TimeInput(format='%H:%M'), input_formats=['%H:%M'])
    ending_on = forms.ChoiceField(
        choices=[("never", "Never"), ("date", "Date"), ("count", "Count")],
        required=False, initial="never"
    )
    ending_on_date = forms.DateField(required=False)
    ending_on_count = forms.IntegerField(required=False)
    time_day = forms.TimeField(required=False, initial="00:00", widget=forms.TimeInput(format='%H:%M'), input_formats=['%H:%M'])
    time_week = forms.TimeField(required=False, initial="00:00", widget=forms.TimeInput(format='%H:%M'), input_formats=['%H:%M'])
    time_month = forms.TimeField(required=False, initial="00:00", widget=forms.TimeInput(format='%H:%M'), input_formats=['%H:%M'])

    ITEM_CHOICES = (
        ("webmaps", "Maps"),
        ("services", "Services/Layers"),
        ("webapps", "Apps"),
        ("users", "Users")
    )
    items = forms.MultipleChoiceField(widget=forms.CheckboxSelectMultiple,
                                      choices=ITEM_CHOICES, initial=[])

    def clean(self):
        cleaned_data = super().clean()
        repeat_type = cleaned_data.get("repeat_type")
        if repeat_type == "minute":
            if cleaned_data.get("day_of_week_minute"):
                cleaned_data["day_of_week_minute"] = ",".join(cleaned_data["day_of_week_minute"])
            if not cleaned_data.get("day_of_week_minute"):
                self.add_error("day_of_week_minute", "A day is required")

        if repeat_type == "hour":
            if cleaned_data.get("day_of_week_hour"):
                cleaned_data["day_of_week_hour"] = ",".join(cleaned_data["day_of_week_hour"])
            if not cleaned_data.get("day_of_week_hour"):
                self.add_error("day_of_week_hour", "A day is required")

        if repeat_type == "week":
            if cleaned_data.get("day_of_week_week"):
                cleaned_data["day_of_week_week"] = ",".join(cleaned_data["day_of_week_week"])
            if not cleaned_data.get("day_of_week_week"):
                self.add_error("day_of_week_week", "A day is required")

        if cleaned_data.get("ending_on") == "date" and not cleaned_data.get("ending_on_date"):
            self.add_error("ending_on_date", "Date is required.")
        if cleaned_data.get("ending_on") == "count" and not cleaned_data.get("ending_on_count"):
            self.add_error("ending_on_count", "Count is required.")
        if not cleaned_data.get("items"):
            self.add_error("items", "Items are required.")
        return cleaned_data

    def save_task(self):
        cleaned_data = self.cleaned_data
        cleaned_data["instance"] = str(cleaned_data.get("instance"))
        try:
            if cleaned_data["repeat_type"] == "minute":
                schedule, _ = CrontabSchedule.objects.get_or_create(
                    minute=f"*/{cleaned_data['repeat_interval_minute']}",
                    hour="*" if (cleaned_data["between_hours_start_minute"].hour == 0 and cleaned_data[
                        "between_hours_end_minute"].hour == 0) else
                    f"{cleaned_data['between_hours_start_minute'].hour}-{cleaned_data['between_hours_end_minute'].hour}",
                    day_of_week=cleaned_data["day_of_week_minute"],
                    day_of_month="*",
                    month_of_year="*",
                )
            elif cleaned_data["repeat_type"] == "hour":
                schedule, _ = CrontabSchedule.objects.get_or_create(
                    minute=f"{cleaned_data['on_minute_hour']}",
                    hour=f"*/{cleaned_data['repeat_interval_hour']}"
                        if (cleaned_data["between_hours_start_hour"].hour == 0 and cleaned_data[
                            "between_hours_end_hour"].hour == 0)
                        else f"{cleaned_data['between_hours_start_hour'].hour}-{cleaned_data['between_hours_end_hour'].hour}/{cleaned_data['repeat_interval_hour']}",
                    day_of_week=cleaned_data["day_of_week_hour"],
                    day_of_month="*",
                    month_of_year="*"
                )
            elif cleaned_data["repeat_type"] == "day":
                schedule, _ = CrontabSchedule.objects.get_or_create(
                    minute=f"{cleaned_data['time_day'].minute}",
                    hour=f"{cleaned_data['time_day'].hour}",
                    day_of_week=f"*/{cleaned_data['repeat_interval_day']}",
                    day_of_month="*",
                    month_of_year="*"
                )
            elif cleaned_data["repeat_type"] == "week":
                schedule, _ = CrontabSchedule.objects.get_or_create(
                    minute=f"{cleaned_data['time_week'].minute}",
                    hour=f"{cleaned_data['time_week'].hour}",
                    day_of_week=cleaned_data['day_of_week_week'],
                    day_of_month="*",
                    month_of_year="*"
                )
            elif cleaned_data["repeat_type"] == "month":
                schedule, _ = CrontabSchedule.objects.get_or_create(
                    minute=f"{cleaned_data['time_month'].minute}",
                    hour=f"{cleaned_data['time_month'].hour}",
                    day_of_week="*",
                    day_of_month=cleaned_data.get("day_of_month", "*"),
                    month_of_year=f"*/{cleaned_data['repeat_interval_month']}"
                )
            else:
                return None, "Invalid repeat type selected"

            # Custom serializer function
            def json_serial(obj):
                if isinstance(obj, (datetime.date, datetime.datetime, datetime.time)):
                    return obj.isoformat()  # Convert to ISO 8601 string
                raise TypeError(f"Type {type(obj)} not serializable")

            # Create Periodic Task
            try:
                task, _ = PeriodicTask.objects.update_or_create(
                    name=cleaned_data["instance"],
                    defaults={
                        "interval": schedule if isinstance(schedule, IntervalSchedule) else None,
                        "crontab": schedule if isinstance(schedule, CrontabSchedule) else None,
                        "task": "app.tasks.update_all",
                        "kwargs": json.dumps(cleaned_data, default=json_serial),
                        "enabled": True,
                        "start_time": cleaned_data.get("beginning_on"),
                        "expires": None if cleaned_data.get("ending_on") != "date" else cleaned_data.get(
                            "ending_on_date")
                    }

                )
                portal = Portal.objects.get(alias=cleaned_data["instance"])
                portal.task = task
                portal.save()
            except Exception as e:
                return None, str(e)
            return task, None

        except Exception as e:
            return None, str(e)


class SiteSettingsForm(forms.ModelForm):
    """
    Form for managing site-wide email configuration settings.

    Based on the `SiteSettings` model. Includes validation for email port numbers
    and encryption types, providing advisory messages for standard port usage.

    Meta:
        model (SiteSettings): The model this form is based on.
        fields (list): List of fields from SiteSettings model to include in the form.
    """

    class Meta:
        model = SiteSettings
        fields = [
            "admin_email", "email_host", "email_port", "email_encryption",
            "email_username", "email_password", "from_email", "reply_to",
        ]
        widgets = {
            'email_password': forms.PasswordInput(render_value=False),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def clean_email_encryption(self):
        """Validates the selected email encryption type."""
        enc = self.cleaned_data.get("email_encryption")
        if enc not in ("plain_text", "starttls", "ssl"):
            raise ValidationError("Invalid encryption type selected.")
        return enc

    def clean_email_port(self):
        """Validates the email port number is within the valid range."""
        port = self.cleaned_data.get("email_port")
        if port is not None and (port <= 0 or port > 65535):
            raise ValidationError("Enter a valid port number (1-65535).")
        return port

    def clean(self):
        """
        Cross-field validation for email settings.

        Ensures port is provided if host is set, and suggests standard ports.
        """
        cleaned_data = super().clean()
        host = cleaned_data.get("email_host")
        port = cleaned_data.get("email_port")
        enc = cleaned_data.get("email_encryption")

        if host and port is None:
            self.add_error("email_port", "Email port is required when a host is provided.")

        if host and port and enc:
            if enc == "ssl" and port != 465:
                self.add_error("email_port", f"Port for SSL is typically 465 (you entered {port}).")
            elif enc == "starttls" and port != 587:
                self.add_error("email_port", f"Port for STARTTLS is typically 587 (you entered {port}).")
            elif enc == "plain_text" and port not in [25,
                                                      587]:
                self.add_error("email_port",
                               f"Port for Plain Text/Submission is typically 25 or 587 (you entered {port}).")

        logger.debug(f"SiteSettingsForm cleaned_data: {cleaned_data}")
        return cleaned_data


class ToolsForm(forms.ModelForm):
    """
    Form for configuring portal automation tools, based on `PortalToolSettings`.

    Handles enabling/disabling various tools and their specific parameters.
    The `save` method saves the `PortalToolSettings` instance and then creates or
    updates separate Celery Beat tasks for each enabled tool.
    """

    class Meta:
        model = PortalToolSettings
        fields = [
            'tool_pro_license_enabled', 'tool_pro_duration', 'tool_pro_warning',
            'tool_public_unshare_enabled', 'tool_public_unshare_score',
            'tool_public_unshare_trigger',
            'tool_public_unshare_grace_period',
            'tool_inactive_user_enabled', 'tool_inactive_user_duration',
            'tool_inactive_user_warning', 'tool_inactive_user_action',
        ]

    def clean(self):
        """Validates tool-specific fields based on whether the tool is enabled."""
        cleaned_data = super().clean()

        tool_configs = [
            ('tool_pro_enabled', ['tool_pro_duration', 'tool_pro_warning']),
            ('tool_public_unshare_enabled',
             ['tool_public_unshare_score', 'tool_public_unshare_grace_period', 'tool_public_unshare_trigger']),
            ('tool_inactive_user_enabled',
             ['tool_inactive_user_duration', 'tool_inactive_user_warning', 'tool_inactive_user_action']),
        ]

        for enabled_field, dependent_fields in tool_configs:
            if cleaned_data.get(enabled_field):
                for dep_field in dependent_fields:
                    if not cleaned_data.get(dep_field) and cleaned_data.get(dep_field) != 0:
                        self.add_error(dep_field,
                                       f"This field is required when '{self.fields[enabled_field].label}' is enabled.")

        return cleaned_data

    def save(self, commit=True, portal=None):
        """
        Saves the PortalToolSettings model instance and then creates/updates the
        associated periodic tasks for each enabled automation tool.

        :param commit: If True, saves the instance to the database.
        :type commit: bool
        :param portal: The Portal model instance associated with these tool settings.
                       If not provided, it's derived from the form's instance.
        :type portal: enterpriseviz.models.Portal, optional
        :return: The saved PortalToolSettings instance.
        :rtype: enterpriseviz.models.PortalToolSettings
        """
        settings_instance = super().save(commit=commit)

        # Handle the periodic task creation/update for each tool
        target_portal = portal or settings_instance.portal
        if not target_portal:
            logger.error(f"Portal not available for tool settings {settings_instance.pk}.")
            return settings_instance

        # Create daily schedule (midnight)
        daily_schedule, _ = CrontabSchedule.objects.get_or_create(
            minute='0', hour='0', day_of_week='*', day_of_month='*', month_of_year='*',
        )

        # Tool configurations for creating separate tasks
        tool_configs = [
            {
                'enabled': settings_instance.tool_pro_license_enabled,
                'task_name': f'process-pro-license-{target_portal.alias}',
                'task_function': 'process_pro_license',
                'description': f'Pro License Management for {target_portal.alias}',
                'kwargs': {
                    'portal_alias': target_portal.alias,
                    'duration_days': settings_instance.tool_pro_duration,
                    'warning_days': settings_instance.tool_pro_warning,
                }
            },
            {
                'enabled': settings_instance.tool_inactive_user_enabled,
                'task_name': f'process-inactive-user-{target_portal.alias}',
                'task_function': 'process_inactive_user',
                'description': f'Inactive User Management for {target_portal.alias}',
                'kwargs': {
                    'portal_alias': target_portal.alias,
                    'duration_days': settings_instance.tool_inactive_user_duration,
                    'warning_days': settings_instance.tool_inactive_user_warning,
                    'action': settings_instance.tool_inactive_user_action,
                }
            },
        ]

        # Handle public unshare tool (only for daily trigger)
        if (settings_instance.tool_public_unshare_enabled and
            settings_instance.tool_public_unshare_trigger == 'daily'):
            tool_configs.append({
                'enabled': True,
                'task_name': f'process-public-unshare-{target_portal.alias}',
                'task_function': 'process_public_unshare',
                'description': f'Public Item Unshare Management for {target_portal.alias}',
                'kwargs': {
                    'portal_alias': target_portal.alias,
                    'score_threshold': settings_instance.tool_public_unshare_score,
                }
            })

        # Create or update/delete tasks for each tool
        for config in tool_configs:
            self._manage_tool_task(config, daily_schedule)

        # Clean up old combined task if it exists (backwards compatibility)
        old_task_name = f'portal-tools-automation-{target_portal.alias}'
        try:
            old_task = PeriodicTask.objects.get(name=old_task_name)
            old_task.delete()
            logger.info(f"Removed old combined tools task: {old_task_name}")
        except PeriodicTask.DoesNotExist:
            pass

        return settings_instance

    def _manage_tool_task(self, config, schedule):
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
                status_msg = "created" if created else "updated"
                logger.info(f"Periodic task '{task_name}' {status_msg} successfully.")
            except Exception as e:
                logger.error(f"Failed to save periodic task '{task_name}': {e}", exc_info=True)
        else:
            # Delete the task if it exists
            try:
                existing_task = PeriodicTask.objects.get(name=task_name)
                existing_task.delete()
                logger.info(f"Disabled and removed periodic task: {task_name}")
            except PeriodicTask.DoesNotExist:
                # Task doesn't exist, nothing to do
                pass

