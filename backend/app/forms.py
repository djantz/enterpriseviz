# Licensed under GPLv3 - See LICENSE file for details.
import datetime
import json

from django import forms
from django_celery_beat.models import PeriodicTask, IntervalSchedule, CrontabSchedule

from .models import Portal


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
