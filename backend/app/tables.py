# Licensed under GPLv3 - See LICENSE file for details.
import django_tables2 as tables
from django.utils.html import format_html
from collections import defaultdict
import json


from .models import Webmap, Service, Layer, App, User, LogEntry


class WebmapTable(tables.Table):
    details = tables.TemplateColumn(
        """<calcite-button
          href="{% url 'enterpriseviz:map' instance=record.portal_instance id=record.webmap_id %}"
          hx-target="#mainbodycontent"
          hx-push-url="{% url 'enterpriseviz:map' instance=record.portal_instance id=record.webmap_id %}"
          hx-swap="innerHTML show:window:top"
          kind="inverse"
          scale="s"
          label="View details for {{ record.webmap_title }}">
          Details
        </calcite-button>""",
        orderable=False, attrs={"td": {"class": "table-cell-details"}})
    title_link = tables.Column(orderable=True, accessor="webmap_title")

    class Meta:
        model = Webmap
        fields = ("webmap_owner", "webmap_created",
                  "webmap_modified",
                  "webmap_access",
                  "webmap_views")
        order_by = "title_link"
        sequence = ("details", "title_link")
        attrs = {
            "class": "table table-striped"
        }

    def render_title_link(self, value, record):
        return format_html('<calcite-link href="{}" target="_blank">{}</calcite-link>', record.webmap_url, value)


class ServiceTable(tables.Table):
    details = tables.TemplateColumn(
        """<calcite-button
        href="{% url 'enterpriseviz:service' instance=record.portal_instance url=record.service_name %}"
        hx-target="#mainbodycontent"
        hx-push-url="{% url 'enterpriseviz:service' instance=record.portal_instance url=record.service_name %}"
        hx-swap="innerHTML show:window:top"
        kind="inverse"
        scale="s"
        label="View details for {{ record.service_name }}">
        Details
        </calcite-button>""",
        orderable=False, attrs={"td": {"class": "table-cell-details"}})
    URL = tables.TemplateColumn(
        '{% for url in record.service_url_as_list %} <calcite-link href="{{ url }}" target="_blank">{{ url }}</calcite-link>{% endfor %}',
        orderable=False)
    grouped_layers = tables.Column(empty_values=(), verbose_name="Layers", orderable=False)
    service_usage_trend = tables.Column(empty_values=(), verbose_name="Usage Trend")
    service_usage = tables.Column(
        verbose_name="Trend",
        orderable=False,
        empty_values=(),
        attrs={'td': {'class': 'sparkline-cell'}}
    )

    class Meta:
        model = Service
        fields = ("service_name", "service_mxd_server", "service_mxd",
                  "service_owner",
                  "service_access", "service_usage_trend")
        order_by = "service_name"
        sequence = ("details", "service_name", "grouped_layers", "URL")
        attrs = {
            "class": "table table-striped"
        }

    def render_grouped_layers(self, value, record):
        """Django Tables2 calls this method to populate the `grouped_layers` column."""
        layers = Layer.objects.filter(layer_service__service_id=record).order_by("layer_database")

        # Group layers by database
        grouped_layers = defaultdict(list)
        for layer in layers:
            grouped_layers[layer.layer_database].append(layer.layer_name)

        # Format as a string or HTML table
        result = []
        for database, layer_names in grouped_layers.items():
            result.append(f"<strong>{database}</strong>: {', '.join(layer_names)}")

        return format_html("<br>".join(result) if result else "No Layers")

    def render_service_usage_trend(self, value):
        if value is None or not isinstance(value, (int, float)):
            return "N/A"

        trend_symbol = "+" if value > 0 else ""
        trend_class = ""
        if value > 0:
            trend_class = "trend-positive"
        elif value < 0:
            trend_class = "trend-negative"


        if trend_class:
            return format_html(
                '<span class="{}">{}{}%</span>',
                trend_class,
                trend_symbol,
                value
            )
        else:
            return format_html(
                '<span>{}{}%</span>',
                trend_symbol,
                value
            )

    def render_service_usage(self, value, record):
        # Create unique ID for this sparkline
        chart_id = f"sparkline-{record.pk}"

        return format_html(
            '<canvas id="{}" class="sparkline-chart" width="110" height="40" data-chart="{}"></canvas>',
            chart_id,
            json.dumps(value)
        )

class LayerTable(tables.Table):
    details=tables.TemplateColumn(
        """<calcite-button
        href="{% url 'enterpriseviz:layer' name=record.layer_name %}?{% if record.layer_server %}server={{ record.layer_server }}&{% endif %}{% if record.layer_database %}database={{ record.layer_database }}&{% endif %}{% if record.layer_version %}version={{ record.layer_version }}{% endif %}"
        hx-target="#mainbodycontent"
        hx-push-url="{% url 'enterpriseviz:layer' name=record.layer_name %}?{% if record.layer_server %}server={{ record.layer_server }}&{% endif %}{% if record.layer_database %}database={{ record.layer_database }}&{% endif %}{% if record.layer_version %}version={{ record.layer_version }}{% endif %}"
        hx-swap="innerHTML show:window:top"
        kind="inverse"
        scale="s"
        label="View details for {{ record.layer_name }}">
        Details
        </calcite-button>""",
        orderable=False, attrs={"td": {"class": "table-cell-details"}})

    class Meta:
        model = Layer
        fields = ("layer_name", "layer_database", "layer_server", "layer_version")
        order_by = "layer_name"
        sequence = ("details", "layer_name")
        attrs = {
            "class": "table table-striped"
        }


class AppTable(tables.Table):
    title_link = tables.Column(orderable=True, accessor="app_title")

    class Meta:
        model = App
        fields = (
            "app_owner", "app_type", "app_created", "app_modified",
            "app_access")
        order_by = "title_link"
        sequence = ("title_link", "app_type")
        attrs = {
            "class": "table table-striped"
        }

    def render_title_link(self, value, record):
        return format_html('<calcite-link href="{}" target="_blank">{}</calcite-link>', record.app_url, value)


class UserTable(tables.Table):
    class Meta:
        model = User
        fields = ("user_username", "user_first_name", "user_last_name", "user_email", "user_created",
                  "user_last_login", "user_role", "user_pro_license", "user_pro_last", "user_items")
        order_by = 'user_username'
        attrs = {
            "class": "table table-striped"
        }


class LogEntryTable(tables.Table):
    DEFAULT_VISIBLE_COLUMNS = ("timestamp", "level", "logger_name", "message", "traceback")

    timestamp = tables.DateTimeColumn(format="Y-m-d H:i:s T", verbose_name="Timestamp")
    message = tables.Column(verbose_name="Message", attrs={
        "td": {"class": "table-cell-message"}})
    traceback = tables.TemplateColumn(
        template_code='''
                {% if record.traceback %}
                    <calcite-button appearance="transparent" scale="s" icon-start="information" id="traceback-btn-{{ record.id }}">View</calcite-button>
                    <calcite-popover reference-element="traceback-btn-{{ record.id }}" label="Traceback" placement="auto" scale="m" auto-close>
                        <pre class="table-cell-message-popover">{{ record.traceback }}</pre>
                    </calcite-popover>
                {% else %}
                    -
                {% endif %}
            ''',
        verbose_name="Details",
        orderable=False
    )

    class Meta:
        model = LogEntry
        fields = (
            "timestamp", "level", "logger_name", "message", "funcName", "traceback",
            "request_id", "request_username", "client_ip", "request_path",
            "request_method", "request_duration", "pathname", "lineno"
        )
        sequence = (
            "timestamp", "level", "logger_name", "funcName", "message", "traceback",
            "request_id", "request_username", "client_ip", "request_path",
            "request_method", "request_duration"
        )
        order_by = "-timestamp"
        attrs = {
            "class": "table table-striped"
        }

    @classmethod
    def get_column_labels(cls):
        """Return a list of tuples (field_name, field_label) for columns in Meta.sequence or Meta.fields."""
        column_source_list = cls.Meta.sequence if hasattr(cls.Meta,
                                                          'sequence') and cls.Meta.sequence else cls.Meta.fields
        labels = []
        if column_source_list:
            for field_name in column_source_list:
                if field_name in cls.base_columns:
                    labels.append((field_name, cls.base_columns[field_name].verbose_name))
        return labels
