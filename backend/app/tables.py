# Licensed under GPLv3 - See LICENSE file for details.
import django_tables2 as tables
from django.utils.html import format_html
from collections import defaultdict


from .models import Webmap, Service, Layer, App, User, LogEntry


class WebmapTable(tables.Table):
    details = tables.TemplateColumn(
        """<calcite-button
  hx-get="{% url 'enterpriseviz:map' instance=record.portal_instance id=record.webmap_id %}"
  hx-target="#mainbodycontent"
  hx-push-url="{% url 'enterpriseviz:map' instance=record.portal_instance id=record.webmap_id %}"
  hx-swap="show:top"
  alignment="center"
  appearance="solid"
  kind="inverse"
  scale="s"
  width="auto"
  type="submit">
  Details
</calcite-button>""",
        orderable=False, attrs={"td": {"style": "width:74px;"}})
    title_link = tables.Column(orderable=True, accessor="webmap_title")

    class Meta:
        model = Webmap
        fields = ("webmap_owner", "webmap_created",
                  "webmap_modified",
                  "webmap_access",
                  "webmap_views")
        order_by = "title_link"
        sequence = ("details", "title_link")

    def render_title_link(self, value, record):
        return format_html('<calcite-link href="{}" target="_blank">{}</calcite-link>', record.webmap_url, value)


class ServiceTable(tables.Table):
    details = tables.TemplateColumn(
        """<calcite-button hx-get="{% url 'enterpriseviz:service' instance=record.portal_instance url=record.service_name %}" hx-target="#mainbodycontent" hx-push-url="{% url 'enterpriseviz:service' instance=record.portal_instance url=record.service_name %}" hx-swap="show:top" alignment="center" appearance="solid" kind="inverse" scale="s" width="auto" type="submit">Details</calcite-button>""",
        orderable=False, attrs={"td": {"style": "width:74px;"}})
    usage = tables.TemplateColumn('<span class="sparkline">{{ record.service_usage|cut:"["|cut:"]"|cut:" " }}</span>',
                                  orderable=False)
    URL = tables.TemplateColumn(
        '{% for url in record.service_url_as_list %} <calcite-link href="{{ url }}" target="_blank">{{ url }}</calcite-link>{% endfor %}',
        orderable=False)
    grouped_layers = tables.Column(empty_values=(), verbose_name="Layers", orderable=False)
    service_usage_trend = tables.Column(empty_values=(), verbose_name="Usage Trend")

    class Meta:
        model = Service
        fields = ("service_name", "service_mxd_server", "service_mxd",
                  "service_owner",
                  "service_access", "service_usage_trend")
        order_by = "service_name"
        sequence = ("details", "service_name", "grouped_layers", "URL")

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
        if value is None:
            return "N/A"

        trend_symbol = "+" if value > 0 else ""
        trend_color = "green" if value > 0 else "red"

        return format_html(
            '<span style="color: {};">{}{}%</span>',
            trend_color, trend_symbol, value
        )

class LayerTable(tables.Table):
    details = tables.TemplateColumn(
        """<calcite-button hx-get="{% url 'enterpriseviz:layer' name=record.layer_name %}?server={{ record.layer_server }}&database={{ record.layer_database }}&version={{ record.layer_version }}" hx-target="#mainbodycontent" hx-push-url="{% url 'enterpriseviz:layer' name=record.layer_name %}?server={{ record.layer_server }}&database={{ record.layer_database }}&version={{ record.layer_version }}" hx-swap="show:top" alignment="center" appearance="solid" kind="inverse" scale="s" width="auto" type="submit">Details</calcite-button>""",
        orderable=False, attrs={"td": {"style": "width:74px;"}})

    class Meta:
        model = Layer
        fields = ("layer_name", "layer_database", "layer_server", "layer_version")
        order_by = "layer_name"
        sequence = ("details", "layer_name")


class AppTable(tables.Table):
    title_link = tables.Column(orderable=True, accessor="app_title")

    class Meta:
        model = App
        fields = (
            "app_owner", "app_type", "app_created", "app_modified",
            "app_access")
        order_by = "title_link"
        sequence = ("title_link", "app_type")

    def render_title_link(self, value, record):
        return format_html('<calcite-link href="{}" target="_blank">{}</calcite-link>', record.app_url, value)


class UserTable(tables.Table):
    class Meta:
        model = User
        fields = ("user_username", "user_first_name", "user_last_name", "user_email", "user_created",
                  "user_last_login", "user_role", "user_pro_license", "user_pro_last", "user_items")
        order_by = 'user_username'


class LogEntryTable(tables.Table):
    DEFAULT_VISIBLE_COLUMNS = ("timestamp", "level", "logger_name", "message", "traceback")

    timestamp = tables.DateTimeColumn(format="Y-m-d H:i:s T", verbose_name="Timestamp")
    message = tables.Column(verbose_name="Message", attrs={
        "td": {"style": "max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"}})
    traceback = tables.TemplateColumn(
        template_code='''
                {% if record.traceback %}
                    <calcite-button appearance="transparent" scale="s" icon-start="information" id="traceback-btn-{{ record.id }}">View</calcite-button>
                    <calcite-popover reference-element="traceback-btn-{{ record.id }}" label="Traceback" placement="auto" scale="m" auto-close>
                        <pre style="white-space: pre-wrap; word-wrap: break-word; max-height: 300px; overflow-y: auto; padding: 0.5rem;">{{ record.traceback }}</pre>
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
