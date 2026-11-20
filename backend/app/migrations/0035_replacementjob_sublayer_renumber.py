"""
Carry the sublayer renumbering plan alongside the replacement string pairs.

A web map that had the whole map image service added holds its sublayer numbers
as bare integers in ``visibleLayers`` and ``layers[].id``, never as ``/0``,
``/1`` URL suffixes — so the URL text replacement the tool runs cannot reach
them. ``build_mapping_rows`` now computes an {old_layer_id: new_layer_id} map
for those, and it has to survive from dry run to execute: recomputing it later
would re-read ``get_source_layer_inventory``, which shifts whenever a portal
sync lands in between.
"""

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("app", "0034_alter_sitesettings_arcgis_user_role"),
    ]

    operations = [
        migrations.AddField(
            model_name="replacementjob",
            name="sublayer_renumber",
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text="{old_layer_id: new_layer_id} for whole-service map image layers, computed at dry-run alongside replacement_pairs. These sublayer numbers are bare integers in the web map JSON, so URL replacement cannot reach them.",
            ),
        ),
    ]
