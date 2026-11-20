"""
Allow one dataset to appear as several layers in the same service.

A feature class can be published more than once in a single service — the same
source with different definition queries, symbology or scale ranges — and each
of those is a distinct layer with its own id in the service. The previous
constraint on (portal_instance, layer_id, service_id) treated them as one row,
so the second layer collided with the first. Adding service_layer_id to the key
keeps the relationship unique per layer in the service.
"""

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("app", "0032_sitesettings_arcgis_signin"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="layer_service",
            unique_together={("portal_instance", "layer_id", "service_id", "service_layer_id")},
        ),
    ]
