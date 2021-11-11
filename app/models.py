from __future__ import unicode_literals

from django.db import models
from django import forms


# Create your models here.
# Table for webmaps
class Webmap(models.Model):
    portal_instance = models.TextField(blank=False, null=True)
    webmap_id = models.CharField(blank=False, max_length=50, primary_key=True)
    webmap_title = models.TextField(blank=False, null=True)
    webmap_url = models.TextField(blank=False, null=True)
    webmap_owner = models.CharField(blank=True, max_length=200)
    webmap_created = models.DateTimeField(blank=True)
    webmap_modified = models.DateTimeField(blank=True)
    webmap_access = models.CharField(blank=True, max_length=100)
    webmap_extent = models.CharField(blank=True, max_length=100)
    webmap_description = models.TextField(blank=True, null=True)
    webmap_views = models.IntegerField(blank=True, null=True)
    webmap_layers = models.JSONField(default=dict)
    webmap_services = models.TextField(blank=True, null=True)
    webmap_dependency = models.JSONField(default=list)

    class Meta:
        ordering = ['webmap_title']

    def __str__(self):
        return '%s' % self.webmap_title


# Attempt at storing portal credentials
class Portal(models.Model):
    alias = models.TextField(blank=False, null=False, unique=True)
    url = models.TextField(blank=False, null=False)
    store_password = models.BooleanField(default=False)
    types = (
        ('agol','ArcGIS Online'),
        ('portal', 'Enterprise Portal'),
    )
    portal_type = models.CharField(max_length=32, choices=types, blank=True, null=True)
    username = models.TextField(blank=True, null=False)
    password = models.TextField(blank=True, null=False)
    token = models.TextField(blank=True, null=True)
    token_expiration = models.DateTimeField(blank=True, null=True)
    webmap_updated = models.DateTimeField(blank=True, null=True)
    service_updated = models.DateTimeField(blank=True, null=True)
    app_updated = models.DateTimeField(blank=True, null=True)

    def __str__(self):
        return self.alias


class Service(models.Model):
    portal_instance = models.TextField(blank=False, null=True)
    service_name = models.TextField(blank=False, null=False)
    service_url = models.TextField(blank=True, null=True)
    service_layers = models.JSONField(default=dict)
    service_mxd_server = models.TextField(blank=True, null=True)
    service_mxd = models.TextField(blank=True, null=True)
    service_type = models.TextField(blank=False, null=False)
    portal_id = models.JSONField(default=dict)

    def __str__(self):
        return '%s' % self.service_name
    def service_url_as_list(self):
        return self.service_url.split(",")


class Layer(models.Model):
    portal_instance = models.TextField(blank=False, null=True)
    layer_server = models.TextField(blank=False, null=True)
    layer_version = models.TextField(blank=False, null=True)
    layer_database = models.TextField(blank=False, null=True)
    layer_name = models.TextField(blank=False, null=True)

    def __str__(self):
        return '%s.%s.%s.%s' % (self.layer_name,self.layer_server,self.layer_database,self.layer_version)


class PortalCreateForm(forms.ModelForm):
    class Meta:
        model = Portal
        fields = ('alias', 'url', 'portal_type', 'store_password', 'username', 'password')

    def clean(self):
        cleaned_data = super(PortalCreateForm, self).clean()
        store_password = cleaned_data.get('store_password')
        username = cleaned_data.get('username')
        password = cleaned_data.get('password')
        if store_password and (username.strip() == '' or password.strip() == ''):
            raise forms.ValidationError(
                'Please fill in username and password fields or uncheck to not store information.')
        return cleaned_data


class App(models.Model):
    portal_instance = models.TextField(blank=False, null=True)
    app_id = models.CharField(blank=False, max_length=50, primary_key=True)
    app_title = models.TextField(blank=False, null=True)
    app_url = models.TextField(blank=False, null=True)
    app_owner = models.CharField(blank=True, max_length=200)
    app_created = models.DateTimeField(blank=True)
    app_modified = models.DateTimeField(blank=True)
    app_access = models.CharField(blank=True, max_length=100)
    app_extent = models.CharField(blank=True, max_length=100)
    app_description = models.TextField(blank=True, null=True)
    app_views = models.IntegerField(blank=True, null=True)
    app_dependent = models.JSONField(default=dict)

    def __str__(self):
        return self.app_title