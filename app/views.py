from django.shortcuts import render, redirect, get_object_or_404
from django.views.generic import DetailView, View
from django.views.generic.edit import CreateView, DeleteView, UpdateView
from django.template import loader
from django.http import HttpResponse, HttpResponseRedirect
from django.contrib.auth.forms import UserCreationForm, AuthenticationForm
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.decorators import login_required
from app import webmaps
from django.contrib import messages
from .models import Webmap, Portal, Service, PortalCreateForm, Layer, App
from django.core.serializers.json import DjangoJSONEncoder
from django.conf import settings
import json
import datetime


class PortalMapView(LoginRequiredMixin, View):
    def get(self, request, *args, **kwargs):
        details, alt, services, layers = webmaps.map_details(kwargs['id'])
        context = {'details': [details, services, layers, alt, details,
                               Portal.objects.get(alias=kwargs['instance']).url, kwargs['id']],
                   'portal': Portal.objects.values_list('alias', flat=True)}
        return render(request, 'portals/portal_detail_map.html', context)


class PortalLayerView(LoginRequiredMixin, View):
    def get(self, request, *args, **kwargs):
        version = request.GET.get('version')
        dbserver = request.GET.get('server')
        database = request.GET.get('database')
        details, services, maps, apps = webmaps.layer_details(dbserver, database, version, kwargs['name'])
        if settings.USE_SERVICE_USAGE_REPORT:
            # usage, message_type, result = webmaps.get_usage_report(services)
            usage = {
                'labels': ['2021-01-08 14:00:00', '2021-01-09 14:00:00', '2021-01-10 14:00:00', '2021-01-11 14:00:00',
                           '2021-01-12 14:00:00', '2021-01-13 14:00:00', '2021-01-14 14:00:00', '2021-01-15 14:00:00',
                           '2021-01-16 14:00:00', '2021-01-17 14:00:00', '2021-01-18 14:00:00', '2021-01-19 14:00:00',
                           '2021-01-20 14:00:00', '2021-01-21 14:00:00', '2021-01-22 14:00:00', '2021-01-23 14:00:00',
                           '2021-01-24 14:00:00', '2021-01-25 14:00:00', '2021-01-26 14:00:00', '2021-01-27 14:00:00',
                           '2021-01-28 14:00:00', '2021-01-29 14:00:00', '2021-01-30 14:00:00', '2021-01-31 14:00:00',
                           '2021-02-01 14:00:00', '2021-02-02 14:00:00', '2021-02-03 14:00:00', '2021-02-04 14:00:00',
                           '2021-02-05 14:00:00', '2021-02-06 14:00:00'], 'datasets': [
                    {'label': 'services/folder1/TestService1.MapServer',
                                      'data': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 27, 0, 65, 34, 0,
                                               0, 0, 4, 0, 0, 5, 0], 'fill': False},
                    ]}
            context = {'details': [details, services, maps, apps],
                       'portal': Portal.objects.values_list('alias', flat=True),
                       'service_usage': usage}
        else:
            context = {'details': [details, services, maps, apps],
                       'portal': Portal.objects.values_list('alias', flat=True)}
        return render(request, 'portals/portal_detail_layer.html', context)


class PortalCreateView(LoginRequiredMixin, CreateView):
    def get(self, request, *args, **kwargs):
        context = {'form': PortalCreateForm(), 'portal': Portal.objects.values_list('alias', flat=True)}
        return render(request, 'portals/portal_add.html', context)

    def post(self, request, *args, **kwargs):
        try:
            form = PortalCreateForm(request.POST)
            if form.is_valid():
                if form.cleaned_data['store_password']:
                    if webmaps.try_connection(form.cleaned_data):
                        add_result = "Successfully connected to {0} as {1}".format(form.cleaned_data['url'],
                                                                                   form.cleaned_data['username'])
                        message_type = "success"
                        portal = form.save()
                        portal.save()
                    else:
                        add_result = "Unable to connect to {0} as {1}. Please verify info.".format(
                            form.cleaned_data['url'],
                            form.cleaned_data['username'])
                        message_type = "error"
                else:
                    portal = form.save()
                    portal.save()
                    add_result = "Successfully added {0} as {1}. Authentication will be required when refreshing data.".format(
                        form.cleaned_data['url'], form.cleaned_data['alias'])
                    message_type = "success"
            else:
                add_result = "Form is not valid. See error above."
                message_type = "error"
        except Exception as ex:
            pass
        finally:
            return render(request, 'portals/portal_add.html',
                          {'form': form, 'add_result': add_result,
                           'portal': Portal.objects.values_list('alias', flat=True),
                           'message_type': message_type})


@login_required
def index(request):
    context = {'query_results': Webmap.objects.all(), 'portal': Portal.objects.values_list('alias', flat=True),
               'services': Service.objects.all(), 'layers': Layer.objects.all(), 'apps': App.objects.all()}
    template = loader.get_template('app/index.html')
    return HttpResponse(template.render(context, request))


@login_required
def viz_html(request, instance):
    print(Webmap.objects.filter(portal_instance=instance))
    message_type = ""
    result = ""
    if request.POST.get('Refresh Webmap'):
        if not Portal.objects.get(alias=instance).store_password:
            username, password = webmaps.get_connection(instance)
            message_type, result = webmaps.update_webmaps(instance, username, password)
        else:
            print("post refresh webmaps")
            message_type, result = webmaps.update_webmaps(instance)
    if request.POST.get('Refresh Services'):
        if not Portal.objects.get(alias=instance).store_password:
            username, password = webmaps.get_connection(instance)
            message_type, result = webmaps.update_services(instance, username, password)
        else:
            print("post refresh services")
            message_type, result = webmaps.update_services(instance)
    if request.POST.get('Get Unused Services'):
        if not Portal.objects.get(alias=instance).store_password:
            username, password = webmaps.get_connection(instance)
            message_type, result = webmaps.get_unused(instance, username, password)
        else:
            print("Get Unused Services")
            message_type, result = webmaps.get_unused(instance)
    if request.POST.get('Refresh Webapp'):
        if not Portal.objects.get(alias=instance).store_password:
            username, password = webmaps.get_connection(instance)
            message_type, result = webmaps.update_webapps(instance, username, password)
        else:
            print('post refresh webapps')
            message_type, result = webmaps.update_webapps(instance)
    context = {'query_results': Webmap.objects.filter(portal_instance=instance),
               'portal': Portal.objects.values_list('alias', flat=True),
               'updated': Portal.objects.get(alias=instance), 'instance': instance,
               'services': Service.objects.filter(portal_instance=instance),
               'layers': Layer.objects.filter(portal_instance=instance),
               'apps': App.objects.filter(portal_instance=instance),
               'message_type': message_type, 'result': result}
    template = loader.get_template('app/index.html')
    return HttpResponse(template.render(context, request))


def login_view(request):
    if request.method == 'POST':
        form = AuthenticationForm(request, data=request.POST)
        if form.is_valid():
            username = form.cleaned_data.get('username')
            password = form.cleaned_data.get('password')
            user = authenticate(username=username, password=password)
            if user is not None:
                login(request, user)
                messages.info(request, f"You are now logged in as {username}")
                return redirect("/")
            else:
                messages.error(request, "Invalid username or password.")
        else:
            messages.error(request, "Invalid username or password...")
    else:
        form = AuthenticationForm()
    return render(request, 'app/login.html', {'form': form})


def logout_view(request):
    logout(request)
    return redirect("/")
