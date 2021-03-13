from .models import Webmap, Portal, Service, Layer, App
from arcgis import gis
from arcgis.gis import server, admin
from arcgis.mapping import WebMap
import time
import datetime
import json
import copy
from django.core.serializers.json import DjangoJSONEncoder


def try_connection(instance):
    try:
        url = instance['url']
        instance_username = instance['username']
        instance_password = instance['password']
        target = gis.GIS(url, instance_username, instance_password)
        if target.properties.user.username == instance_username:
            return True
    except:
        message_type = "error"
        result = ""
        return False


def connect(instance, username=None, password=None):
    try:
        instance_item = Portal.objects.get(alias=instance)
        url = instance_item.url
        token_expiration = instance_item.token_expiration
        if username and password:
            target = gis.GIS(url, username, password)
        elif token_expiration is None or token_expiration < datetime.datetime.now() or instance == "AGOL":
            instance_username = instance_item.username
            instance_password = instance_item.password
            target = gis.GIS(url, instance_username, instance_password)
            instance_item.token = target._con.token
            instance_item.token_expiration = datetime.datetime.now() + datetime.timedelta(
                minutes=target._con._expiration)
            instance_item.save()
        else:
            instance_token = instance_item.token
            target = gis.GIS(url, token=instance_token)
        return target
    except Exception as e:
        return False


def update_webmaps(instance, username=None, password=None):
    try:
        instance_item = Portal.objects.get(alias=instance)
        target = connect(instance, username, password)

        wms = target.content.search("NOT owner:esri*", "Web Map", max_items=2000)
    except Exception as e:
        message_type = "error"
        result = "Unable to connect to {0}...{1}".format(instance, e)
        return message_type, result
    try:
        Webmap.objects.filter(portal_instance=instance).delete()
        for wm in wms:
            services = []
            layers = {}
            alt_dependency = []
            print(wm.title)
            if wm.access == 'shared':  # or used .shared_with https://developers.arcgis.com/python/api-reference/arcgis.gis.toc.html#arcgis.gis.Item.shared_with
                access = "Groups: " + ", ".join(x.title for x in wm.shared_with['groups'])
            else:
                access = wm.access.title()
            try:
                m = WebMap(wm)
                for layer in m.layers:
                    if hasattr(layer, 'url'):
                        url = layer.url if not layer.url.split("/")[-1].isdigit() else "/".join(
                            layer.url.split("/")[:-1])
                    else:
                        url = None
                    try:
                        service_title = '/'.join(url.split('/rest/services/')[1].split('/')[:-1])
                    except:
                        service_title = url
                    if url not in services:
                        services.append(url)
                    layers[layer.title] = [layer.url, layer.layerType, layer.id]
                if wm.dependent_upon()['total'] > 0:
                    d = [i.get('id') for i in wm.dependent_upon()['list']]
                    dl = []
                    for i in d:
                        try:
                            if target.content.get(i) is not None:
                                dl.append(
                                    "{}, {}, {}".format(target.content.get(i).title, target.content.get(i).homepage,
                                                        target.content.get(i).type))
                            else:
                                dl.append("{}".format(i))
                        except Exception as e:
                            dl.append("{}".format(i))
                    alt_dependency.append(
                        "Item: {2} id {0} is dependent on {3} items:\r\n{1}".format(wm.itemid, "\r\n".join(dl),
                                                                                    wm.title,
                                                                                    wm.dependent_upon()['total']))
                else:
                    alt_dependency.append("Item is not dependent on any items.")
                if wm.dependent_to()['total'] > 0:
                    d = [i.get('id') for i in wm.dependent_to()['list']]
                    dl = []
                    for i in d:
                        try:
                            if target.content.get(i) is not None:
                                dl.append(
                                    "{}, {}, {}".format(target.content.get(i).title, target.content.get(i).homepage,
                                                        target.content.get(i).type))
                            else:
                                dl.append("{}".format(i))
                        except Exception as e:
                            dl.append("{}".format(i))
                    alt_dependency.append(
                        "Item: {2} id {0} is a dependency to {3} items:\r\n{1}".format(wm.itemid, "\r\n,".join(dl),
                                                                                       wm.title,
                                                                                       wm.dependent_to()['total']))
                else:
                    alt_dependency.append("Item is not a dependency to any items.")
                new_entry = Webmap(portal_instance=instance, webmap_id=wm.id, webmap_title=wm.title,
                                   webmap_url=wm.homepage,
                                   webmap_owner=wm.owner, webmap_created=ts(wm.created),
                                   webmap_modified=ts(wm.modified),
                                   webmap_access=access, webmap_extent=wm.extent, webmap_description=wm.description,
                                   webmap_views=wm.numViews, webmap_layers=layers, webmap_services=services,
                                   webmap_dependency=alt_dependency)
                new_entry.save()
            except:
                pass
        instance_item.webmap_updated = datetime.datetime.now()
        instance_item.save()
        message_type = "success"
        result = "Updated webmaps for {0}...".format(instance)
        return message_type, result
    except Exception as e:
        message_type = "error"
        result = "Unable to update webmaps for{0}...{1}".format(instance, e)
        return message_type, result


def update_services(instance, username=None, password=None):
    try:
        import re
        instance_item = Portal.objects.get(alias=instance)
        target = connect(instance, username, password)
        gis_servers = target.admin.servers.list()
    except:
        message_type = "error"
        result = "Unable to connect to {0}...{1}".format(instance, e)
        return message_type, result

    try:
        Service.objects.filter(portal_instance=instance).delete()
        Layer.objects.filter(portal_instance=instance).delete()

        regexp_server = re.compile("(?<=SERVER=)([^;]*)")
        regexp_version = re.compile("(?<=VERSION=)([^;]*)")
        regexp_database = re.compile("(?<=DATABASE=)([^;]*)")
        for gis_server in gis_servers:
            folders = gis_server.services.folders
            for folder in folders:
                services = gis_server.services.list(folder=folder)
                for service in services:
                    try:
                        url = {}
                        portal_ids = {}
                        service_layers = {}
                        service_type = service.properties["type"]
                        if folder == '/':
                            name = "{}".format(service.properties["serviceName"])
                            url[service._con.baseurl + "{}/{}".format(name, service_type)] = []
                        else:
                            name = "{}/{}".format(folder, service.properties["serviceName"])
                            url[service._con.baseurl + "{}/{}".format(name, service_type)] = []
                        portal_items = service._json_dict['portalProperties']['portalItems']
                        for item in portal_items:
                            portal_ids[item['type']] = item['itemID']
                            if item['type'] == "FeatureServer":
                                url[service._con.baseurl + "{}/{}".format(name, "FeatureServer")] = []
                        sm = json.loads(service._service_manifest())
                        if 'databases' in sm.keys():
                            db_obj = sm["databases"]
                            for obj in db_obj:
                                db_server = ""
                                db_version = ""
                                db_database = ""
                                if "Sde" in obj["onServerWorkspaceFactoryProgID"]:
                                    db_server = str(
                                        regexp_server.search(obj["onServerConnectionString"]).group(0)).upper()
                                    db_version = str(
                                        regexp_version.search(obj["onServerConnectionString"]).group(0)).upper()
                                    db_database = str(
                                        regexp_database.search(obj["onServerConnectionString"]).group(0)).upper()
                                    db = "{}@{}@{}".format(db_server, db_database, db_version)
                                elif "FileGDB" in obj["onServerWorkspaceFactoryProgID"]:
                                    db_database = obj["onServerConnectionString"].split("DATABASE=")[1].replace('\\\\',
                                                                                                                '\\')
                                    db = db_database
                                datasets = obj["datasets"]
                                for dataset in datasets:
                                    service_layers[dataset["onServerName"]] = db
                                    if db_database and not Layer.objects.filter(portal_instance=instance,
                                                                                layer_server=db_server,
                                                                                layer_version=db_version,
                                                                                layer_database=db_database,
                                                                                layer_name=dataset[
                                                                                    "onServerName"]).exists():
                                        new_layer = Layer(portal_instance=instance, layer_server=db_server,
                                                          layer_version=db_version, layer_database=db_database,
                                                          layer_name=dataset["onServerName"])
                                        new_layer.save()
                        if 'resources' in sm.keys():
                            res_obj = sm["resources"]
                            for obj in res_obj:
                                mxd = obj["onPremisePath"]
                                mxd_server = obj["clientName"]
                        else:
                            mxd = None
                            mxd_server = None
                        new_entry = Service(portal_instance=instance, service_name=name, service_url=url,
                                            service_layers=service_layers, service_mxd_server=mxd_server,
                                            service_mxd=mxd, portal_id=portal_ids, service_type=service_type)
                        new_entry.save()
                    except:
                        pass
        instance_item.service_updated = datetime.datetime.now()
        instance_item.save()
        message_type = "success"
        result = "Updated Services for {0}...".format(instance)
        return message_type, result
    except Exception as e:
        message_type = "error"
        result = "Unable to update Services for {0}...{1}".format(instance, e)
        return message_type, result


def update_webapps(instance, username=None, password=None):
    try:
        instance_item = Portal.objects.get(alias=instance)
        target = connect(instance, username, password)
    except:
        message_type = "error"
        result = "Unable to connect to {0}...{1}".format(instance, e)
        return message_type, result
    try:
        App.objects.filter(portal_instance=instance).delete()
        items = (target.content.search("NOT owner:esri*", "Web Mapping Application", max_items=2000) +
                 target.content.search("NOT owner:esri*", "Dashboard", max_items=2000))
        for item in items:
            alt_dependent = {}
            if item.access == 'shared':  # or used .shared_with https://developers.arcgis.com/python/api-reference/arcgis.gis.toc.html#arcgis.gis.Item.shared_with
                access = "Groups: " + ", ".join(x.title for x in item.shared_with['groups'])
            else:
                access = item.access.title()
            if item.dependent_upon()['total'] > 0:
                d = [i.get('id') for i in item.dependent_upon()['list']]
                alt_dependent['Upon'] = []
                for i in d:
                    if i is not None:
                        try:
                            if target.content.get(i) is not None:
                                alt_dependent['Upon'].append({'Title': '{}'.format(target.content.get(i).title),
                                                              'URL': '{}'.format(target.content.get(i).homepage),
                                                              'Type': '{}'.format(target.content.get(i).type)})
                            else:
                                alt_dependent['Upon'].append({'id': '{}'.format(i)})
                        except Exception as e:
                            print(e)
                            alt_dependent['Upon'].append({'id': '{}'.format(i)})
            else:
                result = item.get_data()
                if hasattr(result, 'map'):
                    i = result['map']['itemId']
                    try:
                        if target.content.get(i) is not None:
                            alt_dependent['Upon'].append({'Title': '{}'.format(target.content.get(i).title),
                                                          'URL': '{}'.format(target.content.get(i).homepage),
                                                          'Type': '{}'.format(target.content.get(i).type)})
                        else:
                            alt_dependent['Upon'].append({'id': '{}'.format(i)})
                    except Exception as e:
                        print(e)
                        alt_dependent['Upon'].append({'id': '{}'.format(i)})
            if item.dependent_to()['total'] > 0:
                d = [i.get('id') for i in item.dependent_to()['list']]
                alt_dependent['To'] = []
                for i in d:
                    if i is not None:
                        try:
                            if target.content.get(i) is not None:
                                alt_dependent['To'].append({'Title': '{}'.format(target.content.get(i).title),
                                                            'URL': '{}'.format(target.content.get(i).homepage),
                                                            'Type': '{}'.format(target.content.get(i).type)})
                            else:
                                alt_dependent['To'].append({'id': '{}'.format(i)})
                        except Exception as e:
                            alt_dependent['To'].append({'id': '{}'.format(i)})

            new_entry = App(portal_instance=instance, app_id=item.id, app_title=item.title, app_url=item.homepage,
                            app_owner=item.owner, app_created=ts(item.created), app_modified=ts(item.modified),
                            app_access=access, app_extent=item.extent, app_description=item.description,
                            app_views=item.numViews, app_dependent=alt_dependent)
            new_entry.save()
        instance_item.app_updated = datetime.datetime.now()
        instance_item.save()
        message_type = "success"
        result = "Updated Apps for {0}...".format(instance)
        return message_type, result
    except Exception as e:
        message_type = "error"
        result = "Unable to update Apps for {0}...{1}".format(instance, e)
        return message_type, result


def map_details(item):
    wm_item = Webmap.objects.get(webmap_id=item)
    l = []
    temp_layers = []
    l.append([-1, 0, wm_item.webmap_title, wm_item.webmap_url, wm_item.webmap_description, wm_item.webmap_owner,
              wm_item.webmap_access, wm_item.webmap_created, wm_item.webmap_modified, wm_item.webmap_views,
              wm_item.webmap_extent, wm_item.portal_instance])

    layers = wm_item.webmap_layers
    counter = 0
    for layer, values in layers.items():
        counter += 1
        url = values[0] if not values[0].split("/")[-1].isdigit() else "/".join(values[0].split("/")[:-1])
        try:
            service_title = '/'.join(url.split('/rest/services/')[1].split('/')[:-1])
        except:
            service_title = url

        if service_title not in temp_layers:
            temp_layers.append(service_title)
            if url is not None:
                qs = Service.objects.filter(service_url__has_key=url)
                if not qs:
                    l.append([0, counter, service_title, url, None, None, None, None, None, None, None, None, None])
                else:
                    l.append(
                        [0, counter, service_title, url, qs[0].service_type if qs else None, qs[0].service_mxd_server,
                         None, None, None, None, list(set(qs[0].service_layers.values())), qs[0].portal_instance])
                    l_counter = counter
                    for i in qs:
                        for k, v in i.service_layers.items():
                            l_counter += 1
                            l.append(
                                [counter, l_counter, k, i.service_name, None, None, None, None, None, None, v, None])
                    counter = l_counter
    del temp_layers
    result = make_tree(l, 0)
    c = get_children(result)
    gc = get_grandchildren(result)
    return result, wm_item.webmap_dependency, c, gc


def layer_details(dbserver, database, version, name):
    layers = []
    if dbserver != '' and version != '':
        db = "{}@{}@{}".format(dbserver, database, version)
    else:
        db = database

    services_set = Service.objects.filter(service_layers__has_key=name)
    layers.append([-1, 0, name, "FeatureClass", None, None, None, None, None, None, db, None])
    s_counter = 0
    for s in services_set:
        s_counter += 1
        layers.append([0, s_counter, s.service_name, s.service_url, None, s.service_mxd_server, None, None, None, None,
                       s.service_layers.get(name), s.portal_instance])
        for url in s.service_url.keys():
            wm_set = Webmap.objects.filter(webmap_services__contains=url)
            wm_counter = s_counter
            for wm in wm_set:
                wm_counter += 1
                if [s_counter, wm_counter, wm.webmap_title, wm.webmap_url, None, wm.webmap_owner, wm.webmap_access,
                    wm.webmap_created, wm.webmap_modified, wm.webmap_views, wm.webmap_id, wm.portal_instance] not in layers:
                    layers.append(
                        [s_counter, wm_counter, wm.webmap_title, wm.webmap_url, None, wm.webmap_owner, wm.webmap_access,
                         wm.webmap_created, wm.webmap_modified, wm.webmap_views, wm.webmap_id, wm.portal_instance])
                app_set = App.objects.filter(app_dependent__Upon__0__URL=wm.webmap_url)
                app_counter = wm_counter
                for app in app_set:
                    app_counter += 1
                    if [wm_counter, app_counter, app.app_title, app.app_url, None, app.app_owner, app.app_access,
                        app.app_created,
                        app.app_modified, app.app_views, None, app.portal_instance] not in layers:
                        layers.append(
                            [wm_counter, app_counter, app.app_title, app.app_url, None, app.app_owner, app.app_access,
                             app.app_created, app.app_modified, app.app_views, None, app.portal_instance])
                wm_counter = app_counter
            s_counter = wm_counter

    result = make_tree(layers, 0)
    c = get_children(result)
    gc = get_grandchildren(result)
    ggc = get_ggc(result)
    return result, c, gc, ggc


def ts(t):  # timestamp_to_date
    if t is not None:
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t / 1000))  # epoch utc to local
        return datetime.datetime.strptime(ts[:21], '%Y-%m-%d %H:%M:%S')
    else:
        return ''


def make_tree(pc_list, head):
    results = {}
    for record in pc_list:
        parent_id = record[0]
        node_id = record[1]
        if node_id in results:
            node = results[node_id]
        else:
            node = results[node_id] = {}

        node['name'] = record[2]
        node['url'] = record[3]
        try:
            node['type'] = record[4]
        except IndexError:
            pass
        try:
            node['owner'] = record[5]
        except IndexError:
            pass
        try:
            node['access'] = record[6]
        except IndexError:
            pass
        try:
            node['created'] = record[7]
        except IndexError:
            pass
        try:
            node['modified'] = record[8]
        except IndexError:
            pass
        try:
            node['views'] = record[9]
        except IndexError:
            pass
        try:
            node['database'] = record[10]
        except IndexError:
            pass
        try:
            node['instance'] = record[11]
        except IndexError:
            pass
        if parent_id != node_id:
            if parent_id in results:
                parent = results[parent_id]
            else:
                parent = results[parent_id] = {}
            if 'children' in parent:
                parent['children'].append(node)
            else:
                parent['children'] = [node]

    # assuming we wanted node id #0 as the top of the tree
    return results[head]


def list_to_dict(input):
    root = {}
    lookup = {}
    for parent_id, name, url, type in input:
        if parent_id == -1:
            root['name'] = name
            lookup[name] = root
        else:
            node = {'name': name}
            lookup[parent_id].setdefault('children', []).append(node)
            lookup[name] = node
    return root


def get_nodes(node, links):
    d = {}
    d['name'] = node
    children = get_children(node, links)
    if children:
        d['children'] = [get_nodes(child, links) for child in children]
    return d


def get_children(node, links):
    return [x[1] for x in links if x[0] == node]


def get_unused(instance, username=None, password=None):
    from arcgis.mapping import WebMap
    # logs into active portal in ArcGIS Pro
    target = connect(instance, username, password)

    # creates list of items of all map image, feature, vector tile and image services (up to 10000 of each) in active portal
    services = (target.content.search(query="NOT owner:esri*", item_type="Map Service", max_items=10000) +
                target.content.search(query="NOT owner:esri*", item_type="Feature Service", max_items=10000) +
                target.content.search(query="NOT owner:esri*", item_type="Vector Tile Service", max_items=10000) +
                target.content.search(query="NOT owner:esri*", item_type="Image Service", max_items=10000))

    # creates list of items of all webmaps in active portal
    web_maps = target.content.search(query="NOT owner:esri*", item_type="Web Map", max_items=10000)
    # loops through list of webmap items
    for item in web_maps:
        # creates a WebMap object from input webmap item
        web_map = WebMap(item)
        # accesses basemap layer(s) in WebMap object
        basemaps = web_map.basemap['baseMapLayers']
        # accesses layers in WebMap object
        layers = web_map.layers
        # loops through basemap layers
        for bm in basemaps:
            # tests whether the bm layer has a styleUrl(VTS) or url (everything else)
            if 'styleUrl' in bm.keys():
                for service in services:
                    if service.url in bm['styleUrl']:
                        services.remove(service)
            elif 'url' in bm.keys():
                for service in services:
                    if service.url in bm['url']:
                        services.remove(service)
        # loops through layers
        for layer in layers:
            # tests whether the layer has a styleUrl(VTS) or url (everything else)
            if hasattr(layer, 'styleUrl'):
                for service in services:
                    if service.url in layer.styleUrl:
                        services.remove(service)
            elif hasattr(layer, 'url'):
                for service in services:
                    if service.url in layer.url:
                        services.remove(service)

    print('The following services are not used in any webmaps in {}'.format(target))
    # as we have removed all services being used in active portal, print list of remaining unused services
    for service in services:
        print("{} | {}".format(service.title, target.url + r'home/item.html?id=' + service.id))
    print("There are a total of {} unused services in your portal".format(str(len(services))))


def get_connection(instance):
    import sys
    import getpass
    user = getpass.getuser()
    print("Username: {}".format(user))
    print("Enter credentials for {}".format(instance))
    username = input("Username: ")
    password = getpass.getpass("Password: ")
    # else:
    #     username = sys.stdin.readline().rstrip()
    #     password = sys.stdin.readline().rstrip()
    print("Username: {}, password: {}".format(username, password))
    return username, password


def get_usage_report(services):
    try:
        chartdata = {}
        query = {}
        for x in services:
            urls = x.get('url').keys()
            instance = x.get('instance')
            if instance not in query:
                query[instance] = []
            for url in urls:
                new_url = url.split('services/')[1].replace('/MapServer', '.MapServer').replace('/FeatureServer',
                                                                                                '.FeatureServer')
                query[instance].append(new_url)
        datasets = []
        for instance, urls in query.items():
            try:
                target = connect(instance)
                gis_servers = target.admin.servers.list()
                query_list = ",".join("services/{}".format(x) for x in urls)
                quick_report = gis_servers[0].usage.quick_report(since="LAST_MONTH",
                                                                 queries=query_list,
                                                                 metrics="RequestCount")
                chartdata['labels'] = [ts_2(x) for x in quick_report['report']['time-slices']]

                for service in quick_report['report']['report-data'][0]:
                    data = {'label': service['resourceURI'].replace('services', instance),
                            'data': [0 if d is None else d for d in service['data']],
                            'fill': False}
                    datasets.append(data)
            except Exception as e:
                message_type = "error"
                result = "Unable to connect to {} to generate Service Usage report...".format(instance)
        chartdata['datasets'] = datasets
        message_type = "success"
        result = ""
        return chartdata, message_type, result
    except Exception as e:
        message_type = "error"
        result = "Unable to generate usage report...{}".format(e)
        return message_type, result


def ts_2(t):
    if t is not None:
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t / 1000))
        return ts
    else:
        return None


def get_children(data):
    children = []
    local_data = copy.deepcopy(data)
    if local_data.get('children'):
        for child in local_data.get('children'):
            if child.get('children'):
                child.pop('children', None)
            if child not in children:
                children.append(child)
    del local_data
    return children


def get_grandchildren(data):
    grandchildren = []
    local_data = copy.deepcopy(data)
    if local_data.get('children'):
        for child in local_data.get('children'):
            if child.get('children'):
                for grand_child in child.get('children'):
                    if grand_child.get('children'):
                        grand_child.pop('children', None)
                    if grand_child not in grandchildren:
                        grandchildren.append(grand_child)
    return grandchildren


def get_ggc(data):
    ggc_list = []
    local_data = copy.deepcopy(data)
    if local_data.get('children'):
        for c in local_data.get('children'):
            if c.get('children'):
                for gc in c.get('children'):
                    if gc.get('children'):
                        for ggc in gc.get('children'):
                            if ggc.get('children'):
                                ggc.pop('children', None)
                            if ggc not in ggc_list:
                                ggc_list.append(ggc)
    del local_data
    return ggc_list
