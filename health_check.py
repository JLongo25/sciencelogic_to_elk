#!/bin/python3
"""
    Program to generate smile brands monthly excel report
    Data collection is based on AIX power pack.  Metrics for non AIX are not capture with this script
    All data collection are filtered for Smile Brands except for Availability
    5/28/2020 added feature to send data to elastic
"""

import time
from datetime import date, timedelta
from elasticsearch import helpers
import es_connect
import silo_helper
from statistics import mean


def get_epoch(yyyy, m, d):
    return date(yyyy, m, d).strftime('%s')


# ScienceLogic ID - can change based on SL instance
# Collecting only AIX metrics
cpu = 6676
memory_used = 6613
swap_used = 6620
interface_total_bandwidth = 6630
filesystem_used = 6681

# url parameters
ORG = 7  # ID for Smile Brands
BASE_URL = 'https://sciencelogicurl'
ORG_FILTER = 'filter.0.organization.eq=6'
# beginstamp  -86400 (1 day) to include start day
# endstamp is inclusive
# uncomment beginstamp and endstamp to force manual data range
# beginstamp = str(int(get_epoch(2020, 3, 1)) - 86400)
# endstamp = get_epoch(2020, 3, 31)
# timestamp = date.today().strftime('%s')

# get last months date range
first_day_this_month = date.today().replace(day=1)
last_day_last_month = (first_day_this_month - timedelta(days=1))
first_day_last_month = last_day_last_month.replace(day=1)
endstamp = last_day_last_month.strftime('%s')
beginstamp = str(int(first_day_last_month.strftime('%s')) - 86400)


cpu = {}
memory = {}
swap = {}
interface = {}
availability = {}
version = {}


def get_inventory():
    api = '/api/device?limit=1000&hide_filterinfo=1&link_disp_field=name&filter.0.organization.eq=6'
    return silo_helper.get_silo_data(api)


def get_inventory_type():
    api = '/api/device?limit=1000&link_disp_field=class_type%2Flogical_name&filter.0.organization.eq=6&hide_filterinfo=1'
    return silo_helper.get_silo_data(api)


def get_inventory_class():
    api = '/api/device?hide_filterinfo=1&limit=1000&link_disp_field=class_type%2Flogical_name&filter.0.organization.eq=6'
    return silo_helper.get_silo_data(api)


def get_inventory_ip():
    api = '/api/device?limit=1000&link_disp_field=ip&filter.0.organization.eq=6&hide_filterinfo=1'
    return silo_helper.get_silo_data(api)


def get_availability():
    api = f'/api/data_performance/device/avail?hide_filterinfo=1&beginstamp={beginstamp}&endstamp={endstamp}&rollup_freq' \
          f'=daily&data_fields=avg_d_check'
    return silo_helper.get_silo_data(api)


def get_performance_bulk(pres_id, fields='avg'):
    api = f'/api/data_performance/device/dynamic_app?hide_filterinfo=1&beginstamp={beginstamp}&endstamp={endstamp}&presentation_objects={pres_id}&rollup_freq=daily' \
          f'&data_fields={fields}&filter.0.device%2Forganization.eq=6'
    print(api)
    return silo_helper.get_silo_data(api)


def dynmaic_app_bulk_average(data_points, metric):
    for d in data_points:
        w = [float(i[1]) for i in d['data']]
        metric[d['device'].split('/')[3]] = round(mean(w), 2)


def get_config_version(device):
    api = f'/api/device/{device}/config_data/D6D7EFD4C8FC1E7A4B702F32E25F971E/data'
    return silo_helper.get_silo_data(api)


# populate dictionaries
result = get_inventory()
devices = {item['URI'].split('/')[3]: item['description'] for item in result}
time.sleep(1)

result = get_inventory_type()
devices_type = {item['URI'].split('/')[3]: item['description'] for item in result}
time.sleep(1)

result = get_inventory_class()
devices_class = {item['URI'].split('/')[3]: item['description'] for item in result}
time.sleep(1)

result = get_inventory_ip()
devices_ip = {item['URI'].split('/')[3]: item['description'] for item in result}
time.sleep(1)

cpu_data = get_performance_bulk(cpu)
dynmaic_app_bulk_average(cpu_data, cpu)
time.sleep(1)

memory_data = get_performance_bulk(memory_used)
dynmaic_app_bulk_average(memory_data, memory)
time.sleep(1)

swap_data = get_performance_bulk(swap_used)
dynmaic_app_bulk_average(swap_data, swap)
time.sleep(1)

interface_data = get_performance_bulk(interface_total_bandwidth)
dynmaic_app_bulk_average(interface_data, interface)
time.sleep(1)

availability_data = get_availability()
dynmaic_app_bulk_average(availability_data, availability)
time.sleep(1)

file_system_data = get_performance_bulk(filesystem_used)
# initialize list
fs = {keys: [] for keys in devices.keys()}


# printing FileSystem Usage
print(f'DID, Device, FS, Usage(Max)')
for data in file_system_data:
    w = [float(i[1]) for i in data['data']]
    # print(data['device'].split('/')[3], devices[data['device'].split('/')[3]], data['index_label'], max(w))
    print(
        f"{data['device'].split('/')[3]}, {devices[data['device'].split('/')[3]]}, {data['index_label']}, {round(max(w), 2)}")
    if int(max(w)) > 80:
        fs[data['device'].split('/')[3]].append(data['index_label'] + " " + str(round(max(w), 2)))

# get device AIX version
for key in devices.keys():
    version_data = get_config_version(key)
    if version_data:
        version[key] = version_data['3']['data']['17662']['0']

# print table to use for csv reporting
print(
    f'DID, Device, Class, IP, Availability, CPU, Memory, Swap, Version, Interface, FS(over 80%), Multipath, Error_Report, Notes')
for key in devices.keys():
    try:
        availability_computed = availability.get(key) * 100
    except TypeError as e:
        # define integer value for NoneType
        availability_computed = -1
    # print(key, devices[key], devices_type[key], p_availability)
    print(
        f'{endstamp}, {key}, {devices[key]}, {devices_type.get(key)}, {devices_ip.get(key)}, {availability_computed}, {cpu.get(key)}, {memory.get(key)}, {swap.get(key)}, {version.get(key)}, {interface.get(key)}, {fs.get(key)}')

# Done getting and printing data, sending to corresponding elastic index
#es = es_connect.connect_elasticsearch()

# index = ms_inventory
# timestamp, device, device_category, device_class, ip, ver_num, company
# create array for inventory

data = []
for device in devices:
    print(endstamp, devices[device], devices_ip.get(device), version.get(device), devices_type.get(device),
          'Smile Brands')
    x = {'timestamp': endstamp, 'device': devices[device], 'device_category': devices_class.get(device),
         'device_class': devices_type.get(device), 'ip': devices_ip.get(device), 'company': 'company',
         'ver_num': version.get(device)}
    data.append(x)

#resp = helpers.bulk(es, data, index='ms_inventory', doc_type='_doc')


# index = ms_disk_utilization
#       "disk_utilization"
#       "file_system"
#       "timestamp"
#       "device_category"
#       "company"

# initialize list
temp_fs = []
print(f'DID, Device, FS, Usage(Max)')
for data in file_system_data:
    w = [float(i[1]) for i in data['data']]
    # print(data['device'].split('/')[3], devices[data['device'].split('/')[3]], data['index_label'], max(w))
    print(
        f"{data['device'].split('/')[3]}, {devices[data['device'].split('/')[3]]}, {data['index_label']}, {round(max(w), 2)}")
    dev = data['device'].split('/')[3]
    f = {'timestamp': endstamp, 'device': devices[dev], 'device_category': devices_class.get(dev),
         'company': 'company', 'file_system': data['index_label'], 'disk_utilization': round(max(w), 2)}
    temp_fs.append(f)

#resp = helpers.bulk(es, temp_fs, index='ms_disk_utilization', doc_type='_doc')


# index = ms_availability
#   avg_availability
#   company
#   device
#   timestamp
#   device_category

data = []
for device in devices:
    x = {'timestamp': endstamp, 'device': devices[device], 'device_category': devices_class.get(device),
         'company': 'company', 'avg_availability': availability.get(device)}
    data.append(x)
    print(x)

#resp = helpers.bulk(es, data, index='ms_availability', doc_type='_doc')


# index = ms_cpu_utilization
#   avg_cpu
#   company
#   device
#   timestamp
#   device_category

data = []
for device in devices:
    x = {'timestamp': endstamp, 'device': devices[device], 'device_category': devices_class.get(device),
         'company': 'company', 'avg_cpu': cpu.get(device)}
    data.append(x)
    print(x)

#resp = helpers.bulk(es, data, index='ms_cpu_utilization', doc_type='_doc')


# index = ms_memory_utilization
#   avg_mem
#   company
#   device
#   timestamp
#   device_category

data = []
for device in devices:
    x = {'timestamp': endstamp, 'device': devices[device], 'device_category': devices_class.get(device),
         'company': 'company', 'avg_mem': memory.get(device)}
    data.append(x)
    print(x)

#resp = helpers.bulk(es, data, index='ms_memory_utilization', doc_type='_doc')

# index = ms_swap
#   avg_swap
#   company
#   device
#   timestamp
#   device_category

data = []
for device in devices:
    x = {'timestamp': endstamp, 'device': devices[device], 'device_category': devices_class.get(device),
         'company': 'company', 'avg_swap': swap.get(device)}
    data.append(x)
    print(x)

#resp = helpers.bulk(es, data, index='ms_swap', doc_type='_doc')


# index = ms_interface_utilization
# TBD - need to verify percent utilization instead of raw bytes usage
#   avg_swap
#   company
#   device
#   timestamp
#   device_category
#   interface_name
#   speed
#   avg_in_percent
#   avg_out_percent
