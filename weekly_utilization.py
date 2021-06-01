#!/bin/python3
import requests
import json
from datetime import datetime
from es_connect import connect_elasticsearch
from elasticsearch import helpers


def api_call(api):
    url_usage = f'https://sciencelogicurl.com{device_id}{api}'
    passparams2 = {'hide_options': '1'}
    r_usage = requests.get(url_usage, auth=('user', 'pass'),
                           verify=False, params=passparams2)
    usage = json.loads(r_usage.content.decode('utf-8'))
    return usage


def empty_index(index):
    body = {
        "query": {
            "match_all": {}
        }
    }
    es.delete_by_query(index=index, body=body)


today = datetime.today()
es = connect_elasticsearch()
passparams = {'hide_filterinfo': '1', 'duration': '1d', 'filter.0.class_type/guid': 'B6A9993CE1CD9232167316CC79F82894',
              'filter.0.organization.eq': '6'}
r = requests.get('https://sciencelogicurl.com/api/device', auth=('user', 'pass'), verify=False, params=passparams)
status = r.status_code
devices = json.loads(r.content.decode('utf-8'))
elk = []

for device in devices:
    disk_total = 0
    disk_total_used = 0
    max_disk = 0
    max_mem = 0
    memory_capacity = 0
    device_id = device['URI']
    device_name = device['description']
    print(device_name)
    disk = api_call('/performance_data/8103/normalized_daily?duration=7d')
    mem_cap = api_call('/config_data/6B2C342727896B9BED64AC156E55B7E0/data')
    disk_committed = api_call('/performance_data/738/normalized_daily?duration=7d')
    mem = api_call('/performance_data/747/normalized_daily?duration=7d')
    try:
        max_disk = max(disk_committed['data']['0']['max'].values())
        max_disk = float(max_disk) / 1024 / 1024 / 1024
        max_mem = max(mem['data']['0']['max'].values())
        max_mem = float(max_mem) / 1024 / 1024
        disk_total = max(disk['data']['0']['max'].values())
        disk_total = float(disk_total) / 1024 / 1024 / 1024
        memory_capacity = int(mem_cap['0']['data']['1158']['0']) / 1024
    except KeyError:
        pass
    x = {'timestamp': today.strftime('%s'), 'device': device_name, 'disk_capacity': float('{:.2f}'.format(disk_total)),
         'disk_used': float('{:.2f}'.format(max_disk)), 'memory_capacity': round(memory_capacity), 'max_mem': float('{:.2f}'.format(max_mem))}
    elk.append(x)

empty_index('weekly')
resp = helpers.bulk(es, elk, index='weekly', doc_type='_doc')
resp1 = helpers.bulk(es, elk, index='weekly_archive', doc_type='_doc')
