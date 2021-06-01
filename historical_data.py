import pandas as pd
import numpy as np
from elasticsearch import helpers
from helper import get_silo_data, connect_elasticsearch


nodes = [7686, 7687, 7688]
perfs = [5420, 5415, 5413]
web_fields = ['avg_d_state', 'avg_d_trans_time']
df = pd.DataFrame()
writer = pd.ExcelWriter('output.xlsx')

devices = get_silo_data('/api/device?limit=1000&hide_filterinfo=1&link_disp_field=name&filter.0.organization.eq=8&filter.0.class_type%2Fguid.eq=F004E70609645E4AA020F7FAC337FFCF')
elk = []
for device in devices:
    avail = get_silo_data(f'{device["URI"]}/vitals/availability/normalized_hourly?duration=90d')
    temp = [avail['data']['d_check']['avg']]
    device_id = device['URI'].split('/', 3)[3]
    for field in web_fields:
        web_content_dict = {}
        web_content = get_silo_data(f'/api/data_performance/device/monitor_cv?duration=90d&rollup_freq=hourly&data_fields={field}&filter.0.device.eq={device_id}')
        for i in web_content['result_set'][0]['data']:
            key, value = i[0], i[1]
            web_content_dict[key] = value
        temp.append(web_content_dict)
    for perf in perfs:
        wu_nodes = get_silo_data(f'{device["URI"]}/performance_data/{perf}/normalized_hourly?duration=90d')
        temp.append(wu_nodes['data']['0']['avg'])
        print(device['description'])
    df = pd.DataFrame.from_dict(temp)
    df = df.transpose()
    df = df.astype('float')
    df = df.rename(columns={0: 'device_avail', 1: 'url_avail', 2: 'trans_time', 3: 'storage', 4: 'memory', 5: 'cpu', 6: 'timestamp', 7: 'device'})
    df['device'] = device['description']
    df['timestamp'] = df.index
    df = df.replace(np.nan, 0)
    for y in df.iterrows():
        x = {'device_avail': y[1][0], 'url_avail': y[1][1], 'trans_time': y[1][2], 'storage': y[1][3], 'memory': y[1][4],
             'cpu': y[1][5], 'timestamp': y[1][7], 'device': y[1][6]}
        elk.append(x)

es = connect_elasticsearch()
resp = helpers.bulk(es, elk, index='node_archive', doc_type='_doc')
