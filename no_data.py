import requests
import json
from helper import elastic_pwd, elastic_u
from es_connect_new import connect_elasticsearch


def sl_post(device_name):
    sl_body = {
        "force_ytype": "0",
        "force_yid": "0",
        "force_yname": "",
        "message": f"No Data Device: {device_name}",
        "value": "0",
        "threshold": "0",
        "message_time": "0",
        "aligned_resource": ""
    }

    r = requests.post('https://hostname.com/api/alert', auth=(elastic_u(), elastic_pwd()),
                      verify=False, data=json.dumps(sl_body))
    return r


es = connect_elasticsearch()

body = {
  "size": "30",
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "status": "true"
          }
        },
        {
          "range": {
            "timestamp": {
              "gte": "now-15m",
              "lt": "now"
            }
          }
        }
      ]
    }
  }
}

search = es.search(index='data_hourly_check', body=body)

for i in search['hits']['hits']:
    if len(search['hits']['hits']) > 0:
        device = i['_source']['device']
        sl_post(device)
        print(f'alert posted: {device}')

