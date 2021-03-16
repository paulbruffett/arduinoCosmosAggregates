import logging
import os
import azure.cosmos.cosmos_client as cosmos_client
from datetime import datetime, timedelta
import azure.functions as func
import pandas as pd
import json



url = os.environ.get('cosmosurl')
key = os.environ.get('cosmoskey')

client = cosmos_client.CosmosClient(url, {'masterKey': key})
database = client.get_database_client('arduino')
container = database.get_container_client('temps')
dashboard_container = database.get_container_client('dashboard')




def main(mytimer: func.TimerRequest) -> None:

    records = list(container.query_items(
        query="SELECT * FROM c WHERE NOT is_defined(c.processed)",
        enable_cross_partition_query=True
    ))
    request_charge = container.client_connection.last_response_headers['x-ms-request-charge']

    logging.info('Query returned {0} items. Operation consumed {1} request units'.format(len(items), request_charge))


    #query aggregates
    counts = list(dashboard_container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    ))

    request_charge = dashboard_container.client_connection.last_response_headers['x-ms-request-charge']

    logging.info('Query returned {0} items. Operation consumed {1} request units'.format(len(counts), request_charge))


    #update records and update aggregates
    counts = {}
    for i in records:
        payload = i
        dt_object = datetime.fromtimestamp(i['timestamp'])
        dt_d = dt_object.strftime("%Y-%m-%d:%H")
        if dt_d in counts.keys():
            counts[dt_d]={'temp_sum':counts[dt_d]['temp_sum']+i['temp'],'count':counts[dt_d]['count']+1,
                        'illum_sum':counts[dt_d]['illum_sum']+i['illuminance']}
        else:
            counts[dt_d]={'temp_sum':i['temp'],'count':1,
                        'illum_sum':i['illuminance']}
            container.upsert_item(
                        {
                            'id': 'ard1010wifi'+str(payload['timestamp']),
                            'temp': payload['temp'],
                            'timestamp': payload['timestamp'],
                            'humidity': payload['humidity'],
                            'pressure': payload['pressure'],
                            'illuminance' : payload['illuminance'],
                            'date':dt_object.strftime("%Y-%m-%d"),
                            'processed':True
                        })

    #log aggregates
    for i in counts:
        logging.info(i,counts[i])
        dashboard_container.upsert_item(
        {
            'id':i,
            'date':i,
            'temp_sum':counts[i]['temp_sum'],
            'count':counts[i]['count'],
            'illum_sum':counts[i]['illum_sum']
        })

