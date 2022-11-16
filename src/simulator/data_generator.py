import pandas as pd
import random
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import asyncio
import argparse
import time
import json
import numpy as np
import os
#Get the connection to your event hub topic and set it to an environment variable
# You also need to install pip install azure-eventhub
#Run this program as python data_generator.py --duration 1000  
con_str=os.getenv("EHCONN")
eh_name= "sales_orders"
def parse_args():
    # arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", default=100, type=int, help="duration to run the simulator")
    parser.add_argument("--eh_name", type=str, default="sales_orders")
    # parse args
    args = parser.parse_args()

    # return args
    return args
async def send_orders(partition, data, eh_name):
    producer = EventHubProducerClient.from_connection_string(conn_str=con_str, eventhub_name=eh_name)
    event_batch = await producer.create_batch(partition_key=partition)
    event_batch.add(EventData(json.dumps(data)))
    async with producer:
        await producer.send_batch(event_batch)
async def main(eh_name, sales_df):

    tenants = ["tenant11","tenant2", "tenant3", "tenant4", "tenant5", "tenant6", "tenant7", "tenant8", "tenant9","tenant10"]
    #boost probability a tenant is present more than others
    high_volume_tenant = "tenant11"
    high_volume_tenant_weight = 0.8
    weights = [(1-high_volume_tenant_weight)/len(tenants)]*len(tenants)
    high_volume_tenant_weight = 1- sum(weights[:-1])
    weights[tenants.index(high_volume_tenant)] = high_volume_tenant_weight
    tenant_partition = {"tenant11":1,"tenant2":1, "tenant3":2, "tenant4":2, "tenant5":3, "tenant6":3, "tenant7":4, "tenant8":4,"tenant9":1,"tenant10":3}
    record_num = 0
    while record_num < sales_df.shape[0]:
        tenant = np.random.choice(tenants, p=weights)
        partition = tenant_partition[tenant]
        num_order = random.randint(1,30)
        records = sales_df[record_num:record_num+num_order]
        data_to_send = {"tenant":tenant, "data":records.to_dict(orient="records")}
        
        await send_orders(partition,data_to_send,eh_name)
        # time.sleep(4)
        print("sent ", records.shape[0], "orders from tenant ", tenant)
        record_num += num_order

if __name__ == "__main__":
    args = parse_args()
    eh_name = args.eh_name
    duration = args.duration
    execution_time=0
    batch =0
    sales_df = pd.read_csv("../../data/sales_data_sample.csv",encoding='Latin-1')
    #change column names for mapping later
    mapping ={"ORDERNUMBER":"ORDER_NUMBER","QUANTITYORDERED":"QTY_ORDERED","PRICEEACH":"PRICE_EACH","ORDERLINENUMBER":"ORDER_LINE"}
    sales_df.rename(columns = mapping, inplace=True)
    sales_df["QTY_ORDERED"][0:int(sales_df.shape[0]/10)] = -5
    sales_df =sales_df.sample(frac=1)
    while execution_time <duration:
        batch += 1
        start_time = time.time()
        asyncio.run(main(eh_name,sales_df))
        batch_time = time.time() - start_time
        execution_time += batch_time
        print("Batch {0} done in {1} seconds.".format(batch, batch_time))
