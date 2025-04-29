#Author: Pelle Meuzelaar
#Date: 12-10-2022
#Assignment: Belsimpel warehouse case


from elasticsearch import Elasticsearch
import json
import urllib3
import pandas as pd
import numpy as np

""" STEP 1.1: DATA GATHERING AND PROCESSING """

#disable annoying warnings before the start of the code:
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#load in elasticsearch client
es = Elasticsearch("https://localhost:9200", ca_certs=False, verify_certs=False, http_auth=('elastic',
                                                                                            'j3nfTPHpJxz5iYTVMu8V'))

#step 1.1 perform a terms aggregation to display the buckets for the product orders per day (730 days total),
search_body = {
  "size": 0,
  "aggs": {
    "total_product_demand": {
      "terms": {
        "field": "product_id",
        "size": 1200
        },
        "aggs": {
          "transactions_per_day": {
            "histogram": {
              "field": "day",
              "interval": 1
          }
        }
      }
    }
  }
}
#note: 1200 is the total number of keys that could possibly be in a bucket because it is the total
#number of products available. However, ES/kibana will not allow that many results, hence you have to
#change it so that it with the set.max.buckets command in Kibana (I set mine to 840.000).

#print the number of orders per product of the above query
result = es.search(index="products", body=search_body)

#print number of orders per product as we discovered using the query
print("query results: ",  json.dumps(result, indent=1))

#make a list of products and orders
orders_per_day = []
id_count = []

#iterate over bucket and subbucket to append their data to the empty list of lists
for bucket in result["aggregations"]["total_product_demand"]["buckets"]: #iterate over the "product" buckets
  id_count = [bucket["key"]] #create a list to be included in the above list of lists
  for bucket in bucket["transactions_per_day"]["buckets"]: #iterate over the sub-buckets (orders per product per day)
    orders_per_day.append([id_count, bucket["key"], bucket["doc_count"]]) #append the data along with the other list

#put list into dataframe and print them sorted by product_id
df = pd.DataFrame(orders_per_day, columns=["product_id", "day", "orders"])
df.sort_values(by=["product_id"], inplace=True)
print(df)
