#Author: Pelle Meuzelaar
#Date: 12-10-2022
#Assignment: Belsimpel warehouse case


from elasticsearch import Elasticsearch
import json
import urllib3
import pandas as pd


""" STEP 1: DATA GATHERING AND PROCESSING """

#disable annoying warnings before the start of the code:
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#load in elasticsearch client
es = Elasticsearch("https://localhost:9200", ca_certs=False, verify_certs=False, http_auth=('elastic',
                                                                                            'j3nfTPHpJxz5iYTVMu8V'))

#step 1.1 perform a terms aggregation to display the buckets for the product orders per day (730 days total),
search_body = {
  "size": 0,
  "aggs": {
    "transactions_per_day": {
      "histogram": {
        "field": "day",
        "interval": 1,
        },
        "aggs": {
          "total_product_demand": {
            "terms": {
              "field": "product_id",
              "size": 1200
            }
          }
        }
      }
    }
  }


#print the number of orders per product of the above query
result = es.search(index="products", body=search_body)

#transfer elasticsearch aggregation into pandas dataframe
orders = []

for bucket in result["aggregations"]["transactions_per_day"]["buckets"]:  # iterate over the "product" buckets
  for bucket in bucket["total_product_demand"]["buckets"]: #iterate over the sub-buckets (orders per product per day)
    orders.append([bucket["key"], bucket["doc_count"]]) #append the data along with the other list


df = pd.DataFrame(orders, columns=["days", "products", "orders"])
print(df)

df2 = df.transpose()
print(df2)


