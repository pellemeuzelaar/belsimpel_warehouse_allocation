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

#step 1.1 perform a terms aggregation to display the buckets for the number of orders for each product (1200 total),
#there are 1262 rows in the sales file
search_body = {
  "size": 0,
    "aggs": {
      "products": {
        "terms": {
          "field": "product_id",
            "size": 1263
      }
    }
  }
}

#these are ranked to most orders per product

#print the number of orders per product of the above query
result = es.search(index="products", body=search_body)

#print number of orders per product as we discovered using the query
print("query results: ",  json.dumps(result, indent=1))

#transfer elasticsearch aggregation into pandas dataframe
df = pd.json_normalize(result['aggregations']['products']['buckets'])

#rename the default pandas column name
df.rename(columns={'key': 'product_id'}, inplace=True)
df.rename(columns={'doc_count': 'total_orders_per_product'}, inplace=True)

#print the dataframe for visualization
print("orders per product into pandas dataframe:\n", df)
