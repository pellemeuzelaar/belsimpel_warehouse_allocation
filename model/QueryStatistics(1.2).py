#Author: Pelle Meuzelaar
#Date: 12-10-2022
#Assignment: Belsimpel warehouse case


from elasticsearch import Elasticsearch
import json
import urllib3
import pandas as pd
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

""" STEP 1.2: DATA DESCRIPTION """

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
        "size": 1263
      },
      "aggs": {
        "transactions_per_day": {
          "histogram": {
            "field": "day",
            "interval": 1,
          }
        },
        "data_description_per_product": {
          "extended_stats_bucket": {
            "buckets_path": "transactions_per_day._count",
            "gap_policy": "insert_zeros"
          }
        }
      }
    }
  }
}
#I added stats bucket in order to count the number of orders per product, the average number of orders
#per day per product and other statistics

#print the number of orders per product of the above query
result = es.search(index="products", body=search_body)

#print number of orders per product as we discovered using the query
print("query results: ",  json.dumps(result, indent=1))

#make a list of product_id and average per product
product_id = []
statistics_per_product = []

#iterate over bucket and subbucket to append their data to the empty list of lists
for bucket in result["aggregations"]["total_product_demand"]["buckets"]: #iterate over the "product" buckets
  product_id = bucket["key"]
  statistics_per_product.append([product_id, bucket["data_description_per_product"]["avg"],
                                 bucket["data_description_per_product"]["std_deviation"]])

#put list into dataframe and print them sorted by highest average order per day
df = pd.DataFrame(statistics_per_product, columns=["product_id", "average_orders_per_day", "standard_deviation"])
df = df.sort_values("average_orders_per_day", ascending=False).reset_index(drop=False)
print(df)

#give the parameters for the errorbar plot
x = df.index
y = df["average_orders_per_day"]
yerror = df["standard_deviation"]

#plot and save the data onto an errorbar
plt.errorbar(x=x, y=y, yerr=yerror, color="red", fmt="o", ecolor="black", elinewidth=0.3, capsize=1, errorevery=5,
             capthick=0.1)
plt.title("Average orders per day with SD error bars")
plt.xlabel("Index number")
plt.ylabel("AAverage orders per day")
plt.savefig("/Users/pellemeuzelaar/PycharmProjects/Practical4/errorbar.png")

