#Author: Pelle Meuzelaar
#Date: 12-10-2022
#Assignment: Belsimpel warehouse case


from elasticsearch import Elasticsearch
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
#no sorting to product_id, otherwise wrong margin per product

""" STEP 1.3: PROFIT COMPUTATION """

#make new dataframe from margins.csv
df2 = pd.read_csv("margins.csv")

#add the margin column from dataframe 2 to dataframe 1:
margins = df2["margin"]
df = pd.concat([df, margins], axis=1, ignore_index=False)
# print(df)

#multiply the average number of orders per product with the margin per product
df["average_daily_profit"] = df["average_orders_per_day"] * df["margin"]

#sort new dataframe to display highest daily profit per product first
df = df.sort_values("average_daily_profit", ascending=False)
print(df)

#determine the array to put in the bin by determining the the i'th quantile (in this case 50, 70 and 80)

bin_ranges = [df.average_daily_profit.min(), df.average_daily_profit.quantile(0.5),
              df.average_daily_profit.quantile(0.8), df.average_daily_profit.max()]

#show average profit per product per day in a histogram, input above range for the bin parameter
plt.hist(df["average_daily_profit"], bins=bin_ranges, rwidth=0.95, color="red")
plt.title("Average profit per product")
plt.ylabel("Number of products")
plt.xlabel("Average profit per day")
plt.show()