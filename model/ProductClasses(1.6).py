#Author: Pelle Meuzelaar
#Date: 12-10-2022
#Assignment: Belsimpel warehouse case


from elasticsearch import Elasticsearch
import urllib3
import pandas as pd
import matplotlib
matplotlib.use('TkAgg')

""" STEP 1.2: DATA DESCRIPTION """

def create_df_product_orders_per_day():
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
  return df

create_df_product_orders_per_day()

""" STEP 1.3: PROFIT COMPUTATION """

def profit_computation():
  df = create_df_product_orders_per_day() #call df from function

  #make new dataframe from margins.csv
  df2 = pd.read_csv("margins.csv")

  #add the margin column from dataframe 2 to a new dataframe:
  margins = df2["margin"]
  df3 = pd.concat([df, margins], axis=1, ignore_index=False)

  #multiply the average number of orders per product with the margin per product
  df3["average_daily_profit"] = df["average_orders_per_day"] * df3["margin"]

  #sort new dataframe to display highest daily profit per product first
  df3 = df3.sort_values("average_daily_profit", ascending=False)
  print(df3)
  return(df3)

profit_computation()

def bin_finder():
  df3 = profit_computation() #call df from function

  # determine the array to put in the bin by determining the the i'th quantile (in this case 50, 70 and 80)
  bin_ranges = [df3.average_daily_profit.min(), df3.average_daily_profit.quantile(0.5),
              df3.average_daily_profit.quantile(0.8), df3.average_daily_profit.max()]
  border_1 = df3.average_daily_profit.min()
  border_2 = df3.average_daily_profit.quantile(0.5)
  border_3 = df3.average_daily_profit.quantile(0.8)
  border_4 = df3.average_daily_profit.max()
  return bin_ranges, border_1, border_2, border_3, border_4

bin_finder()

def products_in_each_class(): #we found the quartiles using the bin_finder() function, so we bin the df accordingly
  df3 = profit_computation() #call df from function

  for bin_ranges in bin_finder():
    df3["binned"] = pd.cut(df3["average_daily_profit"], bin_ranges)
    print(df3)
    return df3["binned"]

products_in_each_class()

