# Author: Pelle Meuzelaar
# Date: 12-10-2022
# Assignment: Belsimpel warehouse case


from elasticsearch import Elasticsearch, helpers
import urllib3
import warnings
import csv
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import json
import numpy as np
from gurobipy import Model, GRB, quicksum
import seaborn as sns
matplotlib.use('TkAgg') #errorsolving

""" STEP 1.1: CREATE INDEX AND GATHER TOTAL DEMAND """

def elasticsearch_setup():
  # Disable annoying warnings before the start of the code:
  urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
  warnings.filterwarnings("ignore", category=DeprecationWarning)
  warnings.filterwarnings("ignore", category=UserWarning)

  # Load in elasticsearch client
  es = Elasticsearch("https://localhost:9200", ca_certs=False, verify_certs=False,
                     http_auth=('elastic', 'j3nfTPHpJxz5iYTVMu8V'))
  return es

# elasticsearch_setup()

def create_product_index():
  es = elasticsearch_setup() # Call from es function
  # Define the mapping of the index to be created
  settings = {
    "mappings": {
      "properties": {
        "day": {
          "type": "long"
        },
        "product_id": {
          "type": "long"
        },
        "product_orders_per_day": {
          "type": "long"
        }
      }
    }
  }

  # Create an empty index
  es.indices.create(index="products", body=settings)

  # Load the csv file into the elastic index using bulk command
  with open('sales.csv') as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='products')

  # Confirm that the index with product orders per day is created
  print("index 'products' is created")

  # To delete the index, uncomment the following line
  # es.indices.delete(index='products', ignore=[400, 404])

# create_product_index() #disabled, because you only need to run create index once

def df_total_demand_per_product():
  es = elasticsearch_setup() # Call from es function

  #Perform a terms aggregation to display the buckets for the product orders per day (730),
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
  # Note: 1200 is the total number of keys that could possibly be in a bucket because it is the
  # total product number available. However, ES/kibana will not allow that many results you have
  # to it with the set.max.buckets command

  # Print the number of orders per product of the above query
  result = es.search(index="products", body=search_body)

  # Print number of orders per product as we discovered using the query
  print("query results: ",  json.dumps(result, indent=1))

  # Make a list of products per day
  orders_per_day = []

  # Iterate over bucket and subbucket to append their data to the empty list of lists
  for bucket in result["aggregations"]["total_product_demand"]["buckets"]: #iterate the "products"
    id_count = bucket["key"] #create a list to be included in the above list of lists
    for bucket in bucket["transactions_per_day"]["buckets"]: #iterate over the sub-buckets
      orders_per_day.append([id_count, bucket["key"], bucket["doc_count"]]) #append the data

  # Put list into dataframe and print them sorted by product_id
  df = pd.DataFrame(orders_per_day, columns=["product_id", "day", "orders"])
  df.sort_values(by=["product_id"], inplace=True)
  return(df)

df_total_demand_per_product()

""" STEP 1.2: AVERAGE AND SD OF DEMAND PER DAY """

def df_product_orders_per_day():
  es = elasticsearch_setup() # Call from es function

  # Perform a terms aggregation to display the buckets for the product orders per day (730)/stats
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
  # I added stats bucket in order to count the average number of orders per day per product & other stats

  # Print the number of orders per product of the above query
  result = es.search(index="products", body=search_body)

  # Make a list of average per product
  statistics_per_product = []

  # Iterate over bucket and subbucket to append their data to the empty list of lists
  for bucket in result["aggregations"]["total_product_demand"]["buckets"]: #iterate the products
    product_id = bucket["key"]
    statistics_per_product.append([product_id,
                                   bucket["data_description_per_product"]["avg"],
                                   bucket["data_description_per_product"]["std_deviation"]])

  print("query results: ",  json.dumps(result, indent=1))

  # Put list into dataframe and print them sorted by highest average order per day
  df = pd.DataFrame(statistics_per_product, columns=["product_id", "avg_orders_per_day", "standard_deviation"])
  df = df.sort_values("avg_orders_per_day", ascending=False).reset_index(drop=False)
  print(df)
  return df

df_product_orders_per_day()

def plot_errorbar_avg_demand():
  df = df_product_orders_per_day() # Call df from above function

  # Give the parameters for the errorbar plot
  x = df.index
  y = df["avg_orders_per_day"]
  yerror = df["standard_deviation"]

  # Plot and save the data onto an errorbar
  plt.errorbar(x=x, y=y, yerr=yerror, color="red", fmt="o", ecolor="black", elinewidth=0.3,
               capsize=1, errorevery=5, capthick=0.1)
  plt.title("Average orders per day with SD error bars")
  plt.xlabel("Index number")
  plt.ylabel("Average orders per day")
  plt.show()

# plot_errorbar_avg_demand()

""" STEP 1.3: PROFIT COMPUTATION """

def profit_computation():
  df = df_product_orders_per_day() # Call df from above function

  # Make new dataframe from margins.csv
  df2 = pd.read_csv("margins.csv")

  # Add the margin column from dataframe 2 to a new dataframe:
  margins = df2["margin"]
  df = pd.concat([df, margins], axis=1, ignore_index=False)

  # Multiply the average number of orders per product with the margin per product
  df["avg_daily_profit"] = df["avg_orders_per_day"] * df["margin"]

  # Sort new dataframe to display highest daily profit per product first
  df = df.sort_values("avg_daily_profit", ascending=False)
  return(df)

# profit_computation()

def bin_finder():
  df = profit_computation() # Call df from function

  # Determine the array to put in the bin by determining the the i'th quantile (namely 50, 70, 80)
  bin_ranges = [df.avg_daily_profit.min(), df.avg_daily_profit.quantile(0.5),
              df.avg_daily_profit.quantile(0.8), df.avg_daily_profit.max()]
  return bin_ranges

# bin_finder()

def plot_histogram_avg_profit():
  df = profit_computation()  # Call from functions
  bin_ranges = bin_finder()

  # Show average profit per product per day in a histogram, input above range for the bin parameter
  plt.hist(df["avg_daily_profit"], bins=bin_ranges, rwidth=0.95, color="red")
  plt.title("Average profit per product")
  plt.ylabel("Number of products")
  plt.xlabel("Average profit per day")
  plt.show()

# plot_histogram_avg_profit()

""" STEP 1.4: VOLUME COMPUTATION """

def volume_computation():
  # Make new dataframe from margins.csv
  df2 = pd.read_csv("dimensions.csv")

  # Multiply the length, width and height to get the volume per product in a column
  df2["volume"] = df2["length"] * df2["width"] * df2["height"]

  # Sort new dataframe to display highest daily profit margin products first
  df2 = df2.sort_values("product_id", ascending=True)

  return df2

# volume_computation()

def plot_volume_histogram():
  df2 = volume_computation() # Call from function

  # Show volume per product per day in a histogram
  plt.hist(df2["volume"], rwidth=0.95, color="red")
  plt.title("Product volume ranges")
  plt.ylabel("Number of products")
  plt.xlabel("Volume")
  plt.show()

# plot_volume_histogram()

""" STEP 1.6: PRODUCT CLASSES """

def products_in_each_class():
  df = profit_computation() # Call from functions
  bin_ranges = bin_finder()

  # We found the quartiles using the bin_finder() function, so we bin the df accordingly
  df["binned"] = pd.cut(df["avg_daily_profit"], bins=bin_ranges, labels=False) # classes are 0, 1, 2
  df.at[1262, "binned"] = 0 # There is a NaN value that throws an error

  return df

# products_in_each_class()

def print_list_products_in_each_class():
  df = products_in_each_class()

  # Make variables
  product_class_low = (df.loc[df["binned"] == 0, "product_id"])
  product_class_medium = (df.loc[df["binned"] == 1, "product_id"])
  product_class_high = (df.loc[df["binned"] == 2, "product_id"])

  # Get the list of products
  print("the lists of product classes: (left = index and right = product_id):",
  "\nProduct class 0-50%:\n", product_class_low,
  "\nProduct class 50-80%:\n", product_class_medium,
  "\nProduct class 80-100%:\n", product_class_high)

# print_list_products_in_each_class()

""" STEP 1.7: PRODUCT CLASS BAR CHARTS """

def plot_product_low_class_chart():
  df = products_in_each_class() # Call from functions

  # Input the parameters
  x = df.loc[df["binned"] == 0, "product_id"]
  height = df.loc[df["binned"] == 0, "avg_daily_profit"]

  # Plot the bar chart for the lower class
  plt.bar(x, width=3.5, height=height, bottom=0, align="center", color="red")
  plt.title("Average profits of 0%-50% class")
  plt.xlabel("Product number")
  plt.ylabel("Average profits")
  plt.savefig("/Users/pellemeuzelaar/PycharmProjects/Practical4/bar1.png")

# plot_product_low_class_chart()

def plot_product_middle_class_chart():
  df = products_in_each_class() # Call from functions

  # Input the parameters
  x = df.loc[df["binned"] == 1, "product_id"]
  height = df.loc[df["binned"] == 1, "avg_daily_profit"]

  # Plot the bar chart for the middle class
  plt.bar(x, width=3.5, height=height, bottom=0, align="center", color ="green")
  plt.title("Average profits of 50%-80% class")
  plt.xlabel("Product number")
  plt.ylabel("Average profits")
  plt.savefig("/Users/pellemeuzelaar/PycharmProjects/Practical4/bar2.png")

# plot_product_middle_class_chart()

def plot_product_high_class_chart():
  df = products_in_each_class() # Call from functions

  # Input the parameters
  x = df.loc[df["binned"] == 2, "product_id"]
  height = df.loc[df["binned"] == 2, "avg_daily_profit"]

  # Plot the bar chart for the middle class
  plt.bar(x, width=3.5, height=height, bottom=0, align="center", color="blue")
  plt.title("Average profits of 80%-100% class")
  plt.xlabel("Product number")
  plt.ylabel("Average profits")
  plt.savefig("/Users/pellemeuzelaar/PycharmProjects/Practical4/bar3.png")

# plot_product_high_class_chart()

""" STEP 1.8: PROFIT CHART ALL CLASSES """

def plot_profits_per_product_sorted():
  df = profit_computation()  # Call from functions

  # Define the bar chart for all classes, sorted highest profit first
  fig, ax = plt.subplots()
  df.sort_values("avg_daily_profit", ascending=False)["avg_daily_profit"].plot.bar(
    ax=ax, xticks=df.index, rot=45, stacked=False, color="red")

  # Set x-axis steps
  ax.set_xticks(np.arange(0, len(df.index) + 1, 100))

  # Plot vertical lines for classes. These are hardcoded: the values are the product_ids that came
  # from the output of the list_products_per_class function

  print("put 50% marker at: ", df.query("product_id==742")["index"]) #prod. 742 is border, 123 index
  print("put 80% marker at: ", df.query("product_id==347")["index"]) #prod. 347 is border, 500 index
  plt.axvline(123, color="k", linestyle="dashed", linewidth=1)
  plt.axvline(500, color="k", linestyle="dashed", linewidth=1)

  # Set titles and labels and print the boxplot
  plt.title("Average profits of all products sorted to class")
  plt.xlabel("Index (not sorted to product number)")
  plt.ylabel("Average profits")
  plt.show()

# plot_profits_per_product_sorted()

""" STEP 1.10: AVERAGE AND MEAN DEMAND OVER REPLENISHMENT INTERVAL """

def avg_and_sd_demand_replenish_interval_low_class():
  df = df_product_orders_per_day() # Call df from the first product function

  # I need to compute the values m and s per product. I have the m and s per product per day.
  # I only need to multiply by the replenish interval (1 week/7 days)
  df["avg_demand_replenish_interval"] = df["avg_orders_per_day"] * 7
  df["sd_demand_replenish_interval"] = (df["standard_deviation"] * 7) ** (1/2)

  df = pd.concat([df,
                   df["avg_demand_replenish_interval"],
                   df["sd_demand_replenish_interval"]], axis=1, ignore_index=False)

  return df

# avg_and_sd_demand_replenish_interval_low_class()

""" STEP 1.11: COMPUTE BASE STOCK LEVEL """

def compute_base_stock_level():
  df = avg_and_sd_demand_replenish_interval_low_class() # Call df from the above function
  df2 = products_in_each_class()

  # Add the bin variables as column (because we dit not have it in this df yet)
  df = pd.concat([df, df2["binned"]], axis=1, ignore_index=False)
  df = df.T.drop_duplicates().T # Got an error from importing df, because of duplicate columns

  # Make functions for the base stock formula, z changes to 0.9, 0.95 and 0.99 per class
  base_stock_low = df["avg_demand_replenish_interval"] + 0.90 * df["sd_demand_replenish_interval"]
  base_stock_medium = df["avg_demand_replenish_interval"] + 0.95 * df["sd_demand_replenish_interval"]
  base_stock_high = df["avg_demand_replenish_interval"] + 0.99 * df["sd_demand_replenish_interval"]

  # Depending on the bin value (i.e. the class), the base stock is calculated and added to the df
  df.loc[df["binned"] == 0, "base_stock"] = base_stock_low
  df.loc[df["binned"] == 1, "base_stock"] = base_stock_medium
  df.loc[df["binned"] == 2, "base_stock"] = base_stock_high

  return df

# compute_base_stock_level()

""" STEP 1.12: COMPUTE PICK UP BOXES """

def pickup_box():
  df2 = volume_computation() # Call df from the above function

  # Compute the volume for the standard box (dimensions given), only 90% can be used!
  volume_standard_box = 0.9 * (40 * 40 * 20)

  # Set up the conditional that determines the amount of boxes required
  df2.loc[df2["volume"] <= volume_standard_box, "required_boxes"] = 1 # If the prod. vol. is </= 1 box
  df2.loc[df2["volume"] > volume_standard_box, "required_boxes"] = 2 # If vol. is > 1 box, 2 box

  # print(df2.loc[df2["required_boxes"] == 2]) # This returns nothing, no products need 2 boxes
  return df2

# pickup_box()

def plot_box_number():
  df2 = pickup_box() # Call from above function

  #Set parameters
  height = df2["required_boxes"]

  # Show required box number per product in a histogram
  plt.bar(df2["product_id"], height=height, color="red")
  plt.title("Number of boxes required per product")
  plt.ylabel("Number of boxes")
  plt.xlabel("Products")
  plt.show()

# plot_box_number()

""" STEP 1.14: CORRELATION MATRIX """

def transpose_data():
  df = df_total_demand_per_product() # Call the function from the beginning, with total products/day

  # Tranpose the current df so that it displays the days as columns instead of as a group in one row
  df2 = df.pivot(index="product_id", columns="day", values="orders").add_prefix("day ").reset_index()
  df.set_index("product_id") #Fix the dataframe
  return df2

# transpose_data()

def correlation_matrix():
  df2 = transpose_data()  # Call from previous function

  # Plot the data in a matrix in python output
  corr = df2.corr()
  print(corr)
  return corr

#correlation_matrix():

def plot_correlation_matrix():
  df = transpose_data() # Call from previous function

  # Plot the above correlation matrix as heatmap
  fig = plt.figure(figsize=(19, 15))
  plt.matshow(df.corr(), fignum=fig.number)
  plt.title("Correlation Matrix", fontsize=16);
  cb = plt.colorbar()
  cb.ax.tick_params(labelsize=14)
  plt.show()

# plot_correlation_matrix()

""" STEP 1.15: PRODUCT COUPLES """

def product_couples():
  corr = correlation_matrix() # Call from previous function

  # Filter the the matrix values to display correlations as a sorted list
  s = corr.unstack()
  so = s.sort_values(kind="quicksort")
  print(so)

  # Make a list that displays
  df = pd.DataFrame(so, columns=["correlations"])

  # Locate the couples that match the threshold 0.6
  df.loc[df["correlations"] >= 0.6, "match"] = 1 # If product corr higher than 0.6, input 1 in col
  df.loc[df["correlations"] <= -0.6, "match"] = 1 # If product has negative 0.6, input 1 in the col
  df.loc[df["correlations"] < 0.6, "match"] = 0 # If product has lower than 0.6, input 0 in the col
  df.loc[df["correlations"] == 1, "match"] = 0 # If product is 1 (same values), we don't want them

  return df

  # I WAS NOT ABLE TO TELL WHICH PRODUCTS WERE CORRELATED AND WHICH 1 OF THE 2 WAS IN A HIGH CLASS

# product_couples()

""" STEP 1.16: PRODUCT COUPLES MATRIX """

def plot_product_couples_matrix():
  df = product_couples() # Call from above function
  corr = correlation_matrix()

  # Formulate x/y values for the matched products that are to be marked
  coords = df.loc[df["match"] == 1]

  # Plot a new correlation matrix and input the match as variable in "patch" plot function to get mark
  fig, ax = plt.subplots()
  ax = sns.heatmap(corr, annot=True, linewidths=.5, ax=ax)
  ax.add_patch(plt.Rectangle((coords), 1, 1, fill=True, edgecolor="red", lw=3))
  plt.show()

# THIS FUNCTION RETURNS NOTHING BECAUSE I FOUND NO MATCHED VARIABLES, IF MATCH = 0 IT WILL WORK

# plot_product_couples_matrix()

""" STEP 2.1: LOSS IN SALES """

def loss_in_profit():
  df = products_in_each_class() # Call df from function

  # It is anticipated that the drop in sales for the three product classes will be 20%, 30%, and 50%,
  # avg_daily_profit (that we computed in the dataframe) will drop by the respective percentages
  profit_loss_low = 0.2 * df["avg_daily_profit"]
  profit_loss_medium = 0.3 * df["avg_daily_profit"]
  profit_loss_high = 0.5 * df["avg_daily_profit"]

  # Depending on the bin value (i.e. the class), the base stock is calculated and added to the df
  df.loc[df["binned"] == 0, "avg_daily_profit_loss"] = profit_loss_low
  df.loc[df["binned"] == 1, "avg_daily_profit_loss"] = profit_loss_medium
  df.loc[df["binned"] == 2, "avg_daily_profit_loss"] = profit_loss_high
  return df

# loss_in_profit()

""" STEP 2.2: RANKING BASED ON PROFIT LOSS """

def ranking_on_profit_loss():
  df = loss_in_profit() # Call from above function

  #Sort the dataframe based on profit loss and reset the index so we can count later
  df.sort_values(by=["avg_daily_profit_loss"], inplace=True)
  df.reset_index(drop=True, inplace=True)

  # Make a variable for the sum of total profit loss if these products were used:
  sum_losses = df.loc[df.index <= 960].avg_daily_profit_loss.sum()

  # Return the products that have the lowest profit losses. The max number of products in the current
  # warehouse is 960, because only 960 boxes fit and all products require only 1 box
  print("The 960 products with the lowest losses are: \n", df.iloc[0:960, 1]) #1 is product_id column
  print("Using these will result in total average daily profit loss of:", sum_losses)

  return df, sum_losses

# ranking_on_profit_loss()

""" STEP 2.3: RANKING TO RATIO OF PROFIT LOSS AND BOXES """

def ranking_on_ratio_losses_and_boxes():
  df = loss_in_profit() # Call from above functions
  df2 = pickup_box()

  # Calculate the ratio of profit losses to boxes required and put into new column:
  df["ratio"] = df["avg_daily_profit_loss"] / df2["required_boxes"]

  #Sort the dataframe based on profit loss and reset the index so we can count later
  df.sort_values(by=["ratio"], inplace=True)
  df.reset_index(drop=True, inplace=True)

  # Make a variable for the sum of total profit loss if these products were used:
  sum_losses = df.loc[df.index <= 960].avg_daily_profit_loss.sum()

  #Print the boxes to be stored in warehouse 1
  print("The products with the lowest ratios are: \n", df.iloc[0:960, 1:2]) #1 is product_id column
  print("Using these will result in total average daily profit loss of:", sum_losses)

  return df, sum_losses

# ranking_on_ratio_losses_and_boxes()

""" STEP 2.4: KNAPSACK PROBLEM """

def knapsack():
  df = loss_in_profit() # Call from previous function

  # The upper limit of storage in the storage warehouse 1 may not exceed 960 pick-up boxes (ub)
  # and if 2 products have a correlation higher than 0.6, they have to be stored together (constraint).
  # However, since none of the products correlate more than 0.6, this is not a constraint.

  # Decision variables (capacity, profit loss, number of products selected):
  p = df["avg_daily_profit_loss"] # P stands for profit loss
  n = len(p) # Number of products that we could potentially lose profits on

  # Call the Gurobi model
  m = Model("Belsimpel")
  x = m.addVars(n, vtype=GRB.BINARY, ub=960, name="Average daily profit loss") # The lower bound is 960

  # Objective (minimize profit loss):
  m.setObjective(quicksum(p[i] * x[i] for i in range(n)), GRB.MAXIMIZE) # We need to display the max
  # loss if the full capacity is used

  # Run the model
  m.optimize()

  # Display the output
  sum_losses_3 = m.ObjVal
  products_selected = []
  for i in range(n):
    if x[i].X > 0.5:
      products_selected.append(i)

  print("If the products", products_selected,
        "are selected, total profit loss will be minimal: ", sum_losses_3)
  return products_selected, sum_losses_3

knapsack()

""" STEP 2.5: TABLE OF PROFIT """

def print_table_with_solutions():
  sum_losses_1 = ranking_on_profit_loss()
  sum_losses_2 = ranking_on_ratio_losses_and_boxes()
  sum_losses_3 = knapsack()

  # Combine losses from al methods into a list
  losses_each_method = [sum_losses_1, sum_losses_2, sum_losses_3]

  # Put the losses in a pandas dataframe
  df = pd.DataFrame(losses_each_method, columns=["product_id", "total_daily_loss_per_method"])
  df = df.drop(["product_id"], axis=1) # We don't need product_ids in the table
  df.insert(0, "method_number", [1, 2, 3], True) # Insert method number for clarity

  # Plot a table with the solutions from the dataframe
  plt.table(cellText=df.values, colLabels=df.columns, loc="center")
  plt.show()

# print_table_with_solutions()