#Author: Pelle Meuzelaar
#Date: 12-10-2022
#Assignment: Belsimpel warehouse case


import pandas as pd
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

""" STEP 1.4: PRODUCT VOLUME """

#make new dataframe from margins.csv
df2 = pd.read_csv("dimensions.csv")

#multiply the length, width and height to get the volume per product in a column
df2["volume"] = df2["length"] * df2["width"] * df2["height"]

#sort new dataframe to display highest daily profit margin products first
df2 = df2.sort_values("product_id", ascending=True)
print(df2)

#show volume per product per day in a histogram, input 3 for the bin parameter because there are 3 classes
plt.hist(df2["volume"], rwidth=0.95, color="red")
plt.title("Product volume ranges")
plt.ylabel("Number of products")
plt.xlabel("Volume")
plt.show()
