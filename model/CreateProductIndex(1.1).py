#Author: Pelle Meuzelaar
#Date: 12-10-2022
#Assignment: Belsimpel warehouse case


from elasticsearch import Elasticsearch, helpers
import csv
import urllib3

""" STEP 1.1: CREATE INDEX """

def create_product_index():
  # Disable annoying warnings before the start of the code:
  urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

  # Load in elasticsearch client
  es = Elasticsearch("https://localhost:9200", ca_certs=False, verify_certs=False, http_auth=('elastic',
                                                                                              'j3nfTPHpJxz5iYTVMu8V'))

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

  # We need to delete the database because we don't want the next Python script to query it again.
  # es.indices.delete(index='products', ignore=[400, 404])


