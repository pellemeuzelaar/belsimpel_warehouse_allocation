#Author: Pelle Meuzelaar
#Date: 12-10-2022
#Assignment: Belsimpel warehouse case


from elasticsearch import Elasticsearch, helpers
import pandasticsearch
import csv
import urllib3

""" STEP 1: DATA GATHERING AND PROCESSING """

#disable annoying warnings before the start of the code:
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#load in elasticsearch client
es = Elasticsearch("https://localhost:9200", ca_certs=False, verify_certs=False, http_auth=('elastic',
                                                                                            'j3nfTPHpJxz5iYTVMu8V'))

#define the mapping of the index to be created
settings = {
  "mappings": {
    "properties": {
      "product_id": {
        "type": "long"
      },
      "length": {
        "type": "long"
      },
      "width": {
        "type": "long"
      },
      "height": {
        "type": "long"
      }
    }
  }
}


#create an empty index
es.indices.create(index='dimensions', body=settings)

#load the csv file into the elastic index using bulk command
with open('dimensions.csv') as f:
  reader = csv.DictReader(f)
  helpers.bulk(es, reader, index='dimensions')


#confirm that the index with product orders per day is created
print("index 'dimensions' is created")

# We need to delete the database because we don't want the next Python script to query it again.
# es.indices.delete(index='margins', ignore=[400, 404])

