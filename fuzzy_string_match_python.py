
# coding: utf-8

# In[6]:

import pandas as pd
brands_df = pd.read_csv("D:\\Data\\Dinesh\Work\\revlon\\fuzzy_classification\\brands.csv", header = None)
brands = brands_df[0].tolist()
len(brands)


# In[10]:

from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch(["34.236.190.112"])

print(len(es.indices.get_alias("revlon_ht_*").keys()))
print("fetching")
response = es.search(
    index = ",".join(es.indices.get_alias("revlon_ht_*").keys()), 
    filter_path=['aggregations.aggs.buckets.key'],
    body = {
        "size": 0,
        "query": {
            "match": {
                "source": "Instagram"
            }
        },
        "aggregations": {
            "aggs": {
                "terms": {
                    "field": "mentions.keyword",
                    "size": 3000000
                }
            }
        }
    },
    request_timeout=(60 * 60 * 24 * 3)
)
print("fetched")
handles = response["aggregations"]["aggs"]["buckets"]
handle_values = []
for handle in handles:
    handle_values.append(handle["key"])
print(len(handle_values))
print(handles[-1])
    
# print(res)
# print("Got %d Hits:" % res['hits']['total'])
# for hit in res['hits']['hits']:
#     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])


# In[11]:

rem = len(handle_values)
iter = 0
no_per_file = 3000000
done = False
while True:
    with open("handle_values_insta_30lakh" + str(iter + 1) + ".csv", "w", encoding="utf-8") as fp:
        print("writing to " + "handle_values_insta_30lakh" + str(iter + 1) + ".csv")
        start = iter * no_per_file
        end = start + no_per_file
        print(str(start) + " " + str(end))
        if(end>=len(handle_values)):
            end = len(handle_values)
            done = True
        to_write = handle_values[start: end]
        fp.write("\n".join(to_write))
        iter += 1
        if done:
            break
print("done written to file")


# In[8]:

import json
json.dump(handle_values, open("handle_values_insta_1lakh.json", "w", encoding="utf-8"))
print("done!")


# In[9]:

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pymongo

client = pymongo.MongoClient("localhost", 27017)
db = client.revlon
print("inserting into db, " + db.name)

# handle_values_df = pd.DataFrame({"handles": handle_values[0:10] })
# handle_values_df
print(len(handle_values))

def find_match(row):
    match = process.extractOne(row, handle_values)
#     print(row)
    db.insta_brands_to_handle.insert_one({
        "brand_name": str(row),
        "handle": match[0] if match else None,
        "score": match[1] if match else None,
        "source": "instagram"
    })
    return pd.Series({"handle_match": match[0], "match_score": match[1]}) if match else pd.Series({"handle_match": None, "match_score": None})

print("starting")
brands_df_full = brands_df.merge(
    brands_df[0].apply(find_match), 
    left_index=True, 
    right_index=True
)
brands_df_full
# handle_values_df_test = handle_values_df_test["handles"].apply(find_match, axis = 1)
# handle_values_df_full.to_csv("handles_to_brands.tsv", "\t", encoding="utf-8", index = False)
print("done!!!")


# In[36]:

brands_df_full.to_csv("insta_handles_to_brands.tsv", "\t", encoding="utf-8", index = False)
print("done!!!")


# In[49]:

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
print(handle_values[0])
process.extractOne(handle_values[0], brands)[0]

