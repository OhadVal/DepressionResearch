import json
import pandas as pd
from elasticsearch import Elasticsearch


data = pd.read_csv(r'C:\Users\Gilad\PycharmProjects\DepressionResearch\Create_Data\SubmissionsDF.csv')
data.to_json('bulk_load.json', orient='index')

json_data = open('bulk_load.json').read()
data = json.loads(json_data)

es = Elasticsearch("http://localhost:9200")
index = 'reddit'
doc_type = 'submission'

if es.indices.exists(index=index):
    index_counter = es.count(index=index)
else:
    es.indices.create(index=index, ignore=400)
    index_counter = es.count(index=index)

count = index_counter['count']
for d in data.items():
    es.index(index=index, doc_type=doc_type, id=count, body=d[1])
    count += 1



