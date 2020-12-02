import pymongo
import os, glob
import json
import gzip
# from bson import json_util
import datetime
from tqdm import tqdm

def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

# load envs
with open('environments.json', 'r') as f:
    envs = json.loads(f.read())
    
for k, v in envs.items():
    os.environ[k] = v

client = pymongo.MongoClient(os.environ['hostname'], int(os.environ['port']))
client.admin.authenticate(
    os.environ['username'], os.environ['password'], mechanism='SCRAM-SHA-1')

with open('databases.txt', 'r') as f:
    databases = [i.strip() for i in f.readlines()]

for db in databases:
    collections = client[db].list_collection_names()
    print(f'collections_sample: {collections[:5]}')

    for collection_name in sorted(collections):
        output_dir = f'data/{db}'
        filename =  f'{output_dir}/{collection_name}.jl'
        collection = client[db][collection_name]
        
        print(f'processing {filename}')
        
        os.makedirs(output_dir, exist_ok=True)
        cursor = collection.find({})
        total_records = collection.estimated_document_count()
        
        with open(filename, 'w') as f:
            for i in tqdm(cursor, total=total_records):
                f.write(json.dumps(i, default=myconverter, ensure_ascii=False))
                f.write('\n')   
        
    #     break