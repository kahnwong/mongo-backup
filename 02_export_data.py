import datetime
import glob
import gzip
import json
import os
import pymongo
from tqdm import tqdm

# deserializer config
def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


# load envs
with open('environments.json', 'r') as f:
    envs = json.loads(f.read())

for k, v in envs.items():
    os.environ[k] = v

# auth
client = pymongo.MongoClient(os.environ['hostname'], int(os.environ['port']))
client.admin.authenticate(
    os.environ['username'], os.environ['password'], mechanism='SCRAM-SHA-1')

# export
with open('databases.txt', 'r') as f:
    databases = [i.strip() for i in f.readlines()]

os.makedirs('data', exist_ok=True)

for db in databases:
    print(f'DB_NAME: {db}')
    collections = client[db].list_collection_names()
    print(f'collections_sample: {collections[:5]}')

    for collection_name in sorted(collections):
        output_dir = f'data/{db}'
        filename = f'{output_dir}/{collection_name}.jl'
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
