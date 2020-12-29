import datetime
import glob
import gzip
import json
import os
import pymongo

# load envs
with open('environments.json', 'r') as f:
    envs = json.loads(f.read())

for k, v in envs.items():
    os.environ[k] = v

# auth
client = pymongo.MongoClient(os.environ['hostname'], int(os.environ['port']))
client.admin.authenticate(
    os.environ['username'], os.environ['password'], mechanism='SCRAM-SHA-1')

# export db list
dbs = client.list_database_names()

with open('databases.txt', 'w') as f:
    f.writelines([i+'\n' for i in dbs])
