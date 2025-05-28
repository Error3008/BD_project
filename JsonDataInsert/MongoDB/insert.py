import pymongo  
import json
from DB_DATA import ConnectionString

myclient = pymongo.MongoClient(ConnectionString)

with open("JsonDataGenerate/output/bilety_kolejowy.json", "r") as file:
    data = json.load(file)["bilety"]
    
mydb = myclient["alex"]
mycol = mydb["bilety_kolejowy"]

x = mycol.insert_many(data)

print(myclient.list_database_names())
print(x.inserted_ids)