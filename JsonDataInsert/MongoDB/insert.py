import pymongo  
import json
from DB_DATA import ConnectionString

myclient = pymongo.MongoClient(ConnectionString)

with open("JsonDataGenerate/output/bilety.json", "r") as file:
    bilety = json.load(file)["bilety"]
with open("JsonDataGenerate/output/przyjazdy_i_odjazdy.json", "r") as file:
    przyjazdy_i_odjazdy = json.load(file)["autobusy"]
    
mydb = myclient["alex"]
bilety_col = mydb["bilety"]
przyjazdy_i_odjazdy_col = mydb["przyjazdy_i_odjazdy"]


bilety_col.insert_many(bilety)
przyjazdy_i_odjazdy_col.insert_many(przyjazdy_i_odjazdy)
