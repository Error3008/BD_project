import psycopg2, pymongo
from resources.PSQL_conn import *
from resources.MongoDB_conn import ConnectionString
from Requests.MongoDB.request import mongo_requests
from time import perf_counter


def get_psql_request(filename):
    requests = []
    with open(f"ExperimentWithTime/Requests/PSQL/{filename}", "r") as file:
        request = ""
        for line in file.readlines():
            request += line
            if line.strip().endswith(";"):
                requests.append(request)
                request = ""
    return requests

def test(psql_request, psql_jsonb_request, mongo_func, mongo_db, psql_cursor):
    def measure_time(func):
        start = perf_counter()
        func()
        end = perf_counter()
        return end - start

    psql_time = measure_time(lambda: (psql_cursor.execute(psql_request), psql_cursor.fetchall()))
    psql_jsonb_time = measure_time(lambda: (psql_cursor.execute(psql_jsonb_request), psql_cursor.fetchall()))
    mongo_time = measure_time(lambda: mongo_func(mongo_db))

    return {
        "psql_time": psql_time,
        "psql_jsonb_time": psql_jsonb_time,
        "mongo_time": mongo_time
    }

mongo_client = pymongo.MongoClient(ConnectionString)
psql_conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

mongo_db = mongo_client["alex"]
psql_cursor = psql_conn.cursor()

psql_request = get_psql_request("request_for_PSQL.sql")
psql_jsonb_request = get_psql_request("request_for_PSQL_JSONB.sql")

for i in range(4):
    print(f"Test {i+1}:")
    result = test(psql_request[i], psql_jsonb_request[i], 
                  mongo_requests[i], mongo_db, psql_cursor)
    print(f"PSQL time: {result['psql_time']:.10f} seconds")
    print(f"PSQL JSONB time: {result['psql_jsonb_time']:.10f} seconds")
    print(f"MongoDB time: {result['mongo_time']:.10f} seconds")
    print("-" * 40)

psql_cursor.close()
psql_conn.close()