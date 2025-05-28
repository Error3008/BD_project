import psycopg2
import json
from DB_DATA import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD


class Database:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            self.cursor = self.connection.cursor()
            print("Database connection successful")
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def insert_data_jsonb(self, table_name, data):
        for item in data:
            self.cursor.execute(
                f"INSERT INTO {table_name} (id, data) VALUES (%s, %s)",
                (item["id"], json.dumps(item))
            )
        self.connection.commit()
        print(f"Inserted {len(data)} records into {table_name}")

    def get_column_names(self, table_name):
        self.cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
        return [row[0] for row in self.cursor.fetchall()]

    def from_json_to_sql(self, column_names, json_data):
        sql_data = []
        for name in column_names:
            try:
                sql_data.append("'"+str(json_data[name])+"'")
            except KeyError:
                name_parts = name.split("__")
                sql_data.append("'"+str(json_data[name_parts[0]][name_parts[1]])+"'")
        return sql_data

    def insert_data(self, table_name, data):
        column_names = self.get_column_names(table_name)
        for item in data:
            self.cursor.execute(
                f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(self.from_json_to_sql(column_names, item))})"
            )
        self.connection.commit()
        print(f"Inserted {len(data)} records into {table_name}")
        
if __name__ == "__main__":
    db = Database()
    db.connect()
    with open("JsonDataGenerate/output/bilety.json", "r") as file:
        data = json.load(file)["bilety"]
        db.insert_data_jsonb("bilety_json", data)
        db.insert_data("bilety", data)
    with open("JsonDataGenerate/output/przyjazdy_i_odjazdy.json", "r") as file:
        data = json.load(file)["autobusy"]
        db.insert_data_jsonb("przyjazdy_i_odjazdy_json", data)
        db.insert_data("przyjazdy_i_odjazdy", data)
    db.close()