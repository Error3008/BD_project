import psycopg2
import json
try:
    connection = psycopg2.connect(
        host="localhost",
        database="alex",
        user="alex",
        password="1404"
    )
    cursor = connection.cursor()
    print("Database connection successful")
except Exception as e:
    print(f"Error connecting to database: {e}")

with open("JsonDataGenerate/output/bilety.json", "r") as file:
    data = json.load(file)["bilety"]
    print("JSON data loaded successfully")

for item in data:
    cursor.execute(
            "INSERT INTO bilety_json (id, data) VALUES (%s, %s)",
            (item["id"], json.dumps(item))
        )
    connection.commit()

for item in data:
    cursor.execute(
        """
            INSERT INTO bilety 
            (id, imie, nazwisko, numer_rejsu, odjazd__miasto, odjazd__peron, odjazd__stanowisko, przyjazd__miasto, przyjazd__peron, przyjazd__stanowisko) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (item["id"], item["imie"], item["nazwisko"], item["numer_rejsu"], 
         item["odjazd"]["miasto"], item["odjazd"]["peron"], item["odjazd"]["stanowisko"],
         item["przyjazd"]["miasto"], item["przyjazd"]["peron"], item["przyjazd"]["stanowisko"])
    )
    connection.commit()
    print(f"Inserted item with id: {item['id']}")

cursor.execute("SELECT * FROM bilety")
rows = cursor.fetchall()
for row in rows:
    print(row)


cursor.close()
connection.close()