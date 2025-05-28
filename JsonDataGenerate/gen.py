import json
import random
import string

def przyjazdy_i_odjazdy(n : int = 100):
    with open("JsonDataGenerate/resources/bus_corp.json", "r", encoding="UTF-8") as f:
        bus_corp = json.load(f)
    with open("JsonDataGenerate/resources/city.json", "r", encoding="UTF-8") as f:
        city = json.load(f)
    
    data = {"autobusy": []}
    for i in range(n):
        day = random.randint(1, 27)
        month = random.randint(1, 12)
        year = 2025
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        travel_time = random.randint(1, 10)
        day_of_arrival = day + (1 if (hour + travel_time) >= 24 else 0)
        hour_of_arrival = (hour + travel_time) % 24
        bus = {
            "id": i,
            "firma": random.choice(bus_corp["firmy_autobusowe"]),
            "numer_rejsu": "".join(random.choice(string.ascii_letters).upper() for _ in range(3)) + "-" + str(random.randint(100, 999)),
            "odjazd": {
                "miasto": random.choice(city["miasta"]),
                "data": f"{year}-{month:02d}-{day:02d}",
                "godzina": f"{hour:02d}:{minute:02d}"
            },
            "przyjazd": {
                "miasto": random.choice(city["miasta"]),
                "data": f"{year}-{month:02d}-{day_of_arrival:02d}",
                "godzina": f"{hour_of_arrival:02d}:{minute:02d}"
            }
        }
        data["autobusy"].append(bus)
    
    with open("JsonDataGenerate/output/przyjazdy_i_odjazdy.json", "w", encoding="UTF-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def woman_surname_check(name, surname):
    if name[-1] == "a":
        if surname.endswith("ski"):
            return surname[:-3] + "ska"
        elif surname.endswith("cki"):
            return surname[:-3] + "cka"
        else:
            return surname
    else:
        return surname
        
def bilety(n : int = 500):
    with open("JsonDataGenerate/output/przyjazdy_i_odjazdy.json", "r", encoding="UTF-8") as f:
        autobusy = json.load(f)["autobusy"]
    with open("JsonDataGenerate/resources/names.json", "r", encoding="UTF-8") as f:
        names = json.load(f)
    with open("JsonDataGenerate/resources/surnames.json", "r", encoding="UTF-8") as f:
        surnames = json.load(f)
    
    data = {"bilety": []}
    for i in range(n):
        name = random.choice(names["imiona"])
        surname = woman_surname_check(name, random.choice(surnames["nazwiska"]))
        autobus = random.choice(autobusy)
        bilet = {
            "id": i,
            "imie": name,
            "nazwisko": surname,
            "numer_rejsu": autobus["numer_rejsu"],
            "odjazd": {
                "miasto": autobus["odjazd"]["miasto"],
                "peron": random.randint(1, 10),
                "stanowisko": random.randint(1, 10)
            },
            "przyjazd": {
                "miasto": autobus["przyjazd"]["miasto"],
                "peron": random.randint(1, 10),
                "stanowisko": random.randint(1, 10)
            }
        }
        data["bilety"].append(bilet)
    
    with open("JsonDataGenerate/output/bilety.json", "w", encoding="UTF-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    przyjazdy_i_odjazdy()
    bilety()