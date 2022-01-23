import requests
import json
import datetime


def earthquake(f):
    parameters = {"format": "geojson",
                  "starttime": "2022-01-01",
                  "endtime": datetime.datetime.now().strftime('%Y-%m-%d'),
                  "alertlevel": "orange"}
    data = requests.get(f, params=parameters)
    data = json.loads(data.text)
    return data


f = r"https://earthquake.usgs.gov/fdsnws/event/1/query?"
a = earthquake(f)

for i in (a["features"]):
    print(i["properties"]["time"], i["properties"]["place"],
          i["properties"]["cdi"], i["properties"]["tsunami"])
