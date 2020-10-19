import requests
import json
from time import sleep
from pymongo import MongoClient
import dateutil.parser

def getJson(url):
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json



if __name__ == "__main__":
    client = MongoClient('mongodb://localhost:27017')
    client.drop_database('veloville_database')
    db = client.veloville_database
    stations = db.lille_col
    stations.create_index([('station_id', 1)], unique=True)

    url_Lille = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"

    while True:
        json_Lille = getJson(url_Lille).get("records", [])
        Lille = []
        for station in json_Lille:
            try:
                tmp = {
                    'city': 'Lille',
                    'station_id': station['fields']['libelle'],
                    'name': station['fields']['nom'],
                    'geolocalisation': station['geometry'],
                    'available': station['fields']['etat'] == 'EN SERVICE',
                    'size': station['fields']['nbplacesdispo'] + station['fields']['nbvelosdispo'],
                    'tpe': station['fields']['type'] == 'AVEC TPE'
                }
                Lille.append(tmp)
            except KeyError:
                print('Field value not found on entry:', station, "\n")
        
        try:
            db.datas.insert_many(Lille)
        except:
            pass
        print("DATABASE UPDATED")
        sleep(10)