import requests
import json
from pymongo import MongoClient
from pprint import pprint
from time import sleep

def getData(url):
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records",[])

def updateData():
    newData = getData(url)
    for data in newData:
        query = {"Name":data["fields"]["nom"]}
        newValue = {"$set":{"Available": data['fields']['etat'] == 'EN SERVICE',}}
        collection.update_one(query,newValue)

if __name__ == "__main__":
    client = MongoClient('mongodb://localhost:27017')
    client.drop_database('veloville_database')

    url = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion" 
    json_data = getData(url)
    db = client.veloville_database
    collection = db.lille_col

    for data in json_data :
        init_post = {
              "Name":data["fields"]["nom"],
          "Geo": {
                        'type': 'Point',
                        'coordinates': data['geometry']['coordinates']
                    },
          "Available": data['fields']['etat'] == 'EN SERVICE',
          "Size": data['fields']['nbplacesdispo'] + data['fields']['nbvelosdispo'],
          "TPE":data["fields"]["type"]
            } 
    
        post_id = collection.insert_one(init_post).inserted_id

    print("--------------\nInitialisation de la database ::::::::: OK\n")
    while True:
        updateData()

        print("---------------\nData updated :::::::: OK")
        sleep(600)