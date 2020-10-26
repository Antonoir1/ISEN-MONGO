import requests
import json
from pymongo import MongoClient, GEO2D, GEOSPHERE
from pprint import pprint
from time import sleep
import dateutil.parser

def getData(url):
    #Recuperation des donnees
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records",[])

def updateData():
    #Mise a jour des donnees
    newData = getData(url)
    for data in newData:
        query = {"name":data["fields"]["nom"]}
        newValue = {"$set":{"available": data['fields']['etat'] == 'EN SERVICE',"date":dateutil.parser.parse(data['fields']['datemiseajour']),"dispo":data["fields"]["nbvelosdispo"]}}
        collection.update_many(query,newValue)

if __name__ == "__main__":
    client = MongoClient('mongodb://localhost:27017')
    
    #Recuperation des donnees ssur l'url choisit    	
    url = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion" 
    json_data = getData(url)
    
    #Creation de la database
    db = client.lille

    #Creation de la collection
    collection = db.velo_stations
    collection.drop() 
    for data in json_data :
        init_post = {
          "station_id":data["fields"]["libelle"],
          "name":data["fields"]["nom"],
          "geo": {
                        'type': 'Point',
                        'coordinates': data['geometry']['coordinates']
                    },
          "available": data['fields']['etat'] == 'EN SERVICE',
          "size": data['fields']['nbplacesdispo'] + data['fields']['nbvelosdispo'],
          "bike":data['fields']['nbvelosdispo'],
          "tpe":data["fields"]["type"],
          'date': dateutil.parser.parse(data['fields']['datemiseajour'])
        } 
    
        post_id = collection.insert_one(init_post).inserted_id
    collection.create_index([("station_id",1),('date',-1)])
    collection.create_index([("geo",GEOSPHERE)])
    print("--------------\nInitialisation de la database ::::::::: OK\n")
    while True:
        try:
           updateData()

           print("---------------\nData updated :::::::: OK")
           sleep(600)
        except KeyboardInterrupt:
           print("\nFin du programme")
           break
