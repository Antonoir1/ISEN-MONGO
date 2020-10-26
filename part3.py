import sys
import json
from pprint import pprint
from pymongo import MongoClient
from pprint import pprint

if __name__ == "__main__":
   #Connexion a MongoDB
   client = MongoClient('mongodb://localhost:27017')

   db = client.lille

   collection = db.velo_stations

   #Lancement de la recherche
   essai = collection.find_one(
   {
     "$and":[
      {"geo":{
       "$near":{
         "$geometry":{
           "type":"Point",
           "coordinates" : [float(sys.argv[1]),float(sys.argv[2])]
         }
        } 
       }},
      {"dispo":{"$ne":0}}
     ]
   })
   #Impression du resultat
   print("La station la plus proche est :")
   pprint(essai['name'])

   print("Il y a",essai['dispo'],"places disponibles")

