# COMMANDS:
# init                       : Initialize the database values
# find search_entry          : Find a station based on the letters in the search entry
# update station_id          : Update a station with it station ID
# delete station_id          : Delete a station with it station ID
# lowest                     : Returns a list of the station with less than 20% bike availability
# deactivate lat long radius : Deactivate all stations within a radius around coordinates

import requests
import json
import sys
from pymongo import MongoClient, GEOSPHERE
import dateutil.parser
from pprint import pprint

# Returns a Json object on an endpoint
def getJson(url):
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json

# Display a command error
def errors():
    print("Error: bad arguments")
    print("Possible arguments:")
    print("find search_entry")
    print("update station_id")
    print("delete station_id")
    print("deactivate lat long radius")
    print("lowest")

# Main function
if __name__ == "__main__":
    client = MongoClient('mongodb://localhost:27017')
    db = client['lille']
    stations = db['velo_stations']

    url_Lille = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"

    # Bad arguments
    if len(sys.argv) < 2:
        errors()
        exit(1)

    # Initialize the data base with values
    if sys.argv[1] == 'init':
        db.drop_collection('velo_stations')
        json_Lille = getJson(url_Lille).get("records", [])
        Lille = []
        for station in json_Lille:
            try:
                ratio = 0
                if station['fields']['nbplacesdispo'] + station['fields']['nbvelosdispo'] != 0:
                    ratio = station['fields']['nbvelosdispo']/(station['fields']['nbplacesdispo'] + station['fields']['nbvelosdispo'])
                tmp = {
                    'station_id': station['fields']['libelle'],
                    'date': dateutil.parser.parse(station['fields']['datemiseajour']),
                    'name': station['fields']['nom'],
                    'geo': station['geometry'],
                    'available': station['fields']['etat'] == 'EN SERVICE',
                    'bike': station['fields']['nbvelosdispo'],
                    'ratio': ratio,
                    'size': station['fields']['nbplacesdispo'] + station['fields']['nbvelosdispo'],
                    'tpe': station['fields']['type'] == 'AVEC TPE'
                }
                Lille.append(tmp)
            except KeyError:
                print('Field value not found on entry:', station, "\n")
        
        try:
            stations.insert_many(Lille, ordered=False)
            stations.create_index([('station_id', 1),('date', -1)], unique=True)
            stations.create_index([("geo", GEOSPHERE)])
        except:
            pass
        print("DATABASE INITIALIZED:", stations.count_documents({}))

    # Find station with name
    if sys.argv[1] == 'find':
        if len(sys.argv) != 3:
            errors()
            exit(1)
        stat = stations.find({"name": {"$regex": ".*"+sys.argv[2]+".*" }})
        for station in stat:
            pprint(station)
    
    # Update a station with it ID
    elif sys.argv[1] == 'update':
        if len(sys.argv) != 3:
            errors()
            exit(1)
        json_Lille = getJson(url_Lille).get("records", [])
        for station in json_Lille:
            if station['fields']['libelle'] == sys.argv[2]:
                ratio = 0
                if station['fields']['nbplacesdispo'] + station['fields']['nbvelosdispo'] != 0:
                    ratio = station['fields']['nbvelosdispo']/(station['fields']['nbplacesdispo'] + station['fields']['nbvelosdispo'])
                stations.insert_one({
                    'station_id': station['fields']['libelle'],
                    'date': dateutil.parser.parse(station['fields']['datemiseajour']),
                    'name': station['fields']['nom'],
                    'geolocalisation': station['geometry'],
                    'available': station['fields']['etat'] == 'EN SERVICE',
                    'bike': station['fields']['nbvelosdispo'],
                    'size': station['fields']['nbplacesdispo'] + station['fields']['nbvelosdispo'],
                    'tpe': station['fields']['type'] == 'AVEC TPE'
                })
                print("Station:", sys.argv[2], "updated!")

    # Delete all datas on a station with it ID
    elif sys.argv[1] == 'delete':
        if len(sys.argv) != 3:
            errors()
            exit(1)
        print(stations.count_documents({}))
        stations.delete_many({"station_id": int(sys.argv[2])})
        print("Station:", sys.argv[2], "deleted!")
        print(stations.count_documents({}))

    # Deactivate all stations within a radius with coordinates
    elif sys.argv[1] == 'deactivate':
        if len(sys.argv) != 5:
            errors()
            exit(1)

        stations.update_many({"geo": {"$near": {"$geometry": {"type": "Point", "coordinates": [float(sys.argv[3]), float(sys.argv[2])]}, "$maxDistance": int(sys.argv[4])}}}, {"$set": {"available": False}})
        stats = stations.find({"geo": {"$near": {"$geometry": {"type": "Point", "coordinates": [float(sys.argv[3]), float(sys.argv[2])]}, "$maxDistance": int(sys.argv[4])}}})
        for s in stats:
            pprint(s)

    # Returns all stations with a ratio under 20% of bike/total_stand between 18h and 19h
    elif sys.argv[1] == 'lowest':
        stats = stations.aggregate([
            {"$match": {
                "available": True,
                "size": {"$gt": 0}
            }},
            {"$project": {
                "name": "$name",
                "station_id": "$station_id",
                "date_day": {
                    "$dayOfWeek" : "$date"
                },
                "date_hour": {
                    "$hour" : "$date"
                },
                "ratio": {
                    "$divide": [ "$bike", "$size" ]
                }
            }},
            {"$match": {
                "ratio": {"$lt": 0.2},
                "date_day" : {"$gte": 1, "$lt": 7},
                "date_hour" : {"$gte": 18, "$lt": 19},
            }},
            {"$group": {
                "_id" : {"station_id": "$station_id", "name": "$name"}
            }}
        ])
        for s in stats:
            pprint(s)