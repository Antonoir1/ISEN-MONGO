import requests
import json

def getJson(url):
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json



if __name__ == "__main__":
    url_Lille = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
    url_Paris = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=3000&facet=name&facet=is_installed&facet=is_renting&facet=is_returning&facet=nom_arrondissement_communes"
    url_Lyon = "https://public.opendatasoft.com/api/records/1.0/search/?dataset=station-velov-grand-lyon&q=&rows=3000&facet=name&facet=commune&facet=bonus&facet=status&facet=available&facet=availabl_1&facet=availabili&facet=availabi_1&facet=last_upd_1"
    url_Rennes = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=etat-des-stations-le-velo-star-en-temps-reel&q=&rows=3000&facet=nom&facet=etat&facet=nombreemplacementsactuels&facet=nombreemplacementsdisponibles&facet=nombrevelosdisponibles"

    print("Fetching ressources:\n")
    print("Lille 1/1...")
    json_Lille = getJson(url_Lille).get("records", [])
    Lille = []
    for station in json_Lille:
        try:
            tmp = {
                'name': station['fields']['nom'],
                'geolocalisation': {
                    'type': 'Point',
                    'coordinates': station['geometry']['coordinates']
                },
                'available': station['fields']['etat'] == 'EN SERVICE',
                'size': station['fields']['nbplacesdispo'] + station['fields']['nbvelosdispo'],
                'tpe': station['fields']['type'] == 'AVEC TPE'
            }
            Lille.append(tmp)
        except KeyError:
            print('Field value not found on entry:', station, "\n")
    print("Done.\n")
    # print(Lille,"\n")

    print("Paris 1/1...")
    json_Paris = getJson(url_Paris).get("records", [])
    Paris = []
    for station in json_Paris:
        try:
            tmp = {
                'name': station['fields']['name'],
                'geolocalisation': {
                    'type': 'Point',
                    'coordinates': station['geometry']['coordinates']
                },
                'available': station['fields']['is_installed'] == 'OUI',
                'size': station['fields']['capacity'],
                'tpe': station['fields']['is_renting'] == 'OUI'
            }
            Paris.append(tmp)
        except KeyError:
            print('Field value not found on entry:', station, "\n")
    print("Done.\n")
    # print(Paris,"\n")

    print("Lyon 1/1...")
    json_Lyon = getJson(url_Lyon).get("records", [])
    Lyon = []
    for station in json_Lyon:
        try:
            tmp = {
                'name': station['fields']['name'],
                'geolocalisation': {
                    'type': 'Point',
                    'coordinates': station['geometry']['coordinates']
                },
                'available': station['fields']['status'] == 'OPEN',
                'size': station['fields']['bike_stand'],
                'tpe': station['fields']['banking'] == 't'
            }
            Lyon.append(tmp)
        except KeyError:
            print('Field value not found on entry:', station, "\n")
    print("Done.\n")
    # print(Lyon,"\n")

    print("Rennes 1/1...")
    json_Rennes = getJson(url_Rennes).get("records", [])
    Rennes = []
    for station in json_Rennes:
        try:
            tmp = {
                'name': station['fields']['nom'],
                'geolocalisation': {
                    'type': 'Point',
                    'coordinates': station['geometry']['coordinates']
                },
                'available': station['fields']['etat'] == 'En fonctionnement',
                'size': station['fields']['nombreemplacementsactuels'] # TPE not found
            }
            Rennes.append(tmp)
        except KeyError:
            print('Field value not found on entry:', station)
    print("Done.\n")
    # print(Rennes, "\n")

    print("Done fetching.")