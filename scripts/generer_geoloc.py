import requests
import pandas as pd
import time

# Exemple : fichier CSV avec codes postaux uniques
codes_postaux_df = pd.read_csv("codes_postaux_uniques.csv")  # colonne: code_postal

def get_lat_lon(cp):
    url = f"https://nominatim.openstreetmap.org/search?postalcode={cp}&country=France&format=json"
    try:
        response = requests.get(url, headers={"User-Agent": "geo-script"})
        response.raise_for_status()
        data = response.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"Erreur pour {cp} : {e}")
    return None, None

results = []
for cp in codes_postaux_df["code_postal"].unique():
    lat, lon = get_lat_lon(cp)
    print(f"{cp} => lat: {lat}, lon: {lon}")
    results.append({"code_postal": cp, "lat": lat, "lon": lon})
    time.sleep(1)  # Pause pour ne pas saturer l'API

df_geo = pd.DataFrame(results)
df_geo.to_csv("geolocalisation_postaux.csv", index=False)
print("Fichier geolocalisation_postaux.csv généré.")
