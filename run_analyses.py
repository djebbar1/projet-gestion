# run_analyses.py
import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("data/analyse_ventes.db")
CSV_VENTES = Path("data/ventes.csv")
CSV_PRODUITS = Path("data/produits.csv")
CSV_MAGASINS = Path("data/magasins.csv")
CSV_GEO = Path("data/geolocalisation_postaux.csv")  # optionnel

def run_analyses():
    conn = sqlite3.connect(DB_PATH)

    # --- Lecture CSV/XLSX ---
    df_ventes = pd.read_csv(CSV_VENTES)
    df_produits = pd.read_csv(CSV_PRODUITS)
    df_mag = pd.read_csv(CSV_MAGASINS)

    # Nettoyage et conversion de types
    df_ventes["Date"] = pd.to_datetime(df_ventes["Date"], errors="coerce")
    df_ventes["Quantité"] = pd.to_numeric(df_ventes["Quantité"], errors="coerce")

    df = df_ventes.merge(df_produits, left_on="ID Référence produit", right_on="ID Référence produit", how="left")
    
    # Création de la colonne Total si absente
    if "Total" not in df.columns:
        df["Prix"] = pd.to_numeric(df["Prix"], errors="coerce")
        df["Total"] = df["Quantité"] * df["Prix"]

    # --- Analyses ---
    analyses = []

    # 1. CA par mois
    df["Mois"] = df["Date"].dt.to_period("M").astype(str)
    ca_mois = df.groupby("Mois", as_index=False).agg(valeur=("Total", "sum"))
    ca_mois["type"] = "CA_par_mois"
    ca_mois["categorie"] = None
    ca_mois.rename(columns={"Mois":"periode"}, inplace=True)
    analyses.append(ca_mois)

    # 2. CA par produit
    ca_produit = df.groupby("Nom", as_index=False).agg(valeur=("Total","sum"))
    ca_produit["type"] = "CA_par_produit"
    ca_produit.rename(columns={"Nom":"categorie"}, inplace=True)
    ca_produit["periode"] = None
    analyses.append(ca_produit)

    # 3. Panier moyen
    panier_moyen = pd.DataFrame([{
        "type":"Panier_moyen",
        "categorie":None,
        "valeur": df["Total"].sum() / df["Quantité"].sum() if df["Quantité"].sum() !=0 else 0,
        "periode":None
    }])
    analyses.append(panier_moyen)

    # 4. CA par ville
    df_mag_full = df.merge(df_mag, left_on="ID Magasin", right_on="ID Magasin", how="left")
    ca_ville = df_mag_full.groupby("Ville", as_index=False).agg(valeur=("Total","sum"))
    ca_ville["type"] = "CA_par_ville"
    ca_ville.rename(columns={"Ville":"categorie"}, inplace=True)
    ca_ville["periode"] = None
    analyses.append(ca_ville)

    # 5. Carte géolocalisée (optionnel)
    if CSV_GEO.exists():
        df_geo = pd.read_csv(CSV_GEO)
        df_geo_full = df_mag_full.merge(df_geo, on="Ville", how="left")
        ca_geo = df_geo_full.groupby(["Latitude","Longitude"], as_index=False).agg(valeur=("Total","sum"))
        ca_geo["type"] = "CA_par_geo"
        ca_geo["categorie"] = None
        ca_geo["periode"] = None
        analyses.append(ca_geo)

    # --- Sauvegarde analyses dans SQLite ---
    df_analyses = pd.concat(analyses, ignore_index=True)
    df_analyses.to_sql("analyses", conn, if_exists="replace", index=False)

    conn.close()
    print("✅ Analyses exécutées et sauvegardées dans 'analyses'.")

if __name__ == "__main__":
    run_analyses()
