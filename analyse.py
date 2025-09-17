import pandas as pd
import sqlite3
import os

DB_PATH = "data/data.db"
EXPORT_DIR = "exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

class AnalyseVentes:
    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.df = None

    def charger_donnees(self):
        query = """
            SELECT v.date, v.qte, v.total, p.nom AS produit, m.nom AS magasin, m.ville
            FROM ventes v
            JOIN produits p ON v.id_produit = p.id
            JOIN magasins m ON v.id_magasin = m.id
        """
        self.df = pd.read_sql_query(query, self.conn, parse_dates=['date'])

    def stats_globales(self):
        df = pd.DataFrame({
            "CA_total": [self.df["total"].sum()],
            "Quantite_totale": [self.df["qte"].sum()]
        })
        df.to_csv(os.path.join(EXPORT_DIR, "stats_globales.csv"), index=False)
        return df

    def top_produits(self, n=5):
        top = self.df.groupby("produit")["total"].sum().sort_values(ascending=False).head(n)
        top.to_csv(os.path.join(EXPORT_DIR, f"top_{n}_produits.csv"))
        return top

    def ca_par_ville(self):
        ca = self.df.groupby("ville")["total"].sum().sort_values(ascending=False)
        ca.to_csv(os.path.join(EXPORT_DIR, "ca_par_ville.csv"))
        return ca

    def evolution_journaliere(self):
        evo = self.df.groupby("date")["total"].sum()
        evo.to_csv(os.path.join(EXPORT_DIR, "evolution_journaliere.csv"))
        return evo

if __name__ == "__main__":
    analyse = AnalyseVentes()
    analyse.charger_donnees()
    print("✅ Résultats exportés dans 'exports/'\n")
    print("Stats globales :\n", analyse.stats_globales())
    print("\nTop 5 produits :\n", analyse.top_produits())
    print("\nCA par ville :\n", analyse.ca_par_ville())
    print("\nÉvolution journalière :\n", analyse.evolution_journaliere())
