# import_donnees.py
import sqlite3
import pandas as pd
import hashlib
import os

DB_PATH = os.path.join("data", "analyse_ventes.db")
os.makedirs("data", exist_ok=True)

CSV_PRODUITS = os.path.join("data", "produits.csv")
CSV_MAGASINS = os.path.join("data", "magasins.csv")
CSV_VENTES = os.path.join("data", "ventes.csv")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Création tables
cur.execute("""
CREATE TABLE IF NOT EXISTS produits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT,
    reference TEXT UNIQUE,
    prix REAL,
    stock INTEGER
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS magasins (
    id INTEGER PRIMARY KEY,
    ville TEXT,
    nb_salaries INTEGER
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS ventes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    qte INTEGER,
    id_produit INTEGER,
    id_magasin INTEGER,
    total REAL,
    hash_unique TEXT UNIQUE,
    FOREIGN KEY (id_produit) REFERENCES produits(id),
    FOREIGN KEY (id_magasin) REFERENCES magasins(id)
);
""")

# Import Produits
df_prod = pd.read_csv(CSV_PRODUITS)
for _, row in df_prod.iterrows():
    cur.execute("""
        INSERT OR IGNORE INTO produits (nom, reference, prix, stock)
        VALUES (?, ?, ?, ?)
    """, (row["Nom"], row["ID Référence produit"], row["Prix"], row["Stock"]))

# Import Magasins
df_mag = pd.read_csv(CSV_MAGASINS)
for _, row in df_mag.iterrows():
    cur.execute("""
        INSERT OR IGNORE INTO magasins (id, ville, nb_salaries)
        VALUES (?, ?, ?)
    """, (row["ID Magasin"], row["Ville"], row["Nombre de salariés"]))

# Import Ventes
df_ventes = pd.read_csv(CSV_VENTES, parse_dates=["Date"])
for _, row in df_ventes.iterrows():
    cur.execute("SELECT id FROM produits WHERE reference = ?", (row["ID Référence produit"],))
    prod_id = cur.fetchone()
    if not prod_id:
        continue
    prod_id = prod_id[0]

    total = row["Quantité"] * row.get("Prix", 1)  # calcul si pas de colonne 'Total'

    hash_val = hashlib.sha256(
        f"{row['Date']}|{row['ID Référence produit']}|{row['ID Magasin']}|{row['Quantité']}|{total}".encode()
    ).hexdigest()

    cur.execute("""
        INSERT OR IGNORE INTO ventes (date, qte, id_produit, id_magasin, total, hash_unique)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (row["Date"].strftime("%Y-%m-%d"), row["Quantité"], prod_id, row["ID Magasin"], total, hash_val))

conn.commit()
conn.close()
print("✅ Import terminé, ventes, produits et magasins remplis.")
