# streamlit_app.py
import os
import sqlite3
from pathlib import Path
from datetime import datetime, date
import io

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import matplotlib.pyplot as plt
from fpdf import FPDF
from fpdf.fonts import FontFace
from fpdf.enums import XPos, YPos
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# ---------------------- CONFIG ----------------------
SQLITE_PATH = "data/analyse_ventes.db"
MYSQL_USER = "root"          # ton utilisateur MySQL
MYSQL_PASSWORD = ""          # ton mot de passe MySQL
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "analyse_ventes" # nom de la base MySQL

TABLES = ["ventes", "produits", "magasins", "geolocalisation_postaux"]

# ---------------------- SQLITE ----------------------
sqlite_conn = sqlite3.connect(SQLITE_PATH)

# ---------------------- MYSQL ----------------------
mysql_engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/"
)

# Créer la base MySQL si elle n'existe pas
with mysql_engine.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}"))
    print(f"✅ Base {MYSQL_DB} vérifiée/créée.")

# Connexion vers la base spécifique
mysql_engine_db = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

# ---------------------- MIGRATION ----------------------
for table in TABLES:
    try:
        # Lire la table SQLite
        df = pd.read_sql(f"SELECT * FROM {table}", sqlite_conn)
        if df.empty:
            print(f"⚠️ Table {table} vide, rien à migrer.")
            continue

        # Envoyer vers MySQL (remplace si existe déjà)
        df.to_sql(table, mysql_engine_db, if_exists="replace", index=False)
        print(f"✅ Table {table} migrée vers MySQL ({len(df)} lignes).")
    except ProgrammingError as e:
        print(f"❌ Erreur pour {table}: {e}")

sqlite_conn.close()
print("🎉 Migration terminée !")

# PDF (fpdf2)
try:
    from fpdf import FPDF
    FPDF_AVAILABLE = True
except ImportError:
    FPDF_AVAILABLE = False


DB_PATH = "data/analyse_ventes.db"
EXPORT_DIR = "exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

st.set_page_config(page_title="Dashboard Ventes", layout="wide")

# --- Connexion à la base ---
conn = sqlite3.connect(DB_PATH)


conn = sqlite3.connect("data/analyse_ventes.db")
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())

@st.cache_data
def load_analyses():
    df = pd.read_sql_query("SELECT * FROM analyses", conn)
    return df
# ------------------- CONFIG -------------------
st.set_page_config(page_title="Analyse des ventes", layout="wide")
st.title("📊 Analyse des ventes – Suite complète")

DATA_DIR = Path("data")

# ------------------- HELPERS -------------------
def _first_existing(*paths: Path) -> Path | None:
    for p in paths:
        if p and Path(p).exists():
            return Path(p)
    return None

@st.cache_data(ttl=600)
def _read_any(path_or_buffer, sheet: str | None = None) -> pd.DataFrame:
    """Lit CSV ou Excel avec encodage tolérant"""
    if hasattr(path_or_buffer, "read"):
        name = getattr(path_or_buffer, "name", "")
        ext = os.path.splitext(name)[1].lower()
        path_or_buffer.seek(0)
        if ext in [".xlsx", ".xls"]:
            return pd.read_excel(path_or_buffer, sheet_name=sheet)
        return pd.read_csv(path_or_buffer, encoding="utf-8", sep=None, engine="python")
    else:
        path = Path(path_or_buffer)
        if not path.exists():
            raise FileNotFoundError(f"{path} introuvable.")
        if path.suffix.lower() in [".xlsx", ".xls"]:
            return pd.read_excel(path, sheet_name=sheet)
        return pd.read_csv(path, encoding="utf-8", sep=None, engine="python")

def _std_col(col: str) -> str:
    c = str(col).strip().lower()
    for a, b in [("é","e"),("è","e"),("ê","e"),("à","a"),("ï","i"),("â","a")]:
        c = c.replace(a, b)
    c = c.replace(" ", "").replace("_", "")
    return c

def standardize_ventes(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    for c in df.columns:
        sc = _std_col(c)
        if sc in ["date", "datede", "jour", "transactiondate"]:
            mapping[c] = "Date"
        elif sc in ["idreferenceproduit", "idreference", "reference", "referenceproduit", "refproduit"]:
            mapping[c] = "ReferenceProduit"
        elif sc in ["quantite", "qte", "quantity"]:
            mapping[c] = "Quantite"
        elif sc in ["idmagasin", "magasinid", "storeid"]:
            mapping[c] = "IDMagasin"
        elif sc in ["total", "montant", "ca", "chiffredaffaires"]:
            mapping[c] = "Total"
        elif sc in ["prixunitaire", "prixunit", "prixu", "prix"]:
            mapping[c] = "PrixUnitaire"
        elif sc in ["codepostal", "cp", "postalcode"]:
            mapping[c] = "CodePostal"
        elif sc in ["type", "operation", "sens"]:
            mapping[c] = "Type"
        else:
            mapping[c] = c
    return df.rename(columns=mapping)

def standardize_produits(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    for c in df.columns:
        sc = _std_col(c)
        if sc in ["idreferenceproduit", "reference", "referenceproduit"]:
            mapping[c] = "ReferenceProduit"
        elif sc in ["nom", "produit", "designation", "libelle"]:
            mapping[c] = "NomProduit"
        elif sc in ["prix", "prixvente", "pv", "prixu", "prixunitaire"]:
            mapping[c] = "Prix"
        elif sc in ["stock", "qtestock", "inventaire"]:
            mapping[c] = "Stock"
        elif sc in ["cout", "prixachat", "coutunitaire"]:
            mapping[c] = "Cout"
        else:
            mapping[c] = c
    return df.rename(columns=mapping)

def standardize_magasins(df):
    if df is None: return None
    mapping = {}
    for c in df.columns:
        sc = _std_col(c)
        if sc in ["idmagasin","storeid","idmag"]: 
            mapping[c] = "IDMagasin"
        elif sc in ["nommagasin","magasin","nom","name"]:
            mapping[c] = "NomMagasin"
        elif sc in ["ville","city"]: 
            mapping[c] = "Ville"
        elif sc in ["codepostal","cp"]: 
            mapping[c] = "CodePostal"
        elif sc in ["effectif","nbsalaries","nombresalaries","nbsalarie","nbresalaries"]: 
            mapping[c] = "NbSalaries"
        else:
            mapping[c] = c
    df = df.rename(columns=mapping)
    if "NomMagasin" not in df.columns and "IDMagasin" in df.columns:
        df["NomMagasin"] = "Magasin " + df["IDMagasin"].astype(str)
    return df

def standardize_geoloc(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    for c in df.columns:
        sc = _std_col(c)
        if sc in ["codepostal", "cp", "postalcode"]:
            mapping[c] = "CodePostal"
        elif sc in ["ville", "city"]:
            mapping[c] = "Ville"
        elif sc in ["latitude", "lat"]:
            mapping[c] = "Latitude"
        elif sc in ["longitude", "lon", "lng"]:
            mapping[c] = "Longitude"
        else:
            mapping[c] = c
    return df.rename(columns=mapping)

def build_master(ventes: pd.DataFrame, produits: pd.DataFrame | None, magasins: pd.DataFrame | None) -> pd.DataFrame:
    df = ventes.copy()
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Quantite"] = pd.to_numeric(df.get("Quantite", 1), errors="coerce").fillna(0)
    if produits is not None and "ReferenceProduit" in df.columns and "ReferenceProduit" in produits.columns:
        keep = ["ReferenceProduit"]
        for opt in ["NomProduit", "Prix", "Cout", "Stock"]:
            if opt in produits.columns:
                keep.append(opt)
        df = df.merge(produits[keep], on="ReferenceProduit", how="left")
    else:
        df["NomProduit"] = df.get("ReferenceProduit", np.nan)
        df["Prix"] = df.get("Prix", np.nan)
    if "Total" in df.columns and df["Total"].notna().any():
        df["CA"] = pd.to_numeric(df["Total"], errors="coerce")
    else:
        df["CA"] = df.get("Prix", 0) * df["Quantite"]
    if magasins is not None and "IDMagasin" in df.columns and "IDMagasin" in magasins.columns:
        keep_cols = [c for c in ["IDMagasin", "NomMagasin", "Ville", "CodePostal"] if c in magasins.columns]
        df = df.merge(magasins[keep_cols], on="IDMagasin", how="left")
    if "Date" in df.columns:
        df["Jour"] = df["Date"].dt.date
        df["Mois"] = df["Date"].dt.to_period("M").astype(str)
    else:
        df["Jour"], df["Mois"] = np.nan, np.nan
    return df

def euro_safe(s: str) -> str:
    return str(s).replace("€", "EUR")

def ensure_session_key(key, default):
    if key not in st.session_state:
        st.session_state[key] = default

# ------------------- SIDEBAR IMPORT -------------------
st.sidebar.header("📥 Import des données (CSV/XLSX)")
up_ventes = st.sidebar.file_uploader("Fichier ventes", type=["csv","xlsx"], key="up_ventes")
up_produits = st.sidebar.file_uploader("Fichier produits", type=["csv","xlsx"], key="up_produits")
up_magasins = st.sidebar.file_uploader("Fichier magasins", type=["csv","xlsx"], key="up_magasins")
up_geoloc = st.sidebar.file_uploader("Fichier géolocalisation", type=["csv","xlsx"], key="up_geoloc")

def auto_path(basename: str) -> Path | None:
    return _first_existing(DATA_DIR / f"{basename}.csv", DATA_DIR / f"{basename}.xlsx",
                           Path(f"{basename}.csv"), Path(f"{basename}.xlsx"))

path_ventes = up_ventes or auto_path("ventes")
path_produits = up_produits or auto_path("produits")
path_magasins = up_magasins or auto_path("magasins")
path_geoloc = up_geoloc or auto_path("geolocalisation_postaux")

if st.sidebar.button("Charger / recharger les données"):
    try:
        if not path_ventes:
            st.sidebar.error("Le fichier **ventes** est obligatoire.")
        else:
            # --- Lecture et standardisation des fichiers ---
            df_v = _read_any(path_ventes)
            df_v = standardize_ventes(df_v)
            df_p = standardize_produits(_read_any(path_produits)) if path_produits else None
            df_m = standardize_magasins(_read_any(path_magasins)) if path_magasins else None
            df_g = standardize_geoloc(_read_any(path_geoloc)) if path_geoloc else None

            # --- Construction du master avant tout enregistrement ---
            master = build_master(df_v, df_p, df_m)

            # --- Enregistrement dans SQLite ---
            df_v.to_sql("ventes", conn, if_exists="replace", index=False)
            if df_p is not None:
                df_p.to_sql("produits", conn, if_exists="replace", index=False)
            if df_m is not None:
                df_m.to_sql("magasins", conn, if_exists="replace", index=False)
            if df_g is not None:
                df_g.to_sql("geolocalisation_postaux", conn, if_exists="replace", index=False)
            master.to_sql("analyses", conn, if_exists="replace", index=False)
            conn.commit()

            # --- Enregistrement dans MySQL ---
            MYSQL_USER = "root"
            MYSQL_PASSWORD = ""
            MYSQL_HOST = "localhost"
            MYSQL_PORT = 3306
            MYSQL_DB = "analyse_ventes"

            mysql_engine = create_engine(
                f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/"
            )
            with mysql_engine.connect() as conn_mysql:
                conn_mysql.execute(text(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}"))

            mysql_engine_db = create_engine(
                f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
            )

            df_v.to_sql("ventes", mysql_engine_db, if_exists="replace", index=False)
            if df_p is not None:
                df_p.to_sql("produits", mysql_engine_db, if_exists="replace", index=False)
            if df_m is not None:
                df_m.to_sql("magasins", mysql_engine_db, if_exists="replace", index=False)
            if df_g is not None:
                df_g.to_sql("geolocalisation_postaux", mysql_engine_db, if_exists="replace", index=False)
            master.to_sql("analyses", mysql_engine_db, if_exists="replace", index=False)

            # --- Mise à jour du session state ---
            st.session_state.update({
                "ventes": df_v, "produits": df_p,
                "magasins": df_m, "geoloc": df_g,
                "master": master
            })

            st.sidebar.success("✅ Données importées et enregistrées dans SQLite et MySQL")

    except Exception as e:
        st.sidebar.error(f"Erreur d'import : {e}")


# ------------------- SESSION STATE -------------------
ensure_session_key("ventes", None)
ensure_session_key("produits", None)
ensure_session_key("magasins", None)
ensure_session_key("geoloc", None)
ensure_session_key("master", None)

df = st.session_state["master"]
df_p = st.session_state["produits"]
df_m = st.session_state["magasins"]
df_g = st.session_state["geoloc"]

if df is None or df.empty:
    st.info("⚠️ Charge un **fichier ventes** pour continuer.")
    st.stop()

# ------------------- MENU HIERARCHIQUE -------------------
menu1 = st.sidebar.selectbox("Menu principal", ["Import", "Visualisation", "Export", "Export Rapport","SQL"])
menu2, menu3 = None, None

if menu1 == "Visualisation":
    menu2 = st.sidebar.selectbox("Type d'analyse", ["Analyses globales", "Analyses détaillées"])
    if menu2 == "Analyses globales":
        menu3 = st.sidebar.selectbox("Analyse globale", [
            "Vue globale", "Taux de marge", "Panier moyen", "Retours mensuels", "Comparaison périodique"
        ])
    else:
        menu3 = st.sidebar.selectbox("Analyse détaillée", [
            "Par produit", "Par ville / magasin", "Par code postal",
            "Carte géolocalisation", "Stocks par produit", 
            "Stocks par ville / magasin", "Stocks par ville / magasin / ref / prix"
        ])

# ------------------- AFFICHAGE SELON MENU -------------------

if menu1 == "Import":
    st.subheader("📥 Import & vérification des fichiers")

    st.markdown("**Ventes**")
    if st.session_state.get("ventes") is not None:
        st.dataframe(st.session_state["ventes"].head(20), use_container_width=True)
    else:
        st.info("Aucun fichier ventes chargé.")

    st.markdown("**Produits**")
    if st.session_state.get("produits") is not None:
        st.dataframe(st.session_state["produits"].head(20), use_container_width=True)
    else:
        st.warning("Produits optionnel mais recommandé (pour CA & prix).")

    st.markdown("**Magasins**")
    if st.session_state.get("magasins") is not None:
        st.dataframe(st.session_state["magasins"].head(20), use_container_width=True)
    else:
        st.info("Magasins optionnel (nécessaire pour CA par ville/magasin).")

    st.markdown("**Géolocalisation**")
    if st.session_state.get("geoloc") is not None:
        st.dataframe(st.session_state["geoloc"].head(20), use_container_width=True)
    else:
        st.info("Géoloc optionnelle (nécessaire pour carte & code postal).")


elif menu1 == "Visualisation":
    st.subheader("📊 Visualisations")

    if menu3 == "Vue globale":
        st.write("📈 Vue globale des ventes")
        st.dataframe(df.head(20))

    elif menu3 == "Taux de marge":
        st.write("💹 Taux de marge")

        # Vérifier les colonnes et créer des valeurs par défaut si manquantes
        df["Cout"] = df.get("Cout", 0)
        df["CA"] = df.get("CA", 0)
        df["Quantite"] = df.get("Quantite", 1)  # Si tu as besoin de multiplier par quantité

        # Calcul de la marge
        df["Marge"] = df["CA"] - df["Cout"] * df["Quantite"]

        # Agréger par période (Mois) si cette colonne existe
        if "Mois" in df.columns:
            marge_par_mois = df.groupby("Mois")["Marge"].sum().reset_index()

            # Afficher graphique
            fig = px.bar(
                marge_par_mois,
                x="Mois",
                y="Marge",
                text="Marge",
                title="Marge par mois"
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)

            # Afficher tableau
            st.dataframe(marge_par_mois)
        else:
            st.warning("Colonne 'Mois' manquante pour l'agrégation de la marge.")



    elif menu3 == "Panier moyen":
        st.subheader("💳 Panier moyen par magasin / période")

        # --- Agrégation du panier moyen ---
        panier = df.groupby(["IDMagasin", "Mois"])["CA"].mean().reset_index()
    
        # --- Graphique ---
        fig = px.bar(
            panier,
            x="Mois",
            y="CA",
            color="IDMagasin",
            barmode="group",
            text=panier["CA"].round(2),
            labels={"CA": "Panier moyen", "Mois": "Mois", "IDMagasin": "Magasin"},
            title="Panier moyen par magasin et par mois"
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

        # --- Tableau en dessous ---
        st.dataframe(panier)


    elif menu3 == "Retours mensuels":
        st.subheader("↩️ Retours mensuels")

        if df is not None and not df.empty:
            # Copier le df master
            df_ventes = df.copy()

            # Colonnes normalisées selon ton code
            required_cols = ["Date", "Quantite", "ReferenceProduit", "IDMagasin"]
            missing_cols = [c for c in required_cols if c not in df_ventes.columns]

            if missing_cols:
                st.warning(f"Impossible de calculer les retours mensuels (colonnes manquantes : {', '.join(missing_cols)}).")
            else:
                # On considère les retours comme Quantite < 0
                df_retours = df_ventes[df_ventes["Quantite"] < 0].copy()

                if df_retours.empty:
                    st.info("Aucun retour détecté dans les données.")
                else:
                # Extraire le mois
                    df_retours["Date"] = pd.to_datetime(df_retours["Date"], errors="coerce")
                    df_retours["Mois"] = df_retours["Date"].dt.to_period("M").astype(str)

                # Agrégation par mois
                    retours_mensuels = df_retours.groupby("Mois")["Quantite"].sum().reset_index()

                # --- Graphique ---
                    fig = px.line(
                        retours_mensuels,
                        x="Mois",
                        y="Quantite",
                        markers=True,
                        title="Retours mensuels"
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # --- Tableau ---
                    st.dataframe(retours_mensuels)
        else:
            st.warning("Données de ventes manquantes pour calculer les retours mensuels.")



    elif menu3 == "Comparaison périodique":
        st.subheader("🕒 Comparaison périodique")

        # --- Sélection de la période ---
        periode_options = ["Mois", "Trimestre", "Année"]
        periode = st.selectbox("Choisir la période", periode_options, index=0)

        # --- Agrégation selon la période ---
        df_period = df.copy()

        if periode == "Mois":
            df_period["Periode"] = df_period["Mois"]  # supposé que tu as une colonne 'Mois'
        elif periode == "Trimestre":
            if "Date" in df_period.columns:
                df_period["Periode"] = pd.to_datetime(df_period["Date"]).dt.to_period("Q").astype(str)
            else:
                st.warning("Colonne 'Date' manquante pour calculer le trimestre.")
                df_period["Periode"] = df_period.get("Mois", "Unknown")
        elif periode == "Année":
            if "Date" in df_period.columns:
                df_period["Periode"] = pd.to_datetime(df_period["Date"]).dt.year.astype(str)
            else:
                st.warning("Colonne 'Date' manquante pour calculer l'année.")
                df_period["Periode"] = df_period.get("Mois", "Unknown")

        # --- Calcul de la variation en % ---
        df_agg = df_period.groupby("Periode")["CA"].sum().sort_index()
        df_pct = df_agg.pct_change().fillna(0) * 100  # en pourcentage

        # --- Graphique ---
        fig = px.bar(
            x=df_pct.index,
            y=df_pct.values,
            labels={"x": periode, "y": "Variation (%)"},
            text=[f"{v:.2f}%" for v in df_pct.values],
            title=f"Variation du CA par {periode.lower()}"
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

        # --- Tableau ---
        st.dataframe(df_pct.reset_index().rename(columns={"Periode": periode, "CA": "Variation (%)"}))


    elif menu3 == "Par produit":
        st.write("🛍️ Analyse par produit")

        if "NomProduit" in df.columns and "CA" in df.columns:
            # Agrégation du CA par produit
            df_prod = df.groupby("NomProduit", as_index=False)["CA"].sum().sort_values("CA", ascending=False)

            # Graphique en barres
            st.bar_chart(df_prod.set_index("NomProduit")["CA"])

            # Tableau en dessous
            st.dataframe(df_prod)
        else:
            st.info("Colonnes 'NomProduit' ou 'CA' absentes.")


    elif menu3 == "Par ville / magasin":
        st.write("🏙️ Analyse par ville / magasin")

        if "Ville" in df.columns and "NomMagasin" in df.columns and "CA" in df.columns:
            # Agréger CA par Ville et Magasin
            df_ville = df.groupby(["Ville", "NomMagasin"], as_index=False)["CA"].sum()

            # Graphique (barres)
            st.bar_chart(df_ville.set_index("NomMagasin")["CA"])

            # Tableau en dessous
            st.dataframe(df_ville)
        else:
            st.info("Colonnes 'Ville', 'NomMagasin' ou 'CA' absentes.")


    elif menu3 == "Par code postal":
        st.subheader("Par code postal")
        df_cp = df.copy()

        # Harmonisation IDMagasin
        if "IDMagasin" in df_cp.columns:
            df_cp["IDMagasin"] = df_cp["IDMagasin"].astype(str)

        # Récupérer ou créer magasins_geo
        df_magasins = st.session_state.get("magasins", pd.DataFrame())
        df_geo = st.session_state.get("geoloc", pd.DataFrame())

        if not df_magasins.empty:
            # Harmoniser IDMagasin
            if "IDMagasin" in df_magasins.columns:
                df_magasins["IDMagasin"] = df_magasins["IDMagasin"].astype(str)

            # Préparer df_geo
            if not df_geo.empty:
                df_geo = df_geo.rename(columns={"code_postal": "CodePostal", "lat": "Latitude", "lon": "Longitude"})
                df_geo["CodePostal"] = df_geo["CodePostal"].astype(str)
                df_geo["Latitude"] = pd.to_numeric(df_geo["Latitude"], errors="coerce")
                df_geo["Longitude"] = pd.to_numeric(df_geo["Longitude"], errors="coerce")
            else:
                df_geo = pd.DataFrame(columns=["CodePostal", "Latitude", "Longitude"])

            # Coordonnées manquantes
            coords_manquantes = pd.DataFrame([
                {"CodePostal": "59000", "Latitude": 50.6292, "Longitude": 3.0573},
                {"CodePostal": "44000", "Latitude": 47.2184, "Longitude": -1.5536},
                {"CodePostal": "67000", "Latitude": 48.5734, "Longitude": 7.7521},
                {"CodePostal": "33000", "Latitude": 44.8378, "Longitude": -0.5792},
                {"CodePostal": "31000", "Latitude": 43.6047, "Longitude": 1.4442},
            ])
            df_geo = pd.concat([df_geo, coords_manquantes], ignore_index=True)

            # Agrégation par code postal
            df_geo_agg = df_geo.groupby("CodePostal", as_index=False).agg({"Latitude": "mean", "Longitude": "mean"})

            # Correspondance Ville → CodePostal
            correspondance_ville_cp = {
                "Paris": "75001", "Marseille": "13001", "Lyon": "69001",
                "Bordeaux": "33000", "Toulouse": "31000", "Lille": "59000",
                "Nantes": "44000", "Strasbourg": "67000"
            }
            df_magasins["CodePostal"] = df_magasins["Ville"].map(correspondance_ville_cp).astype(str)

            # Merge Magasins + Coordonnées
            magasins_geo = df_magasins.merge(df_geo_agg, on="CodePostal", how="left")
            st.session_state["magasins_geo"] = magasins_geo
        else:
            magasins_geo = pd.DataFrame()

        # Merge pour récupérer CodePostal si absent
        if "CodePostal" not in df_cp.columns or df_cp["CodePostal"].isna().all():
            if not magasins_geo.empty:
                df_cp = df_cp.merge(magasins_geo[["IDMagasin", "CodePostal"]], on="IDMagasin", how="left")

        # Agrégation par CodePostal
        if "CodePostal" in df_cp.columns and df_cp["CodePostal"].notna().any():
            cap = df_cp.groupby("CodePostal", as_index=False)["CA"].sum().sort_values("CA", ascending=False)
            figcp = px.bar(cap.head(40), x="CodePostal", y="CA", text="CA", title="CA par code postal")
            figcp.update_traces(textposition="outside")
            st.plotly_chart(figcp, use_container_width=True)
            st.dataframe(cap, use_container_width=True)
        else:
            st.warning("Impossible de calculer par code postal (fournis magasins et/ou geolocalisation).")
        

    elif menu3 == "Carte géolocalisation":
        st.subheader("🗺️ Carte géolocalisée (OpenStreetMap)")

        # --- Étape 1 : Charger les tables ---
        df_cp = df.copy()  # dataframe principal CA
        df_magasins = st.session_state.get("magasins", pd.DataFrame())
        df_geo = st.session_state.get("geoloc", pd.DataFrame())

        if df_magasins.empty:
            st.info("Aucune donnée magasins disponible pour géolocalisation.")
        else:
            df_magasins = df_magasins.copy()
            if "IDMagasin" in df_magasins.columns:
                df_magasins["IDMagasin"] = df_magasins["IDMagasin"].astype(str)

            # Normaliser géolocalisation
            if not df_geo.empty:
                df_geo = df_geo.rename(columns={
                    "code_postal": "CodePostal",
                    "lat": "Latitude",
                    "lon": "Longitude"
                })
                df_geo["CodePostal"] = df_geo["CodePostal"].astype(str)
                df_geo["Latitude"] = pd.to_numeric(df_geo["Latitude"], errors="coerce")
                df_geo["Longitude"] = pd.to_numeric(df_geo["Longitude"], errors="coerce")
            else:
                df_geo = pd.DataFrame(columns=["CodePostal", "Latitude", "Longitude"])

            # Ajouter coordonnées manuelles pour les villes manquantes
            coords_manquantes = pd.DataFrame([
                {"CodePostal": "59000", "Latitude": 50.6292, "Longitude": 3.0573},   # Lille
                {"CodePostal": "44000", "Latitude": 47.2184, "Longitude": -1.5536}, # Nantes
                {"CodePostal": "67000", "Latitude": 48.5734, "Longitude": 7.7521},  # Strasbourg
                {"CodePostal": "33000", "Latitude": 44.8378, "Longitude": -0.5792}, # Bordeaux
                {"CodePostal": "31000", "Latitude": 43.6047, "Longitude": 1.4442},  # Toulouse
            ])
            df_geo = pd.concat([df_geo, coords_manquantes], ignore_index=True)

            # Agréger par CodePostal
            df_geo_agg = df_geo.groupby("CodePostal", as_index=False).agg({
                "Latitude": "mean",
                "Longitude": "mean"
            })

            # Correspondance Ville → CodePostal
            correspondance_ville_cp = {
                "Paris": "75001", "Marseille": "13001", "Lyon": "69001",
                "Bordeaux": "33000", "Toulouse": "31000", "Lille": "59000",
                "Nantes": "44000", "Strasbourg": "67000"
            }
            # Inverse pour CodePostal → Ville
            cp_to_ville = {v: k for k, v in correspondance_ville_cp.items()}

            # Ajouter CodePostal à df_magasins
            df_magasins["CodePostal"] = df_magasins["Ville"].map(correspondance_ville_cp).astype(str)

            # Merge Magasins + Coordonnées
            magasins_geo = df_magasins.merge(
                df_geo_agg,
                on="CodePostal",
                how="left"
            )
            st.session_state["magasins_geo"] = magasins_geo

            # --- Étape 2 : Créer df_map ---
            if "IDMagasin" in df_cp.columns:
                df_cp["IDMagasin"] = df_cp["IDMagasin"].astype(str)
            if "IDMagasin" in magasins_geo.columns:
                magasins_geo["IDMagasin"] = magasins_geo["IDMagasin"].astype(str)

            df_map = df_cp.merge(
                st.session_state["magasins_geo"][["IDMagasin", "Latitude", "Longitude", "CodePostal", "Ville"]],
                on="IDMagasin",
                how="left"
            )

            # Remplir la colonne 'Ville' si manquante
            if "Ville" not in df_map.columns:
                df_map["Ville"] = df_map["CodePostal"].map(cp_to_ville)
            else:
                df_map["Ville"] = df_map["Ville"].fillna(df_map["CodePostal"].map(cp_to_ville))

            # Supprimer les lignes sans coordonnées
            df_map = df_map.dropna(subset=["Latitude", "Longitude"])

            # --- Étape 3 : Agréger et afficher la carte ---
            if not df_map.empty:
                agg_map = df_map.groupby(["Latitude", "Longitude"], as_index=False).agg({
                    "CA": "sum",
                    "CodePostal": "first",
                    "Ville": "first"
                })

                fig_map = px.scatter_mapbox(
                    agg_map,
                    lat="Latitude",
                    lon="Longitude",
                    size="CA",
                    color="CA",
                    hover_name="Ville",  # Affiche la ville
                    hover_data={"CodePostal": True, "CA": ":,.0f", "Latitude": False, "Longitude": False},
                    zoom=5,
                    height=550
                )
                fig_map.update_layout(
                    mapbox_style="open-street-map",
                    margin=dict(l=0, r=0, t=0, b=0)
                )
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.info("Aucune donnée géolocalisée disponible pour la carte.")


    elif menu3 == "Stocks par produit":
        st.write("📦 Stocks par produit")
        if df_p is not None and "Stock" in df_p.columns:
            # Graphique en barres avec Plotly
            fig_stock = px.bar(
                df_p,
                x="NomProduit",
                y="Stock",
                title="Stocks par produit",
                labels={"NomProduit": "Produit", "Stock": "Stock"}
            )
            st.plotly_chart(fig_stock, use_container_width=True)

            # Tableau juste en dessous
            st.dataframe(df_p)
        else:
            st.info("Colonne 'Stock' ou 'NomProduit' absente dans df_p.")
       

    elif menu3 == "Stocks par ville / magasin":
        st.subheader("🏬 Stocks par ville / magasin") 
        if "Stock" in df.columns and "NomMagasin" in df.columns:
            stock_mag = (
                df.groupby(["Ville", "NomMagasin"], as_index=False)["Stock"]
                .sum()
                .sort_values("Stock", ascending=False)
            )

            # --- Graphique camembert par Ville ---
            stock_ville = stock_mag.groupby("Ville", as_index=False)["Stock"].sum()

            fig_pie = px.pie(
                stock_ville,
                names="Ville",
                values="Stock",
                title="Répartition des stocks par ville",
                hole=0.3  # mettre 0.3 pour donut, 0 pour camembert plein
            )
            fig_pie.update_traces(textinfo="percent+label")

            st.plotly_chart(fig_pie, use_container_width=True)

            # --- Tableau détaillé par magasin ---
            st.dataframe(stock_mag)

        else:
            st.info("Colonne 'Stock' ou 'NomMagasin' absente.")

    # --- Normaliser colonnes ---
        def clean_columns(df):
            df.columns = (
                df.columns
                .str.strip()
                .str.replace(" ", "_")
                .str.replace("é", "e")
                .str.replace("É", "E")
            )
            return df

        ventes = clean_columns(st.session_state.get("ventes", pd.DataFrame()))
        produits = clean_columns(st.session_state.get("produits", pd.DataFrame()))
        magasins = clean_columns(st.session_state.get("magasins", pd.DataFrame()))

    # Fusion cohérente avec les colonnes réelles
        df = ventes.merge(produits, on="ReferenceProduit", how="left")
        df = df.merge(magasins, on="IDMagasin", how="left")


        st.session_state["df_master"] = df

    elif menu3 == "Stocks par ville / magasin / ref / prix":
        st.subheader("🏬 Stocks par ville / magasin / produit")

        ventes = st.session_state.get("ventes", pd.DataFrame())
        produits = st.session_state.get("produits", pd.DataFrame())
        magasins = st.session_state.get("magasins", pd.DataFrame())

        if not ventes.empty and not produits.empty and not magasins.empty:
            # Harmoniser les colonnes
            renames = {
                "ID Référence produit": "ReferenceProduit",
                "ID_Reference_produit": "ReferenceProduit",
                "IDMagasin": "IDMagasin",
                "ID_Magasin": "IDMagasin",
                "Nom": "NomProduit"
            }
            ventes = ventes.rename(columns=renames)
            produits = produits.rename(columns=renames)
            magasins = magasins.rename(columns=renames)

            # Fusion
            df = ventes.merge(produits, on="ReferenceProduit", how="left")
            df = df.merge(magasins, on="IDMagasin", how="left")

            # Vérif colonnes nécessaires
            needed_cols = ["Ville", "IDMagasin", "NomProduit", "ReferenceProduit", "Prix", "Stock"]
            if all(c in df.columns for c in needed_cols):
                stock_table = df.groupby(
                    ["Ville", "IDMagasin", "NomProduit", "ReferenceProduit", "Prix"],
                    as_index=False
                )["Stock"].sum()

                # Graph camembert par ville
                fig = px.pie(stock_table, names="Ville", values="Stock", title="Répartition des stocks par ville")
                st.plotly_chart(fig, use_container_width=True)

                # Tableau détaillé
                st.dataframe(stock_table)
            else:
                st.warning(f"Impossible d'afficher : colonnes manquantes ({set(needed_cols) - set(df.columns)})")
        else:
            st.warning("Impossible d'afficher : données manquantes (ventes, produits ou magasins vides).")

# ------------------- SECTION EXPORT (remplacer la section Export existante) -------------------
elif menu1 == "Export":
    st.subheader("📤 Export des analyses")

    #df = st.session_state.df
    df_p = st.session_state.produits
    df_m = st.session_state.magasins
    df_g = st.session_state.geoloc

    if df.empty:
        st.warning("📁 Importer des données avant d’exporter.")
    else:
        # CSV
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Export CSV", data=csv_bytes, file_name="master.csv", mime="text/csv")

        # Excel
        excel_buf = io.BytesIO()
        with pd.ExcelWriter(excel_buf, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="master")
            if not df_p.empty:
                df_p.to_excel(writer, index=False, sheet_name="produits")
            if not df_m.empty:
                df_m.to_excel(writer, index=False, sheet_name="magasins")
            if not df_g.empty:
                df_g.to_excel(writer, index=False, sheet_name="geoloc")
        excel_buf.seek(0)
        st.download_button("📥 Export Excel", data=excel_buf.getvalue(), file_name="analyses.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # PDF simple
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, "Synthèse analyses", ln=True, align="C")
        pdf.ln(5)
        pdf.set_font("Helvetica", size=9)
        for _, row in df.head(20).iterrows():
            txt = " | ".join(str(v) for v in row.values)
            pdf.cell(0, 6, txt, ln=True)
        pdf_bytes = bytes(pdf.output(dest="S"))
        st.download_button("📥 Export PDF", data=pdf_bytes, file_name="synthese.pdf", mime="application/pdf")

# ---------------- PDF complet (TOUS les graphiques) ----------------

elif menu1 == "Export Rapport":
    st.subheader("📤 Export des analyses - complet")
    col1 = st.columns(1)[0]    
    if FPDF_AVAILABLE:
        try:
            # Préparer images (liste de (title, bytesIO))
            images = []

            def save_matplotlib_plot(fig):
                buf = io.BytesIO()
                fig.savefig(buf, format="png", bbox_inches="tight", dpi=160)
                plt.close(fig)
                buf.seek(0)
                return buf

            # 1) CA mensuel
            if "Mois" in df.columns and "CA" in df.columns:
                ca_mois = df.groupby("Mois", as_index=False)["CA"].sum().sort_values("Mois")
                fig, ax = plt.subplots(figsize=(10,3.5))
                ax.plot(ca_mois["Mois"], ca_mois["CA"], marker="o")
                ax.set_title("CA mensuel")
                ax.tick_params(axis='x', rotation=45)
                images.append(("CA mensuel", save_matplotlib_plot(fig)))

            # 2) Marge par mois (si possible)
            if "Cout" in df.columns or "Quantite" in df.columns:
                try:
                    df_tmp = df.copy()
                    df_tmp["Cout"] = df_tmp.get("Cout", 0)
                    df_tmp["Quantite"] = df_tmp.get("Quantite", 1)
                    df_tmp["Marge"] = df_tmp["CA"] - df_tmp["Cout"] * df_tmp["Quantite"]
                    if "Mois" in df_tmp.columns:
                        marge_par_mois = df_tmp.groupby("Mois", as_index=False)["Marge"].sum().sort_values("Mois")
                        fig, ax = plt.subplots(figsize=(10,3.0))
                        ax.bar(marge_par_mois["Mois"], marge_par_mois["Marge"])
                        ax.set_title("Marge par mois")
                        ax.tick_params(axis='x', rotation=45)
                        images.append(("Marge par mois", save_matplotlib_plot(fig)))
                except Exception:
                    pass

            # 3) Panier moyen par magasin (agrégé)
            if "IDMagasin" in df.columns and "CA" in df.columns:
                panier_mag = df.groupby("IDMagasin", as_index=False)["CA"].mean().sort_values("CA", ascending=False).head(15)
                fig, ax = plt.subplots(figsize=(10,3.5))
                ax.bar(panier_mag["IDMagasin"].astype(str), panier_mag["CA"])
                ax.set_title("Panier moyen par magasin (top 15)")
                ax.set_xlabel("IDMagasin")
                ax.tick_params(axis='x', rotation=45)
                images.append(("Panier moyen par magasin", save_matplotlib_plot(fig)))

            # 4) Retours mensuels (Quantite < 0)
            if "Quantite" in df.columns and "Date" in df.columns:
                df_ret = df[df["Quantite"] < 0].copy()
                if not df_ret.empty:
                    df_ret["Date"] = pd.to_datetime(df_ret["Date"], errors="coerce")
                    df_ret["Mois"] = df_ret["Date"].dt.to_period("M").astype(str)
                    retours = df_ret.groupby("Mois", as_index=False)["Quantite"].sum().sort_values("Mois")
                    fig, ax = plt.subplots(figsize=(10,3.0))
                    ax.plot(retours["Mois"], retours["Quantite"], marker="o")
                    ax.set_title("Retours mensuels (Quantite < 0)")
                    ax.tick_params(axis='x', rotation=45)
                    images.append(("Retours mensuels", save_matplotlib_plot(fig)))

            # 5) Top produits CA
            if "NomProduit" in df.columns and "CA" in df.columns:
                prod_ca = df.groupby("NomProduit", as_index=False)["CA"].sum().sort_values("CA", ascending=False).head(15)
                fig, ax = plt.subplots(figsize=(10,4))
                ax.barh(prod_ca["NomProduit"][::-1], prod_ca["CA"][::-1])
                ax.set_title("Top produits par CA (top 15)")
                images.append(("Top produits CA", save_matplotlib_plot(fig)))

            # 6) CA par ville / magasin
            if {"Ville","NomMagasin","CA"}.issubset(df.columns):
                ville_mag = df.groupby(["Ville","NomMagasin"], as_index=False)["CA"].sum().sort_values("CA", ascending=False).head(30)
                # graphique: CA par magasin (top 20)
                top_mag = ville_mag.groupby("NomMagasin", as_index=False)["CA"].sum().sort_values("CA", ascending=False).head(20)
                fig, ax = plt.subplots(figsize=(10,4))
                ax.bar(top_mag["NomMagasin"], top_mag["CA"])
                ax.set_title("CA par magasin (top 20)")
                ax.tick_params(axis='x', rotation=45)
                images.append(("CA par magasin", save_matplotlib_plot(fig)))

            # 7) CA par code postal (si possible)
            df_cp_tmp = df.copy()
            if "CodePostal" in df_cp_tmp.columns and df_cp_tmp["CodePostal"].notna().any():
                cap = df_cp_tmp.groupby("CodePostal", as_index=False)["CA"].sum().sort_values("CA", ascending=False).head(30)
                fig, ax = plt.subplots(figsize=(10,4))
                ax.bar(cap["CodePostal"].astype(str), cap["CA"])
                ax.set_title("CA par CodePostal (top 30)")
                ax.tick_params(axis='x', rotation=45)
                images.append(("CA par code postal", save_matplotlib_plot(fig)))

            # 8) Stocks par produit (depuis df_p si dispo)
            if df_p is not None and not df_p.empty and "NomProduit" in df_p.columns and "Stock" in df_p.columns:
                stock_prod = df_p.sort_values("Stock", ascending=False).head(20)
                fig, ax = plt.subplots(figsize=(10,4))
                ax.barh(stock_prod["NomProduit"][::-1], stock_prod["Stock"][::-1])
                ax.set_title("Stocks par produit (top 20)")
                images.append(("Stocks par produit", save_matplotlib_plot(fig)))

            # 9) Stocks par ville / magasin (depuis master si Stock présent)
            if "Stock" in df.columns and {"Ville","NomMagasin"}.issubset(df.columns):
                stock_mag = df.groupby(["Ville","NomMagasin"], as_index=False)["Stock"].sum().sort_values("Stock", ascending=False).head(30)
                top_by_mag = stock_mag.groupby("NomMagasin", as_index=False)["Stock"].sum().sort_values("Stock", ascending=False).head(20)
                fig, ax = plt.subplots(figsize=(10,4))
                ax.bar(top_by_mag["NomMagasin"], top_by_mag["Stock"])
                ax.set_title("Stocks par magasin (top 20)")
                ax.tick_params(axis='x', rotation=45)
                images.append(("Stocks par magasin", save_matplotlib_plot(fig)))

            # 10) Stocks détaillés par ref/prix (si master contient ReferenceProduit/Prix/Stock)
            if {"ReferenceProduit","NomProduit","Prix","Stock"}.issubset(df.columns):
                stock_prod_d = df.groupby(["ReferenceProduit","NomProduit","Prix"], as_index=False)["Stock"].sum().sort_values("Stock", ascending=False).head(50)
                fig, ax = plt.subplots(figsize=(10,4))
                ax.barh(stock_prod_d["NomProduit"][::-1], stock_prod_d["Stock"][::-1])
                ax.set_title("Stocks détaillés par produit")
                images.append(("Stocks détaillés", save_matplotlib_plot(fig)))

                        # 11) Carte: scatter lat/lon (si magasins_geo dans session ou df_g present)
            magasins_geo = st.session_state.get("magasins_geo", pd.DataFrame())
            if magasins_geo.empty and not df_m is None and not df_g is None:
                try:
                    mg = df_m.copy()
                    mg["IDMagasin"] = mg["IDMagasin"].astype(str)
                    gg = df_g.copy()
                    gg = gg.rename(columns={"lat": "Latitude", "lon": "Longitude"})
                    gg["CodePostal"] = gg["CodePostal"].astype(str)
                    gg["Latitude"] = pd.to_numeric(gg["Latitude"], errors="coerce")
                    gg["Longitude"] = pd.to_numeric(gg["Longitude"], errors="coerce")
                    gg_agg = gg.groupby("CodePostal", as_index=False).agg(
                        {"Latitude": "mean", "Longitude": "mean"}
                    )
                    magasins_geo = mg.merge(gg_agg, on="CodePostal", how="left")
                except Exception:
                    magasins_geo = pd.DataFrame()

            if not magasins_geo.empty and {"Latitude", "Longitude", "IDMagasin"}.issubset(magasins_geo.columns) and "IDMagasin" in df.columns:
                try:
                    df_map = df.merge(
                        magasins_geo[["IDMagasin", "Latitude", "Longitude", "Ville"]],
                        on="IDMagasin", how="left"
                    ).dropna(subset=["Latitude", "Longitude"])
                    if not df_map.empty and "CA" in df_map.columns:
                        agg_map = df_map.groupby(
                            ["Latitude", "Longitude"], as_index=False
                        ).agg({"CA": "sum", "Ville": "first"})
                        fig, ax = plt.subplots(figsize=(8, 6))
                        ax.scatter(
                            agg_map["Longitude"], agg_map["Latitude"],
                            s=(agg_map["CA"].fillna(0) / agg_map["CA"].max() * 200) + 20,
                            alpha=0.6
                        )
                        for i, r in agg_map.iterrows():
                            ax.text(r["Longitude"], r["Latitude"], str(r.get("Ville", "")), fontsize=8)
                        ax.set_title("Points magasins (Longitude / Latitude) - taille ~ CA")
                        ax.set_xlabel("Longitude")
                        ax.set_ylabel("Latitude")
                        images.append(("Carte magasins (scatter)", save_matplotlib_plot(fig)))
                except Exception:
                    pass

            # ---------------- Construction du PDF ----------------
            pdf_full = FPDF(orientation="P", unit="mm", format="A4")
            pdf_full.set_auto_page_break(auto=True, margin=12)
            pdf_full.add_page()

            pdf_full.add_font("DejaVu", "", "fonts/DejaVuSans.ttf")
            pdf_full.add_font("DejaVu", "B", "fonts/DejaVuSans-Bold.ttf")

            pdf_full.set_font("DejaVu", "B", 16)
            pdf_full.cell(
                0, 10, "Export complet : toutes les analyses",
                new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C"
            )
            pdf_full.ln(4)
            pdf_full.set_font("DejaVu", size=11)

            # Insertion des graphiques
            for title, img_buf in images:
                pdf_full.set_font("DejaVu", "B", 12)
                safe_title = (title[:70] + "...") if len(title) > 70 else title
                pdf_full.cell(0, 6, safe_title, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                try:
                    pdf_full.image(img_buf, x=15, w=170)  # largeur réduite
                except Exception:
                    pdf_full.set_font("DejaVu", size=10)
                    pdf_full.cell(0, 6, "(Image introuvable)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                pdf_full.ln(8)

                        # Tableaux clés
            pdf_full.add_page()
            pdf_full.set_font("DejaVu", "B", 12)
            pdf_full.cell(0, 8, "Tableaux clés (extraits)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            pdf_full.ln(2)

            # Top 10 CA produits
            if "NomProduit" in df.columns and "CA" in df.columns:
                prod_ca = (
                    df.groupby("NomProduit", as_index=False)["CA"]
                    .sum()
                    .sort_values("CA", ascending=False)
                    .head(10)
                )
                pdf_full.set_font("DejaVu", size=9)
                pdf_full.cell(0, 6, "Top 10 produits (CA):", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                for _, r in prod_ca.iterrows():
                    nom = str(r["NomProduit"])[:30]  # limite le nom à 30 caractères
                    pdf_full.cell(90, 5, nom)        # première colonne large
                    pdf_full.cell(40, 5, f"{r['CA']:.2f} EUR", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                pdf_full.ln(2)

            # Stocks par magasin
            if "Stock" in df.columns and {"Ville", "NomMagasin"}.issubset(df.columns):
                stock_mag = (
                    df.groupby(["Ville", "NomMagasin"], as_index=False)["Stock"]
                    .sum()
                    .sort_values("Stock", ascending=False)
                    .head(20)
                )
                pdf_full.set_font("DejaVu", size=9)
                pdf_full.cell(0, 6, "Stocks par magasin (top 20):", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                for _, r in stock_mag.iterrows():
                    nom = str(r["NomMagasin"])[:25]
                    pdf_full.cell(80, 5, nom)
                    pdf_full.cell(30, 5, str(r["Stock"]), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                pdf_full.ln(2)

            # Stocks détaillés par ref/prix
            if {"ReferenceProduit", "NomProduit", "Prix", "Stock"}.issubset(df.columns):
                stock_prod_d = (
                    df.groupby(["ReferenceProduit", "NomProduit", "Prix"], as_index=False)["Stock"]
                    .sum()
                    .sort_values("Stock", ascending=False)
                    .head(50)
                )
                pdf_full.set_font("DejaVu", size=8)
                pdf_full.cell(0, 6, "Stocks détaillés par produit:", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                for _, r in stock_prod_d.iterrows():
                    ref = str(r["ReferenceProduit"])[:8]
                    nom = str(r["NomProduit"])[:20]
                    prix = f"{r['Prix']:.2f}"
                    stock = str(r["Stock"])
                    pdf_full.cell(20, 5, ref)
                    pdf_full.cell(50, 5, nom)
                    pdf_full.cell(20, 5, prix)
                    pdf_full.cell(20, 5, stock, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                pdf_full.ln(2)


            # Finalisation
            pdf_bytes_full = bytes(pdf_full.output(dest="S"))

            col1.download_button(
                "📥 Export PDF (complet)",
                data=pdf_bytes_full,
                file_name="export_complet.pdf",
                mime="application/pdf",
                key="exp_pdf_full"
            )

        except Exception as e:
            col1.warning(f"PDF complet indisponible : {e}")
    else:
        col1.info("Module PDF (fpdf2) non installé – `pip install fpdf2` pour activer l'export PDF.")
# ------------------- FIN SECTION EXPORT -------------------


elif menu1 == "SQL":
    st.subheader("Console SQL (SQLite en mémoire)")
    st.caption("Tables disponibles : **ventes**, **produits**, **magasins**, **geoloc**, **master**")
    q = st.text_area("Requête SQL", "SELECT COUNT(*) AS n, SUM(CA) AS ca FROM master;", height=120)
    if st.button("Exécuter la requête", key="run_sql"):
        try:
            conn = sqlite3.connect(":memory:")
            if st.session_state["ventes"] is not None:
                st.session_state["ventes"].to_sql("ventes", conn, index=False, if_exists="replace")
            if df_p is not None:
                df_p.to_sql("produits", conn, index=False, if_exists="replace")
            if df_m is not None:
                df_m.to_sql("magasins", conn, index=False, if_exists="replace")
            if df_g is not None:
                df_g.to_sql("geoloc", conn, index=False, if_exists="replace")
            df.to_sql("master", conn, index=False, if_exists="replace")

            out = pd.read_sql_query(q, conn)
            conn.close()
            st.dataframe(out, use_container_width=True)
        except Exception as e:
            st.error(f"Erreur SQL : {e}")

