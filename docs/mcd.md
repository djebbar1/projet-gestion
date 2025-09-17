# Schéma relationnel (MCD)

**Table PRODUITS**
- id (PK)
- nom
- reference (unique)
- prix
- stock

**Table MAGASINS**
- id (PK)
- ville
- nb_salaries

**Table VENTES**
- id (PK)
- date
- qte
- total
- id_produit (FK → PRODUITS.id)
- id_magasin (FK → MAGASINS.id)

**Table ANALYSES_RESULTATS** (pour stocker les KPI)
- id (PK)
- type_analyse (texte)
- resultat (texte ou nombre)
- date_calcul
