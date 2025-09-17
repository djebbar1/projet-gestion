-- analyses.sql
-- CA par produit
INSERT INTO analyses (type, categorie, valeur, periode)
SELECT 'CA_par_produit', p.nom, SUM(v.total), NULL
FROM ventes v
JOIN produits p ON v.id_produit = p.id
GROUP BY p.nom
ORDER BY SUM(v.total) DESC;

-- CA par ville
INSERT INTO analyses (type, categorie, valeur, periode)
SELECT 'CA_par_ville', m.ville, SUM(v.total), NULL
FROM ventes v
JOIN magasins m ON v.id_magasin = m.id
GROUP BY m.ville
ORDER BY SUM(v.total) DESC;

-- Evolution journalière
INSERT INTO analyses (type, categorie, valeur, periode)
SELECT 'Evolution_CA', NULL, SUM(v.total), v.date
FROM ventes v
GROUP BY v.date
ORDER BY v.date;

-- Panier moyen
INSERT INTO analyses (type, categorie, valeur, periode)
SELECT 'Panier_moyen', 'Global', SUM(v.total)/COUNT(*), NULL
FROM ventes v;

-- Top 5 produits par quantité
INSERT INTO analyses (type, categorie, valeur, periode)
SELECT 'Top5_produits_qte', p.nom, SUM(v.qte), NULL
FROM ventes v
JOIN produits p ON v.id_produit = p.id
GROUP BY p.nom
ORDER BY SUM(v.qte) DESC
LIMIT 5;

-- CA par magasin
INSERT INTO analyses (type, categorie, valeur, periode)
SELECT 'CA_par_magasin', m.ville, SUM(v.total), NULL
FROM ventes v
JOIN magasins m ON v.id_magasin = m.id
GROUP BY m.ville
ORDER BY SUM(v.total) DESC;

-- Nb de ventes
INSERT INTO analyses (type, categorie, valeur, periode)
SELECT 'Nb_ventes', 'Global', COUNT(*), NULL
FROM ventes;

-- CA dernier mois
INSERT INTO analyses (type, categorie, valeur, periode)
SELECT 'CA_dernier_mois',
       strftime('%Y-%m', MAX(date)),
       SUM(total),
       strftime('%Y-%m', MAX(date))
FROM ventes
WHERE strftime('%Y-%m', date) = strftime('%Y-%m', (SELECT MAX(date) FROM ventes));
