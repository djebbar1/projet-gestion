# Architecture des services

            +---------------------+
            |   Service "app"     |
            | (Python + Streamlit)|
            |---------------------|
            | - Import CSV        |
            | - Analyses SQL      |
            | - Affichage         |
            +----------+----------+
                       |
                       | accès DB (SQLite)
                       v
            +---------------------+
            |   Service "db"      |
            | (SQLite3 container) |
            |---------------------|
            | - Stockage données  |
            | - Requêtes SQL      |
            +---------------------+

- Port exposé : `8501` (Streamlit → navigateur web)
- Communication interne : `app` <-> `db` (via volume partagé `./data`)
