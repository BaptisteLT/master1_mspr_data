import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, ForeignKey, MetaData, text, insert

# Installation des dépendances: pip install pandas sqlalchemy pymysql

# Connection à MYSQL sans utiliser de base de données
DB_URL_NO_DB = "mysql+pymysql://root:@localhost:3306/"
engine_no_db = create_engine(DB_URL_NO_DB)

# Création de la base de données elections si elle n'existe pas
with engine_no_db.connect() as conn:
    conn.execute(text("CREATE DATABASE IF NOT EXISTS elections;"))
print("✅ Base de données \"elections\" créée avec succès!")

# Connexion à la base de données "elections créée à l'étape précédente"
DB_URL = "mysql+pymysql://root:@localhost:3306/elections"
engine = create_engine(DB_URL)
metadata = MetaData()

# Table "departement"
departement = Table(
    "departement", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("code", String(3), unique=True, nullable=False),
    Column("nom", String(191), unique=True, nullable=False)
)

# Table "elections" (Liée à "departement")
elections = Table(
    "elections", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("annee", Integer, nullable=False),
    Column("departement_id", Integer, ForeignKey("departement.id"), nullable=False),
    Column("moyenne_age", Float, nullable=False),
    Column("moyenne_pouvoir_achat", Float, nullable=False),
    Column("taux_chomage", Float, nullable=True),
    Column("type_de_position", Integer, ForeignKey("type_de_position.id"), nullable=False),
    Column("nom_gagnant", String(100), nullable=False),
    Column("prenom_gagnant", String(100), nullable=False),
    Column("nom_perdant", String(100), nullable=False),
    Column("prenom_perdant", String(100), nullable=False),
    Column("pourcentage_vote_gagnant", Float, nullable=False),
    #Column("pourcentage_vote_perdant", Float, nullable=False),
    Column("pourcentage_vote_blanc", Float, nullable=False),
    Column("pourcentage_abstention", Float, nullable=False),
    Column("temperature_moyenne", Float, nullable=False)
)

# Table "temperature_moyenne" (Table indépendente reliée à aucune autre car ce sont des données généralisées à l'échelle planétaire)
#temperature_moyenne = Table(
#    "temperature_moyenne", metadata,
#    Column("id", Integer, primary_key=True, autoincrement=True),
#    Column("annee", Integer, unique=True, nullable=False),
#    Column("temperature_moyenne", Float, nullable=False)
#)

# Table "type_de_position" (Les valeurs seront: Gauche, droite, milieu)
type_de_position = Table(
    "type_de_position", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("libelle", String(10), unique=True, nullable=False)
)

# Insert values into "type_de_position" table
with engine.connect() as conn:
    # Prepare the insert statement
    insert_stmt = insert(type_de_position).values([
        {"libelle": "gauche"},
        {"libelle": "milieu"},
        {"libelle": "droite"}
    ])
    # Execute the statement
    conn.execute(insert_stmt)

metadata.create_all(engine)
print("✅ Les tables ont été créées avec succès!")
