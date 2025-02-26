import os
import pandas as pd
import random
import numpy as np
import unicodedata
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, ForeignKey, MetaData, text

# Database connection
DB_URL = "mysql+pymysql://root:@localhost:3306/elections"
engine = create_engine(DB_URL)
metadata = MetaData()
metadata.reflect(bind=engine)

# Load tables
departement = metadata.tables["departement"]
elections = metadata.tables["elections"]

#TODO: à mettre dans le doc: 
# Départements et vos associés au département à ne pas insérer en BDD car il manque des données du chômage par exemple (pour MAYOTTE)
departements_to_ignore = {"MAYOTTE", "FRANÇAIS ÉTABLIS HORS DE FRANCE", "NOUVELLE CALEDONIE", "SAINT-MARTIN/SAINT-BARTHÉLEMY", "SAINT-PIERRE-ET-MIQUELON", "POLYNESIE FRANCAISE","POLYNÉSIE FRANÇAISE", "NOUVELLE-CALÉDONIE",  "SAINT PIERRE ET MIQUELON", "SAINT-MARTIN/SAINT-BARTHELEMY", "WALLIS ET FUTUNA", "WALLIS-ET-FUTUNA", "FRANCAIS DE L'ETRANGER"}


# Effacer toutes les tables au début du script
def truncate_tables(database_url, tables):
    """
    Truncate the given list of tables in the specified database.

    :param database_url: str - The SQLAlchemy database connection URL.
    :param tables: list - A list of table names to truncate.
    """
    engine = create_engine(database_url)
    
    with engine.connect() as connection:
        for table in tables:
            connection.execute(text(f"TRUNCATE TABLE {table}"))
        connection.commit()

    print(f"Truncated tables: {', '.join(tables)}")

def convert_to_csv(file_path):
    """Convert Excel files to CSV format and save in ./data/votes/csvs."""
    csv_folder = os.path.join(os.path.dirname(file_path), "csvs")
    os.makedirs(csv_folder, exist_ok=True)  # Ensure folder exists

    file_name = os.path.basename(file_path).replace(".xlsx", ".csv").replace(".xls", ".csv")
    csv_path = os.path.join(csv_folder, file_name)

    try:
        if file_path.endswith(".xlsx"):
            xls = pd.ExcelFile(file_path, engine="openpyxl")
        elif file_path.endswith(".xls"):
            xls = pd.ExcelFile(file_path, engine="xlrd")
        else:
            print(f"⚠️ Not an Excel file: {file_path}")
            return None

        # Print all available sheet names for debugging
        print(f"📄 Available sheets in {file_name}: {xls.sheet_names}")

        # Updated sheet name check
        sheet_name = None
        if "Résultats par niveau Dpt T2 Fra" in xls.sheet_names:
            sheet_name = "Résultats par niveau Dpt T2 Fra"
        elif "Départements Tour 2" in xls.sheet_names:
            sheet_name = "Départements Tour 2"
        elif "Départements T2" in xls.sheet_names:
            sheet_name = "Départements T2"

        if not sheet_name:
            print(f"❌ No matching sheet found in {file_name}.")
            return None

        # Read the correct sheet
        df = pd.read_excel(xls, sheet_name=sheet_name, engine="openpyxl" if file_path.endswith(".xlsx") else "xlrd")
        df.to_csv(csv_path, index=False, sep=';')

        print(f"✅ Converted {file_name} to CSV: {csv_path}")
        return csv_path

    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")
 
    return None



def get_average_age(year):
    # Load age data
    age_file = "./data/donnees_croisees/age_population.csv"  # Adjust the path if needed
    age_df = pd.read_csv(age_file, sep=",", dtype=str)  # Load CSV as strings
    age_df["Année"] = age_df["Année"].astype(int)  # Convert "Année" to integer
    age_df["Âge moyen Ensemble"] = age_df["Âge moyen Ensemble"].str.replace(",", ".").astype(float)  # Convert to float

    """Retrieve the average age for a given year."""
    row = age_df[age_df["Année"] == year]
    if row.empty:
        raise ValueError(f"No age data found for year {year}. Stopping execution.")

    return row["Âge moyen Ensemble"].values[0]

def get_average_temperature(year):
    print('year')
    print(year)
    # Load age data
    age_file = "./data/donnees_croisees/rechauffement_planete.csv"  # Adjust the path if needed
    age_df = pd.read_csv(age_file, sep=",", dtype=str)  # Load CSV as strings
    age_df["Year"] = age_df["Year"].astype(int)  # Convert "Year" to integer
    age_df["J-D"] = (
        age_df["J-D"]
        .astype(str)  # Ensure it's treated as a string
        .str.replace(",", ".")  # Replace commas with dots
        .replace(r"[^\d\.\-]", "", regex=True)  # Remove invalid characters but keep negatives
        .replace(r"^\.+$", "0", regex=True)  # Replace isolated dots with 0
        .replace("", "0")  # Replace empty strings with "0"
        .astype(float)  # Convert to float
    )

    """Retrieve the average age for a given year."""
    row = age_df[age_df["Year"] == year]
    if row.empty:
        raise ValueError(f"No age data found for year {year}. Stopping execution.")

    return row["J-D"].values[0]


#TODO: la fonction est optimisable car la même que get_average_age
def get_moyenne_pouvoir_achat(year):
    # Load age data
    age_file = "./data/donnees_croisees/pouvoir_achat.csv"  # Adjust the path if needed
    age_df = pd.read_csv(age_file, sep=",", dtype=str)  # Load CSV as strings
    age_df["Année"] = age_df["Année"].astype(int)  # Convert "Année" to integer
    age_df["Pouvoir d'achat arbitrable2 (par rapport à l'année précédente en %)"] = age_df["Pouvoir d'achat arbitrable2 (par rapport à l'année précédente en %)"].str.replace(",", ".").astype(float)  # Convert to float

    """Retrieve the average age for a given year."""
    row = age_df[age_df["Année"] == year]
    if row.empty:
        raise ValueError(f"No age data found for year {year} for moyenne_pouvoir_achat. Stopping execution.")

    return row["Pouvoir d'achat arbitrable2 (par rapport à l'année précédente en %)"].values[0]




# Charger les données
chomage_file = "./data/donnees_croisees/chomage.csv"  # Ajuste le chemin si nécessaire
chomage_df = pd.read_csv(chomage_file, sep=",", dtype=str)  # Charger le CSV en chaînes

# Convertir en float uniquement si ce sont des chaînes
def safe_replace(value):
    if isinstance(value, str):
        return float(value.replace(",", "."))
    return value

chomage_df.iloc[:, 2:] = chomage_df.iloc[:, 2:].map(safe_replace)

# Transformer les données pour faciliter l'accès
chomage_df = chomage_df.melt(id_vars=["Code", "Libellé"], var_name="Période", value_name="Chômage")
# Remplacer tous les "-" par des espaces dans la colonne "Libellé"
chomage_df["Libellé"] = chomage_df["Libellé"].str.replace("-", " ")
chomage_df["Trimestre"] = chomage_df["Période"].apply(lambda x: x.split("_")[0])
chomage_df["Année"] = chomage_df["Période"].apply(lambda x: int(x.split("_")[1]))
chomage_df.drop(columns=["Période"], inplace=True)

def remove_accents(text):
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')


def get_unemployment_rate(departement, year):
    """Retourne la moyenne annuelle du taux de chômage pour un département donné.
    Si aucune donnée n'est trouvée pour l'année demandée, retourne la première année avec une valeur non nulle.
    """
    departement = remove_accents(departement.replace("-", " "))

    corrections = {
        "CORSE SUD": "CORSE DU SUD", 
    }

    departement = corrections.get(departement, departement)

    # Filtrer pour l'année demandée
    rows = chomage_df[(chomage_df["Libellé"] == departement) & (chomage_df["Année"] == year)]
    
    if not rows["Chômage"].dropna().empty:
        return rows["Chômage"].mean()

    #TODO: préciser dans le doc final que l'on prend la première valeur de chômage trouvée pour la ligne correspondante si vide.
    # Si aucune donnée pour l'année demandée, chercher la première année avec une valeur
    first_valid_year = chomage_df[(chomage_df["Libellé"] == departement) & chomage_df["Chômage"].notna()].groupby("Année").first().reset_index()


    if not first_valid_year.empty:
        return first_valid_year.iloc[0]["Chômage"]

    raise ValueError(f"Aucune donnée trouvée pour {departement}.")





def process_vote_files():

    TABLES_TO_TRUNCATE = ["departement", "elections", "type_de_position"]

    truncate_tables(DB_URL, TABLES_TO_TRUNCATE)


    """Process all vote files in ./data/votes."""
    folder = "./data/votes"
    csv_folder = os.path.join(folder, "csvs")
    os.makedirs(csv_folder, exist_ok=True)  # Ensure folder exists

    for file in os.listdir(folder):
        file_path = os.path.join(folder, file)

        # Convert to CSV if necessary
        if file.endswith(".xlsx") or file.endswith(".xls"):
            file_path = convert_to_csv(file_path)
            print(file_path)
        
        if file_path and file_path.endswith(".csv"):
            annee = int(file.split("_")[-1].split(".")[0])
            # Read the file without assuming the header row
            df_raw = pd.read_csv(file_path, sep=';', header=None, dtype=str)

            # On trouve le header en utilisant la première colonne "Code du département", car certains fichiers ont le header en ligne 1 ou 4
            header_row_index = df_raw[df_raw.eq("Code du département").any(axis=1)].index[0]

            # Read the file again using the correct header row
            df = pd.read_csv(file_path, sep=';', header=header_row_index, dtype=str)

            # Show the first rows to verify
            #print(df.head())
            with engine.begin() as conn:  # Gestion de transaction
                for _, row in df.iterrows():  # Parcours des lignes du fichier CSV
                    departement_nom = row.get("Libellé du département", "Unknown")
                    departement_code = row.get("Code du département", "Unknown")

                    # Vérifie que les valeurs sont bien des chaînes de caractères
                    if not isinstance(departement_nom, str) or not isinstance(departement_code, str):
                        continue  # Passe à la ligne suivante si une des valeurs n'est pas une chaîne

                    departement_nom = departement_nom.strip().upper()
                    departement_code = departement_code.strip().upper()
                                    
                    print(departement_nom)
                    if departement_nom in departements_to_ignore:
                        # Process the department
                        print(f"Manually ignoring {departement_nom}")
                        continue  # Skip this iteration

                    # **Ignore rows where department fields are missing**
                    if pd.isna(departement_nom) or pd.isna(departement_code):
                        print(f"Ignoring row with missing department data: {row}")
                        continue

                    # Vérifier si un département avec ce code OU ce nom existe déjà
                    result = conn.execute(
                        departement.select().where(
                            (departement.c.code == departement_code) | (departement.c.nom == departement_nom)
                        )
                    ).fetchone()

              
                        

                    if result is None:
                        conn.execute(departement.insert().values(code=departement_code, nom=departement_nom))
                        result = conn.execute(
                            departement.select().where(departement.c.code == departement_code)
                        ).fetchone()

                    #print(result)
                    departement_id = result[0]  # Récupérer l'ID
                    #print(departement_id)

                    #print("Column names:", df.columns.tolist())  # Print column names
                    # Extract winner details
                    nom_gagnant = row["Nom"]
                    prenom_gagnant = row["Prénom"]

                    #On récupère la première colonne "% Voix/Ins" trouvée car parfois il peut y en avoir deux qui ont le même nom "% Voix/Ins"
                    first_col_index = row.index.get_loc("% Voix/Ins")  # Get first occurrence index
                    value = row.iloc[first_col_index]
                    pourcentage_vote_gagnant = float(value.replace(',', '.')) if isinstance(value, str) else float(value)

                    possible_columns = ["% BlNuls/Ins", "% Blancs/Ins"]
                    pourcentage_vote_blanc = None  # Default to None

                    # Find the first available column with a defined value
                    for col in possible_columns:
                        if col in df.columns and pd.notna(row.get(col)):  # Check if column exists and value is not NaN
                            pourcentage_vote_blanc = row[col]
                            break

                    # If no valid column was found, raise an error
                    if pourcentage_vote_blanc is None:
                        raise ValueError("No valid column found for vote blanc percentage")

                    pourcentage_abstention = row["% Abs/Ins"];

                    #On récupère les 5 dernieres colonnes qui correspondent au perdant des élections
                    loser_data = row.iloc[-5:].dropna().values  # Extract last 5 columns & drop NaN values
                    nom_perdant = loser_data[0]
                    prenom_perdant = loser_data[1]
                    #pourcentage_vote_perdant = loser_data[3] #colonne "% Voix/Ins"

                    print(f"unemployment: {get_unemployment_rate(departement_nom, annee)} | annee: {annee} | departement: {departement_nom}")

                 

                    # Insert into elections
                    conn.execute(elections.insert().values(
                        annee=annee,
                        departement_id=departement_id,
                        moyenne_age=get_average_age(annee),
                        moyenne_pouvoir_achat=get_moyenne_pouvoir_achat(annee),
                        taux_chomage=get_unemployment_rate(departement_nom, annee),
                        type_de_position=random.randint(1, 5),
                        temperature_moyenne=get_average_temperature(annee),
                        nom_gagnant=nom_gagnant,
                        prenom_gagnant=prenom_gagnant,
                        nom_perdant=nom_perdant,
                        prenom_perdant=prenom_perdant,
                        pourcentage_vote_gagnant=pourcentage_vote_gagnant,
                        #pourcentage_vote_perdant=pourcentage_vote_perdant,
                        pourcentage_vote_blanc=pourcentage_vote_blanc, 
                        pourcentage_abstention=pourcentage_abstention
                    ))

if __name__ == "__main__":
    process_vote_files()

#TODO il reste à implémenter les données croisées