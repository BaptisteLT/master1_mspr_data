# TODO: get:
# - CSV chomage, 
# - CSV age,
# - CSV pouvoir_achat,
# - CSV temperatures_moyennes







# 🔹 Load CSV Data (Replace with actual file paths)
file_donnees = "./donnees_annee.csv"
file_temperature = "./temperature_moyenne.csv"

df_donnees = pd.read_csv(file_donnees)
df_temperature = pd.read_csv(file_temperature)

# 🔹 Insert unique departments into `departement`
def insert_departements():
    unique_departements = df_donnees[["departement_code", "departement_nom"]].drop_duplicates().dropna()

    with engine.connect() as conn:
        with conn.begin():
            for _, row in unique_departements.iterrows():
                result = conn.execute(departement.select().where(departement.c.code == row["departement_code"])).fetchone()
                if result is None:
                    conn.execute(departement.insert().values(code=row["departement_code"], nom=row["departement_nom"]))
                else:
                    print(f"✔ Département {row['departement_nom']} already exists.")

# 🔹 Insert `donnees_annee`
def insert_donnees_annee():
    with engine.connect() as conn:
        with conn.begin():
            for _, row in df_donnees.iterrows():
                dept_id = conn.execute(departement.select().where(departement.c.code == row["departement_code"])).fetchone()
                if dept_id:
                    conn.execute(donnees_annee.insert().values(
                        annee=row["annee"],
                        departement_id=dept_id.id,
                        moyenne_age=row["moyenne_age"],
                        moyenne_pouvoir_achat=row["moyenne_pouvoir_achat"],
                        taux_chomage=row["taux_chomage"]
                    ))

# 🔹 Insert `temperature_moyenne`
def insert_temperature():
    with engine.connect() as conn:
        with conn.begin():
            for _, row in df_temperature.iterrows():
                result = conn.execute(temperature_moyenne.select().where(temperature_moyenne.c.annee == row["annee"])).fetchone()
                if result is None:
                    conn.execute(temperature_moyenne.insert().values(
                        annee=row["annee"], temperature_moyenne=row["temperature_moyenne"]
                    ))

# 🔥 Execute insertions
insert_departements()
insert_donnees_annee()
insert_temperature()

print("✅ Data successfully inserted into MySQL!")