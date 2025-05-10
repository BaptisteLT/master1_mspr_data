import pandas as pd
import joblib
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

# Connexion à la base de données
DB_URL = "mysql+pymysql://root:@localhost:3306/elections"
engine = create_engine(DB_URL)



# Chargement des données depuis la base de données
query = "SELECT * FROM elections INNER JOIN type_de_position ON type_de_position.id = elections.type_de_position WHERE departement_id = 83"
df = pd.read_sql(query, engine)

# Définition des caractéristiques (features) et de la variable cible (target)
features = ['moyenne_age', 'moyenne_pouvoir_achat', 'taux_chomage', 'temperature_moyenne', 
            'pourcentage_vote_gagnant', 'pourcentage_vote_blanc', 'pourcentage_abstention']

target = 'libelle'  # 'gauche', 'droite', 'milieu'

X = df[features]  # Caractéristiques
y = df[target]    # Cible

# Encodage de la variable cible catégorielle
label_encoder = LabelEncoder()
y_encoded = label_encoder.fit_transform(y)
print("Labels encodés :", label_encoder.classes_)

#On sauvegarde l'encodeur utilisé pour le réutiliser lors de la prédiction et retrouver les labels utilisés
joblib.dump(label_encoder, "./label_encoder.pkl")

# Séparation des données en ensembles d'entraînement et de test (80%/20%)
X_train, X_test, y_train, y_test = train_test_split(X, y_encoded, test_size=0.2, random_state=42)

# Entraînement du classificateur RandomForest
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Évaluation du modèle sur les données de test
accuracy = model.score(X_test, y_test)
print(f"Accuracy du modèle sur les données de test : {accuracy:.2f}")

# Sauvegarde du modèle avec joblib
joblib.dump(model, "./election_rf_model.pkl")
print("Le modèle Random Forest a été enregistré avec succès sous 'election_rf_model.pkl'.")