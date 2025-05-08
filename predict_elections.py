import pandas as pd
import joblib
from sqlalchemy import create_engine
from sklearn.metrics import accuracy_score, classification_report

# Load trained model correctly
MODEL_PATH = "election_rf_model.pkl"
model = joblib.load(MODEL_PATH)

# Verify model is valid
if not hasattr(model, "predict"):
    raise ValueError("Loaded model is not a valid scikit-learn model. Check your pickle file.")

# Database connection
DB_URL = "mysql+pymysql://root:@localhost:3306/elections"
engine = create_engine(DB_URL)





# Pour l'exemple avec des données utilisées pendant l'entrainement, cette requête retourne Emmanuel Macron et donc milieu
query = "SELECT * FROM elections WHERE departement_id = 83  AND annee = 2022 LIMIT 1"

# Pour l'exemple avec des données utilisées pendant l'entrainement, cette requête retourne Jacques Chirac et donc droite
query = "SELECT * FROM elections WHERE departement_id = 83 LIMIT 1"

df = pd.read_sql(query, engine)






#Ou avec des données d'essai
data = {
    'moyenne_age': [30.5],  # One value for this feature
    'moyenne_pouvoir_achat': [-1.245526],  # One value for this feature
    'taux_chomage': [-0.085195],  # One value for this feature
    'temperature_moyenne': [-1.56138],  # One value for this feature
    'pourcentage_vote_gagnant': [-0.344884],  # One value for this feature
    'pourcentage_vote_blanc': [0.112227],  # One value for this feature
    'pourcentage_abstention': [-0.746214],  # One value for this feature
}

# Conversion en DataFrame
df = pd.DataFrame(data)


# Check if data exists
if df.empty:
    print("No data found")
    exit()

# Features used in training
features = ['moyenne_age', 'moyenne_pouvoir_achat', 'taux_chomage', 'temperature_moyenne', 'pourcentage_vote_gagnant', 'pourcentage_vote_blanc', 'pourcentage_abstention']
# Prepare data
X_new = df[features].apply(pd.to_numeric, errors='coerce').fillna(0)

# Make prediction
prediction = model.predict(X_new)
# Probabilités pour chaque classe
probabilities = model.predict_proba(X_new)

label_encoder = joblib.load("./label_encoder.pkl")
original_label = label_encoder.inverse_transform([prediction[0]])

# Afficher les résultats
print(f"Predicted label: {original_label[0]}")
print(f"Prediction probabilities: {probabilities[0]}")

# Afficher les probabilités de chaque classe
for label, prob in zip(label_encoder.classes_, probabilities[0]):
    print(f"Probability of '{label}': {prob:.4f}")

