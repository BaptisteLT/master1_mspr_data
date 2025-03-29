import pandas as pd
import joblib
from sqlalchemy import create_engine

# Load trained model correctly
MODEL_PATH = "election_model.pkl"
model = joblib.load(MODEL_PATH)

# Verify model is valid
if not hasattr(model, "predict"):
    raise ValueError("Loaded model is not a valid scikit-learn model. Check your pickle file.")

# Database connection
DB_URL = "mysql+pymysql://root:@localhost:3306/elections"
engine = create_engine(DB_URL)

# Load data
query = "SELECT * FROM elections WHERE departement_id = 84 LIMIT 1"
df = pd.read_sql(query, engine)

# Check if data exists
if df.empty:
    print("No data found for departement_id = 84")
    exit()

# Features used in training
features = ['moyenne_age', 'moyenne_pouvoir_achat', 'taux_chomage', 
            'pourcentage_vote_gagnant', 'pourcentage_vote_blanc', 'pourcentage_abstention']

# Prepare data
X_new = df[features].apply(pd.to_numeric, errors='coerce').fillna(0)

# Make prediction
prediction = model.predict(X_new)
print(f"Prediction for the first row: {prediction[0]}")
