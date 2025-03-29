import pandas as pd
import joblib
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

# Database connection
DB_URL = "mysql+pymysql://root:@localhost:3306/elections"
engine = create_engine(DB_URL)

# Load data
query = "SELECT * FROM elections WHERE departement_id = 84"
df = pd.read_sql(query, engine)

# Ensure numeric data
df['moyenne_age'] = pd.to_numeric(df['moyenne_age'], errors='coerce')
df['moyenne_pouvoir_achat'] = pd.to_numeric(df['moyenne_pouvoir_achat'], errors='coerce')
df['taux_chomage'] = pd.to_numeric(df['taux_chomage'], errors='coerce')

# Drop missing values
df.dropna(inplace=True)

# Features & Target
features = ['moyenne_age', 'moyenne_pouvoir_achat', 'taux_chomage', 
            'pourcentage_vote_gagnant', 'pourcentage_vote_blanc', 'pourcentage_abstention']
target = 'temperature_moyenne'  

X = df[features]
y = df[target]

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = LinearRegression()
model.fit(X_train, y_train)

# Save with joblib
joblib.dump(model, "election_model.pkl")
print("Model successfully saved as 'election_model.pkl'.")
