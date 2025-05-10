# Guide d'installation du projet

Suis ces étapes pour installer et exécuter le projet en local.

## 1. Créer un environnement virtuel (optionnel mais recommandé)

Crée et active un environnement virtuel :

```bash
# Créer l'environnement virtuel
python -m venv venv

# L'activer
# Sur Linux/macOS
source venv/bin/activate

# Sur Windows
venv\Scripts\activate


## 2. Puis installer les requirements

pip install -r requirements.txt

## 3. Avoir une base de données MYSQL en local

## 4. Lancer les scripts dans l'ordre

create_data_structure.py
csv_data_to_mysql.py
train_model.py
predict_elections.py