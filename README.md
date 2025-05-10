# Guide d'installation du projet

Suivez ces étapes pour installer et exécuter le projet en local.

## 1. Créer un environnement virtuel (optionnel mais recommandé)

Créer et activer un environnement virtuel :


# Créer l'environnement virtuel
python -m venv venv

L'activer sur Linux/macOS: \
source venv/bin/activate

Sur Windows\
venv\Scripts\activate


## 2. Puis installer les requirements

pip install -r requirements.txt

## 3. Avoir une base de données MYSQL en local

Les informations de connexion dans les scripts sont par défaut:

user: root \
password (empty) \
port: 3306

Si vos informations de connexion sont différentes, il faudra modifier dans les différents scripts également

## 4. Lancer les scripts dans l'ordre

create_data_structure.py \
csv_data_to_mysql.py \
train_model.py \
predict_elections.py \