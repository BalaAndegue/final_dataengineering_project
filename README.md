# Ecommerce Analytics - Pipeline ETL

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Pandas](https://img.shields.io/badge/Pandas-1.6.2-lightgrey)
![Google API](https://img.shields.io/badge/Google-API-green)

## 📌 Table des matières

* [Contexte](#-contexte)
* [Structure du projet](#-structure-du-projet)
* [Fonctionnement du pipeline](#-fonctionnement-du-pipeline)
* [Prérequis](#-prérequis)
* [Installation et lancement](#-installation-et-lancement)
* [Gestion des erreurs](#-gestion-des-erreurs)
* [Technologies utilisées](#-technologies-utilisées)
* [Licence](#-licence)
* [Auteur](#-auteur)

## 📌 Contexte

Ce projet a été réalisé dans le cadre de la formation **Data Engineering de DATA Afrique HUB**.
Il implémente un **pipeline ETL complet** pour collecter, transformer et charger des données e-commerce depuis Google Drive afin de les préparer pour analyse.

Le pipeline gère :

* Les données clients (plusieurs fichiers CSV).
* Les données produits (fichier CSV unique).

## 📁 Structure du projet

```
ecommerce-analytics/
│
├─ data/
│  ├─ raw_data/          # Données brutes téléchargées depuis Google Drive
│  │  ├─ clients/
│  │  └─ products/
│  ├─ processed/         # Données nettoyées et transformées
│  └─ load/              # Fichiers finaux prêts pour analyse
│
├─ src/
│  ├─ dags/
│  │  └─ common/
│  │     ├─ clean.py    # Extraction depuis Google Drive
│  │     ├─ enrichi.py  # Nettoyage et transformation
│  │     └─ extract.py       # Chargement final des fichiers
|     └─dags_definition
         └─extract.py
│  └─ config.py           # Configuration des chemins et paramètres
│
├─ main.py                # Script principal pour exécuter le pipeline ETL
└─genere.py
└─ generer2.py
└─ README.md
```

## ⚙️ Fonctionnement du pipeline

Le pipeline ETL suit trois étapes principales :

### 1. Extraction (`extract.py`)

* Téléchargement des fichiers clients et produits depuis Google Drive.
* Stockage dans `data/raw_data/clients` et `data/raw_data/products`.
* Gestion des erreurs pour fichiers manquants ou inaccessibles.

### 2. Transformation (`transform.py`)

* Clients : concaténation, suppression des doublons et vérification des fichiers vides.
* Produits : nettoyage et gestion des doublons.
* Sauvegarde des fichiers transformés dans `data/processed/`.

### 3. Chargement (`load.py`)

* Déplacement des fichiers transformés vers `data/load/`.
* Fichiers finaux générés :

  * `clients_final.csv`
  * `products_final.csv`

## 🛠️ Prérequis

* Python 3.10 ou supérieur
* Bibliothèques Python :

```bash
pip install pandas google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib
```

* Compte Service Account Google Drive configuré avec accès aux fichiers CSV.

## 🚀 Installation et lancement

1. Cloner le dépôt :

```bash
git clone https://github.com/AlphaLansar/ecommerce-analytics.git
cd ecommerce-analytics
```

2. Créer les dossiers de données :

```bash
mkdir -p data/raw_data/clients data/raw_data/products data/processed data/load
```

3. Exporter le PYTHONPATH :

```bash
export PYTHONPATH=$(pwd)/src
```

4. Exécuter le pipeline ETL complet :

```bash
python main.py
```

5. Vérifier les fichiers finaux :

* `data/load/clients_final.csv`
* `data/load/products_final.csv`

## ⚠️ Gestion des erreurs

* Fichiers clients vides : warning, ignorés.
* Fichiers produits vides ou invalides : erreur affichée.
* Les logs détaillent chaque étape pour faciliter le debug.

## 🖥️ Technologies utilisées

* Python 3.12
* Pandas 1.6.2
* Google API (Drive, OAuth2)
* Git & GitHub

## 📄 Licence

Ce projet n'est pas sous licence.
## 💡 Auteur

**BALA ANDEGUE FRANCOIS LIONNEL**
Formation Data Engineering – Projet TP `ecommerce-analytics`
