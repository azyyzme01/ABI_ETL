# 🧬 ABI ETL Pipeline - Intelligence Biotech Data Warehouse

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Prefect](https://img.shields.io/badge/Prefect-2.14.21-purple.svg)](https://www.prefect.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue.svg)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 🎯 Vue d'ensemble

Pipeline ETL moderne orchestré par **Prefect** qui transforme les données d'intelligence biotechnologique en entrepôt de données optimisé pour l'analytique BI. Conçu pour l'**ABI Platform** avec monitoring avancé, retry automatique et validation qualité complète.

## 🏗️ Architecture

### Design Star Schema
Architecture **Star Schema** optimisée pour l'analyse BI avec les avantages suivants:

1. **Performance analytique** - Requêtes OLAP ultra-rapides
2. **Simplicité** - Structure intuitive : faits au centre, dimensions autour
3. **Compatibilité BI** - Optimisé pour Power BI, Tableau, Superset
4. **Scalabilité** - Gestion de volumes massifs de données biotech
5. **Évolutivité** - Ajout facile de nouvelles dimensions/faits

### Schema de Données
- **Tables de Faits** : Événements mesurables (évaluations technologiques, métriques financières, scores d'entreprises)
- **Tables de Dimensions** : Attributs descriptifs (technologies, publications, entreprises, dates)
- **Clés de Substitution** : Chaque dimension possède une clé synthétique pour performance et cohérence
- **Contraintes d'Intégrité** : Foreign keys, unique constraints, validations métier

### Orchestration Prefect
- **🔄 Retry automatique** - 2-3 tentatives configurables par tâche
- **⏱️ Timeout management** - Délais adaptés par étape ETL
- **📊 Monitoring temps réel** - Interface web complète
- **🚨 Alertes** - Notifications échecs/succès configurables
- **📅 Scheduling** - Exécution quotidienne automatique
- **🎯 Artifacts** - Rapports et métriques sauvegardés

## 📂 Structure du Projet
```
bi-etl-pipeline/
├── 📁 config/                  # Fichiers de configuration
│   ├── config.yaml            # Configuration principale
│   ├── prefect_config.py      # Configuration Prefect
│   └── performance_config.py  # Paramètres de performance
├── 📁 data/                   # Répertoires de données
│   ├── raw/                   # Données sources
│   ├── processed/             # Données transformées
│   ├── archive/               # Archives
│   └── exports/               # Exports BI
├── 📁 scripts/                # Scripts ETL
│   ├── extract.py             # Extraction de données
│   ├── transform.py           # Transformation star schema
│   ├── load.py                # Chargement PostgreSQL
│   └── setup_prefect.py       # Configuration Prefect
├── 📁 flows/                  # Flows Prefect
│   └── prefect_etl_flows.py   # Orchestration principale
├── 📁 models/                 # Modèles SQLAlchemy ORM
│   └── models.py              # Schéma de la base
├── 📁 validation/             # Contrôles qualité
│   └── data_validator.py      # Validation multi-niveaux
├── 📁 logs/                   # Journaux d'exécution
├── 📁 tests/                  # Tests unitaires
│   └── test_etl.py            # Tests des composants ETL
├── 📁 docs/                   # Documentation
│   └── PREFECT_GUIDE.md       # Guide complet Prefect
├── etl_pipeline.py            # Pipeline standard
├── etl_pipeline_prefect.py    # Pipeline orchestré Prefect
└── requirements.txt           # Dépendances Python
```

## 🚀 Démarrage Rapide

### 1. Installation de l'environnement
```bash
# Cloner le repository
git clone https://github.com/azyyzme01/ABI_ETL.git
cd bi-etl-pipeline

# Créer l'environnement virtuel
python -m venv mon_env
mon_env\Scripts\activate  # Windows
# source mon_env/bin/activate  # Linux/Mac

# Installer les dépendances
pip install -r requirements.txt
```

### 2. Configuration Base de Données
```bash
# Créer le fichier .env à partir du template
copy .env.template .env  # Windows
# cp .env.template .env  # Linux/Mac

# Éditer .env avec vos credentials
DB_HOST=localhost
DB_PORT=5432
DB_NAME=abi_warehouse
DB_USER=your_username
DB_PASSWORD=your_password
```

### 3. Initialisation PostgreSQL
```sql
-- Créer la base de données
CREATE DATABASE abi_warehouse;
CREATE USER etl_user WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE abi_warehouse TO etl_user;
```

### 4. Configuration Prefect
```bash
# Setup automatique de Prefect
python scripts/setup_prefect.py

# Démarrer le serveur Prefect (terminal 1)
prefect server start

# Démarrer un worker (terminal 2)
prefect worker start --pool default-agent-pool
```

### 5. Exécution du Pipeline
```bash
# Mode Prefect (recommandé)
python etl_pipeline_prefect.py

# Mode standard (fallback)
python etl_pipeline_prefect.py --force-standard

# Avec options avancées
python etl_pipeline_prefect.py --config config/config.yaml --skip-validation
```

### 6. Interface de Monitoring
- **Prefect UI** : http://127.0.0.1:4200
- **Flows** → `ABI ETL Pipeline` → **Run** pour exécution manuelle

## 📊 Sources de Données

### 🧪 Données Scientifiques
- **Prédictions TRL** (`realistic_trl_predictions.xlsx`)
  - Évolution de la maturité technologique (Technology Readiness Level)
  - Prédictions temporelles par secteur biotechnologique
  - Modèles ML de progression TRL

- **Articles PubMed** (`pubmed_articles_2015_2025_mergeds.xlsx`)
  - Publications scientifiques 2015-2025
  - Métadonnées complètes : auteurs, keywords, impact factor
  - Classification par domaine de recherche

### 🏢 Données Entreprises
- **Entreprises Biotech** (`abi_targeted_biotech_dataset_2019_2025.csv`)
  - Informations financières et de marché
  - Scores stratégiques et d'innovation
  - Métriques de performance trimestrielles

## 🎯 Cas d'Usage BI

### 📈 Analytics Supportés
1. **Suivi Maturité Technologique**
   - Heatmaps de progression TRL
   - Tendances par secteur biotech
   - Prédictions d'évolution

2. **Analyse des Tendances de Recherche**
   - Patterns de publication par domaine
   - Analyse d'impact et citations
   - Identification d'innovations émergentes

3. **Évaluation d'Entreprises**
   - Scoring stratégique multi-facteurs
   - Benchmarking sectoriel
   - Matrices de positionnement

4. **Analyse Financière**
   - Métriques de performance Q/Q
   - Ratios financiers sectoriels
   - Détection d'anomalies

5. **Intelligence d'Investissement**
   - Recommandations data-driven
   - Scoring de potentiel d'innovation
   - Analyse de risque sectoriel

## � Caractéristiques Techniques

### ✅ Idempotence Garantie
Le pipeline est **entièrement idempotent** - exécutions multiples avec les mêmes données produisent des résultats identiques sans doublons grâce aux techniques UPSERT PostgreSQL.

### 🔍 Validation Multi-Niveaux
1. **Fichiers sources** - Existence, format, taille
2. **Données extraites** - Complétude, types, cohérence
3. **Données transformées** - Intégrité référentielle, schéma
4. **Base de données** - Contraintes, règles métier
5. **Règles business** - Validations sectorielles spécifiques

### 📊 Métriques et Monitoring
- **Durée d'exécution** par étape ETL
- **Nombre d'enregistrements** traités/rejetés
- **Taux de succès/échec** historique
- **Utilisation ressources** système
- **Qualité des données** en temps réel
- **Artifacts Prefect** - Rapports détaillés sauvegardés

### 🛡️ Robustesse et Fiabilité
- **Retry automatique** - Configuration par type d'erreur
- **Timeout management** - Délais adaptés par complexité
- **Error handling** - Classification et escalade
- **Logging structuré** - Traçabilité complète
- **Rollback capabilities** - Restauration en cas d'échec

## 🛠️ Maintenance et Operations

### 📋 Commandes Utiles
```bash
# Tests complets
pytest tests/ -v --cov=scripts

# Validation qualité données
python validation/data_validator.py

# Reset environnement Prefect
prefect server database reset -y

# Monitoring logs
tail -f logs/etl_pipeline_$(date +%Y%m%d)*.log

# Export métriques
python scripts/export_metrics.py --format json
```

### 📁 Fichiers de Logs
- **Logs d'exécution** : `logs/etl_pipeline_YYYYMMDD_HHMMSS.log`
- **Métriques de performance** : `logs/etl_metrics_YYYYMMDD_HHMMSS.yaml`
- **Rapports de validation** : `logs/validation_report_YYYYMMDD_HHMMSS.yaml`
- **Logs Prefect** : Interface web + base de données intégrée

### 🚨 Alertes et Notifications
Configuration dans `config/prefect_config.py` :
- **Échecs critiques** - Email + Slack
- **Succès avec warnings** - Log + Dashboard
- **Métriques de performance** - Seuils configurables
- **Qualité des données** - Alertes par seuil

## 📚 Documentation

- **[Guide Prefect Complet](docs/PREFECT_GUIDE.md)** - Orchestration avancée
- **[Architecture Technique](docs/ARCHITECTURE.md)** - Design détaillé
- **[Guide de Déploiement](docs/DEPLOYMENT.md)** - Production setup
- **[API Reference](docs/API.md)** - Documentation code

## 🤝 Contribution

1. **Fork** le repository
2. **Créer** une branche feature (`git checkout -b feature/amélioration`)
3. **Commit** les changements (`git commit -m 'Ajout fonctionnalité'`)
4. **Push** vers la branche (`git push origin feature/amélioration`)
5. **Créer** une Pull Request

## 📝 License

Ce projet est sous licence **MIT** - voir [LICENSE](LICENSE) pour les détails.

## 👥 Équipe

- **Data Engineering** - [@azyyzme01](https://github.com/azyyzme01)
- **Architecture** - ABI Platform Team
- **Analytics** - BI Team

---

**🚀 Développé avec ❤️ pour l'intelligence biotechnologique**
