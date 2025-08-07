# Guide Prefect pour ETL ABI Pipeline

## 🚀 Introduction

Ce guide explique comment utiliser Prefect pour l'orchestration et le monitoring du pipeline ETL ABI. Prefect apporte des fonctionnalités avancées de surveillance, de planification et de gestion d'erreurs.

## 📦 Installation et Configuration

### 1. Installation des dépendances

```bash
# Installation de Prefect
pip install prefect==2.14.21 prefect-sqlalchemy==0.4.1

# Ou utiliser le requirements.txt mis à jour
pip install -r requirements.txt
```

### 2. Configuration automatique

```bash
# Configuration automatique de Prefect
python scripts/setup_prefect.py
```

### 3. Test de l'intégration

```bash
# Test complet de l'intégration
python test_prefect_integration.py
```

## 🎯 Utilisation

### Démarrage rapide

```bash
# 1. Démarrer le serveur Prefect (dans un terminal)
prefect server start

# 2. Démarrer un worker (dans un autre terminal)
prefect worker start --pool default-agent-pool

# 3. Exécuter le pipeline avec Prefect
python etl_pipeline_prefect.py
```

### Exécution via l'interface Prefect

1. **Ouvrir Prefect UI**: http://127.0.0.1:4200
2. **Naviguer vers Flows** → `ABI ETL Pipeline`
3. **Cliquer sur "Run"** pour exécuter manuellement

### Modes d'exécution

```bash
# Mode Prefect (par défaut)
python etl_pipeline_prefect.py

# Mode standard (fallback)
python etl_pipeline_prefect.py --force-standard

# Avec paramètres
python etl_pipeline_prefect.py --config config/config.yaml --skip-validation

# Configuration Prefect
python etl_pipeline_prefect.py --setup-prefect
```

## 🏗️ Architecture Prefect

### Flows et Tasks

Le pipeline est décomposé en 4 tasks principales:

1. **`prefect_extract_data`** - Extraction des données
2. **`prefect_transform_data`** - Transformation en schéma étoile
3. **`prefect_load_data`** - Chargement en base
4. **`prefect_validate_data`** - Validation qualité

### Flow principal

```python
@flow(name="ABI ETL Pipeline")
def abi_etl_pipeline_flow(config_path: str, skip_validation: bool = False):
    # Orchestration séquentielle avec monitoring
    extracted_data = prefect_extract_data(config_path)
    dimensions, facts = prefect_transform_data(extracted_data, config_path)
    load_results = prefect_load_data(dimensions, facts, config_path)
    
    if not skip_validation:
        validation_results = prefect_validate_data(dimensions, facts, config_path)
```

### Fonctionnalités Prefect activées

- ✅ **Retry automatique** - 2-3 tentatives par task
- ✅ **Timeout management** - Délais configurés par étape
- ✅ **Logging centralisé** - Logs structurés dans Prefect UI
- ✅ **Artifacts** - Rapports et métriques sauvegardés
- ✅ **Monitoring temps réel** - Suivi d'exécution via UI
- ✅ **Planification** - Exécution quotidienne automatique
- ✅ **Notifications** - Alertes en cas d'échec

## 📊 Monitoring et Observabilité

### Interface Prefect UI

Accès: http://127.0.0.1:4200

**Sections importantes:**
- **Flows** - Liste des pipelines
- **Flow Runs** - Historique d'exécution
- **Tasks** - Détail des tâches
- **Artifacts** - Rapports et métriques
- **Logs** - Journaux centralisés

### Artifacts générés

Pour chaque exécution, Prefect génère:

1. **Extraction Summary** - Métriques d'extraction
2. **Transformation Summary** - Détails des transformations
3. **Loading Summary** - Résultats du chargement
4. **Validation Summary** - Rapport de qualité
5. **Pipeline Execution Summary** - Résumé global

### Métriques surveillées

- **Durée d'exécution** par étape
- **Nombre d'enregistrements** traités
- **Taux de succès/échec**
- **Utilisation des ressources**
- **Qualité des données**

## ⚙️ Configuration

### Retry et Timeout

```python
# Configuration dans config/prefect_config.py
RETRY_CONFIG = {
    'extraction': {'retries': 3, 'retry_delay_seconds': 30},
    'transformation': {'retries': 2, 'retry_delay_seconds': 60},
    'load': {'retries': 3, 'retry_delay_seconds': 45},
    'validation': {'retries': 2, 'retry_delay_seconds': 30}
}

TIMEOUT_CONFIG = {
    'extraction': 3600,    # 1 heure
    'transformation': 7200, # 2 heures
    'load': 1800,          # 30 minutes
    'validation': 900      # 15 minutes
}
```

### Planification

Le pipeline est configuré pour s'exécuter:
- **Quotidiennement à 2h00** (UTC)
- **Queue**: `abi-etl-queue`
- **Tags**: `etl`, `abi`, `production`

## 🚨 Gestion des erreurs

### Stratégie de retry

1. **Erreurs temporaires** - Retry automatique avec délai
2. **Erreurs de données** - Arrêt avec rapport détaillé
3. **Erreurs système** - Notification et escalade

### Notifications

Configuration dans `config/prefect_config.py`:

```python
NOTIFICATION_CONFIG = {
    'on_failure': True,
    'on_success': True,
    'on_retry': True,
    'webhook_url': None,  # À configurer
    'email_recipients': [] # À configurer
}
```

## 🔧 Commandes utiles

### Gestion du serveur

```bash
# Démarrer le serveur Prefect
prefect server start

# Démarrer avec une base de données personnalisée
prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql://user:pass@localhost/prefect"
prefect server start

# Arrêter le serveur
Ctrl+C
```

### Gestion des workers

```bash
# Démarrer un worker par défaut
prefect worker start --pool default-agent-pool

# Démarrer un worker spécialisé
prefect worker start --pool abi-etl-pool --name abi-worker

# Lister les workers actifs
prefect worker ls
```

### Gestion des flows

```bash
# Lister les flows
prefect flow ls

# Exécuter un flow spécifique
prefect flow run "ABI ETL Pipeline"

# Voir l'historique
prefect flow-run ls --flow-name "ABI ETL Pipeline"
```

### Déploiements

```bash
# Créer un déploiement
prefect deployment build flows/prefect_etl_flows.py:abi_etl_pipeline_flow -n "daily-etl"

# Appliquer le déploiement
prefect deployment apply abi_etl_pipeline_flow-deployment.yaml

# Exécuter un déploiement
prefect deployment run "ABI ETL Pipeline/daily-etl"
```

## 🐛 Dépannage

### Problèmes courants

1. **Prefect UI inaccessible**
   ```bash
   # Vérifier le serveur
   prefect server start --host 0.0.0.0 --port 4200
   ```

2. **Tasks qui ne s'exécutent pas**
   ```bash
   # Vérifier les workers
   prefect worker ls
   prefect worker start --pool default-agent-pool
   ```

3. **Erreurs d'import**
   ```bash
   # Vérifier PYTHONPATH
   export PYTHONPATH="${PYTHONPATH}:$(pwd)"
   python etl_pipeline_prefect.py
   ```

4. **Base de données Prefect corrompue**
   ```bash
   # Reset de la base
   prefect server database reset -y
   prefect server start
   ```

### Logs de débogage

```bash
# Mode verbose
prefect config set PREFECT_LOGGING_LEVEL=DEBUG
python etl_pipeline_prefect.py

# Logs dans fichier
prefect server start > prefect_server.log 2>&1 &
```

## 📚 Ressources

- **Documentation Prefect**: https://docs.prefect.io/
- **Exemples**: https://github.com/PrefectHQ/prefect/tree/main/examples
- **Community**: https://discourse.prefect.io/

## ✨ Avantages de l'intégration Prefect

1. **🎯 Monitoring avancé** - Interface graphique intuitive
2. **🔄 Orchestration robuste** - Gestion des dépendances et erreurs
3. **📈 Métriques détaillées** - Suivi de performance en temps réel
4. **🚨 Alertes automatiques** - Notifications sur échecs/succès
5. **📅 Planification flexible** - Exécution programmée ou manuelle
6. **🔍 Traçabilité complète** - Historique des exécutions
7. **⚡ Scalabilité** - Support multi-workers et parallélisation
