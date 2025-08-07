# scripts/setup_prefect.py
"""
Script de configuration et déploiement Prefect
Configure l'environnement Prefect pour le pipeline ETL ABI
"""

import os
import sys
import subprocess
import json
from pathlib import Path
from datetime import datetime, timedelta

def install_prefect():
    """Installe Prefect si nécessaire"""
    try:
        import prefect
        print(f"✅ Prefect déjà installé (version: {prefect.__version__})")
        return True
    except ImportError:
        print("📦 Installation de Prefect...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "prefect==2.14.21", "prefect-sqlalchemy==0.4.1"])
            print("✅ Prefect installé avec succès")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Erreur lors de l'installation de Prefect: {e}")
            return False

def setup_prefect_workspace():
    """Configure l'espace de travail Prefect"""
    try:
        from prefect.client.orchestration import get_client
        from prefect.server.schemas.schedules import CronSchedule
        
        print("🔧 Configuration de l'espace de travail Prefect...")
        
        # Créer le répertoire de travail si nécessaire
        work_dir = Path("prefect_workspace")
        work_dir.mkdir(exist_ok=True)
        
        print("✅ Espace de travail Prefect configuré")
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de la configuration: {e}")
        return False

def create_deployment_config():
    """Crée la configuration de déploiement"""
    deployment_config = {
        "name": "abi-etl-pipeline",
        "version": "1.0.0",
        "description": "Pipeline ETL ABI avec orchestration Prefect",
        "tags": ["etl", "abi", "business-intelligence"],
        "parameters": {
            "config_path": "config/config.yaml",
            "skip_validation": False
        },
        "work_queue_name": "abi-etl-queue",
        "schedule": {
            "cron": "0 2 * * *",  # Tous les jours à 2h du matin
            "timezone": "Europe/Paris"
        },
        "infrastructure": {
            "type": "process",
            "env": {
                "PYTHONPATH": str(Path.cwd()),
                "ETL_ENV": "production"
            }
        }
    }
    
    config_file = Path("deployments/abi_etl_deployment.json")
    config_file.parent.mkdir(exist_ok=True)
    
    with open(config_file, 'w') as f:
        json.dump(deployment_config, f, indent=2)
    
    print(f"✅ Configuration de déploiement sauvegardée: {config_file}")
    return deployment_config

def create_prefect_deployment():
    """Crée un déploiement Prefect"""
    try:
        from prefect import deploy
        from flows.prefect_etl_flows import abi_etl_pipeline_flow
        
        print("🚀 Création du déploiement Prefect...")
        
        # Déploiement principal
        deployment = abi_etl_pipeline_flow.to_deployment(
            name="abi-etl-daily",
            description="Pipeline ETL ABI exécuté quotidiennement",
            tags=["etl", "abi", "production", "daily"],
            parameters={
                "config_path": "config/config.yaml",
                "skip_validation": False
            },
            work_queue_name="abi-etl-queue",
            cron="0 2 * * *",  # Tous les jours à 2h du matin
        )
        
        # Déployer
        deployment_id = deploy(deployment)
        print(f"✅ Déploiement créé avec succès - ID: {deployment_id}")
        
        return deployment_id
        
    except Exception as e:
        print(f"❌ Erreur lors de la création du déploiement: {e}")
        return None

def create_monitoring_dashboard():
    """Crée un tableau de bord de monitoring simple"""
    dashboard_html = """
<!DOCTYPE html>
<html>
<head>
    <title>ABI ETL Pipeline - Monitoring Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .header { background-color: #2563eb; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        .card { background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .status { padding: 10px; border-radius: 4px; margin: 10px 0; }
        .status.success { background-color: #d1fae5; color: #065f46; }
        .status.warning { background-color: #fef3c7; color: #92400e; }
        .status.error { background-color: #fee2e2; color: #991b1b; }
        .metric { display: inline-block; margin: 10px 20px 10px 0; }
        .metric-value { font-size: 2em; font-weight: bold; color: #2563eb; }
        .metric-label { font-size: 0.9em; color: #6b7280; }
        .links { margin-top: 20px; }
        .links a { color: #2563eb; text-decoration: none; margin-right: 20px; }
        .links a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏢 ABI ETL Pipeline</h1>
        <p>Monitoring Dashboard - Business Intelligence Data Pipeline</p>
    </div>
    
    <div class="card">
        <h2>📊 Pipeline Status</h2>
        <div class="status success">
            ✅ Pipeline configuré et prêt à l'exécution
        </div>
        <div class="status warning">
            ⚠️ Monitoring en temps réel disponible via Prefect UI
        </div>
    </div>
    
    <div class="card">
        <h2>🔗 Liens Utiles</h2>
        <div class="links">
            <a href="http://127.0.0.1:4200" target="_blank">🎛️ Prefect UI</a>
            <a href="http://127.0.0.1:4200/flows" target="_blank">🔄 Flows</a>
            <a href="http://127.0.0.1:4200/deployments" target="_blank">🚀 Déploiements</a>
            <a href="http://127.0.0.1:4200/work-queues" target="_blank">📋 Queues</a>
        </div>
    </div>
    
    <div class="card">
        <h2>📝 Instructions</h2>
        <ol>
            <li><strong>Démarrer Prefect Server:</strong> <code>prefect server start</code></li>
            <li><strong>Démarrer Worker:</strong> <code>prefect worker start --pool default-agent-pool</code></li>
            <li><strong>Exécuter Pipeline:</strong> <code>python -m flows.prefect_etl_flows</code></li>
            <li><strong>Surveiller:</strong> Ouvrir <a href="http://127.0.0.1:4200">Prefect UI</a></li>
        </ol>
    </div>
    
    <div class="card">
        <h2>⚙️ Configuration</h2>
        <p><strong>Queue:</strong> abi-etl-queue</p>
        <p><strong>Schedule:</strong> Quotidien à 2h00 (UTC)</p>
        <p><strong>Retry Policy:</strong> 2 tentatives avec délai</p>
        <p><strong>Timeout:</strong> 2 heures maximum</p>
    </div>
</body>
</html>
    """
    
    dashboard_file = Path("monitoring/dashboard.html")
    dashboard_file.parent.mkdir(exist_ok=True)
    
    with open(dashboard_file, 'w', encoding='utf-8') as f:
        f.write(dashboard_html)
    
    print(f"✅ Tableau de bord créé: {dashboard_file}")
    return dashboard_file

def main():
    """Fonction principale de configuration"""
    print("🚀 Configuration de Prefect pour le pipeline ETL ABI")
    print("=" * 60)
    
    # Étape 1: Installation
    if not install_prefect():
        return False
    
    # Étape 2: Configuration de l'espace de travail
    if not setup_prefect_workspace():
        return False
    
    # Étape 3: Création de la configuration de déploiement
    create_deployment_config()
    
    # Étape 4: Création du tableau de bord
    dashboard_file = create_monitoring_dashboard()
    
    print("\n" + "=" * 60)
    print("✅ Configuration Prefect terminée avec succès!")
    print("\n📋 Prochaines étapes:")
    print("1. Démarrer le serveur Prefect:")
    print("   prefect server start")
    print("\n2. Dans un nouveau terminal, démarrer un worker:")
    print("   prefect worker start --pool default-agent-pool")
    print("\n3. Tester le pipeline:")
    print("   python -m flows.prefect_etl_flows")
    print("\n4. Ouvrir le tableau de bord:")
    print(f"   {dashboard_file.resolve()}")
    print("\n5. Accéder à Prefect UI:")
    print("   http://127.0.0.1:4200")
    
    return True

if __name__ == "__main__":
    main()
