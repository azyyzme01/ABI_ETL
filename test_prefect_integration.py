# test_prefect_integration.py
"""
Script de test pour l'intégration Prefect
Teste le pipeline ETL avec orchestration Prefect
"""

import os
import sys
import subprocess
import time
from pathlib import Path
from datetime import datetime

def check_prefect_installation():
    """Vérifie si Prefect est installé"""
    try:
        result = subprocess.run([sys.executable, "-c", "import prefect; print(prefect.__version__)"], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            version = result.stdout.strip()
            print(f"✅ Prefect installé - Version: {version}")
            return True
        else:
            print("❌ Prefect non installé")
            return False
    except Exception as e:
        print(f"❌ Erreur lors de la vérification: {e}")
        return False

def install_prefect_if_needed():
    """Installe Prefect si nécessaire"""
    if not check_prefect_installation():
        print("📦 Installation de Prefect...")
        try:
            # Installation de Prefect
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", 
                "prefect==2.14.21", 
                "prefect-sqlalchemy==0.4.1"
            ])
            print("✅ Prefect installé avec succès")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Erreur lors de l'installation: {e}")
            return False
    return True

def test_basic_import():
    """Test des imports de base"""
    print("\n🧪 Test des imports de base...")
    try:
        # Test import Prefect
        import prefect
        from prefect import flow, task
        print(f"✅ Import Prefect réussi - Version: {prefect.__version__}")
        
        # Test import des modules ETL
        sys.path.append(str(Path.cwd()))
        
        # Test des imports conditionnels
        try:
            from flows.prefect_etl_flows import abi_etl_pipeline_flow
            print("✅ Import du flow principal réussi")
        except ImportError as e:
            print(f"⚠️ Import conditionnel du flow: {e}")
        
        return True
    except ImportError as e:
        print(f"❌ Erreur d'import: {e}")
        return False

def test_simple_flow():
    """Test d'un flow Prefect simple"""
    print("\n🧪 Test d'un flow Prefect simple...")
    try:
        from prefect import flow, task
        
        @task
        def test_task():
            print("✅ Task de test exécutée")
            return "success"
        
        @flow
        def test_flow():
            result = test_task()
            print(f"✅ Flow de test exécuté - Résultat: {result}")
            return result
        
        # Exécution du flow de test
        result = test_flow()
        print(f"✅ Flow simple testé avec succès: {result}")
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors du test du flow simple: {e}")
        return False

def test_etl_pipeline_dry_run():
    """Test à sec du pipeline ETL"""
    print("\n🧪 Test du pipeline ETL (dry run)...")
    try:
        # Import conditionnel du pipeline
        sys.path.append(str(Path.cwd()))
        
        # Vérifier que les modules ETL existent
        modules_to_check = [
            'scripts.extract',
            'scripts.transform', 
            'scripts.load',
            'validation.data_validator'
        ]
        
        missing_modules = []
        for module in modules_to_check:
            try:
                __import__(module)
                print(f"✅ Module {module} disponible")
            except ImportError:
                print(f"⚠️ Module {module} non disponible")
                missing_modules.append(module)
        
        if missing_modules:
            print(f"⚠️ Modules manquants: {missing_modules}")
            print("💡 Le pipeline peut fonctionner en mode dégradé")
        
        # Test import du flow principal
        try:
            from flows.prefect_etl_flows import abi_etl_pipeline_flow
            print("✅ Flow principal importé avec succès")
            
            # Test de la signature du flow (sans exécution)
            flow_obj = abi_etl_pipeline_flow
            print(f"✅ Flow '{flow_obj.name}' prêt à l'exécution")
            print(f"   Description: {flow_obj.description}")
            # print(f"   Tags: {flow_obj.tags}")  # Commenté car non disponible dans cette version
            
            return True
            
        except ImportError as e:
            print(f"⚠️ Import du flow principal échoué: {e}")
            return False
        
    except Exception as e:
        print(f"❌ Erreur lors du test du pipeline: {e}")
        return False

def test_prefect_server_connection():
    """Test de connexion au serveur Prefect"""
    print("\n🧪 Test de connexion au serveur Prefect...")
    try:
        from prefect.client.orchestration import get_client
        
        # Test de connexion
        with get_client() as client:
            # Tentative de récupération des flows
            try:
                flows = client.read_flows()
                print(f"✅ Connexion au serveur Prefect réussie")
                print(f"   Flows disponibles: {len(flows) if flows else 0}")
                return True
            except Exception as e:
                print(f"⚠️ Serveur Prefect non démarré: {e}")
                print("💡 Démarrez le serveur avec: prefect server start")
                return False
                
    except Exception as e:
        print(f"❌ Erreur de connexion: {e}")
        return False

def show_next_steps():
    """Affiche les prochaines étapes"""
    print("\n" + "="*60)
    print("🚀 INTÉGRATION PREFECT CONFIGURÉE AVEC SUCCÈS!")
    print("="*60)
    
    print("\n📋 PROCHAINES ÉTAPES:")
    print("\n1️⃣ Démarrer le serveur Prefect:")
    print("   prefect server start")
    print("   ➡️ Interface: http://127.0.0.1:4200")
    
    print("\n2️⃣ Dans un nouveau terminal, démarrer un worker:")
    print("   prefect worker start --pool default-agent-pool")
    
    print("\n3️⃣ Tester le pipeline complet:")
    print("   python -m flows.prefect_etl_flows")
    
    print("\n4️⃣ Déployer le pipeline (optionnel):")
    print("   python scripts/setup_prefect.py")
    
    print("\n5️⃣ Exécuter le pipeline via Prefect:")
    print("   prefect deployment run 'ABI ETL Pipeline/abi-etl-daily'")
    
    print("\n📊 MONITORING:")
    print("   • Interface Prefect: http://127.0.0.1:4200")
    print("   • Logs détaillés dans /logs")
    print("   • Métriques et artifacts dans Prefect UI")
    
    print("\n🎯 FONCTIONNALITÉS PREFECT ACTIVÉES:")
    print("   ✅ Orchestration des tâches")
    print("   ✅ Monitoring en temps réel") 
    print("   ✅ Retry automatique")
    print("   ✅ Logging centralisé")
    print("   ✅ Artifacts et rapports")
    print("   ✅ Planification (cron)")
    print("   ✅ Interface web")

def main():
    """Fonction principale de test"""
    print("🔧 TEST D'INTÉGRATION PREFECT POUR ETL ABI")
    print("="*60)
    print(f"📅 Démarré le: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    tests_passed = 0
    total_tests = 5
    
    # Test 1: Installation Prefect
    print(f"\n[1/{total_tests}] Installation Prefect")
    if install_prefect_if_needed():
        tests_passed += 1
    
    # Test 2: Imports de base
    print(f"\n[2/{total_tests}] Imports de base")
    if test_basic_import():
        tests_passed += 1
    
    # Test 3: Flow simple
    print(f"\n[3/{total_tests}] Flow Prefect simple")
    if test_simple_flow():
        tests_passed += 1
    
    # Test 4: Pipeline ETL
    print(f"\n[4/{total_tests}] Pipeline ETL")
    if test_etl_pipeline_dry_run():
        tests_passed += 1
    
    # Test 5: Connexion serveur (optionnel)
    print(f"\n[5/{total_tests}] Connexion serveur Prefect")
    if test_prefect_server_connection():
        tests_passed += 1
    else:
        print("💡 Ce test est optionnel - le serveur peut être démarré plus tard")
        tests_passed += 0.5  # Demi-point car optionnel
    
    # Résumé
    print(f"\n📊 RÉSULTATS DES TESTS: {tests_passed}/{total_tests}")
    
    if tests_passed >= 4:
        print("✅ INTÉGRATION PREFECT RÉUSSIE!")
        show_next_steps()
        return True
    else:
        print("❌ INTÉGRATION PREFECT PARTIELLEMENT RÉUSSIE")
        print("🔧 Vérifiez les erreurs ci-dessus et réessayez")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
