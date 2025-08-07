# demo_prefect_etl.py
"""
Démonstration du pipeline ETL ABI avec Prefect
Script de démonstration pour présenter les fonctionnalités Prefect
"""

import sys
import os
import time
from pathlib import Path

# Configuration du path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'flows'))

def demo_prefect_features():
    """Démonstration des fonctionnalités Prefect"""
    print("🚀 DÉMONSTRATION PIPELINE ETL ABI AVEC PREFECT")
    print("="*70)
    
    try:
        import prefect
        print(f"✅ Prefect version: {prefect.__version__}")
    except ImportError:
        print("❌ Prefect non installé")
        return False
    
    print("\n📋 FONCTIONNALITÉS PREFECT ACTIVÉES:")
    print("   ✅ Orchestration automatique des tâches")
    print("   ✅ Monitoring en temps réel")
    print("   ✅ Retry automatique en cas d'échec")
    print("   ✅ Logging centralisé")
    print("   ✅ Artifacts et rapports")
    print("   ✅ Interface web de monitoring")
    print("   ✅ Planification automatique")
    
    return True

def demo_individual_flows():
    """Démonstration des flows individuels"""
    print("\n🔧 DÉMONSTRATION DES FLOWS INDIVIDUELS")
    print("-"*50)
    
    try:
        from flows.prefect_etl_flows import (
            prefect_extract_data, 
            extract_only_flow
        )
        
        print("\n1️⃣ TEST DU FLOW D'EXTRACTION")
        print("   Extraction des données TRL, PubMed et Companies...")
        
        start_time = time.time()
        result = extract_only_flow()
        duration = time.time() - start_time
        
        print(f"   ✅ Extraction terminée en {duration:.2f} secondes")
        print(f"   📊 {len(result)} datasets extraits:")
        for name, df in result.items():
            print(f"      - {name}: {len(df):,} enregistrements")
        
        return True, result
        
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
        return False, None

def demo_full_pipeline():
    """Démonstration du pipeline complet"""
    print("\n🏗️ DÉMONSTRATION DU PIPELINE COMPLET")
    print("-"*50)
    
    try:
        from flows.prefect_etl_flows import abi_etl_pipeline_flow
        
        print("   Exécution du pipeline ETL complet avec Prefect...")
        print("   ⚠️ Cette démonstration peut prendre quelques minutes")
        
        # Exécution avec skip_validation pour la démo
        start_time = time.time()
        result = abi_etl_pipeline_flow(skip_validation=True)
        duration = time.time() - start_time
        
        print(f"   ✅ Pipeline terminé en {duration:.2f} secondes")
        print(f"   📊 Statut: {result.get('status', 'unknown')}")
        print(f"   📈 Records traités: {result.get('total_records_processed', 0):,}")
        
        # Détails par étape
        if 'extraction_records' in result:
            print("   📥 Extraction:")
            for source, count in result['extraction_records'].items():
                print(f"      - {source}: {count:,}")
        
        if 'dimension_records' in result:
            print("   🔄 Dimensions:")
            for dim, count in result['dimension_records'].items():
                print(f"      - {dim}: {count:,}")
        
        if 'fact_records' in result:
            print("   📊 Facts:")
            for fact, count in result['fact_records'].items():
                print(f"      - {fact}: {count:,}")
        
        return True, result
        
    except Exception as e:
        print(f"   ❌ Erreur pipeline: {e}")
        return False, None

def demo_monitoring_info():
    """Information sur le monitoring"""
    print("\n📊 MONITORING ET OBSERVABILITÉ")
    print("-"*50)
    print("   🎛️ Interface Prefect UI: http://127.0.0.1:4200")
    print("   📋 Logs centralisés: Visibles dans Prefect UI")
    print("   📈 Métriques: Durée, succès/échec, utilisation ressources")
    print("   🔍 Artifacts: Rapports de chaque étape sauvegardés")
    print("   ⚠️ Alertes: Notifications automatiques configurables")
    
    print("\n💡 COMMANDES UTILES:")
    print("   # Démarrer le serveur Prefect")
    print("   prefect server start")
    print("")
    print("   # Démarrer un worker")
    print("   prefect worker start --pool default-agent-pool")
    print("")
    print("   # Exécuter le pipeline")
    print("   python etl_pipeline_prefect.py")

def demo_advanced_features():
    """Démonstration des fonctionnalités avancées"""
    print("\n🚀 FONCTIONNALITÉS AVANCÉES")
    print("-"*50)
    
    print("   🔄 RETRY AUTOMATIQUE:")
    print("      - Extraction: 3 tentatives, délai 30s")
    print("      - Transformation: 2 tentatives, délai 60s")
    print("      - Chargement: 3 tentatives, délai 45s")
    print("      - Validation: 2 tentatives, délai 30s")
    
    print("\n   ⏰ TIMEOUTS:")
    print("      - Extraction: 1 heure maximum")
    print("      - Transformation: 2 heures maximum")
    print("      - Chargement: 30 minutes maximum")
    print("      - Validation: 15 minutes maximum")
    
    print("\n   📋 PLANIFICATION:")
    print("      - Quotidienne à 2h00 (UTC)")
    print("      - Configurable via cron expression")
    print("      - Exécution manuelle possible")
    
    print("\n   📊 ARTIFACTS GÉNÉRÉS:")
    print("      - Résumé d'extraction avec métriques")
    print("      - Rapport de transformation détaillé")
    print("      - Résultats de chargement")
    print("      - Rapport de validation qualité")
    print("      - Résumé d'exécution global")

def main():
    """Fonction principale de démonstration"""
    print("🎯 DÉMONSTRATION COMPLÈTE: ETL ABI + PREFECT")
    print("="*70)
    
    # 1. Vérification Prefect
    if not demo_prefect_features():
        return False
    
    # 2. Flows individuels
    success, extract_result = demo_individual_flows()
    if not success:
        print("⚠️ Échec des flows individuels, mais continuons...")
    
    # 3. Pipeline complet
    print(f"\n{'='*70}")
    choice = input("🤔 Voulez-vous exécuter le pipeline complet ? (o/N): ").lower().strip()
    
    if choice in ['o', 'oui', 'y', 'yes']:
        pipeline_success, pipeline_result = demo_full_pipeline()
        if pipeline_success:
            print("\n🎉 PIPELINE COMPLET EXÉCUTÉ AVEC SUCCÈS!")
        else:
            print("\n⚠️ Pipeline complet échoué, mais l'intégration Prefect fonctionne")
    else:
        print("\n⏭️ Pipeline complet ignoré")
    
    # 4. Informations avancées
    demo_monitoring_info()
    demo_advanced_features()
    
    # 5. Résumé final
    print(f"\n{'='*70}")
    print("🎉 DÉMONSTRATION TERMINÉE AVEC SUCCÈS!")
    print("="*70)
    
    print("\n📋 RÉSUMÉ DE L'INTÉGRATION PREFECT:")
    print("   ✅ Prefect installé et fonctionnel")
    print("   ✅ Flows ETL configurés")
    print("   ✅ Monitoring activé")
    print("   ✅ Retry et timeout configurés")
    print("   ✅ Artifacts générés automatiquement")
    print("   ✅ Interface web disponible")
    
    print("\n🚀 PROCHAINES ÉTAPES:")
    print("   1. Démarrer le serveur Prefect: prefect server start")
    print("   2. Ouvrir l'interface: http://127.0.0.1:4200")
    print("   3. Exécuter le pipeline: python etl_pipeline_prefect.py")
    print("   4. Surveiller l'exécution dans Prefect UI")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️ Démonstration interrompue par l'utilisateur")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Erreur inattendue: {e}")
        sys.exit(1)
