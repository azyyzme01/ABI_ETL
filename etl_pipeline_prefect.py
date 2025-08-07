# etl_pipeline_prefect.py
"""
Pipeline ETL ABI avec intégration Prefect
Version améliorée avec orchestration et monitoring
"""

import argparse
import logging
import sys
import os
from datetime import datetime
from pathlib import Path

# Configuration du path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'flows'))

def check_prefect_availability():
    """Vérifie si Prefect est disponible"""
    try:
        import prefect
        return True, prefect.__version__
    except ImportError:
        return False, None

def run_with_prefect(config_path: str = 'config/config.yaml', skip_validation: bool = False):
    """Exécute le pipeline avec Prefect"""
    try:
        from flows.prefect_etl_flows import abi_etl_pipeline_flow
        
        print("🚀 Exécution du pipeline ETL ABI avec Prefect")
        print(f"📋 Configuration: {config_path}")
        print(f"⚠️ Skip validation: {skip_validation}")
        
        # Exécution du flow Prefect
        result = abi_etl_pipeline_flow(
            config_path=config_path,
            skip_validation=skip_validation
        )
        
        print(f"✅ Pipeline terminé avec succès!")
        print(f"📊 Statut: {result.get('status', 'unknown')}")
        print(f"⏱️ Durée: {result.get('duration_seconds', 0):.2f} secondes")
        print(f"📈 Records traités: {result.get('total_records_processed', 0):,}")
        
        return True, result
        
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution Prefect: {e}")
        return False, str(e)

def run_without_prefect(config_path: str = 'config/config.yaml', skip_validation: bool = False):
    """Exécute le pipeline sans Prefect (fallback)"""
    try:
        # Import du pipeline original
        from etl_pipeline import ETLPipeline
        
        print("🔄 Exécution du pipeline ETL ABI (mode standard)")
        print(f"📋 Configuration: {config_path}")
        
        # Exécution du pipeline standard
        pipeline = ETLPipeline(config_path)
        result = pipeline.run_pipeline(skip_validation=skip_validation)
        
        print(f"✅ Pipeline terminé avec succès!")
        return True, result
        
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution standard: {e}")
        return False, str(e)

def main():
    """Fonction principale avec support Prefect"""
    parser = argparse.ArgumentParser(description='Pipeline ETL ABI avec support Prefect')
    parser.add_argument('--config', '-c', default='config/config.yaml',
                       help='Chemin vers le fichier de configuration')
    parser.add_argument('--skip-validation', action='store_true',
                       help='Ignorer l\'étape de validation')
    parser.add_argument('--force-standard', action='store_true',
                       help='Forcer l\'exécution en mode standard (sans Prefect)')
    parser.add_argument('--setup-prefect', action='store_true',
                       help='Configurer Prefect pour le projet')
    
    args = parser.parse_args()
    
    print("🏢 PIPELINE ETL ABI - Business Intelligence")
    print("="*60)
    print(f"📅 Démarré le: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Option: Configuration Prefect
    if args.setup_prefect:
        print("🔧 Configuration de Prefect...")
        try:
            from scripts.setup_prefect import main as setup_main
            return setup_main()
        except ImportError:
            print("❌ Script de configuration Prefect non trouvé")
            return False
    
    # Vérification de Prefect
    prefect_available, prefect_version = check_prefect_availability()
    
    if prefect_available and not args.force_standard:
        print(f"✅ Prefect disponible (v{prefect_version}) - Mode orchestration activé")
        success, result = run_with_prefect(args.config, args.skip_validation)
    else:
        if args.force_standard:
            print("⚠️ Mode standard forcé - Prefect désactivé")
        else:
            print("⚠️ Prefect non disponible - Fallback vers mode standard")
            print("💡 Pour installer Prefect: pip install prefect==2.14.21")
        
        success, result = run_without_prefect(args.config, args.skip_validation)
    
    # Résumé final
    print("\n" + "="*60)
    if success:
        print("🎉 PIPELINE ETL ABI TERMINÉ AVEC SUCCÈS!")
        if prefect_available and not args.force_standard:
            print("📊 Consultez Prefect UI pour plus de détails: http://127.0.0.1:4200")
    else:
        print("❌ PIPELINE ETL ABI ÉCHOUÉ")
        print(f"🔍 Erreur: {result}")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
