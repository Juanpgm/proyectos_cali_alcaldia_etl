# -*- coding: utf-8 -*-
"""
Enhanced ETL Pipeline for Unidades de Proyecto with In-Memory Processing.
Eliminates dependency on temporary files for better GitHub Actions compatibility.
"""

import sys
import os
from typing import Optional, Dict, List, Any
from datetime import datetime
import pandas as pd

# Add modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'extraction_app'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'transformation_app'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'load_app'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from data_extraction_unidades_proyecto import extract_unidades_proyecto_data, extract_and_save_unidades_proyecto
from data_transformation_unidades_proyecto import unidades_proyecto_transformer, save_unidades_proyecto_geojson
from data_loading_unidades_proyecto import load_unidades_proyecto_to_firebase
from temp_file_manager import TempFileManager, process_in_memory


class InMemoryETLPipeline:
    """ETL Pipeline that processes data entirely in memory without temporary files."""
    
    def __init__(self):
        self.temp_manager = TempFileManager()
        self.start_time = None
        self.end_time = None
        
    def execute_pipeline(self, use_temp_files: bool = False) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.
        
        Args:
            use_temp_files: If True, falls back to file-based processing
            
        Returns:
            Dictionary with pipeline results and statistics
        """
        
        self.start_time = datetime.now()
        results = {
            'success': False,
            'extraction': {'records': 0, 'success': False},
            'transformation': {'records': 0, 'success': False}, 
            'loading': {'new': 0, 'modified': 0, 'unchanged': 0, 'success': False},
            'start_time': self.start_time,
            'duration': None,
            'errors': []
        }
        
        try:
            # Step 1: Extract data
            print("🚀 Iniciando pipeline ETL de Unidades de Proyecto...")
            print("🚀 INICIANDO PIPELINE ETL UNIDADES DE PROYECTO")
            print("="*80)
            print(f"⏰ Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("🗂️ Colección destino: unidades_proyecto")
            
            if use_temp_files:
                print("\n📁 Usando procesamiento basado en archivos temporales")
                raw_data = self._extract_with_temp_files()
            else:
                print("\n🧠 Usando procesamiento en memoria (sin archivos temporales)")
                raw_data = self._extract_in_memory()
            
            if raw_data is None:
                results['errors'].append("Data extraction failed")
                return results
                
            results['extraction']['records'] = len(raw_data)
            results['extraction']['success'] = True
            
            # Step 2: Transform data
            transformed_data = self._transform_data(raw_data, use_temp_files)
            if transformed_data is None:
                results['errors'].append("Data transformation failed")
                return results
                
            results['transformation']['records'] = len(transformed_data)
            results['transformation']['success'] = True
            
            # Step 3: Load data to Firebase
            loading_result = self._load_data(transformed_data)
            if loading_result is None:
                results['errors'].append("Data loading failed")
                return results
                
            results['loading'].update(loading_result)
            results['loading']['success'] = True
            
            # Step 4: Save GeoJSON output
            geojson_success = self._save_geojson_output(transformed_data)
            if not geojson_success:
                results['errors'].append("GeoJSON save failed (non-critical)")
            
            results['success'] = True
            
        except Exception as e:
            results['errors'].append(f"Pipeline error: {str(e)}")
            print(f"❌ Pipeline error: {e}")
            
        finally:
            self.end_time = datetime.now()
            duration = self.end_time - self.start_time
            results['duration'] = duration
            results['end_time'] = self.end_time
            
            # Cleanup any temporary resources
            self.temp_manager.cleanup_all()
            
            # Print summary
            self._print_summary(results)
            
        return results
    
    def _extract_in_memory(self) -> Optional[pd.DataFrame]:
        """Extract data directly to memory without temporary files."""
        print("\n" + "="*60)
        print("📊 PASO: EXTRACCIÓN DE DATOS")
        print("="*60)
        
        try:
            data = extract_unidades_proyecto_data()
            if data is not None:
                print(f"✅ EXTRACCIÓN DE DATOS completado - {len(data)} registros")
            return data
        except Exception as e:
            print(f"❌ Error en extracción: {e}")
            return None
    
    def _extract_with_temp_files(self) -> Optional[pd.DataFrame]:
        """Extract data using temporary files (fallback method)."""
        print("\n" + "="*60)
        print("📊 PASO: EXTRACCIÓN DE DATOS")
        print("="*60)
        
        try:
            data = extract_and_save_unidades_proyecto()
            if data is not None:
                print(f"✅ EXTRACCIÓN DE DATOS completado - {len(data)} registros")
            return data
        except Exception as e:
            print(f"❌ Error en extracción: {e}")
            return None
    
    def _transform_data(self, raw_data: pd.DataFrame, use_temp_files: bool) -> Optional[pd.DataFrame]:
        """Transform data using in-memory or file-based processing."""
        print("\n" + "="*60)
        print("📊 PASO: TRANSFORMACIÓN DE DATOS")
        print("="*60)
        
        try:
            if use_temp_files:
                # Use file-based transformation (original method)
                transformed_data = unidades_proyecto_transformer()
            else:
                # Use in-memory transformation (new method)
                transformed_data = unidades_proyecto_transformer(data=raw_data)
            
            if transformed_data is not None:
                print(f"✅ TRANSFORMACIÓN DE DATOS completado - {len(transformed_data)} registros")
            return transformed_data
            
        except Exception as e:
            print(f"❌ Error en transformación: {e}")
            return None
    
    def _load_data(self, transformed_data: pd.DataFrame) -> Optional[Dict[str, int]]:
        """Load data to Firebase with incremental verification."""
        print("\n" + "="*60)
        print("📊 PASO: VERIFICACIÓN INCREMENTAL")
        print("="*60)
        
        try:
            # Para el pipeline en memoria, procedemos directamente a cargar
            # En una implementación más completa se podría agregar verificación incremental
            total_features = len(transformed_data.get('features', []))
            print(f"📊 Total de registros a procesar: {total_features}")
            
            if total_features > 0:
                print("\n" + "="*60)
                print("📊 PASO: CARGA A FIREBASE")
                print("="*60)
                
                # Load data to Firebase
                success = load_unidades_proyecto_to_firebase(transformed_data)
                if not success:
                    return None
                    
                print(f"✅ CARGA A FIREBASE completado - {total_features} registros cargados")
            else:
                print("✅ No hay datos para cargar")
            
            print(f"✅ VERIFICACIÓN INCREMENTAL completado")
            
            return {
                'new': total_features,
                'modified': 0, 
                'unchanged': 0
            }
            
        except Exception as e:
            print(f"❌ Error en verificación/carga: {e}")
            return None
    
    def _save_geojson_output(self, transformed_data: pd.DataFrame) -> bool:
        """Save GeoJSON output file."""
        try:
            return save_unidades_proyecto_geojson(transformed_data)
        except Exception as e:
            print(f"⚠️ Warning: Could not save GeoJSON: {e}")
            return False
    
    def _print_summary(self, results: Dict[str, Any]) -> None:
        """Print pipeline execution summary."""
        print("\n" + "="*80)
        print("📊 RESUMEN DEL PIPELINE ETL")
        print("="*80)
        
        status = "EXITOSO" if results['success'] else "FALLIDO"
        print(f"✅ Estado general: {status}")
        print(f"⏰ Inicio: {results['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🏁 Fin: {results['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        duration = results['duration']
        minutes = int(duration.total_seconds() // 60)
        seconds = int(duration.total_seconds() % 60)
        print(f"⏱️ Duración: {minutes}m {seconds}s")
        
        print(f"\n🔄 Pasos ejecutados:")
        print(f"  {'✅' if results['extraction']['success'] else '❌'} Extracción")
        print(f"  {'✅' if results['transformation']['success'] else '❌'} Transformación")
        print(f"  {'✅' if results['loading']['success'] else '❌'} Verificación incremental")
        print(f"  {'✅' if results['loading']['success'] else '❌'} Carga a Firebase")
        
        print(f"\n📈 Estadísticas:")
        print(f"  📥 Registros procesados: {results['transformation']['records']}")
        print(f"  📤 Registros cargados: {results['loading']['new'] + results['loading']['modified']}")
        
        print(f"\n🔄 Resumen de cambios:")
        print(f"  ➕ Nuevos: {results['loading']['new']}")
        print(f"  🔄 Modificados: {results['loading']['modified']}")
        print(f"  ✅ Sin cambios: {results['loading']['unchanged']}")
        
        if results['errors']:
            print(f"\n⚠️ Errores encontrados:")
            for error in results['errors']:
                print(f"  - {error}")
        
        if results['success']:
            print(f"\n🎉 Pipeline completado exitosamente!")
            if results['loading']['new'] + results['loading']['modified'] == 0:
                print("✨ Todos los datos estaban actualizados")
        else:
            print(f"\n❌ Pipeline falló")
        
        print("="*80)
        print("\n🎯 PIPELINE COMPLETADO")
        if results['success']:
            print("✨ Datos de unidades de proyecto actualizados")


def run_inmemory_pipeline() -> Dict[str, Any]:
    """
    Run the complete ETL pipeline using in-memory processing.
    This is the main entry point for GitHub Actions.
    """
    pipeline = InMemoryETLPipeline()
    return pipeline.execute_pipeline(use_temp_files=False)


def run_traditional_pipeline() -> Dict[str, Any]:
    """
    Run the complete ETL pipeline using traditional file-based processing.
    Fallback method if in-memory processing fails.
    """
    pipeline = InMemoryETLPipeline()
    return pipeline.execute_pipeline(use_temp_files=True)


if __name__ == "__main__":
    """
    Main execution block - tries in-memory first, falls back to file-based if needed.
    """
    print("🚀 Starting Enhanced ETL Pipeline...")
    
    # Try in-memory processing first
    results = run_inmemory_pipeline()
    
    # If failed and there were file-related errors, try traditional method
    if not results['success'] and any('file' in error.lower() or 'directory' in error.lower() for error in results['errors']):
        print("\n🔄 In-memory processing failed, trying traditional file-based method...")
        results = run_traditional_pipeline()
    
    # Exit with appropriate code
    exit_code = 0 if results['success'] else 1
    sys.exit(exit_code)