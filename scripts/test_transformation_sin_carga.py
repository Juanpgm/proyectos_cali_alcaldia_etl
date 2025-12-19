# -*- coding: utf-8 -*-
"""
Script para probar la transformación con clustering sin carga a Firebase.

Este script ejecuta todo el pipeline de transformación incluyendo:
- Extracción de datos desde Google Drive
- Clustering geoespacial inteligente
- Transformación completa de datos
- Generación de GeoJSON
- Sin carga a Firebase

Uso:
    python scripts/test_transformation_sin_carga.py
"""

import os
import sys
from pathlib import Path

# Agregar rutas necesarias
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'transformation_app'))

from transformation_app.data_transformation_unidades_proyecto import transform_and_save_unidades_proyecto

def main():
    """Ejecuta transformación completa sin carga a Firebase."""
    print("="*80)
    print("PRUEBA DE TRANSFORMACIÓN CON CLUSTERING")
    print("Sin carga a Firebase")
    print("="*80)
    
    try:
        # Ejecutar transformación completa
        # use_extraction=True: Extrae datos frescos desde Google Drive
        # upload_to_s3=False: No subir a S3 para pruebas más rápidas
        gdf_result = transform_and_save_unidades_proyecto(
            data=None,
            use_extraction=True,
            upload_to_s3=False  # Desactivar S3 para prueba rápida
        )
        
        if gdf_result is not None:
            print("\n" + "="*80)
            print("✅ TRANSFORMACIÓN COMPLETADA EXITOSAMENTE")
            print("="*80)
            
            print(f"\n📊 Resumen:")
            print(f"   • Total registros: {len(gdf_result):,}")
            print(f"   • Total columnas: {len(gdf_result.columns)}")
            
            if 'upid' in gdf_result.columns:
                print(f"   • Unidades de proyecto (UPIDs únicos): {gdf_result['upid'].nunique():,}")
            
            if 'n_intervenciones' in gdf_result.columns:
                print(f"   • Promedio intervenciones/unidad: {gdf_result['n_intervenciones'].mean():.2f}")
            
            if 'geometry' in gdf_result.columns:
                geom_count = gdf_result['geometry'].notna().sum()
                print(f"   • Registros con geometría: {geom_count:,} ({geom_count/len(gdf_result)*100:.1f}%)")
            
            # Verificar archivo GeoJSON generado
            output_dir = Path(project_root) / 'app_outputs'
            geojson_file = output_dir / 'unidades_proyecto_transformed.geojson'
            
            if geojson_file.exists():
                size_mb = geojson_file.stat().st_size / (1024 * 1024)
                print(f"\n📁 Archivo GeoJSON generado:")
                print(f"   • Ruta: {geojson_file}")
                print(f"   • Tamaño: {size_mb:.2f} MB")
            
            # Mostrar ejemplo de unidades
            if 'upid' in gdf_result.columns and 'n_intervenciones' in gdf_result.columns:
                print(f"\n📋 Ejemplos de unidades agrupadas:")
                
                # Mostrar las 5 unidades con más intervenciones
                top_units = gdf_result.groupby('upid').agg({
                    'n_intervenciones': 'first',
                    'nombre_up': 'first'
                }).sort_values('n_intervenciones', ascending=False).head(5)
                
                for idx, (upid, row) in enumerate(top_units.iterrows(), 1):
                    print(f"   {idx}. {upid}: {row['nombre_up'][:50]} ({int(row['n_intervenciones'])} intervenciones)")
            
            return True
        else:
            print("\n❌ Error: La transformación falló")
            return False
            
    except Exception as e:
        print(f"\n❌ Error durante la transformación: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\n🧪 Iniciando prueba de transformación con clustering...\n")
    
    success = main()
    
    if success:
        print("\n✅ Prueba completada exitosamente")
        print("   El GeoJSON ha sido generado con la nueva estructura de clustering")
        print("   Revisa app_outputs/unidades_proyecto_transformed.geojson")
    else:
        print("\n❌ La prueba falló")
        print("   Revisa los errores anteriores")
    
    sys.exit(0 if success else 1)
