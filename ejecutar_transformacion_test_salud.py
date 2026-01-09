# -*- coding: utf-8 -*-
"""
Script simplificado para ejecutar solo la transformación y probar la corrección
de inferencia de valores para Secretaría de Salud Pública.
"""

import sys
import os
from pathlib import Path

# Agregar rutas necesarias
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transformation_app.data_transformation_unidades_proyecto import transform_and_save_unidades_proyecto

def main():
    """Ejecuta solo la transformación para probar la inferencia"""
    print("="*80)
    print("PRUEBA DE TRANSFORMACIÓN - Inferencia Secretaría de Salud Pública")
    print("="*80)
    
    # Buscar el archivo de extracción más reciente
    extraction_dir = Path(__file__).parent / 'app_outputs' / 'extraction'
    
    # Buscar archivos CSV en extraction
    csv_files = sorted(extraction_dir.glob('unidades_proyecto_*.csv'), reverse=True)
    
    if not csv_files:
        print("❌ No se encontró archivo de extracción")
        print("   Por favor ejecuta primero la extracción o usa un archivo existente")
        return False
    
    input_file = csv_files[0]
    print(f"\n📂 Usando archivo: {input_file.name}")
    print(f"   Ruta: {input_file}")
    
    # Ejecutar transformación
    print("\n🔄 Ejecutando transformación...")
    try:
        result = transform_and_save_unidades_proyecto(
            use_extraction=True,
            input_file=str(input_file)
        )
        
        if result:
            print(f"\n✅ Transformación completada exitosamente")
            
            # Ahora ejecutar el test
            print("\n" + "="*80)
            print("EJECUTANDO TEST DE VALIDACIÓN")
            print("="*80)
            
            import subprocess
            test_result = subprocess.run(
                [sys.executable, 'test_salud_publica_frente_activo.py'],
                cwd=Path(__file__).parent,
                capture_output=False
            )
            
            return test_result.returncode == 0
        else:
            print(f"\n❌ Transformación falló")
            return False
            
    except Exception as e:
        print(f"\n❌ Error durante transformación: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
