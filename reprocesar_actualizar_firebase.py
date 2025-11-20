# -*- coding: utf-8 -*-
"""
Script para re-procesar y actualizar TODOS los registros en Firebase con la nueva normalización.
Esto corregirá los estados "Finalizado" y "En liquidación" a "Terminado".
"""

import sys
import os

# Add paths
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'transformation_app'))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'load_app'))

from transformation_app.data_transformation_unidades_proyecto import transform_and_save_unidades_proyecto

def main():
    """
    Ejecuta el pipeline completo de ETL:
    1. Extrae datos desde Google Drive
    2. Transforma con la nueva normalización de estados
    3. Carga a Firebase (actualizando registros existentes)
    """
    
    print("="*80)
    print("RE-PROCESAMIENTO COMPLETO DE DATOS")
    print("Objetivo: Normalizar estados 'Finalizado' y 'En liquidación' a 'Terminado'")
    print("="*80)
    print()
    
    print("IMPORTANTE:")
    print("  - Este proceso extraerá datos frescos de Google Drive")
    print("  - Aplicará la normalización mejorada de estados")
    print("  - Actualizará TODOS los registros en Firebase")
    print("  - Los registros con upid existente se actualizarán")
    print()
    
    input("Presiona Enter para continuar o Ctrl+C para cancelar...")
    print()
    
    # Ejecutar el pipeline completo
    print("Iniciando pipeline ETL completo...")
    print("-" * 80)
    
    result = transform_and_save_unidades_proyecto(
        data=None,  # Force extraction from Google Drive
        use_extraction=True,  # Extract fresh data
        upload_to_s3=True  # Upload to S3 and Firebase
    )
    
    if result is not None:
        print()
        print("="*80)
        print("✅ RE-PROCESAMIENTO COMPLETADO")
        print("="*80)
        print()
        print("Los datos han sido actualizados en Firebase.")
        print("Por favor, recarga el frontend para ver los cambios.")
        print()
        print("Ahora deberías ver solo 3 estados:")
        print("  - En Alistamiento")
        print("  - En Ejecución")
        print("  - Terminado")
        print()
    else:
        print()
        print("="*80)
        print("❌ RE-PROCESAMIENTO FALLÓ")
        print("="*80)
        print()
        print("Revisa los errores arriba para más detalles.")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
