"""
Script de prueba para verificar la estructura de archivos en S3.
Simula la carga y lectura con la nueva estructura current/archive.
"""
import sys
import os
from pathlib import Path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.s3_uploader import S3Uploader
from utils.s3_downloader import S3Downloader

def test_upload_structure():
    """Prueba la estructura de carga a S3"""
    
    print("="*80)
    print("PRUEBA DE ESTRUCTURA DE ARCHIVOS EN S3")
    print("="*80)
    
    # Verificar que existe el archivo de salida
    output_file = Path("app_outputs/unidades_proyecto_transformed.geojson")
    
    if not output_file.exists():
        print(f"\n❌ Archivo no encontrado: {output_file}")
        print("   Ejecuta primero el pipeline de transformación")
        return False
    
    print(f"\n✓ Archivo encontrado: {output_file}")
    print(f"  Tamaño: {output_file.stat().st_size / (1024*1024):.2f} MB")
    
    # Inicializar uploader
    try:
        uploader = S3Uploader("aws_credentials.json")
        print("\n✓ S3Uploader inicializado correctamente")
    except Exception as e:
        print(f"\n❌ Error inicializando S3Uploader: {e}")
        return False
    
    # Subir archivo con nueva estructura
    print("\n" + "="*80)
    print("SUBIENDO ARCHIVO CON ESTRUCTURA current/archive")
    print("="*80)
    
    try:
        results = uploader.upload_transformed_data(output_file, archive=True)
        
        print("\n" + "="*80)
        print("RESULTADOS DE CARGA")
        print("="*80)
        
        for upload_type, success in results.items():
            status = "✓" if success else "✗"
            print(f"{status} {upload_type}: {'Exitoso' if success else 'Fallido'}")
        
        # Verificar lectura desde current
        print("\n" + "="*80)
        print("VERIFICANDO LECTURA DESDE CURRENT")
        print("="*80)
        
        downloader = S3Downloader("aws_credentials.json")
        s3_key = "up-geodata/unidades_proyecto_transformed/current/unidades_proyecto_transformed.geojson.gz"
        
        data = downloader.read_json_from_s3(s3_key)
        
        if data:
            features = data.get('features', [])
            print(f"\n✓ Lectura exitosa desde CURRENT")
            print(f"  - Total features: {len(features)}")
            print(f"  - Features con geometría: {len([f for f in features if f.get('geometry')])}")
            return True
        else:
            print(f"\n❌ No se pudo leer desde CURRENT")
            return False
            
    except Exception as e:
        print(f"\n❌ Error en la prueba: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    try:
        success = test_upload_structure()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️ Prueba interrumpida")
        sys.exit(1)
