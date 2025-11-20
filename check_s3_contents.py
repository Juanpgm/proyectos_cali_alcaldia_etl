"""Verificar contenido del bucket S3"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.s3_downloader import S3Downloader

def check_s3_bucket():
    """Lista el contenido del bucket S3"""
    
    print("="*80)
    print("CONTENIDO DEL BUCKET S3")
    print("="*80)
    
    try:
        downloader = S3Downloader("aws_credentials.json")
        
        # Listar todos los objetos en el bucket
        print(f"\nBucket: {downloader.bucket_name}")
        print("-"*80)
        
        s3_client = downloader.s3_client
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=downloader.bucket_name)
        
        objects = []
        for page in pages:
            if 'Contents' in page:
                objects.extend(page['Contents'])
        
        if not objects:
            print("⚠️ El bucket está vacío")
            return
        
        print(f"\nTotal de objetos: {len(objects)}\n")
        
        # Agrupar por carpeta
        folders = {}
        for obj in objects:
            key = obj['Key']
            parts = key.split('/')
            folder = parts[0] if len(parts) > 1 else 'root'
            
            if folder not in folders:
                folders[folder] = []
            folders[folder].append({
                'key': key,
                'size': obj['Size'],
                'modified': obj['LastModified']
            })
        
        # Mostrar por carpeta
        for folder, files in sorted(folders.items()):
            print(f"\n📁 {folder}/")
            print("-"*80)
            for file in files:
                size_mb = file['size'] / (1024 * 1024)
                print(f"  {file['key']}")
                print(f"    Size: {size_mb:.2f} MB")
                print(f"    Modified: {file['modified']}")
        
        # Buscar específicamente unidades_proyecto
        print("\n" + "="*80)
        print("BÚSQUEDA: unidades_proyecto")
        print("="*80)
        
        found = [obj for obj in objects if 'unidades_proyecto' in obj['Key'].lower()]
        if found:
            print(f"\n✅ Encontrados {len(found)} archivos:")
            for obj in found:
                print(f"  {obj['Key']}")
        else:
            print("\n⚠️ No se encontraron archivos con 'unidades_proyecto'")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_s3_bucket()
