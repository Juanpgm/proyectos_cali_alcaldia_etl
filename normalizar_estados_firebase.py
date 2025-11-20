# -*- coding: utf-8 -*-
"""
Script para normalizar estados directamente en Firebase sin necesidad de re-procesar desde Google Drive.
Lee los documentos existentes en Firebase, normaliza sus estados, y actualiza solo los que cambiaron.
"""

import sys
import os
from typing import Dict, Any
from datetime import datetime

# Add paths
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'load_app'))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database'))

from database.config import get_firestore_client
from load_app.data_loading_unidades_proyecto import normalize_estado_value
from tqdm import tqdm


def normalize_estados_in_firebase(collection_name: str = "unidades_proyecto", dry_run: bool = False):
    """
    Normaliza estados directamente en Firebase.
    
    Args:
        collection_name: Nombre de la colección en Firebase
        dry_run: Si es True, solo muestra qué cambiaría sin hacer cambios reales
    """
    print("="*80)
    print("NORMALIZACIÓN DE ESTADOS EN FIREBASE")
    print("="*80)
    print(f"Colección: {collection_name}")
    print(f"Modo: {'DRY RUN (sin cambios)' if dry_run else 'ACTUALIZACIÓN REAL'}")
    print()
    
    # Conectar a Firestore
    db = get_firestore_client()
    if not db:
        print("❌ Error: No se pudo conectar a Firestore")
        return False
    
    collection_ref = db.collection(collection_name)
    
    # Obtener todos los documentos
    print("📥 Leyendo documentos de Firebase...")
    docs = list(collection_ref.stream())
    total_docs = len(docs)
    print(f"   Total de documentos: {total_docs}")
    print()
    
    # Estadísticas
    stats = {
        'total': total_docs,
        'needs_update': 0,
        'updated': 0,
        'unchanged': 0,
        'errors': 0,
        'estado_changes': {}  # Original -> Normalized
    }
    
    # Procesar documentos
    print("🔄 Procesando documentos...")
    with tqdm(total=total_docs, desc="Normalizando estados") as pbar:
        for doc in docs:
            try:
                doc_id = doc.id
                data = doc.to_dict()
                
                # Obtener estado actual y avance_obra
                current_estado = data.get('estado')
                avance_obra = data.get('avance_obra')
                
                if current_estado is None:
                    stats['unchanged'] += 1
                    pbar.update(1)
                    continue
                
                # Normalizar estado
                normalized_estado = normalize_estado_value(current_estado, avance_obra)
                
                # Verificar si cambió
                if normalized_estado != current_estado:
                    stats['needs_update'] += 1
                    
                    # Registrar el cambio
                    change_key = f"{current_estado} → {normalized_estado}"
                    stats['estado_changes'][change_key] = stats['estado_changes'].get(change_key, 0) + 1
                    
                    if not dry_run:
                        # Actualizar documento en Firebase
                        doc_ref = collection_ref.document(doc_id)
                        doc_ref.update({
                            'estado': normalized_estado,
                            'updated_at': datetime.now().isoformat()
                        })
                        stats['updated'] += 1
                else:
                    stats['unchanged'] += 1
                
                pbar.update(1)
                
            except Exception as e:
                stats['errors'] += 1
                print(f"\n⚠️ Error procesando documento {doc_id}: {e}")
                pbar.update(1)
    
    # Mostrar resultados
    print()
    print("="*80)
    print("RESULTADOS")
    print("="*80)
    print(f"📊 Total de documentos procesados: {stats['total']}")
    print(f"   ✏️  Necesitan actualización: {stats['needs_update']}")
    
    if not dry_run:
        print(f"   ✅ Actualizados: {stats['updated']}")
    
    print(f"   ✓  Sin cambios: {stats['unchanged']}")
    print(f"   ❌ Errores: {stats['errors']}")
    print()
    
    if stats['estado_changes']:
        print("📝 Cambios de estado detectados:")
        print("-" * 80)
        for change, count in sorted(stats['estado_changes'].items(), key=lambda x: x[1], reverse=True):
            print(f"   {change}: {count} documentos")
        print()
    
    # Validar estados finales
    print("🔍 Validando estados finales...")
    final_docs = list(collection_ref.stream())
    estado_distribution = {}
    invalid_estados = []
    
    valid_estados = {'En Alistamiento', 'En Ejecución', 'Terminado'}
    
    for doc in final_docs:
        data = doc.to_dict()
        estado = data.get('estado')
        if estado:
            estado_distribution[estado] = estado_distribution.get(estado, 0) + 1
            if estado not in valid_estados:
                invalid_estados.append((doc.id, estado))
    
    print()
    print("📊 Distribución final de estados:")
    print("-" * 80)
    for estado, count in sorted(estado_distribution.items(), key=lambda x: x[1], reverse=True):
        symbol = '✅' if estado in valid_estados else '❌'
        print(f"   {symbol} {estado}: {count}")
    print()
    
    if invalid_estados:
        print(f"❌ ADVERTENCIA: Se encontraron {len(invalid_estados)} documentos con estados inválidos:")
        for doc_id, estado in invalid_estados[:10]:  # Mostrar solo los primeros 10
            print(f"   - {doc_id}: '{estado}'")
        if len(invalid_estados) > 10:
            print(f"   ... y {len(invalid_estados) - 10} más")
        print()
        return False
    else:
        print("✅ ÉXITO: Todos los estados son válidos")
        print()
        return True


def main():
    """Función principal"""
    
    print()
    print("Este script normalizará los estados en Firebase:")
    print("  • 'Finalizado' → 'Terminado'")
    print("  • 'En liquidación' → 'Terminado'")
    print("  • Otros estados inválidos → Estados válidos correspondientes")
    print()
    
    # Primero ejecutar en modo dry-run
    print("PASO 1: Simulación (Dry Run)")
    print("-" * 80)
    normalize_estados_in_firebase(dry_run=True)
    
    print()
    print("="*80)
    response = input("¿Deseas aplicar estos cambios a Firebase? (si/no): ").strip().lower()
    
    if response in ['si', 's', 'yes', 'y']:
        print()
        print("PASO 2: Aplicando cambios reales")
        print("-" * 80)
        success = normalize_estados_in_firebase(dry_run=False)
        
        if success:
            print()
            print("="*80)
            print("✅ NORMALIZACIÓN COMPLETADA EXITOSAMENTE")
            print("="*80)
            print()
            print("Por favor, recarga el frontend para ver los cambios.")
            print("Ahora deberías ver solo 3 estados en la leyenda.")
        else:
            print()
            print("="*80)
            print("⚠️ NORMALIZACIÓN COMPLETADA CON ADVERTENCIAS")
            print("="*80)
            print("Revisa los mensajes arriba para más detalles.")
    else:
        print()
        print("❌ Operación cancelada por el usuario")
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
