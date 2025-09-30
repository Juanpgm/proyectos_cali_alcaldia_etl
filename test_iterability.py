#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para probar que las listas de referencia_proceso y referencia_contrato son iterables
"""

import json

def test_iterability():
    """Probar que las listas son iterables correctamente"""
    
    # Cargar datos
    with open('transformation_app/app_inputs/indice_procesos_emprestito/indice_procesos.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print('🔍 Probando iterabilidad de las listas:')
    
    test_count = 0
    total_proceso_items = 0
    total_contrato_items = 0
    
    for item in data:
        if test_count >= 5:  # Probar solo los primeros 5 ejemplos
            break
        
        ref_proceso = item.get('referencia_proceso', [])
        ref_contrato = item.get('referencia_contrato', [])
        
        if len(ref_proceso) > 0 or len(ref_contrato) > 0:
            print(f'\n📋 BP: {item.get("bp", "N/A")}')
            
            if len(ref_proceso) > 0:
                print(f'   🔄 Proceso ({len(ref_proceso)} elementos):')
                for i, ref in enumerate(ref_proceso):
                    print(f'      [{i}]: {ref}')
                    total_proceso_items += 1
            
            if len(ref_contrato) > 0:
                print(f'   🔄 Contrato ({len(ref_contrato)} elementos):')
                for i, ref in enumerate(ref_contrato):
                    print(f'      [{i}]: {ref}')
                    total_contrato_items += 1
            
            test_count += 1
    
    # Estadísticas generales
    print(f'\n📊 Estadísticas de iterabilidad:')
    print(f'   📋 Ejemplos probados: {test_count}')
    print(f'   🔄 Total elementos de proceso iterados: {total_proceso_items}')
    print(f'   🔄 Total elementos de contrato iterados: {total_contrato_items}')
    
    # Probar operaciones comunes con listas
    print(f'\n🧪 Probando operaciones de lista:')
    
    for item in data:
        ref_proceso = item.get('referencia_proceso', [])
        ref_contrato = item.get('referencia_contrato', [])
        
        if len(ref_contrato) > 1:  # Encontrar un ejemplo con múltiples elementos
            print(f'   📋 BP: {item.get("bp", "N/A")}')
            print(f'   📏 Longitud de contrato: {len(ref_contrato)}')
            print(f'   🔍 Primer elemento: {ref_contrato[0]}')
            print(f'   🔍 Último elemento: {ref_contrato[-1]}')
            print(f'   🔄 Iteración con enumerate:')
            for i, ref in enumerate(ref_contrato):
                print(f'      Posición {i}: {ref}')
            
            # Probar slicing
            if len(ref_contrato) > 2:
                print(f'   ✂️ Slice [1:]: {ref_contrato[1:]}')
            
            break
    
    print(f'\n✅ Prueba completada: Las listas son iterables correctamente')
    return True

if __name__ == "__main__":
    test_iterability()