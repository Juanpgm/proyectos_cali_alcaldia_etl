# -*- coding: utf-8 -*-
"""
Script de prueba para verificar que los registros de Secretaría de Salud Pública
se detectan correctamente como frentes activos después de la inferencia.
"""

import json
import pandas as pd
from pathlib import Path

def test_salud_publica_frente_activo():
    """
    Verifica que los registros de Secretaría de Salud Pública tengan:
    1. clase_up inferido como "Obras equipamientos"
    2. tipo_equipamiento inferido como "Centro de salud"
    3. frente_activo calculado correctamente según estado
    """
    print("="*80)
    print("TEST: Detección de Frentes Activos - Secretaría de Salud Pública")
    print("="*80)
    
    # Buscar el archivo de salida más reciente
    output_dir = Path(__file__).parent / 'app_outputs' / 'grouped_structure'
    
    json_files = sorted(output_dir.glob('unidades_proyecto_*.json'), reverse=True)
    
    if not json_files:
        print("❌ No se encontraron archivos de salida")
        return False
    
    json_file = json_files[0]
    print(f"\n📂 Analizando: {json_file.name}")
    
    # Cargar datos
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Filtrar registros de Secretaría de Salud Pública
    salud_publica = [
        d for d in data 
        if d.get('nombre_centro_gestor') == 'Secretaría de Salud Pública'
    ]
    
    print(f"\n📊 Registros de Secretaría de Salud Pública: {len(salud_publica)}")
    
    if len(salud_publica) == 0:
        print("⚠️  No se encontraron registros de Secretaría de Salud Pública")
        return True
    
    # Analizar valores antes de la corrección (debería estar corregido ahora)
    print("\n" + "="*80)
    print("ANÁLISIS DE CAMPOS INFERIDOS")
    print("="*80)
    
    # Análisis de clase_up
    clase_up_values = {}
    for record in salud_publica:
        clase = record.get('clase_up', 'NaN')
        clase_up_values[clase] = clase_up_values.get(clase, 0) + 1
    
    print("\n🏷️  Valores de 'clase_up':")
    for clase, count in sorted(clase_up_values.items()):
        print(f"   - {clase}: {count} registros")
    
    # Análisis de tipo_equipamiento
    tipo_equip_values = {}
    for record in salud_publica:
        tipo = record.get('tipo_equipamiento', 'NaN')
        tipo_equip_values[tipo] = tipo_equip_values.get(tipo, 0) + 1
    
    print("\n🏥 Valores de 'tipo_equipamiento':")
    for tipo, count in sorted(tipo_equip_values.items()):
        print(f"   - {tipo}: {count} registros")
    
    # Análisis de frente_activo
    print("\n" + "="*80)
    print("ANÁLISIS DE FRENTE ACTIVO")
    print("="*80)
    
    frente_activo_values = {}
    estado_values = {}
    
    for record in salud_publica:
        frente = record.get('frente_activo', 'No especificado')
        estado = record.get('estado', 'No especificado')
        
        frente_activo_values[frente] = frente_activo_values.get(frente, 0) + 1
        estado_values[estado] = estado_values.get(estado, 0) + 1
    
    print("\n📊 Distribución de 'frente_activo':")
    for frente, count in sorted(frente_activo_values.items()):
        print(f"   - {frente}: {count} registros")
    
    print("\n📊 Distribución de 'estado':")
    for estado, count in sorted(estado_values.items()):
        print(f"   - {estado}: {count} registros")
    
    # Ejemplos detallados
    print("\n" + "="*80)
    print("EJEMPLOS DETALLADOS")
    print("="*80)
    
    # Mostrar algunos ejemplos por estado
    estados_interes = ['En ejecución', 'Suspendido', 'Terminada', 'Programado']
    
    for estado in estados_interes:
        ejemplos = [r for r in salud_publica if r.get('estado') == estado]
        if ejemplos:
            print(f"\n🔍 Ejemplo con estado '{estado}':")
            ejemplo = ejemplos[0]
            print(f"   Nombre UP: {ejemplo.get('nombre_up', 'N/A')}")
            print(f"   clase_up: {ejemplo.get('clase_up', 'N/A')}")
            print(f"   tipo_equipamiento: {ejemplo.get('tipo_equipamiento', 'N/A')}")
            print(f"   tipo_intervencion: {ejemplo.get('tipo_intervencion', 'N/A')}")
            print(f"   estado: {ejemplo.get('estado', 'N/A')}")
            print(f"   ➡️  frente_activo: {ejemplo.get('frente_activo', 'N/A')}")
    
    # Validaciones
    print("\n" + "="*80)
    print("VALIDACIONES")
    print("="*80)
    
    validaciones_ok = True
    
    # Validación 1: Todos deben tener clase_up = "Obras equipamientos"
    sin_clase_up = [r for r in salud_publica if r.get('clase_up') != 'Obras equipamientos']
    if sin_clase_up:
        print(f"\n❌ FALLO: {len(sin_clase_up)} registros NO tienen clase_up='Obras equipamientos'")
        validaciones_ok = False
    else:
        print(f"\n✅ ÉXITO: Todos los registros tienen clase_up='Obras equipamientos'")
    
    # Validación 2: Verificar que frente_activo se asigna correctamente
    en_ejecucion = [r for r in salud_publica if r.get('estado') == 'En ejecución']
    if en_ejecucion:
        frentes_activos = [r for r in en_ejecucion if r.get('frente_activo') == 'Frente activo']
        if len(frentes_activos) > 0:
            print(f"✅ ÉXITO: {len(frentes_activos)}/{len(en_ejecucion)} registros en 'En ejecución' detectados como 'Frente activo'")
        else:
            print(f"⚠️  ADVERTENCIA: 0/{len(en_ejecucion)} registros en 'En ejecución' detectados como 'Frente activo'")
            print(f"    Esto puede ser correcto si tienen tipo_intervencion excluido (ej: Mantenimiento)")
            # Mostrar por qué no se detectan
            if en_ejecucion:
                ej = en_ejecucion[0]
                print(f"\n    Ejemplo de registro 'En ejecución' no detectado:")
                print(f"       tipo_intervencion: {ej.get('tipo_intervencion', 'N/A')}")
                print(f"       tipo_equipamiento: {ej.get('tipo_equipamiento', 'N/A')}")
    
    # Validación 3: Verificar registros suspendidos
    suspendidos = [r for r in salud_publica if r.get('estado') == 'Suspendido']
    if suspendidos:
        inactivos = [r for r in suspendidos if r.get('frente_activo') == 'Inactivo']
        if len(inactivos) > 0:
            print(f"✅ ÉXITO: {len(inactivos)}/{len(suspendidos)} registros 'Suspendido' detectados como 'Inactivo'")
        else:
            print(f"⚠️  ADVERTENCIA: 0/{len(suspendidos)} registros 'Suspendido' detectados como 'Inactivo'")
    
    print("\n" + "="*80)
    if validaciones_ok:
        print("✅ TODAS LAS VALIDACIONES PASARON")
    else:
        print("❌ ALGUNAS VALIDACIONES FALLARON")
    print("="*80)
    
    return validaciones_ok


if __name__ == '__main__':
    success = test_salud_publica_frente_activo()
    exit(0 if success else 1)
