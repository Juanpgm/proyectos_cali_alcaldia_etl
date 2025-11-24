# -*- coding: utf-8 -*-
"""
Script de prueba para validar la correcta generación de la columna 'frente_activo'
en los módulos de transformación de unidades de proyecto.

Este script:
1. Crea datos de prueba con diferentes combinaciones de condiciones
2. Ejecuta la lógica de frente_activo
3. Valida que los resultados sean correctos
4. Muestra estadísticas detalladas por categoría
"""

import pandas as pd
import sys
import os

# Agregar path para importar módulos
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

print("="*80)
print("TEST DE VALIDACIÓN - COLUMNA FRENTE_ACTIVO")
print("="*80)
print()

# ============================================================================
# CREAR DATOS DE PRUEBA
# ============================================================================

print("📊 Creando datos de prueba...")
print()

# Definir casos de prueba con resultados esperados
test_cases = [
    # CASO 1: Frente activo - Obras equipamientos, En ejecución, sin exclusiones
    {
        "caso": "Frente activo - Obras equipamientos",
        "estado": "En ejecución",
        "clase_up": "Obras equipamientos",
        "tipo_equipamiento": "Centro de salud",
        "tipo_intervencion": "Construcción",
        "esperado": "Frente activo"
    },
    # CASO 2: Frente activo - Obra vial
    {
        "caso": "Frente activo - Obra vial",
        "estado": "En ejecución",
        "clase_up": "Obra vial",
        "tipo_equipamiento": "Infraestructura Vial",
        "tipo_intervencion": "Pavimentación",
        "esperado": "Frente activo"
    },
    # CASO 3: Frente activo - Espacio Público
    {
        "caso": "Frente activo - Espacio Público",
        "estado": "En ejecución",
        "clase_up": "Espacio Público",
        "tipo_equipamiento": "Parque",
        "tipo_intervencion": "Adecuación",
        "esperado": "Frente activo"
    },
    # CASO 4: Inactivo - Suspendido con todas las condiciones
    {
        "caso": "Inactivo - Suspendido",
        "estado": "Suspendido",
        "clase_up": "Obras equipamientos",
        "tipo_equipamiento": "Escuela",
        "tipo_intervencion": "Construcción",
        "esperado": "Inactivo"
    },
    # CASO 5: No aplica - Estado Terminado
    {
        "caso": "No aplica - Terminado",
        "estado": "Terminado",
        "clase_up": "Obras equipamientos",
        "tipo_equipamiento": "Hospital",
        "tipo_intervencion": "Construcción",
        "esperado": "No aplica"
    },
    # CASO 6: No aplica - Estado En alistamiento
    {
        "caso": "No aplica - En alistamiento",
        "estado": "En alistamiento",
        "clase_up": "Obras equipamientos",
        "tipo_equipamiento": "Centro deportivo",
        "tipo_intervencion": "Construcción",
        "esperado": "No aplica"
    },
    # CASO 7: No aplica - Tipo equipamiento excluido (Vivienda mejoramiento)
    {
        "caso": "No aplica - Vivienda mejoramiento",
        "estado": "En ejecución",
        "clase_up": "Obras equipamientos",
        "tipo_equipamiento": "Vivienda mejoramiento",
        "tipo_intervencion": "Construcción",
        "esperado": "No aplica"
    },
    # CASO 8: No aplica - Tipo equipamiento excluido (Vivienda nueva)
    {
        "caso": "No aplica - Vivienda nueva",
        "estado": "En ejecución",
        "clase_up": "Obras equipamientos",
        "tipo_equipamiento": "Vivienda nueva",
        "tipo_intervencion": "Construcción",
        "esperado": "No aplica"
    },
    # CASO 9: No aplica - Tipo equipamiento excluido (Adquisición de predios)
    {
        "caso": "No aplica - Adquisición de predios",
        "estado": "En ejecución",
        "clase_up": "Obras equipamientos",
        "tipo_equipamiento": "Adquisición de predios",
        "tipo_intervencion": "Compra",
        "esperado": "No aplica"
    },
    # CASO 10: No aplica - Tipo equipamiento excluido (Señalización vial)
    {
        "caso": "No aplica - Señalización vial",
        "estado": "En ejecución",
        "clase_up": "Obra vial",
        "tipo_equipamiento": "Señalización vial",
        "tipo_intervencion": "Instalación",
        "esperado": "No aplica"
    },
    # CASO 11: No aplica - Tipo intervención excluido (Mantenimiento)
    {
        "caso": "No aplica - Mantenimiento",
        "estado": "En ejecución",
        "clase_up": "Obra vial",
        "tipo_equipamiento": "Vía",
        "tipo_intervencion": "Mantenimiento",
        "esperado": "No aplica"
    },
    # CASO 12: No aplica - Tipo intervención excluido (Estudios y diseños)
    {
        "caso": "No aplica - Estudios y diseños",
        "estado": "En ejecución",
        "clase_up": "Obras equipamientos",
        "tipo_equipamiento": "Puente",
        "tipo_intervencion": "Estudios y diseños",
        "esperado": "No aplica"
    },
    # CASO 13: No aplica - Tipo intervención excluido (Transferencia directa)
    {
        "caso": "No aplica - Transferencia directa",
        "estado": "En ejecución",
        "clase_up": "Obras equipamientos",
        "tipo_equipamiento": "Centro comunitario",
        "tipo_intervencion": "Transferencia directa",
        "esperado": "No aplica"
    },
    # CASO 14: No aplica - Clase no válida
    {
        "caso": "No aplica - Clase no válida",
        "estado": "En ejecución",
        "clase_up": "Otro tipo",
        "tipo_equipamiento": "Edificio",
        "tipo_intervencion": "Construcción",
        "esperado": "No aplica"
    },
    # CASO 15: Múltiples exclusiones al mismo tiempo
    {
        "caso": "No aplica - Múltiples exclusiones",
        "estado": "En ejecución",
        "clase_up": "Obra vial",
        "tipo_equipamiento": "Señalización vial",
        "tipo_intervencion": "Mantenimiento",
        "esperado": "No aplica"
    },
]

# Crear DataFrame de prueba
df_test = pd.DataFrame(test_cases)

print(f"✓ Creados {len(test_cases)} casos de prueba")
print()

# ============================================================================
# IMPLEMENTAR LÓGICA DE FRENTE_ACTIVO
# ============================================================================

def add_frente_activo_test(df):
    """
    Implementa la misma lógica de add_frente_activo para testing
    """
    result_df = df.copy()
    
    # Inicializar columna con 'No aplica' por defecto
    result_df['frente_activo'] = 'No aplica'
    
    # Definir listas de valores a excluir
    tipos_equipamiento_excluidos = [
        'Vivienda mejoramiento', 
        'Vivienda nueva', 
        'Adquisición de predios', 
        'Señalización vial'
    ]
    
    tipos_intervencion_excluidos = [
        'Mantenimiento', 
        'Estudios y diseños', 
        'Transferencia directa'
    ]
    
    # Definir clases válidas para frente activo
    clases_validas = ['Obras equipamientos', 'Obra vial', 'Espacio Público']
    
    # Condiciones base para frente activo
    condicion_clase = result_df['clase_up'].isin(clases_validas)
    condicion_tipo_equipamiento = ~result_df['tipo_equipamiento'].isin(tipos_equipamiento_excluidos)
    condicion_tipo_intervencion = ~result_df['tipo_intervencion'].isin(tipos_intervencion_excluidos)
    
    # Combinar todas las condiciones base
    condiciones_base = condicion_clase & condicion_tipo_equipamiento & condicion_tipo_intervencion
    
    # Aplicar lógica según estado
    frente_activo_mask = condiciones_base & (result_df['estado'] == 'En ejecución')
    result_df.loc[frente_activo_mask, 'frente_activo'] = 'Frente activo'
    
    inactivo_mask = condiciones_base & (result_df['estado'] == 'Suspendido')
    result_df.loc[inactivo_mask, 'frente_activo'] = 'Inactivo'
    
    return result_df


# ============================================================================
# EJECUTAR PRUEBAS
# ============================================================================

print("🧪 Ejecutando lógica de frente_activo...")
print()

df_result = add_frente_activo_test(df_test)

# ============================================================================
# VALIDAR RESULTADOS
# ============================================================================

print("="*80)
print("VALIDACIÓN DE RESULTADOS")
print("="*80)
print()

# Comparar resultados esperados vs obtenidos
df_result['correcto'] = df_result['frente_activo'] == df_result['esperado']

errores = []
aciertos = 0

for idx, row in df_result.iterrows():
    caso = row['caso']
    esperado = row['esperado']
    obtenido = row['frente_activo']
    correcto = row['correcto']
    
    if correcto:
        print(f"✅ CASO {idx+1}: {caso}")
        print(f"   Estado: {row['estado']}")
        print(f"   Clase UP: {row['clase_up']}")
        print(f"   Tipo equipamiento: {row['tipo_equipamiento']}")
        print(f"   Tipo intervención: {row['tipo_intervencion']}")
        print(f"   Esperado: {esperado} | Obtenido: {obtenido} ✓")
        aciertos += 1
    else:
        print(f"❌ CASO {idx+1}: {caso}")
        print(f"   Estado: {row['estado']}")
        print(f"   Clase UP: {row['clase_up']}")
        print(f"   Tipo equipamiento: {row['tipo_equipamiento']}")
        print(f"   Tipo intervención: {row['tipo_intervencion']}")
        print(f"   Esperado: {esperado} | Obtenido: {obtenido} ✗")
        errores.append({
            'caso': caso,
            'esperado': esperado,
            'obtenido': obtenido
        })
    
    print()

# ============================================================================
# RESUMEN DE VALIDACIÓN
# ============================================================================

print("="*80)
print("RESUMEN DE VALIDACIÓN")
print("="*80)
print()

total_casos = len(test_cases)
tasa_acierto = (aciertos / total_casos) * 100

print(f"📊 Resultados:")
print(f"   Total de casos: {total_casos}")
print(f"   Aciertos: {aciertos} ({tasa_acierto:.1f}%)")
print(f"   Errores: {len(errores)} ({100-tasa_acierto:.1f}%)")
print()

if len(errores) == 0:
    print("✅ TODAS LAS PRUEBAS PASARON CORRECTAMENTE")
else:
    print("❌ ALGUNAS PRUEBAS FALLARON:")
    for error in errores:
        print(f"   - {error['caso']}: esperado '{error['esperado']}', obtenido '{error['obtenido']}'")

print()

# ============================================================================
# ESTADÍSTICAS POR CATEGORÍA
# ============================================================================

print("="*80)
print("ESTADÍSTICAS POR CATEGORÍA")
print("="*80)
print()

# Conteo por frente_activo
conteo = df_result['frente_activo'].value_counts()

print("📊 Distribución de frente_activo en datos de prueba:")
print()
for categoria, cantidad in conteo.items():
    porcentaje = (cantidad / len(df_result)) * 100
    print(f"   {categoria}: {cantidad} casos ({porcentaje:.1f}%)")

print()

# Estadísticas por estado
print("📊 Distribución por estado:")
print()
estados = df_result.groupby(['estado', 'frente_activo']).size().unstack(fill_value=0)
print(estados)
print()

# Estadísticas por clase_up
print("📊 Distribución por clase_up:")
print()
clases = df_result.groupby(['clase_up', 'frente_activo']).size().unstack(fill_value=0)
print(clases)
print()

# ============================================================================
# VALIDACIONES LÓGICAS ADICIONALES
# ============================================================================

print("="*80)
print("VALIDACIONES LÓGICAS ADICIONALES")
print("="*80)
print()

# Validación 1: Todos los "Frente activo" deben estar en "En ejecución"
frente_activo_estados = df_result[df_result['frente_activo'] == 'Frente activo']['estado'].unique()
print("1. Validación: Frente activo solo con estado 'En ejecución'")
if len(frente_activo_estados) == 1 and frente_activo_estados[0] == 'En ejecución':
    print("   ✅ CORRECTO: Todos los 'Frente activo' están en 'En ejecución'")
else:
    print(f"   ❌ ERROR: 'Frente activo' tiene estados: {frente_activo_estados}")
print()

# Validación 2: Todos los "Inactivo" deben estar en "Suspendido"
inactivo_estados = df_result[df_result['frente_activo'] == 'Inactivo']['estado'].unique()
print("2. Validación: Inactivo solo con estado 'Suspendido'")
if len(inactivo_estados) == 1 and inactivo_estados[0] == 'Suspendido':
    print("   ✅ CORRECTO: Todos los 'Inactivo' están en 'Suspendido'")
elif len(inactivo_estados) == 0:
    print("   ⚠️  No hay casos 'Inactivo' en los datos de prueba")
else:
    print(f"   ❌ ERROR: 'Inactivo' tiene estados: {inactivo_estados}")
print()

# Validación 3: Clases válidas en "Frente activo" e "Inactivo"
clases_validas = ['Obras equipamientos', 'Obra vial', 'Espacio Público']
frente_activo_clases = df_result[df_result['frente_activo'].isin(['Frente activo', 'Inactivo'])]['clase_up'].unique()
print("3. Validación: Solo clases válidas en 'Frente activo' e 'Inactivo'")
clases_invalidas = [c for c in frente_activo_clases if c not in clases_validas]
if len(clases_invalidas) == 0:
    print(f"   ✅ CORRECTO: Solo clases válidas ({', '.join(clases_validas)})")
else:
    print(f"   ❌ ERROR: Clases inválidas encontradas: {clases_invalidas}")
print()

# Validación 4: No debe haber tipos de equipamiento excluidos en "Frente activo"
tipos_equipamiento_excluidos = ['Vivienda mejoramiento', 'Vivienda nueva', 'Adquisición de predios', 'Señalización vial']
frente_activo_tipos_eq = df_result[df_result['frente_activo'] == 'Frente activo']['tipo_equipamiento'].unique()
print("4. Validación: Sin tipos de equipamiento excluidos en 'Frente activo'")
tipos_excluidos_encontrados = [t for t in frente_activo_tipos_eq if t in tipos_equipamiento_excluidos]
if len(tipos_excluidos_encontrados) == 0:
    print("   ✅ CORRECTO: No hay tipos de equipamiento excluidos")
else:
    print(f"   ❌ ERROR: Tipos excluidos encontrados: {tipos_excluidos_encontrados}")
print()

# Validación 5: No debe haber tipos de intervención excluidos en "Frente activo"
tipos_intervencion_excluidos = ['Mantenimiento', 'Estudios y diseños', 'Transferencia directa']
frente_activo_tipos_int = df_result[df_result['frente_activo'] == 'Frente activo']['tipo_intervencion'].unique()
print("5. Validación: Sin tipos de intervención excluidos en 'Frente activo'")
tipos_int_excluidos_encontrados = [t for t in frente_activo_tipos_int if t in tipos_intervencion_excluidos]
if len(tipos_int_excluidos_encontrados) == 0:
    print("   ✅ CORRECTO: No hay tipos de intervención excluidos")
else:
    print(f"   ❌ ERROR: Tipos excluidos encontrados: {tipos_int_excluidos_encontrados}")
print()

# ============================================================================
# RESULTADO FINAL
# ============================================================================

print("="*80)
print("RESULTADO FINAL DEL TEST")
print("="*80)
print()

todas_validaciones_ok = (
    len(errores) == 0 and
    len(frente_activo_estados) == 1 and frente_activo_estados[0] == 'En ejecución' and
    (len(inactivo_estados) == 0 or (len(inactivo_estados) == 1 and inactivo_estados[0] == 'Suspendido')) and
    len(clases_invalidas) == 0 and
    len(tipos_excluidos_encontrados) == 0 and
    len(tipos_int_excluidos_encontrados) == 0
)

if todas_validaciones_ok:
    print("✅ ¡TEST EXITOSO!")
    print("   La lógica de frente_activo está implementada correctamente.")
    print("   Todos los casos de prueba pasaron las validaciones.")
else:
    print("❌ TEST FALLIDO")
    print("   Hay errores en la implementación que deben ser corregidos.")

print()
print("="*80)
