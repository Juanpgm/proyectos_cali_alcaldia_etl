import json
import pandas as pd
import re

# Cargar ambos archivos
with open('transformation_app/app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json', 'r', encoding='utf-8') as f:
    datos_proyectos = json.load(f)

with open('transformation_app/app_outputs/emprestito_outputs/contratos_secop_emprestito_transformed.json', 'r', encoding='utf-8') as f:
    contratos = json.load(f)

df_proyectos = pd.DataFrame(datos_proyectos)
df_contratos = pd.DataFrame(contratos)

print('🔍 Buscando patrones de mapeo...')

# Extraer códigos de las referencias de contrato
referencias = df_contratos['referencia_contrato'].tolist()
print(f'Referencias de contrato: {referencias[:3]}')

# Extraer códigos BP de proyectos
bps = df_proyectos['bp'].tolist()
print(f'Códigos BP de proyectos: {bps[:3]}')

# Buscar coincidencias por descripción de proceso
print(f'\n📝 Intentando mapeo por descripción de proceso...')
for i, contrato in enumerate(contratos[:3]):
    descripcion = contrato['descripcion_proceso']
    print(f'Contrato {i+1}: {descripcion[:100]}...')
    
    # Buscar palabras clave en nombres de proyectos
    palabras_clave = descripcion.lower().split()[:5]  # Primeras 5 palabras
    for palabra in palabras_clave:
        if len(palabra) > 4:  # Solo palabras significativas
            coincidencias = df_proyectos[df_proyectos['nombre_proyecto'].str.lower().str.contains(palabra, na=False)]
            if len(coincidencias) > 0:
                print(f'  Palabra "{palabra}" encontrada en {len(coincidencias)} proyectos')
                if len(coincidencias) <= 3:
                    for _, proyecto in coincidencias.iterrows():
                        print(f'    BPIN: {proyecto["bpin"]} - {proyecto["nombre_proyecto"][:80]}...')
                break

print(f'\n📊 Resumen:')
print(f'- Contratos con BPIN válido: {len(df_contratos[df_contratos["bpin"] > 0])}')
print(f'- Contratos sin BPIN: {len(df_contratos[df_contratos["bpin"] == 0])}')
print(f'- Total proyectos disponibles: {len(df_proyectos)}')

# Analizar contratos sin BPIN
contratos_sin_bpin = df_contratos[df_contratos['bpin'] == 0]
print(f'\n🔍 Análisis de contratos sin BPIN ({len(contratos_sin_bpin)} contratos):')
for i, (_, contrato) in enumerate(contratos_sin_bpin.iterrows()):
    if i < 5:  # Solo primeros 5
        print(f'{i+1}. Ref: {contrato["referencia_contrato"]}')
        print(f'   Desc: {contrato["descripcion_proceso"][:80]}...')