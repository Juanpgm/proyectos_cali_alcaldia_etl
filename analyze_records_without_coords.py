# -*- coding: utf-8 -*-
"""
Analizar registros sin coordenadas por tipo y secretaría
"""
import json
import pandas as pd

# Leer JSON extraído
json_path = 'cloud_functions/transformation_app/app_inputs/unidades_proyecto_input/unidades_proyecto.json'
with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

df = pd.DataFrame(data)

print("="*80)
print("ANÁLISIS DE REGISTROS SIN COORDENADAS EN JSON EXTRAÍDO")
print("="*80)

# Convertir a numérico
df['lat_num'] = pd.to_numeric(df['lat'], errors='coerce')
df['lon_num'] = pd.to_numeric(df['lon'], errors='coerce')

# Identificar registros sin coordenadas
sin_coords = df[(df['lat_num'].isna()) | (df['lon_num'].isna())]

print(f"\n1. Total registros: {len(df)}")
print(f"   Registros SIN coordenadas: {len(sin_coords)} ({len(sin_coords)*100/len(df):.1f}%)")

if len(sin_coords) > 0:
    # Analizar por centro gestor
    por_centro = sin_coords.groupby('nombre_centro_gestor').size().sort_values(ascending=False)
    
    print(f"\n2. Distribución por Centro Gestor:")
    for centro, count in por_centro.items():
        print(f"   - {centro}: {count} ({count*100/len(sin_coords):.1f}%)")
    
    # Analizar por tipo de intervención si existe
    if 'tipo_intervencion' in sin_coords.columns:
        por_tipo = sin_coords.groupby('tipo_intervencion').size().sort_values(ascending=False)
        print(f"\n3. Distribución por Tipo de Intervención:")
        for tipo, count in por_tipo.items():
            print(f"   - {tipo}: {count} ({count*100/len(sin_coords):.1f}%)")
    
    # Analizar por tipo de equipamiento si existe
    if 'tipo_equipamiento' in sin_coords.columns:
        por_equip = sin_coords[sin_coords['tipo_equipamiento'].notna()].groupby('tipo_equipamiento').size().sort_values(ascending=False)
        if len(por_equip) > 0:
            print(f"\n4. Distribución por Tipo de Equipamiento:")
            for equip, count in por_equip.head(10).items():
                print(f"   - {equip}: {count}")
    
    # Buscar patrones en nombres
    print(f"\n5. Palabras clave en nombres (primeras 20):")
    palabras_clave = ['subsidio', 'transferencia', 'apoyo', 'mejoramiento', 'programa']
    for palabra in palabras_clave:
        if 'nombre_largo' in sin_coords.columns:
            count = sin_coords['nombre_largo'].str.contains(palabra, case=False, na=False).sum()
            if count > 0:
                print(f"   - '{palabra}': {count} registros")
    
    # Mostrar ejemplos
    print(f"\n6. Ejemplos de registros sin coordenadas:")
    columnas = ['nombre_largo' if 'nombre_largo' in sin_coords.columns else 'nombre_corto',
                'nombre_centro_gestor', 'tipo_intervencion' if 'tipo_intervencion' in sin_coords.columns else None]
    columnas = [c for c in columnas if c]
    
    for idx, row in sin_coords.head(15).iterrows():
        print(f"\n   [{idx+1}]")
        for col in columnas:
            if col in row:
                val = str(row[col])[:80]
                print(f"     {col}: {val}")
        print(f"     lat: {row['lat']}")
        print(f"     lon: {row['lon']}")

# Analizar registros CON coordenadas válidas
con_coords = df[(df['lat_num'].notna()) & (df['lon_num'].notna()) &
               (df['lat_num'] >= 2.5) & (df['lat_num'] <= 4.5) &
               (df['lon_num'] >= -77.5) & (df['lon_num'] <= -75.5)]

print(f"\n\n7. Registros CON coordenadas válidas: {len(con_coords)} ({len(con_coords)*100/len(df):.1f}%)")

if len(con_coords) > 0:
    por_centro_valido = con_coords.groupby('nombre_centro_gestor').size().sort_values(ascending=False)
    print(f"\n8. Distribución CON coordenadas por Centro Gestor:")
    for centro, count in por_centro_valido.items():
        print(f"   - {centro}: {count}")
