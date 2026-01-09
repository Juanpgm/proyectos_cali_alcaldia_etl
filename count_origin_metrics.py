# -*- coding: utf-8 -*-
"""
Script para contar registros de origen y calcular métricas finales del pipeline.
"""

import sys
import pandas as pd
sys.path.append('extraction_app')
sys.path.append('transformation_app')
from data_extraction_unidades_proyecto import extract_unidades_proyecto_data

print('='*80)
print('CONTEO DE REGISTROS DE ORIGEN')
print('='*80)

# Extraer datos
df = extract_unidades_proyecto_data()

if df is not None:
    print(f'\n[OK] Total de registros en origen: {len(df)}')
    print(f'[OK] Total de columnas: {len(df.columns)}')
    
    # Verificar columnas clave
    if 'upid' in df.columns:
        upids_validos = df['upid'].notna().sum()
        print(f'[OK] Registros con UPID: {upids_validos}')
    
    # Contar por estado
    if 'estado' in df.columns:
        print('\n[INFO] Distribucion por estado:')
        estados = df['estado'].value_counts()
        for estado, count in estados.items():
            print(f'  - {estado}: {count}')
    
    # Mostrar columnas disponibles
    print(f'\n[INFO] Primeras columnas: {", ".join(df.columns[:10].tolist())}...')
    
    # Calcular métricas solicitadas sobre datos de ORIGEN
    print('\n' + '='*80)
    print('METRICAS DE ORIGEN (ANTES DE TRANSFORMACION)')
    print('='*80)
    
    # Contar UPIDs únicos
    if 'upid' in df.columns:
        upids_unicos = df['upid'].dropna().nunique()
        print(f'\n1. Numero total de unidades proyecto (UPID unicos): {upids_unicos}')
    
    # Total de intervenciones (registros/filas)
    print(f'2. Numero total de intervenciones (registros): {len(df)}')
    
    # Sumatoria de presupuesto_base
    if 'presupuesto_base' in df.columns:
        # Limpiar valores
        def clean_currency(val):
            if pd.isna(val):
                return 0
            val_str = str(val).replace('$', '').replace(',', '').replace('.', '').strip()
            try:
                return float(val_str)
            except:
                return 0
        
        df['presupuesto_limpio'] = df['presupuesto_base'].apply(clean_currency)
        suma_presupuesto = df['presupuesto_limpio'].sum()
        print(f'3. Sumatoria de presupuesto_base: ${suma_presupuesto:,.2f}')
    else:
        print('[WARNING] Columna "presupuesto_base" no encontrada')
    
    # Promedio de avance_obra
    if 'avance_obra' in df.columns:
        # Limpiar porcentajes
        def clean_percentage(val):
            if pd.isna(val):
                return None
            val_str = str(val).replace('%', '').replace(',', '.').strip()
            try:
                return float(val_str)
            except:
                return None
        
        df['avance_limpio'] = df['avance_obra'].apply(clean_percentage)
        valores_validos = df['avance_limpio'].dropna()
        if len(valores_validos) > 0:
            promedio_avance = valores_validos.mean()
            print(f'4. Promedio de avance_obra: {promedio_avance:.2f}%')
            print(f'   (Calculado sobre {len(valores_validos)} registros con valores validos)')
        else:
            print('[WARNING] No hay valores validos de avance_obra')
    else:
        print('[WARNING] Columna "avance_obra" no encontrada')
    
else:
    print('[ERROR] No se pudieron extraer datos del origen')
