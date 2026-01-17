# -*- coding: utf-8 -*-
"""
Script para detectar y corregir coordenadas en formato DMS sin separadores
"""
import pandas as pd
import numpy as np
import re
from typing import Optional

def detect_and_convert_dms(value: float, coord_type: str = 'lat') -> Optional[float]:
    """
    Detecta y convierte coordenadas en formato DMS sin separadores
    
    Ejemplos:
    - 3435805556 → 3 + 43/60 + 58.0556/3600 = 3.73279322°
    - 337769444 → 33 + 77/60 + 69.444/3600 (inválido: minutos > 60)
    - 342508333 → 34 + 25/60 + 08.333/3600 (inválido: grados > 90)
    
    Args:
        value: Valor numérico potencialmente en formato DMS
        coord_type: 'lat' o 'lon' para rangos de validación
        
    Returns:
        Coordenada en formato decimal o None si es inválida
    """
    if pd.isna(value) or value is None:
        return None
    
    # Rangos válidos para Cali
    if coord_type == 'lat':
        valid_range = (2.5, 4.5)
    else:  # lon
        valid_range = (-77.5, -75.5)
    
    # Si ya está en rango válido, retornar tal cual
    if valid_range[0] <= value <= valid_range[1]:
        return value
    
    # Detectar si podría ser DMS sin separadores
    # DMS sin separadores: DDMMSS.SSS o DDDMMSS.SSS
    # Para latitud (Cali ~3°): valores entre 3000000 y 4000000
    # Para longitud (Cali ~-76°): valores entre -77000000 y -75000000
    
    abs_value = abs(value)
    
    # Caso 1: Valores muy grandes (probablemente DMS sin separadores)
    if abs_value >= 1000000:  # Al menos 7 dígitos antes del punto decimal
        str_value = str(int(abs_value))
        
        # Intentar extraer DMS
        if len(str_value) >= 6:
            # Últimos 2 dígitos: segundos enteros
            # Siguientes 2 dígitos: minutos
            # Resto: grados
            
            # Para lat: DDmmss (2 dígitos grados) o Dmmss (1 dígito)
            # Para lon: DDDmmss (3 dígitos grados) o DDmmss (2 dígitos)
            
            if coord_type == 'lat':
                # Probar con 2 dígitos de grados
                if len(str_value) >= 6:
                    grados = int(str_value[:-4])
                    minutos = int(str_value[-4:-2])
                    segundos = float(str_value[-2:] + '.' + str(value).split('.')[1] if '.' in str(value) else str_value[-2:])
                else:
                    return None
            else:  # lon
                # Probar con 2 o 3 dígitos de grados
                if len(str_value) >= 7:
                    grados = int(str_value[:-4])
                    minutos = int(str_value[-4:-2])
                    segundos = float(str_value[-2:] + '.' + str(value).split('.')[1] if '.' in str(value) else str_value[-2:])
                elif len(str_value) >= 6:
                    grados = int(str_value[:-4])
                    minutos = int(str_value[-4:-2])
                    segundos = float(str_value[-2:])
                else:
                    return None
            
            # Validar componentes DMS
            if minutos >= 60 or segundos >= 60:
                return None  # Formato inválido
            
            # Convertir a decimal
            decimal = grados + minutos/60.0 + segundos/3600.0
            
            # Restaurar signo
            if value < 0:
                decimal = -decimal
            
            # Validar rango
            if valid_range[0] <= decimal <= valid_range[1]:
                return decimal
    
    # Caso 2: Valores medianos (posiblemente DDmm o DDmm.mmmm)
    elif 1000 <= abs_value < 1000000:
        str_value = str(int(abs_value))
        
        if len(str_value) >= 4:
            # Últimos 2 dígitos: minutos
            # Resto: grados
            grados = int(str_value[:-2])
            minutos_decimal = float(str_value[-2:] + '.' + str(value).split('.')[1] if '.' in str(value) else str_value[-2:])
            
            if minutos_decimal >= 60:
                return None
            
            decimal = grados + minutos_decimal/60.0
            
            if value < 0:
                decimal = -decimal
            
            if valid_range[0] <= decimal <= valid_range[1]:
                return decimal
    
    return None  # No se pudo convertir


def fix_dataframe_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige coordenadas en formato DMS en un DataFrame
    
    Args:
        df: DataFrame con columnas 'lat' y 'lon'
        
    Returns:
        DataFrame con coordenadas corregidas
    """
    df = df.copy()
    
    stats = {
        'total': len(df),
        'lat_fixed': 0,
        'lon_fixed': 0,
        'both_fixed': 0
    }
    
    if 'lat' in df.columns and 'lon' in df.columns:
        for idx in df.index:
            lat_orig = df.loc[idx, 'lat']
            lon_orig = df.loc[idx, 'lon']
            
            lat_fixed = detect_and_convert_dms(lat_orig, 'lat')
            lon_fixed = detect_and_convert_dms(lon_orig, 'lon')
            
            if lat_fixed is not None and lat_fixed != lat_orig:
                df.loc[idx, 'lat'] = lat_fixed
                stats['lat_fixed'] += 1
            
            if lon_fixed is not None and lon_fixed != lon_orig:
                df.loc[idx, 'lon'] = lon_fixed
                stats['lon_fixed'] += 1
            
            if (lat_fixed is not None and lat_fixed != lat_orig and 
                lon_fixed is not None and lon_fixed != lon_orig):
                stats['both_fixed'] += 1
    
    print(f"\n[OK] Coordenadas corregidas:")
    print(f"   - Total registros: {stats['total']}")
    print(f"   - Latitudes corregidas: {stats['lat_fixed']}")
    print(f"   - Longitudes corregidas: {stats['lon_fixed']}")
    print(f"   - Ambas corregidas: {stats['both_fixed']}")
    
    return df


if __name__ == '__main__':
    # Pruebas con valores de ejemplo del análisis
    test_values = [
        (334387752299999975702528.0, 'lat'),  # Valor absurdo
        (3435805556.0, 'lat'),  # 34°35'80.556" → inválido (seg > 60)
        (342550000.0, 'lat'),   # 34°25'50" → 34.430555° (fuera de rango Cali)
        (342508333.0, 'lat'),   # 34°25'08.333" → 34.418981° (fuera de rango Cali)
        (341884.0, 'lat'),      # 34°18'84" → inválido o 3.41884°
        (-76569666670.0, 'lon'), # -765°69'66.667" → inválido
        (-7649858333.0, 'lon'),  # -764°98'58.333" → inválido
        (-7655863889.0, 'lon'),  # -765°58'63.889" → inválido
    ]
    
    print("="*80)
    print("PRUEBAS DE CONVERSIÓN DMS")
    print("="*80)
    
    for value, coord_type in test_values:
        result = detect_and_convert_dms(value, coord_type)
        print(f"\n{coord_type.upper()}: {value}")
        print(f"  Resultado: {result}")
        if result:
            print(f"  ✓ Conversión exitosa")
        else:
            print(f"  ✗ No se pudo convertir")
