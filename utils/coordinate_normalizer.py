#!/usr/bin/env python3
"""
Utilidad para normalizar y corregir formatos de coordenadas geográficas.
Maneja múltiples formatos numéricos y errores comunes en datos de Cali, Colombia.
"""

import pandas as pd
import re
from typing import Tuple, Optional


class CoordinateNormalizer:
    """
    Normaliza y corrige coordenadas geográficas para Cali, Colombia.
    
    Rangos esperados para Cali:
    - Latitud: 3.0 a 4.0 (hemisferio norte)
    - Longitud: -77.0 a -76.0 (hemisferio oeste)
    """
    
    # Rangos válidos para Cali y área metropolitana
    CALI_LAT_MIN = 3.0
    CALI_LAT_MAX = 4.0
    CALI_LON_MIN = -77.0
    CALI_LON_MAX = -76.0
    
    # Rangos ampliados para validación (con margen)
    LAT_MIN_EXTENDED = 2.5
    LAT_MAX_EXTENDED = 4.5
    LON_MIN_EXTENDED = -77.5
    LON_MAX_EXTENDED = -75.5
    
    @staticmethod
    def normalize_decimal_format(value) -> Optional[float]:
        """
        Normaliza formato decimal: maneja comas, puntos, espacios.
        
        Ejemplos:
        - "3,440123" -> 3.440123
        - "3.440.123" -> 3440.123
        - "-76,532927" -> -76.532927
        - "3 440 123" -> 3440.123
        """
        if value is None or pd.isna(value):
            return None
        
        # Convertir a string
        value_str = str(value).strip()
        
        # Vacíos
        if value_str in ['', 'nan', 'None', 'null', 'NaN']:
            return None
        
        try:
            # Caso 1: Ya es un número válido
            try:
                return float(value_str)
            except ValueError:
                pass
            
            # Caso 2: Tiene formato europeo (coma como decimal)
            # Ejemplo: "3,440123" o "-76,532"
            if ',' in value_str and '.' not in value_str:
                value_str = value_str.replace(',', '.')
                return float(value_str)
            
            # Caso 3: Tiene separadores de miles y decimal
            # Ejemplo: "1.234.567,89" (europeo) o "1,234,567.89" (americano)
            if ',' in value_str and '.' in value_str:
                # Detectar cuál es el separador decimal (el último)
                last_comma = value_str.rfind(',')
                last_dot = value_str.rfind('.')
                
                if last_comma > last_dot:
                    # Formato europeo: punto = miles, coma = decimal
                    value_str = value_str.replace('.', '').replace(',', '.')
                else:
                    # Formato americano: coma = miles, punto = decimal
                    value_str = value_str.replace(',', '')
                
                return float(value_str)
            
            # Caso 4: Espacios como separadores
            if ' ' in value_str:
                value_str = value_str.replace(' ', '')
                return float(value_str)
            
            # Intentar conversión directa
            return float(value_str)
            
        except (ValueError, TypeError, AttributeError):
            return None
    
    @staticmethod
    def fix_longitude_sign(lon: float, lat: float = None) -> float:
        """
        Corrige el signo de la longitud para Cali (debe ser negativa).
        
        Args:
            lon: Longitud a corregir
            lat: Latitud (opcional, para validación de contexto)
        
        Returns:
            Longitud corregida
        """
        if lon is None or pd.isna(lon):
            return None
        
        # Cali está en el hemisferio oeste: longitud debe ser negativa
        # Rango esperado: -77.0 a -76.0
        
        # Si es positiva y está en el rango de Cali (sin el signo)
        if 76.0 <= lon <= 77.5:
            return -lon
        
        # Si es negativa y está en el rango correcto
        if -77.5 <= lon <= -75.5:
            return lon
        
        # Si es un valor pequeño negativo (puede estar truncado)
        # Ejemplo: -7.650763 debería ser -76.650763
        if -10.0 < lon < 0:
            # Intentar reconstruir: -7.xxx -> -76.xxx
            lon_str = str(abs(lon))
            if '.' in lon_str:
                parts = lon_str.split('.')
                if len(parts[0]) == 1:  # Solo un dígito antes del decimal
                    # Asumir que falta el "76"
                    reconstructed = float(f"-76.{parts[1]}")
                    if -77.5 <= reconstructed <= -75.5:
                        return reconstructed
        
        return lon
    
    @staticmethod
    def fix_latitude_range(lat: float) -> Optional[float]:
        """
        Valida y corrige el rango de latitud para Cali.
        
        Args:
            lat: Latitud a validar
        
        Returns:
            Latitud válida o None si es irrecuperable
        """
        if lat is None or pd.isna(lat):
            return None
        
        # Rechazar valores astronómicos (datos corruptos)
        if abs(lat) > 90:
            return None
        
        # Rango válido para Cali
        if 2.5 <= lat <= 4.5:
            return lat
        
        # Fuera de rango razonable
        return None
    
    @classmethod
    def normalize_coordinate_pair(cls, lat, lon) -> Tuple[Optional[float], Optional[float]]:
        """
        Normaliza y corrige un par de coordenadas (lat, lon).
        
        Args:
            lat: Latitud (cualquier formato)
            lon: Longitud (cualquier formato)
        
        Returns:
            Tupla (lat_normalizada, lon_normalizada)
        """
        # Paso 1: Normalizar formato decimal
        lat_normalized = cls.normalize_decimal_format(lat)
        lon_normalized = cls.normalize_decimal_format(lon)
        
        # Si ambas son None, retornar
        if lat_normalized is None and lon_normalized is None:
            return None, None
        
        # Paso 2: Corregir signo de longitud
        if lon_normalized is not None:
            lon_normalized = cls.fix_longitude_sign(lon_normalized, lat_normalized)
        
        # Paso 3: Validar rango de latitud
        if lat_normalized is not None:
            lat_normalized = cls.fix_latitude_range(lat_normalized)
        
        # Paso 4: Validar rango de longitud
        if lon_normalized is not None:
            if not (-77.5 <= lon_normalized <= -75.5):
                lon_normalized = None
        
        return lat_normalized, lon_normalized
    
    @classmethod
    def normalize_dataframe_coordinates(cls, df: pd.DataFrame, 
                                       lat_col: str = 'lat', 
                                       lon_col: str = 'lon',
                                       inplace: bool = False) -> pd.DataFrame:
        """
        Normaliza coordenadas en un DataFrame completo.
        
        Args:
            df: DataFrame con coordenadas
            lat_col: Nombre de la columna de latitud
            lon_col: Nombre de la columna de longitud
            inplace: Si True, modifica el DataFrame original
        
        Returns:
            DataFrame con coordenadas normalizadas
        """
        if not inplace:
            df = df.copy()
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"[WARNING] Columnas {lat_col}/{lon_col} no encontradas")
            return df
        
        # Aplicar normalización a cada fila
        normalized_coords = df.apply(
            lambda row: cls.normalize_coordinate_pair(row[lat_col], row[lon_col]),
            axis=1
        )
        
        # Separar resultados
        df[lat_col] = normalized_coords.apply(lambda x: x[0])
        df[lon_col] = normalized_coords.apply(lambda x: x[1])
        
        # Estadísticas
        valid_count = (df[lat_col].notna() & df[lon_col].notna()).sum()
        total_count = len(df)
        
        print(f"[OK] Coordenadas normalizadas: {valid_count}/{total_count} válidas ({valid_count/total_count*100:.1f}%)")
        
        return df


def test_normalizer():
    """Pruebas de la función de normalización."""
    print("="*80)
    print("PRUEBAS DE NORMALIZADOR DE COORDENADAS")
    print("="*80)
    
    test_cases = [
        # (lat_input, lon_input, lat_expected, lon_expected, descripcion)
        (3.440123, 76.494757, 3.440123, -76.494757, "Longitud positiva -> negativa"),
        (3.443499, 76.532927, 3.443499, -76.532927, "Longitud positiva -> negativa"),
        (3.369127, -7.650763, 3.369127, -76.650763, "Longitud truncada"),
        ("3,440123", "-76,532927", 3.440123, -76.532927, "Formato europeo (coma)"),
        ("3.440123", "-76.532927", 3.440123, -76.532927, "Formato americano (punto)"),
        (34550800868900000, -76.535201, None, -76.535201, "Latitud astronómica -> None"),
        (3.4, -76.5, 3.4, -76.5, "Coordenadas válidas"),
        (None, None, None, None, "Valores nulos"),
        ("", "", None, None, "Strings vacíos"),
    ]
    
    normalizer = CoordinateNormalizer()
    
    for lat_in, lon_in, lat_exp, lon_exp, desc in test_cases:
        lat_out, lon_out = normalizer.normalize_coordinate_pair(lat_in, lon_in)
        
        # Verificar resultados
        lat_match = (lat_out == lat_exp) or (pd.isna(lat_out) and lat_exp is None)
        lon_match = (lon_out == lon_exp) or (pd.isna(lon_out) and lon_exp is None)
        
        status = "✓" if lat_match and lon_match else "✗"
        
        print(f"\n{status} {desc}")
        print(f"  Input:  lat={lat_in}, lon={lon_in}")
        print(f"  Output: lat={lat_out}, lon={lon_out}")
        if not (lat_match and lon_match):
            print(f"  Expected: lat={lat_exp}, lon={lon_exp}")


if __name__ == "__main__":
    test_normalizer()
