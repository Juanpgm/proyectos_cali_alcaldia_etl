"""
Módulo de Validación y Corrección Automática de Coordenadas
Garantiza que las coordenadas geográficas estén correctas antes de crear geometrías
"""

import pandas as pd
import numpy as np
from typing import Tuple, Optional, Dict, Any
import re
from datetime import datetime


class CoordinateValidator:
    """
    Validador y corrector automático de coordenadas geográficas.
    Detecta y corrige problemas comunes como:
    - Coordenadas invertidas (lat, lon en lugar de lon, lat)
    - Formatos incorrectos (comas en lugar de puntos)
    - Valores fuera de rango
    - Coordenadas inválidas
    """
    
    # Bounding box de Cali y área metropolitana (con margen)
    CALI_BBOX = {
        'lat_min': 3.25,
        'lat_max': 3.65,
        'lon_min': -76.70,
        'lon_max': -76.35
    }
    
    # Rangos globales válidos
    GLOBAL_RANGES = {
        'lat_min': -90,
        'lat_max': 90,
        'lon_min': -180,
        'lon_max': 180
    }
    
    def __init__(self, verbose: bool = False):
        """
        Inicializa el validador.
        
        Args:
            verbose: Si True, imprime mensajes detallados
        """
        self.verbose = verbose
        self.stats = {
            'total_processed': 0,
            'decimal_separator_fixed': 0,
            'inverted_coords_fixed': 0,
            'out_of_range_global': 0,
            'out_of_range_cali': 0,
            'invalid_values': 0,
            'null_values': 0,
            'successfully_validated': 0
        }
    
    def normalize_decimal_separator(self, value: Any) -> Optional[float]:
        """
        Normaliza separadores decimales (coma → punto) y convierte a float.
        
        Args:
            value: Valor a normalizar
            
        Returns:
            Float normalizado o None si no es válido
        """
        if pd.isna(value) or value is None:
            return None
        
        try:
            # Si ya es numérico, retornar
            if isinstance(value, (int, float)):
                return float(value)
            
            # Convertir a string y limpiar
            str_value = str(value).strip()
            
            # Reemplazar coma por punto
            if ',' in str_value:
                str_value = str_value.replace(',', '.')
                self.stats['decimal_separator_fixed'] += 1
            
            # Remover espacios internos
            str_value = str_value.replace(' ', '')
            
            # Convertir a float
            result = float(str_value)
            
            return result
            
        except (ValueError, TypeError):
            return None
    
    def is_coordinate_inverted(self, lat: float, lon: float) -> bool:
        """
        Detecta si las coordenadas están invertidas.
        
        GeoJSON requiere [lon, lat], pero a veces se almacenan como [lat, lon].
        
        Args:
            lat: Supuesta latitud
            lon: Supuesta longitud
            
        Returns:
            True si las coordenadas parecen estar invertidas
        """
        # Caso 1: lat tiene valores típicos de longitud de Cali (-76.x)
        # y lon tiene valores típicos de latitud de Cali (3.x)
        if (self.CALI_BBOX['lon_min'] <= lat <= self.CALI_BBOX['lon_max'] and 
            self.CALI_BBOX['lat_min'] <= lon <= self.CALI_BBOX['lat_max']):
            return True
        
        # Caso 2: lat está en rango de longitud global y lon en rango de latitud
        # pero fuera de los rangos esperados para sus posiciones
        if (abs(lat) > 90 or  # lat fuera de rango válido
            abs(lon) > 180):   # lon fuera de rango válido
            return False  # Son inválidas, no solo invertidas
        
        return False
    
    def validate_and_correct_coordinate(
        self, 
        lat: Any, 
        lon: Any,
        record_id: str = None
    ) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
        """
        Valida y corrige automáticamente un par de coordenadas.
        
        Args:
            lat: Latitud (puede estar en cualquier formato)
            lon: Longitud (puede estar en cualquier formato)
            record_id: Identificador del registro (para logging)
            
        Returns:
            Tupla (lat_corregida, lon_corregida, metadata_de_corrección)
        """
        self.stats['total_processed'] += 1
        
        metadata = {
            'original_lat': lat,
            'original_lon': lon,
            'corrections_applied': [],
            'warnings': [],
            'is_valid': False,
            'record_id': record_id
        }
        
        # Paso 1: Normalizar separadores decimales
        lat_normalized = self.normalize_decimal_separator(lat)
        lon_normalized = self.normalize_decimal_separator(lon)
        
        if lat != lat_normalized or lon != lon_normalized:
            metadata['corrections_applied'].append('decimal_separator')
        
        # Paso 2: Verificar valores nulos
        if lat_normalized is None or lon_normalized is None:
            self.stats['null_values'] += 1
            metadata['warnings'].append('Coordenadas nulas o inválidas')
            return None, None, metadata
        
        # Paso 3: Verificar rangos globales válidos
        if not (self.GLOBAL_RANGES['lat_min'] <= lat_normalized <= self.GLOBAL_RANGES['lat_max']):
            # Verificar si es un valor absurdo (error de tipo de dato)
            if abs(lat_normalized) > 1000:
                self.stats['invalid_values'] += 1
                metadata['warnings'].append(f'Latitud con valor absurdo: {lat_normalized}')
                return None, None, metadata
            
            self.stats['out_of_range_global'] += 1
            metadata['warnings'].append(f'Latitud fuera de rango global: {lat_normalized}')
        
        if not (self.GLOBAL_RANGES['lon_min'] <= lon_normalized <= self.GLOBAL_RANGES['lon_max']):
            if abs(lon_normalized) > 1000:
                self.stats['invalid_values'] += 1
                metadata['warnings'].append(f'Longitud con valor absurdo: {lon_normalized}')
                return None, None, metadata
            
            self.stats['out_of_range_global'] += 1
            metadata['warnings'].append(f'Longitud fuera de rango global: {lon_normalized}')
        
        # Paso 4: Detectar y corregir coordenadas invertidas
        if self.is_coordinate_inverted(lat_normalized, lon_normalized):
            # Invertir
            lat_corrected = lon_normalized
            lon_corrected = lat_normalized
            self.stats['inverted_coords_fixed'] += 1
            metadata['corrections_applied'].append('inverted_coordinates')
            
            if self.verbose:
                print(f"  ⚠️  Coordenadas invertidas detectadas y corregidas:")
                print(f"      Antes: lat={lat_normalized}, lon={lon_normalized}")
                print(f"      Después: lat={lat_corrected}, lon={lon_corrected}")
        else:
            lat_corrected = lat_normalized
            lon_corrected = lon_normalized
        
        # Paso 5: Validar que estén en el área de Cali
        if not (self.CALI_BBOX['lat_min'] <= lat_corrected <= self.CALI_BBOX['lat_max'] and
                self.CALI_BBOX['lon_min'] <= lon_corrected <= self.CALI_BBOX['lon_max']):
            self.stats['out_of_range_cali'] += 1
            metadata['warnings'].append(
                f'Coordenadas fuera del área de Cali: lat={lat_corrected:.6f}, lon={lon_corrected:.6f}'
            )
            # Nota: No retornamos None aquí porque podrían ser válidas pero fuera de Cali
        
        # Paso 6: Validación final
        metadata['is_valid'] = True
        self.stats['successfully_validated'] += 1
        
        return lat_corrected, lon_corrected, metadata
    
    def validate_dataframe(
        self, 
        df: pd.DataFrame,
        lat_col: str = 'lat',
        lon_col: str = 'lon',
        id_col: str = 'upid'
    ) -> pd.DataFrame:
        """
        Valida y corrige coordenadas en un DataFrame completo.
        
        Args:
            df: DataFrame con coordenadas
            lat_col: Nombre de la columna de latitud
            lon_col: Nombre de la columna de longitud
            id_col: Nombre de la columna de identificador
            
        Returns:
            DataFrame con coordenadas corregidas y columnas de metadata
        """
        if self.verbose:
            print(f"\n{'='*80}")
            print("🌍 VALIDACIÓN Y CORRECCIÓN DE COORDENADAS")
            print(f"{'='*80}")
            print(f"Total de registros: {len(df)}")
        
        # Reiniciar estadísticas
        self.stats = {k: 0 for k in self.stats.keys()}
        
        # Aplicar validación a cada fila
        results = []
        for idx, row in df.iterrows():
            record_id = row.get(id_col, f"ROW-{idx}")
            lat = row.get(lat_col)
            lon = row.get(lon_col)
            
            lat_corr, lon_corr, metadata = self.validate_and_correct_coordinate(
                lat, lon, record_id
            )
            
            results.append({
                f'{lat_col}_original': lat,
                f'{lon_col}_original': lon,
                lat_col: lat_corr,
                lon_col: lon_corr,
                'coord_is_valid': metadata['is_valid'],
                'coord_corrections': ','.join(metadata['corrections_applied']) if metadata['corrections_applied'] else None,
                'coord_warnings': ','.join(metadata['warnings']) if metadata['warnings'] else None
            })
        
        # Crear DataFrame de resultados
        results_df = pd.DataFrame(results)
        
        # Actualizar DataFrame original
        df[lat_col] = results_df[lat_col]
        df[lon_col] = results_df[lon_col]
        df['coord_is_valid'] = results_df['coord_is_valid']
        df['coord_corrections'] = results_df['coord_corrections']
        df['coord_warnings'] = results_df['coord_warnings']
        
        if self.verbose:
            self._print_statistics()
        
        return df
    
    def _print_statistics(self):
        """Imprime estadísticas de validación."""
        print(f"\n📊 ESTADÍSTICAS DE VALIDACIÓN:")
        print(f"  • Total procesados: {self.stats['total_processed']:,}")
        print(f"  • ✅ Válidos: {self.stats['successfully_validated']:,} ({self.stats['successfully_validated']/self.stats['total_processed']*100:.1f}%)")
        print(f"\n🔧 CORRECCIONES APLICADAS:")
        print(f"  • Separadores decimales corregidos: {self.stats['decimal_separator_fixed']:,}")
        print(f"  • Coordenadas invertidas corregidas: {self.stats['inverted_coords_fixed']:,}")
        print(f"\n⚠️  PROBLEMAS DETECTADOS:")
        print(f"  • Valores nulos: {self.stats['null_values']:,}")
        print(f"  • Valores inválidos: {self.stats['invalid_values']:,}")
        print(f"  • Fuera de rango global: {self.stats['out_of_range_global']:,}")
        print(f"  • Fuera de Cali: {self.stats['out_of_range_cali']:,}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Retorna estadísticas de validación."""
        return self.stats.copy()


def validate_and_fix_coordinates(
    df: pd.DataFrame,
    lat_col: str = 'lat',
    lon_col: str = 'lon',
    id_col: str = 'upid',
    verbose: bool = True
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Función de utilidad para validar y corregir coordenadas en un DataFrame.
    
    Args:
        df: DataFrame con coordenadas
        lat_col: Nombre de columna de latitud
        lon_col: Nombre de columna de longitud
        id_col: Nombre de columna de identificador
        verbose: Si True, imprime información detallada
        
    Returns:
        Tupla (DataFrame corregido, estadísticas)
    """
    validator = CoordinateValidator(verbose=verbose)
    df_corrected = validator.validate_dataframe(df, lat_col, lon_col, id_col)
    stats = validator.get_statistics()
    
    return df_corrected, stats


# Función auxiliar para corregir coordenadas en clustering
def fix_coordinate_format(value: Any) -> Optional[float]:
    """
    Corrige formato de coordenadas (separador decimal, espacios, etc).
    Versión simplificada para uso rápido.
    
    Args:
        value: Valor a corregir
        
    Returns:
        Float corregido o None
    """
    if pd.isna(value) or value is None:
        return None
    
    try:
        if isinstance(value, (int, float)):
            return float(value)
        
        str_value = str(value).strip().replace(',', '.').replace(' ', '')
        return float(str_value)
    except:
        return None
