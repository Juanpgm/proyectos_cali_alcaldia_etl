# -*- coding: utf-8 -*-
"""
Sistema de Control de Calidad de Datos - ISO 19157
==================================================

Sistema integral de validación de calidad de datos geoespaciales basado en el estándar ISO 19157.
Proporciona validaciones exhaustivas a nivel de registro individual y agregado por centro gestor.

Elementos de Calidad ISO 19157 implementados:
1. Consistencia Lógica - Adherencia a reglas lógicas de estructura de datos
2. Completitud - Presencia/ausencia de características y atributos
3. Exactitud Posicional - Precisión de coordenadas y CRS
4. Exactitud Temática - Corrección de clasificaciones y valores categóricos
5. Calidad Temporal - Validez y coherencia de atributos temporales

Author: ETL QA Team
Date: November 2025
Version: 1.0
"""

import re
import json
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime
from enum import Enum
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString, MultiPoint, MultiPolygon, MultiLineString
from shapely.validation import explain_validity
import numpy as np


class SeverityLevel(Enum):
    """Niveles de severidad para problemas de calidad."""
    CRITICAL = "CRITICAL"  # Bloqueante, impide uso del dato
    HIGH = "HIGH"  # Grave, afecta significativamente la calidad
    MEDIUM = "MEDIUM"  # Importante, debe corregirse
    LOW = "LOW"  # Menor, puede corregirse después
    INFO = "INFO"  # Informativo, no es un problema


class QualityDimension(Enum):
    """Dimensiones de calidad según ISO 19157."""
    LOGICAL_CONSISTENCY = "Consistencia Lógica"
    COMPLETENESS = "Completitud"
    POSITIONAL_ACCURACY = "Exactitud Posicional"
    THEMATIC_ACCURACY = "Exactitud Temática"
    TEMPORAL_QUALITY = "Calidad Temporal"


class ValidationRule:
    """Representa una regla de validación individual."""
    
    def __init__(
        self,
        rule_id: str,
        name: str,
        dimension: QualityDimension,
        severity: SeverityLevel,
        description: str,
        applies_to_geometry: bool = False,
        applies_to_attributes: bool = True
    ):
        self.rule_id = rule_id
        self.name = name
        self.dimension = dimension
        self.severity = severity
        self.description = description
        self.applies_to_geometry = applies_to_geometry
        self.applies_to_attributes = applies_to_attributes


class QualityIssue:
    """Representa un problema de calidad detectado."""
    
    def __init__(
        self,
        rule: ValidationRule,
        field_name: Optional[str],
        current_value: Any,
        expected_value: Optional[Any],
        details: str,
        suggestion: Optional[str] = None
    ):
        self.rule = rule
        self.field_name = field_name
        self.current_value = current_value
        self.expected_value = expected_value
        self.details = details
        self.suggestion = suggestion
        self.detected_at = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convierte el problema a diccionario."""
        return {
            'rule_id': self.rule.rule_id,
            'rule_name': self.rule.name,
            'dimension': self.rule.dimension.value,
            'severity': self.rule.severity.value,
            'field_name': self.field_name,
            'current_value': str(self.current_value) if self.current_value is not None else None,
            'expected_value': str(self.expected_value) if self.expected_value is not None else None,
            'details': self.details,
            'suggestion': self.suggestion,
            'detected_at': self.detected_at
        }


class DataQualityValidator:
    """
    Validador principal de calidad de datos.
    Implementa todas las reglas de validación basadas en ISO 19157.
    """
    
    # Valores válidos para campos categóricos (SEGÚN DATOS REALES DE UNIDADES DE PROYECTO)
    VALID_ESTADOS = {'En alistamiento', 'En ejecución', 'Terminado'}
    VALID_TIPOS_INTERVENCION = {
        'Obra nueva',  # Valor común en datos reales
        'Construcción Nueva',  # Alias permitido
        'Adecuaciones y Mantenimientos', 
        'Mejoramiento', 
        'Rehabilitación', 
        'Ampliación'
    }
    VALID_PLATAFORMAS = {'SECOP I', 'SECOP II', 'Contratación Directa', 'Otro'}
    VALID_UNIDADES = {'UND', 'M2', 'ML', 'M3', 'KM', 'HA', 'GLB'}
    
    # Rangos válidos para Cali
    CALI_BBOX = {
        'min_lat': 3.35,
        'max_lat': 3.56,
        'min_lon': -76.60,
        'max_lon': -76.40
    }
    
    # Campos requeridos
    REQUIRED_FIELDS = {
        'upid', 'nombre_up', 'estado', 'avance_obra', 'ano',
        'nombre_centro_gestor', 'comuna_corregimiento', 'tipo_intervencion'
    }
    
    # Campos monetarios
    MONETARY_FIELDS = {'presupuesto_base'}
    
    # Campos de fecha
    DATE_FIELDS = {'fecha_inicio', 'fecha_fin'}
    
    def __init__(self):
        """Inicializa el validador con todas las reglas."""
        self.rules = self._initialize_rules()
        self.validation_stats = {
            'total_validations': 0,
            'by_dimension': {},
            'by_severity': {}
        }
    
    def _initialize_rules(self) -> Dict[str, ValidationRule]:
        """Inicializa todas las reglas de validación."""
        rules = {}
        
        # ==================== CONSISTENCIA LÓGICA ====================
        
        rules['LC001'] = ValidationRule(
            'LC001',
            'Congruencia Estado vs Avance de Obra',
            QualityDimension.LOGICAL_CONSISTENCY,
            SeverityLevel.CRITICAL,
            'El estado debe ser congruente con el avance de obra (0%=Alistamiento, 0-100%=Ejecución, 100%=Terminado)'
        )
        
        rules['LC002'] = ValidationRule(
            'LC002',
            'Rango válido de Avance de Obra',
            QualityDimension.LOGICAL_CONSISTENCY,
            SeverityLevel.CRITICAL,
            'El avance de obra debe estar entre 0 y 100'
        )
        
        rules['LC003'] = ValidationRule(
            'LC003',
            'Tipo de dato numérico para Avance',
            QualityDimension.LOGICAL_CONSISTENCY,
            SeverityLevel.CRITICAL,
            'El avance de obra debe ser un valor numérico válido'
        )
        
        rules['LC004'] = ValidationRule(
            'LC004',
            'Valores monetarios no negativos',
            QualityDimension.LOGICAL_CONSISTENCY,
            SeverityLevel.HIGH,
            'Los valores monetarios deben ser positivos o cero'
        )
        
        rules['LC005'] = ValidationRule(
            'LC005',
            'Cantidad debe ser positiva',
            QualityDimension.LOGICAL_CONSISTENCY,
            SeverityLevel.HIGH,
            'La cantidad de unidades debe ser mayor a cero'
        )
        
        rules['LC006'] = ValidationRule(
            'LC006',
            'Geometría válida y sin errores topológicos',
            QualityDimension.LOGICAL_CONSISTENCY,
            SeverityLevel.HIGH,
            'La geometría no debe tener auto-intersecciones, polígonos abiertos u otros errores topológicos',
            applies_to_geometry=True
        )
        
        rules['LC007'] = ValidationRule(
            'LC007',
            'Año debe ser numérico y válido',
            QualityDimension.LOGICAL_CONSISTENCY,
            SeverityLevel.HIGH,
            'El año debe ser un valor numérico de 4 dígitos en rango razonable (2020-2030)'
        )
        
        # ==================== COMPLETITUD ====================
        
        rules['CO001'] = ValidationRule(
            'CO001',
            'Campos obligatorios presentes',
            QualityDimension.COMPLETENESS,
            SeverityLevel.CRITICAL,
            'Todos los campos obligatorios deben tener valores no nulos'
        )
        
        rules['CO002'] = ValidationRule(
            'CO002',
            'Geometría presente cuando se espera',
            QualityDimension.COMPLETENESS,
            SeverityLevel.HIGH,
            'Los registros con dirección deben tener geometría asociada',
            applies_to_geometry=True
        )
        
        rules['CO003'] = ValidationRule(
            'CO003',
            'Identificador único presente',
            QualityDimension.COMPLETENESS,
            SeverityLevel.CRITICAL,
            'Debe existir al menos un identificador (UPID, BPIN o Identificador)'
        )
        
        rules['CO004'] = ValidationRule(
            'CO004',
            'Campos de fecha completos',
            QualityDimension.COMPLETENESS,
            SeverityLevel.MEDIUM,
            'Ambas fechas (inicio y fin) deben estar presentes si el proyecto está en ejecución'
        )
        
        rules['CO005'] = ValidationRule(
            'CO005',
            'Información de contratación completa',
            QualityDimension.COMPLETENESS,
            SeverityLevel.MEDIUM,
            'Proyectos en ejecución deben tener referencia de contrato o proceso'
        )
        
        rules['CO006'] = ValidationRule(
            'CO006',
            'Dirección presente',
            QualityDimension.COMPLETENESS,
            SeverityLevel.HIGH,
            'Todos los registros deben tener dirección física'
        )
        
        # ==================== EXACTITUD POSICIONAL ====================
        
        rules['PA001'] = ValidationRule(
            'PA001',
            'Coordenadas dentro del bounding box de Cali',
            QualityDimension.POSITIONAL_ACCURACY,
            SeverityLevel.HIGH,
            'Las coordenadas deben estar dentro del área geográfica de Cali',
            applies_to_geometry=True
        )
        
        rules['PA002'] = ValidationRule(
            'PA002',
            'Sistema de referencia espacial consistente',
            QualityDimension.POSITIONAL_ACCURACY,
            SeverityLevel.CRITICAL,
            'Todas las geometrías deben usar el mismo CRS (EPSG:4326)',
            applies_to_geometry=True
        )
        
        rules['PA003'] = ValidationRule(
            'PA003',
            'Coordenadas válidas (no cero ni extremos)',
            QualityDimension.POSITIONAL_ACCURACY,
            SeverityLevel.HIGH,
            'Las coordenadas no deben ser (0,0) ni valores extremos inválidos',
            applies_to_geometry=True
        )
        
        rules['PA004'] = ValidationRule(
            'PA004',
            'Geometría dentro de la comuna/corregimiento declarado',
            QualityDimension.POSITIONAL_ACCURACY,
            SeverityLevel.MEDIUM,
            'La ubicación geográfica debe coincidir con la comuna/corregimiento en atributos',
            applies_to_geometry=True
        )
        
        rules['PA005'] = ValidationRule(
            'PA005',
            'Geometría dentro del barrio/vereda declarado',
            QualityDimension.POSITIONAL_ACCURACY,
            SeverityLevel.MEDIUM,
            'La ubicación geográfica debe coincidir con el barrio/vereda en atributos',
            applies_to_geometry=True
        )
        
        # ==================== EXACTITUD TEMÁTICA ====================
        
        rules['TA001'] = ValidationRule(
            'TA001',
            'Estado con valor permitido',
            QualityDimension.THEMATIC_ACCURACY,
            SeverityLevel.CRITICAL,
            f'El estado debe ser uno de: {", ".join(self.VALID_ESTADOS)}'
        )
        
        rules['TA002'] = ValidationRule(
            'TA002',
            'Tipo de intervención válido',
            QualityDimension.THEMATIC_ACCURACY,
            SeverityLevel.HIGH,
            f'El tipo de intervención debe ser uno de los valores permitidos'
        )
        
        rules['TA003'] = ValidationRule(
            'TA003',
            'Plataforma de contratación válida',
            QualityDimension.THEMATIC_ACCURACY,
            SeverityLevel.MEDIUM,
            f'La plataforma debe ser una de: {", ".join(self.VALID_PLATAFORMAS)}'
        )
        
        rules['TA004'] = ValidationRule(
            'TA004',
            'Unidad de medida válida',
            QualityDimension.THEMATIC_ACCURACY,
            SeverityLevel.MEDIUM,
            f'La unidad debe ser una de: {", ".join(self.VALID_UNIDADES)}'
        )
        
        rules['TA005'] = ValidationRule(
            'TA005',
            'Formato de URL correcto',
            QualityDimension.THEMATIC_ACCURACY,
            SeverityLevel.LOW,
            'Las URL de procesos deben tener formato válido'
        )
        
        rules['TA006'] = ValidationRule(
            'TA006',
            'Comuna/Corregimiento reconocido',
            QualityDimension.THEMATIC_ACCURACY,
            SeverityLevel.HIGH,
            'La comuna o corregimiento debe ser válido para Cali'
        )
        
        rules['TA007'] = ValidationRule(
            'TA007',
            'Centro gestor reconocido',
            QualityDimension.THEMATIC_ACCURACY,
            SeverityLevel.MEDIUM,
            'El nombre del centro gestor debe estar en el catálogo de dependencias'
        )
        
        # ==================== CALIDAD TEMPORAL ====================
        
        rules['TQ001'] = ValidationRule(
            'TQ001',
            'Fecha de inicio anterior a fecha de fin',
            QualityDimension.TEMPORAL_QUALITY,
            SeverityLevel.CRITICAL,
            'La fecha de inicio debe ser anterior a la fecha de finalización'
        )
        
        rules['TQ002'] = ValidationRule(
            'TQ002',
            'Formato de fecha válido',
            QualityDimension.TEMPORAL_QUALITY,
            SeverityLevel.HIGH,
            'Las fechas deben tener formato válido y ser fechas reales'
        )
        
        rules['TQ003'] = ValidationRule(
            'TQ003',
            'Fechas en rango temporal lógico',
            QualityDimension.TEMPORAL_QUALITY,
            SeverityLevel.MEDIUM,
            'Las fechas deben estar en un rango razonable (no muy pasadas ni muy futuras)'
        )
        
        rules['TQ004'] = ValidationRule(
            'TQ004',
            'Coherencia temporal con estado del proyecto',
            QualityDimension.TEMPORAL_QUALITY,
            SeverityLevel.MEDIUM,
            'Las fechas deben ser coherentes con el estado actual del proyecto'
        )
        
        rules['TQ005'] = ValidationRule(
            'TQ005',
            'Duración del proyecto razonable',
            QualityDimension.TEMPORAL_QUALITY,
            SeverityLevel.LOW,
            'La duración del proyecto debe estar en un rango razonable (no más de 5 años)'
        )
        
        return rules
    
    # ==================== MÉTODOS DE VALIDACIÓN ====================
    
    def validate_record(self, record: Dict[str, Any], geometry: Any = None) -> List[QualityIssue]:
        """
        Valida un registro completo y retorna lista de problemas detectados.
        
        Args:
            record: Diccionario con las propiedades del registro
            geometry: Geometría shapely asociada (opcional)
            
        Returns:
            Lista de QualityIssue detectados
        """
        issues = []
        
        # Validaciones de Consistencia Lógica
        issues.extend(self._validate_logical_consistency(record, geometry))
        
        # Validaciones de Completitud
        issues.extend(self._validate_completeness(record, geometry))
        
        # Validaciones de Exactitud Posicional
        if geometry:
            issues.extend(self._validate_positional_accuracy(record, geometry))
        
        # Validaciones de Exactitud Temática
        issues.extend(self._validate_thematic_accuracy(record))
        
        # Validaciones de Calidad Temporal
        issues.extend(self._validate_temporal_quality(record))
        
        return issues
    
    def _validate_logical_consistency(self, record: Dict[str, Any], geometry: Any) -> List[QualityIssue]:
        """Valida consistencia lógica."""
        issues = []
        
        # LC001: Congruencia estado vs avance
        estado = record.get('estado')
        avance = record.get('avance_obra')
        
        if estado and avance is not None:
            try:
                avance_num = float(avance)
                
                # Verificar congruencia
                # CORRECCIÓN: 0% con "En alistamiento" es válido, no es error
                if avance_num == 0 and estado not in ['En alistamiento']:
                    issues.append(QualityIssue(
                        self.rules['LC001'],
                        'estado',
                        estado,
                        'En alistamiento',
                        f'Avance de obra es 0% pero estado es "{estado}"',
                        'Cambiar estado a "En alistamiento"'
                    ))
                elif avance_num == 100 and estado != 'Terminado':
                    issues.append(QualityIssue(
                        self.rules['LC001'],
                        'estado',
                        estado,
                        'Terminado',
                        f'Avance de obra es 100% pero estado es "{estado}"',
                        'Cambiar estado a "Terminado"'
                    ))
                elif 0 < avance_num < 100 and estado == 'En alistamiento':
                    issues.append(QualityIssue(
                        self.rules['LC001'],
                        'estado',
                        estado,
                        'En ejecución',
                        f'Avance de obra es {avance_num}% pero estado es "En alistamiento"',
                        'Cambiar estado a "En ejecución"'
                    ))
            except (ValueError, TypeError):
                pass  # Se maneja en LC003
        
        # LC002: Rango válido de avance
        if avance is not None:
            try:
                avance_num = float(avance)
                if avance_num < 0 or avance_num > 100:
                    issues.append(QualityIssue(
                        self.rules['LC002'],
                        'avance_obra',
                        avance_num,
                        '0-100',
                        f'Avance de obra fuera de rango: {avance_num}%',
                        'Ajustar a un valor entre 0 y 100'
                    ))
            except (ValueError, TypeError):
                pass  # Se maneja en LC003
        
        # LC003: Tipo de dato numérico para avance
        if avance is not None:
            try:
                float(avance)
            except (ValueError, TypeError):
                issues.append(QualityIssue(
                    self.rules['LC003'],
                    'avance_obra',
                    avance,
                    'número (0-100)',
                    f'Avance de obra no es numérico: "{avance}"',
                    'Corregir a un valor numérico entre 0 y 100'
                ))
        
        # LC004: Valores monetarios no negativos
        for field in self.MONETARY_FIELDS:
            value = record.get(field)
            if value is not None:
                try:
                    value_num = float(value)
                    if value_num < 0:
                        issues.append(QualityIssue(
                            self.rules['LC004'],
                            field,
                            value_num,
                            '>= 0',
                            f'Valor monetario negativo en {field}: ${value_num:,.2f}',
                            'Corregir a un valor positivo o cero'
                        ))
                except (ValueError, TypeError):
                    issues.append(QualityIssue(
                        self.rules['LC004'],
                        field,
                        value,
                        'número >= 0',
                        f'Valor monetario no numérico en {field}: "{value}"',
                        'Corregir a un valor numérico válido'
                    ))
        
        # LC005: Cantidad positiva
        cantidad = record.get('cantidad')
        if cantidad is not None:
            try:
                cantidad_num = float(cantidad)
                if cantidad_num <= 0:
                    issues.append(QualityIssue(
                        self.rules['LC005'],
                        'cantidad',
                        cantidad_num,
                        '> 0',
                        f'Cantidad no positiva: {cantidad_num}',
                        'Ajustar a un valor mayor a cero'
                    ))
            except (ValueError, TypeError):
                issues.append(QualityIssue(
                    self.rules['LC005'],
                    'cantidad',
                    cantidad,
                    'número > 0',
                    f'Cantidad no numérica: "{cantidad}"',
                    'Corregir a un valor numérico mayor a cero'
                ))
        
        # LC006: Geometría válida
        if geometry and not geometry.is_empty:
            if not geometry.is_valid:
                validity_msg = explain_validity(geometry)
                issues.append(QualityIssue(
                    self.rules['LC006'],
                    'geometry',
                    'Inválida',
                    'Válida',
                    f'Geometría con error topológico: {validity_msg}',
                    'Corregir la geometría usando herramientas GIS'
                ))
        
        # LC007: Año válido
        ano = record.get('ano')
        if ano is not None:
            try:
                ano_num = int(ano)
                if ano_num < 2020 or ano_num > 2030:
                    issues.append(QualityIssue(
                        self.rules['LC007'],
                        'ano',
                        ano_num,
                        '2020-2030',
                        f'Año fuera de rango esperado: {ano_num}',
                        'Verificar y corregir el año del proyecto'
                    ))
            except (ValueError, TypeError):
                issues.append(QualityIssue(
                    self.rules['LC007'],
                    'ano',
                    ano,
                    '2020-2030',
                    f'Año no numérico: "{ano}"',
                    'Corregir a un año válido (2020-2030)'
                ))
        
        return issues
    
    def _validate_completeness(self, record: Dict[str, Any], geometry: Any) -> List[QualityIssue]:
        """Valida completitud de datos."""
        issues = []
        
        # CO001: Campos obligatorios presentes
        for field in self.REQUIRED_FIELDS:
            value = record.get(field)
            if value is None or (isinstance(value, str) and value.strip() == ''):
                issues.append(QualityIssue(
                    self.rules['CO001'],
                    field,
                    None,
                    'Valor requerido',
                    f'Campo obligatorio vacío o nulo: {field}',
                    f'Completar el campo {field}'
                ))
        
        # CO002: Geometría presente cuando se espera
        direccion = record.get('direccion')
        if direccion and (isinstance(direccion, str) and direccion.strip()):
            if geometry is None or geometry.is_empty:
                issues.append(QualityIssue(
                    self.rules['CO002'],
                    'geometry',
                    'NULL',
                    'Point, Polygon o LineString',
                    f'Registro con dirección "{direccion}" pero sin geometría',
                    'Geolocalizar la dirección o verificar coordenadas'
                ))
        
        # CO003: Identificador único presente
        upid = record.get('upid')
        bpin = record.get('bpin')
        identificador = record.get('identificador')
        
        if not any([upid, bpin, identificador]):
            issues.append(QualityIssue(
                self.rules['CO003'],
                'upid/bpin/identificador',
                None,
                'Al menos uno requerido',
                'No existe ningún identificador único para el registro',
                'Asignar UPID, BPIN o Identificador'
            ))
        
        # CO004: Campos de fecha completos para proyectos en ejecución
        estado = record.get('estado')
        fecha_inicio = record.get('fecha_inicio')
        fecha_fin = record.get('fecha_fin')
        
        if estado in ['En ejecución', 'Terminado']:
            if not fecha_inicio:
                issues.append(QualityIssue(
                    self.rules['CO004'],
                    'fecha_inicio',
                    None,
                    'Fecha válida',
                    f'Proyecto en "{estado}" sin fecha de inicio',
                    'Completar fecha de inicio del proyecto'
                ))
            if not fecha_fin:
                issues.append(QualityIssue(
                    self.rules['CO004'],
                    'fecha_fin',
                    None,
                    'Fecha válida',
                    f'Proyecto en "{estado}" sin fecha de fin',
                    'Completar fecha de finalización del proyecto'
                ))
        
        # CO005: Información de contratación completa
        if estado in ['En ejecución', 'Terminado']:
            ref_contrato = record.get('referencia_contrato')
            ref_proceso = record.get('referencia_proceso')
            
            if not ref_contrato and not ref_proceso:
                issues.append(QualityIssue(
                    self.rules['CO005'],
                    'referencia_contrato/referencia_proceso',
                    None,
                    'Al menos una referencia',
                    f'Proyecto en "{estado}" sin referencia de contrato ni proceso',
                    'Completar referencia de contrato o proceso'
                ))
        
        # CO006: Dirección presente
        if not direccion or (isinstance(direccion, str) and not direccion.strip()):
            issues.append(QualityIssue(
                self.rules['CO006'],
                'direccion',
                None,
                'Dirección válida',
                'Registro sin dirección física',
                'Completar la dirección del proyecto'
            ))
        
        return issues
    
    def _validate_positional_accuracy(self, record: Dict[str, Any], geometry: Any) -> List[QualityIssue]:
        """Valida exactitud posicional."""
        issues = []
        
        if not geometry or geometry.is_empty:
            return issues
        
        # Extraer coordenadas según tipo de geometría
        coords = None
        if isinstance(geometry, Point):
            coords = [(geometry.x, geometry.y)]
        elif isinstance(geometry, (Polygon, LineString)):
            coords = list(geometry.coords) if isinstance(geometry, LineString) else list(geometry.exterior.coords)
        elif isinstance(geometry, (MultiPoint, MultiPolygon, MultiLineString)):
            coords = []
            for geom in geometry.geoms:
                if isinstance(geom, Point):
                    coords.append((geom.x, geom.y))
                elif isinstance(geom, LineString):
                    coords.extend(list(geom.coords))
                elif isinstance(geom, Polygon):
                    coords.extend(list(geom.exterior.coords))
        
        if not coords:
            return issues
        
        # PA001: Coordenadas dentro del bounding box de Cali
        for lon, lat in coords:
            if not (self.CALI_BBOX['min_lat'] <= lat <= self.CALI_BBOX['max_lat'] and
                    self.CALI_BBOX['min_lon'] <= lon <= self.CALI_BBOX['max_lon']):
                issues.append(QualityIssue(
                    self.rules['PA001'],
                    'geometry',
                    f'({lon:.6f}, {lat:.6f})',
                    f"Lat: {self.CALI_BBOX['min_lat']}-{self.CALI_BBOX['max_lat']}, Lon: {self.CALI_BBOX['min_lon']}-{self.CALI_BBOX['max_lon']}",
                    f'Coordenadas fuera del área de Cali: ({lon:.6f}, {lat:.6f})',
                    'Verificar y corregir coordenadas o sistema de referencia'
                ))
                break  # Solo reportar una vez por geometría
        
        # PA003: Coordenadas válidas (no cero ni extremos)
        for lon, lat in coords:
            if (abs(lon) < 0.0001 and abs(lat) < 0.0001) or abs(lon) > 180 or abs(lat) > 90:
                issues.append(QualityIssue(
                    self.rules['PA003'],
                    'geometry',
                    f'({lon:.6f}, {lat:.6f})',
                    'Coordenadas válidas',
                    f'Coordenadas inválidas o sospechosas: ({lon:.6f}, {lat:.6f})',
                    'Verificar y corregir coordenadas'
                ))
                break
        
        # PA004 y PA005: Validación espacial con columnas de validación existentes
        comuna_val_s2 = record.get('comunas_corregimientos_val_s2')
        barrio_val_s2 = record.get('barrio_vereda_val_s2')
        
        if comuna_val_s2 == 'ERROR':
            issues.append(QualityIssue(
                self.rules['PA004'],
                'comuna_corregimiento',
                record.get('comuna_corregimiento'),
                'Geometría dentro de la comuna',
                'La ubicación geográfica no coincide con la comuna/corregimiento declarado',
                'Verificar comuna o corregir coordenadas'
            ))
        
        if barrio_val_s2 == 'ERROR':
            issues.append(QualityIssue(
                self.rules['PA005'],
                'barrio_vereda',
                record.get('barrio_vereda'),
                'Geometría dentro del barrio',
                'La ubicación geográfica no coincide con el barrio/vereda declarado',
                'Verificar barrio o corregir coordenadas'
            ))
        
        return issues
    
    def _validate_thematic_accuracy(self, record: Dict[str, Any]) -> List[QualityIssue]:
        """Valida exactitud temática."""
        issues = []
        
        # TA001: Estado con valor permitido
        estado = record.get('estado')
        if estado and estado not in self.VALID_ESTADOS:
            issues.append(QualityIssue(
                self.rules['TA001'],
                'estado',
                estado,
                ', '.join(self.VALID_ESTADOS),
                f'Estado no válido: "{estado}"',
                f'Cambiar a uno de: {", ".join(self.VALID_ESTADOS)}'
            ))
        
        # TA002: Tipo de intervención válido
        tipo_intervencion = record.get('tipo_intervencion')
        if tipo_intervencion and tipo_intervencion not in self.VALID_TIPOS_INTERVENCION:
            # Verificar si es similar a alguno válido
            similar = self._find_similar_value(tipo_intervencion, self.VALID_TIPOS_INTERVENCION)
            suggestion = f'¿Quisiste decir "{similar}"?' if similar else f'Usar uno de: {", ".join(self.VALID_TIPOS_INTERVENCION)}'
            
            issues.append(QualityIssue(
                self.rules['TA002'],
                'tipo_intervencion',
                tipo_intervencion,
                ', '.join(self.VALID_TIPOS_INTERVENCION),
                f'Tipo de intervención no estándar: "{tipo_intervencion}"',
                suggestion
            ))
        
        # TA003: Plataforma de contratación válida
        plataforma = record.get('plataforma')
        if plataforma and plataforma not in self.VALID_PLATAFORMAS:
            similar = self._find_similar_value(plataforma, self.VALID_PLATAFORMAS)
            suggestion = f'¿Quisiste decir "{similar}"?' if similar else f'Usar una de: {", ".join(self.VALID_PLATAFORMAS)}'
            
            issues.append(QualityIssue(
                self.rules['TA003'],
                'plataforma',
                plataforma,
                ', '.join(self.VALID_PLATAFORMAS),
                f'Plataforma no estándar: "{plataforma}"',
                suggestion
            ))
        
        # TA004: Unidad de medida válida
        unidad = record.get('unidad')
        if unidad and unidad not in self.VALID_UNIDADES:
            similar = self._find_similar_value(unidad, self.VALID_UNIDADES)
            suggestion = f'¿Quisiste decir "{similar}"?' if similar else f'Usar una de: {", ".join(self.VALID_UNIDADES)}'
            
            issues.append(QualityIssue(
                self.rules['TA004'],
                'unidad',
                unidad,
                ', '.join(self.VALID_UNIDADES),
                f'Unidad de medida no estándar: "{unidad}"',
                suggestion
            ))
        
        # TA005: Formato de URL correcto
        url_proceso = record.get('url_proceso')
        if url_proceso and isinstance(url_proceso, str):
            if not self._is_valid_url(url_proceso):
                issues.append(QualityIssue(
                    self.rules['TA005'],
                    'url_proceso',
                    url_proceso,
                    'URL válida (http:// o https://)',
                    f'URL con formato inválido: "{url_proceso}"',
                    'Verificar y corregir la URL del proceso'
                ))
        
        # TA006: Comuna/Corregimiento reconocido (usando validación existente)
        comuna_val = record.get('comunas_corregimientos_val')
        if comuna_val == 'REVISAR' or comuna_val == 'ERROR':
            issues.append(QualityIssue(
                self.rules['TA006'],
                'comuna_corregimiento',
                record.get('comuna_corregimiento'),
                'Comuna o corregimiento válido de Cali',
                f'Comuna/Corregimiento requiere revisión: "{record.get("comuna_corregimiento")}"',
                'Verificar y corregir el nombre de la comuna o corregimiento'
            ))
        
        return issues
    
    def _validate_temporal_quality(self, record: Dict[str, Any]) -> List[QualityIssue]:
        """Valida calidad temporal."""
        issues = []
        
        # Parsear fechas
        fecha_inicio = self._parse_date(record.get('fecha_inicio'))
        fecha_fin = self._parse_date(record.get('fecha_fin'))
        
        # TQ001: Fecha de inicio anterior a fecha de fin
        if fecha_inicio and fecha_fin:
            if fecha_inicio >= fecha_fin:
                issues.append(QualityIssue(
                    self.rules['TQ001'],
                    'fecha_inicio/fecha_fin',
                    f'{record.get("fecha_inicio")} / {record.get("fecha_fin")}',
                    'Fecha inicio < Fecha fin',
                    f'Fecha de inicio ({record.get("fecha_inicio")}) es posterior o igual a fecha de fin ({record.get("fecha_fin")})',
                    'Verificar y corregir las fechas del proyecto'
                ))
        
        # TQ002: Formato de fecha válido
        for field in self.DATE_FIELDS:
            value = record.get(field)
            if value and not self._parse_date(value):
                issues.append(QualityIssue(
                    self.rules['TQ002'],
                    field,
                    value,
                    'Fecha válida (DD/MM/YYYY o YYYY-MM-DD)',
                    f'Formato de fecha inválido en {field}: "{value}"',
                    'Corregir al formato DD/MM/YYYY o YYYY-MM-DD'
                ))
        
        # TQ003: Fechas en rango temporal lógico
        current_year = datetime.now().year
        if fecha_inicio:
            if fecha_inicio.year < 2015 or fecha_inicio.year > current_year + 5:
                issues.append(QualityIssue(
                    self.rules['TQ003'],
                    'fecha_inicio',
                    record.get('fecha_inicio'),
                    f'2015-{current_year + 5}',
                    f'Fecha de inicio fuera de rango razonable: {record.get("fecha_inicio")}',
                    'Verificar año de inicio del proyecto'
                ))
        
        if fecha_fin:
            if fecha_fin.year < 2015 or fecha_fin.year > current_year + 10:
                issues.append(QualityIssue(
                    self.rules['TQ003'],
                    'fecha_fin',
                    record.get('fecha_fin'),
                    f'2015-{current_year + 10}',
                    f'Fecha de fin fuera de rango razonable: {record.get("fecha_fin")}',
                    'Verificar año de finalización del proyecto'
                ))
        
        # TQ004: Coherencia temporal con estado
        estado = record.get('estado')
        if estado == 'Terminado' and fecha_fin:
            if fecha_fin > datetime.now():
                issues.append(QualityIssue(
                    self.rules['TQ004'],
                    'fecha_fin',
                    record.get('fecha_fin'),
                    'Fecha pasada',
                    f'Proyecto marcado como "Terminado" pero fecha de fin es futura: {record.get("fecha_fin")}',
                    'Verificar estado del proyecto o ajustar fecha de fin'
                ))
        
        # TQ005: Duración del proyecto razonable
        if fecha_inicio and fecha_fin:
            duracion = (fecha_fin - fecha_inicio).days
            if duracion > 1825:  # 5 años
                issues.append(QualityIssue(
                    self.rules['TQ005'],
                    'fecha_inicio/fecha_fin',
                    f'{duracion} días',
                    '< 1825 días (5 años)',
                    f'Duración del proyecto excesivamente larga: {duracion} días ({duracion/365:.1f} años)',
                    'Verificar fechas del proyecto'
                ))
            elif duracion < 1:
                issues.append(QualityIssue(
                    self.rules['TQ005'],
                    'fecha_inicio/fecha_fin',
                    f'{duracion} días',
                    '>= 1 día',
                    'Duración del proyecto menor a 1 día',
                    'Verificar fechas del proyecto'
                ))
        
        return issues
    
    # ==================== MÉTODOS AUXILIARES ====================
    
    def _parse_date(self, date_value: Any) -> Optional[datetime]:
        """Intenta parsear una fecha desde múltiples formatos."""
        if not date_value:
            return None
        
        date_formats = [
            '%d/%m/%Y',
            '%Y-%m-%d',
            '%Y-%m-%d %H:%M:%S',
            '%d-%m-%Y',
            '%m/%d/%Y'
        ]
        
        date_str = str(date_value).strip()
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def _is_valid_url(self, url: str) -> bool:
        """Verifica si una URL tiene formato válido."""
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
            r'localhost|'  # localhost
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # IP
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        return bool(url_pattern.match(url))
    
    def _find_similar_value(self, value: str, valid_values: Set[str], threshold: float = 0.6) -> Optional[str]:
        """Encuentra un valor similar en un conjunto de valores válidos."""
        from difflib import SequenceMatcher
        
        best_match = None
        best_ratio = 0
        
        for valid in valid_values:
            ratio = SequenceMatcher(None, value.lower(), valid.lower()).ratio()
            if ratio > best_ratio and ratio >= threshold:
                best_ratio = ratio
                best_match = valid
        
        return best_match


def validate_geojson(geojson_path: str, verbose: bool = True) -> Dict[str, Any]:
    """
    Valida un archivo GeoJSON completo y retorna reporte de calidad.
    
    Args:
        geojson_path: Ruta al archivo GeoJSON
        verbose: Si True, imprime progreso
        
    Returns:
        Diccionario con reporte de calidad completo
    """
    import json
    
    if verbose:
        print(f"\n{'='*80}")
        print(f"🔍 VALIDACIÓN DE CALIDAD DE DATOS - ISO 19157")
        print(f"{'='*80}")
        print(f"Archivo: {geojson_path}")
    
    # Cargar GeoJSON
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    
    features = geojson_data.get('features', [])
    
    if verbose:
        print(f"Total de registros: {len(features)}")
        print(f"\n🔄 Validando registros...")
    
    # Crear validador
    validator = DataQualityValidator()
    
    # Validar cada registro
    all_issues = []
    records_with_issues = 0
    
    for idx, feature in enumerate(features):
        if verbose and (idx + 1) % 100 == 0:
            print(f"  Validados: {idx + 1}/{len(features)}")
        
        properties = feature.get('properties', {})
        geometry = feature.get('geometry')
        
        # Convertir geometría a shapely si existe
        shapely_geom = None
        if geometry:
            from shapely.geometry import shape
            try:
                shapely_geom = shape(geometry)
            except:
                pass
        
        # Validar registro
        issues = validator.validate_record(properties, shapely_geom)
        
        if issues:
            records_with_issues += 1
            all_issues.extend([{
                **issue.to_dict(),
                'upid': properties.get('upid'),
                'nombre_up': properties.get('nombre_up'),
                'nombre_centro_gestor': properties.get('nombre_centro_gestor'),
                'record_index': idx
            } for issue in issues])
    
    # Generar estadísticas
    stats = _generate_quality_statistics(all_issues, len(features))
    
    if verbose:
        print(f"\n✅ Validación completada")
        print(f"\n📊 RESUMEN:")
        print(f"  Total de registros: {len(features)}")
        print(f"  Registros con problemas: {records_with_issues} ({records_with_issues/len(features)*100:.1f}%)")
        print(f"  Total de problemas: {len(all_issues)}")
        print(f"\n  Por severidad:")
        for severity, count in stats['by_severity'].items():
            print(f"    {severity}: {count}")
        print(f"\n  Por dimensión ISO 19157:")
        for dimension, count in stats['by_dimension'].items():
            print(f"    {dimension}: {count}")
    
    return {
        'total_records': len(features),
        'records_with_issues': records_with_issues,
        'total_issues': len(all_issues),
        'issues': all_issues,
        'statistics': stats,
        'validated_at': datetime.now().isoformat()
    }


def _generate_quality_statistics(issues: List[Dict], total_records: int) -> Dict[str, Any]:
    """Genera estadísticas de calidad."""
    stats = {
        'by_severity': {},
        'by_dimension': {},
        'by_rule': {},
        'by_field': {},
        'quality_score': 0
    }
    
    for issue in issues:
        # Por severidad
        severity = issue['severity']
        stats['by_severity'][severity] = stats['by_severity'].get(severity, 0) + 1
        
        # Por dimensión
        dimension = issue['dimension']
        stats['by_dimension'][dimension] = stats['by_dimension'].get(dimension, 0) + 1
        
        # Por regla
        rule_id = issue['rule_id']
        stats['by_rule'][rule_id] = stats['by_rule'].get(rule_id, 0) + 1
        
        # Por campo
        field = issue['field_name']
        if field:
            stats['by_field'][field] = stats['by_field'].get(field, 0) + 1
    
    # Calcular score de calidad (0-100)
    # Penalizaciones por severidad
    penalties = {
        'CRITICAL': 10,
        'HIGH': 5,
        'MEDIUM': 2,
        'LOW': 1,
        'INFO': 0
    }
    
    total_penalty = sum(stats['by_severity'].get(sev, 0) * pen for sev, pen in penalties.items())
    max_penalty = total_records * 10  # Máximo si todos tuvieran un CRITICAL
    
    stats['quality_score'] = max(0, 100 - (total_penalty / max_penalty * 100)) if max_penalty > 0 else 100
    
    return stats
