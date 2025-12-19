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
    
    @staticmethod
    def _load_standard_categories():
        """Load standard categories from JSON file."""
        try:
            import os
            from pathlib import Path
            
            # Try to find the JSON file relative to this script
            current_dir = Path(__file__).parent.parent
            categories_path = current_dir / 'app_inputs' / 'unidades_proyecto_input' / 'defaults' / 'unidades_proyecto_std_categories.json'
            
            if categories_path.exists():
                with open(categories_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                print(f"⚠️ Warning: Could not find categories file at {categories_path}")
                return {}
        except Exception as e:
            print(f"⚠️ Warning: Could not load standard categories: {e}")
            return {}
    
    # Load standard categories from JSON
    _STANDARD_CATEGORIES = _load_standard_categories.__func__()
    
    # Valores válidos para campos categóricos (cargados desde JSON)
    VALID_ESTADOS = set(_STANDARD_CATEGORIES.get('estado', ['En alistamiento', 'En ejecución', 'Terminado', 'Suspendido', 'Inaugurado']))
    VALID_TIPOS_INTERVENCION = set(_STANDARD_CATEGORIES.get('tipo_intervencion', [
        'Adecuaciones', 'Demolición', 'Estudios y diseños', 'Inyección de capital',
        'Mantenimiento', 'Obra nueva', 'Rehabilitación / Reforzamiento'
    ]))
    VALID_CLASE_UP = set(_STANDARD_CATEGORIES.get('clase_up', []))
    VALID_TIPO_EQUIPAMIENTO = set(_STANDARD_CATEGORIES.get('tipo_equipamiento', []))
    VALID_FUENTE_FINANCIACION = set(_STANDARD_CATEGORIES.get('fuente_financiacion', []))
    
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
        
        rules['LC008'] = ValidationRule(
            'LC008',
            'Registro completamente duplicado',
            QualityDimension.LOGICAL_CONSISTENCY,
            SeverityLevel.CRITICAL,
            'No deben existir registros con valores idénticos en todos los campos clave'
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
            'Clase UP válida',
            QualityDimension.THEMATIC_ACCURACY,
            SeverityLevel.HIGH,
            'La clase de unidad de proyecto debe ser uno de los valores estándar'
        )
        
        rules['TA008'] = ValidationRule(
            'TA008',
            'Tipo de equipamiento válido',
            QualityDimension.THEMATIC_ACCURACY,
            SeverityLevel.MEDIUM,
            'El tipo de equipamiento debe ser uno de los valores estándar'
        )
        
        rules['TA009'] = ValidationRule(
            'TA009',
            'Fuente de financiación válida',
            QualityDimension.THEMATIC_ACCURACY,
            SeverityLevel.HIGH,
            'La fuente de financiación debe ser uno de los valores estándar'
        )
        
        rules['TA010'] = ValidationRule(
            'TA010',
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
        
        # Estados especiales que tienen sus propias reglas y no siguen la lógica estándar
        # Inaugurado: proyecto terminado que fue inaugurado oficialmente (avance debe ser 100%)
        # Suspendido: proyecto pausado (puede tener cualquier avance)
        ESTADOS_ESPECIALES = {'Inaugurado', 'Suspendido'}
        
        # LC001: Congruencia estado vs avance
        estado = record.get('estado')
        avance = record.get('avance_obra')
        
        if estado and avance is not None:
            try:
                avance_num = float(avance)
                
                # Los estados especiales tienen reglas diferentes
                if estado in ESTADOS_ESPECIALES:
                    # Inaugurado debe tener avance 100%
                    if estado == 'Inaugurado' and avance_num != 100:
                        issues.append(QualityIssue(
                            self.rules['LC001'],
                            'avance_obra',
                            avance_num,
                            100,
                            f'Estado es "Inaugurado" pero avance de obra es {avance_num}%',
                            'Ajustar avance a 100% o cambiar estado'
                        ))
                    # Suspendido puede tener cualquier avance - no genera error
                    # Si está en ESTADOS_ESPECIALES, NO continuar con validación normal
                else:
                    # Estados normales: En alistamiento, En ejecución, Terminado
                    if avance_num == 0 and estado not in ['En alistamiento']:
                        issues.append(QualityIssue(
                            self.rules['LC001'],
                            'estado',
                            estado,
                            'En alistamiento',
                            f'Avance de obra es 0% pero estado es "{estado}"',
                            'Cambiar estado a "En alistamiento"'
                        ))
                    elif avance_num == 100 and estado not in ['Terminado', 'Inaugurado']:
                        # Solo error si no es Terminado ni Inaugurado
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
                    elif estado == 'Terminado' and avance_num < 100:
                        # Estado Terminado pero avance no es 100%
                        issues.append(QualityIssue(
                            self.rules['LC001'],
                            'avance_obra',
                            avance_num,
                            100,
                            f'Estado es "Terminado" pero avance de obra es {avance_num}%',
                            'Ajustar avance a 100% o cambiar estado a "En ejecución"'
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
                # Convertir a float primero para manejar "2024.0", luego a int
                ano_num = int(float(ano))
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
                # Solo reportar si realmente no es numérico
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
        
        # TA007: Clase UP válida
        clase_up = record.get('clase_up')
        if clase_up and self.VALID_CLASE_UP and clase_up not in self.VALID_CLASE_UP:
            similar = self._find_similar_value(clase_up, self.VALID_CLASE_UP)
            suggestion = f'¿Quisiste decir "{similar}"?' if similar else f'Usar una de: {", ".join(sorted(self.VALID_CLASE_UP)[:5])}...'
            
            issues.append(QualityIssue(
                self.rules.get('TA007', self.rules['TA002']),  # Fallback to TA002 if not defined
                'clase_up',
                clase_up,
                'Valor válido de clase_up',
                f'Clase UP no estándar: "{clase_up}"',
                suggestion
            ))
        
        # TA008: Tipo de equipamiento válido
        tipo_equipamiento = record.get('tipo_equipamiento')
        if tipo_equipamiento and self.VALID_TIPO_EQUIPAMIENTO and tipo_equipamiento not in self.VALID_TIPO_EQUIPAMIENTO:
            similar = self._find_similar_value(tipo_equipamiento, self.VALID_TIPO_EQUIPAMIENTO)
            suggestion = f'¿Quisiste decir "{similar}"?' if similar else f'Usar uno de los valores estándar'
            
            issues.append(QualityIssue(
                self.rules.get('TA008', self.rules['TA002']),  # Fallback to TA002 if not defined
                'tipo_equipamiento',
                tipo_equipamiento,
                'Valor válido de tipo_equipamiento',
                f'Tipo de equipamiento no estándar: "{tipo_equipamiento}"',
                suggestion
            ))
        
        # TA009: Fuente de financiación válida
        fuente_financiacion = record.get('fuente_financiacion')
        if fuente_financiacion and self.VALID_FUENTE_FINANCIACION and fuente_financiacion not in self.VALID_FUENTE_FINANCIACION:
            similar = self._find_similar_value(fuente_financiacion, self.VALID_FUENTE_FINANCIACION)
            suggestion = f'¿Quisiste decir "{similar}"?' if similar else f'Usar una de: {", ".join(sorted(self.VALID_FUENTE_FINANCIACION)[:3])}...'
            
            issues.append(QualityIssue(
                self.rules.get('TA009', self.rules['TA002']),  # Fallback to TA002 if not defined
                'fuente_financiacion',
                fuente_financiacion,
                'Valor válido de fuente_financiacion',
                f'Fuente de financiación no estándar: "{fuente_financiacion}"',
                suggestion
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
    Maneja estructura jerárquica: unidades de proyecto con array de intervenciones.
    
    Args:
        geojson_path: Ruta al archivo GeoJSON
        verbose: Si True, imprime progreso
        
    Returns:
        Diccionario con reporte de calidad completo
    """
    import json
    import hashlib
    
    if verbose:
        print(f"\n{'='*80}")
        print(f"🔍 VALIDACIÓN DE CALIDAD DE DATOS - ISO 19157")
        print(f"{'='*80}")
        print(f"Archivo: {geojson_path}")
        print("📊 Estructura: Unidades de Proyecto (jerárquica)")
    
    # Cargar GeoJSON
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    
    features = geojson_data.get('features', [])
    
    # Contar total de intervenciones
    total_intervenciones = sum(len(f.get('properties', {}).get('intervenciones', [])) for f in features)
    
    if verbose:
        print(f"Total unidades de proyecto: {len(features)}")
        print(f"Total intervenciones: {total_intervenciones}")
        print(f"\n🔄 Validando unidades e intervenciones...")
    
    # Crear validador
    validator = DataQualityValidator()
    
    # Validar cada unidad y sus intervenciones
    all_issues = []
    unidades_with_issues = 0
    intervenciones_with_issues = 0
    
    # Detectar duplicados completos
    record_hashes = {}
    duplicate_groups = []
    
    for idx, feature in enumerate(features):
        if verbose and (idx + 1) % 100 == 0:
            print(f"  Validadas: {idx + 1}/{len(features)} unidades, {sum(len(features[i].get('properties', {}).get('intervenciones', [])) for i in range(idx+1))} intervenciones")
        
        properties = feature.get('properties', {})
        geometry = feature.get('geometry')
        upid = properties.get('upid')
        nombre_up = properties.get('nombre_up')
        
        unidad_has_issues = False
        
        # ================================================================
        # VALIDAR UNIDAD DE PROYECTO
        # ================================================================
        
        # Campos obligatorios a nivel de unidad
        unit_required_fields = ['upid', 'nombre_up', 'direccion', 'tipo_equipamiento', 'clase_up']
        
        for field in unit_required_fields:
            value = properties.get(field)
            if value is None or (isinstance(value, str) and value.strip() == ''):
                all_issues.append({
                    'rule_id': 'CO001',
                    'rule_name': 'Campo obligatorio vacío (unidad)',
                    'dimension': QualityDimension.COMPLETENESS.value,
                    'severity': SeverityLevel.CRITICAL.value,
                    'field_name': f'unidad.{field}',
                    'current_value': None,
                    'expected_value': 'Valor requerido',
                    'details': f'Campo obligatorio vacío en unidad: {field}',
                    'suggestion': f'Completar el campo {field}',
                    'detected_at': datetime.now().isoformat(),
                    'upid': upid,
                    'nombre_up': nombre_up,
                    'record_index': idx,
                    'level': 'unidad'
                })
                unidad_has_issues = True
        
        # Validar geometría
        if properties.get('direccion'):
            shapely_geom = None
            if geometry:
                from shapely.geometry import shape
                try:
                    shapely_geom = shape(geometry)
                    if not shapely_geom.is_empty:
                        # Validar que esté en Cali
                        coords = list(shapely_geom.coords)[0] if hasattr(shapely_geom, 'coords') else None
                        if coords:
                            lon, lat = coords
                            # Bounding box extendido de Cali (incluyendo área metropolitana)
                            # Lon: -76.7 a -76.4, Lat: 3.3 a 3.6
                            
                            # Detectar coordenadas invertidas (lat, lon en lugar de lon, lat)
                            if 3.3 <= lon <= 3.6 and -76.7 <= lat <= -76.4:
                                all_issues.append({
                                    'rule_id': 'PA002',
                                    'rule_name': 'Coordenadas invertidas',
                                    'dimension': QualityDimension.POSITIONAL_ACCURACY.value,
                                    'severity': SeverityLevel.CRITICAL.value,
                                    'field_name': 'geometry',
                                    'current_value': f'[{lon}, {lat}]',
                                    'expected_value': '[lon, lat] no [lat, lon]',
                                    'details': f'Las coordenadas parecen estar invertidas (lat, lon)',
                                    'suggestion': 'Invertir coordenadas: [{lat}, {lon}]',
                                    'detected_at': datetime.now().isoformat(),
                                    'upid': upid,
                                    'nombre_up': nombre_up,
                                    'record_index': idx,
                                    'level': 'unidad'
                                })
                                unidad_has_issues = True
                            elif not (-76.7 <= lon <= -76.4 and 3.3 <= lat <= 3.6):
                                # Coordenadas claramente fuera de Cali
                                all_issues.append({
                                    'rule_id': 'PA001',
                                    'rule_name': 'Coordenadas fuera de Cali',
                                    'dimension': QualityDimension.POSITIONAL_ACCURACY.value,
                                    'severity': SeverityLevel.HIGH.value,
                                    'field_name': 'geometry',
                                    'current_value': f'[{lon}, {lat}]',
                                    'expected_value': 'Lon: -76.7 a -76.4, Lat: 3.3 a 3.6',
                                    'details': f'Coordenadas fuera del área de Cali y región',
                                    'suggestion': 'Verificar y corregir coordenadas',
                                    'detected_at': datetime.now().isoformat(),
                                    'upid': upid,
                                    'nombre_up': nombre_up,
                                    'record_index': idx,
                                    'level': 'unidad'
                                })
                                unidad_has_issues = True
                except:
                    pass
            
            if geometry is None or (shapely_geom and shapely_geom.is_empty):
                all_issues.append({
                    'rule_id': 'CO002',
                    'rule_name': 'Geometría faltante',
                    'dimension': QualityDimension.COMPLETENESS.value,
                    'severity': SeverityLevel.MEDIUM.value,
                    'field_name': 'geometry',
                    'current_value': 'NULL',
                    'expected_value': 'Point',
                    'details': f'Unidad con dirección pero sin geometría',
                    'suggestion': 'Geolocalizar la dirección',
                    'detected_at': datetime.now().isoformat(),
                    'upid': upid,
                    'nombre_up': nombre_up,
                    'record_index': idx,
                    'level': 'unidad'
                })
                unidad_has_issues = True
        
        # Validar tipo_equipamiento y clase_up
        tipo_equipamiento = properties.get('tipo_equipamiento')
        if tipo_equipamiento and tipo_equipamiento not in validator.VALID_TIPO_EQUIPAMIENTO:
            all_issues.append({
                'rule_id': 'TA008',
                'rule_name': 'Tipo de equipamiento inválido',
                'dimension': QualityDimension.THEMATIC_ACCURACY.value,
                'severity': SeverityLevel.LOW.value,
                'field_name': 'tipo_equipamiento',
                'current_value': tipo_equipamiento,
                'expected_value': 'Valor del catálogo',
                'details': f'Tipo de equipamiento no reconocido',
                'suggestion': 'Usar valor del catálogo estándar',
                'detected_at': datetime.now().isoformat(),
                'upid': upid,
                'nombre_up': nombre_up,
                'record_index': idx,
                'level': 'unidad'
            })
            unidad_has_issues = True
        
        # ================================================================
        # VALIDAR INTERVENCIONES
        # ================================================================
        
        intervenciones = properties.get('intervenciones', [])
        
        if not intervenciones:
            all_issues.append({
                'rule_id': 'LC009',
                'rule_name': 'Unidad sin intervenciones',
                'dimension': QualityDimension.LOGICAL_CONSISTENCY.value,
                'severity': SeverityLevel.HIGH.value,
                'field_name': 'intervenciones',
                'current_value': '[]',
                'expected_value': 'Al menos 1 intervención',
                'details': f'Unidad de proyecto sin intervenciones asociadas',
                'suggestion': 'Asociar al menos una intervención',
                'detected_at': datetime.now().isoformat(),
                'upid': upid,
                'nombre_up': nombre_up,
                'record_index': idx,
                'level': 'unidad'
            })
            unidad_has_issues = True
        
        for interv_idx, intervencion in enumerate(intervenciones):
            intervencion_has_issues = False
            
            # Validar que intervencion sea un diccionario
            if not isinstance(intervencion, dict):
                all_issues.append({
                    'rule_id': 'LC010',
                    'rule_name': 'Formato de intervención inválido',
                    'dimension': QualityDimension.LOGICAL_CONSISTENCY.value,
                    'severity': SeverityLevel.CRITICAL.value,
                    'field_name': 'intervenciones',
                    'current_value': str(type(intervencion).__name__),
                    'expected_value': 'dict',
                    'details': f'Intervención en posición {interv_idx} no es un diccionario (es {type(intervencion).__name__})',
                    'suggestion': 'Verificar estructura de datos en Firebase - intervenciones debe ser array de objetos',
                    'detected_at': datetime.now().isoformat(),
                    'upid': upid,
                    'nombre_up': nombre_up,
                    'record_index': idx,
                    'intervencion_index': interv_idx,
                    'level': 'intervencion'
                })
                unidad_has_issues = True
                continue  # Saltar esta intervención
            
            intervencion_id = intervencion.get('intervencion_id', f'{upid}-INT{interv_idx}')
            
            # Campos obligatorios de intervención
            interv_required_fields = ['estado', 'tipo_intervencion', 'presupuesto_base', 'ano']
            
            for field in interv_required_fields:
                value = intervencion.get(field)
                if value is None or (isinstance(value, str) and value.strip() == ''):
                    all_issues.append({
                        'rule_id': 'CO001',
                        'rule_name': 'Campo obligatorio vacío (intervención)',
                        'dimension': QualityDimension.COMPLETENESS.value,
                        'severity': SeverityLevel.HIGH.value,
                        'field_name': f'intervencion.{field}',
                        'current_value': None,
                        'expected_value': 'Valor requerido',
                        'details': f'Campo obligatorio vacío en intervención: {field}',
                        'suggestion': f'Completar el campo {field}',
                        'detected_at': datetime.now().isoformat(),
                        'upid': upid,
                        'nombre_up': nombre_up,
                        'intervencion_id': intervencion_id,
                        'record_index': idx,
                        'intervencion_index': interv_idx,
                        'level': 'intervencion'
                    })
                    intervencion_has_issues = True
            
            # Validar estado
            estado = intervencion.get('estado')
            if estado and estado not in validator.VALID_ESTADOS:
                all_issues.append({
                    'rule_id': 'TA001',
                    'rule_name': 'Estado inválido',
                    'dimension': QualityDimension.THEMATIC_ACCURACY.value,
                    'severity': SeverityLevel.MEDIUM.value,
                    'field_name': 'estado',
                    'current_value': estado,
                    'expected_value': ', '.join(validator.VALID_ESTADOS),
                    'details': f'Estado no reconocido: {estado}',
                    'suggestion': 'Normalizar estado',
                    'detected_at': datetime.now().isoformat(),
                    'upid': upid,
                    'nombre_up': nombre_up,
                    'intervencion_id': intervencion_id,
                    'record_index': idx,
                    'intervencion_index': interv_idx,
                    'level': 'intervencion'
                })
                intervencion_has_issues = True
            
            # Validar presupuesto
            presupuesto = intervencion.get('presupuesto_base')
            if presupuesto is not None:
                try:
                    pres_float = float(presupuesto)
                    if pres_float <= 0:
                        all_issues.append({
                            'rule_id': 'LC001',
                            'rule_name': 'Presupuesto inválido',
                            'dimension': QualityDimension.LOGICAL_CONSISTENCY.value,
                            'severity': SeverityLevel.HIGH.value,
                            'field_name': 'presupuesto_base',
                            'current_value': pres_float,
                            'expected_value': '> 0',
                            'details': f'Presupuesto cero o negativo',
                            'suggestion': 'Verificar presupuesto',
                            'detected_at': datetime.now().isoformat(),
                            'upid': upid,
                            'nombre_up': nombre_up,
                            'intervencion_id': intervencion_id,
                            'record_index': idx,
                            'intervencion_index': interv_idx,
                            'level': 'intervencion'
                        })
                        intervencion_has_issues = True
                except:
                    pass
            
            # Validar avance_obra
            avance = intervencion.get('avance_obra')
            if avance is not None:
                try:
                    avance_float = float(avance)
                    if not (0 <= avance_float <= 100):
                        all_issues.append({
                            'rule_id': 'LC002',
                            'rule_name': 'Avance fuera de rango',
                            'dimension': QualityDimension.LOGICAL_CONSISTENCY.value,
                            'severity': SeverityLevel.MEDIUM.value,
                            'field_name': 'avance_obra',
                            'current_value': avance_float,
                            'expected_value': '0-100',
                            'details': f'Avance fuera del rango 0-100',
                            'suggestion': 'Corregir avance',
                            'detected_at': datetime.now().isoformat(),
                            'upid': upid,
                            'nombre_up': nombre_up,
                            'intervencion_id': intervencion_id,
                            'record_index': idx,
                            'intervencion_index': interv_idx,
                            'level': 'intervencion'
                        })
                        intervencion_has_issues = True
                except:
                    pass
            
            # Validar consistencia estado-avance
            if estado == 'Terminado' and avance and float(avance) < 100:
                all_issues.append({
                    'rule_id': 'LC003',
                    'rule_name': 'Inconsistencia estado-avance',
                    'dimension': QualityDimension.LOGICAL_CONSISTENCY.value,
                    'severity': SeverityLevel.MEDIUM.value,
                    'field_name': 'estado/avance_obra',
                    'current_value': f'{estado} / {avance}%',
                    'expected_value': 'Terminado / 100%',
                    'details': f'Estado "Terminado" pero avance {avance}%',
                    'suggestion': 'Ajustar estado o avance',
                    'detected_at': datetime.now().isoformat(),
                    'upid': upid,
                    'nombre_up': nombre_up,
                    'intervencion_id': intervencion_id,
                    'record_index': idx,
                    'intervencion_index': interv_idx,
                    'level': 'intervencion'
                })
                intervencion_has_issues = True
            
            if intervencion_has_issues:
                intervenciones_with_issues += 1
                unidad_has_issues = True
        
        if unidad_has_issues:
            unidades_with_issues += 1
    
    # No validar duplicados en estructura jerárquica (UPIDs son únicos por diseño)
    
    # Generar estadísticas mejoradas
    stats = _generate_quality_statistics(all_issues, len(features), 0)
    
    if verbose:
        print(f"\n✅ Validación completada")
        print(f"\n📊 RESUMEN:")
        print(f"  Total unidades de proyecto: {len(features)}")
        print(f"  Total intervenciones: {total_intervenciones}")
        print(f"  Unidades con problemas: {unidades_with_issues} ({unidades_with_issues/len(features)*100:.1f}%)")
        print(f"  Intervenciones con problemas: {intervenciones_with_issues} ({intervenciones_with_issues/total_intervenciones*100:.1f}% del total)")
        print(f"  Total de problemas detectados: {len(all_issues)}")
        print(f"\n  Por severidad:")
        for severity, count in stats['by_severity'].items():
            emoji = {'CRITICAL': '🔴', 'HIGH': '🟠', 'MEDIUM': '🟡', 'LOW': '🔵', 'INFO': '⚪'}.get(severity, '')
            print(f"    {emoji} {severity}: {count}")
        print(f"\n  Por dimensión ISO 19157:")
        for dimension, count in stats['by_dimension'].items():
            print(f"    {dimension}: {count}")
        print(f"\n  Top 5 problemas más frecuentes:")
        for rule_id, rule_info in list(stats['top_issues'].items())[:5]:
            print(f"    {rule_id}: {rule_info['count']} ocurrencias - {rule_info['name']}")
    
    return {
        'total_records': len(features),
        'total_unidades': len(features),
        'total_intervenciones': total_intervenciones,
        'unique_records': len(features),
        'duplicate_groups': 0,
        'duplicate_records': 0,
        'unidades_with_issues': unidades_with_issues,
        'intervenciones_with_issues': intervenciones_with_issues,
        'records_with_issues': unidades_with_issues,
        'records_without_issues': len(features) - unidades_with_issues,
        'total_issues': len(all_issues),
        'issues': all_issues,
        'duplicate_details': [],
        'statistics': stats,
        'validated_at': datetime.now().isoformat()
    }


def _generate_quality_statistics(issues: List[Dict], total_records: int, duplicate_groups: int = 0) -> Dict[str, Any]:
    """Genera estadísticas de calidad mejoradas y más comprensibles."""
    stats = {
        'by_severity': {},
        'by_dimension': {},
        'by_rule': {},
        'by_field': {},
        'top_issues': {},
        'quality_score': 0,
        'quality_rating': '',
        'total_records': total_records,
        'records_affected_percentage': 0,
        'duplicate_groups': duplicate_groups
    }
    
    # Contador de registros únicos afectados
    affected_records = set()
    
    for issue in issues:
        # Rastrear registros afectados
        if 'upid' in issue and issue['upid']:
            affected_records.add(issue['upid'])
        
        # Por severidad
        severity = issue['severity']
        stats['by_severity'][severity] = stats['by_severity'].get(severity, 0) + 1
        
        # Por dimensión
        dimension = issue['dimension']
        stats['by_dimension'][dimension] = stats['by_dimension'].get(dimension, 0) + 1
        
        # Por regla (con información adicional)
        rule_id = issue['rule_id']
        if rule_id not in stats['by_rule']:
            stats['by_rule'][rule_id] = {
                'count': 0,
                'name': issue.get('rule_name', rule_id),
                'severity': issue['severity'],
                'dimension': issue['dimension']
            }
        stats['by_rule'][rule_id]['count'] += 1
        
        # Por campo
        field = issue['field_name']
        if field:
            if field not in stats['by_field']:
                stats['by_field'][field] = {
                    'count': 0,
                    'issues': []
                }
            stats['by_field'][field]['count'] += 1
            if issue['rule_id'] not in stats['by_field'][field]['issues']:
                stats['by_field'][field]['issues'].append(issue['rule_id'])
    
    # Calcular top issues (más frecuentes)
    sorted_rules = sorted(stats['by_rule'].items(), key=lambda x: x[1]['count'], reverse=True)
    stats['top_issues'] = dict(sorted_rules)
    
    # Calcular porcentaje de registros afectados
    stats['records_affected'] = len(affected_records)
    stats['records_affected_percentage'] = (len(affected_records) / total_records * 100) if total_records > 0 else 0
    
    # Calcular score de calidad (0-100) - Nueva fórmula más realista
    # Basada en el porcentaje de registros sin problemas críticos/altos
    # Más enfocada en la proporción de registros "limpios" vs "problemáticos"
    
    # Contar registros únicos por nivel de severidad máxima
    records_by_max_severity = {}
    for upid in affected_records:
        record_issues = [i for i in issues if i.get('upid') == upid]
        if record_issues:
            # Encontrar la severidad máxima de ese registro
            severities_order = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO']
            max_sev = 'INFO'
            for issue in record_issues:
                sev = issue.get('severity', 'INFO')
                if severities_order.index(sev) < severities_order.index(max_sev):
                    max_sev = sev
            records_by_max_severity[max_sev] = records_by_max_severity.get(max_sev, 0) + 1
    
    # Penalizaciones por registro según su peor problema
    severity_weights = {
        'CRITICAL': 1.0,   # Registro totalmente penalizado
        'HIGH': 0.7,       # 70% penalizado
        'MEDIUM': 0.3,     # 30% penalizado
        'LOW': 0.1,        # 10% penalizado
        'INFO': 0.0        # No penaliza
    }
    
    # Calcular penalización total basada en registros, no en issues
    total_weighted_records = sum(
        records_by_max_severity.get(sev, 0) * weight 
        for sev, weight in severity_weights.items()
    )
    
    # El score es el porcentaje de "calidad" (registros no penalizados)
    if total_records > 0:
        penalty_ratio = total_weighted_records / total_records
        stats['quality_score'] = round(max(0, (1 - penalty_ratio) * 100), 2)
    else:
        stats['quality_score'] = 100
    
    # Asignar rating cualitativo
    score = stats['quality_score']
    if score >= 90:
        stats['quality_rating'] = 'EXCELENTE'
    elif score >= 75:
        stats['quality_rating'] = 'BUENA'
    elif score >= 60:
        stats['quality_rating'] = 'ACEPTABLE'
    elif score >= 40:
        stats['quality_rating'] = 'REGULAR'
    else:
        stats['quality_rating'] = 'DEFICIENTE'
    
    # Estadísticas adicionales útiles
    stats['issues_per_record'] = len(issues) / total_records if total_records > 0 else 0
    stats['critical_issues'] = stats['by_severity'].get('CRITICAL', 0)
    stats['high_issues'] = stats['by_severity'].get('HIGH', 0)
    stats['actionable_issues'] = stats['critical_issues'] + stats['high_issues']  # Issues que deben corregirse
    
    return stats
