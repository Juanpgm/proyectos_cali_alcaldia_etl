# -*- coding: utf-8 -*-
"""
Database models for ETL pipeline - Cali Municipality Projects
This module contains SQLAlchemy models for storing project data extracted from various sources.
"""

from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Date, Text, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime, date
from typing import Optional


def safe_parse_date(date_value):
    """
    Safely parse various date formats including Excel serial numbers.
    
    Args:
        date_value: String, number, or None representing a date
        
    Returns:
        date object or None if parsing fails
    """
    if not date_value or date_value == "Sin información":
        return None
    
    # Handle string values
    if isinstance(date_value, str):
        # Remove extra whitespace
        date_value = date_value.strip()
        
        # Handle empty or "Sin información"
        if not date_value or date_value.lower() == "sin información":
            return None
            
        # Try standard format first
        try:
            return datetime.strptime(date_value, '%Y-%m-%d').date()
        except ValueError:
            pass
            
        # Try other common formats
        formats = ['%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', '%d-%m-%Y']
        for fmt in formats:
            try:
                return datetime.strptime(date_value, fmt).date()
            except ValueError:
                continue
                
        # Try to parse as Excel serial number if it's numeric string
        try:
            serial_number = float(date_value)
            # Excel epoch starts at 1900-01-01, but Excel treats 1900 as leap year
            # So we need to subtract 2 days to get correct date
            excel_epoch = date(1900, 1, 1)
            delta_days = int(serial_number) - 2
            return date.fromordinal(excel_epoch.toordinal() + delta_days)
        except (ValueError, OverflowError):
            pass
    
    # Handle numeric values (Excel serial numbers)
    elif isinstance(date_value, (int, float)):
        try:
            # Excel epoch starts at 1900-01-01, but Excel treats 1900 as leap year
            excel_epoch = date(1900, 1, 1)
            delta_days = int(date_value) - 2
            return date.fromordinal(excel_epoch.toordinal() + delta_days)
        except (ValueError, OverflowError):
            pass
    
    # If all parsing attempts fail, return None
    return None
import json

Base = declarative_base()


class UnidadProyecto(Base):
    """
    Model for Project Units (Unidades de Proyecto) - Infrastructure Equipment
    
    This model stores data extracted from Google Sheets about municipal infrastructure
    projects including education, health, sports and other public facilities.
    
    Data Source: Google Sheets 'obras_equipamientos' 
    Generated from: extraction_app/data_extraction_unidades_proyecto_equipamientos.py
    """
    
    __tablename__ = 'unidad_proyecto'
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Original identifiers from source data
    key = Column(String(50), unique=True, nullable=False, index=True, 
                 comment="Unique identifier from source (e.g., UNPEQP-1)")
    origen_sheet = Column(String(100), nullable=True, 
                         comment="Source sheet type (e.g., educacion)")
    bpin = Column(BigInteger, nullable=True, index=True,
                  comment="Bank of Investment Projects identifier")
    identificador = Column(String(500), nullable=True,
                          comment="Project identifier/description")
    dataframe = Column(String(500), nullable=True,
                      comment="Source Excel file name")
    
    # Financial and administrative data
    fuente_financiacion = Column(String(100), nullable=True, index=True,
                                comment="Funding source (e.g., Empréstito, SGP)")
    presupuesto_base = Column(Float, nullable=True,
                             comment="Base budget amount")
    avance_obra = Column(Float, nullable=True,
                        comment="Construction progress percentage (0-100)")
    ano = Column(Integer, nullable=True, index=True,
                comment="Project year")
    usuarios = Column(Float, nullable=True,
                     comment="Number of beneficiary users")
    
    # Project classification
    clase_obra = Column(String(200), nullable=True, index=True,
                       comment="Project class (e.g., Infraestructura Educativa)")
    subclase = Column(String(200), nullable=True,
                     comment="Project subclass/category")
    tipo_intervencion = Column(String(200), nullable=True,
                              comment="Type of intervention")
    descripcion_intervencion = Column(Text, nullable=True,
                                     comment="Detailed intervention description")
    
    # Location data
    comuna_corregimiento = Column(String(100), nullable=True, index=True,
                                 comment="Municipality/district")
    barrio_vereda = Column(String(200), nullable=True,
                          comment="Neighborhood/village")
    direccion = Column(String(500), nullable=True,
                      comment="Physical address")
    
    # Responsible parties
    nickname = Column(String(200), nullable=True,
                     comment="Responsible person nickname/name")
    nickname_detalle = Column(String(200), nullable=True,
                             comment="Responsibility detail")
    
    # Technical specifications
    unidad = Column(String(50), nullable=True,
                   comment="Unit of measurement")
    cantidad = Column(String(100), nullable=True,
                     comment="Quantity (stored as string due to mixed formats)")
    
    # Geographic coordinates
    latitude = Column(Float, nullable=True, index=True,
                     comment="Latitude in decimal degrees")
    longitude = Column(Float, nullable=True, index=True,
                      comment="Longitude in decimal degrees")
    lat = Column(String(50), nullable=True,
                comment="Original latitude string from source")
    lon = Column(String(50), nullable=True,
                comment="Original longitude string from source")
    geometry_type = Column(String(50), nullable=True,
                          comment="GeoJSON geometry type (e.g., Point)")
    geometry_bounds = Column(Text, nullable=True,
                           comment="JSON string with geometry bounds")
    
    # Process references and tracking
    referencia_proceso = Column(String(200), nullable=True,
                               comment="Process reference number")
    referencia_contrato = Column(String(200), nullable=True,
                                comment="Contract reference number")
    
    # Timeline
    fecha_inicio = Column(Date, nullable=True,
                         comment="Project start date")
    fecha_fin = Column(Date, nullable=True,
                      comment="Project end date")
    
    # Flags and additional data
    centros_gravedad = Column(Boolean, default=False,
                             comment="Gravity centers flag")
    microtio = Column(String(200), nullable=True,
                     comment="Micro territory classification")
    
    # Metadata
    processed_timestamp = Column(DateTime, nullable=True,
                                comment="When the record was processed")
    created_at = Column(DateTime, default=func.now(), nullable=False,
                       comment="Record creation timestamp")
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False,
                       comment="Record last update timestamp")
    
    def __repr__(self):
        return f"<UnidadProyecto(key='{self.key}', identificador='{self.identificador}')>"
    
    def to_dict(self) -> dict:
        """Convert model instance to dictionary."""
        return {
            'id': self.id,
            'key': self.key,
            'origen_sheet': self.origen_sheet,
            'bpin': self.bpin,
            'identificador': self.identificador,
            'dataframe': self.dataframe,
            'fuente_financiacion': self.fuente_financiacion,
            'presupuesto_base': self.presupuesto_base,
            'avance_obra': self.avance_obra,
            'ano': self.ano,
            'usuarios': self.usuarios,
            'clase_obra': self.clase_obra,
            'subclase': self.subclase,
            'tipo_intervencion': self.tipo_intervencion,
            'descripcion_intervencion': self.descripcion_intervencion,
            'comuna_corregimiento': self.comuna_corregimiento,
            'barrio_vereda': self.barrio_vereda,
            'direccion': self.direccion,
            'nickname': self.nickname,
            'nickname_detalle': self.nickname_detalle,
            'unidad': self.unidad,
            'cantidad': self.cantidad,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'lat': self.lat,
            'lon': self.lon,
            'geometry_type': self.geometry_type,
            'geometry_bounds': self.geometry_bounds,
            'referencia_proceso': self.referencia_proceso,
            'referencia_contrato': self.referencia_contrato,
            'fecha_inicio': self.fecha_inicio,
            'fecha_fin': self.fecha_fin,
            'centros_gravedad': self.centros_gravedad,
            'microtio': self.microtio,
            'processed_timestamp': self.processed_timestamp,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
    
    @classmethod
    def from_geojson_feature(cls, feature: dict) -> 'UnidadProyecto':
        """
        Create UnidadProyecto instance from GeoJSON feature.
        
        Args:
            feature: GeoJSON feature dictionary
            
        Returns:
            UnidadProyecto instance
        """
        properties = feature.get('properties', {})
        geometry = feature.get('geometry', {})
        
        # Parse processed_timestamp if present
        processed_timestamp = None
        if properties.get('processed_timestamp'):
            try:
                processed_timestamp = datetime.fromisoformat(
                    properties['processed_timestamp'].replace('Z', '+00:00')
                )
            except (ValueError, AttributeError):
                processed_timestamp = None
        
        # Parse date fields (fecha_* fields are Date type, not DateTime)
        fecha_inicio = None
        fecha_fin = None
        
        if properties.get('fecha_inicio'):
            try:
                # Parse as date only (no time component)
                date_str = str(properties['fecha_inicio'])
                if 'T' in date_str:
                    # If it's a full datetime string, extract just the date part
                    fecha_inicio = datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
                else:
                    # If it's already just a date string
                    fecha_inicio = safe_parse_date(date_str)
            except (ValueError, TypeError):
                pass
                
        if properties.get('fecha_fin'):
            try:
                # Parse as date only (no time component)
                date_str = str(properties['fecha_fin'])
                if 'T' in date_str:
                    # If it's a full datetime string, extract just the date part
                    fecha_fin = datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
                else:
                    # If it's already just a date string
                    fecha_fin = safe_parse_date(date_str)
            except (ValueError, TypeError):
                pass
        
        # Extract coordinates from geometry if available
        latitude = properties.get('latitude')
        longitude = properties.get('longitude')
        
        if geometry and geometry.get('type') == 'Point' and geometry.get('coordinates'):
            coords = geometry['coordinates']
            if len(coords) >= 2:
                # GeoJSON format: [longitude, latitude]
                if longitude is None:
                    longitude = coords[0]
                if latitude is None:
                    latitude = coords[1]
        
        return cls(
            key=properties.get('key'),
            origen_sheet=properties.get('origen_sheet'),
            bpin=int(properties['bpin']) if properties.get('bpin') and properties['bpin'] != 0 else None,
            identificador=properties.get('identificador'),
            dataframe=properties.get('dataframe'),
            fuente_financiacion=properties.get('fuente_financiacion'),
            presupuesto_base=properties.get('presupuesto_base'),
            avance_obra=properties.get('avance_obra'),
            ano=int(properties['ano']) if properties.get('ano') else None,
            usuarios=properties.get('usuarios'),
            clase_obra=properties.get('clase_obra'),
            subclase=properties.get('subclase'),
            tipo_intervencion=properties.get('tipo_intervencion'),
            descripcion_intervencion=properties.get('descripcion_intervencion'),
            comuna_corregimiento=properties.get('comuna_corregimiento'),
            barrio_vereda=properties.get('barrio_vereda'),
            direccion=properties.get('direccion'),
            nickname=properties.get('nickname'),
            nickname_detalle=properties.get('nickname_detalle'),
            unidad=properties.get('unidad'),
            cantidad=str(properties.get('cantidad')) if properties.get('cantidad') is not None else None,
            latitude=latitude,
            longitude=longitude,
            lat=properties.get('lat'),
            lon=properties.get('lon'),
            geometry_type=properties.get('geometry_type'),
            geometry_bounds=properties.get('geometry_bounds'),
            referencia_proceso=properties.get('referencia_proceso'),
            referencia_contrato=properties.get('referencia_contrato'),
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            centros_gravedad=bool(properties.get('centros_gravedad', False)),
            microtio=properties.get('microtio'),
            processed_timestamp=processed_timestamp
        )
    
    @property
    def has_coordinates(self) -> bool:
        """Check if the project has valid geographic coordinates."""
        return (self.latitude is not None and self.longitude is not None and
                -90 <= self.latitude <= 90 and -180 <= self.longitude <= 180)
    
    @property
    def geometry_bounds_dict(self) -> Optional[dict]:
        """Parse geometry_bounds JSON string to dictionary."""
        if self.geometry_bounds:
            try:
                return json.loads(self.geometry_bounds)
            except (json.JSONDecodeError, TypeError):
                return None
        return None


class DatosCaracteristicosProyecto(Base):
    """
    Model for Project Characteristic Data (Datos Característicos de Proyectos)
    
    This model stores characteristic and descriptive data about municipal projects
    including budget classifications, organizational structure, and project categorization.
    
    Data Source: transformation_app/app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json
    Records: ~1,254 entries
    """
    
    __tablename__ = 'datos_caracteristicos_proyecto'
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Project identifiers
    bpin = Column(BigInteger, nullable=False, index=True,
                  comment="Bank of Investment Projects identifier")
    bp = Column(String(20), nullable=True, index=True,
               comment="Budget project code (e.g., BP26005292)")
    
    # Project descriptive data
    nombre_proyecto = Column(Text, nullable=True,
                           comment="Full project name")
    nombre_actividad = Column(Text, nullable=True,
                            comment="Activity or component name")
    
    # Organizational structure
    programa_presupuestal = Column(String(50), nullable=True, index=True,
                                 comment="Budget program code")
    nombre_centro_gestor = Column(String(200), nullable=True, index=True,
                                comment="Management center name")
    nombre_area_funcional = Column(Text, nullable=True,
                                 comment="Functional area description")
    
    # Financial classification
    nombre_fondo = Column(String(200), nullable=True, index=True,
                        comment="Fund name")
    clasificacion_fondo = Column(String(200), nullable=True, index=True,
                               comment="Fund classification")
    nombre_pospre = Column(Text, nullable=True,
                         comment="POSPRE classification name")
    
    # Strategic planning
    nombre_dimension = Column(String(200), nullable=True,
                            comment="Strategic dimension name")
    nombre_linea_estrategica = Column(String(200), nullable=True,
                                    comment="Strategic line name")
    nombre_programa = Column(String(200), nullable=True, index=True,
                           comment="Program name")
    
    # Geographic and administrative
    comuna = Column(String(100), nullable=True, index=True,
                   comment="Municipality/commune")
    origen = Column(String(50), nullable=True, index=True,
                   comment="Origin classification (e.g., Organismo)")
    anio = Column(Integer, nullable=True, index=True,
                 comment="Project year")
    tipo_gasto = Column(String(50), nullable=True, index=True,
                       comment="Expense type (e.g., Inversión)")
    
    # Additional codes
    cod_sector = Column(String(50), nullable=True,
                       comment="Sector code")
    cod_producto = Column(String(50), nullable=True,
                         comment="Product code")
    validador_cuipo = Column(String(50), nullable=True,
                           comment="CUIPO validator")
    
    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False,
                       comment="Record creation timestamp")
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False,
                       comment="Record last update timestamp")
    
    def __repr__(self):
        return f"<DatosCaracteristicosProyecto(bpin={self.bpin}, bp='{self.bp}', nombre_proyecto='{self.nombre_proyecto[:50]}...')>"
    
    @classmethod
    def from_json(cls, data: dict) -> 'DatosCaracteristicosProyecto':
        """Create instance from JSON data."""
        return cls(
            bpin=data.get('bpin'),
            bp=data.get('bp'),
            nombre_proyecto=data.get('nombre_proyecto'),
            nombre_actividad=data.get('nombre_actividad'),
            programa_presupuestal=str(data.get('programa_presupuestal')) if data.get('programa_presupuestal') is not None else None,
            nombre_centro_gestor=data.get('nombre_centro_gestor'),
            nombre_area_funcional=data.get('nombre_area_funcional'),
            nombre_fondo=data.get('nombre_fondo'),
            clasificacion_fondo=data.get('clasificacion_fondo'),
            nombre_pospre=data.get('nombre_pospre'),
            nombre_dimension=data.get('nombre_dimension'),
            nombre_linea_estrategica=data.get('nombre_linea_estrategica'),
            nombre_programa=data.get('nombre_programa'),
            comuna=data.get('comuna'),
            origen=data.get('origen'),
            anio=data.get('anio'),
            tipo_gasto=data.get('tipo_gasto'),
            cod_sector=data.get('cod_sector'),
            cod_producto=data.get('cod_producto'),
            validador_cuipo=data.get('validador_cuipo')
        )


class EjecucionPresupuestal(Base):
    """
    Model for Budget Execution Data (Ejecución Presupuestal)
    
    This model stores monthly budget execution information including expenses,
    payments, available budget, and accumulated totals for project tracking.
    
    Data Source: transformation_app/app_outputs/ejecucion_presupuestal_outputs/ejecucion_presupuestal.json
    Records: ~13,833 entries
    """
    
    __tablename__ = 'ejecucion_presupuestal'
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Project identifier
    bpin = Column(BigInteger, nullable=False, index=True,
                  comment="Bank of Investment Projects identifier")
    
    # Time period
    periodo_corte = Column(String(10), nullable=False, index=True,
                          comment="Period cut-off date (YYYY-MM-DD)")
    
    # Execution amounts (stored as BigInteger for large financial values)
    ejecucion = Column(BigInteger, nullable=True,
                      comment="Execution amount")
    pagos = Column(BigInteger, nullable=True,
                  comment="Payments amount")
    ppto_disponible = Column(BigInteger, nullable=True,
                           comment="Available budget")
    saldos_cdp = Column(BigInteger, nullable=True,
                       comment="CDP (Certificate of Budget Availability) balances")
    total_acumul_obligac = Column(BigInteger, nullable=True,
                                comment="Total accumulated obligations")
    total_acumulado_cdp = Column(BigInteger, nullable=True,
                               comment="Total accumulated CDP")
    total_acumulado_rpc = Column(BigInteger, nullable=True,
                               comment="Total accumulated RPC (Accounts Payable)")
    
    # Data lineage
    dataframe_origen = Column(String(100), nullable=True, index=True,
                            comment="Source dataframe name")
    archivo_origen = Column(String(200), nullable=True,
                          comment="Source file name")
    
    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False,
                       comment="Record creation timestamp")
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False,
                       comment="Record last update timestamp")
    
    def __repr__(self):
        return f"<EjecucionPresupuestal(bpin={self.bpin}, periodo='{self.periodo_corte}', ejecucion={self.ejecucion})>"
    
    @classmethod
    def from_json(cls, data: dict) -> 'EjecucionPresupuestal':
        """Create instance from JSON data."""
        return cls(
            bpin=data.get('bpin'),
            periodo_corte=data.get('periodo_corte'),
            ejecucion=data.get('ejecucion'),
            pagos=data.get('pagos'),
            ppto_disponible=data.get('ppto_disponible'),
            saldos_cdp=data.get('saldos_cdp'),
            total_acumul_obligac=data.get('total_acumul_obligac'),
            total_acumulado_cdp=data.get('total_acumulado_cdp'),
            total_acumulado_rpc=data.get('total_acumulado_rpc'),
            dataframe_origen=data.get('dataframe_origen'),
            archivo_origen=data.get('archivo_origen')
        )


class MovimientoPresupuestal(Base):
    """
    Model for Budget Movements (Movimientos Presupuestales)
    
    This model stores budget modification data including additions, reductions,
    credits, and other budget adjustments throughout the project lifecycle.
    
    Data Source: transformation_app/app_outputs/ejecucion_presupuestal_outputs/movimientos_presupuestales.json
    Records: ~14,016 entries
    """
    
    __tablename__ = 'movimiento_presupuestal'
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Project identifier
    bpin = Column(BigInteger, nullable=False, index=True,
                  comment="Bank of Investment Projects identifier")
    
    # Time period
    periodo_corte = Column(String(10), nullable=False, index=True,
                          comment="Period cut-off date (YYYY-MM-DD)")
    
    # Budget movement amounts (stored as BigInteger for large financial values)
    adiciones = Column(BigInteger, nullable=True,
                      comment="Budget additions")
    aplazamiento = Column(BigInteger, nullable=True,
                         comment="Budget postponements")
    contracreditos = Column(BigInteger, nullable=True,
                          comment="Counter-credits")
    creditos = Column(BigInteger, nullable=True,
                     comment="Budget credits")
    desaplazamiento = Column(BigInteger, nullable=True,
                           comment="Postponement reversals")
    ppto_inicial = Column(BigInteger, nullable=True,
                         comment="Initial budget")
    ppto_modificado = Column(BigInteger, nullable=True,
                           comment="Modified budget")
    reducciones = Column(BigInteger, nullable=True,
                        comment="Budget reductions")
    
    # Data lineage
    dataframe_origen = Column(String(100), nullable=True, index=True,
                            comment="Source dataframe name")
    archivo_origen = Column(String(200), nullable=True,
                          comment="Source file name")
    
    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False,
                       comment="Record creation timestamp")
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False,
                       comment="Record last update timestamp")
    
    def __repr__(self):
        return f"<MovimientoPresupuestal(bpin={self.bpin}, periodo='{self.periodo_corte}', ppto_modificado={self.ppto_modificado})>"
    
    @classmethod
    def from_json(cls, data: dict) -> 'MovimientoPresupuestal':
        """Create instance from JSON data."""
        return cls(
            bpin=data.get('bpin'),
            periodo_corte=data.get('periodo_corte'),
            adiciones=data.get('adiciones'),
            aplazamiento=data.get('aplazamiento'),
            contracreditos=data.get('contracreditos'),
            creditos=data.get('creditos'),
            desaplazamiento=data.get('desaplazamiento'),
            ppto_inicial=data.get('ppto_inicial'),
            ppto_modificado=data.get('ppto_modificado'),
            reducciones=data.get('reducciones'),
            dataframe_origen=data.get('dataframe_origen'),
            archivo_origen=data.get('archivo_origen')
        )


class ProcesoContratacionDacp(Base):
    """
    Model for DACP Contracting Processes (Procesos de Contratación DACP)
    
    This model stores data about public contracting processes from DACP 
    (Datos Abiertos Contratación Pública) including SECOP II and other platforms.
    
    Data Source: DACP - Datos Abiertos Contratación Pública
    Generated from: transformation_app/data_transformation_contracts_dacp.py
    """
    
    __tablename__ = 'proceso_contratacion_dacp'
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Platform and source information
    plataforma = Column(String(50), nullable=True, index=True,
                       comment="Platform (e.g., SECOP II)")
    nombre_centro_gestor = Column(String(500), nullable=True,
                                 comment="Managing center name")
    nombre_corto = Column(String(100), nullable=True,
                         comment="Short name of center")
    
    # Process identification
    referencia_proceso = Column(String(100), nullable=True, index=True,
                               comment="Process reference number")
    mes_publicacion_proceso = Column(String(50), nullable=True,
                                    comment="Process publication month")
    valor_proceso = Column(BigInteger, nullable=True,
                          comment="Process value")
    modalidad_contratacion = Column(String(200), nullable=True, index=True,
                                   comment="Contracting modality")
    justificacion_modalidad_contratacion = Column(Text, nullable=True,
                                                  comment="Justification for contracting modality")
    
    # Contract details
    referencia_contrato = Column(String(100), nullable=True, index=True,
                                comment="Contract reference number")
    objeto_contrato = Column(Text, nullable=True,
                            comment="Contract object/description")
    fecha_firma_contrato = Column(Date, nullable=True,
                                 comment="Contract signing date")
    mes_firma_contrato = Column(String(50), nullable=True,
                               comment="Contract signing month")
    numero_mes = Column(String(10), nullable=True,
                       comment="Month number")
    
    # Contractor information
    tipo_documento = Column(String(20), nullable=True,
                           comment="Document type (NIT, CC, etc.)")
    tipo_persona = Column(String(50), nullable=True,
                         comment="Person type (Jurídica, Natural)")
    numero_ps = Column(String(10), nullable=True,
                      comment="PS number")
    identificacion_contratista = Column(String(50), nullable=True, index=True,
                                       comment="Contractor identification")
    nombre_razon_social_contratista = Column(String(500), nullable=True,
                                            comment="Contractor name/business name")
    
    # Contract classification and details
    url_proceso = Column(Text, nullable=True,
                        comment="Process URL")
    tipo_contrato = Column(String(200), nullable=True, index=True,
                          comment="Contract type")
    destino_gasto = Column(String(100), nullable=True, index=True,
                          comment="Expense destination")
    fecha_inicio_contrato = Column(Date, nullable=True,
                                  comment="Contract start date")
    fecha_fin_contrato = Column(Date, nullable=True,
                               comment="Contract end date")
    valor_contrato = Column(BigInteger, nullable=True,
                           comment="Contract value")
    valor_contrato_ejecutado_sap = Column(BigInteger, nullable=True,
                                         comment="Contract executed value in SAP")
    duracion_contrato_dias = Column(String(20), nullable=True,
                                   comment="Contract duration in days")
    duracion_contrato_meses = Column(String(20), nullable=True,
                                    comment="Contract duration in months")
    
    # Modifications and status
    indicador_adiciones = Column(String(10), nullable=True,
                                comment="Additions indicator")
    numero_modificaciones = Column(String(10), nullable=True,
                                  comment="Number of modifications")
    tuvo_alguna_modificacion_fecha = Column(String(10), nullable=True,
                                           comment="Had date modification")
    tuvo_alguna_modificacion_valor = Column(String(10), nullable=True,
                                           comment="Had value modification")
    
    # Categories and development plan
    categoria = Column(String(200), nullable=True, index=True,
                      comment="Category")
    macrocategoria = Column(String(100), nullable=True, index=True,
                           comment="Macro category")
    dimension_plan_desarrollo = Column(String(200), nullable=True,
                                      comment="Development plan dimension")
    
    # Metadata
    fecha_transformacion = Column(String(50), nullable=True,
                                 comment="Transformation date")
    fuente_datos = Column(String(200), nullable=True,
                         comment="Data source")
    version_transformacion = Column(String(20), nullable=True,
                                   comment="Transformation version")
    total_registros = Column(Integer, nullable=True,
                            comment="Total records")
    fuente_archivo_original = Column(String(200), nullable=True,
                                    comment="Original file source")
    fuente_fecha_extraccion = Column(String(20), nullable=True,
                                    comment="Extraction date")
    fuente_registros_originales = Column(Integer, nullable=True,
                                        comment="Original records count")
    
    # Audit fields
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    @classmethod
    def from_json(cls, data):
        """Create instance from JSON data"""
        return cls(
            plataforma=data.get('plataforma'),
            nombre_centro_gestor=data.get('nombre_centro_gestor'),
            nombre_corto=data.get('nombre_corto'),
            referencia_proceso=data.get('referencia_proceso'),
            mes_publicacion_proceso=data.get('mes_publicación_proceso'),
            valor_proceso=data.get('valor_proceso'),
            modalidad_contratacion=data.get('modalidad_contratación'),
            justificacion_modalidad_contratacion=data.get('justificacion_modalidad_contratación'),
            referencia_contrato=data.get('referencia_contrato'),
            objeto_contrato=data.get('objeto_contrato'),
            fecha_firma_contrato=datetime.strptime(data.get('fecha_firma_contrato'), '%Y-%m-%d').date() if data.get('fecha_firma_contrato') else None,
            mes_firma_contrato=data.get('mes_firma_contrato'),
            numero_mes=str(data.get('número_mes', '')) if data.get('número_mes') is not None else None,
            tipo_documento=data.get('tipo_documento'),
            tipo_persona=data.get('tipo_persona'),
            numero_ps=str(data.get('numero_ps', '')) if data.get('numero_ps') is not None else None,
            identificacion_contratista=str(data.get('identificacion_contratista', '')) if data.get('identificacion_contratista') is not None else None,
            nombre_razon_social_contratista=data.get('nombre_razón_social_contratista'),
            url_proceso=data.get('url_proceso'),
            tipo_contrato=data.get('tipo_contrato'),
            destino_gasto=data.get('destino_gasto'),
            fecha_inicio_contrato=datetime.strptime(data.get('fecha_inicio_contrato'), '%Y-%m-%d').date() if data.get('fecha_inicio_contrato') else None,
            fecha_fin_contrato=datetime.strptime(data.get('fecha_fin_contrato'), '%Y-%m-%d').date() if data.get('fecha_fin_contrato') else None,
            valor_contrato=data.get('valor_contrato'),
            valor_contrato_ejecutado_sap=data.get('valor_contrato_ejecutado_sap'),
            duracion_contrato_dias=str(data.get('duración_contrato_días', '')) if data.get('duración_contrato_días') is not None else None,
            duracion_contrato_meses=str(data.get('duración_contrato_meses', '')) if data.get('duración_contrato_meses') is not None else None,
            indicador_adiciones=str(data.get('indicador_adiciones', '')) if data.get('indicador_adiciones') is not None else None,
            numero_modificaciones=str(data.get('número_modificaciones', '')) if data.get('número_modificaciones') is not None else None,
            tuvo_alguna_modificacion_fecha=data.get('tuvo_alguna_modificación_fecha'),
            tuvo_alguna_modificacion_valor=data.get('tuvo_alguna_modificación_valor'),
            categoria=data.get('categoría'),
            macrocategoria=data.get('macrocategoría'),
            dimension_plan_desarrollo=data.get('dimensión_plan_desarrollo'),
            fecha_transformacion=data.get('_fecha_transformacion'),
            fuente_datos=data.get('_fuente_datos'),
            version_transformacion=data.get('_version_transformacion'),
            total_registros=data.get('_total_registros'),
            fuente_archivo_original=data.get('_fuente_archivo_original'),
            fuente_fecha_extraccion=data.get('_fuente_fecha_extraccion'),
            fuente_registros_originales=data.get('_fuente_registros_originales')
        )


class OrdenCompraDacp(Base):
    """
    Model for DACP Purchase Orders (Órdenes de Compra DACP)
    
    This model stores data about purchase orders from DACP including TVEC platform orders.
    
    Data Source: DACP - Datos Abiertos Contratación Pública
    Generated from: transformation_app/data_transformation_contracts_dacp.py
    """
    
    __tablename__ = 'orden_compra_dacp'
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Platform and source information
    plataforma = Column(String(50), nullable=True, index=True,
                       comment="Platform (e.g., TVEC)")
    nombre_centro_gestor = Column(String(500), nullable=True,
                                 comment="Managing center name")
    nombre_corto = Column(String(100), nullable=True,
                         comment="Short name of center")
    
    # Process identification
    referencia_proceso = Column(String(100), nullable=True, index=True,
                               comment="Process reference number")
    mes_publicacion_proceso = Column(String(50), nullable=True,
                                    comment="Process publication month")
    valor_proceso = Column(BigInteger, nullable=True,
                          comment="Process value")
    modalidad_contratacion = Column(String(200), nullable=True, index=True,
                                   comment="Contracting modality")
    justificacion_modalidad_contratacion = Column(Text, nullable=True,
                                                  comment="Justification for contracting modality")
    
    # Contract details
    referencia_contrato = Column(String(100), nullable=True, index=True,
                                comment="Contract reference number")
    objeto_contrato = Column(Text, nullable=True,
                            comment="Contract object/description")
    fecha_firma_contrato = Column(Date, nullable=True,
                                 comment="Contract signing date")
    mes_firma_contrato = Column(String(50), nullable=True,
                               comment="Contract signing month")
    numero_mes = Column(String(10), nullable=True,
                       comment="Month number")
    
    # Contractor information
    tipo_documento = Column(String(20), nullable=True,
                           comment="Document type (NIT, CC, etc.)")
    tipo_persona = Column(String(50), nullable=True,
                         comment="Person type (Jurídica, Natural)")
    numero_ps = Column(String(10), nullable=True,
                      comment="PS number")
    identificacion_contratista = Column(String(50), nullable=True, index=True,
                                       comment="Contractor identification")
    nombre_razon_social_contratista = Column(String(500), nullable=True,
                                            comment="Contractor name/business name")
    
    # Contract classification and details
    url_proceso = Column(Text, nullable=True,
                        comment="Process URL")
    tipo_contrato = Column(String(200), nullable=True, index=True,
                          comment="Contract type")
    destino_gasto = Column(String(100), nullable=True, index=True,
                          comment="Expense destination")
    fecha_inicio_contrato = Column(Date, nullable=True,
                                  comment="Contract start date")
    fecha_fin_contrato = Column(Date, nullable=True,
                               comment="Contract end date")
    valor_contrato = Column(BigInteger, nullable=True,
                           comment="Contract value")
    valor_contrato_ejecutado_sap = Column(BigInteger, nullable=True,
                                         comment="Contract executed value in SAP")
    duracion_contrato_dias = Column(String(20), nullable=True,
                                   comment="Contract duration in days")
    duracion_contrato_meses = Column(String(20), nullable=True,
                                    comment="Contract duration in months")
    
    # Modifications and status
    indicador_adiciones = Column(String(10), nullable=True,
                                comment="Additions indicator")
    numero_modificaciones = Column(String(10), nullable=True,
                                  comment="Number of modifications")
    tuvo_alguna_modificacion_fecha = Column(String(10), nullable=True,
                                           comment="Had date modification")
    tuvo_alguna_modificacion_valor = Column(String(10), nullable=True,
                                           comment="Had value modification")
    
    # Categories and development plan
    categoria = Column(String(200), nullable=True, index=True,
                      comment="Category")
    macrocategoria = Column(String(100), nullable=True, index=True,
                           comment="Macro category")
    dimension_plan_desarrollo = Column(String(200), nullable=True,
                                      comment="Development plan dimension")
    
    # Metadata
    fecha_transformacion = Column(String(50), nullable=True,
                                 comment="Transformation date")
    fuente_datos = Column(String(200), nullable=True,
                         comment="Data source")
    version_transformacion = Column(String(20), nullable=True,
                                   comment="Transformation version")
    total_registros = Column(Integer, nullable=True,
                            comment="Total records")
    fuente_archivo_original = Column(String(200), nullable=True,
                                    comment="Original file source")
    fuente_fecha_extraccion = Column(String(20), nullable=True,
                                    comment="Extraction date")
    fuente_registros_originales = Column(Integer, nullable=True,
                                        comment="Original records count")
    
    # Audit fields
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    @classmethod
    def from_json(cls, data):
        """Create instance from JSON data"""
        return cls(
            plataforma=data.get('plataforma'),
            nombre_centro_gestor=data.get('nombre_centro_gestor'),
            nombre_corto=data.get('nombre_corto'),
            referencia_proceso=data.get('referencia_proceso'),
            mes_publicacion_proceso=data.get('mes_publicación_proceso'),
            valor_proceso=data.get('valor_proceso'),
            modalidad_contratacion=data.get('modalidad_contratación'),
            justificacion_modalidad_contratacion=data.get('justificacion_modalidad_contratación'),
            referencia_contrato=data.get('referencia_contrato'),
            objeto_contrato=data.get('objeto_contrato'),
            fecha_firma_contrato=datetime.strptime(data.get('fecha_firma_contrato'), '%Y-%m-%d').date() if data.get('fecha_firma_contrato') else None,
            mes_firma_contrato=data.get('mes_firma_contrato'),
            numero_mes=str(data.get('número_mes', '')) if data.get('número_mes') is not None else None,
            tipo_documento=data.get('tipo_documento'),
            tipo_persona=data.get('tipo_persona'),
            numero_ps=str(data.get('numero_ps', '')) if data.get('numero_ps') is not None else None,
            identificacion_contratista=str(data.get('identificacion_contratista', '')) if data.get('identificacion_contratista') is not None else None,
            nombre_razon_social_contratista=data.get('nombre_razón_social_contratista'),
            url_proceso=data.get('url_proceso'),
            tipo_contrato=data.get('tipo_contrato'),
            destino_gasto=data.get('destino_gasto'),
            fecha_inicio_contrato=datetime.strptime(data.get('fecha_inicio_contrato'), '%Y-%m-%d').date() if data.get('fecha_inicio_contrato') else None,
            fecha_fin_contrato=datetime.strptime(data.get('fecha_fin_contrato'), '%Y-%m-%d').date() if data.get('fecha_fin_contrato') else None,
            valor_contrato=data.get('valor_contrato'),
            valor_contrato_ejecutado_sap=data.get('valor_contrato_ejecutado_sap'),
            duracion_contrato_dias=str(data.get('duración_contrato_días', '')) if data.get('duración_contrato_días') is not None else None,
            duracion_contrato_meses=str(data.get('duración_contrato_meses', '')) if data.get('duración_contrato_meses') is not None else None,
            indicador_adiciones=str(data.get('indicador_adiciones', '')) if data.get('indicador_adiciones') is not None else None,
            numero_modificaciones=str(data.get('número_modificaciones', '')) if data.get('número_modificaciones') is not None else None,
            tuvo_alguna_modificacion_fecha=data.get('tuvo_alguna_modificación_fecha'),
            tuvo_alguna_modificacion_valor=data.get('tuvo_alguna_modificación_valor'),
            categoria=data.get('categoría'),
            macrocategoria=data.get('macrocategoría'),
            dimension_plan_desarrollo=data.get('dimensión_plan_desarrollo'),
            fecha_transformacion=data.get('_fecha_transformacion'),
            fuente_datos=data.get('_fuente_datos'),
            version_transformacion=data.get('_version_transformacion'),
            total_registros=data.get('_total_registros'),
            fuente_archivo_original=data.get('_fuente_archivo_original'),
            fuente_fecha_extraccion=data.get('_fuente_fecha_extraccion'),
            fuente_registros_originales=data.get('_fuente_registros_originales')
        )


class PaaDacp(Base):
    """
    Model for PAA DACP (Plan Anual de Adquisiciones DACP)
    
    This model stores data about annual acquisition plans from DACP.
    
    Data Source: DACP - Datos Abiertos Contratación Pública
    Generated from: transformation_app/data_transformation_paa_dacp.py
    """
    
    __tablename__ = 'paa_dacp'
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Basic information
    descripcion = Column(Text)
    vigencia = Column(String(10), nullable=True, index=True,
                     comment="Validity year")
    fecha_inicio = Column(String(50), nullable=True,
                         comment="Start date/month")
    fecha_oferta = Column(String(50), nullable=True,
                         comment="Offer date/month")
    duracion_valor = Column(String(50), nullable=True,
                           comment="Duration value")
    duracion_intervalo = Column(String(50), nullable=True,
                               comment="Duration interval")
    modalidad_contratacion = Column(String(200), nullable=True, index=True,
                                   comment="Contracting modality")
    fuente_recursos = Column(String(200), nullable=True, index=True,
                            comment="Resource source")
    
    # Project identification
    elemento_pep = Column(Text, nullable=True, index=True,
                         comment="PEP element")
    descripcion_elemento_pep = Column(Text)
    pospre = Column(String(50), nullable=True,
                   comment="POSPRE code")
    nombre_pospre = Column(String(200), nullable=True,
                          comment="POSPRE name")
    
    # Financial values
    valor_actividad = Column(BigInteger, nullable=True,
                            comment="Activity value")
    valor_disponible = Column(BigInteger, nullable=True,
                             comment="Available value")
    valor_apropiado = Column(BigInteger, nullable=True,
                            comment="Appropriated value")
    valor_total_estimado = Column(BigInteger, nullable=True,
                                 comment="Total estimated value")
    valor_vigencia_actual = Column(BigInteger, nullable=True,
                                  comment="Current validity value")
    vigencias_futuras = Column(String(10), nullable=True,
                              comment="Future validities")
    estado_vigencias = Column(String(50), nullable=True,
                             comment="Validities status")
    
    # Process details
    id_interno = Column(String(20), nullable=True, index=True,
                       comment="Internal ID")
    tipo_de_solicitud = Column(String(100), nullable=True,
                              comment="Request type")
    categoria = Column(String(100), nullable=True, index=True,
                      comment="Category")
    subcategoria = Column(String(200), nullable=True, index=True,
                         comment="Subcategory")
    id_paa = Column(String(20), nullable=True, index=True,
                   comment="PAA ID")
    estado = Column(String(50), nullable=True, index=True,
                   comment="Status")
    funcionamiento_real_estimado = Column(String(20), nullable=True,
                                         comment="Estimated real functioning")
    inversion_real_estimado = Column(BigInteger, nullable=True,
                                    comment="Estimated real investment")
    
    # Process information
    tiene_proceso = Column(String(10), nullable=True,
                          comment="Has process")
    tipo_de_plataforma = Column(String(50), nullable=True,
                               comment="Platform type")
    link_del_proceso = Column(Text)
    vencida = Column(String(10), nullable=True,
                    comment="Expired")
    justificacion_vencida = Column(Text)
    
    # Center and management
    nombre_abreviado = Column(String(100), nullable=True,
                             comment="Abbreviated name")
    fecha_de_firma_de_contrato = Column(String(50), nullable=True,
                                       comment="Contract signing date")
    fondo = Column(String(100), nullable=True, index=True,
                  comment="Fund")
    presupuesto_participativo = Column(String(100), nullable=True,
                                      comment="Participatory budget")
    rpc = Column(String(100), nullable=True,
                comment="RPC")
    delegaciones = Column(String(10), nullable=True,
                         comment="Delegations")
    llave = Column(String(100), nullable=True, index=True,
                  comment="Key")
    cdp_bloqueado = Column(String(100), nullable=True,
                          comment="Blocked CDP")
    modificaciones = Column(Text, nullable=True,
                           comment="Modifications")
    
    # Management center
    nombre_centro_gestor = Column(String(200), nullable=True, index=True,
                                 comment="Management center name")
    cod_centro_gestor = Column(Integer, nullable=True, index=True,
                              comment="Management center code")
    emprestito = Column(String(10), nullable=True,
                       comment="Loan")
    bp = Column(String(20), nullable=True, index=True,
               comment="BP code")
    bpin = Column(BigInteger, nullable=True, index=True,
                 comment="BPIN")
    
    # Processing information
    fecha_procesamiento = Column(String(30), nullable=True,
                                comment="Processing date")
    clasificacion_valor = Column(String(20), nullable=True, index=True,
                                comment="Value classification")
    anio = Column(Integer, nullable=True, index=True,
                 comment="Year")
    
    # Audit fields
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    @classmethod
    def from_json(cls, data):
        """Create instance from JSON data"""
        return cls(
            descripcion=data.get('descripcion'),
            vigencia=data.get('vigencia'),
            fecha_inicio=data.get('fecha_inicio'),
            fecha_oferta=data.get('fecha_oferta'),
            duracion_valor=str(data.get('duracion_valor', '')) if data.get('duracion_valor') is not None else None,
            duracion_intervalo=data.get('duracion_intervalo'),
            modalidad_contratacion=data.get('modalidad_contratacion'),
            fuente_recursos=data.get('fuente_recursos'),
            elemento_pep=data.get('elemento_pep'),
            descripcion_elemento_pep=data.get('descripcion_elemento_pep'),
            pospre=str(data.get('pospre', '')) if data.get('pospre') is not None else None,
            nombre_pospre=data.get('nombre_pospre'),
            valor_actividad=data.get('valor_actividad'),
            valor_disponible=data.get('valor_disponible'),
            valor_apropiado=data.get('valor_apropiado'),
            valor_total_estimado=data.get('valor_total_estimado'),
            valor_vigencia_actual=data.get('valor_vigencia_actual'),
            vigencias_futuras=data.get('vigencias_futuras'),
            estado_vigencias=data.get('estado_vigencias'),
            id_interno=str(data.get('id', '')) if data.get('id') is not None else None,
            tipo_de_solicitud=data.get('tipo_de_solicitud'),
            categoria=data.get('categoria'),
            subcategoria=data.get('subcategoria'),
            id_paa=str(data.get('id_paa', '')) if data.get('id_paa') is not None else None,
            estado=data.get('estado'),
            funcionamiento_real_estimado=data.get('funcionamiento_real_estimado'),
            inversion_real_estimado=data.get('inversión_real_estimado'),
            tiene_proceso=data.get('tiene_proceso'),
            tipo_de_plataforma=str(data.get('tipo_de_plataforma', '')) if data.get('tipo_de_plataforma') is not None else None,
            link_del_proceso=str(data.get('link_del_proceso', '')) if data.get('link_del_proceso') is not None else None,
            vencida=str(data.get('vencida', '')) if data.get('vencida') is not None else None,
            justificacion_vencida=data.get('justificación_vencida'),
            nombre_abreviado=data.get('nombre_abreviado'),
            fecha_de_firma_de_contrato=data.get('fecha_de_firma_de_contrato'),
            fondo=data.get('fondo'),
            presupuesto_participativo=str(data.get('presupuesto_participativo', '')) if data.get('presupuesto_participativo') is not None else None,
            rpc=str(data.get('rpc', '')) if data.get('rpc') is not None else None,
            delegaciones=str(data.get('delegaciones', '')) if data.get('delegaciones') is not None else None,
            llave=data.get('llave'),
            cdp_bloqueado=str(data.get('cdp_bloqueado', '')) if data.get('cdp_bloqueado') is not None else None,
            modificaciones=data.get('modificaciones'),
            nombre_centro_gestor=data.get('nombre_centro_gestor'),
            cod_centro_gestor=data.get('cod_centro_gestor'),
            emprestito=str(data.get('emprestito', '')) if data.get('emprestito') is not None else None,
            bp=data.get('bp'),
            bpin=data.get('bpin'),
            fecha_procesamiento=data.get('fecha_procesamiento'),
            clasificacion_valor=data.get('clasificacion_valor'),
            anio=data.get('anio')
        )


class EmpPaaDacp(Base):
    """
    Model for EMP PAA DACP (Empréstito Plan Anual de Adquisiciones DACP)
    
    This model stores data about loan-related annual acquisition plans from DACP.
    
    Data Source: DACP - Datos Abiertos Contratación Pública
    Generated from: transformation_app/data_transformation_paa_dacp.py
    """
    
    __tablename__ = 'emp_paa_dacp'
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Basic information
    descripcion = Column(Text)
    vigencia = Column(String(10), nullable=True, index=True,
                     comment="Validity year")
    fecha_inicio = Column(String(50), nullable=True,
                         comment="Start date/month")
    fecha_oferta = Column(String(50), nullable=True,
                         comment="Offer date/month")
    duracion_valor = Column(String(50), nullable=True,
                           comment="Duration value")
    duracion_intervalo = Column(String(50), nullable=True,
                               comment="Duration interval")
    modalidad_contratacion = Column(String(200), nullable=True, index=True,
                                   comment="Contracting modality")
    fuente_recursos = Column(String(200), nullable=True, index=True,
                            comment="Resource source")
    
    # Project identification
    elemento_pep = Column(Text, nullable=True, index=True,
                         comment="PEP element")
    descripcion_elemento_pep = Column(Text)
    pospre = Column(String(50), nullable=True,
                   comment="POSPRE code")
    nombre_pospre = Column(String(200), nullable=True,
                          comment="POSPRE name")
    
    # Financial values
    valor_actividad = Column(BigInteger, nullable=True,
                            comment="Activity value")
    valor_disponible = Column(BigInteger, nullable=True,
                             comment="Available value")
    valor_apropiado = Column(BigInteger, nullable=True,
                            comment="Appropriated value")
    valor_total_estimado = Column(BigInteger, nullable=True,
                                 comment="Total estimated value")
    valor_vigencia_actual = Column(BigInteger, nullable=True,
                                  comment="Current validity value")
    vigencias_futuras = Column(String(10), nullable=True,
                              comment="Future validities")
    estado_vigencias = Column(String(50), nullable=True,
                             comment="Validities status")
    
    # Process details
    id_interno = Column(String(20), nullable=True, index=True,
                       comment="Internal ID")
    tipo_de_solicitud = Column(String(100), nullable=True,
                              comment="Request type")
    categoria = Column(String(100), nullable=True, index=True,
                      comment="Category")
    subcategoria = Column(String(200), nullable=True, index=True,
                         comment="Subcategory")
    id_paa = Column(String(20), nullable=True, index=True,
                   comment="PAA ID")
    estado = Column(String(50), nullable=True, index=True,
                   comment="Status")
    funcionamiento_real_estimado = Column(String(20), nullable=True,
                                         comment="Estimated real functioning")
    inversion_real_estimado = Column(BigInteger, nullable=True,
                                    comment="Estimated real investment")
    
    # Process information
    tiene_proceso = Column(String(10), nullable=True,
                          comment="Has process")
    tipo_de_plataforma = Column(String(50), nullable=True,
                               comment="Platform type")
    link_del_proceso = Column(Text)
    vencida = Column(String(10), nullable=True,
                    comment="Expired")
    justificacion_vencida = Column(Text)
    
    # Center and management
    nombre_abreviado = Column(String(100), nullable=True,
                             comment="Abbreviated name")
    fecha_de_firma_de_contrato = Column(String(50), nullable=True,
                                       comment="Contract signing date")
    fondo = Column(String(100), nullable=True, index=True,
                  comment="Fund")
    presupuesto_participativo = Column(String(100), nullable=True,
                                      comment="Participatory budget")
    rpc = Column(String(100), nullable=True,
                comment="RPC")
    delegaciones = Column(String(10), nullable=True,
                         comment="Delegations")
    llave = Column(String(100), nullable=True, index=True,
                  comment="Key")
    cdp_bloqueado = Column(String(100), nullable=True,
                          comment="Blocked CDP")
    modificaciones = Column(Text, nullable=True,
                           comment="Modifications")
    
    # Management center
    nombre_centro_gestor = Column(String(200), nullable=True, index=True,
                                 comment="Management center name")
    cod_centro_gestor = Column(Integer, nullable=True, index=True,
                              comment="Management center code")
    emprestito = Column(String(10), nullable=True,
                       comment="Loan")
    bp = Column(String(20), nullable=True, index=True,
               comment="BP code")
    bpin = Column(BigInteger, nullable=True, index=True,
                 comment="BPIN")
    
    # Processing information
    fecha_procesamiento = Column(String(30), nullable=True,
                                comment="Processing date")
    clasificacion_valor = Column(String(20), nullable=True, index=True,
                                comment="Value classification")
    anio = Column(Integer, nullable=True, index=True,
                 comment="Year")
    
    # Audit fields
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    @classmethod
    def from_json(cls, data):
        """Create instance from JSON data"""
        return cls(
            descripcion=data.get('descripcion'),
            vigencia=data.get('vigencia'),
            fecha_inicio=data.get('fecha_inicio'),
            fecha_oferta=data.get('fecha_oferta'),
            duracion_valor=str(data.get('duracion_valor', '')) if data.get('duracion_valor') is not None else None,
            duracion_intervalo=data.get('duracion_intervalo'),
            modalidad_contratacion=data.get('modalidad_contratacion'),
            fuente_recursos=data.get('fuente_recursos'),
            elemento_pep=data.get('elemento_pep'),
            descripcion_elemento_pep=data.get('descripcion_elemento_pep'),
            pospre=str(data.get('pospre', '')) if data.get('pospre') is not None else None,
            nombre_pospre=data.get('nombre_pospre'),
            valor_actividad=data.get('valor_actividad'),
            valor_disponible=data.get('valor_disponible'),
            valor_apropiado=data.get('valor_apropiado'),
            valor_total_estimado=data.get('valor_total_estimado'),
            valor_vigencia_actual=data.get('valor_vigencia_actual'),
            vigencias_futuras=data.get('vigencias_futuras'),
            estado_vigencias=data.get('estado_vigencias'),
            id_interno=str(data.get('id', '')) if data.get('id') is not None else None,
            tipo_de_solicitud=data.get('tipo_de_solicitud'),
            categoria=data.get('categoria'),
            subcategoria=data.get('subcategoria'),
            id_paa=str(data.get('id_paa', '')) if data.get('id_paa') is not None else None,
            estado=data.get('estado'),
            funcionamiento_real_estimado=data.get('funcionamiento_real_estimado'),
            inversion_real_estimado=data.get('inversión_real_estimado'),
            tiene_proceso=data.get('tiene_proceso'),
            tipo_de_plataforma=str(data.get('tipo_de_plataforma', '')) if data.get('tipo_de_plataforma') is not None else None,
            link_del_proceso=str(data.get('link_del_proceso', '')) if data.get('link_del_proceso') is not None else None,
            vencida=str(data.get('vencida', '')) if data.get('vencida') is not None else None,
            justificacion_vencida=data.get('justificación_vencida'),
            nombre_abreviado=data.get('nombre_abreviado'),
            fecha_de_firma_de_contrato=data.get('fecha_de_firma_de_contrato'),
            fondo=data.get('fondo'),
            presupuesto_participativo=str(data.get('presupuesto_participativo', '')) if data.get('presupuesto_participativo') is not None else None,
            rpc=str(data.get('rpc', '')) if data.get('rpc') is not None else None,
            delegaciones=str(data.get('delegaciones', '')) if data.get('delegaciones') is not None else None,
            llave=data.get('llave'),
            cdp_bloqueado=str(data.get('cdp_bloqueado', '')) if data.get('cdp_bloqueado') is not None else None,
            modificaciones=data.get('modificaciones'),
            nombre_centro_gestor=data.get('nombre_centro_gestor'),
            cod_centro_gestor=data.get('cod_centro_gestor'),
            emprestito=str(data.get('emprestito', '')) if data.get('emprestito') is not None else None,
            bp=data.get('bp'),
            bpin=data.get('bpin'),
            fecha_procesamiento=data.get('fecha_procesamiento'),
            clasificacion_valor=data.get('clasificacion_valor'),
            anio=data.get('anio')
        )


# Export the models for easier importing
__all__ = ['Base', 'UnidadProyecto', 'DatosCaracteristicosProyecto', 'EjecucionPresupuestal', 'MovimientoPresupuestal', 
           'ProcesoContratacionDacp', 'OrdenCompraDacp', 'PaaDacp', 'EmpPaaDacp']
