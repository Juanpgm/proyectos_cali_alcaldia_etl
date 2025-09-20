# -*- coding: utf-8 -*-
"""
Database models for ETL pipeline - Cali Municipality Projects
This module contains SQLAlchemy models for storing project data extracted from various sources.
"""

from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Date, Text, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime
from typing import Optional
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
                    fecha_inicio = datetime.strptime(date_str, '%Y-%m-%d').date()
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
                    fecha_fin = datetime.strptime(date_str, '%Y-%m-%d').date()
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


# Export the models for easier importing
__all__ = ['Base', 'UnidadProyecto']
