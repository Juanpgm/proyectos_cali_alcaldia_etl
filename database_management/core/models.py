"""
Modelos SQLAlchemy para el sistema ETL de la Alcaldía de Cali.

Este módulo contiene todas las definiciones de modelos de datos para las tablas
de la base de datos PostgreSQL.
"""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import Column, String, Integer, BigInteger, Numeric, Text, Boolean, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

# Base para todos los modelos
Base = declarative_base()


class BaseModel:
    """Clase base para todos los modelos con campos comunes."""
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class CaracteristicasProyectos(BaseModel, Base):
    """Modelo para datos característicos de proyectos de ejecución presupuestal"""
    __tablename__ = 'caracteristicas_proyectos'
    
    # Identificadores de proyecto
    bpin = Column(Integer, nullable=False, index=True)
    bp = Column(String(15), nullable=False)
    
    # Información del proyecto
    nombre_proyecto = Column(Text, nullable=False)
    nombre_actividad = Column(Text, nullable=False)
    programa_presupuestal = Column(String(20), nullable=False, index=True)
    
    # Entidades responsables
    nombre_centro_gestor = Column(String(100), nullable=False, index=True)
    nombre_area_funcional = Column(Text, nullable=False)
    
    # Información financiera
    nombre_fondo = Column(String(150), nullable=False)
    clasificacion_fondo = Column(String(50), nullable=False)
    nombre_pospre = Column(String(200), nullable=False)
    
    # Estrategia y planificación (pueden ser null)
    nombre_dimension = Column(String(50), nullable=True)
    nombre_linea_estrategica = Column(String(60), nullable=True)
    nombre_programa = Column(String(100), nullable=False)
    
    # Localización
    comuna = Column(String(25), nullable=False, index=True)
    origen = Column(String(25), nullable=False)
    
    # Temporal
    anio = Column(Integer, nullable=False, index=True)
    
    # Clasificación
    tipo_gasto = Column(String(20), nullable=False, index=True)
    
    # Códigos adicionales (pueden ser null)
    cod_sector = Column(Integer, nullable=True)
    cod_producto = Column(Integer, nullable=True)
    validador_cuipo = Column(String(50), nullable=True)
    
    # Metadatos de carga
    fecha_carga = Column(DateTime, default=datetime.utcnow, nullable=False)
    fecha_actualizacion = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Índices compuestos para consultas frecuentes
    __table_args__ = (
        Index('idx_caracteristicas_bpin_anio', 'bpin', 'anio'),
        Index('idx_caracteristicas_programa_anio', 'programa_presupuestal', 'anio'),
        Index('idx_caracteristicas_centro_gestor_anio', 'nombre_centro_gestor', 'anio'),
        Index('idx_caracteristicas_comuna_anio', 'comuna', 'anio'),
        Index('idx_caracteristicas_tipo_gasto_anio', 'tipo_gasto', 'anio'),
        Index('idx_caracteristicas_clasificacion_fondo', 'clasificacion_fondo'),
        Index('idx_caracteristicas_fecha_carga', 'fecha_carga'),
    )
    
    def __repr__(self):
        return f"<CaracteristicasProyectos(bpin={self.bpin}, bp='{self.bp}', nombre_proyecto='{self.nombre_proyecto[:50]}...')>"
        
    def to_dict(self):
        """Convertir a diccionario para serialización"""
        return {
            'id': str(self.id),
            'bpin': self.bpin,
            'bp': self.bp,
            'nombre_proyecto': self.nombre_proyecto,
            'nombre_actividad': self.nombre_actividad,
            'programa_presupuestal': self.programa_presupuestal,
            'nombre_centro_gestor': self.nombre_centro_gestor,
            'nombre_area_funcional': self.nombre_area_funcional,
            'nombre_fondo': self.nombre_fondo,
            'clasificacion_fondo': self.clasificacion_fondo,
            'nombre_pospre': self.nombre_pospre,
            'nombre_dimension': self.nombre_dimension,
            'nombre_linea_estrategica': self.nombre_linea_estrategica,
            'nombre_programa': self.nombre_programa,
            'comuna': self.comuna,
            'origen': self.origen,
            'anio': self.anio,
            'tipo_gasto': self.tipo_gasto,
            'cod_sector': self.cod_sector,
            'cod_producto': self.cod_producto,
            'validador_cuipo': self.validador_cuipo,
            'fecha_carga': self.fecha_carga.isoformat() if self.fecha_carga else None,
            'fecha_actualizacion': self.fecha_actualizacion.isoformat() if self.fecha_actualizacion else None
        }