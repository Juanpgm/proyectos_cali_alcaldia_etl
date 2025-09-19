"""
Cargador de datos para características de proyectos.

Este módulo procesa el archivo JSON datos_caracteristicos_proyectos.json
y carga los datos en la tabla caracteristicas_proyectos usando SQLAlchemy.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from database_management.core.models import Base, CaracteristicasProyectos
from database_management.core.config import DatabaseConfig

logger = logging.getLogger(__name__)


class CaracteristicasProyectosLoader:
    """Cargador especializado para datos de características de proyectos."""
    
    def __init__(self, database_config: Optional[DatabaseConfig] = None):
        """
        Inicializar el cargador.
        
        Args:
            database_config: Configuración de la base de datos. Si es None, usa la configuración por defecto.
        """
        self.config = database_config or DatabaseConfig()
        self.engine = None
        self.session_maker = None
        self.stats = {
            'total_records': 0,
            'successful_inserts': 0,
            'failed_inserts': 0,
            'skipped_records': 0,
            'start_time': None,
            'end_time': None
        }
        
    def setup_database_connection(self) -> bool:
        """
        Configurar la conexión a la base de datos.
        
        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario.
        """
        try:
            connection_string = self.config.connection_string
            self.engine = create_engine(
                connection_string,
                echo=False,  # Cambiar a True para debugging SQL
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Crear las tablas si no existen
            Base.metadata.create_all(self.engine)
            
            self.session_maker = sessionmaker(bind=self.engine)
            
            # Verificar conexión
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            logger.info("✅ Conexión a la base de datos establecida correctamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error al conectar con la base de datos: {e}")
            return False
    
    def load_json_data(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Cargar datos desde el archivo JSON.
        
        Args:
            file_path: Ruta al archivo JSON.
            
        Returns:
            List[Dict]: Lista de registros JSON.
            
        Raises:
            FileNotFoundError: Si el archivo no existe.
            json.JSONDecodeError: Si el archivo no es JSON válido.
        """
        try:
            if not file_path.exists():
                raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
                
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
            logger.info(f"✅ Cargados {len(data)} registros desde {file_path}")
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error al decodificar JSON: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Error al cargar archivo: {e}")
            raise
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """
        Validar un registro antes de insertarlo.
        
        Args:
            record: Registro a validar.
            
        Returns:
            bool: True si el registro es válido.
        """
        required_fields = ['bpin', 'bp', 'nombre_proyecto', 'nombre_actividad', 
                          'programa_presupuestal', 'nombre_centro_gestor', 
                          'nombre_area_funcional', 'anio']
        
        for field in required_fields:
            if field not in record or record[field] is None:
                logger.warning(f"⚠️ Registro inválido: falta campo requerido '{field}'")
                return False
                
        # Validar tipos específicos
        try:
            if not isinstance(record['bpin'], int):
                logger.warning(f"⚠️ BPIN debe ser entero: {record.get('bpin')}")
                return False
                
            if not isinstance(record['anio'], int):
                logger.warning(f"⚠️ Año debe ser entero: {record.get('anio')}")
                return False
                
        except (ValueError, TypeError) as e:
            logger.warning(f"⚠️ Error de validación de tipos: {e}")
            return False
            
        return True
    
    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transformar un registro JSON al formato del modelo.
        
        Args:
            record: Registro JSON original.
            
        Returns:
            Dict: Registro transformado para el modelo.
        """
        # Mapeamos los campos del JSON a los campos del modelo
        transformed = {
            'bpin': record['bpin'],
            'bp': str(record['bp'])[:15],  # Limitar longitud
            'nombre_proyecto': record['nombre_proyecto'],
            'nombre_actividad': record['nombre_actividad'],
            'programa_presupuestal': str(record['programa_presupuestal'])[:20],
            'nombre_centro_gestor': str(record['nombre_centro_gestor'])[:100],
            'nombre_area_funcional': record['nombre_area_funcional'],
            'nombre_fondo': str(record['nombre_fondo'])[:150],
            'clasificacion_fondo': str(record['clasificacion_fondo'])[:50],
            'nombre_pospre': str(record['nombre_pospre'])[:200],
            'nombre_dimension': record.get('nombre_dimension'),
            'nombre_linea_estrategica': record.get('nombre_linea_estrategica'),
            'nombre_programa': str(record['nombre_programa'])[:100],
            'comuna': str(record['comuna'])[:25],
            'origen': str(record['origen'])[:25],
            'anio': record['anio'],
            'tipo_gasto': str(record['tipo_gasto'])[:20],
            'cod_sector': record.get('cod_sector'),
            'cod_producto': record.get('cod_producto'),
            'validador_cuipo': record.get('validador_cuipo'),
            'fecha_carga': datetime.utcnow(),
            'fecha_actualizacion': datetime.utcnow()
        }
        
        # Limpiar campos None en strings opcionales
        for key in ['nombre_dimension', 'nombre_linea_estrategica', 'validador_cuipo']:
            if transformed[key] is not None:
                transformed[key] = str(transformed[key])[:50] if key != 'validador_cuipo' else str(transformed[key])[:50]
        
        return transformed
    
    def batch_insert(self, session: Session, records: List[Dict[str, Any]], batch_size: int = 1000) -> None:
        """
        Insertar registros en lotes para mejor rendimiento.
        
        Args:
            session: Sesión de SQLAlchemy.
            records: Lista de registros a insertar.
            batch_size: Tamaño del lote.
        """
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                # Crear objetos del modelo
                model_objects = [CaracteristicasProyectos(**record) for record in batch]
                
                # Insertar lote
                session.bulk_save_objects(model_objects)
                session.commit()
                
                self.stats['successful_inserts'] += len(batch)
                logger.info(f"✅ Lote {batch_num}/{total_batches} insertado exitosamente ({len(batch)} registros)")
                
            except IntegrityError as e:
                session.rollback()
                logger.error(f"❌ Error de integridad en lote {batch_num}: {e}")
                
                # Intentar insertar uno por uno para identificar el problema
                self._insert_individual_records(session, batch)
                
            except Exception as e:
                session.rollback()
                logger.error(f"❌ Error al insertar lote {batch_num}: {e}")
                self.stats['failed_inserts'] += len(batch)
    
    def _insert_individual_records(self, session: Session, records: List[Dict[str, Any]]) -> None:
        """
        Insertar registros individualmente cuando falla un lote.
        
        Args:
            session: Sesión de SQLAlchemy.
            records: Lista de registros a insertar.
        """
        for record in records:
            try:
                model_object = CaracteristicasProyectos(**record)
                session.add(model_object)
                session.commit()
                self.stats['successful_inserts'] += 1
                
            except IntegrityError as e:
                session.rollback()
                logger.warning(f"⚠️ Registro duplicado o con violación de integridad: BPIN {record.get('bpin')}")
                self.stats['skipped_records'] += 1
                
            except Exception as e:
                session.rollback()
                logger.error(f"❌ Error al insertar registro individual: {e}")
                self.stats['failed_inserts'] += 1
    
    def clear_existing_data(self, session: Session, confirm: bool = False) -> bool:
        """
        Limpiar datos existentes en la tabla.
        
        Args:
            session: Sesión de SQLAlchemy.
            confirm: Confirmación explícita para eliminar datos.
            
        Returns:
            bool: True si se eliminaron los datos.
        """
        if not confirm:
            logger.warning("⚠️ Operación de limpieza cancelada: falta confirmación")
            return False
            
        try:
            count = session.query(CaracteristicasProyectos).count()
            session.query(CaracteristicasProyectos).delete()
            session.commit()
            
            logger.info(f"✅ Eliminados {count} registros existentes")
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Error al limpiar datos existentes: {e}")
            return False
    
    def load_data(self, file_path: Path, clear_existing: bool = False, batch_size: int = 1000) -> Dict[str, Any]:
        """
        Ejecutar el proceso completo de carga de datos.
        
        Args:
            file_path: Ruta al archivo JSON.
            clear_existing: Si limpiar datos existentes antes de cargar.
            batch_size: Tamaño de lote para inserción.
            
        Returns:
            Dict: Estadísticas del proceso de carga.
        """
        self.stats['start_time'] = datetime.utcnow()
        logger.info(f"🚀 Iniciando carga de datos desde {file_path}")
        
        try:
            # Configurar conexión
            if not self.setup_database_connection():
                raise Exception("No se pudo establecer conexión con la base de datos")
            
            # Cargar datos JSON
            json_data = self.load_json_data(file_path)
            self.stats['total_records'] = len(json_data)
            
            # Crear sesión
            with self.session_maker() as session:
                
                # Limpiar datos existentes si se solicita
                if clear_existing:
                    self.clear_existing_data(session, confirm=True)
                
                # Validar y transformar registros
                valid_records = []
                for record in json_data:
                    if self.validate_record(record):
                        transformed = self.transform_record(record)
                        valid_records.append(transformed)
                    else:
                        self.stats['skipped_records'] += 1
                
                logger.info(f"📊 Procesando {len(valid_records)} registros válidos de {len(json_data)} totales")
                
                # Insertar datos en lotes
                if valid_records:
                    self.batch_insert(session, valid_records, batch_size)
                
        except Exception as e:
            logger.error(f"❌ Error durante la carga de datos: {e}")
            raise
        
        finally:
            self.stats['end_time'] = datetime.utcnow()
            duration = self.stats['end_time'] - self.stats['start_time']
            
            logger.info(f"✅ Proceso de carga completado en {duration}")
            logger.info(f"📊 Estadísticas finales: {self.stats}")
            
        return self.stats
    
    def get_load_summary(self) -> str:
        """
        Generar un resumen del proceso de carga.
        
        Returns:
            str: Resumen formateado.
        """
        if not self.stats['start_time']:
            return "No se ha ejecutado ningún proceso de carga."
        
        duration = self.stats['end_time'] - self.stats['start_time']
        success_rate = (self.stats['successful_inserts'] / self.stats['total_records'] * 100) if self.stats['total_records'] > 0 else 0
        
        return f"""
🔄 RESUMEN DE CARGA - CARACTERÍSTICAS DE PROYECTOS
================================================================
📅 Inicio: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}
📅 Fin: {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}
⏱️ Duración: {duration}

📊 ESTADÍSTICAS:
- Total de registros procesados: {self.stats['total_records']:,}
- Inserciones exitosas: {self.stats['successful_inserts']:,}
- Registros fallidos: {self.stats['failed_inserts']:,}
- Registros omitidos: {self.stats['skipped_records']:,}
- Tasa de éxito: {success_rate:.2f}%

✅ Proceso completado {'exitosamente' if self.stats['failed_inserts'] == 0 else 'con errores'}
================================================================
"""


def main():
    """Función principal para ejecución directa del script."""
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Ruta del archivo JSON
    json_file = Path("transformation_app/app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json")
    
    if not json_file.exists():
        logger.error(f"❌ Archivo no encontrado: {json_file}")
        return
    
    # Crear y ejecutar el cargador
    loader = CaracteristicasProyectosLoader()
    
    try:
        stats = loader.load_data(
            file_path=json_file,
            clear_existing=False,  # Cambiar a True para limpiar datos existentes
            batch_size=500
        )
        
        print(loader.get_load_summary())
        
    except Exception as e:
        logger.error(f"❌ Error durante la ejecución: {e}")
        raise


if __name__ == "__main__":
    main()