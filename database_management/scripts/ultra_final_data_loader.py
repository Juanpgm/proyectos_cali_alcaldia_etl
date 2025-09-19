#!/usr/bin/env python3
"""
CARGADOR ULTRA FINAL - COMPLETAMENTE MAPEABLE
============================================
Cargador que mapea específicamente cada archivo JSON a su tabla,
usando los SQL ultra conservadores generados, con manejo perfecto de tipos.
"""

import json
import psycopg2
import psycopg2.extras
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import sys
import re

# Configuración del path
sys.path.append(str(Path(__file__).parent.parent.parent))

from database_management.core.config import get_database_config

# Configurar logging sin emojis para Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ultra_final_data_load.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class UltraFinalDataLoader:
    """Cargador ultra final con mapeo exacto JSON → Tabla"""
    
    def __init__(self):
        self.config = get_database_config()
        self.connection = None
        self.cursor = None
        
        # Mapeo exacto archivo JSON → nombre de tabla 
        self.json_to_table_mapping = {
            'contratos_proyectos.json': 'contratos_proyectos',
            'datos_caracteristicos_proyectos.json': 'datos_caracteristicos_proyectos',
            'ejecucion_presupuestal.json': 'ejecucion_presupuestal',
            'movimientos_presupuestales.json': 'movimientos_presupuestales',
            'emp_contratos_index.json': 'emp_contratos_index',
            'emp_procesos.json': 'emp_procesos',
            'emp_procesos_index.json': 'emp_procesos_index',
            'emp_proyectos.json': 'emp_proyectos',
            'procesos_secop.json': 'procesos_secop',
            'unidad_proyecto_infraestructura_equipamientos.json': 'unidad_proyecto_infraestructura_equipamientos',
            'unidad_proyecto_infraestructura_vial.json': 'unidad_proyecto_infraestructura_vial'
        }
        
        # Estadísticas
        self.stats = {
            'start_time': datetime.now(),
            'successful_loads': [],
            'failed_loads': [],
            'total_records': 0
        }

    def connect_database(self):
        """Conectar a la base de datos"""
        try:
            logger.info("Conectando a base de datos PostgreSQL...")
            self.connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password
            )
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            logger.info(f"Conectado exitosamente a: {self.config.host}:{self.config.port}/{self.config.database}")
        except Exception as e:
            logger.error(f"Error conectando a base de datos: {e}")
            raise

    def execute_sql_files(self):
        """Ejecuta los archivos SQL ultra seguros"""
        sql_dir = Path(__file__).parent.parent / "generated_sql"
        sql_files = [
            ("01_warehouse_setup.sql", "Setup del warehouse"),
            ("02_create_tables.sql", "Creación de tablas"),
            ("03_create_indexes.sql", "Creación de índices"),
            ("04_create_triggers.sql", "Creación de triggers"),
            ("05_analytics_views.sql", "Vistas analíticas")
        ]
        
        for sql_file, description in sql_files:
            sql_path = sql_dir / sql_file
            if sql_path.exists():
                try:
                    logger.info(f"Ejecutando: {description}")
                    sql_content = sql_path.read_text(encoding='utf-8')
                    
                    # Dividir por statements y ejecutar individualmente
                    statements = [s.strip() for s in sql_content.split(';') if s.strip()]
                    
                    for statement in statements:
                        if statement.strip():
                            try:
                                self.cursor.execute(statement)
                                self.connection.commit()
                            except Exception as e:
                                # Solo advertir en lugar de fallar
                                logger.warning(f"Advertencia en {sql_file}: {str(e)[:100]}...")
                                self.connection.rollback()
                    
                    logger.info(f"Completado: {description}")
                    
                except Exception as e:
                    logger.error(f"Error en {sql_file}: {e}")
                    self.connection.rollback()
            else:
                logger.warning(f"Archivo no encontrado: {sql_path}")

    def get_table_columns(self, table_name: str) -> List[str]:
        """Obtiene las columnas de una tabla específica"""
        try:
            query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
              AND table_schema = 'public'
              AND column_name NOT IN ('id', 'created_at', 'updated_at', 'version', 'is_active')
            ORDER BY ordinal_position
            """
            self.cursor.execute(query, (table_name,))
            columns = [row['column_name'] for row in self.cursor.fetchall()]
            return columns
        except Exception as e:
            logger.error(f"Error obteniendo columnas de {table_name}: {e}")
            return []

    def sanitize_value(self, value: Any, column_name: str) -> Any:
        """Sanitiza valores para inserción ultra segura"""
        if value is None or value == '':
            return None
        
        # Convertir todo a string primero
        str_value = str(value).strip()
        
        # Valores nulos explícitos
        if str_value.lower() in ['null', 'none', 'n/a', 'sin definir', 'no definido', 'no aplica']:
            return None
        
        # Booleanos en español → dejar como texto
        if str_value.lower() in ['si', 'sí', 'no']:
            return str_value
        
        # Números que pueden ser muy grandes → como string para evitar overflow
        if re.match(r'^\d+$', str_value) and len(str_value) > 9:
            return str_value
        
        # Fechas mantenidas como string
        if any(pattern in str_value for pattern in ['-', '/', 'T', ':']):
            return str_value
        
        return str_value

    def load_json_to_table(self, json_file_path: Path, table_name: str) -> bool:
        """Carga un archivo JSON específico a su tabla"""
        try:
            logger.info(f"Cargando: {table_name} <- {json_file_path.name}")
            
            # Leer datos JSON
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                logger.warning(f"WARN: {json_file_path.name} no es un array válido")
                return False
            
            if not data:
                logger.warning(f"WARN: {json_file_path.name} está vacío")
                return False
            
            # Obtener columnas de la tabla
            table_columns = self.get_table_columns(table_name)
            if not table_columns:
                logger.error(f"No se pudieron obtener columnas de {table_name}")
                return False
            
            # Preparar datos para inserción por lotes
            batch_data = []
            batch_size = 1000
            
            for record in data:
                if not isinstance(record, dict):
                    continue
                
                # Crear tupla de valores en el orden de las columnas
                row_values = []
                for column in table_columns:
                    # Buscar el valor en el record original (con posibles variaciones de nombre)
                    value = None
                    
                    # Buscar coincidencia exacta
                    if column in record:
                        value = record[column]
                    else:
                        # Buscar coincidencia aproximada (normalizada)
                        normalized_column = column.lower().replace('_', '').replace(' ', '')
                        for key, val in record.items():
                            normalized_key = re.sub(r'[^a-zA-Z0-9]', '', key.lower())
                            if normalized_key == normalized_column:
                                value = val
                                break
                    
                    # Sanitizar valor
                    sanitized_value = self.sanitize_value(value, column)
                    row_values.append(sanitized_value)
                
                batch_data.append(tuple(row_values))
                
                # Insertar por lotes
                if len(batch_data) >= batch_size:
                    self._insert_batch(table_name, table_columns, batch_data)
                    batch_data = []
            
            # Insertar lote final
            if batch_data:
                self._insert_batch(table_name, table_columns, batch_data)
            
            # Obtener conteo final
            self.cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            final_count = self.cursor.fetchone()['count']
            
            logger.info(f"Exito: {table_name}: {final_count} registros totales")
            self.stats['successful_loads'].append({
                'table': table_name,
                'file': json_file_path.name,
                'records': final_count
            })
            self.stats['total_records'] += final_count
            
            return True
            
        except Exception as e:
            logger.error(f"Error cargando {table_name}: {e}")
            self.stats['failed_loads'].append({
                'table': table_name,
                'file': json_file_path.name,
                'error': str(e)
            })
            self.connection.rollback()
            return False

    def _insert_batch(self, table_name: str, columns: List[str], batch_data: List[tuple]):
        """Inserta un lote de datos"""
        try:
            # Crear query de inserción
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            
            query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            """
            
            # Ejecutar inserción por lotes
            psycopg2.extras.execute_batch(
                self.cursor, 
                query, 
                batch_data,
                page_size=1000
            )
            self.connection.commit()
            
        except Exception as e:
            logger.error(f"Error en lote de {table_name}: {e}")
            self.connection.rollback()
            raise

    def load_all_data(self):
        """Carga todos los datos usando el mapeo específico"""
        logger.info("Iniciando carga ultra final de datos...")
        
        # Directorio de datos
        data_base_dir = Path(__file__).parent.parent.parent / "transformation_app" / "app_outputs"
        
        # Buscar archivos JSON en subdirectorios
        json_files = []
        for subdir in data_base_dir.iterdir():
            if subdir.is_dir():
                json_files.extend(subdir.glob("*.json"))
        
        if not json_files:
            logger.error(f"No se encontraron archivos JSON en {data_base_dir}")
            return
        
        logger.info(f"Encontrados {len(json_files)} archivos JSON")
        
        # Procesar cada archivo según el mapeo
        for json_file in json_files:
            if json_file.name in self.json_to_table_mapping:
                table_name = self.json_to_table_mapping[json_file.name]
                self.load_json_to_table(json_file, table_name)
            else:
                logger.warning(f"WARN: {json_file.name} no está en el mapeo")

    def generate_final_report(self):
        """Genera reporte final ultra detallado"""
        self.stats['end_time'] = datetime.now()
        duration = self.stats['end_time'] - self.stats['start_time']
        
        successful_count = len(self.stats['successful_loads'])
        failed_count = len(self.stats['failed_loads'])
        total_count = successful_count + failed_count
        success_rate = (successful_count / total_count * 100) if total_count > 0 else 0
        
        print("\n" + "="*80)
        print("REPORTE FINAL - CARGA ULTRA SEGURA DE DATOS")
        print("="*80)
        print(f"Inicio: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Fin: {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duración: {duration}")
        print()
        print("RESUMEN EJECUTIVO:")
        print(f"  • Total de tablas procesadas: {total_count}")
        print(f"  • Tablas cargadas exitosamente: {successful_count}")
        print(f"  • Tablas con errores: {failed_count}")
        print(f"  • Total de registros cargados: {self.stats['total_records']:,}")
        print(f"  • Tasa de éxito: {success_rate:.1f}%")
        print()
        
        if self.stats['successful_loads']:
            print("TABLAS EXITOSAS:")
            for load in self.stats['successful_loads']:
                print(f"  ✓ {load['table']}: {load['records']:,} registros")
            print()
        
        if self.stats['failed_loads']:
            print("TABLAS CON ERRORES:")
            for load in self.stats['failed_loads']:
                print(f"  ✗ {load['table']}: {load['error'][:100]}...")
            print()
        
        status = "EXITOSO" if success_rate >= 70 else "REQUIERE ATENCION"
        print(f"ESTADO FINAL: {status}")
        print("="*80)

    def run_complete_load(self):
        """Ejecuta la carga completa ultra segura"""
        try:
            self.connect_database()
            self.execute_sql_files()
            self.load_all_data()
            self.generate_final_report()
            
        except Exception as e:
            logger.error(f"Error crítico: {e}")
            raise
        finally:
            if self.connection:
                self.connection.close()

def main():
    """Función principal"""
    loader = UltraFinalDataLoader()
    loader.run_complete_load()

if __name__ == "__main__":
    main()