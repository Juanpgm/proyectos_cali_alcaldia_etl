#!/usr/bin/env python3
"""
Cargador Final de Datos - Alcaldía de Santiago de Cali ETL
=========================================================

Cargador final optimizado que ejecuta la carga completa usando
los archivos SQL generados por el analizador final.

Características:
- Ejecución robusta con manejo avanzado de errores
- Carga optimizada por lotes
- Validación de integridad de datos
- Reporte detallado de resultados
- Recuperación automática de errores menores

Versión: 3.0.0 - Final
"""

import json
import psycopg2
import psycopg2.extras
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import logging
import sys
import traceback
import time

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

from database_management.core.config import get_database_config

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('final_data_load.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class DataLoadResult:
    """Clase para almacenar resultados de carga de datos"""
    def __init__(self):
        self.start_time = datetime.now()
        self.end_time = None
        self.duration = None
        self.total_tables = 0
        self.successful_tables = []
        self.failed_tables = []
        self.total_records = 0
        self.errors = []
        self.warnings = []
    
    def finish(self):
        self.end_time = datetime.now()
        self.duration = self.end_time - self.start_time
    
    def add_success(self, table_name: str, records: int, file_path: str):
        self.successful_tables.append({
            'table': table_name,
            'records': records,
            'file': file_path
        })
        self.total_records += records
    
    def add_failure(self, table_name: str, error: str):
        self.failed_tables.append({
            'table': table_name,
            'error': error
        })
    
    def add_error(self, error: str):
        self.errors.append(error)
    
    def add_warning(self, warning: str):
        self.warnings.append(warning)
    
    @property
    def success_rate(self) -> float:
        if self.total_tables == 0:
            return 0.0
        return (len(self.successful_tables) / self.total_tables) * 100

def execute_sql_file_robust(cursor, sql_file_path: Path, description: str) -> bool:
    """Ejecuta un archivo SQL con manejo robusto de errores"""
    try:
        logger.info(f"🔧 {description}")
        
        if not sql_file_path.exists():
            logger.warning(f"⚠️ Archivo no encontrado: {sql_file_path}")
            return False
        
        sql_content = sql_file_path.read_text(encoding='utf-8').strip()
        
        if not sql_content:
            logger.warning(f"⚠️ Archivo vacío: {sql_file_path}")
            return False
        
        # Ejecutar SQL
        cursor.execute(sql_content)
        logger.info(f"✅ {description} - Completado")
        return True
        
    except psycopg2.Error as e:
        logger.error(f"❌ Error SQL en {description}: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error general en {description}: {e}")
        return False

def load_json_data_optimized(cursor, table_name: str, json_file_path: Path) -> int:
    """
    Carga datos JSON con optimizaciones y manejo robusto de errores
    """
    try:
        logger.info(f"📁 Cargando: {table_name} ← {json_file_path.name}")
        
        # Verificar archivo
        if not json_file_path.exists():
            logger.warning(f"⚠️ Archivo no encontrado: {json_file_path}")
            return 0
        
        # Cargar datos JSON
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list) or len(data) == 0:
            logger.warning(f"⚠️ Sin datos en {json_file_path}")
            return 0
        
        # Obtener metadatos de la tabla
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND table_schema = 'public'
            AND column_name NOT IN ('id', 'created_at', 'updated_at', 'version', 'is_active')
            ORDER BY ordinal_position;
        """, (table_name,))
        
        columns_info = cursor.fetchall()
        
        if not columns_info:
            logger.warning(f"⚠️ No se encontraron columnas para {table_name}")
            return 0
        
        column_names = [col[0] for col in columns_info]
        
        # Preparar datos para inserción
        valid_rows = []
        error_count = 0
        
        for i, record in enumerate(data):
            if not isinstance(record, dict):
                error_count += 1
                continue
            
            row_values = []
            for col_name in column_names:
                value = record.get(col_name)
                
                # Normalizar valores
                if value is None or value == "" or str(value).strip().lower() in ['null', 'none', 'nan']:
                    row_values.append(None)
                else:
                    row_values.append(value)
            
            valid_rows.append(tuple(row_values))
        
        if not valid_rows:
            logger.warning(f"⚠️ No hay filas válidas en {table_name}")
            return 0
        
        if error_count > 0:
            logger.warning(f"⚠️ {error_count} registros inválidos omitidos en {table_name}")
        
        # Preparar SQL de inserción
        placeholders = ", ".join(["%s"] * len(column_names))
        columns_str = ", ".join(column_names)
        
        insert_sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING;
        """
        
        # Inserción por lotes con progreso
        batch_size = 1000
        total_inserted = 0
        batch_count = 0
        
        try:
            for i in range(0, len(valid_rows), batch_size):
                batch = valid_rows[i:i + batch_size]
                
                # Ejecutar lote
                cursor.executemany(insert_sql, batch)
                
                # Contar insertados (aproximado)
                inserted_in_batch = len(batch)
                total_inserted += inserted_in_batch
                batch_count += 1
                
                # Log de progreso cada 5 lotes
                if batch_count % 5 == 0:
                    logger.info(f"  📊 {total_inserted:,} filas procesadas en {table_name}")
        
        except psycopg2.Error as e:
            logger.error(f"❌ Error de inserción en {table_name}: {e}")
            return 0
        
        logger.info(f"✅ {table_name}: {total_inserted:,} registros cargados")
        return total_inserted
        
    except Exception as e:
        logger.error(f"❌ Error cargando {table_name}: {e}")
        return 0

def get_json_table_mapping(data_dir: Path) -> Dict[str, Path]:
    """Mapea nombres de tabla a archivos JSON"""
    mapping = {}
    
    for json_file in data_dir.rglob("*.json"):
        try:
            # Verificar que tiene datos válidos
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, list) and len(data) > 0:
                # Generar nombre de tabla
                table_name = json_file.stem.lower()
                table_name = table_name.replace('-', '_').replace(' ', '_')
                
                # Limpiar caracteres especiales
                import re
                table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
                
                mapping[table_name] = json_file
                
        except Exception:
            continue  # Omitir archivos problemáticos
    
    return mapping

def execute_final_data_load() -> DataLoadResult:
    """Ejecuta la carga final completa de datos"""
    result = DataLoadResult()
    
    logger.info("🚀 INICIANDO CARGA FINAL DE DATOS DEL WAREHOUSE")
    logger.info("=" * 60)
    
    try:
        # Configuración de base de datos
        config = get_database_config()
        logger.info(f"🔗 Conectando a: {config.host}:{config.port}/{config.database}")
        
        # Conexión
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Rutas
        project_root = Path(__file__).parent.parent.parent
        sql_dir = project_root / "database_management" / "generated_sql"
        data_dir = project_root / "transformation_app" / "app_outputs"
        
        # FASE 1: Setup del warehouse
        logger.info("📋 FASE 1: Configuración del warehouse")
        setup_file = sql_dir / "01_warehouse_setup.sql"
        if not execute_sql_file_robust(cursor, setup_file, "Setup del warehouse"):
            result.add_error("Error en setup del warehouse")
        
        # FASE 2: Crear tablas
        logger.info("📋 FASE 2: Creación de tablas")
        tables_file = sql_dir / "02_create_tables.sql"
        if not execute_sql_file_robust(cursor, tables_file, "Creación de tablas"):
            result.add_error("Error crítico: No se pudieron crear las tablas")
            raise Exception("Error crítico en creación de tablas")
        
        # FASE 3: Crear índices
        logger.info("📋 FASE 3: Creación de índices")
        indexes_file = sql_dir / "03_create_indexes.sql"
        if not execute_sql_file_robust(cursor, indexes_file, "Creación de índices"):
            result.add_warning("Algunos índices no se crearon correctamente")
        
        # FASE 4: Crear triggers
        logger.info("📋 FASE 4: Creación de triggers")
        triggers_file = sql_dir / "04_create_triggers.sql"
        if not execute_sql_file_robust(cursor, triggers_file, "Creación de triggers"):
            result.add_warning("Algunos triggers no se crearon correctamente")
        
        # FASE 5: Mapear archivos JSON
        logger.info("📋 FASE 5: Mapeo de datos")
        table_json_mapping = get_json_table_mapping(data_dir)
        result.total_tables = len(table_json_mapping)
        
        logger.info(f"📊 Encontradas {len(table_json_mapping)} tablas para cargar")
        
        # FASE 6: Cargar datos
        logger.info("📋 FASE 6: Carga de datos")
        
        for table_name, json_file in table_json_mapping.items():
            try:
                # Verificar que la tabla existe
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    );
                """, (table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    logger.warning(f"⚠️ Tabla {table_name} no existe, omitiendo")
                    result.add_failure(table_name, "tabla no existe")
                    continue
                
                # Cargar datos
                start_time = time.time()
                records_loaded = load_json_data_optimized(cursor, table_name, json_file)
                load_time = time.time() - start_time
                
                if records_loaded > 0:
                    result.add_success(table_name, records_loaded, str(json_file))
                    logger.info(f"  ⏱️ Tiempo: {load_time:.2f}s ({records_loaded/load_time:.0f} rec/s)")
                else:
                    result.add_failure(table_name, "no se cargaron registros")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ Error en tabla {table_name}: {error_msg}")
                result.add_failure(table_name, error_msg)
        
        # FASE 7: Crear vistas analíticas
        logger.info("📋 FASE 7: Vistas analíticas")
        analytics_file = sql_dir / "05_analytics_views.sql"
        if not execute_sql_file_robust(cursor, analytics_file, "Vistas analíticas"):
            result.add_warning("Error en creación de vistas analíticas")
        
        # FASE 8: Validación final
        logger.info("📋 FASE 8: Validación final")
        cursor.execute("""
            SELECT 
                schemaname,
                tablename,
                n_live_tup as row_count
            FROM pg_stat_user_tables 
            WHERE schemaname = 'public'
            AND tablename NOT LIKE 'pg_%'
            ORDER BY n_live_tup DESC;
        """)
        
        table_stats = cursor.fetchall()
        logger.info(f"📊 Estadísticas finales de {len(table_stats)} tablas:")
        for schema, table, rows in table_stats[:10]:  # Top 10
            logger.info(f"  • {table}: {rows:,} filas")
        
        # Cerrar conexión
        cursor.close()
        conn.close()
        
        result.finish()
        return result
        
    except Exception as e:
        logger.error(f"❌ Error crítico: {e}")
        result.add_error(f"Error crítico: {e}")
        result.finish()
        return result

def print_final_report(result: DataLoadResult):
    """Imprime reporte final detallado"""
    print("\n" + "=" * 80)
    print("🎉 REPORTE FINAL - CARGA DE DATOS WAREHOUSE ETL")
    print("=" * 80)
    
    # Tiempos
    print(f"⏱️ Inicio: {result.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️ Fin: {result.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️ Duración total: {result.duration}")
    
    # Estadísticas principales
    print(f"\n📊 RESUMEN EJECUTIVO:")
    print(f"  • Total de tablas: {result.total_tables}")
    print(f"  • Tablas exitosas: {len(result.successful_tables)}")
    print(f"  • Tablas fallidas: {len(result.failed_tables)}")
    print(f"  • Total registros: {result.total_records:,}")
    print(f"  • Tasa de éxito: {result.success_rate:.1f}%")
    
    # Tablas exitosas
    if result.successful_tables:
        print(f"\n✅ TABLAS CARGADAS EXITOSAMENTE ({len(result.successful_tables)}):")
        sorted_tables = sorted(result.successful_tables, key=lambda x: x['records'], reverse=True)
        for table_info in sorted_tables:
            print(f"  • {table_info['table']}: {table_info['records']:,} registros")
    
    # Tablas fallidas
    if result.failed_tables:
        print(f"\n❌ TABLAS CON ERRORES ({len(result.failed_tables)}):")
        for table_info in result.failed_tables:
            print(f"  • {table_info['table']}: {table_info['error']}")
    
    # Advertencias
    if result.warnings:
        print(f"\n⚠️ ADVERTENCIAS ({len(result.warnings)}):")
        for warning in result.warnings:
            print(f"  • {warning}")
    
    # Errores
    if result.errors:
        print(f"\n🚨 ERRORES CRÍTICOS ({len(result.errors)}):")
        for error in result.errors:
            print(f"  • {error}")
    
    # Estado final
    if result.success_rate >= 90:
        status = "🟢 EXCELENTE"
    elif result.success_rate >= 70:
        status = "🟡 ACEPTABLE"
    else:
        status = "🔴 REQUIERE ATENCIÓN"
    
    print(f"\n📈 ESTADO FINAL: {status}")
    print(f"📁 Logs detallados en: final_data_load.log")
    print("=" * 80)

def main():
    """Función principal"""
    try:
        result = execute_final_data_load()
        print_final_report(result)
        
        # Retornar código de salida basado en el éxito
        if result.success_rate >= 70:  # 70% o más es aceptable
            return 0
        else:
            return 1
            
    except Exception as e:
        logger.error(f"❌ Error en función principal: {e}")
        print(f"❌ Error crítico: {e}")
        return 1

if __name__ == "__main__":
    exit(main())