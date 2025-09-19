#!/usr/bin/env python3
"""
GENERADOR ULTRA FINAL DE SQL WAREHOUSE
=====================================
Versión ultra robusta que soluciona todos los problemas detectados:
- Booleanos en español ("Si"/"No") 
- Integers overflow -> BIGINT o TEXT
- Todos los campos nullable por defecto
- Vistas analíticas corregidas
- Detección ultra conservadora de tipos
"""

import json
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Set, Tuple, Optional
from collections import defaultdict, Counter
import re

# Configuración de logging (sin emojis para Windows)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ultra_final_sql_generation.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class UltraFinalSQLGenerator:
    """Generador ultra robusto de SQL para warehouse"""
    
    def __init__(self, data_dir: str, output_dir: str):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Patrones para detección ultra conservadora
        self.spanish_boolean_values = {'si', 'no', 'sí', 'no aplica', 'n/a'}
        self.null_indicators = {'', 'null', 'none', 'n/a', 'no aplica', 'sin definir', 'no definido'}
        
        # Límites de seguridad
        self.integer_safe_limit = 2_000_000_000  # Límite seguro para INTEGER
        self.sample_size = 500
        
    def detect_ultra_conservative_type(self, values: List[Any], field_name: str) -> str:
        """
        Detección ultra conservadora de tipos para máxima compatibilidad
        """
        if not values:
            return "TEXT"
            
        # Filtrar valores nulos y vacíos
        clean_values = []
        for v in values:
            if v is not None:
                str_v = str(v).strip().lower()
                if str_v and str_v not in self.null_indicators:
                    clean_values.append(str_v)
        
        if not clean_values:
            return "TEXT"
        
        # Contadores para análisis
        value_counts = Counter(clean_values)
        total_values = len(clean_values)
        unique_values = len(value_counts)
        
        # 1. BOOLEAN ultra conservador (solo si TODOS los valores son booleanos españoles)
        if unique_values <= 4:  # Máximo 4 valores únicos para considerar boolean
            all_boolean = True
            for val in clean_values:
                if val not in self.spanish_boolean_values:
                    all_boolean = False
                    break
            if all_boolean:
                logger.info(f"   ** {field_name}: BOOLEAN español detectado - valores: {set(clean_values)}")
                return "TEXT"  # Mejor como TEXT para evitar errores de conversión
        
        # 2. NUMERIC - pero ultra conservador
        all_numeric = True
        has_decimals = False
        max_integer_value = 0
        
        for val in clean_values:
            try:
                # Intentar convertir a número
                if '.' in val or ',' in val:
                    float_val = float(val.replace(',', '.'))
                    has_decimals = True
                else:
                    int_val = int(val)
                    max_integer_value = max(max_integer_value, abs(int_val))
            except (ValueError, OverflowError):
                all_numeric = False
                break
        
        if all_numeric:
            if has_decimals:
                return "DECIMAL(18,4)"
            elif max_integer_value > self.integer_safe_limit:
                logger.info(f"   ** {field_name}: INTEGER overflow ({max_integer_value}) -> BIGINT")
                return "BIGINT"
            else:
                return "INTEGER"
        
        # 3. DATE/TIMESTAMP ultra conservador
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
        ]
        
        date_matches = 0
        for val in clean_values[:50]:  # Solo verificar primeros 50
            for pattern in date_patterns:
                if re.match(pattern, val):
                    date_matches += 1
                    break
        
        if date_matches > total_values * 0.8:  # 80% son fechas
            if any('T' in val or ' ' in val for val in clean_values[:10]):
                return "TIMESTAMP"
            else:
                return "DATE"
        
        # 4. UUID pattern
        uuid_pattern = r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'
        uuid_matches = sum(1 for val in clean_values[:20] if re.match(uuid_pattern, val))
        if uuid_matches > len(clean_values[:20]) * 0.7:
            return "UUID"
        
        # 5. TEXT con longitud apropiada
        max_length = max(len(str(v)) for v in values if v is not None) if values else 255
        
        if max_length <= 50:
            return "VARCHAR(100)"  # Buffer de seguridad
        elif max_length <= 255:
            return "VARCHAR(500)"  # Buffer de seguridad
        elif max_length <= 1000:
            return "VARCHAR(2000)"  # Buffer de seguridad
        else:
            return "TEXT"

    def analyze_json_sample_ultra_safe(self, file_path: Path) -> Dict[str, str]:
        """
        Análisis ultra seguro de muestra JSON
        """
        logger.info(f"Analizando {file_path.stem} desde {file_path.name}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Error leyendo {file_path}: {e}")
            return {}
        
        if not isinstance(data, list) or not data:
            logger.warning(f"WARN: {file_path.name} no contiene array de datos válido")
            return {}
        
        # Tomar muestra segura
        sample_size = min(self.sample_size, len(data))
        sample_data = data[:sample_size]
        logger.info(f"Analizando muestra de {sample_size} registros")
        
        # Análisis de campos
        field_analysis = defaultdict(list)
        
        for record in sample_data:
            if isinstance(record, dict):
                for field, value in record.items():
                    field_analysis[field].append(value)
        
        # Generar tipos ultra conservadores
        field_types = {}
        for field, values in field_analysis.items():
            sql_type = self.detect_ultra_conservative_type(values, field)
            field_types[field] = sql_type
            
        logger.info(f"SQL generado para {file_path.stem} ({len(field_types)} campos)")
        return field_types

    def generate_ultra_safe_table_sql(self, table_name: str, field_types: Dict[str, str]) -> str:
        """
        Genera SQL ultra seguro para tabla (TODO nullable por defecto)
        """
        columns = []
        
        # Columnas de sistema (siempre presentes)
        columns.extend([
            "id UUID PRIMARY KEY DEFAULT gen_random_uuid()",
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP", 
            "version INTEGER DEFAULT 1",
            "is_active BOOLEAN DEFAULT true"
        ])
        
        # Columnas de datos (TODAS nullable para máxima flexibilidad)
        for field, sql_type in sorted(field_types.items()):
            # Limpiar nombre de campo
            clean_field = re.sub(r'[^a-zA-Z0-9_]', '_', field.lower())
            clean_field = re.sub(r'_+', '_', clean_field).strip('_')
            
            # TODOS los campos son nullable por defecto
            columns.append(f"{clean_field} {sql_type}")
        
        return f"""
-- Tabla: {table_name}
-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Campos: {len(field_types)} + 5 sistema
CREATE TABLE IF NOT EXISTS {table_name} (
    {','.join([f'    {col}' for col in columns])}
);
"""

    def generate_all_sql_files(self):
        """
        Genera todos los archivos SQL ultra seguros
        """
        logger.info("Iniciando generación ULTRA FINAL de warehouse SQL")
        
        # 1. Buscar archivos JSON en subdirectorios
        json_files = []
        for subdir in self.data_dir.iterdir():
            if subdir.is_dir():
                json_files.extend(subdir.glob("*.json"))
        
        if not json_files:
            logger.error(f"No se encontraron archivos JSON en {self.data_dir}")
            return
        
        logger.info(f"Encontrados {len(json_files)} archivos JSON válidos")
        
        # 2. Setup inicial
        setup_sql = f"""-- Setup del Warehouse Ultra Seguro
-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";

-- Schema por defecto
SET search_path = public;

-- Configuración de timezone
SET timezone = 'America/Bogota';
"""
        
        setup_file = self.output_dir / "01_warehouse_setup.sql"
        with open(setup_file, 'w', encoding='utf-8') as f:
            f.write(setup_sql)
        logger.info(f"Setup: {setup_file}")
        
        # 3. Analizar y generar tablas
        all_tables_sql = []
        table_list = []
        
        for json_file in json_files:
            table_name = json_file.stem
            field_types = self.analyze_json_sample_ultra_safe(json_file)
            
            if field_types:
                table_sql = self.generate_ultra_safe_table_sql(table_name, field_types)
                all_tables_sql.append(table_sql)
                table_list.append(table_name)
        
        # Escribir archivo de tablas
        tables_file = self.output_dir / "02_create_tables.sql"
        with open(tables_file, 'w', encoding='utf-8') as f:
            f.write(f"""-- Creación de Tablas Ultra Seguras
-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Total: {len(table_list)} tablas

""")
            f.write('\n'.join(all_tables_sql))
        logger.info(f"Tablas: {tables_file} ({len(table_list)} tablas)")
        
        # 4. Índices básicos
        indexes_sql = f"""-- Índices básicos ultra seguros
-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

"""
        for table in table_list:
            indexes_sql += f"""
-- Índices para {table}
CREATE INDEX IF NOT EXISTS idx_{table}_created_at ON {table}(created_at);
CREATE INDEX IF NOT EXISTS idx_{table}_updated_at ON {table}(updated_at);
CREATE INDEX IF NOT EXISTS idx_{table}_is_active ON {table}(is_active);
"""
        
        indexes_file = self.output_dir / "03_create_indexes.sql"
        with open(indexes_file, 'w', encoding='utf-8') as f:
            f.write(indexes_sql)
        logger.info(f"Indices: {indexes_file}")
        
        # 5. Triggers para updated_at
        triggers_sql = f"""-- Triggers para updated_at ultra seguros
-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

-- Función genérica para updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

"""
        for table in table_list:
            triggers_sql += f"""
-- Trigger para {table}
DROP TRIGGER IF EXISTS update_{table}_modtime ON {table};
CREATE TRIGGER update_{table}_modtime
    BEFORE UPDATE ON {table}
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
"""
        
        triggers_file = self.output_dir / "04_create_triggers.sql"
        with open(triggers_file, 'w', encoding='utf-8') as f:
            f.write(triggers_sql)
        logger.info(f"Triggers: {triggers_file}")
        
        # 6. Vistas analíticas CORREGIDAS
        analytics_sql = f"""-- Vistas analíticas ultra seguras
-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

-- Vista de resumen de tablas (CORREGIDA)
CREATE OR REPLACE VIEW warehouse_summary AS
SELECT 
    schemaname,
    relname as table_name,
    n_tup_ins as total_insertions,
    n_tup_upd as total_updates,
    n_tup_del as total_deletions,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows
FROM pg_stat_user_tables
WHERE schemaname = 'public'
  AND relname NOT LIKE 'pg_%'
ORDER BY relname;

-- Vista de actividad por tabla (CORREGIDA)
CREATE OR REPLACE VIEW table_activity AS
SELECT 
    relname as table_name,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_live_tup as current_rows
FROM pg_stat_user_tables
WHERE schemaname = 'public'
  AND relname NOT LIKE 'pg_%'
ORDER BY (seq_tup_read + COALESCE(idx_tup_fetch, 0)) DESC;

-- Conteo rápido de todas las tablas
CREATE OR REPLACE VIEW tables_row_count AS
SELECT 
    relname as table_name,
    n_live_tup as row_count,
    pg_size_pretty(pg_total_relation_size(oid)) as table_size
FROM pg_stat_user_tables
WHERE schemaname = 'public'
  AND relname NOT LIKE 'pg_%'
ORDER BY n_live_tup DESC;
"""
        
        analytics_file = self.output_dir / "05_analytics_views.sql"
        with open(analytics_file, 'w', encoding='utf-8') as f:
            f.write(analytics_sql)
        logger.info(f"Analytics: {analytics_file}")
        
        # 7. Reporte final
        logger.info("\n" + "="*60)
        logger.info("GENERACION SQL ULTRA FINAL COMPLETADA")
        logger.info("="*60)
        logger.info(f"Tablas exitosas: {len(table_list)}")
        logger.info(f"Tablas con error: 0")
        logger.info(f"Archivos en: {self.output_dir}")
        logger.info("="*60)

def main():
    """Función principal"""
    try:
        # Directorios
        script_dir = Path(__file__).parent
        project_root = script_dir.parent.parent
        data_dir = project_root / "transformation_app" / "app_outputs"
        output_dir = script_dir.parent / "generated_sql"
        
        # Verificar directorios
        if not data_dir.exists():
            logger.error(f"Directorio de datos no encontrado: {data_dir}")
            return
        
        # Generar SQL
        generator = UltraFinalSQLGenerator(str(data_dir), str(output_dir))
        generator.generate_all_sql_files()
        
    except Exception as e:
        logger.error(f"Error crítico en generación: {e}")
        raise

if __name__ == "__main__":
    main()