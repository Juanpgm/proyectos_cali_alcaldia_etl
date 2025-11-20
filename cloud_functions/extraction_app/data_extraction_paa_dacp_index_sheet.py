"""
Extracción y transformación completa de datos de índice de procesos desde Google Sheets.
Script unificado que incluye:
1. Extracción de datos desde Google Sheets
2. Conversión de tipos de datos (bpin, id_paa a enteros)
3. Conversión de referencias a listas iterables
4. Agregado de prefijos BP y cruce con datos característicos
5. Validaciones y verificaciones completas
"""

import os
import json
import logging
import pandas as pd
import re
from datetime import datetime
from typing import Dict, List, Optional, Union, Tuple

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ID del spreadsheet específico para índice de procesos
INDICE_PROCESOS_SPREADSHEET_ID = "1CqIxNeD4KT1Z3dQACVc1f46OVPIZClcO11cxiWMrpVE"


# ============================================================================
# FUNCIONES UTILITARIAS PARA MANEJO DE ARCHIVOS JSON
# ============================================================================

def load_json_file(file_path: str) -> List[Dict]:
    """Cargar archivo JSON con manejo de errores"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        logger.error(f"No se encontró el archivo {file_path}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Error al decodificar JSON en {file_path}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error inesperado al cargar {file_path}: {e}")
        return []

def save_json_file(data: List[Dict], file_path: str) -> bool:
    """Guardar archivo JSON con manejo de errores"""
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error al guardar {file_path}: {e}")
        return False

def create_backup(file_path: str, backup_suffix: str) -> str:
    """Crear backup de un archivo con timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{file_path}.{backup_suffix}_{timestamp}.backup"
    
    try:
        data = load_json_file(file_path)
        if data and save_json_file(data, backup_path):
            logger.info(f"💾 Backup creado: {backup_path}")
            return backup_path
        else:
            logger.warning(f"No se pudo crear backup: {backup_path}")
            return ""
    except Exception as e:
        logger.error(f"Error creando backup: {e}")
        return ""


# ============================================================================
# FUNCIONES DE CONVERSIÓN DE TIPOS DE DATOS
# ============================================================================

def convert_to_integer(value: Union[str, int, float, None]) -> Optional[int]:
    """Convertir valor a entero, manejando diferentes casos"""
    if value is None:
        return None
    
    # Si ya es un entero, devolverlo
    if isinstance(value, int):
        return value
    
    # Si es string, intentar convertir
    if isinstance(value, str):
        value = value.strip()
        
        # Si está vacío o contiene texto no numérico, devolver None
        if not value or not re.match(r'^-?\d+(\.\d+)?$', value):
            # Verificar si contiene solo dígitos
            if re.match(r'^\d+$', value):
                return int(value)
            return None
        
        # Convertir a float primero y luego a int para manejar decimales
        try:
            float_val = float(value)
            return int(float_val)
        except ValueError:
            return None
    
    # Si es float, convertir a int
    if isinstance(value, float):
        return int(value)
    
    return None

def convert_string_to_list(value: Union[str, List, None]) -> List[str]:
    """Convertir string separado por comas en lista"""
    if value is None or value == "":
        return []
    
    # Si ya es una lista, devolverla tal como está
    if isinstance(value, list):
        return value
    
    # Convertir string a lista
    if isinstance(value, str):
        # Dividir por comas y limpiar espacios
        items = [item.strip() for item in value.split(',')]
        # Filtrar elementos vacíos
        items = [item for item in items if item]
        return items
    
    # Para otros tipos, convertir a string y procesar
    return convert_string_to_list(str(value))


# ============================================================================
# FUNCIONES DE MAPEO Y CRUCE DE DATOS
# ============================================================================

def create_bpin_to_centro_gestor_mapping(datos_caracteristicos: List[Dict]) -> Dict[str, str]:
    """Crear mapeo de bpin a nombre_centro_gestor"""
    mapping = {}
    
    for record in datos_caracteristicos:
        bpin = record.get('bpin')
        nombre_centro_gestor = record.get('nombre_centro_gestor')
        
        if bpin is not None and nombre_centro_gestor is not None:
            bpin_str = str(bpin)
            if bpin_str not in mapping:
                mapping[bpin_str] = nombre_centro_gestor
    
    logger.info(f"✅ Creado mapeo de {len(mapping)} registros bpin -> nombre_centro_gestor")
    return mapping

def create_bp_to_bpin_mapping(datos_caracteristicos: List[Dict]) -> Dict[str, str]:
    """Crear mapeo de BP a BPIN"""
    mapping = {}
    
    for item in datos_caracteristicos:
        bp = item.get('bp')
        bpin = item.get('bpin')
        
        if bp and bpin:
            # Extraer el número del BP (sin el prefijo "BP")
            if bp.startswith('BP'):
                bp_number = bp[2:]  # Remover "BP" del inicio
                mapping[bp_number] = str(bpin)
    
    logger.info(f"✅ Creado mapeo BP->BPIN con {len(mapping)} entradas")
    return mapping


# ============================================================================
# FUNCIONES DE PROCESAMIENTO Y TRANSFORMACIÓN
# ============================================================================

def process_data_transformations(data: List[Dict], 
                               bpin_to_centro_gestor_mapping: Dict[str, str],
                               bp_to_bpin_mapping: Dict[str, str]) -> Tuple[List[Dict], Dict]:
    """Procesar todas las transformaciones de datos en un solo paso"""
    
    stats = {
        'total_records': len(data),
        'bp_prefixed': 0,
        'bpin_from_bp_added': 0,
        'bpin_converted_to_int': 0,
        'id_paa_converted_to_int': 0,
        'nombre_centro_gestor_added': 0,
        'referencia_proceso_converted': 0,
        'referencia_contrato_converted': 0,
        'referencia_proceso_with_multiple': 0,
        'referencia_contrato_with_multiple': 0,
        'errors': []
    }
    
    processed_data = []
    
    for i, record in enumerate(data):
        try:
            processed_record = record.copy()
            
            # 1. Agregar prefijo "BP" al valor de bp y obtener BPIN
            bp_value = record.get('bp')
            if bp_value is not None:
                bp_str = str(bp_value)
                processed_record['bp'] = f"BP{bp_str}"
                stats['bp_prefixed'] += 1
                
                # Buscar BPIN correspondiente desde el mapeo BP->BPIN
                bpin_from_bp = bp_to_bpin_mapping.get(bp_str)
                if bpin_from_bp and not processed_record.get('bpin'):
                    processed_record['bpin'] = bpin_from_bp
                    stats['bpin_from_bp_added'] += 1
            
            # 2. Convertir bpin a entero
            original_bpin = processed_record.get('bpin')
            if original_bpin is not None:
                converted_bpin = convert_to_integer(original_bpin)
                if converted_bpin is not None:
                    processed_record['bpin'] = converted_bpin
                    stats['bpin_converted_to_int'] += 1
            
            # 3. Convertir id_paa a entero
            original_id_paa = record.get('id_paa')
            if original_id_paa is not None:
                converted_id_paa = convert_to_integer(original_id_paa)
                if converted_id_paa is not None:
                    processed_record['id_paa'] = converted_id_paa
                    stats['id_paa_converted_to_int'] += 1
            
            # 4. Agregar nombre_centro_gestor
            bpin_for_lookup = str(processed_record.get('bpin', ''))
            if bpin_for_lookup in bpin_to_centro_gestor_mapping:
                processed_record['nombre_centro_gestor'] = bpin_to_centro_gestor_mapping[bpin_for_lookup]
                stats['nombre_centro_gestor_added'] += 1
            else:
                processed_record['nombre_centro_gestor'] = None
            
            # 5. Convertir referencia_proceso a lista
            original_proceso = record.get('referencia_proceso')
            converted_proceso = convert_string_to_list(original_proceso)
            processed_record['referencia_proceso'] = converted_proceso
            if original_proceso is not None:
                stats['referencia_proceso_converted'] += 1
                if len(converted_proceso) > 1:
                    stats['referencia_proceso_with_multiple'] += 1
            
            # 6. Convertir referencia_contrato a lista
            original_contrato = record.get('referencia_contrato')
            converted_contrato = convert_string_to_list(original_contrato)
            processed_record['referencia_contrato'] = converted_contrato
            if original_contrato is not None:
                stats['referencia_contrato_converted'] += 1
                if len(converted_contrato) > 1:
                    stats['referencia_contrato_with_multiple'] += 1
            
            processed_data.append(processed_record)
            
        except Exception as e:
            error_msg = f"Error procesando registro {i}: {e}"
            logger.error(error_msg)
            stats['errors'].append(error_msg)
            # Agregar el registro original en caso de error
            processed_data.append(record)
    
    return processed_data, stats


# ============================================================================
# FUNCIONES DE VALIDACIÓN
# ============================================================================

def validate_transformations(data: List[Dict]) -> Dict[str, bool]:
    """Validar que todas las transformaciones se aplicaron correctamente"""
    validation_results = {
        'bp_prefixes_valid': True,
        'bpin_integers_valid': True,
        'id_paa_integers_valid': True,
        'lists_iterable': True,
        'centro_gestor_present': True
    }
    
    logger.info("🔍 Validando transformaciones...")
    
    for i, record in enumerate(data[:10]):  # Validar muestra de 10 registros
        try:
            # Validar prefijo BP
            bp_value = record.get('bp')
            if bp_value and not str(bp_value).startswith('BP'):
                validation_results['bp_prefixes_valid'] = False
                logger.warning(f"Registro {i}: BP sin prefijo: {bp_value}")
            
            # Validar bpin como entero
            bpin_value = record.get('bpin')
            if bpin_value is not None and not isinstance(bpin_value, int):
                validation_results['bpin_integers_valid'] = False
                logger.warning(f"Registro {i}: BPIN no es entero: {bpin_value} ({type(bpin_value)})")
            
            # Validar id_paa como entero cuando no es None
            id_paa_value = record.get('id_paa')
            if id_paa_value is not None and not isinstance(id_paa_value, int):
                validation_results['id_paa_integers_valid'] = False
                logger.warning(f"Registro {i}: ID_PAA no es entero: {id_paa_value} ({type(id_paa_value)})")
            
            # Validar listas iterables
            ref_proceso = record.get('referencia_proceso', [])
            ref_contrato = record.get('referencia_contrato', [])
            
            if not isinstance(ref_proceso, list) or not isinstance(ref_contrato, list):
                validation_results['lists_iterable'] = False
                logger.warning(f"Registro {i}: Referencias no son listas")
            
            # Intentar iterar las listas
            try:
                for _ in ref_proceso:
                    pass
                for _ in ref_contrato:
                    pass
            except Exception as e:
                validation_results['lists_iterable'] = False
                logger.warning(f"Registro {i}: Error iterando listas: {e}")
            
        except Exception as e:
            logger.error(f"Error validando registro {i}: {e}")
    
    return validation_results


# ============================================================================
# FUNCIONES DE REPORTE Y ESTADÍSTICAS
# ============================================================================

def print_transformation_statistics(stats: Dict):
    """Imprimir estadísticas detalladas de las transformaciones"""
    logger.info("\n" + "="*60)
    logger.info("📊 ESTADÍSTICAS DE TRANSFORMACIÓN")
    logger.info("="*60)
    logger.info(f"📁 Total de registros procesados: {stats['total_records']}")
    logger.info(f"🏷️  Prefijos BP agregados: {stats['bp_prefixed']}")
    logger.info(f"🔗 BPIN obtenidos desde BP: {stats['bpin_from_bp_added']}")
    logger.info(f"🔢 BPIN convertidos a entero: {stats['bpin_converted_to_int']}")
    logger.info(f"🔢 ID_PAA convertidos a entero: {stats['id_paa_converted_to_int']}")
    logger.info(f"🏢 Nombre_centro_gestor agregados: {stats['nombre_centro_gestor_added']}")
    logger.info(f"📋 Referencia_proceso convertidas: {stats['referencia_proceso_converted']}")
    logger.info(f"📋 Referencia_contrato convertidas: {stats['referencia_contrato_converted']}")
    logger.info(f"📋 Procesos con múltiples referencias: {stats['referencia_proceso_with_multiple']}")
    logger.info(f"📋 Contratos con múltiples referencias: {stats['referencia_contrato_with_multiple']}")
    
    if stats['errors']:
        logger.warning(f"⚠️  Errores encontrados: {len(stats['errors'])}")
        for error in stats['errors'][:5]:  # Mostrar solo los primeros 5 errores
            logger.warning(f"   {error}")
    
    # Calcular tasas de éxito
    if stats['total_records'] > 0:
        centro_gestor_rate = (stats['nombre_centro_gestor_added'] / stats['total_records']) * 100
        logger.info(f"📈 Tasa de éxito centro gestor: {centro_gestor_rate:.1f}%")

def show_transformation_examples(data: List[Dict], num_examples: int = 3):
    """Mostrar ejemplos de las transformaciones realizadas"""
    logger.info("\n" + "="*60)
    logger.info("🔍 EJEMPLOS DE TRANSFORMACIONES")
    logger.info("="*60)
    
    examples_shown = 0
    for i, record in enumerate(data):
        if examples_shown >= num_examples:
            break
            
        if record.get('bpin') is not None or record.get('bp'):
            logger.info(f"\n📋 Ejemplo {examples_shown + 1} (Registro {i+1}):")
            logger.info(f"   bp: {record.get('bp')}")
            logger.info(f"   bpin: {record.get('bpin')} (tipo: {type(record.get('bpin')).__name__})")
            logger.info(f"   id_paa: {record.get('id_paa')} (tipo: {type(record.get('id_paa')).__name__})")
            logger.info(f"   nombre_centro_gestor: {record.get('nombre_centro_gestor')}")
            logger.info(f"   referencia_proceso: {record.get('referencia_proceso', [])} (len: {len(record.get('referencia_proceso', []))})")
            logger.info(f"   referencia_contrato: {record.get('referencia_contrato', [])} (len: {len(record.get('referencia_contrato', []))})")
            examples_shown += 1


# ============================================================================
# FUNCIÓN PRINCIPAL DE EXTRACCIÓN
# ============================================================================

def extract_indice_procesos_to_json():
    """Extrae la tabla de índice de procesos desde Google Sheets y la exporta como JSON"""
    try:
        logger.info("🚀 Iniciando extracción de índice de procesos...")
        
        # Convertir el ID del spreadsheet a URL de CSV para pandas
        csv_url = f"https://docs.google.com/spreadsheets/d/{INDICE_PROCESOS_SPREADSHEET_ID}/export?format=csv&gid=0"
        
        logger.info(f"📡 Extrayendo datos desde Google Sheets...")
        
        # Leer datos usando pandas
        df = pd.read_csv(csv_url)
        
        logger.info(f"✅ Datos extraídos exitosamente. Shape: {df.shape}")
        logger.info(f"📋 Columnas: {list(df.columns)}")
        
        # Crear directorio de salida si no existe
        output_dir = os.path.join(os.path.dirname(__file__), "..", "transformation_app", "app_inputs", "indice_procesos_emprestito")
        os.makedirs(output_dir, exist_ok=True)
        
        # Ruta del archivo de salida
        output_file = os.path.join(output_dir, "indice_procesos.json")
        
        # Convertir DataFrame a JSON y guardar
        df.to_json(output_file, orient='records', indent=2, force_ascii=False)
        
        logger.info(f"💾 Archivo JSON guardado exitosamente en: {output_file}")
        logger.info(f"📊 Total de registros exportados: {len(df)}")
        
        return df, output_file
        
    except Exception as e:
        logger.error(f"❌ Error durante la extracción: {str(e)}")
        raise


# ============================================================================
# FUNCIÓN PRINCIPAL DE TRANSFORMACIÓN COMPLETA
# ============================================================================

def transform_indice_procesos_data(indice_file: str):
    """Aplicar todas las transformaciones al archivo de índice de procesos"""
    
    logger.info("🔄 Iniciando transformaciones completas...")
    
    # Rutas de archivos de referencia
    datos_caracteristicos_path = os.path.join(
        os.path.dirname(__file__), "..", 
        "transformation_app", "app_outputs", "ejecucion_presupuestal_outputs", 
        "datos_caracteristicos_proyectos.json"
    )
    
    # Verificar que los archivos existen
    if not os.path.exists(indice_file):
        logger.error(f"❌ No se encontró el archivo {indice_file}")
        return False
    
    if not os.path.exists(datos_caracteristicos_path):
        logger.error(f"❌ No se encontró el archivo {datos_caracteristicos_path}")
        return False
    
    # Cargar datos
    logger.info("📂 Cargando archivos...")
    indice_data = load_json_file(indice_file)
    datos_caracteristicos = load_json_file(datos_caracteristicos_path)
    
    if not indice_data:
        logger.error("❌ No se pudieron cargar los datos del índice")
        return False
    
    if not datos_caracteristicos:
        logger.error("❌ No se pudieron cargar los datos característicos")
        return False
    
    logger.info(f"📊 Datos cargados:")
    logger.info(f"   - Índice de procesos: {len(indice_data)} registros")
    logger.info(f"   - Datos característicos: {len(datos_caracteristicos)} registros")
    
    # Crear mapeos necesarios
    logger.info("🔗 Creando mapeos de datos...")
    bpin_to_centro_gestor_mapping = create_bpin_to_centro_gestor_mapping(datos_caracteristicos)
    bp_to_bpin_mapping = create_bp_to_bpin_mapping(datos_caracteristicos)
    
    # Crear backup antes de las transformaciones
    backup_path = create_backup(indice_file, "pre_transformations")
    if not backup_path:
        logger.warning("⚠️ No se pudo crear backup, continuando sin backup...")
    
    # Aplicar todas las transformaciones
    logger.info("🔄 Aplicando transformaciones...")
    transformed_data, stats = process_data_transformations(
        indice_data, 
        bpin_to_centro_gestor_mapping, 
        bp_to_bpin_mapping
    )
    
    # Validar transformaciones
    validation_results = validate_transformations(transformed_data)
    
    # Mostrar estadísticas y ejemplos
    print_transformation_statistics(stats)
    show_transformation_examples(transformed_data)
    
    # Mostrar resultados de validación
    logger.info("\n" + "="*60)
    logger.info("✅ RESULTADOS DE VALIDACIÓN")
    logger.info("="*60)
    for validation, result in validation_results.items():
        status = "✅" if result else "❌"
        logger.info(f"{status} {validation}: {'VÁLIDO' if result else 'INVÁLIDO'}")
    
    # Guardar archivo transformado
    logger.info("💾 Guardando archivo transformado...")
    if save_json_file(transformed_data, indice_file):
        logger.info(f"✅ Archivo transformado guardado exitosamente: {indice_file}")
        return True
    else:
        logger.error("❌ Error al guardar el archivo transformado")
        return False


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

if __name__ == "__main__":
    try:
        # Paso 1: Extraer datos desde Google Sheets
        df, output_file = extract_indice_procesos_to_json()
        
        # Paso 2: Aplicar todas las transformaciones
        success = transform_indice_procesos_data(output_file)
        
        if success:
            logger.info("\n" + "="*60)
            logger.info("🎉 PROCESO COMPLETADO EXITOSAMENTE")
            logger.info("="*60)
            logger.info(f"📁 Archivo final: {output_file}")
            logger.info(f"📊 Registros procesados: {len(df)}")
            logger.info("✅ Todas las transformaciones aplicadas:")
            logger.info("   - Extracción desde Google Sheets")
            logger.info("   - Prefijos BP agregados")
            logger.info("   - BPIN y ID_PAA convertidos a enteros")
            logger.info("   - Referencias convertidas a listas iterables")
            logger.info("   - Nombre_centro_gestor agregado")
            logger.info("   - Validaciones completadas")
        else:
            logger.error("❌ Error durante las transformaciones")
            exit(1)
            
    except Exception as e:
        logger.error(f"❌ Error durante el proceso: {e}")
        exit(1)
