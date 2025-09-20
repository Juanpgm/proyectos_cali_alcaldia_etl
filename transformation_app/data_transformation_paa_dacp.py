"""
Transformación simple de datos PAA DACP usando programación funcional.
"""

import sys
import os
import json
from typing import Dict, List, Any, Tuple, Optional
from functools import partial
from datetime import datetime
from pathlib import Path

# Agregar el directorio de extraction_app al path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'extraction_app'))

from data_extraction_paa_dacp_sheet import extract_paa_dacp_data

def load_bpin_mapping() -> Dict[str, int]:
    """
    Cargar mapeo de BP -> BPIN desde datos_caracteristicos_proyectos.json (función pura).
    
    Returns:
        Diccionario con mapeo BP -> BPIN
    """
    try:
        bpin_file_path = Path(__file__).parent / "app_outputs" / "ejecucion_presupuestal_outputs" / "datos_caracteristicos_proyectos.json"
        
        if not bpin_file_path.exists():
            print(f"⚠️ Archivo de mapeo BPIN no encontrado: {bpin_file_path}")
            return {}
        
        with open(bpin_file_path, 'r', encoding='utf-8') as f:
            datos_caracteristicos = json.load(f)
        
        # Crear mapeo BP -> BPIN
        bp_to_bpin = {}
        for item in datos_caracteristicos:
            bp = item.get('bp')
            bpin = item.get('bpin')
            if bp and bpin:
                bp_to_bpin[bp] = int(bpin)
        
        print(f"✅ Mapeo BPIN cargado: {len(bp_to_bpin)} registros")
        return bp_to_bpin
        
    except Exception as e:
        print(f"❌ Error cargando mapeo BPIN: {e}")
        return {}

def transform_field_names(data: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
    """Transformar nombres de campos según especificaciones (función pura)"""
    result = []
    
    for record in data:
        new_record = dict(record)
        
        # 1. Cambiar 'organismo' por 'nombre_centro_gestor'
        if 'organismo' in new_record:
            new_record['nombre_centro_gestor'] = new_record.pop('organismo')
        
        # 2. Cambiar 'codigo_organismo' por 'cod_centro_gestor' y convertir a entero
        if 'codigo_organismo' in new_record:
            cod_value = new_record.pop('codigo_organismo')
            try:
                new_record['cod_centro_gestor'] = int(cod_value) if cod_value else 0
            except (ValueError, TypeError):
                new_record['cod_centro_gestor'] = 0
        
        # 3. Cambiar 'empréstito' por 'emprestito'
        if 'empréstito' in new_record:
            new_record['emprestito'] = new_record.pop('empréstito')
        
        # 4. Cambiar 'elemento_pep_2' por 'bp'
        if 'elemento_pep_2' in new_record:
            new_record['bp'] = new_record.pop('elemento_pep_2')
        
        result.append(new_record)
    
    return tuple(result)

def add_bpin_field(data: Tuple[Dict[str, Any], ...], bp_to_bpin_map: Dict[str, int]) -> Tuple[Dict[str, Any], ...]:
    """Agregar campo BPIN usando mapeo BP -> BPIN (función pura)"""
    result = []
    bpin_found = 0
    
    for record in data:
        new_record = dict(record)
        
        # 5. Añadir campo 'bpin' basado en 'bp'
        bp_value = new_record.get('bp')
        if bp_value and bp_value in bp_to_bpin_map:
            new_record['bpin'] = bp_to_bpin_map[bp_value]
            bpin_found += 1
        else:
            new_record['bpin'] = None
        
        result.append(new_record)
    
    print(f"✅ Campo BPIN agregado: {bpin_found}/{len(data)} registros con BPIN válido")
    return tuple(result)

def remove_unwanted_fields(data: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
    """Remover campos innecesarios según especificaciones (función pura)"""
    # 6. Campos a eliminar
    fields_to_remove = {
        'ppto._disponible', 'número_modificaciones', 'fuente_recursos_final', 
        'inclusion_social', 'fuente_recursos-valor', 'proyecto', 'comuna', 
        'origen', '_metadata', 'fila_origen', 'tipo_registro', 'año_vigencia'
    }
    
    result = []
    
    for record in data:
        new_record = {}
        
        for key, value in record.items():
            if key not in fields_to_remove:
                new_record[key] = value
        
        result.append(new_record)
    
    print(f"✅ Campos removidos: {len(fields_to_remove)} campos eliminados")
    return tuple(result)

def standardize_monetary_values(data: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
    """Convertir valores monetarios a enteros (bigint) sin decimales (función pura)"""
    def parse_money_to_int(value: Any) -> int:
        """Convertir valor a entero monetario"""
        if value is None or value == '':
            return 0
        
        if isinstance(value, (int, float)):
            return int(float(value))
        
        if isinstance(value, str):
            # Limpiar string monetario
            import re
            cleaned = re.sub(r'[^\d.,\-]', '', value.strip())
            
            if not cleaned or cleaned in ['-', '.', ',']:
                return 0
            
            # Manejar separadores
            if ',' in cleaned and '.' in cleaned:
                # Formato: 1.234.567,89
                parts = cleaned.split(',')
                if len(parts) == 2 and len(parts[1]) <= 2:
                    integer_part = parts[0].replace('.', '')
                    decimal_part = parts[1]
                    cleaned = f"{integer_part}.{decimal_part}"
                else:
                    cleaned = cleaned.replace(',', '').replace('.', '')
            elif ',' in cleaned:
                # Verificar si es decimal o miles
                parts = cleaned.split(',')
                if len(parts) == 2 and len(parts[1]) <= 2:
                    cleaned = cleaned.replace(',', '.')
                else:
                    cleaned = cleaned.replace(',', '')
            
            try:
                return int(float(cleaned))
            except ValueError:
                return 0
        
        return 0
    
    # Campos que pueden contener valores monetarios según los datos PAA DACP
    money_fields = [
        'valor_actividad', 'valor_disponible', 'valor_apropiado', 
        'valor_total_estimado', 'valor_vigencia_actual',
        'funcionamiento_real_estimado', 'inversión_real_estimado'
    ]
    
    result = []
    for record in data:
        new_record = dict(record)
        
        for key, value in record.items():
            if key.startswith('valor_') or key in money_fields:
                new_record[key] = parse_money_to_int(value)
        
        result.append(new_record)
    
    return tuple(result)

def clean_text_fields(data: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
    """Limpiar campos de texto específicos de PAA DACP (función pura)"""
    def clean_text(value: Any) -> Optional[str]:
        """Limpiar un texto individual"""
        if value is None or value == '':
            return None
        
        text = str(value).strip()
        if not text or text.lower() in ['none', 'null', '0']:
            return None
        
        # Limpiar espacios múltiples
        import re
        cleaned = re.sub(r'\s+', ' ', text)
        
        return cleaned if cleaned else None
    
    # Campos de texto específicos de PAA DACP a limpiar
    text_fields = [
        'descripcion', 'organismo', 'modalidad_contratacion',
        'fuente_recursos', 'categoria', 'subcategoria',
        'nombre_abreviado', 'justificación_vencida'
    ]
    
    result = []
    for record in data:
        new_record = dict(record)
        
        for field in text_fields:
            if field in new_record:
                new_record[field] = clean_text(new_record[field])
        
        result.append(new_record)
    
    return tuple(result)

def add_derived_fields(data: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
    """Agregar campos derivados específicos de PAA DACP (función pura)"""
    result = []
    
    for i, record in enumerate(data):
        new_record = dict(record)
        
        # Agregar timestamp de procesamiento
        new_record['fecha_procesamiento'] = datetime.now().isoformat()
        
        # Clasificación por valor total
        valor_total = new_record.get('valor_total_estimado', 0)
        if isinstance(valor_total, (int, float)):
            if valor_total >= 1000000000:  # >= 1 billón
                new_record['clasificacion_valor'] = 'alto'
            elif valor_total >= 100000000:  # >= 100 millones
                new_record['clasificacion_valor'] = 'medio'
            elif valor_total > 0:
                new_record['clasificacion_valor'] = 'bajo'
            else:
                new_record['clasificacion_valor'] = 'sin_valor'
        else:
            new_record['clasificacion_valor'] = 'sin_valor'
        
        # Extraer año de vigencia
        vigencia = record.get('vigencia')
        if vigencia:
            try:
                new_record['anio'] = int(vigencia)
            except (ValueError, TypeError):
                new_record['anio'] = None
        else:
            new_record['anio'] = None
        
        result.append(new_record)
    
    return tuple(result)

def filter_valid_records(data: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
    """Filtrar registros válidos (función pura)"""
    def is_valid_record(record: Dict[str, Any]) -> bool:
        """Determinar si un registro PAA DACP es válido"""
        # Un registro es válido si tiene descripción y nombre_centro_gestor
        descripcion = record.get('descripcion', '').strip() if record.get('descripcion') else ''
        nombre_centro_gestor = record.get('nombre_centro_gestor', '').strip() if record.get('nombre_centro_gestor') else ''
        
        return bool(descripcion and nombre_centro_gestor)
    
    return tuple(record for record in data if is_valid_record(record))

def transform_paa_dacp_data(raw_data: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
    """
    Pipeline de transformación principal con las nuevas especificaciones (función pura).
    
    Args:
        raw_data: Datos crudos extraídos
        
    Returns:
        Datos transformados y estandarizados según especificaciones
    """
    print("Transformando datos PAA DACP...")
    
    # Cargar mapeo BP -> BPIN al inicio
    print("Cargando mapeo BP -> BPIN...")
    bp_to_bpin_map = load_bpin_mapping()
    
    # Pipeline de transformación usando composición funcional
    transformed = raw_data
    
    # Aplicar transformaciones secuenciales según especificaciones
    transformations = [
        # 1. Convertir valores monetarios a enteros (bigint)
        standardize_monetary_values,
        # 2. Limpiar campos de texto
        clean_text_fields,
        # 3. Transformar nombres de campos (organismo -> nombre_centro_gestor, etc.)
        transform_field_names,
        # 4. Agregar campo BPIN usando mapeo
        lambda data: add_bpin_field(data, bp_to_bpin_map),
        # 5. Agregar campos derivados
        add_derived_fields,
        # 6. Remover campos innecesarios
        remove_unwanted_fields,
        # 7. Filtrar registros válidos
        filter_valid_records
    ]
    
    for transform_func in transformations:
        transformed = transform_func(transformed)
        # Obtener nombre de función para logging
        func_name = transform_func.__name__ if hasattr(transform_func, '__name__') else 'lambda_transform'
        print(f"Después de {func_name}: {len(transformed)} registros")
    
    return transformed

def filter_emprestito_records(data: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
    """
    Filtrar registros donde emprestito sea igual a 'SI' (función pura).
    
    Args:
        data: Datos transformados
        
    Returns:
        Registros filtrados donde emprestito = 'SI'
    """
    result = []
    
    for record in data:
        emprestito_value = record.get('emprestito', '').strip().upper() if record.get('emprestito') else ''
        if emprestito_value == 'SI':
            result.append(record)
    
    print(f"✅ Filtro empréstito aplicado: {len(result)} registros con emprestito = 'SI'")
    return tuple(result)

def save_to_json(data: Tuple[Dict[str, Any], ...], output_path: str) -> bool:
    """
    Guardar datos en archivo JSON (función con efecto I/O).
    
    Args:
        data: Datos a guardar
        output_path: Ruta del archivo de salida
        
    Returns:
        True si se guardó exitosamente
    """
    try:
        # Crear directorio si no existe
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Convertir tupla a lista para JSON
        json_data = list(data)
        
        # Guardar archivo
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"Datos guardados en: {output_path}")
        return True
        
    except Exception as e:
        print(f"Error guardando datos: {e}")
        return False

def generate_summary(data: Tuple[Dict[str, Any], ...]) -> Dict[str, Any]:
    """
    Generar resumen estadístico de los datos PAA DACP (función pura).
    
    Args:
        data: Datos transformados
        
    Returns:
        Diccionario con estadísticas específicas de PAA DACP
    """
    if not data:
        return {"total_records": 0}
    
    # Estadísticas básicas
    total_records = len(data)
    
    # Contadores específicos de PAA DACP
    centros_gestores = {}
    modalidades = {}
    categorias = {}
    estados = {}
    años = {}
    clasificaciones_valor = {}
    
    total_valor = 0
    registros_con_valor = 0
    
    for record in data:
        # Contar centros gestores (antes organismos)
        centro_gestor = record.get('nombre_centro_gestor', 'Sin centro gestor')
        centros_gestores[centro_gestor] = centros_gestores.get(centro_gestor, 0) + 1
        
        # Contar modalidades de contratación
        modalidad = record.get('modalidad_contratacion', 'Sin modalidad')
        modalidades[modalidad] = modalidades.get(modalidad, 0) + 1
        
        # Contar categorías
        categoria = record.get('categoria', 'Sin categoría')
        categorias[categoria] = categorias.get(categoria, 0) + 1
        
        # Contar estados
        estado = record.get('estado', 'Sin estado')
        estados[estado] = estados.get(estado, 0) + 1
        
        # Contar años de vigencia
        año = record.get('anio')  # Cambio de año_vigencia a anio
        if año:
            años[año] = años.get(año, 0) + 1
        
        # Contar clasificaciones de valor
        clasificacion = record.get('clasificacion_valor', 'sin_clasificacion')
        clasificaciones_valor[clasificacion] = clasificaciones_valor.get(clasificacion, 0) + 1
        
        # Sumar valores totales estimados (ahora son enteros)
        valor = record.get('valor_total_estimado', 0)
        if isinstance(valor, (int, float)) and valor > 0:
            total_valor += valor
            registros_con_valor += 1
    
    return {
        "total_records": total_records,
        "registros_con_valor": registros_con_valor,
        "valor_total": total_valor,
        "valor_promedio": total_valor / registros_con_valor if registros_con_valor > 0 else 0,
        "top_centros_gestores": dict(sorted(centros_gestores.items(), key=lambda x: x[1], reverse=True)[:10]),
        "modalidades_contratacion": modalidades,
        "categorias": categorias,
        "estados": estados,
        "años_vigencia": años,
        "clasificaciones_valor": clasificaciones_valor,
        "timestamp": datetime.now().isoformat()
    }

def process_complete_paa_dacp():
    """
    Proceso completo: extraer, transformar y guardar datos PAA DACP.
    """
    print("=== INICIANDO PROCESO PAA DACP ===")
    
    try:
        # Paso 1: Extraer datos
        print("Paso 1: Extrayendo datos...")
        raw_data = extract_paa_dacp_data()
        
        if not raw_data:
            print("❌ No se encontraron datos para procesar")
            return False
        
        print(f"✅ Datos extraídos: {len(raw_data)} registros")
        
        # Paso 2: Transformar datos
        print("Paso 2: Transformando datos...")
        transformed_data = transform_paa_dacp_data(raw_data)
        
        if not transformed_data:
            print("❌ No quedaron datos válidos después de la transformación")
            return False
        
        print(f"✅ Datos transformados: {len(transformed_data)} registros válidos")
        
        # Paso 3: Guardar datos completos
        print("Paso 3: Guardando datos completos...")
        output_dir = Path(__file__).parent / "app_outputs" / "paa_dacp"
        output_file = output_dir / "paa_dacp.json"
        
        success = save_to_json(transformed_data, str(output_file))
        
        if not success:
            print("❌ Error guardando los datos completos")
            return False
        
        # Paso 4: Crear archivo filtrado para emprestito = 'SI'
        print("Paso 4: Creando archivo filtrado para emprestito = 'SI'...")
        emprestito_data = filter_emprestito_records(transformed_data)
        
        if emprestito_data:
            emprestito_output_file = output_dir / "emp_paa_dacp.json"
            emprestito_success = save_to_json(emprestito_data, str(emprestito_output_file))
            
            if emprestito_success:
                print(f"✅ Archivo filtrado creado: {emprestito_output_file}")
                print(f"� Registros con emprestito = 'SI': {len(emprestito_data)}")
            else:
                print("❌ Error guardando archivo filtrado de emprestito")
        else:
            print("⚠️ No se encontraron registros con emprestito = 'SI'")
        
        print("✅ Proceso completado exitosamente")
        print(f"📄 Archivo principal: {output_file}")
        if emprestito_data:
            print(f"📄 Archivo filtrado: {output_dir / 'emp_paa_dacp.json'}")
        
        # Mostrar estadísticas básicas sin generar archivo de resumen
        summary = generate_summary(transformed_data)
        print(f"\n📊 ESTADÍSTICAS:")
        print(f"   Total de registros: {summary['total_records']}")
        print(f"   Registros con valor: {summary['registros_con_valor']}")
        print(f"   Valor total: ${summary['valor_total']:,.0f}")
        print(f"   Valor promedio: ${summary['valor_promedio']:,.0f}")
        
        print(f"\n🏢 TOP 5 CENTROS GESTORES:")
        for i, (centro_gestor, count) in enumerate(list(summary['top_centros_gestores'].items())[:5]):
            print(f"   {i+1}. {centro_gestor}: {count} registros")
        
        return True
            
    except Exception as e:
        print(f"❌ Error en el proceso: {e}")
        return False

if __name__ == "__main__":
    # Ejecutar proceso completo
    success = process_complete_paa_dacp()
    
    if success:
        print("\n🎉 ¡Proceso completado con éxito!")
    else:
        print("\n💥 Proceso falló. Revisa los errores arriba.")
