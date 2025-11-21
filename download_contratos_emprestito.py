"""
Script para descargar datos unificados de las colecciones de empréstito de Firebase:
- contratos_emprestito
- ordenes_compra_emprestito
- convenios_transferencias_emprestito

Exporta los datos a un archivo Excel (.xlsx) con una tabla unificada,
similar al endpoint GET /contratos_emprestito_all

Uso:
    python download_contratos_emprestito.py
    
    # O con parámetros personalizados:
    python download_contratos_emprestito.py --output emprestito_unificado.xlsx --limit 100
"""

import pandas as pd
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import argparse
from database.config import get_firestore_client, initialize_firebase


# Mapeo de campos para unificar esquemas
# Estructura: {colección: {campo_origen: campo_destino}}
MAPEO_CAMPOS = {
    "ordenes_compra_emprestito": {
        "numero_orden": "referencia_contrato",
        "referencia_orden": "referencia_contrato",
        "valor_orden": "valor_contrato",
        "estado_orden": "estado_contrato",
        "fecha_publicacion_orden": "fecha_firma_contrato",
        "fecha_vencimiento_orden": "fecha_fin_contrato",
        "entidad_compradora": "entidad_contratante",
        "nombre_proveedor": "contratista",
        "nit_proveedor": "nit_contratista",
        "descripcion_orden": "objeto_contrato",
        "objeto_orden": "objeto_contrato",
        "tipo_orden": "modalidad_contratacion",
        "modalidad_contratacion": "modalidad_contratacion",  # Ya existe
        "nombre_banco": "banco",  # Normalizar nombre
    },
    "convenios_transferencias_emprestito": {
        "valor_convenio": "valor_contrato",
        "fecha_inicio_contrato": "fecha_firma_contrato",
        "nombre_banco": "banco",  # Normalizar nombre
        # Los demás campos ya coinciden con el esquema estándar
    },
    "contratos_emprestito": {
        # Esta es la colección base, solo normalizamos banco si es necesario
        "nombre_banco": "banco",
    }
}


def normalizar_campos(df: pd.DataFrame, collection_name: str) -> pd.DataFrame:
    """
    Normaliza los campos de un DataFrame según el esquema estándar de contratos.
    
    Args:
        df: DataFrame con los datos originales
        collection_name: Nombre de la colección de origen
        
    Returns:
        DataFrame con campos normalizados
    """
    if df.empty or collection_name not in MAPEO_CAMPOS:
        return df
    
    df_normalizado = df.copy()
    mapeo = MAPEO_CAMPOS[collection_name]
    
    # Aplicar mapeo de campos
    for campo_origen, campo_destino in mapeo.items():
        if campo_origen in df_normalizado.columns:
            # Si el campo destino no existe, renombrar
            if campo_destino not in df_normalizado.columns:
                df_normalizado.rename(columns={campo_origen: campo_destino}, inplace=True)
            # Si el campo destino existe pero está vacío y el origen tiene datos, copiar
            elif campo_origen != campo_destino:
                mask = df_normalizado[campo_destino].isna() | (df_normalizado[campo_destino] == '')
                df_normalizado.loc[mask, campo_destino] = df_normalizado.loc[mask, campo_origen]
    
    return df_normalizado


def descargar_coleccion(
    collection_name: str,
    limit: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
    tipo_registro: Optional[str] = None
) -> pd.DataFrame:
    """
    Descarga todos los documentos de una colección específica de Firebase.
    
    Args:
        collection_name: Nombre de la colección en Firebase
        limit: Límite de documentos a descargar (None = todos)
        filters: Diccionario de filtros a aplicar (ej: {'estado': 'activo'})
        tipo_registro: Etiqueta para identificar el tipo de registro en la tabla unificada
        
    Returns:
        DataFrame de pandas con los datos descargados y normalizados
        
    Raises:
        Exception: Si hay error conectando a Firebase o descargando datos
    """
    try:
        client = get_firestore_client()
        
        # Obtener referencia a la colección
        print(f"   📥 Descargando '{collection_name}'...")
        collection_ref = client.collection(collection_name)
        
        # Aplicar filtros si se proporcionan
        if filters:
            for field, value in filters.items():
                collection_ref = collection_ref.where(field, '==', value)
                print(f"      🔍 Filtro: {field} = {value}")
        
        # Aplicar límite si se proporciona
        if limit:
            collection_ref = collection_ref.limit(limit)
            print(f"      📊 Límite: {limit} documentos")
        
        # Descargar documentos
        docs = collection_ref.stream()
        
        # Convertir a lista de diccionarios
        data = []
        count = 0
        for doc in docs:
            doc_data = doc.to_dict()
            # Agregar el ID del documento
            doc_data['document_id'] = doc.id
            # Agregar tipo de registro si se especifica
            if tipo_registro:
                doc_data['tipo_registro'] = tipo_registro
            data.append(doc_data)
            count += 1
            
            # Mostrar progreso cada 50 documentos
            if count % 50 == 0:
                print(f"      ⏳ {count} documentos...")
        
        print(f"   ✅ {count} documentos descargados")
        
        if not data:
            return pd.DataFrame()
        
        # Convertir a DataFrame
        df = pd.DataFrame(data)
        
        # Normalizar campos según el esquema estándar
        print(f"      🔄 Normalizando campos al esquema estándar...")
        df = normalizar_campos(df, collection_name)
        
        return df
        
    except Exception as e:
        print(f"   ❌ Error descargando '{collection_name}': {e}")
        raise


def unificar_esquema(dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Unifica múltiples DataFrames asegurando que todos tengan las mismas columnas.
    Las columnas faltantes se agregan con valores NaN.
    
    Args:
        dataframes: Lista de DataFrames a unificar
        
    Returns:
        DataFrame unificado con todas las columnas
    """
    if not dataframes:
        return pd.DataFrame()
    
    # Obtener todas las columnas únicas
    todas_columnas = set()
    for df in dataframes:
        todas_columnas.update(df.columns)
    
    # Agregar columnas faltantes a cada DataFrame
    dataframes_normalizados = []
    for df in dataframes:
        df_normalizado = df.copy()
        for col in todas_columnas:
            if col not in df_normalizado.columns:
                # Usar pd.NA en lugar de None para compatibilidad con pandas
                df_normalizado[col] = pd.NA
        dataframes_normalizados.append(df_normalizado)
    
    # Concatenar con el mismo orden de columnas
    columnas_ordenadas = sorted(todas_columnas)
    for i, df in enumerate(dataframes_normalizados):
        dataframes_normalizados[i] = df[columnas_ordenadas]
    
    # Concatenar y convertir columnas object con NA a nullable string cuando sea apropiado
    df_resultado = pd.concat(dataframes_normalizados, ignore_index=True)
    
    return df_resultado


def descargar_contratos_emprestito_all(
    limit: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Descarga y unifica datos de las tres colecciones de empréstito:
    - contratos_emprestito
    - ordenes_compra_emprestito
    - convenios_transferencias_emprestito
    
    Normaliza todos los datos al esquema estándar de contratos.
    Similar al endpoint GET /contratos_emprestito_all
    
    Args:
        limit: Límite de documentos a descargar por colección (None = todos)
        filters: Diccionario de filtros a aplicar a todas las colecciones
        
    Returns:
        DataFrame unificado con los datos de las tres colecciones en esquema estándar
        
    Raises:
        Exception: Si hay error conectando a Firebase o descargando datos
    """
    try:
        # Inicializar Firebase
        print(f"🔧 Inicializando conexión a Firebase...")
        initialize_firebase()
        
        print(f"\n📥 Descargando datos de las colecciones de empréstito...")
        print("=" * 70)
        
        # Descargar cada colección con su etiqueta
        colecciones = [
            ("contratos_emprestito", "contrato"),
            ("ordenes_compra_emprestito", "orden_compra"),
            ("convenios_transferencias_emprestito", "convenio_transferencia")
        ]
        
        dataframes = []
        conteos = {}
        campos_normalizados = {}
        
        for collection_name, tipo_registro in colecciones:
            df = descargar_coleccion(
                collection_name=collection_name,
                limit=limit,
                filters=filters,
                tipo_registro=tipo_registro
            )
            
            if not df.empty:
                dataframes.append(df)
                conteos[tipo_registro] = len(df)
                
                # Registrar campos normalizados
                if collection_name in MAPEO_CAMPOS:
                    mapeo = MAPEO_CAMPOS[collection_name]
                    campos_aplicados = [f"{orig}→{dest}" for orig, dest in mapeo.items() if orig in df.columns]
                    if campos_aplicados:
                        campos_normalizados[tipo_registro] = campos_aplicados
            else:
                conteos[tipo_registro] = 0
        
        print("=" * 70)
        
        if not dataframes:
            print("⚠️  No se encontraron datos en ninguna colección")
            return pd.DataFrame()
        
        # Unificar todos los DataFrames con el mismo esquema
        print(f"\n🔄 Unificando datos al esquema estándar...")
        df_unificado = unificar_esquema(dataframes)
        
        # Información del DataFrame unificado
        print(f"\n📊 Resumen de datos descargados:")
        print(f"   - Total registros: {len(df_unificado)}")
        print(f"   - Contratos: {conteos.get('contrato', 0)}")
        print(f"   - Órdenes de compra: {conteos.get('orden_compra', 0)}")
        print(f"   - Convenios/Transferencias: {conteos.get('convenio_transferencia', 0)}")
        print(f"   - Total columnas: {len(df_unificado.columns)}")
        
        # Mostrar campos normalizados
        if campos_normalizados:
            print(f"\n🔧 Campos normalizados al esquema estándar:")
            for tipo, campos in campos_normalizados.items():
                print(f"   - {tipo}:")
                for campo in campos[:5]:  # Mostrar máximo 5
                    print(f"     • {campo}")
                if len(campos) > 5:
                    print(f"     ... y {len(campos) - 5} más")
        
        # Mostrar columnas clave del esquema estándar
        columnas_clave = [
            'tipo_registro', 'referencia_contrato', 'banco', 
            'nombre_centro_gestor', 'valor_contrato', 'estado_contrato',
            'fecha_firma_contrato', 'objeto_contrato', 'contratista'
        ]
        columnas_presentes = [col for col in columnas_clave if col in df_unificado.columns]
        print(f"\n   - Campos estándar presentes: {', '.join(columnas_presentes)}")
        
        return df_unificado
        
    except Exception as e:
        print(f"❌ Error descargando datos: {e}")
        raise


def exportar_a_excel(
    df: pd.DataFrame,
    output_file: str = "emprestito_unificado.xlsx",
    include_timestamp: bool = True
) -> Path:
    """
    Exporta un DataFrame a un archivo Excel (.xlsx).
    
    Args:
        df: DataFrame a exportar
        output_file: Nombre del archivo de salida
        include_timestamp: Si True, agrega timestamp al nombre del archivo
        
    Returns:
        Path del archivo creado
        
    Raises:
        Exception: Si hay error al escribir el archivo
    """
    try:
        # Crear una copia del DataFrame para no modificar el original
        df_export = df.copy()
        
        # Convertir columnas datetime con timezone a timezone-naive
        # Excel no soporta datetimes con timezone
        print(f"\n🔧 Preparando datos para Excel...")
        for col in df_export.columns:
            if pd.api.types.is_datetime64tz_dtype(df_export[col]):
                print(f"   📅 Convirtiendo columna '{col}' (timezone-aware → naive)")
                df_export[col] = df_export[col].dt.tz_localize(None)
            elif pd.api.types.is_datetime64_dtype(df_export[col]):
                # Ya es timezone-naive, no hacer nada
                pass
            # Convertir objetos Timestamp de Firebase a datetime
            elif df_export[col].dtype == 'object':
                # Verificar si la columna contiene timestamps de Firebase
                sample = df_export[col].dropna().head(1)
                if not sample.empty:
                    first_val = sample.iloc[0]
                    # Verificar si es un objeto con método timestamp()
                    if hasattr(first_val, 'timestamp'):
                        try:
                            print(f"   🔥 Convirtiendo columna '{col}' (Firebase Timestamp → datetime)")
                            df_export[col] = pd.to_datetime(df_export[col], errors='coerce')
                            # Si quedó con timezone, removerlo
                            if pd.api.types.is_datetime64tz_dtype(df_export[col]):
                                df_export[col] = df_export[col].dt.tz_localize(None)
                        except Exception as e:
                            print(f"   ⚠️  No se pudo convertir columna '{col}': {e}")
        
        # Agregar timestamp al nombre si se solicita
        if include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = Path(output_file)
            output_file = f"{output_path.stem}_{timestamp}{output_path.suffix}"
        
        output_path = Path(output_file)
        
        print(f"\n💾 Exportando a Excel: {output_path.name}")
        
        # Crear el directorio si no existe
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Reorganizar columnas: columnas clave primero, luego el resto
        columnas_prioritarias = [
            'tipo_registro',
            'referencia_contrato', 
            'banco',
            'nombre_centro_gestor',
            'valor_contrato',
            'estado_contrato',
            'fecha_firma_contrato',
            'objeto_contrato',
            'modalidad_contratacion',
            'contratista',
            'nit_contratista',
            'fecha_fin_contrato',
            'referencia_proceso',
            'nombre_resumido_proceso',
            'entidad_contratante',
            'document_id'
        ]
        
        # Columnas que existen y están en la lista prioritaria
        cols_inicio = [col for col in columnas_prioritarias if col in df_export.columns]
        # Resto de columnas
        cols_resto = [col for col in df_export.columns if col not in cols_inicio]
        # Reorganizar
        df_export = df_export[cols_inicio + cols_resto]
        
        # Exportar a Excel con formato
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_export.to_excel(writer, sheet_name='Emprestito Unificado', index=False)
            
            # Obtener el workbook y worksheet para aplicar formato
            workbook = writer.book
            worksheet = writer.sheets['Emprestito Unificado']
            
            # Ajustar ancho de columnas
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)  # Máximo 50
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Aplicar filtros automáticos
            worksheet.auto_filter.ref = worksheet.dimensions
        
        # Verificar tamaño del archivo
        file_size = output_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"✅ Archivo Excel creado exitosamente")
        print(f"   📁 Ubicación: {output_path.absolute()}")
        print(f"   📏 Tamaño: {file_size_mb:.2f} MB")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Error exportando a Excel: {e}")
        raise


def main():
    """Función principal del script."""
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(
        description='Descargar datos unificados de empréstito de Firebase a archivo Excel',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
    # Descargar todas las colecciones (modo por defecto):
    python download_contratos_emprestito.py
    
    # Con archivo de salida personalizado:
    python download_contratos_emprestito.py --output emprestito_2024.xlsx
    
    # Limitar registros por colección:
    python download_contratos_emprestito.py --limit 50
    
    # Sin timestamp en el nombre:
    python download_contratos_emprestito.py --no-timestamp
        """
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='emprestito_unificado.xlsx',
        help='Nombre del archivo de salida (default: emprestito_unificado.xlsx)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Límite de documentos a descargar por colección (default: todos)'
    )
    
    parser.add_argument(
        '--no-timestamp',
        action='store_true',
        help='No agregar timestamp al nombre del archivo'
    )
    
    parser.add_argument(
        '--filter',
        type=str,
        action='append',
        help='Filtro en formato campo:valor (se puede usar múltiples veces)'
    )
    
    args = parser.parse_args()
    
    # Procesar filtros
    filters = None
    if args.filter:
        filters = {}
        for filter_str in args.filter:
            try:
                field, value = filter_str.split(':', 1)
                filters[field.strip()] = value.strip()
            except ValueError:
                print(f"⚠️  Filtro ignorado (formato incorrecto): {filter_str}")
                print("   Use formato: campo:valor")
    
    try:
        print("=" * 70)
        print("📥 DESCARGA UNIFICADA DE DATOS DE EMPRÉSTITO")
        print("=" * 70)
        print("\n📚 Colecciones incluidas:")
        print("   1. contratos_emprestito")
        print("   2. ordenes_compra_emprestito")
        print("   3. convenios_transferencias_emprestito")
        print("\n   Similar al endpoint: GET /contratos_emprestito_all")
        print("=" * 70)
        
        # Descargar datos unificados de Firebase
        df = descargar_contratos_emprestito_all(
            limit=args.limit,
            filters=filters
        )
        
        if df.empty:
            print("\n⚠️  No hay datos para exportar")
            return
        
        # Exportar a Excel
        output_path = exportar_a_excel(
            df=df,
            output_file=args.output,
            include_timestamp=not args.no_timestamp
        )
        
        print("\n" + "=" * 70)
        print("✅ PROCESO COMPLETADO EXITOSAMENTE")
        print("=" * 70)
        
        # Contar registros por tipo
        if 'tipo_registro' in df.columns:
            print(f"\n📊 Resumen detallado:")
            conteo_tipos = df['tipo_registro'].value_counts()
            for tipo, count in conteo_tipos.items():
                print(f"   - {tipo}: {count} registros")
            print(f"   - Total: {len(df)} registros")
        else:
            print(f"\n📊 Resumen:")
            print(f"   - Total registros: {len(df)}")
        
        print(f"   - Total columnas: {len(df.columns)}")
        print(f"   - Archivo: {output_path.name}")
        print(f"   - Ruta completa: {output_path.absolute()}")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"\n❌ Error en el proceso: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
