"""
Script para convertir archivos .xlsx a .json
Convierte todos los archivos Excel del directorio app_inputs/unidades_proyecto_input/defaults
"""
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Union


def convert_xlsx_to_json(
    input_dir: str = "app_inputs/unidades_proyecto_input/defaults",
    output_dir: str = None,
    unique_values: bool = True
) -> Dict[str, str]:
    """
    Convierte todos los archivos .xlsx de un directorio a formato .json
    
    Args:
        input_dir: Directorio de entrada con archivos .xlsx
        output_dir: Directorio de salida para archivos .json (si es None, usa el mismo que input_dir)
        unique_values: Si True, extrae solo valores únicos por columna (por defecto: True)
    
    Returns:
        Diccionario con nombres de archivos procesados y sus rutas de salida
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir else input_path
    
    # Crear directorio de salida si no existe
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Buscar todos los archivos .xlsx
    xlsx_files = list(input_path.glob("*.xlsx"))
    
    if not xlsx_files:
        print(f"No se encontraron archivos .xlsx en {input_dir}")
        return {}
    
    results = {}
    
    for xlsx_file in xlsx_files:
        try:
            print(f"Procesando: {xlsx_file.name}")
            
            # Leer el archivo Excel
            xls = pd.ExcelFile(xlsx_file)
            
            # Si el archivo tiene múltiples hojas
            if len(xls.sheet_names) > 1:
                # Crear un diccionario con todas las hojas
                data = {}
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xlsx_file, sheet_name=sheet_name)
                    
                    if unique_values:
                        # Extraer valores únicos por columna
                        sheet_data = {}
                        for column in df.columns:
                            unique_vals = df[column].dropna().unique().tolist()
                            # Ordenar si son strings o números
                            try:
                                unique_vals = sorted(unique_vals)
                            except:
                                pass  # Mantener orden original si no se puede ordenar
                            sheet_data[column] = unique_vals
                        data[sheet_name] = sheet_data
                    else:
                        # Reemplazar NaN con None para mejor manejo en JSON
                        df = df.where(pd.notnull(df), None)
                        data[sheet_name] = df.to_dict(orient='records')
            else:
                # Si solo hay una hoja
                df = pd.read_excel(xlsx_file, sheet_name=0)
                
                if unique_values:
                    # Extraer valores únicos por columna
                    data = {}
                    for column in df.columns:
                        unique_vals = df[column].dropna().unique().tolist()
                        # Ordenar si son strings o números
                        try:
                            unique_vals = sorted(unique_vals)
                        except:
                            pass  # Mantener orden original si no se puede ordenar
                        data[column] = unique_vals
                    
                    # Agregar metadatos
                    total_rows = len(df)
                    print(f"    Columnas procesadas: {len(data)}")
                    for col, vals in data.items():
                        print(f"      - {col}: {len(vals)} valores únicos (de {total_rows} filas)")
                else:
                    # Reemplazar NaN con None para mejor manejo en JSON
                    df = df.where(pd.notnull(df), None)
                    data = df.to_dict(orient='records')
            
            # Crear nombre del archivo JSON
            json_filename = xlsx_file.stem + ".json"
            json_path = output_path / json_filename
            
            # Guardar como JSON
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            results[xlsx_file.name] = str(json_path)
            print(f"  ✓ Convertido a: {json_filename}")
            
        except Exception as e:
            print(f"  ✗ Error al procesar {xlsx_file.name}: {str(e)}")
            results[xlsx_file.name] = f"Error: {str(e)}"
    
    return results


def main():
    """Función principal para ejecutar la conversión"""
    print("=" * 70)
    print("Conversión de archivos XLSX a JSON")
    print("=" * 70)
    
    # Ejecutar conversión
    results = convert_xlsx_to_json()
    
    # Mostrar resumen
    print("\n" + "=" * 70)
    print("RESUMEN")
    print("=" * 70)
    successful = sum(1 for v in results.values() if not v.startswith("Error"))
    failed = len(results) - successful
    
    print(f"Total archivos procesados: {len(results)}")
    print(f"Exitosos: {successful}")
    print(f"Fallidos: {failed}")
    
    if failed > 0:
        print("\nArchivos con errores:")
        for filename, result in results.items():
            if result.startswith("Error"):
                print(f"  - {filename}: {result}")


if __name__ == "__main__":
    main()
