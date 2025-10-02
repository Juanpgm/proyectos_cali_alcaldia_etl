#!/usr/bin/env python3
"""
Script simple para verificar las hojas disponibles en el spreadsheet
"""

import sys
import os

# Agregar paths
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from database.config import get_sheets_client

SPREADSHEET_ID = "1n6I8lgJoXDjeg5um6B2uLIdGQDNnX2_WY0Kj5z5B-hs"

def main():
    print("Verificando hojas disponibles en el spreadsheet...")
    print(f"ID: {SPREADSHEET_ID}")
    
    try:
        # Obtener cliente
        client = get_sheets_client()
        if not client:
            print("❌ No se pudo obtener cliente de Google Sheets")
            return
        
        print("✅ Cliente obtenido exitosamente")
        
        # Abrir spreadsheet
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        print(f"✅ Spreadsheet abierto: '{spreadsheet.title}'")
        
        # Listar hojas
        worksheets = spreadsheet.worksheets()
        print(f"\nHojas disponibles ({len(worksheets)} total):")
        
        for i, worksheet in enumerate(worksheets):
            print(f"  {i+1}. '{worksheet.title}' (ID: {worksheet.id})")
            
            # Intentar obtener algunas filas para verificar acceso
            try:
                data = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
                print(f"      Filas: {len(data)}, Columnas: {len(data[0]) if data else 0}")
                
                if data and len(data) > 1:
                    print(f"      Headers: {data[0][:5]}...")  # Primeras 5 columnas
                    
            except Exception as e:
                print(f"      ❌ Error accediendo a datos: {e}")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()