#!/usr/bin/env python
"""
Script de prueba para investigar el dataset de contratos electrónicos
y encontrar referencias de contrato disponibles
"""

import pandas as pd
from sodapy import Socrata
import json

# Configuración
SECOP_DOMAIN = "www.datos.gov.co"
CONTRATOS_ELECTRONICOS_DATASET_ID = "jbjy-vk9h"
NIT_ENTIDAD_CALI = "890399011"

def test_contratos_electronicos():
    """Prueba el dataset de contratos electrónicos para ver qué datos están disponibles."""
    try:
        print("🔍 Investigando dataset de contratos electrónicos...")
        
        # Crear cliente
        client = Socrata(SECOP_DOMAIN, None)
        
        # Primero, obtener una muestra general del dataset
        print("📊 Obteniendo muestra general del dataset...")
        sample_results = client.get(CONTRATOS_ELECTRONICOS_DATASET_ID, limit=10)
        
        if sample_results:
            print(f"✅ Dataset accesible. Muestra de {len(sample_results)} registros obtenida")
            
            # Mostrar estructura de un registro
            print("\n📋 Estructura de un registro de ejemplo:")
            sample_df = pd.DataFrame.from_records(sample_results)
            print(f"Columnas disponibles ({len(sample_df.columns)}):")
            for i, col in enumerate(sample_df.columns, 1):
                print(f"  {i:2d}. {col}")
            
            # Mostrar algunos valores de referencia_del_contrato si existe
            if 'referencia_del_contrato' in sample_df.columns:
                print(f"\n🎯 Ejemplos de 'referencia_del_contrato':")
                refs = sample_df['referencia_del_contrato'].dropna().head(5)
                for i, ref in enumerate(refs, 1):
                    print(f"  {i}. {ref}")
            
            # Verificar si hay registros para el NIT de Cali
            print(f"\n🏛️ Buscando registros para NIT Cali ({NIT_ENTIDAD_CALI})...")
            
            # Buscar por diferentes campos posibles que contengan el NIT
            nit_fields_to_check = ['nit_entidad', 'nit_contratante', 'nit_entidad_estatal']
            
            for field in nit_fields_to_check:
                if field in sample_df.columns:
                    print(f"  Probando campo: {field}")
                    cali_results = client.get(
                        CONTRATOS_ELECTRONICOS_DATASET_ID, 
                        where=f"{field}='{NIT_ENTIDAD_CALI}'",
                        limit=5
                    )
                    
                    if cali_results:
                        print(f"    ✅ Encontrados {len(cali_results)} registros con {field}={NIT_ENTIDAD_CALI}")
                        cali_df = pd.DataFrame.from_records(cali_results)
                        if 'referencia_del_contrato' in cali_df.columns:
                            refs = cali_df['referencia_del_contrato'].dropna().head(3)
                            print(f"    📋 Referencias encontradas:")
                            for ref in refs:
                                print(f"      - {ref}")
                    else:
                        print(f"    ❌ No se encontraron registros con {field}={NIT_ENTIDAD_CALI}")
            
            # Buscar sin filtro de NIT para ver si hay datos recientes
            print(f"\n📅 Buscando registros recientes del dataset...")
            recent_results = client.get(
                CONTRATOS_ELECTRONICOS_DATASET_ID,
                order=":updated_at DESC",
                limit=5
            )
            
            if recent_results:
                recent_df = pd.DataFrame.from_records(recent_results)
                print(f"✅ Encontrados {len(recent_results)} registros recientes")
                if 'referencia_del_contrato' in recent_df.columns:
                    refs = recent_df['referencia_del_contrato'].dropna().head(3)
                    print(f"📋 Referencias recientes:")
                    for ref in refs:
                        print(f"  - {ref}")
        
        else:
            print("❌ No se pudieron obtener datos del dataset")
            
        client.close()
        
    except Exception as e:
        print(f"❌ Error investigando dataset: {e}")

def check_specific_references():
    """Verificar si las referencias específicas del empréstito existen en el dataset."""
    try:
        print("\n" + "="*60)
        print("🎯 Verificando referencias específicas del empréstito")
        print("="*60)
        
        # Cargar referencias del archivo JSON
        referencias_file = "transformation_app/app_outputs/emprestito_outputs/emp_procesos_index.json"
        with open(referencias_file, 'r', encoding='utf-8') as f:
            referencias_data = json.load(f)
        
        target_references = [
            item['referencia_proceso'].strip() 
            for item in referencias_data 
            if 'referencia_proceso' in item and item['referencia_proceso']
        ]
        
        print(f"📋 Verificando {len(target_references)} referencias específicas...")
        
        client = Socrata(SECOP_DOMAIN, None)
        
        # Buscar cada referencia sin filtrar por NIT primero
        found_contracts = 0
        for i, ref in enumerate(target_references[:5], 1):  # Solo las primeras 5 para prueba
            print(f"\n{i}. Buscando: {ref}")
            
            # Buscar sin filtro de NIT
            results = client.get(
                CONTRATOS_ELECTRONICOS_DATASET_ID,
                where=f"referencia_del_contrato='{ref}'",
                limit=10
            )
            
            if results:
                found_contracts += len(results)
                print(f"   ✅ Encontrados {len(results)} contratos")
                df = pd.DataFrame.from_records(results)
                
                # Mostrar información del contratante si está disponible
                nit_fields = ['nit_entidad', 'nit_contratante', 'nit_entidad_estatal']
                for field in nit_fields:
                    if field in df.columns and not df[field].isna().all():
                        nits = df[field].dropna().unique()
                        print(f"   📋 {field}: {list(nits)}")
                        break
            else:
                print(f"   ❌ No encontrado")
        
        print(f"\n📊 Resumen: {found_contracts} contratos encontrados para las primeras 5 referencias")
        client.close()
        
    except Exception as e:
        print(f"❌ Error verificando referencias específicas: {e}")

if __name__ == "__main__":
    test_contratos_electronicos()
    check_specific_references()