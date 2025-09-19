#!/usr/bin/env python3
"""
CORRECTOR FINAL DE DDL ULTRA CONSERVADOR
=======================================
Corrige específicamente los problemas detectados en el DDL generado.
"""

import re
from pathlib import Path

def fix_ddl_ultra_conservative():
    """Aplica correcciones ultra conservadoras al DDL"""
    
    sql_file = Path(__file__).parent.parent / "generated_sql" / "02_create_tables.sql"
    
    if not sql_file.exists():
        print(f"Error: No se encontró {sql_file}")
        return
    
    # Leer contenido
    content = sql_file.read_text(encoding='utf-8')
    
    print("Aplicando correcciones ultra conservadoras...")
    
    # Correcciones específicas por tabla
    corrections = [
        # 1. contratos_proyectos - campos problemáticos a TEXT
        ("contrato_puede_ser_prorrogado BOOLEAN", "contrato_puede_ser_prorrogado TEXT"),
        ("es_grupo BOOLEAN", "es_grupo TEXT"),
        ("es_pyme BOOLEAN", "es_pyme TEXT"),
        ("espostconflicto BOOLEAN", "espostconflicto TEXT"),
        ("liquidaci_n BOOLEAN", "liquidaci_n TEXT"),
        ("obligaciones_postconsumo BOOLEAN", "obligaciones_postconsumo TEXT"),
        ("obligaci_n_ambiental BOOLEAN", "obligaci_n_ambiental TEXT"),
        ("reversion BOOLEAN", "reversion TEXT"),
        
        # 2. datos_caracteristicos_proyectos - programa_presupuestal que causa error
        ("programa_presupuestal INTEGER", "programa_presupuestal TEXT"),
        
        # 3. movimientos_presupuestales - campos que pueden ser negativos → TEXT
        ("adiciones BIGINT", "adiciones TEXT"),
        ("contracreditos BIGINT", "contracreditos TEXT"),
        ("creditos BIGINT", "creditos TEXT"),
        ("reducciones INTEGER", "reducciones TEXT"),
        ("aplazamiento INTEGER", "aplazamiento TEXT"),
        ("desaplazamiento INTEGER", "desaplazamiento TEXT"),
        
        # 4. procesos_secop - campos NOT NULL → nullable
        ("codigoproveedor INTEGER", "codigoproveedor TEXT"),
        ("adjudicado BOOLEAN", "adjudicado TEXT"),
        
        # 5. Otros campos problemáticos detectados
        ("codigo_proveedor INTEGER", "codigo_proveedor TEXT"),
        ("nombre_ordenador_pago INTEGER", "nombre_ordenador_pago TEXT"),
        ("n_mero_documento_ordenador_pago INTEGER", "n_mero_documento_ordenador_pago TEXT"),
        ("tipo_documento_ordenador_pago INTEGER", "tipo_documento_ordenador_pago TEXT"),
        
        # 6. Campos que pueden tener valores como "No Definido"
        ("numero_contacto BIGINT", "numero_contacto TEXT"),
        ("valor_plataforma BIGINT", "valor_plataforma TEXT"),
        ("valor_total BIGINT", "valor_total TEXT"),
        ("precio_base BIGINT", "precio_base TEXT"),
        ("valor_total_adjudicacion BIGINT", "valor_total_adjudicacion TEXT"),
        
        # 7. NUEVAS CORRECCIONES basadas en errores detectados
        ("recursos_credito INTEGER", "recursos_credito TEXT"),
        ("sistema_general_regal_as INTEGER", "sistema_general_regal_as TEXT"),
        ("valor_amortizado INTEGER", "valor_amortizado TEXT"),
        ("valor_facturado INTEGER", "valor_facturado TEXT"),
        ("valor_pagado INTEGER", "valor_pagado TEXT"),
        ("valor_pago_adelantado INTEGER", "valor_pago_adelantado TEXT"),
        ("valor_pendiente_amortizacion INTEGER", "valor_pendiente_amortizacion TEXT"),
        ("habilita_pago_adelantado INTEGER", "habilita_pago_adelantado TEXT"),
        ("dias_adicionados INTEGER", "dias_adicionados TEXT"),
        ("anno_bpin INTEGER", "anno_bpin TEXT"),
        ("anio INTEGER", "anio TEXT"),
        
        # 8. Campos DECIMAL/NUMERIC que pueden tener formato con comas
        ("duraci_n_proceso DECIMAL(18,4)", "duraci_n_proceso TEXT"),
        ("precio_base DECIMAL(18,4)", "precio_base TEXT"),
        ("presupuesto_estimado DECIMAL(18,4)", "presupuesto_estimado TEXT"),
    ]
    
    # Aplicar correcciones
    for old_pattern, new_pattern in corrections:
        if old_pattern in content:
            content = content.replace(old_pattern, new_pattern)
            print(f"  ✓ Corregido: {old_pattern} → {new_pattern}")
    
    # Escribir contenido corregido
    sql_file.write_text(content, encoding='utf-8')
    print(f"\n✅ DDL corregido guardado en: {sql_file}")
    print("🔧 Aplicadas todas las correcciones ultra conservadoras")

if __name__ == "__main__":
    fix_ddl_ultra_conservative()