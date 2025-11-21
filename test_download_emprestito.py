"""
Script de prueba para verificar la descarga y normalización de datos de empréstito.
Descarga una muestra pequeña y muestra información de normalización.
"""

import sys
from download_contratos_emprestito import (
    descargar_contratos_emprestito_all,
    MAPEO_CAMPOS
)


def test_descarga_muestra():
    """Prueba la descarga con límite pequeño."""
    print("=" * 80)
    print("🧪 PRUEBA DE DESCARGA Y NORMALIZACIÓN DE DATOS DE EMPRÉSTITO")
    print("=" * 80)
    
    try:
        # Descargar muestra pequeña (5 registros por colección)
        df = descargar_contratos_emprestito_all(limit=5)
        
        if df.empty:
            print("\n⚠️  No se descargaron datos")
            return
        
        print("\n" + "=" * 80)
        print("✅ PRUEBA EXITOSA")
        print("=" * 80)
        
        # Análisis de la normalización
        print(f"\n📊 Análisis de Normalización:")
        print(f"   Total filas: {len(df)}")
        print(f"   Total columnas: {len(df.columns)}")
        
        # Verificar campos clave del esquema estándar
        campos_estandar = [
            'tipo_registro',
            'referencia_contrato',
            'banco',
            'nombre_centro_gestor',
            'valor_contrato',
            'estado_contrato',
            'objeto_contrato',
            'contratista',
            'modalidad_contratacion'
        ]
        
        print(f"\n🔍 Verificación de campos estándar:")
        for campo in campos_estandar:
            if campo in df.columns:
                valores_no_nulos = df[campo].notna().sum()
                print(f"   ✅ {campo}: {valores_no_nulos}/{len(df)} valores")
            else:
                print(f"   ❌ {campo}: NO ENCONTRADO")
        
        # Distribución por tipo
        if 'tipo_registro' in df.columns:
            print(f"\n📈 Distribución por tipo:")
            for tipo, count in df['tipo_registro'].value_counts().items():
                print(f"   - {tipo}: {count} registros")
        
        # Verificar normalización específica por tipo
        print(f"\n🔄 Verificación de normalización:")
        
        if 'tipo_registro' in df.columns:
            # Verificar órdenes de compra
            ordenes = df[df['tipo_registro'] == 'orden_compra']
            if len(ordenes) > 0:
                print(f"\n   📦 Órdenes de compra ({len(ordenes)} registros):")
                # Verificar que los campos mapeados existan
                campos_verificar = ['referencia_contrato', 'valor_contrato', 'contratista']
                for campo in campos_verificar:
                    if campo in ordenes.columns:
                        valores = ordenes[campo].notna().sum()
                        print(f"      ✅ {campo}: {valores} valores")
                    else:
                        print(f"      ❌ {campo}: FALTA")
            
            # Verificar convenios
            convenios = df[df['tipo_registro'] == 'convenio_transferencia']
            if len(convenios) > 0:
                print(f"\n   📄 Convenios/Transferencias ({len(convenios)} registros):")
                campos_verificar = ['referencia_contrato', 'valor_contrato', 'banco']
                for campo in campos_verificar:
                    if campo in convenios.columns:
                        valores = convenios[campo].notna().sum()
                        print(f"      ✅ {campo}: {valores} valores")
                    else:
                        print(f"      ❌ {campo}: FALTA")
            
            # Verificar contratos
            contratos = df[df['tipo_registro'] == 'contrato']
            if len(contratos) > 0:
                print(f"\n   📝 Contratos ({len(contratos)} registros):")
                campos_verificar = ['referencia_contrato', 'valor_contrato', 'banco']
                for campo in campos_verificar:
                    if campo in contratos.columns:
                        valores = contratos[campo].notna().sum()
                        print(f"      ✅ {campo}: {valores} valores")
                    else:
                        print(f"      ❌ {campo}: FALTA")
        
        # Mostrar muestra de datos
        print(f"\n📋 Muestra de primeras filas (columnas clave):")
        columnas_muestra = [col for col in campos_estandar if col in df.columns][:6]
        print(df[columnas_muestra].head(3).to_string())
        
        print("\n" + "=" * 80)
        print("✅ Prueba completada exitosamente")
        print("=" * 80)
        
        return df
        
    except Exception as e:
        print(f"\n❌ Error en la prueba: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    df_resultado = test_descarga_muestra()
    
    if df_resultado is not None and not df_resultado.empty:
        print(f"\n💡 Tip: Los datos están listos para análisis")
        print(f"   Puedes ejecutar el script completo con:")
        print(f"   python download_contratos_emprestito.py")
        sys.exit(0)
    else:
        print(f"\n⚠️  La prueba no retornó datos")
        sys.exit(1)
