"""
Prueba simple del modelo de características de proyectos.
"""

import json
from pathlib import Path

def test_model_creation():
    """Probar la creación del modelo."""
    print("🔄 Probando importación del modelo...")
    
    try:
        from database_management.core.models import CaracteristicasProyectos, Base
        print("✅ Modelo importado exitosamente")
        
        # Crear instancia de prueba
        proyecto = CaracteristicasProyectos(
            bpin=123456,
            bp="TEST001",
            nombre_proyecto="Proyecto de Prueba",
            nombre_actividad="Actividad de Prueba",
            programa_presupuestal="PROG001",
            nombre_centro_gestor="Centro Prueba",
            nombre_area_funcional="Área Prueba",
            nombre_fondo="Fondo Prueba",
            clasificacion_fondo="Clasificación Prueba",
            nombre_pospre="POSPRE Prueba",
            nombre_programa="Programa Prueba",
            comuna="Comuna 1",
            origen="Origen Prueba",
            anio=2024,
            tipo_gasto="Inversión"
        )
        
        print(f"✅ Instancia creada: BPIN {proyecto.bpin}")
        print(f"   Proyecto: {proyecto.nombre_proyecto}")
        
        # Probar método to_dict
        dict_result = proyecto.to_dict()
        print(f"✅ Método to_dict funciona: {len(dict_result)} campos")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_json_file():
    """Probar carga del archivo JSON."""
    print("\n🔄 Probando carga del archivo JSON...")
    
    json_file = Path("transformation_app/app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json")
    
    if not json_file.exists():
        print(f"❌ Archivo no encontrado: {json_file}")
        return False
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"✅ Archivo cargado: {len(data)} registros")
        
        if data:
            first_record = data[0]
            print(f"   Primer registro BPIN: {first_record.get('bpin')}")
            print(f"   Campos en primer registro: {len(first_record)}")
            
            # Mostrar algunos campos
            campos_importantes = ['bpin', 'bp', 'nombre_proyecto', 'anio', 'programa_presupuestal']
            for campo in campos_importantes:
                if campo in first_record:
                    valor = first_record[campo]
                    if isinstance(valor, str) and len(valor) > 50:
                        valor = valor[:50] + "..."
                    print(f"   {campo}: {valor}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error cargando JSON: {e}")
        return False

def main():
    """Función principal de prueba."""
    print("🚀 PRUEBA SIMPLE - SISTEMA CARACTERÍSTICAS DE PROYECTOS")
    print("=" * 60)
    
    # Probar modelo
    model_ok = test_model_creation()
    
    # Probar archivo JSON
    json_ok = test_json_file()
    
    # Resumen
    print("\n" + "=" * 60)
    print("📊 RESUMEN DE PRUEBAS")
    print("=" * 60)
    print(f"Modelo SQLAlchemy: {'✅ OK' if model_ok else '❌ Error'}")
    print(f"Archivo JSON: {'✅ OK' if json_ok else '❌ Error'}")
    
    if model_ok and json_ok:
        print("\n🎯 ¡COMPONENTES BÁSICOS FUNCIONANDO!")
        print("💡 Siguiente paso: Configurar base de datos y ejecutar carga")
        
        # Mostrar DDL generado si existe
        ddl_file = Path("caracteristicas_proyectos_ddl.sql")
        if ddl_file.exists():
            print(f"✅ Script DDL disponible: {ddl_file}")
        else:
            print("⚠️ Script DDL no encontrado")
            
        return True
    else:
        print("\n❌ Hay problemas con los componentes básicos")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)