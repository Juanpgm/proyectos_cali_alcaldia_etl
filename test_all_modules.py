#!/usr/bin/env python3
"""
Script final de verificación para todos los módulos de transformation_app
Ejecuta todos los módulos y reporta el estado de cada uno.
"""

import subprocess
import sys
import os
import time

def run_module(module_name):
    """Ejecuta un módulo y retorna el estado de éxito"""
    print(f"\n{'='*60}")
    print(f"🧪 EJECUTANDO: {module_name}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, module_name],
            capture_output=True,
            text=True,
            timeout=120  # 2 minutos timeout
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        if result.returncode == 0:
            print(f"✅ {module_name}: ÉXITO ({duration:.2f}s)")
            return True, duration, None
        else:
            print(f"❌ {module_name}: ERROR ({duration:.2f}s)")
            print(f"Error output: {result.stderr[:500]}...")
            return False, duration, result.stderr
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {module_name}: TIMEOUT (>120s)")
        return False, 120, "Timeout"
    except Exception as e:
        print(f"💥 {module_name}: EXCEPCIÓN - {e}")
        return False, 0, str(e)

def main():
    """Función principal"""
    print("🔬 VERIFICACIÓN FINAL DE TODOS LOS MÓDULOS")
    print("="*70)
    
    # Cambiar al directorio transformation_app
    os.chdir('transformation_app')
    
    # Lista de módulos a probar
    modules = [
        'data_transformation_procesos_secop.py',
        'data_transformation_contratos_secop.py',
        'data_transformation_ejecucion_presupuestal.py',
        'data_transformation_emprestito.py',
        'data_transformation_paa.py',
        'data_transformation_seguimiento_pa.py',
        'data_transformation_unidades_proyecto.py',
        'data_trasnformation_centros_gravedad.py'
    ]
    
    results = []
    total_time = 0
    
    # Ejecutar cada módulo
    for module in modules:
        success, duration, error = run_module(module)
        results.append({
            'module': module,
            'success': success,
            'duration': duration,
            'error': error
        })
        total_time += duration
        
        # Pausa breve entre módulos
        time.sleep(1)
    
    # Reporte final
    print(f"\n{'='*70}")
    print("📊 REPORTE FINAL")
    print(f"{'='*70}")
    
    successful = sum(1 for r in results if r['success'])
    total = len(results)
    
    print(f"Módulos exitosos: {successful}/{total}")
    print(f"Tiempo total: {total_time:.2f}s")
    print(f"Promedio por módulo: {total_time/total:.2f}s")
    
    print(f"\n📋 DETALLE POR MÓDULO:")
    for r in results:
        status = "✅ ÉXITO" if r['success'] else "❌ ERROR"
        duration_str = f"{r['duration']:.2f}s"
        print(f"  {r['module']:<45} {status:<8} {duration_str:>8}")
        if not r['success'] and r['error']:
            # Mostrar solo las primeras 100 caracteres del error
            error_preview = r['error'][:100] + "..." if len(r['error']) > 100 else r['error']
            print(f"    Error: {error_preview}")
    
    print(f"\n🎯 RESULTADO GENERAL:")
    if successful == total:
        print("🎉 ¡TODOS LOS MÓDULOS FUNCIONAN PERFECTAMENTE!")
        print("✅ El proyecto está completamente funcional sin FastAPI")
        print("✅ Todas las transformaciones de datos operan correctamente")
    elif successful >= total * 0.8:  # 80% o más
        print("✅ La mayoría de módulos funcionan correctamente")
        print(f"⚠️  {total - successful} módulo(s) con problemas menores")
    else:
        print("⚠️  Varios módulos tienen problemas")
        print("🔧 Se requiere revisión adicional")
    
    print(f"\n📝 PRÓXIMOS PASOS:")
    print("  1. Módulos funcionan independientemente")
    print("  2. Colocar archivos de datos en app_inputs/")
    print("  3. Ejecutar módulos según necesidad")
    print("  4. Revisar resultados en app_outputs/")

if __name__ == "__main__":
    main()
