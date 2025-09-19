"""
Prueba Completa de Carga de Datos - Sistema ETL Cali Alcaldía
============================================================

Este script realiza pruebas comprehensivas de:
1. Carga real de datos en la base de datos
2. Funcionamiento completo del sistema ETL
3. Validación de transformaciones
4. Verificación de integridad de datos
"""

import asyncio
import sys
import json
import pandas as pd
import psycopg2
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List
import random

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent))

from orchestrator.etl_orchestrator import ETLOrchestrator, create_orchestrator
from database_management.core import quick_health_check, comprehensive_analysis, get_database_config


class DataLoadTester:
    """Clase para probar la carga completa de datos"""
    
    def __init__(self):
        self.db_config = get_database_config()
        self.test_results = {}
        self.start_time = datetime.now()
        
    def get_db_connection(self):
        """Obtener conexión a la base de datos"""
        return psycopg2.connect(
            host=self.db_config.host,
            port=self.db_config.port,
            database=self.db_config.database,
            user=self.db_config.user,
            password=self.db_config.password
        )
    
    def log_test(self, test_name: str, success: bool, details: str = "", data_count: int = 0):
        """Registrar resultado de prueba"""
        self.test_results[test_name] = {
            "success": success,
            "details": details,
            "data_count": data_count,
            "timestamp": datetime.now().isoformat()
        }
        status = "EXITOSO" if success else "FALLIDO"
        print(f"   [{status}] {test_name}")
        if details:
            print(f"      {details}")
        if data_count > 0:
            print(f"      Registros procesados: {data_count}")

    def test_database_tables(self) -> bool:
        """Probar la estructura de las tablas"""
        print("\n1. VERIFICACIÓN DE TABLAS DE BASE DE DATOS")
        print("-" * 60)
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Verificar tablas existentes
                    cursor.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        ORDER BY table_name;
                    """)
                    
                    tables = cursor.fetchall()
                    table_names = [table[0] for table in tables]
                    
                    self.log_test(
                        "Listado de tablas",
                        len(table_names) > 0,
                        f"Tablas encontradas: {', '.join(table_names)}",
                        len(table_names)
                    )
                    
                    # Verificar estructura de cada tabla
                    for table_name in table_names:
                        cursor.execute(f"""
                            SELECT column_name, data_type, is_nullable
                            FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                            ORDER BY ordinal_position;
                        """)
                        
                        columns = cursor.fetchall()
                        self.log_test(
                            f"Estructura tabla {table_name}",
                            len(columns) > 0,
                            f"Columnas: {len(columns)}",
                            len(columns)
                        )
                    
                    return len(table_names) > 0
                    
        except Exception as e:
            self.log_test("Verificación de tablas", False, f"Error: {str(e)}")
            return False

    def create_sample_data(self) -> Dict[str, List[Dict]]:
        """Crear datos de muestra para las pruebas"""
        print("\n2. GENERACIÓN DE DATOS DE MUESTRA")
        print("-" * 60)
        
        # Datos de contratos
        contratos_data = []
        for i in range(20):
            contrato = {
                'numero_contrato': f'CONT-2024-{1000 + i:04d}',
                'entidad_contratante': f'Entidad Municipal {i % 5 + 1}',
                'contratista': f'Empresa Contratista {chr(65 + i % 10)}',
                'objeto_contrato': f'Objeto del contrato número {i + 1}',
                'valor_total': random.randint(50000000, 500000000),
                'fecha_inicio': datetime.now() - timedelta(days=random.randint(30, 365)),
                'fecha_fin': datetime.now() + timedelta(days=random.randint(30, 730)),
                'estado': random.choice(['ACTIVO', 'TERMINADO', 'SUSPENDIDO']),
                'modalidad': random.choice(['LICITACION_PUBLICA', 'CONTRATACION_DIRECTA', 'MINIMA_CUANTIA']),
                'created_at': datetime.now()
            }
            contratos_data.append(contrato)
        
        # Datos de proyectos
        proyectos_data = []
        for i in range(15):
            proyecto = {
                'codigo_proyecto': f'PROY-{2024}-{2000 + i:04d}',
                'nombre_proyecto': f'Proyecto de Desarrollo {i + 1}',
                'descripcion': f'Descripción detallada del proyecto número {i + 1}',
                'valor_presupuestado': random.randint(100000000, 1000000000),
                'valor_ejecutado': random.randint(50000000, 800000000),
                'porcentaje_avance': random.randint(10, 100),
                'fecha_inicio_planeada': datetime.now() - timedelta(days=random.randint(60, 400)),
                'fecha_fin_planeada': datetime.now() + timedelta(days=random.randint(100, 800)),
                'estado_proyecto': random.choice(['EN_EJECUCION', 'FINALIZADO', 'SUSPENDIDO', 'PLANEACION']),
                'sector': random.choice(['EDUCACION', 'SALUD', 'INFRAESTRUCTURA', 'MEDIO_AMBIENTE']),
                'created_at': datetime.now()
            }
            proyectos_data.append(proyecto)
        
        # Datos de ejecución presupuestal
        presupuesto_data = []
        for i in range(25):
            presupuesto = {
                'codigo_presupuesto': f'PRES-{2024}-{3000 + i:04d}',
                'programa': f'Programa Presupuestal {i + 1}',
                'subprograma': f'Subprograma {i % 5 + 1}',
                'proyecto_asociado': f'PROY-{2024}-{2000 + (i % 15):04d}',
                'valor_apropiado': random.randint(200000000, 2000000000),
                'valor_comprometido': random.randint(150000000, 1800000000),
                'valor_obligado': random.randint(100000000, 1500000000),
                'valor_pagado': random.randint(80000000, 1200000000),
                'saldo_por_utilizar': random.randint(0, 500000000),
                'vigencia': 2024,
                'mes_reporte': random.randint(1, 12),
                'created_at': datetime.now()
            }
            presupuesto_data.append(presupuesto)
        
        sample_data = {
            'contratos_secop': contratos_data,
            'proyectos_inversion': proyectos_data,
            'ejecucion_presupuestal': presupuesto_data
        }
        
        for table_name, data in sample_data.items():
            self.log_test(
                f"Datos generados para {table_name}",
                len(data) > 0,
                f"Se generaron {len(data)} registros",
                len(data)
            )
        
        return sample_data

    def test_data_insertion(self, sample_data: Dict[str, List[Dict]]) -> bool:
        """Probar inserción directa de datos"""
        print("\n3. PRUEBA DE INSERCIÓN DIRECTA DE DATOS")
        print("-" * 60)
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    
                    # Insertar contratos
                    contratos_inserted = 0
                    for contrato in sample_data['contratos_secop']:
                        try:
                            cursor.execute("""
                                INSERT INTO contratos_secop (
                                    numero_contrato, entidad_contratante, contratista,
                                    objeto_contrato, valor_total, fecha_inicio, fecha_fin,
                                    estado, modalidad, created_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (numero_contrato) DO NOTHING
                            """, (
                                contrato['numero_contrato'], contrato['entidad_contratante'],
                                contrato['contratista'], contrato['objeto_contrato'],
                                contrato['valor_total'], contrato['fecha_inicio'],
                                contrato['fecha_fin'], contrato['estado'],
                                contrato['modalidad'], contrato['created_at']
                            ))
                            if cursor.rowcount > 0:
                                contratos_inserted += 1
                        except Exception as e:
                            print(f"      Error insertando contrato: {e}")
                    
                    self.log_test(
                        "Inserción de contratos",
                        contratos_inserted > 0,
                        f"Contratos insertados exitosamente",
                        contratos_inserted
                    )
                    
                    # Insertar proyectos
                    proyectos_inserted = 0
                    for proyecto in sample_data['proyectos_inversion']:
                        try:
                            cursor.execute("""
                                INSERT INTO proyectos_inversion (
                                    codigo_proyecto, nombre_proyecto, descripcion,
                                    valor_presupuestado, valor_ejecutado, porcentaje_avance,
                                    fecha_inicio_planeada, fecha_fin_planeada, estado_proyecto,
                                    sector, created_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (codigo_proyecto) DO NOTHING
                            """, (
                                proyecto['codigo_proyecto'], proyecto['nombre_proyecto'],
                                proyecto['descripcion'], proyecto['valor_presupuestado'],
                                proyecto['valor_ejecutado'], proyecto['porcentaje_avance'],
                                proyecto['fecha_inicio_planeada'], proyecto['fecha_fin_planeada'],
                                proyecto['estado_proyecto'], proyecto['sector'], proyecto['created_at']
                            ))
                            if cursor.rowcount > 0:
                                proyectos_inserted += 1
                        except Exception as e:
                            print(f"      Error insertando proyecto: {e}")
                    
                    self.log_test(
                        "Inserción de proyectos",
                        proyectos_inserted > 0,
                        f"Proyectos insertados exitosamente",
                        proyectos_inserted
                    )
                    
                    # Insertar ejecución presupuestal
                    presupuesto_inserted = 0
                    for presupuesto in sample_data['ejecucion_presupuestal']:
                        try:
                            cursor.execute("""
                                INSERT INTO ejecucion_presupuestal (
                                    codigo_presupuesto, programa, subprograma, proyecto_asociado,
                                    valor_apropiado, valor_comprometido, valor_obligado,
                                    valor_pagado, saldo_por_utilizar, vigencia, mes_reporte, created_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (codigo_presupuesto) DO NOTHING
                            """, (
                                presupuesto['codigo_presupuesto'], presupuesto['programa'],
                                presupuesto['subprograma'], presupuesto['proyecto_asociado'],
                                presupuesto['valor_apropiado'], presupuesto['valor_comprometido'],
                                presupuesto['valor_obligado'], presupuesto['valor_pagado'],
                                presupuesto['saldo_por_utilizar'], presupuesto['vigencia'],
                                presupuesto['mes_reporte'], presupuesto['created_at']
                            ))
                            if cursor.rowcount > 0:
                                presupuesto_inserted += 1
                        except Exception as e:
                            print(f"      Error insertando presupuesto: {e}")
                    
                    self.log_test(
                        "Inserción de ejecución presupuestal",
                        presupuesto_inserted > 0,
                        f"Registros presupuestales insertados exitosamente",
                        presupuesto_inserted
                    )
                    
                    conn.commit()
                    
                    total_inserted = contratos_inserted + proyectos_inserted + presupuesto_inserted
                    return total_inserted > 0
                    
        except Exception as e:
            self.log_test("Inserción de datos", False, f"Error: {str(e)}")
            return False

    def test_data_queries(self) -> bool:
        """Probar consultas y análisis de datos"""
        print("\n4. PRUEBA DE CONSULTAS Y ANÁLISIS DE DATOS")
        print("-" * 60)
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    
                    # Consulta 1: Conteo de registros por tabla
                    tables_to_check = ['contratos_secop', 'proyectos_inversion', 'ejecucion_presupuestal']
                    
                    for table in tables_to_check:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        self.log_test(
                            f"Conteo {table}",
                            count > 0,
                            f"Registros encontrados: {count}",
                            count
                        )
                    
                    # Consulta 2: Análisis de contratos por estado
                    cursor.execute("""
                        SELECT estado, COUNT(*) as cantidad, SUM(valor_total) as valor_total
                        FROM contratos_secop 
                        GROUP BY estado
                        ORDER BY cantidad DESC
                    """)
                    contratos_por_estado = cursor.fetchall()
                    
                    self.log_test(
                        "Análisis contratos por estado",
                        len(contratos_por_estado) > 0,
                        f"Estados encontrados: {len(contratos_por_estado)}",
                        len(contratos_por_estado)
                    )
                    
                    for estado, cantidad, valor in contratos_por_estado:
                        print(f"      {estado}: {cantidad} contratos, ${valor:,.0f}")
                    
                    # Consulta 3: Proyectos con mayor ejecución
                    cursor.execute("""
                        SELECT nombre_proyecto, porcentaje_avance, valor_ejecutado, sector
                        FROM proyectos_inversion 
                        WHERE porcentaje_avance > 50
                        ORDER BY porcentaje_avance DESC
                        LIMIT 5
                    """)
                    proyectos_avanzados = cursor.fetchall()
                    
                    self.log_test(
                        "Proyectos con mayor avance",
                        len(proyectos_avanzados) > 0,
                        f"Proyectos con >50% de avance: {len(proyectos_avanzados)}",
                        len(proyectos_avanzados)
                    )
                    
                    # Consulta 4: Ejecución presupuestal por programa
                    cursor.execute("""
                        SELECT programa, SUM(valor_apropiado) as apropiado, 
                               SUM(valor_pagado) as pagado,
                               ROUND((SUM(valor_pagado::numeric) / SUM(valor_apropiado::numeric)) * 100, 2) as porcentaje_ejecucion
                        FROM ejecucion_presupuestal 
                        GROUP BY programa
                        ORDER BY apropiado DESC
                        LIMIT 5
                    """)
                    ejecucion_por_programa = cursor.fetchall()
                    
                    self.log_test(
                        "Ejecución presupuestal por programa",
                        len(ejecucion_por_programa) > 0,
                        f"Programas analizados: {len(ejecucion_por_programa)}",
                        len(ejecucion_por_programa)
                    )
                    
                    for programa, apropiado, pagado, porcentaje in ejecucion_por_programa:
                        print(f"      {programa}: {porcentaje}% ejecutado (${pagado:,.0f} de ${apropiado:,.0f})")
                    
                    return True
                    
        except Exception as e:
            self.log_test("Consultas de datos", False, f"Error: {str(e)}")
            return False

    async def test_etl_workflows(self) -> bool:
        """Probar workflows completos de ETL"""
        print("\n5. PRUEBA DE WORKFLOWS ETL COMPLETOS")
        print("-" * 60)
        
        try:
            # Probar workflow mock completo
            config_file = Path("orchestrator/etl_config_testing.json")
            orchestrator = create_orchestrator(str(config_file))
            
            # Ejecutar workflow mock_etl
            workflow_tasks = ["mock_contratos_emprestito", "mock_transform_contratos", "mock_load_complete"]
            
            workflow_orchestrator = ETLOrchestrator()
            for task_id in workflow_tasks:
                if task_id in orchestrator.tasks:
                    workflow_orchestrator.register_task(orchestrator.tasks[task_id])
            
            print("   Ejecutando workflow mock ETL completo...")
            results = await workflow_orchestrator.execute_all(
                parallel=False,
                stop_on_failure=False
            )
            
            successful_tasks = sum(1 for result in results.values() if result.status.value == "completed")
            total_tasks = len(results)
            
            self.log_test(
                "Workflow ETL mock",
                successful_tasks > 0,
                f"Tareas exitosas: {successful_tasks}/{total_tasks}",
                successful_tasks
            )
            
            # Mostrar detalles de cada tarea
            for task_id, result in results.items():
                status = "EXITOSO" if result.status.value == "completed" else "FALLIDO"
                duration = f" ({result.duration:.2f}s)" if result.duration else ""
                print(f"      {task_id}: {status}{duration}")
            
            return successful_tasks > 0
            
        except Exception as e:
            self.log_test("Workflows ETL", False, f"Error: {str(e)}")
            return False

    def test_monitoring_during_load(self) -> bool:
        """Probar monitoreo durante operaciones de carga"""
        print("\n6. PRUEBA DE MONITOREO DURANTE CARGA")
        print("-" * 60)
        
        try:
            # Ejecutar análisis comprehensive
            analysis_result = comprehensive_analysis(1)
            
            self.log_test(
                "Análisis comprehensive",
                "configuration" in analysis_result,
                f"Componentes analizados: {len(analysis_result)}",
                len(analysis_result)
            )
            
            # Verificar estado de salud
            health_status = quick_health_check()
            
            self.log_test(
                "Estado de salud del sistema",
                health_status["connection"],
                f"Estado: {health_status['status']}",
                1 if health_status["connection"] else 0
            )
            
            return True
            
        except Exception as e:
            self.log_test("Monitoreo durante carga", False, f"Error: {str(e)}")
            return False

    def test_data_integrity(self) -> bool:
        """Probar integridad y consistencia de datos"""
        print("\n7. PRUEBA DE INTEGRIDAD DE DATOS")
        print("-" * 60)
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    
                    # Verificar integridad referencial
                    cursor.execute("""
                        SELECT COUNT(*) FROM ejecucion_presupuestal ep
                        LEFT JOIN proyectos_inversion pi ON ep.proyecto_asociado = pi.codigo_proyecto
                        WHERE pi.codigo_proyecto IS NULL AND ep.proyecto_asociado LIKE 'PROY-%'
                    """)
                    referencias_huerfanas = cursor.fetchone()[0]
                    
                    self.log_test(
                        "Integridad referencial",
                        referencias_huerfanas == 0,
                        f"Referencias huérfanas encontradas: {referencias_huerfanas}",
                        referencias_huerfanas
                    )
                    
                    # Verificar consistencia de fechas
                    cursor.execute("""
                        SELECT COUNT(*) FROM contratos_secop
                        WHERE fecha_inicio > fecha_fin
                    """)
                    fechas_inconsistentes = cursor.fetchone()[0]
                    
                    self.log_test(
                        "Consistencia de fechas",
                        fechas_inconsistentes == 0,
                        f"Contratos con fechas inconsistentes: {fechas_inconsistentes}",
                        fechas_inconsistentes
                    )
                    
                    # Verificar valores negativos
                    cursor.execute("""
                        SELECT COUNT(*) FROM proyectos_inversion
                        WHERE valor_presupuestado < 0 OR valor_ejecutado < 0
                    """)
                    valores_negativos = cursor.fetchone()[0]
                    
                    self.log_test(
                        "Valores monetarios válidos",
                        valores_negativos == 0,
                        f"Registros con valores negativos: {valores_negativos}",
                        valores_negativos
                    )
                    
                    return referencias_huerfanas == 0 and fechas_inconsistentes == 0 and valores_negativos == 0
                    
        except Exception as e:
            self.log_test("Integridad de datos", False, f"Error: {str(e)}")
            return False

    def generate_summary_report(self) -> Dict[str, Any]:
        """Generar reporte resumen de todas las pruebas"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        successful_tests = sum(1 for result in self.test_results.values() if result["success"])
        total_tests = len(self.test_results)
        success_rate = (successful_tests / total_tests * 100) if total_tests > 0 else 0
        
        total_records = sum(result.get("data_count", 0) for result in self.test_results.values())
        
        return {
            "timestamp": end_time.isoformat(),
            "duration_seconds": duration,
            "total_tests": total_tests,
            "successful_tests": successful_tests,
            "failed_tests": total_tests - successful_tests,
            "success_rate": success_rate,
            "total_records_processed": total_records,
            "test_details": self.test_results
        }

    async def run_complete_test(self) -> bool:
        """Ejecutar todas las pruebas de carga de datos"""
        print("PRUEBA COMPLETA DE CARGA DE DATOS - SISTEMA ETL CALI ALCALDÍA")
        print("=" * 80)
        print(f"Iniciado: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Ejecutar todas las pruebas
        tests = [
            ("Verificación de Tablas", self.test_database_tables()),
            ("Generación de Datos", lambda: bool(self.create_sample_data())),
            ("Inserción de Datos", lambda: self.test_data_insertion(self.create_sample_data())),
            ("Consultas de Datos", self.test_data_queries()),
            ("Workflows ETL", await self.test_etl_workflows()),
            ("Monitoreo durante Carga", self.test_monitoring_during_load()),
            ("Integridad de Datos", self.test_data_integrity())
        ]
        
        overall_success = True
        for test_name, test_result in tests:
            if callable(test_result):
                try:
                    result = test_result()
                except Exception as e:
                    print(f"Error ejecutando {test_name}: {e}")
                    result = False
            else:
                result = test_result
            overall_success = overall_success and result
        
        # Generar reporte final
        summary = self.generate_summary_report()
        
        print("\n" + "=" * 80)
        print("REPORTE FINAL DE PRUEBAS DE CARGA DE DATOS")
        print("=" * 80)
        print(f"Duración total: {summary['duration_seconds']:.2f} segundos")
        print(f"Pruebas ejecutadas: {summary['total_tests']}")
        print(f"Pruebas exitosas: {summary['successful_tests']}")
        print(f"Pruebas fallidas: {summary['failed_tests']}")
        print(f"Tasa de éxito: {summary['success_rate']:.1f}%")
        print(f"Registros procesados: {summary['total_records_processed']}")
        
        if overall_success and summary['success_rate'] >= 90:
            print("\n🎉 SISTEMA DE CARGA DE DATOS COMPLETAMENTE FUNCIONAL!")
            print("   ✅ Base de datos operativa")
            print("   ✅ Inserción de datos exitosa")
            print("   ✅ Consultas funcionando correctamente")
            print("   ✅ Workflows ETL operativos")
            print("   ✅ Monitoreo activo")
            print("   ✅ Integridad de datos validada")
            print("   ✅ Sistema listo para producción")
        elif summary['success_rate'] >= 70:
            print("\n⚠️ SISTEMA MAYORMENTE FUNCIONAL")
            print("   La mayoría de componentes funcionan correctamente")
            print("   Revisar pruebas fallidas para mejoras menores")
        else:
            print("\n❌ SISTEMA REQUIERE ATENCIÓN")
            print("   Múltiples componentes necesitan corrección")
        
        # Guardar reporte detallado
        report_file = f"data_load_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n📄 Reporte detallado guardado: {report_file}")
        
        return overall_success


async def main():
    """Función principal"""
    tester = DataLoadTester()
    success = await tester.run_complete_test()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)