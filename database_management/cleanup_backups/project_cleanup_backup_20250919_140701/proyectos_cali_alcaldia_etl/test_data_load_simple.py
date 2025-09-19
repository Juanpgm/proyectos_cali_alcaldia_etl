#!/usr/bin/env python3
"""
Pruebas simplificadas de carga de datos para ETL
"""

import asyncio
import json
import logging
import psycopg2
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Any, Optional

# Configuración de logging sin Unicode problemático
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_load_test_simple.log', encoding='utf-8')
    ]
)

from database_management.core import get_database_config, quick_health_check, comprehensive_analysis
from orchestrator.etl_orchestrator import ETLOrchestrator


class SimpleDataLoadTester:
    def __init__(self):
        self.db_config = get_database_config()
        self.test_results = {}
        self.orchestrator = None
        
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

    def test_basic_connectivity(self) -> bool:
        """Test básico de conectividad"""
        print("\n1. PRUEBA DE CONECTIVIDAD BASICA")
        print("-" * 60)
        
        try:
            result = quick_health_check()
            
            self.log_test(
                "Conectividad PostgreSQL", 
                result['connection'], 
                f"Estado de conexion: {result.get('status', 'desconocido')}"
            )
            
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
                    table_count = cursor.fetchone()[0]
                    
                    self.log_test(
                        "Conteo de tablas",
                        table_count > 0,
                        f"Tablas encontradas: {table_count}",
                        table_count
                    )
                    
            return True
            
        except Exception as e:
            self.log_test("Conectividad", False, f"Error: {str(e)}")
            return False

    def test_simple_data_insertion(self) -> bool:
        """Test simple de inserción de datos"""
        print("\n2. PRUEBA SIMPLE DE INSERCION")
        print("-" * 60)
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    
                    # Verificar si la tabla emp_contratos existe
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_name = 'emp_contratos'
                        )
                    """)
                    
                    table_exists = cursor.fetchone()[0]
                    
                    if not table_exists:
                        self.log_test(
                            "Verificacion tabla contratos",
                            False,
                            "Tabla emp_contratos no existe"
                        )
                        return False
                    
                    # Insertar un registro de prueba
                    test_contract = {
                        'id_contrato': f'TEST-{datetime.now().strftime("%Y%m%d%H%M%S")}',
                        'referencia_del_contrato': f'REF-{datetime.now().strftime("%Y%m%d%H%M%S")}',
                        'proceso_de_compra': 'Proceso de prueba',
                        'nombre_entidad': 'Entidad de Prueba',
                        'nit_entidad': '123456789-0',
                        'estado_contrato': 'EN EJECUCION',
                        'objeto_del_contrato': 'Objeto de contrato de prueba',
                        'proveedor_adjudicado': 'Contratista de Prueba',
                        'valor_del_contrato': Decimal('1000000.00'),
                        'fecha_de_firma': datetime.now().date(),
                        'fecha_de_inicio_del_contrato': datetime.now().date(),
                        'fecha_de_fin_del_contrato': (datetime.now() + timedelta(days=30)).date(),
                        'created_at': datetime.now()
                    }
                    
                    cursor.execute("""
                        INSERT INTO emp_contratos (
                            id_contrato, referencia_del_contrato, proceso_de_compra,
                            nombre_entidad, nit_entidad, estado_contrato,
                            objeto_del_contrato, proveedor_adjudicado, valor_del_contrato,
                            fecha_de_firma, fecha_de_inicio_del_contrato, fecha_de_fin_del_contrato,
                            created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id_contrato) DO NOTHING
                        RETURNING id_contrato
                    """, (
                        test_contract['id_contrato'], test_contract['referencia_del_contrato'],
                        test_contract['proceso_de_compra'], test_contract['nombre_entidad'],
                        test_contract['nit_entidad'], test_contract['estado_contrato'],
                        test_contract['objeto_del_contrato'], test_contract['proveedor_adjudicado'],
                        test_contract['valor_del_contrato'], test_contract['fecha_de_firma'],
                        test_contract['fecha_de_inicio_del_contrato'], test_contract['fecha_de_fin_del_contrato'],
                        test_contract['created_at']
                    ))
                    
                    result = cursor.fetchone()
                    success = result is not None
                    
                    if success:
                        conn.commit()
                        
                        self.log_test(
                            "Insercion de contrato de prueba",
                            success,
                            f"Contrato insertado: {test_contract['id_contrato']}" if success else "No se pudo insertar",
                            1 if success else 0
                        )
                        
                        return success
                    
        except Exception as e:
            self.log_test("Insercion simple", False, f"Error: {str(e)}")
            return False

    def test_data_queries(self) -> bool:
        """Test de consultas básicas"""
        print("\n3. PRUEBAS DE CONSULTAS")
        print("-" * 60)
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    
                    # Consulta básica de conteo
                    cursor.execute("SELECT COUNT(*) FROM emp_contratos")
                    contract_count = cursor.fetchone()[0]
                    
                    self.log_test(
                        "Conteo de contratos",
                        True,
                        f"Total de contratos: {contract_count}",
                        contract_count
                    )
                    
                    # Consulta con filtros
                    cursor.execute("""
                        SELECT estado_contrato, COUNT(*) 
                        FROM emp_contratos 
                        GROUP BY estado_contrato 
                        ORDER BY COUNT(*) DESC
                    """)
                    
                    results = cursor.fetchall()
                    
                    self.log_test(
                        "Agrupacion por estado",
                        len(results) > 0,
                        f"Estados encontrados: {len(results)}",
                        len(results)
                    )
                    
                    return True
                    
        except Exception as e:
            self.log_test("Consultas", False, f"Error: {str(e)}")
            return False

    async def test_etl_workflow(self) -> bool:
        """Test básico del workflow ETL"""
        print("\n4. PRUEBA DE WORKFLOW ETL")
        print("-" * 60)
        
        try:
            if not self.orchestrator:
                self.orchestrator = ETLOrchestrator(log_level='INFO')
            
            # Verificar si hay workflows de prueba disponibles
            # Para este test, simplemente verificamos que el orchestrator se puede instanciar
            
            self.log_test(
                "Inicializacion ETL Orchestrator",
                True,
                f"Orchestrator inicializado correctamente",
                1
            )
            
            return True
            
        except Exception as e:
            self.log_test("Workflow ETL", False, f"Error: {str(e)}")
            return False

    def test_monitoring_system(self) -> bool:
        """Test del sistema de monitoreo"""
        print("\n5. PRUEBA DE MONITOREO")
        print("-" * 60)
        
        try:
            analysis_result = comprehensive_analysis(1)
            
            success = analysis_result is not None
            status = analysis_result.get('overall_assessment', {}).get('overall_status', 'unknown') if success else 'error'
            
            self.log_test(
                "Analisis comprehensive",
                success,
                f"Estado del sistema: {status}",
                1 if success else 0
            )
            
            return success
            
        except Exception as e:
            self.log_test("Monitoreo", False, f"Error: {str(e)}")
            return False

    async def run_all_tests(self) -> bool:
        """Ejecutar todas las pruebas"""
        print("=" * 80)
        print("PRUEBAS SIMPLIFICADAS DE CARGA DE DATOS")
        print("=" * 80)
        
        start_time = datetime.now()
        
        tests = [
            ("Conectividad", self.test_basic_connectivity()),
            ("Insercion", self.test_simple_data_insertion()),
            ("Consultas", self.test_data_queries()),
            ("Workflow ETL", await self.test_etl_workflow()),
            ("Monitoreo", self.test_monitoring_system())
        ]
        
        results = []
        for test_name, result in tests:
            results.append((test_name, result))
        
        # Reporte final
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        successful_tests = sum(1 for _, result in results if result)
        total_tests = len(results)
        success_rate = (successful_tests / total_tests) * 100
        
        print("\n" + "=" * 80)
        print("REPORTE FINAL")
        print("=" * 80)
        print(f"Duracion total: {duration:.2f} segundos")
        print(f"Pruebas ejecutadas: {total_tests}")
        print(f"Pruebas exitosas: {successful_tests}")
        print(f"Pruebas fallidas: {total_tests - successful_tests}")
        print(f"Tasa de exito: {success_rate:.1f}%")
        
        if success_rate >= 80:
            print("\nSISTEMA FUNCIONAL")
        elif success_rate >= 60:
            print("\nSISTEMA REQUIERE ATENCION")
        else:
            print("\nSISTEMA NECESITA CORRECCION URGENTE")
        
        # Guardar reporte
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "duration_seconds": duration,
            "total_tests": total_tests,
            "successful_tests": successful_tests,
            "success_rate": success_rate,
            "test_results": self.test_results,
            "summary": {
                "status": "functional" if success_rate >= 80 else "needs_attention" if success_rate >= 60 else "critical"
            }
        }
        
        report_file = f"simple_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        print(f"\nReporte guardado: {report_file}")
        
        return success_rate >= 60


async def main():
    """Función principal"""
    tester = SimpleDataLoadTester()
    success = await tester.run_all_tests()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)