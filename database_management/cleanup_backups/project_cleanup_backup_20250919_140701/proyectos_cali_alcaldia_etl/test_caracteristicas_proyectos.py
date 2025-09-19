"""
Tests comprehensivos para el sistema de características de proyectos.

Este módulo contiene todas las pruebas para validar:
1. Estructura de la tabla y modelo
2. Proceso de carga de datos 
3. Integridad de los datos cargados
4. Rendimiento del sistema
"""

import json
import logging
import unittest
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from database_management.core.models import Base, CaracteristicasProyectos
from database_management.core.config import DatabaseConfig
from load_app.caracteristicas_proyectos_loader import CaracteristicasProyectosLoader

# Configurar logging para las pruebas
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestCaracteristicasProyectosSchema(unittest.TestCase):
    """Pruebas para la estructura del esquema y modelo."""
    
    @classmethod
    def setUpClass(cls):
        """Configuración inicial para todas las pruebas."""
        try:
            cls.config = DatabaseConfig()
            cls.engine = create_engine(cls.config.connection_string)
            cls.session_maker = sessionmaker(bind=cls.engine)
            
            # Crear las tablas
            Base.metadata.create_all(cls.engine)
        except Exception as e:
            print(f"⚠️ No se pudo configurar base de datos: {e}")
            cls.config = None
            cls.engine = None
            cls.session_maker = None
    
    def test_table_exists(self):
        """Verificar que la tabla caracteristicas_proyectos existe."""
        if self.engine is None:
            self.skipTest("Base de datos no disponible")
            
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        
        self.assertIn('caracteristicas_proyectos', tables, 
                     "La tabla caracteristicas_proyectos debe existir")
        logger.info("✅ Tabla caracteristicas_proyectos existe")
    
    def test_table_columns(self):
        """Verificar que la tabla tiene todas las columnas esperadas."""
        if self.engine is None:
            self.skipTest("Base de datos no disponible")
            
        inspector = inspect(self.engine)
        columns = inspector.get_columns('caracteristicas_proyectos')
        column_names = [col['name'] for col in columns]
        
        expected_columns = [
            'id', 'created_at', 'updated_at',  # Campos de BaseModel
            'bpin', 'bp', 'nombre_proyecto', 'nombre_actividad',
            'programa_presupuestal', 'nombre_centro_gestor', 'nombre_area_funcional',
            'nombre_fondo', 'clasificacion_fondo', 'nombre_pospre',
            'nombre_dimension', 'nombre_linea_estrategica', 'nombre_programa',
            'comuna', 'origen', 'anio', 'tipo_gasto',
            'cod_sector', 'cod_producto', 'validador_cuipo',
            'fecha_carga', 'fecha_actualizacion'
        ]
        
        for col in expected_columns:
            self.assertIn(col, column_names, f"Columna {col} debe existir")
        
        logger.info(f"✅ Todas las columnas esperadas existen: {len(expected_columns)} columnas")
    
    def test_table_indexes(self):
        """Verificar que la tabla tiene los índices esperados."""
        if self.engine is None:
            self.skipTest("Base de datos no disponible")
            
        inspector = inspect(self.engine)
        indexes = inspector.get_indexes('caracteristicas_proyectos')
        index_names = [idx['name'] for idx in indexes]
        
        expected_indexes = [
            'idx_caracteristicas_bpin_anio',
            'idx_caracteristicas_programa_anio',
            'idx_caracteristicas_centro_gestor_anio',
            'idx_caracteristicas_comuna_anio',
            'idx_caracteristicas_tipo_gasto_anio',
            'idx_caracteristicas_clasificacion_fondo',
            'idx_caracteristicas_fecha_carga'
        ]
        
        for idx in expected_indexes:
            self.assertIn(idx, index_names, f"Índice {idx} debe existir")
        
        logger.info(f"✅ Todos los índices esperados existen: {len(expected_indexes)} índices")
    
    def test_model_creation(self):
        """Verificar que se puede crear una instancia del modelo."""
        test_data = {
            'bpin': 123456,
            'bp': 'BP001',
            'nombre_proyecto': 'Proyecto de Prueba',
            'nombre_actividad': 'Actividad de Prueba',
            'programa_presupuestal': 'PROG001',
            'nombre_centro_gestor': 'Centro Gestor Prueba',
            'nombre_area_funcional': 'Área Funcional Prueba',
            'nombre_fondo': 'Fondo de Prueba',
            'clasificacion_fondo': 'Clasificación Prueba',
            'nombre_pospre': 'POSPRE Prueba',
            'nombre_programa': 'Programa Prueba',
            'comuna': 'Comuna 1',
            'origen': 'Origen Prueba',
            'anio': 2024,
            'tipo_gasto': 'Inversión'
        }
        
        # Crear instancia del modelo
        proyecto = CaracteristicasProyectos(**test_data)
        
        # Verificar que los campos se asignaron correctamente
        self.assertEqual(proyecto.bpin, 123456)
        self.assertEqual(proyecto.bp, 'BP001')
        self.assertEqual(proyecto.nombre_proyecto, 'Proyecto de Prueba')
        self.assertEqual(proyecto.anio, 2024)
        
        logger.info("✅ Instancia del modelo creada correctamente")
    
    def test_model_to_dict(self):
        """Verificar que el método to_dict funciona correctamente."""
        if self.session_maker is None:
            self.skipTest("Base de datos no disponible")
            
        with self.session_maker() as session:
            test_data = {
                'bpin': 789012,
                'bp': 'BP002',
                'nombre_proyecto': 'Proyecto Dict Test',
                'nombre_actividad': 'Actividad Dict Test',
                'programa_presupuestal': 'PROG002',
                'nombre_centro_gestor': 'Centro Dict Test',
                'nombre_area_funcional': 'Área Dict Test',
                'nombre_fondo': 'Fondo Dict Test',
                'clasificacion_fondo': 'Clasificación Dict',
                'nombre_pospre': 'POSPRE Dict Test',
                'nombre_programa': 'Programa Dict Test',
                'comuna': 'Comuna 2',
                'origen': 'Origen Dict',
                'anio': 2024,
                'tipo_gasto': 'Funcionamiento'
            }
            
            proyecto = CaracteristicasProyectos(**test_data)
            proyecto_dict = proyecto.to_dict()
            
            # Verificar que el diccionario contiene las claves esperadas
            expected_keys = ['id', 'bpin', 'bp', 'nombre_proyecto', 'anio']
            for key in expected_keys:
                self.assertIn(key, proyecto_dict)
            
            # Verificar que los valores son correctos
            self.assertEqual(proyecto_dict['bpin'], 789012)
            self.assertEqual(proyecto_dict['bp'], 'BP002')
            
            logger.info("✅ Método to_dict funciona correctamente")


class TestCaracteristicasProyectosLoader(unittest.TestCase):
    """Pruebas para el cargador de datos."""
    
    @classmethod
    def setUpClass(cls):
        """Configuración inicial para las pruebas del cargador."""
        cls.loader = CaracteristicasProyectosLoader()
        cls.test_json_file = Path("test_data_caracteristicas.json")
        cls._create_test_json_file()
    
    @classmethod
    def _create_test_json_file(cls):
        """Crear archivo JSON de prueba."""
        test_data = [
            {
                "bpin": 111111,
                "bp": "BP111",
                "nombre_proyecto": "Proyecto Test 1",
                "nombre_actividad": "Actividad Test 1",
                "programa_presupuestal": "PROG111",
                "nombre_centro_gestor": "Centro Test 1",
                "nombre_area_funcional": "Área Test 1",
                "nombre_fondo": "Fondo Test 1",
                "clasificacion_fondo": "Clasificación Test 1",
                "nombre_pospre": "POSPRE Test 1",
                "nombre_dimension": "Dimensión Test 1",
                "nombre_linea_estrategica": "Línea Test 1",
                "nombre_programa": "Programa Test 1",
                "comuna": "Comuna Test 1",
                "origen": "Origen Test 1",
                "anio": 2024,
                "tipo_gasto": "Inversión",
                "cod_sector": 1,
                "cod_producto": 101,
                "validador_cuipo": "CUIPO111"
            },
            {
                "bpin": 222222,
                "bp": "BP222",
                "nombre_proyecto": "Proyecto Test 2",
                "nombre_actividad": "Actividad Test 2",
                "programa_presupuestal": "PROG222",
                "nombre_centro_gestor": "Centro Test 2",
                "nombre_area_funcional": "Área Test 2",
                "nombre_fondo": "Fondo Test 2",
                "clasificacion_fondo": "Clasificación Test 2",
                "nombre_pospre": "POSPRE Test 2",
                "nombre_dimension": None,
                "nombre_linea_estrategica": None,
                "nombre_programa": "Programa Test 2",
                "comuna": "Comuna Test 2",
                "origen": "Origen Test 2",
                "anio": 2024,
                "tipo_gasto": "Funcionamiento",
                "cod_sector": None,
                "cod_producto": None,
                "validador_cuipo": None
            }
        ]
        
        with open(cls.test_json_file, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ Archivo de prueba creado: {cls.test_json_file}")
    
    @classmethod
    def tearDownClass(cls):
        """Limpieza después de las pruebas."""
        if cls.test_json_file.exists():
            cls.test_json_file.unlink()
            logger.info(f"✅ Archivo de prueba eliminado: {cls.test_json_file}")
    
    def test_database_connection(self):
        """Verificar que se puede establecer conexión con la base de datos."""
        success = self.loader.setup_database_connection()
        self.assertTrue(success, "Debe poder conectarse a la base de datos")
        logger.info("✅ Conexión a base de datos establecida")
    
    def test_load_json_data(self):
        """Verificar que se puede cargar el archivo JSON."""
        data = self.loader.load_json_data(self.test_json_file)
        
        self.assertIsInstance(data, list, "Los datos deben ser una lista")
        self.assertEqual(len(data), 2, "Debe haber 2 registros de prueba")
        
        # Verificar estructura del primer registro
        first_record = data[0]
        self.assertEqual(first_record['bpin'], 111111)
        self.assertEqual(first_record['bp'], 'BP111')
        
        logger.info("✅ Datos JSON cargados correctamente")
    
    def test_validate_record(self):
        """Verificar validación de registros."""
        # Registro válido
        valid_record = {
            'bpin': 123456,
            'bp': 'BP123',
            'nombre_proyecto': 'Proyecto Válido',
            'nombre_actividad': 'Actividad Válida',
            'programa_presupuestal': 'PROG123',
            'nombre_centro_gestor': 'Centro Válido',
            'nombre_area_funcional': 'Área Válida',
            'anio': 2024
        }
        
        self.assertTrue(self.loader.validate_record(valid_record), 
                       "Registro válido debe pasar la validación")
        
        # Registro inválido (falta campo requerido)
        invalid_record = valid_record.copy()
        del invalid_record['bpin']
        
        self.assertFalse(self.loader.validate_record(invalid_record),
                        "Registro sin BPIN debe fallar la validación")
        
        logger.info("✅ Validación de registros funciona correctamente")
    
    def test_transform_record(self):
        """Verificar transformación de registros."""
        original_record = {
            'bpin': 654321,
            'bp': 'BP654',
            'nombre_proyecto': 'Proyecto Original',
            'nombre_actividad': 'Actividad Original',
            'programa_presupuestal': 'PROG654',
            'nombre_centro_gestor': 'Centro Original',
            'nombre_area_funcional': 'Área Original',
            'nombre_fondo': 'Fondo Original',
            'clasificacion_fondo': 'Clasificación Original',
            'nombre_pospre': 'POSPRE Original',
            'nombre_programa': 'Programa Original',
            'comuna': 'Comuna Original',
            'origen': 'Origen Original',
            'anio': 2024,
            'tipo_gasto': 'Inversión'
        }
        
        transformed = self.loader.transform_record(original_record)
        
        # Verificar que los campos se transformaron correctamente
        self.assertEqual(transformed['bpin'], 654321)
        self.assertEqual(transformed['bp'], 'BP654')
        self.assertIsInstance(transformed['fecha_carga'], datetime)
        self.assertIsInstance(transformed['fecha_actualizacion'], datetime)
        
        logger.info("✅ Transformación de registros funciona correctamente")


class TestCaracteristicasProyectosIntegration(unittest.TestCase):
    """Pruebas de integración completas."""
    
    @classmethod
    def setUpClass(cls):
        """Configuración para pruebas de integración."""
        cls.json_file = Path("transformation_app/app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json")
        cls.loader = CaracteristicasProyectosLoader()
        cls.config = DatabaseConfig()
        cls.engine = create_engine(cls.config.get_connection_string())
        cls.session_maker = sessionmaker(bind=cls.engine)
    
    def test_file_exists(self):
        """Verificar que el archivo de datos reales existe."""
        self.assertTrue(self.json_file.exists(), 
                       f"Archivo de datos debe existir: {self.json_file}")
        logger.info(f"✅ Archivo de datos encontrado: {self.json_file}")
    
    def test_file_structure(self):
        """Verificar la estructura del archivo de datos reales."""
        if not self.json_file.exists():
            self.skipTest("Archivo de datos no encontrado")
        
        with open(self.json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self.assertIsInstance(data, list, "Los datos deben ser una lista")
        self.assertGreater(len(data), 0, "Debe haber al menos un registro")
        
        # Verificar estructura del primer registro
        first_record = data[0]
        required_fields = ['bpin', 'bp', 'nombre_proyecto', 'anio']
        
        for field in required_fields:
            self.assertIn(field, first_record, f"Campo {field} debe existir")
        
        logger.info(f"✅ Estructura del archivo válida: {len(data)} registros")
    
    def test_data_sample_analysis(self):
        """Analizar una muestra de los datos reales."""
        if not self.json_file.exists():
            self.skipTest("Archivo de datos no encontrado")
        
        with open(self.json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Analizar primeros 10 registros
        sample = data[:10]
        
        for i, record in enumerate(sample):
            try:
                is_valid = self.loader.validate_record(record)
                if not is_valid:
                    logger.warning(f"⚠️ Registro {i} no es válido: {record.get('bpin', 'Sin BPIN')}")
                else:
                    logger.info(f"✅ Registro {i} válido: BPIN {record.get('bpin')}")
            except Exception as e:
                logger.error(f"❌ Error analizando registro {i}: {e}")
        
        logger.info(f"✅ Análisis de muestra completado: {len(sample)} registros analizados")
    
    def test_database_performance(self):
        """Probar rendimiento de consultas en la base de datos."""
        with self.session_maker() as session:
            try:
                # Contar registros existentes
                start_time = datetime.utcnow()
                count = session.query(CaracteristicasProyectos).count()
                end_time = datetime.utcnow()
                
                duration = (end_time - start_time).total_seconds()
                
                logger.info(f"✅ Consulta de conteo: {count} registros en {duration:.3f} segundos")
                
                # Probar consulta con índice
                start_time = datetime.utcnow()
                result = session.query(CaracteristicasProyectos).filter(
                    CaracteristicasProyectos.anio == 2024
                ).limit(10).all()
                end_time = datetime.utcnow()
                
                duration = (end_time - start_time).total_seconds()
                
                logger.info(f"✅ Consulta con filtro: {len(result)} registros en {duration:.3f} segundos")
                
            except Exception as e:
                logger.error(f"❌ Error en prueba de rendimiento: {e}")
                raise


def run_comprehensive_tests():
    """Ejecutar todas las pruebas de forma organizada."""
    print("🔄 INICIANDO PRUEBAS COMPREHENSIVAS - CARACTERÍSTICAS DE PROYECTOS")
    print("=" * 80)
    
    # Configurar el test runner
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Agregar todas las clases de prueba
    test_classes = [
        TestCaracteristicasProyectosSchema,
        TestCaracteristicasProyectosLoader,
        TestCaracteristicasProyectosIntegration
    ]
    
    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Ejecutar las pruebas
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Resumen final
    print("\n" + "=" * 80)
    print("📊 RESUMEN DE PRUEBAS")
    print("=" * 80)
    print(f"✅ Pruebas exitosas: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"❌ Pruebas fallidas: {len(result.failures)}")
    print(f"💥 Errores: {len(result.errors)}")
    print(f"🔍 Total ejecutadas: {result.testsRun}")
    
    if result.failures:
        print("\n❌ PRUEBAS FALLIDAS:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print("\n💥 ERRORES:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    print(f"\n🎯 RESULTADO FINAL: {'✅ TODAS LAS PRUEBAS EXITOSAS' if success else '❌ HAY PRUEBAS FALLIDAS'}")
    
    return success


if __name__ == "__main__":
    run_comprehensive_tests()