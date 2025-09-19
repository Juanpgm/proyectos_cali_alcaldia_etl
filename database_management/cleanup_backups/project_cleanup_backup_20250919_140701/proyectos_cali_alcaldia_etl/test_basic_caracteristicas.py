"""
Pruebas básicas del modelo sin conexión a base de datos.
"""

import unittest
import json
from pathlib import Path
from database_management.core.models import CaracteristicasProyectos
from load_app.caracteristicas_proyectos_loader import CaracteristicasProyectosLoader

class TestCaracteristicasProyectosBasic(unittest.TestCase):
    """Pruebas básicas del modelo."""
    
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
        
        print("✅ Instancia del modelo creada correctamente")
    
    def test_model_to_dict(self):
        """Verificar que el método to_dict funciona correctamente."""
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
        
        print("✅ Método to_dict funciona correctamente")

class TestCaracteristicasProyectosLoader(unittest.TestCase):
    """Pruebas del cargador sin conexión a base de datos."""
    
    @classmethod
    def setUpClass(cls):
        """Configuración inicial para las pruebas del cargador."""
        cls.loader = CaracteristicasProyectosLoader()
        cls.test_json_file = Path("test_data_caracteristicas_basic.json")
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
        
        print(f"✅ Archivo de prueba creado: {cls.test_json_file}")
    
    @classmethod
    def tearDownClass(cls):
        """Limpieza después de las pruebas."""
        if cls.test_json_file.exists():
            cls.test_json_file.unlink()
            print(f"✅ Archivo de prueba eliminado: {cls.test_json_file}")
    
    def test_load_json_data(self):
        """Verificar que se puede cargar el archivo JSON."""
        data = self.loader.load_json_data(self.test_json_file)
        
        self.assertIsInstance(data, list, "Los datos deben ser una lista")
        self.assertEqual(len(data), 2, "Debe haber 2 registros de prueba")
        
        # Verificar estructura del primer registro
        first_record = data[0]
        self.assertEqual(first_record['bpin'], 111111)
        self.assertEqual(first_record['bp'], 'BP111')
        
        print("✅ Datos JSON cargados correctamente")
    
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
        
        print("✅ Validación de registros funciona correctamente")
    
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
        
        print("✅ Transformación de registros funciona correctamente")

class TestCaracteristicasProyectosIntegration(unittest.TestCase):
    """Pruebas de integración básicas."""
    
    def test_file_exists(self):
        """Verificar que el archivo de datos reales existe."""
        json_file = Path("transformation_app/app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json")
        self.assertTrue(json_file.exists(), 
                       f"Archivo de datos debe existir: {json_file}")
        print(f"✅ Archivo de datos encontrado: {json_file}")
    
    def test_file_structure(self):
        """Verificar la estructura del archivo de datos reales."""
        json_file = Path("transformation_app/app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json")
        
        if not json_file.exists():
            self.skipTest("Archivo de datos no encontrado")
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self.assertIsInstance(data, list, "Los datos deben ser una lista")
        self.assertGreater(len(data), 0, "Debe haber al menos un registro")
        
        # Verificar estructura del primer registro
        first_record = data[0]
        required_fields = ['bpin', 'bp', 'nombre_proyecto', 'anio']
        
        for field in required_fields:
            self.assertIn(field, first_record, f"Campo {field} debe existir")
        
        print(f"✅ Estructura del archivo válida: {len(data)} registros")

def run_basic_tests():
    """Ejecutar todas las pruebas básicas."""
    print("🔄 INICIANDO PRUEBAS BÁSICAS - CARACTERÍSTICAS DE PROYECTOS")
    print("=" * 70)
    
    # Configurar el test runner
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Agregar todas las clases de prueba
    test_classes = [
        TestCaracteristicasProyectosBasic,
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
    print("\n" + "=" * 70)
    print("📊 RESUMEN DE PRUEBAS BÁSICAS")
    print("=" * 70)
    print(f"✅ Pruebas exitosas: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"❌ Pruebas fallidas: {len(result.failures)}")
    print(f"💥 Errores: {len(result.errors)}")
    print(f"🔍 Total ejecutadas: {result.testsRun}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    print(f"\n🎯 RESULTADO FINAL: {'✅ TODAS LAS PRUEBAS EXITOSAS' if success else '❌ HAY PRUEBAS FALLIDAS'}")
    
    return success

if __name__ == "__main__":
    run_basic_tests()