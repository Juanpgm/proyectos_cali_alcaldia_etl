# Script de Testing de Calidad de Datos ETL

## 📋 Descripción

Script completo de testing que verifica la calidad de los datos transformados por la ETL, garantizando que cumplan con las reglas de negocio y los estándares de calidad establecidos.

## ✅ Pruebas Implementadas

### 1. Congruencia entre `estado` y `avance_obra`

Verifica que la variable `estado` sea congruente con el valor de `avance_obra`:

- **Regla Crítica**: Si `avance_obra` = 0, entonces `estado` DEBE ser "En Alistamiento"
- **Advertencia**: Si `avance_obra` > 0 pero `estado` = "En Alistamiento", se reporta como sospechoso
- **Advertencia**: Si `avance_obra` = 100 pero `estado` ≠ "Terminado", se reporta como inconsistente

**Severidad**: CRÍTICA

### 2. Validación de Datos Numéricos en `avance_obra`

Garantiza que `avance_obra` solo maneje datos numéricos válidos:

- ✓ Todos los valores deben ser numéricos (int o float)
- ✓ No debe haber valores NaN o None
- ✓ Los valores deben estar en el rango [0, 100]
- ✓ No debe haber valores negativos

**Severidad**: CRÍTICA

### 3. Validación de Valores Permitidos en `estado`

Revisa que la variable `estado` solo tome los valores permitidos:

- ✅ "En Alistamiento"
- ✅ "En Ejecución"
- ✅ "Terminado"

Cualquier otro valor se reporta como **ERROR CRÍTICO**.

**Severidad**: CRÍTICA

### 4. Detección de Funciones Duplicadas o Intrusas

Revisa el módulo de transformación para detectar:

- 🔍 Funciones con código idéntico (duplicados exactos)
- 🔍 Funciones con nombres sospechosamente similares
- 🔍 Múltiples versiones de funciones de normalización
- 🔍 Funciones que puedan estar añadiendo errores o sesgos

**Severidad**: ADVERTENCIA/CRÍTICA

## 🚀 Uso

### Uso desde Línea de Comandos

```bash
# Ejecutar todas las pruebas con un archivo CSV
python test_etl_data_quality.py --data app_outputs/transformed_data.csv

# Especificar módulo de transformación a analizar
python test_etl_data_quality.py --data output.csv --module transformation_app/data_transformation_unidades_proyecto.py

# Guardar reporte en ubicación específica
python test_etl_data_quality.py --data output.csv --output reports/quality_report.json

# Modo silencioso (menos output)
python test_etl_data_quality.py --data output.csv --quiet
```

### Uso desde Código Python

```python
from test_etl_data_quality import ETLDataQualityTester
import pandas as pd

# Opción 1: Cargar desde archivo
tester = ETLDataQualityTester(data_path='output.csv')
tester.load_data()
resultados = tester.run_all_tests()

# Opción 2: Usar DataFrame existente
df = pd.read_csv('output.csv')
tester = ETLDataQualityTester()
tester.load_data(df)
resultados = tester.run_all_tests()

# Guardar reporte
tester.save_report('quality_report.json')
```

### Ejecutar Tests Individuales

```python
from test_etl_data_quality import ETLDataQualityTester

tester = ETLDataQualityTester()
tester.load_data('output.csv')

# Test 1: Congruencia
resultado_1 = tester.test_estado_avance_consistency()

# Test 2: Validación numérica
resultado_2 = tester.test_avance_obra_numeric()

# Test 3: Valores válidos
resultado_3 = tester.test_estado_valid_values()

# Test 4: Funciones duplicadas
resultado_4 = tester.test_duplicate_functions(
    module_path='transformation_app/data_transformation_unidades_proyecto.py'
)
```

## 📊 Interpretación de Resultados

### Códigos de Salida

- **0**: Todos los tests pasaron ✅
- **1**: Uno o más tests fallaron ❌

### Niveles de Severidad

| Nivel       | Símbolo | Descripción                                   |
| ----------- | ------- | --------------------------------------------- |
| **SUCCESS** | ✓✓      | Test pasado exitosamente                      |
| **INFO**    | ✓       | Información general                           |
| **WARNING** | ⚠       | Advertencia - requiere revisión               |
| **ERROR**   | ✗       | Error crítico - requiere corrección inmediata |

### Ejemplo de Output

```
======================================================================
TEST 1: Congruencia entre 'estado' y 'avance_obra'
======================================================================
✓ Todos los registros con avance_obra=0 tienen estado='En Alistamiento'
⚠ ADVERTENCIA: 5 registros con avance_obra>0 pero estado='En Alistamiento'

✓✓ TEST 1 PASADO: Estado y avance_obra son congruentes

======================================================================
TEST 2: Validación de datos numéricos en 'avance_obra'
======================================================================
✓ Todos los valores de 'avance_obra' son numéricos
✓ No hay valores nulos en 'avance_obra'
✓ Todos los valores están en el rango [0, 100]
  Estadísticas: Media=45.32, Mediana=42.50, Min=0.00, Max=100.00

✓✓ TEST 2 PASADO: avance_obra contiene solo datos numéricos válidos

======================================================================
RESUMEN DE PRUEBAS DE CALIDAD
======================================================================

Total de pruebas ejecutadas: 4
✓ Pruebas pasadas: 4 (100.0%)
✗ Pruebas falladas: 0 (0.0%)
⚠ Advertencias: 2

✓ BUENO: Todos los tests pasaron, pero hay advertencias a revisar.
```

## 📁 Estructura del Reporte JSON

El reporte generado tiene la siguiente estructura:

```json
{
  "timestamp": "2025-11-18T10:30:00",
  "total_tests": 4,
  "passed_tests": 4,
  "failed_tests": 0,
  "warnings": 2,
  "details": [
    {
      "test_name": "estado_avance_consistency",
      "passed": true,
      "timestamp": "2025-11-18T10:30:05",
      "total_records": 1500,
      "inconsistencies": [],
      "warnings": [
        {
          "rule": "avance_obra > 0 con estado = 'En Alistamiento' es sospechoso",
          "count": 5,
          "sample_indices": [23, 45, 67, 89, 101]
        }
      ],
      "summary": {
        "zero_avance_count": 150,
        "partial_avance_count": 1200,
        "complete_avance_count": 150,
        "estado_distribution": {
          "En Alistamiento": 150,
          "En Ejecución": 1200,
          "Terminado": 150
        },
        "consistency_rate": "100.00%"
      }
    }
  ]
}
```

## 🔧 Integración con Pipeline ETL

### Opción 1: Integración Automática

Agregar al final del script de transformación:

```python
from test_etl_data_quality import ETLDataQualityTester

# Después de transformar los datos
df_transformed = transform_data(df_raw)

# Ejecutar pruebas de calidad
tester = ETLDataQualityTester()
tester.load_data(df_transformed)
resultados = tester.run_all_tests()

# Guardar reporte
tester.save_report('app_outputs/reports/quality_report.json')

# Decidir si continuar con el pipeline
if tester.test_results['failed_tests'] > 0:
    raise Exception("Datos no cumplen con estándares de calidad")
else:
    # Continuar con carga a Firebase/S3
    load_to_firebase(df_transformed)
```

### Opción 2: Script Independiente

Ejecutar como validación post-transformación:

```bash
# 1. Ejecutar ETL
python pipelines/run_etl.py

# 2. Validar calidad
python test_etl_data_quality.py --data app_outputs/transformed_data.csv

# 3. Si exitoso, continuar con deployment
if [ $? -eq 0 ]; then
    python load_to_firebase.py
else
    echo "Validación de calidad falló"
    exit 1
fi
```

## 📝 Ejemplos de Uso

Ver `ejemplo_test_calidad.py` para ejemplos detallados:

1. **Ejemplo Básico con Archivo**: Cargar datos desde CSV y ejecutar todas las pruebas
2. **Ejemplo con DataFrame**: Usar DataFrame en memoria
3. **Tests Individuales**: Ejecutar pruebas específicas
4. **Análisis de Módulo**: Analizar funciones del módulo de transformación
5. **Pipeline Completo**: Ejemplo de pipeline completo de testing

```bash
# Ejecutar ejemplos
python ejemplo_test_calidad.py
```

## 🎯 Casos de Uso

### Desarrollo

Durante el desarrollo, ejecutar después de cada cambio en el módulo de transformación:

```bash
python test_etl_data_quality.py --data test_outputs/sample_data.csv
```

### Producción

Como parte del pipeline de CI/CD o scheduler:

```bash
#!/bin/bash
# Ejecutar ETL
python pipelines/run_etl.py

# Validar calidad
python test_etl_data_quality.py \
    --data app_outputs/transformed_data.csv \
    --output app_outputs/reports/quality_report_$(date +%Y%m%d_%H%M%S).json

# Verificar resultado
if [ $? -eq 0 ]; then
    echo "✓ Calidad de datos validada"
    # Continuar con deployment
else
    echo "✗ Validación falló - revisar reporte"
    exit 1
fi
```

### Debugging

Para diagnosticar problemas específicos:

```python
from test_etl_data_quality import ETLDataQualityTester

# Cargar datos problemáticos
tester = ETLDataQualityTester(verbose=True)
tester.load_data('problematic_data.csv')

# Ejecutar test específico
resultado = tester.test_estado_avance_consistency()

# Analizar inconsistencias
for inconsistency in resultado['inconsistencies']:
    print(f"Regla violada: {inconsistency['rule']}")
    print(f"Índices afectados: {inconsistency['sample_indices']}")
```

## 🛠️ Personalización

### Agregar Nuevos Tests

```python
class ETLDataQualityTester:
    # ... código existente ...

    def test_custom_rule(self) -> Dict[str, Any]:
        """Implementar nueva regla de validación."""
        if self.df is None:
            return {'error': 'No hay datos cargados'}

        results = {'total_records': len(self.df)}

        # Implementar lógica de validación
        # ...

        # Registrar resultado
        test_passed = True  # o False según validación
        self._record_test('custom_rule', test_passed, results)

        return results
```

### Modificar Valores Permitidos

```python
# En el script, modificar la constante:
VALID_ESTADO_VALUES = {'En Alistamiento', 'En Ejecución', 'Terminado', 'Nuevo Estado'}
```

## 📚 Dependencias

```txt
pandas>=1.3.0
numpy>=1.21.0
```

Instalar con:

```bash
pip install pandas numpy
```

## 🤝 Contribuciones

Para agregar nuevas pruebas o mejoras:

1. Crear nueva función `test_<nombre>()`
2. Seguir el patrón de las funciones existentes
3. Usar `self._record_test()` para registrar resultados
4. Documentar claramente las reglas de validación

## 📞 Soporte

Para preguntas o problemas:

- Revisar ejemplos en `ejemplo_test_calidad.py`
- Consultar logs detallados con `verbose=True`
- Revisar reportes JSON generados

---

**Última actualización**: Noviembre 2025  
**Versión**: 1.0.0
