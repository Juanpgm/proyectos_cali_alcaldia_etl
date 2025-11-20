# 🎯 Sistema de Testing de Calidad de Datos ETL

## Resumen Ejecutivo

Se ha creado un **sistema completo de testing de calidad de datos** para la ETL que verifica automáticamente que los datos transformados cumplan con las reglas de negocio y estándares establecidos.

## 📦 Archivos Creados

| Archivo                    | Descripción                                                      |
| -------------------------- | ---------------------------------------------------------------- |
| `test_etl_data_quality.py` | **Script principal** - Sistema completo de testing (760+ líneas) |
| `ejemplo_test_calidad.py`  | **Ejemplos de uso** - 5 ejemplos prácticos de implementación     |
| `prueba_rapida_tester.py`  | **Prueba rápida** - Verificación automática con datos sintéticos |
| `TEST_CALIDAD_README.md`   | **Documentación completa** - Guía detallada de uso               |

## ✅ Pruebas Implementadas

### 1️⃣ Congruencia `estado` ↔ `avance_obra`

- ✓ Si `avance_obra = 0` → `estado` DEBE ser "En Alistamiento"
- ⚠ Alerta si `avance_obra > 0` pero `estado = "En Alistamiento"`
- ⚠ Alerta si `avance_obra = 100` pero `estado ≠ "Terminado"`

### 2️⃣ Validación Numérica de `avance_obra`

- ✓ Solo valores numéricos (int/float)
- ✓ Sin valores NaN o None
- ✓ Rango válido: [0, 100]
- ✓ Sin valores negativos

### 3️⃣ Valores Permitidos en `estado`

- ✓ "En Alistamiento"
- ✓ "En Ejecución"
- ✓ "Terminado"
- ✗ Cualquier otro valor es ERROR CRÍTICO

### 4️⃣ Detección de Funciones Duplicadas

- ✓ Detecta código idéntico (duplicados exactos)
- ✓ Identifica nombres sospechosamente similares
- ✓ Verifica funciones específicas de transformación
- ✓ Previene errores por funciones intrusas

## 🚀 Uso Rápido

### Desde Línea de Comandos

```bash
# Ejecutar todas las pruebas
python test_etl_data_quality.py --data app_outputs/transformed_data.csv

# Con análisis de módulo
python test_etl_data_quality.py --data output.csv --module transformation_app/data_transformation_unidades_proyecto.py

# Guardar reporte personalizado
python test_etl_data_quality.py --data output.csv --output reports/quality_$(date +%Y%m%d).json
```

### Desde Código Python

```python
from test_etl_data_quality import ETLDataQualityTester

# Opción 1: Con archivo
tester = ETLDataQualityTester(data_path='output.csv')
tester.load_data()
resultados = tester.run_all_tests()
tester.save_report('quality_report.json')

# Opción 2: Con DataFrame
tester = ETLDataQualityTester()
tester.load_data(mi_dataframe)
resultados = tester.run_all_tests()
```

## 📊 Resultados

### ✅ Prueba Exitosa

```
======================================================================
RESUMEN DE PRUEBAS DE CALIDAD
======================================================================

Total de pruebas ejecutadas: 4
✓ Pruebas pasadas: 4 (100.0%)
✗ Pruebas falladas: 0 (0.0%)
⚠ Advertencias: 0

🎉 EXCELENTE: Todos los tests pasaron sin errores ni advertencias!
```

### ⚠️ Con Advertencias

```
Total de pruebas ejecutadas: 4
✓ Pruebas pasadas: 4 (100.0%)
✗ Pruebas falladas: 0 (0.0%)
⚠ Advertencias: 3

✓ BUENO: Todos los tests pasaron, pero hay advertencias a revisar.
```

### ❌ Con Errores

```
Total de pruebas ejecutadas: 4
✓ Pruebas pasadas: 2 (50.0%)
✗ Pruebas falladas: 2 (50.0%)
⚠ Advertencias: 5

⚠ ATENCIÓN: 2 test(s) fallaron. Revisar errores críticos.
```

## 🔧 Integración con Pipeline ETL

### Opción 1: Validación Post-Transformación

```python
# En tu script de transformación
from test_etl_data_quality import ETLDataQualityTester

# Después de transformar
df_transformed = transform_data(df_raw)

# Validar calidad
tester = ETLDataQualityTester()
tester.load_data(df_transformed)
resultados = tester.run_all_tests()

# Verificar antes de continuar
if tester.test_results['failed_tests'] > 0:
    raise Exception("❌ Datos no cumplen estándares de calidad")

# Si todo OK, continuar
load_to_firebase(df_transformed)
```

### Opción 2: Script Independiente en CI/CD

```bash
#!/bin/bash
# Pipeline de producción

# 1. Ejecutar ETL
python pipelines/run_etl.py

# 2. Validar calidad
python test_etl_data_quality.py --data app_outputs/transformed_data.csv

# 3. Si pasa, desplegar
if [ $? -eq 0 ]; then
    python load_to_firebase.py
    echo "✅ Deployment exitoso"
else
    echo "❌ Validación falló - no se desplegará"
    exit 1
fi
```

## 🧪 Verificación del Sistema

```bash
# Ejecutar prueba rápida de verificación
python prueba_rapida_tester.py

# Resultado esperado:
# 🎉 EXCELENTE: Todas las pruebas pasaron!
# El sistema de testing de calidad está funcionando correctamente.
```

## 📈 Análisis del Código de Transformación

El sistema también detectó:

- ✅ **45 funciones** analizadas en el módulo de transformación
- ✅ **0 funciones duplicadas** (código idéntico)
- ⚠️ **35 funciones con nombres similares** (advertencias por convención de nombres)
- ✅ Funciones críticas encontradas:
  - `normalize_estado_values` (43 líneas)
  - `clean_numeric_column` (7 líneas)
  - `clean_numeric_column_safe` (8 líneas)

**Nota**: Las advertencias de nombres similares son normales y esperadas en módulos grandes con nomenclatura consistente (ej: `clean_*`, `normalize_*`).

## 📝 Reportes Generados

Los reportes JSON contienen:

- Timestamp de ejecución
- Resumen de tests (pasados/fallados/advertencias)
- Detalles de cada prueba
- Muestras de registros con problemas
- Estadísticas descriptivas
- Distribuciones de valores

Ejemplo de estructura:

```json
{
  "timestamp": "2025-11-18T10:30:00",
  "total_tests": 4,
  "passed_tests": 4,
  "failed_tests": 0,
  "warnings": 2,
  "details": [...]
}
```

## 🎓 Ejemplos Disponibles

Ver `ejemplo_test_calidad.py` para:

1. **Ejemplo Básico** - Cargar desde archivo CSV
2. **Con DataFrame** - Usar datos en memoria
3. **Tests Individuales** - Ejecutar pruebas específicas
4. **Análisis de Módulo** - Detectar funciones duplicadas
5. **Pipeline Completo** - Flujo de trabajo completo

```bash
python ejemplo_test_calidad.py
```

## 📚 Documentación Completa

Ver `TEST_CALIDAD_README.md` para:

- Guía detallada de uso
- Interpretación de resultados
- Personalización de pruebas
- Casos de uso específicos
- Troubleshooting

## ✨ Características Principales

| Característica                          | Estado          |
| --------------------------------------- | --------------- |
| Validación de reglas de negocio         | ✅ Implementado |
| Detección de tipos de datos incorrectos | ✅ Implementado |
| Validación de valores permitidos        | ✅ Implementado |
| Análisis de funciones duplicadas        | ✅ Implementado |
| Reportes JSON detallados                | ✅ Implementado |
| Modo verbose/silencioso                 | ✅ Implementado |
| Integración con CLI                     | ✅ Implementado |
| Integración con código Python           | ✅ Implementado |
| Ejemplos de uso                         | ✅ Implementado |
| Documentación completa                  | ✅ Implementado |
| Tests de verificación                   | ✅ Implementado |

## 🎯 Próximos Pasos Recomendados

1. **Ejecutar prueba rápida** para verificar instalación:

   ```bash
   python prueba_rapida_tester.py
   ```

2. **Probar con datos reales**:

   ```bash
   python test_etl_data_quality.py --data app_outputs/transformed_data.csv
   ```

3. **Integrar en pipeline** según necesidades (ver opciones arriba)

4. **Revisar reportes** para identificar áreas de mejora

5. **Personalizar** tests adicionales según reglas específicas

## 📞 Soporte

- **Documentación completa**: `TEST_CALIDAD_README.md`
- **Ejemplos de uso**: `ejemplo_test_calidad.py`
- **Verificación rápida**: `prueba_rapida_tester.py`

---

**Estado**: ✅ Sistema completamente funcional y verificado  
**Fecha**: Noviembre 18, 2025  
**Versión**: 1.0.0
