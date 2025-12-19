# 🌍 Integración de Clustering Geoespacial en Pipeline

## 📋 Resumen

Se ha integrado la lógica de **clustering geoespacial inteligente** en el pipeline de transformación de unidades de proyecto. Esta funcionalidad agrupa intervenciones en unidades de proyecto basándose en su ubicación física y similitud textual, reduciendo significativamente la duplicación de datos.

**Fecha de integración**: 18 de Diciembre, 2025
**Versión**: 1.0

---

## 🎯 Cambios Implementados

### 1. Nuevo Módulo: `geospatial_clustering.py`

**Ubicación**: `transformation_app/geospatial_clustering.py`

Módulo especializado que implementa:

- ✅ **DBSCAN Clustering**: Agrupa registros por proximidad GPS (< 20 metros)
- ✅ **Fuzzy Matching**: Agrupa registros sin coordenadas por similitud textual
- ✅ **Exclusión de Subsidios**: Cada subsidio es una unidad independiente
- ✅ **Diferenciación por `nombre_up_detalle`**: Sedes diferentes = unidades diferentes
- ✅ **Estructura jerárquica**: Unidades de Proyecto → Intervenciones

### 2. Actualización en `data_transformation_unidades_proyecto.py`

**Cambios**:

1. **Nueva función**: `generate_upid_for_records(df, use_clustering=True)`

   - Parámetro `use_clustering`: Activa/desactiva clustering geoespacial
   - Fallback automático a método simple si clustering falla

2. **Función legacy**: `generate_upid_for_records_simple(df)`

   - Método anterior preservado para compatibilidad
   - Se usa como fallback o cuando `use_clustering=False`

3. **Importación del módulo de clustering**:
   ```python
   from geospatial_clustering import (
       agrupar_datos_geoespacial,
       convert_unidades_to_dataframe
   )
   ```

### 3. Campos Modificados

**Eliminados**:

- ❌ `cluster_original` (a nivel de unidad)
- ❌ `intervencion_num` (a nivel de intervención)

**Agregados**:

- ✅ `n_intervenciones` (a nivel de unidad) - Contador de intervenciones por unidad
- ✅ `intervencion_id` - Formato: `UNP-###-##` (ya existía, se mantiene)

### 4. Dependencias Nuevas

Agregadas a `requirements.txt`:

```txt
scikit-learn>=1.3.0      # Para DBSCAN
rapidfuzz>=3.0.0         # Para fuzzy matching
unidecode>=1.3.0         # Para normalización de texto
```

**Instalación**:

```bash
pip install scikit-learn rapidfuzz unidecode
```

---

## 🚀 Cómo Usar

### Opción 1: En el Pipeline Completo

El pipeline ahora usa clustering geoespacial **por defecto**:

```python
from pipelines.unidades_proyecto_pipeline import run_full_pipeline

# Ejecutar pipeline completo con clustering
result = run_full_pipeline()
```

### Opción 2: Solo Transformación con Clustering

```python
from transformation_app.data_transformation_unidades_proyecto import (
    transform_and_save_unidades_proyecto
)

# Transformar datos con clustering geoespacial
gdf = transform_and_save_unidades_proyecto(
    data=None,              # Extrae desde Google Drive
    use_extraction=True,
    upload_to_s3=True
)
```

### Opción 3: Desactivar Clustering (Usar Método Simple)

Si necesitas usar el método simple por alguna razón:

```python
from transformation_app.data_transformation_unidades_proyecto import (
    generate_upid_for_records
)

# Generar UPIDs sin clustering
df_simple = generate_upid_for_records(df, use_clustering=False)
```

### Opción 4: Clustering Directo

```python
from transformation_app.geospatial_clustering import (
    agrupar_datos_geoespacial,
    convert_unidades_to_dataframe
)

# Aplicar clustering a un DataFrame
unidades_dict = agrupar_datos_geoespacial(df)

# Convertir a DataFrame plano
df_with_upids = convert_unidades_to_dataframe(unidades_dict)
```

---

## 🧪 Pruebas

### Script de Prueba de Integración

**Ubicación**: `scripts/test_integration_clustering.py`

Este script verifica que la integración funciona correctamente:

```bash
python scripts/test_integration_clustering.py
```

**Verifica**:

- ✅ Extracción de datos funcional
- ✅ Clustering geoespacial ejecuta sin errores
- ✅ Campos nuevos (`n_intervenciones`, `intervencion_id`) presentes
- ✅ Campos antiguos (`cluster_original`, `intervencion_num`) eliminados
- ✅ Fallback a método simple funcional
- ✅ Comparación de resultados entre métodos

### Scripts de Prueba Existentes

Los scripts de prueba originales siguen funcionando:

```bash
# Prueba de clustering geoespacial completa
python scripts/test_agrupacion_geoespacial.py

# Prueba de método simple
python scripts/test_agrupacion_unidades_intervenciones.py
```

---

## 📊 Resultados Esperados

### Comparación de Métodos

**Dataset**: 1,695 registros totales

| Métrica                   | Método Simple | Clustering Geoespacial | Mejora         |
| ------------------------- | ------------- | ---------------------- | -------------- |
| **Unidades de Proyecto**  | 1,579         | 1,379                  | -200 (12.67%)  |
| **Intervenciones/Unidad** | 1.07          | 1.23                   | +0.16          |
| **Agrupables**            | 578           | 378                    | -200 (34.6%)   |
| **Subsidios**             | 1,001         | 1,001                  | 0 (sin cambio) |

### Desglose por Tipo de Agrupación

**Clustering Geoespacial**:

- 📍 Clusters por GPS (DBSCAN): 214
- 🔤 Clusters por fuzzy matching: 164
- 💰 Subsidios individuales: 1,001
- **Total unidades**: 1,379

---

## 🔧 Configuración Avanzada

### Parámetros Ajustables

En `transformation_app/geospatial_clustering.py`:

```python
# Radio de agrupación DBSCAN (metros)
CLUSTERING_RADIUS_METERS = 20

# Umbral de similitud para fuzzy matching (0-100)
FUZZY_THRESHOLD = 85
```

**Recomendaciones**:

- **Radio más pequeño** (10-15m): Unidades más granulares, menos agrupación
- **Radio más grande** (30-50m): Más agrupación, posible sobre-consolidación
- **Threshold más alto** (90-95): Matching más estricto, menos agrupación por texto
- **Threshold más bajo** (75-80): Matching más permisivo, más agrupación

---

## 📁 Estructura de Datos Resultante

### Estructura Jerárquica

```json
{
  "UNP-1": {
    "upid": "UNP-1",
    "n_intervenciones": 4,
    "nombre_up": "I.E. LUIS FERNANDO CAICEDO",
    "nombre_up_detalle": "Principal",
    "comuna_corregimiento": "El Saladito",
    "direccion": "AVENIDA 43 OESTE 5A 49",
    "tipo_equipamiento": "Instituciones Educativas",
    "lat": null,
    "lon": null,
    "intervenciones": [
      {
        "intervencion_id": "UNP-1-01",
        "referencia_proceso": "...",
        "referencia_contrato": "...",
        "bpin": "2023760010146",
        "estado": "En alistamiento",
        "presupuesto_base": 707333965,
        "avance_obra": 0.0,
        ...
      },
      {
        "intervencion_id": "UNP-1-02",
        ...
      }
    ]
  }
}
```

### DataFrame Plano (para pipeline)

El módulo convierte automáticamente la estructura jerárquica a DataFrame plano:

| upid  | n_intervenciones | intervencion_id | nombre_up    | ... | estado          | presupuesto_base |
| ----- | ---------------- | --------------- | ------------ | --- | --------------- | ---------------- |
| UNP-1 | 4                | UNP-1-01        | I.E. LUIS... | ... | En alistamiento | 707333965        |
| UNP-1 | 4                | UNP-1-02        | I.E. LUIS... | ... | En alistamiento | 158456961        |
| UNP-1 | 4                | UNP-1-03        | I.E. LUIS... | ... | En alistamiento | 635826336        |
| UNP-1 | 4                | UNP-1-04        | I.E. LUIS... | ... | Terminado       | 107039840        |

---

## ⚠️ Consideraciones Importantes

### 1. Tiempo de Ejecución

El clustering geoespacial es **más lento** que el método simple:

- **Método Simple**: ~1-2 segundos para 1,695 registros
- **Clustering Geoespacial**: ~10-15 segundos para 1,695 registros

**Recomendación**: El tiempo adicional vale la pena por la reducción de duplicados.

### 2. Memoria

DBSCAN puede consumir más memoria con datasets muy grandes (>10,000 registros).

**Solución**: El algoritmo procesa en dos fases (agrupables vs subsidios) para optimizar.

### 3. Subsidios

Los subsidios **NUNCA** se agrupan, sin importar su ubicación o nombre.

**Razón**: Cada beneficiario de subsidio es único y debe mantener su propia unidad.

### 4. Coordenadas Faltantes

Registros sin coordenadas GPS usan **fuzzy matching** basado en texto.

**Limitación**: Menos preciso que clustering geoespacial, pero mejor que nada.

---

## 🐛 Resolución de Problemas

### Error: "Could not import clustering module"

**Causa**: Dependencias no instaladas o módulo no encontrado.

**Solución**:

```bash
# Instalar dependencias
pip install scikit-learn rapidfuzz unidecode

# Verificar que el archivo existe
ls transformation_app/geospatial_clustering.py
```

### Error: "Clustering failed, using fallback"

**Causa**: Error en el proceso de clustering (datos inválidos, memoria insuficiente).

**Resultado**: El sistema automáticamente usa el método simple como fallback.

**Acción**: Revisar logs para detalles del error. El pipeline continúa funcionando.

### Resultados Inesperados

**Problema**: Demasiadas unidades o muy pocas.

**Solución**: Ajustar parámetros `CLUSTERING_RADIUS_METERS` y `FUZZY_THRESHOLD`.

**Debug**: Ejecutar scripts de prueba con diferentes parámetros:

```bash
python scripts/test_agrupacion_geoespacial.py
```

---

## 📚 Referencias

- **Documentación técnica**: [`docs/COMPARACION_CLUSTERING_GEOESPACIAL.md`](../docs/COMPARACION_CLUSTERING_GEOESPACIAL.md)
- **Diferenciación por detalle**: [`docs/DIFERENCIACION_NOMBRE_UP_DETALLE.md`](../docs/DIFERENCIACION_NOMBRE_UP_DETALLE.md)
- **Resultados sin subsidios**: [`docs/RESULTADOS_CLUSTERING_SIN_SUBSIDIOS.md`](../docs/RESULTADOS_CLUSTERING_SIN_SUBSIDIOS.md)

---

## ✅ Checklist de Integración

Antes de hacer commit, verificar:

- [ ] Dependencias instaladas (`scikit-learn`, `rapidfuzz`, `unidecode`)
- [ ] Script de prueba ejecuta sin errores
- [ ] Pipeline completo ejecuta correctamente
- [ ] Campos antiguos (`cluster_original`, `intervencion_num`) eliminados
- [ ] Campo nuevo (`n_intervenciones`) presente
- [ ] Documentación actualizada
- [ ] Tests pasan exitosamente

---

## 📞 Soporte

Si tienes problemas con la integración:

1. Ejecuta el script de prueba: `python scripts/test_integration_clustering.py`
2. Revisa los logs en `logs/`
3. Verifica que las dependencias estén instaladas
4. Consulta la documentación en `docs/`

---

**Última actualización**: 18 de Diciembre, 2025
**Versión del módulo**: 1.0
**Estado**: ✅ Listo para producción
