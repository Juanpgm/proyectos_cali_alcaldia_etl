# Refactorización de Validación de Coordenadas

## Mejoras Implementadas en `data_transformation_unidades_proyecto.py`

**Fecha:** 14 de Noviembre, 2025  
**Archivo modificado:** `transformation_app/data_transformation_unidades_proyecto.py`

---

## 📋 Resumen Ejecutivo

Se ha realizado una refactorización completa del sistema de manejo de coordenadas geográficas en el módulo de transformación de unidades de proyecto. El objetivo principal es **garantizar que todas las coordenadas sean correctas y válidas para Cali, Colombia**, mediante validaciones robustas y correcciones automáticas.

---

## 🎯 Objetivos Logrados

1. ✅ **Validación específica para Cali, Colombia**

   - Rangos geográficos definidos: Lat 3.0° - 4.0° N, Lon -77.0° - -76.0° W

2. ✅ **Detección y corrección automática de errores comunes**

   - Errores de signo (latitudes negativas, longitudes positivas)
   - Errores de punto decimal (34.5 en lugar de 3.45)
   - Coordenadas en sistemas de referencia incorrectos

3. ✅ **Validación cruzada de pares lat/lon**

   - Detección de coordenadas intercambiadas
   - Corrección automática cuando es posible

4. ✅ **Validación de geometrías GeoJSON**
   - Validación de todos los puntos en líneas y polígonos
   - Eliminación de puntos fuera de rango
   - Preservación de la integridad geométrica

---

## 🔧 Cambios Técnicos Implementados

### 1. Constantes Geográficas de Cali

```python
# Nuevas constantes globales
CALI_LAT_MIN, CALI_LAT_MAX = 3.0, 4.0
CALI_LON_MIN, CALI_LON_MAX = -77.0, -76.0
```

### 2. Nueva Función de Validación

```python
def is_valid_cali_coordinate(lat: float, lon: float) -> bool:
    """
    Valida que las coordenadas caigan dentro de los límites de Cali.
    """
```

### 3. Función `normalize_coordinate_value` Mejorada

**Antes:** Validación genérica con rangos WGS84 globales (-180 a 180, -90 a 90)

**Ahora:**

- Validación específica por tipo de coordenada (`coord_type='lat'` o `'lon'`)
- Corrección automática de errores comunes:
  - ✅ Signo incorrecto
  - ✅ Punto decimal desplazado
  - ✅ Valores en sistemas de coordenadas incorrectos
- Logging detallado de correcciones aplicadas

**Ejemplo de corrección automática:**

```python
# Input: lat = -3.45 (signo incorrecto)
# Output: lat = 3.45 (corregido automáticamente)
# Log: "Auto-corrected latitude: -3.45 → 3.45 (removed negative sign)"
```

### 4. Función `normalize_coordinates_array` Mejorada

**Antes:** Normalización básica sin validación de rangos

**Ahora:**

- Soporte para múltiples formatos: `'lon_lat'` (GeoJSON) y `'lat_lon'` (custom)
- Validación específica por tipo para cada coordenada
- Validación cruzada del par completo
- Rechazo de pares inválidos incluso si individualmente están en rango

**Características:**

```python
# Valida que AMBAS coordenadas estén en rango
if is_valid_cali_coordinate(lat, lon):
    return [lat, lon]  # Par válido
else:
    return None  # Par inválido - fuera de Cali
```

### 5. Función `parse_geojson_geometry` Mejorada

**Antes:** Parsing básico con normalización simple

**Ahora:**

- Validación de formato de coordenadas (`expected_format` parameter)
- Validación completa de estructura GeoJSON
- Logging mejorado de errores específicos
- Validación recursiva de coordenadas anidadas

### 6. Función `normalize_geojson_coordinates` Mejorada

**Mejoras principales:**

- Validación de cada punto contra límites de Cali
- Manejo robusto de `LineString` y `Polygon`:
  - Eliminación de puntos inválidos
  - Preservación de geometrías válidas
  - Logging de puntos removidos
- Validación de mínimos de puntos para geometrías válidas:
  - LineString: mínimo 2 puntos
  - Polygon: mínimo 4 puntos (anillo cerrado)

**Ejemplo:**

```python
# Input: LineString con 5 puntos, 2 fuera de Cali
# Output: LineString con 3 puntos válidos
# Log: "LineString: 2 points outside Cali bounds (removed)"
```

### 7. Función `validate_and_fix_coordinate_format` Mejorada

**Nuevas capacidades:**

1. **Validación individual con tipo específico**

   ```python
   normalized_lat = normalize_coordinate_value(lat_val, coord_type='lat')
   normalized_lon = normalize_coordinate_value(lon_val, coord_type='lon')
   ```

2. **Detección de coordenadas intercambiadas**

   ```python
   # Si el par [lat, lon] no es válido, intenta [lon, lat]
   if not is_valid_cali_coordinate(lat, lon):
       if is_valid_cali_coordinate(lon, lat):
           # Intercambia las coordenadas
           lat, lon = lon, lat
   ```

3. **Estadísticas detalladas**
   - Coordenadas válidas
   - Correcciones aplicadas
   - Coordenadas intercambiadas
   - Coordenadas inválidas no corregibles

### 8. Función `create_point_from_coordinates` Mejorada

**Antes:** Validación básica WGS84

**Ahora:**

- Validación con tipo específico para lat/lon
- Validación final del par completo contra límites de Cali
- Logging de rechazos con coordenadas específicas
- Manejo robusto de múltiples nombres de columna

### 9. Función `perform_spatial_intersection` Mejorada

**Mejoras clave:**

1. **Pre-validación de coordenadas**

   - Todas las coordenadas se validan antes de intersección
   - Coordenadas fuera de Cali se rechazan antes de procesamiento

2. **Estadísticas mejoradas**

   ```python
   - Total records: 500
   - Processed: 500
   - Successful intersections: 450 (90.0%)
   - Invalid coordinates (outside Cali): 20
   - No intersection found: 30
   ```

3. **Manejo robusto de errores**

   - Validación de estructura de geometrías antes de usar
   - Logging de errores específicos por feature
   - Continuación del proceso ante errores individuales

4. **Formato de salida mejorado**
   - Distribución de valores con porcentajes
   - Top 5 valores encontrados
   - Conteo de "REVISAR" vs valores válidos

---

## 📊 Validaciones Implementadas

### Nivel 1: Validación Individual de Coordenadas

- ✅ Tipo de dato correcto (float)
- ✅ Rango válido para Cali
- ✅ No nulo/vacío
- ✅ Corrección automática de errores comunes

### Nivel 2: Validación de Pares

- ✅ Ambas coordenadas presentes
- ✅ Par completo dentro de límites de Cali
- ✅ Detección de intercambios
- ✅ Validación cruzada lat/lon

### Nivel 3: Validación de Geometrías

- ✅ Estructura GeoJSON válida
- ✅ Tipo de geometría válido
- ✅ Todos los puntos validados
- ✅ Geometrías con suficientes puntos válidos

### Nivel 4: Validación Espacial

- ✅ Intersección con polígonos de barrios/comunas
- ✅ Coordenadas dentro de límites administrativos
- ✅ Validación de pertenencia geográfica

---

## 🚀 Beneficios

1. **Calidad de Datos Mejorada**

   - Todas las coordenadas garantizadas dentro de Cali
   - Reducción de errores de geolocalización
   - Corrección automática de problemas comunes

2. **Debugging Facilitado**

   - Logging detallado de todas las correcciones
   - Estadísticas comprensivas en cada paso
   - Identificación clara de problemas

3. **Mantenibilidad**

   - Código bien documentado
   - Funciones con responsabilidad única
   - Validación consistente en todo el pipeline

4. **Robustez**
   - Manejo de múltiples formatos de entrada
   - Tolerancia a errores sin pérdida de datos válidos
   - Validación en múltiples niveles

---

## 📈 Estadísticas de Validación

El sistema ahora reporta estadísticas detalladas en cada fase:

### Ejemplo de salida de validación de coordenadas:

```
============================================================
VALIDATING AND FIXING COORDINATE FORMAT
============================================================
  Expected ranges for Cali, Colombia:
    Latitude:  3.0° to 4.0° N
    Longitude: -77.0° to -76.0° W

  Latitude validation:
    - Valid (already correct): 450
    - Auto-fixed: 30
    - Invalid/Cannot fix: 20

  Longitude validation:
    - Valid (already correct): 460
    - Auto-fixed: 25
    - Invalid/Cannot fix: 15

  Cross-validation:
    - Swapped coordinates detected and fixed: 10

  Summary:
    - Total records: 500
    - Records with both valid coordinates: 470 (94.0%)
    - Total fixes applied: 65
    ⚠️  WARNING: 30 coordinate values could not be fixed!
```

### Ejemplo de salida de intersección espacial:

```
============================================================
SPATIAL INTERSECTION: barrios_veredas.geojson
============================================================
  ✓ Loaded 249 features from GeoJSON

  Results:
    - Total records: 500
    - Processed: 500
    - Successful intersections: 450 (90.0%)
    - No/invalid geometry: 15
    - Invalid coordinates (outside Cali): 5
    - No intersection found: 30

  Distribution of barrio_vereda_val:
    - Valid values: 450
    - REVISAR: 50

  Top 5 values:
    • EL REFUGIO: 25
    • PANCE: 20
    • MELÉNDEZ: 18
    • CIUDAD JARDÍN: 15
    • LIMONAR: 12
```

---

## 🔍 Casos de Uso Cubiertos

### 1. Coordenadas con signo incorrecto

```python
Input:  lat = -3.4567, lon = 76.5432
Output: lat = 3.4567, lon = -76.5432
```

### 2. Coordenadas con punto decimal desplazado

```python
Input:  lat = 34.567, lon = -765.432
Output: lat = 3.4567, lon = -76.5432
```

### 3. Coordenadas intercambiadas

```python
Input:  lat = -76.5432, lon = 3.4567
Output: lat = 3.4567, lon = -76.5432
```

### 4. Geometrías con puntos inválidos

```python
Input:  LineString con 5 puntos (2 fuera de Cali)
Output: LineString con 3 puntos válidos
```

### 5. Validación de intersección espacial

```python
Input:  Point(3.4567, -76.5432)
Validation: Dentro de barrio "EL REFUGIO" ✓
Output: barrio_vereda_val = "EL REFUGIO"
```

---

## 🛡️ Garantías del Sistema

1. **Ninguna coordenada inválida para Cali pasa las validaciones**

   - Todas las coordenadas están en rango [3.0-4.0, -77.0--76.0]

2. **Las correcciones automáticas son seguras**

   - Solo se aplican cuando el resultado está dentro de rangos válidos
   - Se loggea cada corrección para auditoría

3. **Los datos válidos nunca se pierden**

   - Sistema tolerante a errores individuales
   - Preservación de geometrías parcialmente válidas

4. **Trazabilidad completa**
   - Logging detallado de todas las operaciones
   - Estadísticas comprensivas en cada fase
   - Identificación clara de registros problemáticos

---

## 📝 Notas de Implementación

### Formato de Coordenadas

El sistema maneja dos formatos:

- **GeoJSON estándar:** `[longitude, latitude]`
- **Formato custom:** `[latitude, longitude]`

El formato se especifica mediante el parámetro `expected_format` en las funciones relevantes.

### Límites Geográficos de Cali

Los límites se basan en el área metropolitana de Cali:

- **Latitud:** 3.0° a 4.0° N (aproximadamente)
- **Longitud:** -77.0° a -76.0° W (aproximadamente)

Estos límites incluyen un margen para cubrir toda el área urbana y rural del municipio.

### Manejo de Errores

- **Errores corregibles:** Se corrigen automáticamente con logging
- **Errores no corregibles:** Se marcan como `None` o `ERROR`
- **Errores de geometría:** Se preservan puntos válidos cuando sea posible

---

## 🔮 Mejoras Futuras Sugeridas

1. **Validación contra límites oficiales**

   - Usar límites exactos del IGAC
   - Validación contra shapes oficiales del municipio

2. **Machine Learning para detección de errores**

   - Aprender patrones de errores comunes
   - Sugerencias de corrección más inteligentes

3. **Validación de elevación**

   - Validar coordenada Z cuando esté disponible
   - Verificar contra modelo digital de elevación

4. **Integración con APIs de geocoding**
   - Verificación cruzada con servicios de mapas
   - Corrección basada en direcciones cuando coordenadas fallan

---

## ✅ Conclusión

La refactorización implementada garantiza que todas las coordenadas procesadas sean válidas para Cali, Colombia, mediante:

1. **Validación multi-nivel** desde coordenadas individuales hasta geometrías completas
2. **Corrección automática** de errores comunes con logging detallado
3. **Detección de patrones** de error como coordenadas intercambiadas
4. **Trazabilidad completa** con estadísticas comprensivas en cada fase
5. **Robustez** mediante manejo de errores y preservación de datos válidos

El sistema está listo para procesar datos de entrada con diversos problemas de calidad y garantizar que solo coordenadas válidas lleguen a las etapas siguientes del pipeline.
