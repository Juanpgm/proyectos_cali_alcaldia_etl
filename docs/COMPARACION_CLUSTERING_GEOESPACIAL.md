# 🌍 Comparación: Agrupación Simple vs Geoespacial

## 📊 Resultados de la Prueba

### Método 1: Agrupación Simple (Hash de Campos)

- **Unidades de Proyecto**: 936
- **Intervenciones**: 1,695
- **Promedio**: 1.81 intervenciones por unidad

### Método 2: Agrupación Geoespacial (DBSCAN + Fuzzy Matching)

- **Unidades de Proyecto**: 635
- **Intervenciones**: 1,695
- **Promedio**: 2.67 intervenciones por unidad

## 🎯 Mejoras Logradas

✅ **Reducción de Unidades Duplicadas**: **301 unidades (32.16%)**
✅ **Mejor Consolidación**: +0.86 intervenciones promedio por unidad
✅ **Datos Más Limpios**: 635 ubicaciones físicas únicas vs 936 grupos textuales

## 🔬 ¿Cómo Funciona?

### 1. Clustering Geoespacial con DBSCAN

```python
# Para registros CON coordenadas (lat/lon)
- Radio de búsqueda: 20 metros
- Algoritmo: DBSCAN con métrica Haversine
- Resultado: 472 clusters geográficos
```

**Ventaja**: Las coordenadas GPS son la "verdad fundamental". Si dos registros tienen:

- `nombre_up`: "I.E. Pance" vs "Institución Educativa Pance"
- `direccion`: "Calle 5" vs "Cll 5"

Pero están a 10 metros de distancia → **Mismo lugar, se agrupan automáticamente**

### 2. Fuzzy Matching para Registros sin Coordenadas

```python
# Para registros SIN coordenadas (533 registros)
- Normalización de texto (sin acentos, minúsculas)
- Eliminación de stopwords (I.E., IPS, sede, etc.)
- Similitud Jaro-Winkler: 85% umbral
- Resultado: 163 clusters textuales
```

**Ventaja**: Detecta variaciones en nombres:

- "Puesto de Salud La Flora" ≈ "Puesto Salud La Flora"
- "Biblioteca Central" ≈ "Biblioteca Central Sede Principal"

### 3. Consolidación Inteligente

Cuando múltiples registros se agrupan, el sistema:

1. **Nombres**: Toma el más frecuente (moda) o el más largo (más completo)
2. **Coordenadas**: Promedia lat/lon de todos los registros del grupo
3. **Otros campos**: Toma el primer valor no nulo o el más común

## 📈 Impacto en el Almacenamiento

### Antes (Agrupación Simple)

```
Firebase: 936 documentos
Redundancia: Alta (mismo lugar, múltiples nombres)
```

### Después (Agrupación Geoespacial)

```
Firebase: 635 documentos (-32%)
Redundancia: Mínima (ubicaciones físicas únicas)
Consistencia: Mejorada (datos consolidados)
```

## 🔍 Ejemplo Real

### Caso: Instituciones Educativas

**Método Simple** encontró 10 "unidades" diferentes:

```
UNP-45: "I.E. Pance"
UNP-67: "Institución Educativa Pance"
UNP-89: "IE Pance Sede A"
UNP-123: "PANCE I.E."
... (y 6 más con variaciones)
```

**Método Geoespacial** las agrupó en **1 única unidad**:

```
UNP-45: "Institución Educativa Pance"
  ├─ Intervención 01: Construcción aula 2020
  ├─ Intervención 02: Mejoramiento infraestructura 2021
  ├─ Intervención 03: Dotación equipos 2022
  ├─ ... (10 intervenciones en total)
```

**Consolidación**:

- Nombre elegido: "Institución Educativa Pance" (más completo)
- Coordenadas: Promedio de las 10 coordenadas registradas
- Dirección: "Carrera 100 # 16-00" (valor más frecuente)

## ⚙️ Configuración Ajustable

### Radio de Clustering

```python
CLUSTERING_RADIUS_METERS = 20  # Cambiar según necesidad
```

- **20m**: Ideal para equipamientos (escuelas, hospitales)
- **50m**: Para parques o zonas amplias
- **100m**: Para infraestructura dispersa

### Umbral de Similitud Textual

```python
FUZZY_THRESHOLD = 85  # 0-100
```

- **90+**: Muy estricto (solo variaciones mínimas)
- **85**: Balanceado (recomendado)
- **80-**: Permisivo (puede agrupar cosas diferentes)

## 📝 Campos Agrupados

### Nivel Superior (Unidad de Proyecto)

- `nombre_up` ← Consolidado
- `nombre_up_detalle` ← Consolidado
- `comuna_corregimiento` ← Consolidado
- `barrio_vereda` ← Consolidado
- `direccion` ← Consolidado
- `tipo_equipamiento` ← Consolidado
- `lat` ← Promediado
- `lon` ← Promediado

### Nivel Inferior (Intervenciones)

- Todos los demás campos se mantienen sin cambios
- Cada intervención conserva su información original
- IDs asignados: `UNP-###-##`

## 🚀 Próximos Pasos

### 1. Validación Manual ✅

- Revisar archivos JSON generados
- Verificar que agrupaciones sean correctas
- Ajustar parámetros si es necesario

### 2. Integración en el Pipeline 🔲

```
Flujo ETL:
Extracción → Transformación → [CLUSTERING AQUÍ] → GeoJSON → Firebase
```

### 3. Migración de Datos Existentes 🔲

- Opción A: Limpiar Firebase y recargar todo
- Opción B: Migración incremental con mapeo de IDs antiguos

## 🎓 Conceptos Técnicos

### DBSCAN (Density-Based Spatial Clustering)

- **No requiere** especificar número de clusters
- **Maneja ruido**: Puntos aislados se identifican automáticamente
- **Formas arbitrarias**: Funciona con cualquier distribución espacial
- **Métrica Haversine**: Calcula distancias en superficie esférica (Tierra)

### Fuzzy Matching

- **Jaro-Winkler**: Algoritmo optimizado para nombres propios
- **Normalización**: Elimina ruido tipográfico
- **Token Set Ratio**: Compara palabras sin importar orden

## 📦 Dependencias Adicionales

```bash
pip install scikit-learn  # DBSCAN y clustering
pip install unidecode     # Normalización de texto
pip install rapidfuzz     # Fuzzy matching rápido
```

## 🔬 Métricas de Calidad

### Precisión del Clustering Geoespacial

- Registros con coordenadas: **1,162** (68.6%)
- Clusters creados: **472**
- Promedio por cluster: **2.46 registros**

### Precisión del Fuzzy Matching

- Registros sin coordenadas: **533** (31.4%)
- Clusters creados: **163**
- Promedio por cluster: **3.27 registros**

## ⚠️ Consideraciones

### Calidad de Coordenadas

- Si las coordenadas son incorrectas, DBSCAN puede separar ubicaciones que deberían estar juntas
- **Solución**: Validar coordenadas antes del clustering

### Nombres Muy Diferentes

- Fuzzy matching puede fallar si nombres son completamente distintos
- **Ejemplo**: "Parque Central" vs "Polideportivo Norte"
- **Solución**: Estos casos se mantienen separados (correcto)

### Rendimiento

- DBSCAN: O(n log n) con ball_tree
- Fuzzy matching: O(n²) para grupos sin coordenadas
- **Dataset actual**: < 5 segundos para 1,695 registros

## 📚 Referencias

- [Scikit-learn DBSCAN](https://scikit-learn.org/stable/modules/generated/sklearn.cluster.DBSCAN.html)
- [Rapidfuzz Documentation](https://rapidfuzz.github.io/RapidFuzz/)
- [Haversine Formula](https://en.wikipedia.org/wiki/Haversine_formula)

---

**Fecha de última actualización**: 18 de Diciembre, 2025
**Versión**: 1.0
**Autor**: Sistema ETL Cali Alcaldía
