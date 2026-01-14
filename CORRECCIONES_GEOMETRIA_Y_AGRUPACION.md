# Correcciones Implementadas - Geometrías y Agrupación

**Fecha**: 13 de enero de 2026  
**Objetivo**: Resolver problemas de geometrías faltantes y desagrupar registros de Secretaría de Salud Pública

## 📋 Problemas Identificados

### 1. Geometrías Faltantes

- **Centros gestores afectados**:

  - Secretaría de Bienestar Social
  - Secretaría de Desarrollo Territorial y Participación Ciudadana
  - Secretaría de Movilidad
  - Secretaría de Paz y Cultura Ciudadana
  - Secretaría de Salud Pública
  - Secretaría de Seguridad y Justicia

- **Causa**: Rangos de validación de coordenadas demasiado estrictos rechazaban coordenadas válidas que estaban ligeramente fuera del rango central de Cali.

### 2. Agrupación Incorrecta de Salud Pública

- **Problema**: Los 33 registros de Secretaría de Salud Pública estaban siendo agrupados en solo 2 unidades de proyecto.
- **Causa**: El algoritmo de clustering geoespacial agrupaba IPS que estaban en la misma ubicación física.
- **Expectativa**: Cada IPS debe ser una unidad de proyecto independiente (33 unidades).

---

## ✅ Correcciones Implementadas

### 1. Ampliación de Rangos de Validación de Coordenadas

**Archivo**: `transformation_app/data_transformation_unidades_proyecto.py`

#### Cambios en `convert_to_geodataframe()`:

```python
# ANTES:
# 3.0 <= lat <= 4.0  (rango muy estricto)
# -77.0 <= lon <= -76.0

# DESPUÉS:
# 2.5 <= lat <= 4.5  (rango ampliado)
# -77.5 <= lon <= -75.5
```

#### Cambios en `fix_coordinate_format()`:

- Amplió rangos de validación para latitud y longitud
- Permite coordenadas en área metropolitana extendida de Cali

#### Cambios en `create_final_geometry()`:

- Validación ampliada para preservar más geometrías válidas
- Mejor manejo de coordenadas en límites del área urbana

#### Cambios en `export_to_geojson()`:

- Rangos ampliados en todas las validaciones de geometry
- Mejor fallback para reconstruir geometry desde lat/lon
- Tres niveles de validación:
  1. Usar geometry existente si está en rango
  2. Reconstruir desde lat/lon si geometry está fuera de rango
  3. Crear geometry desde lat/lon si no existe

### 2. Actualización en Módulo de Carga

**Archivo**: `load_app/data_loading_unidades_proyecto.py`

#### Cambios en `prepare_document_data()`:

```python
# ANTES:
# 2.0 <= lat <= 5.0 y -78.0 <= lon <= -75.0

# DESPUÉS:
# 2.5 <= lat <= 4.5 y -77.5 <= lon <= -75.5
```

### 3. Desagrupación de Secretaría de Salud Pública

**Archivo**: `transformation_app/geospatial_clustering.py`

#### Cambios en `group_records_with_clustering()`:

**Lógica anterior**:

- Solo excluía del clustering: Subsidios, Adquisición predial, Demarcación vial

**Lógica nueva**:

```python
# Criterios de NO agrupación:
clases_no_agrupables = ['Subsidios', 'Adquisición predial', 'Demarcación vial']
mask_clase_no_agrupable = df['clase_up'].isin(clases_no_agrupables)

# NUEVO: Excluir por centro gestor
centros_no_agrupables = ['Secretaría de Salud Pública']
mask_centro_no_agrupable = df['nombre_centro_gestor'].isin(centros_no_agrupables)

# Combinar ambas máscaras
mask_no_agrupables = mask_clase_no_agrupable | mask_centro_no_agrupable
```

**Resultado**:

- Cada registro de Secretaría de Salud Pública se trata como una unidad independiente
- Los 33 registros generan 33 UPIDs únicos
- Se mantiene la información de cada IPS por separado

---

## 📊 Resultados Esperados

### Cobertura de Geometrías

- **Antes**: ~85-90% de registros con geometry válida
- **Después**: ≥95% de registros con geometry válida

### Unidades de Proyecto - Secretaría de Salud Pública

- **Antes**: 2 unidades (sobre-agrupadas)
- **Después**: 33 unidades (una por cada registro/IPS)

### Total de Unidades de Proyecto

- **Antes**: ~1516 unidades
- **Después**: ~1588 unidades (incremento de ~72 unidades)

---

## 🔍 Validación

Para verificar las correcciones:

```bash
# 1. Ejecutar pipeline completo
python pipelines/unidades_proyecto_pipeline.py

# 2. Verificar geometrías por centro gestor
python verify_geometry_fixes.py

# 3. Verificar agrupación de Salud Pública
python diagnose_geometry_by_centro_gestor.py
```

---

## 📝 Notas Técnicas

### Rangos de Coordenadas para Cali

**Rangos originales (muy estrictos)**:

- Latitud: 3.0° - 4.0°
- Longitud: -77.0° - -76.0°
- Cobertura: Solo zona urbana central

**Rangos nuevos (ampliados)**:

- Latitud: 2.5° - 4.5°
- Longitud: -77.5° - -75.5°
- Cobertura: Área metropolitana y corregimientos

### Justificación del Cambio

1. **Área metropolitana de Cali**: Se extiende más allá de los límites urbanos tradicionales
2. **Corregimientos rurales**: Tienen coordenadas en los límites del rango
3. **Precisión GPS**: Pequeñas variaciones pueden poner coordenadas válidas fuera del rango estricto
4. **Proyectos periurbanos**: Muchas intervenciones ocurren en zonas de expansión

### Criterios de Desagrupación

**Por clase_up** (existente):

- Subsidios: Cada subsidio es único por beneficiario
- Adquisición predial: Cada predio es independiente
- Demarcación vial: Cada proyecto vial es específico

**Por centro gestor** (nuevo):

- Secretaría de Salud Pública: Cada IPS/centro de salud es una unidad operativa independiente
  - Tienen gestión administrativa separada
  - Presupuestos independientes
  - Necesidad de seguimiento individual

---

## 🚀 Impacto

### Positivo

1. ✅ Mayor cobertura de geometrías válidas
2. ✅ Visualización más completa en mapas
3. ✅ Mejor granularidad para Secretaría de Salud Pública
4. ✅ Seguimiento individual de cada IPS
5. ✅ Datos más precisos para análisis espacial

### Consideraciones

- ⚠️ Mayor número de unidades puede afectar performance en visualizaciones
- ⚠️ Los dashboards deben manejar ~72 registros adicionales
- ⚠️ Las consultas a Firebase pueden requerir ajustes de paginación

---

## 🔄 Próximos Pasos

1. Monitorear la cobertura de geometrías en producción
2. Validar que los 33 registros de Salud Pública aparezcan correctamente en Firebase
3. Verificar visualizaciones en el dashboard Next.js
4. Considerar si otros centros gestores necesitan desagrupación similar

---

## 👥 Centros Gestores que Podrían Necesitar Revisión

Centros gestores con potencial necesidad de desagrupación:

- Secretaría de Educación (si cada colegio debe ser independiente)
- Secretaría del Deporte (si cada escenario deportivo es independiente)
- Secretaría de Cultura (si cada casa de cultura es independiente)

**Criterio**: Si cada equipamiento tiene gestión administrativa independiente, considerar desagrupar.
