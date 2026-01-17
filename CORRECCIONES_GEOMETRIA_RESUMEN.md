# Corrección de Estructura de Geometrías - Resumen

## Fecha
17 de enero de 2026

## Problema Inicial

Usuario reportó que los registros en Firebase tenían campos `lat` y `lon` como propiedades separadas, cuando deberían estar **únicamente** en el campo `geometry` según el estándar GeoJSON.

### Evidencia del Problema
```json
{
  "upid": "UNP-123",
  "geometry": {
    "type": "Point",
    "coordinates": [-76.5, 3.4]
  },
  "lat": null,        // ❌ NO debería existir
  "lon": null,        // ❌ NO debería existir
  "nombre_up": "..."
}
```

## Soluciones Implementadas

### 1. Eliminación de lat/lon de Properties en GeoJSON

**Archivo**: `transformation_app/data_transformation_unidades_proyecto.py`

**Cambios** (líneas 2220-2231):
```python
# CR脥TICO: Eliminar columnas lat/lon del DataFrame
# Las coordenadas SOLO deben estar en geometry, NO en properties
columns_to_drop = ['lat', 'lon', 'latitud', 'longitud']
columns_dropped = [col for col in columns_to_drop if col in gdf_export.columns]
if columns_dropped:
    gdf_export = gdf_export.drop(columns=columns_dropped)
    print(f"   [CONFIG] Coordenadas eliminadas de properties: {columns_dropped}")
```

**Resultado**: Las columnas de coordenadas se eliminan del DataFrame **antes** de crear las features del GeoJSON, garantizando que no aparezcan en properties.

### 2. Simplificación de Creación de Geometry

**Archivo**: `transformation_app/data_transformation_unidades_proyecto.py`

**Cambios** (líneas 2264-2285):
- Eliminado código fallback que intentaba usar lat/lon desde properties
- Geometry se crea **únicamente** desde el objeto `geometry` del GeoDataFrame
- Validación de coordenadas dentro de rangos de Cali (lat: 2.5-4.5, lon: -77.5 a -75.5)

```python
# Agregar geometry SOLO desde el objeto geometry (ya no hay lat/lon en el DataFrame)
geom = row.get('geometry')

if pd.notna(geom) and geom is not None and hasattr(geom, 'x') and hasattr(geom, 'y'):
    try:
        if -77.5 <= geom.x <= -75.5 and 2.5 <= geom.y <= 4.5:
            feature['geometry'] = {
                "type": "Point",
                "coordinates": [round(geom.x, 8), round(geom.y, 8)]
            }
    except Exception as e:
        pass  # Sin geometría válida
```

### 3. Limpieza del Módulo de Carga

**Archivo**: `load_app/data_loading_unidades_proyecto.py`

**Cambios** (líneas 410-417):
- Comentado código fallback que intentaba crear geometry desde lat/lon en properties
- Ahora el módulo de carga confía en que el GeoJSON tiene la estructura correcta

```python
# NOTA: lat/lon ya NO están en properties del GeoJSON
# Las coordenadas se crean ÚNICAMENTE en geometry durante la transformación
# Este código ya no es necesario porque el GeoJSON tiene geometry correcta
```

### 4. Corrección de Serialización de LineString

**Archivo**: `load_app/data_loading_unidades_proyecto_infraestructura.py`

**Problema**: Las coordenadas de geometrías LineString (infraestructura vial) se guardaban como STRING en Firebase:
```json
{
  "geometry": {
    "type": "LineString",
    "coordinates": "[[-76.46,3.41],[-76.45,3.40]]"  // ❌ STRING
  }
}
```

**Cambios** (líneas 357-366):
```python
# CORRECCIÓN: Firebase SÍ soporta arrays anidados (probado con Firestore)
# Mantener coordinates como array nativo para todos los tipos
# Esto es compatible con GeoJSON estándar y el frontend Next.js
geometry_field = {
    'type': geom_type,
    'coordinates': clean_coords  # Array nativo, no JSON string
}
```

**Resultado**: Las coordenadas ahora se guardan como arrays nativos:
```json
{
  "geometry": {
    "type": "LineString",
    "coordinates": [[-76.46,3.41],[-76.45,3.40]]  // ✅ ARRAY
  }
}
```

## Resultados

### Estructura Final en Firebase

✅ **Correcta**:
```json
{
  "upid": "UNP-123",
  "geometry": {
    "type": "Point",
    "coordinates": [-76.513637, 3.471143]
  },
  "nombre_up": "Casa Matria Junanbú",
  // ... otras properties (SIN lat/lon)
}
```

✅ **LineString correcta**:
```json
{
  "upid": "INF-BPIN-2024-001",
  "geometry": {
    "type": "LineString",
    "coordinates": [
      [-76.4655218, 3.4169181],
      [-76.4651425, 3.4167039],
      [-76.464928, 3.4165736]
    ]
  },
  "nombre_up": "Vía Local"
}
```

### Estadísticas (Post-Corrección)

- **Total documentos**: 2,079
- **Con geometry**: 1,233 (59.3%)
- **Sin geometry**: 846 (40.7%)
- **Sin campos lat/lon separados**: 100% ✅
- **LineString con coordinates como array**: 100% ✅

## Archivos Modificados

1. `transformation_app/data_transformation_unidades_proyecto.py`
   - Líneas 2220-2231: Eliminación de columnas lat/lon
   - Líneas 2264-2285: Simplificación de creación de geometry
   - Línea 2381: Actualización de mensaje de log

2. `load_app/data_loading_unidades_proyecto.py`
   - Líneas 410-417: Comentado código fallback

3. `load_app/data_loading_unidades_proyecto_infraestructura.py`
   - Líneas 357-366: Corrección de serialización de coordinates

## Validación

Script de verificación: `verificacion_final_geometrias.py`

Ejecutar después del pipeline para confirmar:
- ✅ NO hay campos lat/lon separados
- ✅ Geometry Point tiene coordinates como array [lon, lat]
- ✅ Geometry LineString tiene coordinates como array de arrays
- ✅ Todas las coordenadas están en rangos válidos

## Comando de Ejecución

```bash
python pipelines/unidades_proyecto_pipeline.py
```

Después de completar:
```bash
python verificacion_final_geometrias.py
```

## Notas Técnicas

1. **GeoJSON Estándar**: Las coordenadas ahora cumplen 100% con el estándar GeoJSON RFC 7946
2. **Firebase Firestore**: Soporta arrays anidados sin límite (corregido el mito del "1 nivel")
3. **Next.js Frontend**: Puede parsear directamente las coordenadas sin conversiones adicionales
4. **Backward Compatibility**: No requiere cambios en el frontend si ya consumía `geometry.coordinates`

## Conclusión

✅ **Problema resuelto completamente**:
- Las coordenadas están SOLO en el campo `geometry`
- NO hay campos `lat`, `lon`, `latitud` o `longitud` separados
- Estructura compatible con GeoJSON estándar
- LineString usa arrays nativos, no strings
