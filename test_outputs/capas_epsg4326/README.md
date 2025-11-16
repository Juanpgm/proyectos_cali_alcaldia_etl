# Capas Geoespaciales en EPSG:4326 (WGS84)

## 🌍 Sistema de Coordenadas

**EPSG:4326 - WGS84 (World Geodetic System 1984)**
- **Tipo**: Sistema de coordenadas geográficas
- **Unidades**: Grados decimales (latitud/longitud)
- **Datum**: WGS84
- **Uso**: Estándar mundial para GPS, Google Earth, mapas web

## 📁 Estructura de Carpetas

```
capas_epsg4326/
├── shapefile/          # Archivos ESRI Shapefile en EPSG:4326
│   ├── ZONAS_SICOC_EPSG4326.*
│   ├── Unidades_Proyecto_EPSG4326.*
│   └── Superposicion_Espacial_EPSG4326.*
├── geojson/            # Archivos GeoJSON en EPSG:4326
│   ├── ZONAS_SICOC_EPSG4326.geojson
│   ├── Unidades_Proyecto_EPSG4326.geojson
│   └── Superposicion_Espacial_EPSG4326.geojson
├── kml/                # Archivos KML en EPSG:4326
│   ├── ZONAS_SICOC_EPSG4326.kml
│   ├── Unidades_Proyecto_EPSG4326.kml
│   └── Superposicion_Espacial_EPSG4326.kml
└── kmz/                # Archivos KMZ en EPSG:4326
    ├── ZONAS_SICOC_EPSG4326.kmz
    ├── Unidades_Proyecto_EPSG4326.kmz
    └── Superposicion_Espacial_EPSG4326.kmz
```

## 🗺️ Capas Disponibles

### 1. ZONAS_SICOC_EPSG4326
- **Descripción**: Polígonos de zonas SICOC
- **Tipo de geometría**: Polígono
- **CRS**: EPSG:4326 (WGS84)
- **Formatos**: .shp, .geojson, .kml, .kmz

### 2. Unidades_Proyecto_EPSG4326
- **Descripción**: Unidades de proyecto
- **Tipo de geometría**: Punto/Polígono
- **CRS**: EPSG:4326 (WGS84)
- **Formatos**: .shp, .geojson, .kml, .kmz

### 3. Superposicion_Espacial_EPSG4326
- **Descripción**: Resultado del análisis de superposición espacial
- **Tipo de geometría**: Punto/Polígono
- **CRS**: EPSG:4326 (WGS84)
- **Contiene**: Atributos de unidades + información de zonas SICOC
- **Formatos**: .shp, .geojson, .kml, .kmz

## 🔧 Guía de Uso

### En ArcGIS Desktop / ArcGIS Pro

1. **Abrir ArcGIS**
2. **Agregar datos**:
   - Clic en "Add Data" o arrastra el archivo
   - Navega a la carpeta `shapefile/` o `geojson/`
   - Selecciona el archivo `.shp` o `.geojson`
3. **El CRS se detectará automáticamente como EPSG:4326**
4. **Para KML**: Usar herramienta "KML to Layer"

### En QGIS

1. **Abrir QGIS**
2. **Agregar capa**:
   - Método 1: Arrastra cualquier archivo al lienzo
   - Método 2: Layer → Add Layer → Add Vector Layer
3. **Formatos soportados**:
   - ✓ Shapefile (.shp)
   - ✓ GeoJSON (.geojson)
   - ✓ KML (.kml)
   - ✓ KMZ (.kmz)
4. **El CRS EPSG:4326 se reconocerá automáticamente**

### En Google Earth

1. **Abrir Google Earth**
2. **File → Open**
3. **Seleccionar archivo**:
   - `.kml` (recomendado)
   - `.kmz` (más compacto)
4. **La capa se visualizará directamente en el globo 3D**

## 📊 Características del CRS EPSG:4326

### ✅ Ventajas
- Estándar mundial universalmente reconocido
- Compatible con GPS y navegación
- Perfecto para mapas web y aplicaciones móviles
- No requiere reproyección para Google Earth/Maps
- Coordenadas fáciles de entender (lat/lon)

### ⚠️ Consideraciones
- Las distancias en grados no son uniformes
- Para análisis de distancias, considerar usar proyecciones UTM
- Los ángulos y áreas pueden distorsionarse en latitudes extremas

## 🎯 Casos de Uso Recomendados

- ✅ Visualización en Google Earth
- ✅ Mapas web (Leaflet, Mapbox, OpenLayers)
- ✅ Integración con servicios de mapas online
- ✅ Compartir datos con GPS
- ✅ Aplicaciones móviles de ubicación
- ✅ Interoperabilidad entre diferentes sistemas GIS

## 📝 Verificación del CRS

### En ArcGIS Pro
1. Click derecho en la capa → Properties
2. Ir a "Source" tab
3. Verificar "Spatial Reference": WGS 1984 (EPSG:4326)

### En QGIS
1. Click derecho en la capa → Properties
2. Ir a pestaña "Information"
3. Buscar "CRS": EPSG:4326 - WGS 84

### Usando Python (GeoPandas)
```python
import geopandas as gpd

# Leer shapefile
gdf = gpd.read_file("shapefile/ZONAS_SICOC_EPSG4326.shp")

# Verificar CRS
print(gdf.crs)  # Debe mostrar: EPSG:4326
print(gdf.crs.to_epsg())  # Debe mostrar: 4326
```

## 💾 Tamaño de Archivos

- **Shapefile**: Incluye múltiples archivos (.shp, .shx, .dbf, .prj, .cpg)
- **GeoJSON**: Archivo único, formato texto (más grande)
- **KML**: Archivo único, formato XML
- **KMZ**: Archivo comprimido (más pequeño que KML)

**Recomendación**: Usar KMZ para compartir por email o transferencias rápidas.

## 🆘 Solución de Problemas

### El archivo no se visualiza correctamente
- Verificar que todos los archivos del shapefile estén presentes
- Confirmar que el software GIS soporta el formato
- Revisar que la extensión del archivo sea correcta

### El CRS no se reconoce automáticamente
- Los archivos incluyen archivo .prj con la definición del CRS
- Manualmente seleccionar EPSG:4326 si es necesario
- En QGIS: Click derecho → Set CRS → Buscar "4326"

### Archivos KML/KMZ no se abren
- Verificar que Google Earth esté instalado
- Probar abrir con QGIS como alternativa
- Los archivos deben tener extensión .kml o .kmz

## 📞 Información Adicional

Para más detalles sobre el análisis espacial realizado, consultar:
- Notebook Jupyter: `analisis_superposicion_espacial.ipynb`
- Reportes Excel en: `test_outputs/`

---
**Fecha de generación**: 2025-11-14
**Sistema de coordenadas garantizado**: EPSG:4326 (WGS84)
