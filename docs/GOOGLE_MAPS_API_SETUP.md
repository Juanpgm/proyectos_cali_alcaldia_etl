# Configuración de Google Maps API para Reverse Geocoding

## 📋 Resumen

Se ha implementado la integración con Google Maps API para realizar **reverse geocoding** automático en el archivo `gdf_geolocalizar`. Esta funcionalidad convierte coordenadas geográficas en direcciones y extrae información de barrio/vereda y comuna/corregimiento.

## 🎯 Nuevas Columnas Generadas

Cuando se ejecuta el reverse geocoding, se crean dos nuevas columnas:

- **`barrio_vereda_val_s3`**: Barrio o Vereda obtenido de Google Maps API
- **`comuna_corregimiento_val_s3`**: Comuna o Corregimiento obtenido de Google Maps API

Estas columnas se agregan automáticamente a los registros que tienen `corregir = "INTENTAR GEORREFERENCIAR"`.

## ⚙️ Configuración Requerida

### 1. Obtener Google Maps API Key

1. Ve a [Google Cloud Console](https://console.cloud.google.com/)
2. Crea un proyecto o selecciona uno existente
3. Habilita las siguientes APIs:
   - **Geocoding API** (requerido)
   - Maps JavaScript API (opcional)
4. Ve a **Credenciales** > **Crear credenciales** > **Clave de API**
5. Copia la API Key generada

### 2. Configurar la API Key

Agrega la API Key a tus archivos de configuración:

#### Para Producción (`.env.prod`):

```bash
GOOGLE_MAPS_API_KEY=tu-api-key-aqui
```

#### Para Desarrollo Local (`.env.local`):

```bash
GOOGLE_MAPS_API_KEY=tu-api-key-de-desarrollo
```

### 3. (Opcional) Configurar Application Default Credentials

Para mayor seguridad, configura ADC:

```bash
gcloud auth application-default login
```

Esto permite que la aplicación use las credenciales de tu proyecto de Google Cloud automáticamente.

### 4. Instalar Dependencias

Si no lo has hecho, instala la librería de Google Maps:

```bash
pip install googlemaps
```

O instala todas las dependencias:

```bash
pip install -r requirements.txt
```

## 🚀 Uso

### Opción 1: Durante la Transformación (Recomendado)

El reverse geocoding se ejecuta automáticamente cuando generas el archivo `gdf_geolocalizar`:

```bash
cd transformation_app
python data_transformation_unidades_proyecto.py
```

Esto:

1. Genera `unidades_proyecto.geojson`
2. Crea `gdf_geolocalizar.xlsx` con columnas básicas
3. **Ejecuta reverse geocoding automáticamente**
4. Guarda los resultados en `gdf_geolocalizar.xlsx` con las nuevas columnas

### Opción 2: Solo Reverse Geocoding (Archivo Existente)

Si ya tienes el archivo `gdf_geolocalizar.xlsx` y solo quieres ejecutar el reverse geocoding:

```bash
cd transformation_app
python run_reverse_geocoding.py
```

**Opciones adicionales:**

```bash
# Modo prueba (solo primeros 10 registros)
python run_reverse_geocoding.py --test

# Limitar número de solicitudes (útil para control de costos)
python run_reverse_geocoding.py --max-requests 50

# Especificar archivo de entrada/salida
python run_reverse_geocoding.py --input ruta/al/archivo.xlsx --output ruta/salida.xlsx
```

## ✅ Verificar Configuración

Antes de ejecutar el reverse geocoding, verifica que todo esté configurado correctamente:

```bash
cd transformation_app
python check_maps_config.py
```

Este script verifica:

- ✅ API Key configurado
- ✅ Librería `googlemaps` instalada
- ✅ ADC configurado (opcional)
- ✅ Conexión con Google Maps API
- ✅ Archivo `gdf_geolocalizar.xlsx` existe

## 🔧 Cómo Funciona

### Flujo de Ejecución

1. **Filtrado**: Solo procesa registros con `corregir = "INTENTAR GEORREFERENCIAR"`
2. **Extracción de coordenadas**: Lee las coordenadas del campo `geometry` (GeoJSON)
3. **Reverse Geocoding**: Llama a Google Maps API para convertir coordenadas en dirección
4. **Extracción de componentes**: Analiza los componentes de la dirección para extraer:
   - Barrio/Vereda (neighborhood, sublocality_level_1)
   - Comuna/Corregimiento (administrative_area_level_3, administrative_area_level_2)
5. **Actualización**: Guarda los resultados en las nuevas columnas
6. **Error handling**: Si no se encuentra información, marca como "ERROR"

### Lógica de Extracción

**Para Barrio/Vereda (`barrio_vereda_val_s3`):**

- Busca en componentes de dirección con tipos:
  - `neighborhood` (prioridad 1)
  - `sublocality_level_1` (prioridad 2)
  - `sublocality` (prioridad 3)
  - `locality` (fallback)

**Para Comuna/Corregimiento (`comuna_corregimiento_val_s3`):**

- Busca en componentes de dirección con tipos:
  - `administrative_area_level_3` (prioridad 1)
  - `administrative_area_level_2` (prioridad 2)
  - `sublocality_level_1` (prioridad 3)
- Filtra resultados que contienen "COMUNA" o "CORREGIMIENTO"

## 💰 Costos y Límites

### Google Maps API - Geocoding API

- **Precio**: $5.00 USD por 1,000 solicitudes
- **Crédito gratuito mensual**: $200 USD (≈ 40,000 solicitudes gratis/mes)
- **Rate Limiting**: El código incluye un delay de 100ms entre solicitudes

### Estimación para tu Dataset

- **Total de registros**: 1,019
- **Registros a procesar**: ~558 (54.8% con `INTENTAR GEORREFERENCIAR`)
- **Costo estimado**: ~$2.79 USD (558 solicitudes)
- **Tiempo estimado**: ~1-2 minutos (con delay de 100ms)

> 💡 **Consejo**: Usa el modo `--test` primero para verificar que todo funciona antes de procesar todos los registros.

## 📊 Ejemplo de Resultados

Después de ejecutar el reverse geocoding:

| upid  | nombre_up                       | barrio_vereda_val_s3 | comuna_corregimiento_val_s3 |
| ----- | ------------------------------- | -------------------- | --------------------------- |
| UNP-1 | IPS - Union de Vivienda Popular | República de Israel  | COMUNA 16                   |
| UNP-2 | IPS - Polvorines                | Alto Jordán          | COMUNA 18                   |

## 🐛 Solución de Problemas

### Error: "GOOGLE_MAPS_API_KEY not found"

**Solución**: Asegúrate de agregar la API Key a `.env.prod` o `.env.local`

### Error: "API Key inválido"

**Solución**: Verifica que:

1. La API Key esté copiada correctamente
2. La Geocoding API esté habilitada en tu proyecto
3. No haya restricciones de IP que bloqueen las solicitudes

### Error: "Cuota excedida"

**Solución**:

- Espera hasta el próximo mes (se reinicia el crédito)
- Usa `--max-requests` para limitar las solicitudes
- Verifica tu cuota en Google Cloud Console

### Resultados con muchos "ERROR"

**Posibles causas**:

- Coordenadas inválidas o fuera del rango de Cali
- Geometrías vacías o con formato incorrecto
- Problema de conexión con la API

**Solución**: Verifica las coordenadas en el campo `geometry`

## 📝 Archivos Generados

Después de ejecutar el proceso completo:

```
app_outputs/unidades_proyecto_outputs/
├── unidades_proyecto.geojson          # Archivo principal completo
├── unidades_proyecto_simple.xlsx      # Excel con todas las columnas (incluye geometry)
├── gdf_geolocalizar.xlsx             # Excel temporal con columnas seleccionadas + reverse geocoding
└── gdf_geolocalizar.geojson          # GeoJSON temporal
```

## 🔗 Referencias

- [Geocoding API Documentation](https://developers.google.com/maps/documentation/geocoding)
- [Google Maps API Pricing](https://mapsplatform.google.com/pricing/)
- [Python googlemaps Client](https://github.com/googlemaps/google-maps-services-python)
