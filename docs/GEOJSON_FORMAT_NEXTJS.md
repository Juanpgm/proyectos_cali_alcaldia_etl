# Formato de Geometrías GeoJSON para Firebase y Next.js

## Estándar de Geometrías

Todas las geometrías en Firebase siguen el estándar GeoJSON para garantizar compatibilidad con bibliotecas de mapas en Next.js (Mapbox, Leaflet, Google Maps, etc.).

### Formato General

```json
{
  "geometry": {
    "type": "Point|LineString|Polygon|MultiPoint|MultiLineString|MultiPolygon",
    "coordinates": "array o JSON string"
  }
}
```

**Importante:** Debido a limitaciones de Firebase con arrays anidados:

- **Point**: coordinates es un array `[lon, lat]`
- **Otros tipos**: coordinates es un JSON string que debe ser parseado

```javascript
// En Next.js
if (geometry.type === "Point") {
  const coords = geometry.coordinates; // Ya es array
} else {
  const coords = JSON.parse(geometry.coordinates); // Parsear string
}
```

## Tipos de Geometría Soportados

### 1. Point (Punto)

Un único punto geográfico.

**Formato:**

```json
{
  "type": "Point",
  "coordinates": [longitude, latitude]
}
```

**Ejemplo (Cali, Colombia):**

```json
{
  "type": "Point",
  "coordinates": [-76.520562, 3.4418833]
}
```

**Uso en Next.js:**

```javascript
// Con Mapbox GL JS
new mapboxgl.Marker().setLngLat(geometry.coordinates).addTo(map);

// Con Leaflet
L.marker([geometry.coordinates[1], geometry.coordinates[0]]).addTo(map);
```

---

### 2. LineString (Línea)

Una línea conectando múltiples puntos (ideal para vías, ríos, rutas).

**Formato:**

```json
{
  "type": "LineString",
  "coordinates": [
    [longitude1, latitude1],
    [longitude2, latitude2],
    [longitude3, latitude3]
  ]
}
```

**Ejemplo (Vía en Cali):**

```json
{
  "type": "LineString",
  "coordinates": [
    [-76.520562, 3.4418833],
    [-76.521234, 3.4425678],
    [-76.522456, 3.443289]
  ]
}
```

**Uso en Next.js:**

```javascript
// Con Mapbox GL JS
map.addSource("route", {
  type: "geojson",
  data: {
    type: "Feature",
    geometry: geometry, // geometry con type: 'LineString'
  },
});

map.addLayer({
  id: "route",
  type: "line",
  source: "route",
  paint: {
    "line-width": 3,
    "line-color": "#007cbf",
  },
});

// Con Leaflet
const latlngs = geometry.coordinates.map((c) => [c[1], c[0]]);
L.polyline(latlngs, { color: "blue" }).addTo(map);
```

---

### 3. Polygon (Polígono)

Un área cerrada (ideal para zonas, predios, barrios).

**Formato:**

```json
{
  "type": "Polygon",
  "coordinates": [
    [
      [longitude1, latitude1],
      [longitude2, latitude2],
      [longitude3, latitude3],
      [longitude4, latitude4],
      [longitude1, latitude1]  // Cierra el polígono
    ]
  ]
}
```

**Con huecos (opcional):**

```json
{
  "type": "Polygon",
  "coordinates": [
    // Ring exterior
    [
      [-76.52, 3.44],
      [-76.51, 3.44],
      [-76.51, 3.45],
      [-76.52, 3.45],
      [-76.52, 3.44]
    ],
    // Ring interior (hueco)
    [
      [-76.518, 3.443],
      [-76.515, 3.443],
      [-76.515, 3.446],
      [-76.518, 3.446],
      [-76.518, 3.443]
    ]
  ]
}
```

**Uso en Next.js:**

```javascript
// Con Mapbox GL JS
map.addSource("zone", {
  type: "geojson",
  data: {
    type: "Feature",
    geometry: geometry, // geometry con type: 'Polygon'
  },
});

map.addLayer({
  id: "zone-fill",
  type: "fill",
  source: "zone",
  paint: {
    "fill-color": "#088",
    "fill-opacity": 0.4,
  },
});

// Con Leaflet
const rings = geometry.coordinates.map((ring) => ring.map((c) => [c[1], c[0]]));
L.polygon(rings, { color: "green" }).addTo(map);
```

---

### 4. MultiPoint (Múltiples Puntos)

Colección de puntos independientes.

**Formato:**

```json
{
  "type": "MultiPoint",
  "coordinates": [
    [longitude1, latitude1],
    [longitude2, latitude2],
    [longitude3, latitude3]
  ]
}
```

**Ejemplo:**

```json
{
  "type": "MultiPoint",
  "coordinates": [
    [-76.520562, 3.4418833],
    [-76.521234, 3.4425678],
    [-76.522456, 3.443289]
  ]
}
```

---

### 5. MultiLineString (Múltiples Líneas)

Colección de líneas independientes (ideal para redes de vías).

**Formato:**

```json
{
  "type": "MultiLineString",
  "coordinates": [
    [
      [longitude1, latitude1],
      [longitude2, latitude2]
    ],
    [
      [longitude3, latitude3],
      [longitude4, latitude4]
    ]
  ]
}
```

---

### 6. MultiPolygon (Múltiples Polígonos)

Colección de polígonos independientes (ideal para territorios fragmentados).

**Formato:**

```json
{
  "type": "MultiPolygon",
  "coordinates": [
    [
      [
        [longitude1, latitude1],
        [longitude2, latitude2],
        [longitude3, latitude3],
        [longitude1, latitude1]
      ]
    ],
    [
      [
        [longitude4, latitude4],
        [longitude5, latitude5],
        [longitude6, latitude6],
        [longitude4, latitude4]
      ]
    ]
  ]
}
```

---

## Reglas Importantes

### 1. Orden de Coordenadas

**SIEMPRE** usar orden GeoJSON estándar: `[longitude, latitude]`

```javascript
// ✅ CORRECTO
coordinates: [-76.520562, 3.4418833]; // [lon, lat]

// ❌ INCORRECTO
coordinates: [3.4418833, -76.520562]; // [lat, lon]
```

### 2. Coordenadas 2D

Todas las coordenadas deben ser 2D (longitud, latitud). No incluir elevación.

```javascript
// ✅ CORRECTO
coordinates: [-76.520562, 3.4418833];

// ❌ INCORRECTO (tiene elevación)
coordinates: [-76.520562, 3.4418833, 1500];
```

### 3. Precisión

Redondear a 8 decimales (~1.1 mm de precisión).

```javascript
coordinates: [-76.52056234, 3.44188334];
```

### 4. Cierre de Polígonos

El primer y último punto deben ser idénticos.

```javascript
// ✅ CORRECTO
[
  [-76.52, 3.44],
  [-76.51, 3.44],
  [-76.51, 3.45],
  [-76.52, 3.45],
  [-76.52, 3.44], // Igual al primer punto
];
```

---

## Ejemplo Completo de Documento Firebase

```json
{
  "upid": "UNP-1234",
  "nombre_up": "Pavimentación Calle 5",
  "tipo_equipamiento": "Vias",
  "estado": "En ejecución",
  "presupuesto_base": "500000000",
  "avance_obra": "45.5",
  "fecha_inicio_std": "2025-01-15",
  "fecha_fin_std": "2025-12-31",
  "comuna_corregimiento": "COMUNA 18",
  "barrio_vereda": "Polvorines",

  "geometry": {
    "type": "LineString",
    "coordinates": [
      [-76.520562, 3.4418833],
      [-76.521234, 3.4425678],
      [-76.522456, 3.443289],
      [-76.523678, 3.4440123]
    ]
  },

  "has_geometry": true,
  "geometry_type": "LineString",
  "created_at": "2025-01-01T00:00:00",
  "updated_at": "2025-11-18T10:30:00"
}
```

---

## Componente Next.js de Ejemplo

```typescript
// components/GeometryRenderer.tsx
import { useEffect, useRef } from "react";
import mapboxgl from "mapbox-gl";

interface Geometry {
  type:
    | "Point"
    | "LineString"
    | "Polygon"
    | "MultiPoint"
    | "MultiLineString"
    | "MultiPolygon";
  coordinates: any;
}

interface GeometryRendererProps {
  geometry: Geometry;
  properties?: any;
}

export default function GeometryRenderer({
  geometry,
  properties,
}: GeometryRendererProps) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<mapboxgl.Map | null>(null);

  useEffect(() => {
    if (!mapContainer.current || !geometry) return;

    mapboxgl.accessToken = process.env.NEXT_PUBLIC_MAPBOX_TOKEN!;

    // Calcular centro
    const center = calculateCenter(geometry);

    map.current = new mapboxgl.Map({
      container: mapContainer.current,
      style: "mapbox://styles/mapbox/streets-v11",
      center: center,
      zoom: 14,
    });

    map.current.on("load", () => {
      // Agregar fuente
      map.current!.addSource("geometry", {
        type: "geojson",
        data: {
          type: "Feature",
          geometry: geometry,
          properties: properties || {},
        },
      });

      // Renderizar según tipo
      if (geometry.type === "Point" || geometry.type === "MultiPoint") {
        // Marcadores
        map.current!.addLayer({
          id: "points",
          type: "circle",
          source: "geometry",
          paint: {
            "circle-radius": 8,
            "circle-color": "#007cbf",
          },
        });
      } else if (
        geometry.type === "LineString" ||
        geometry.type === "MultiLineString"
      ) {
        // Líneas
        map.current!.addLayer({
          id: "lines",
          type: "line",
          source: "geometry",
          paint: {
            "line-width": 3,
            "line-color": "#007cbf",
          },
        });
      } else if (
        geometry.type === "Polygon" ||
        geometry.type === "MultiPolygon"
      ) {
        // Polígonos
        map.current!.addLayer({
          id: "polygons-fill",
          type: "fill",
          source: "geometry",
          paint: {
            "fill-color": "#088",
            "fill-opacity": 0.4,
          },
        });
        map.current!.addLayer({
          id: "polygons-outline",
          type: "line",
          source: "geometry",
          paint: {
            "line-color": "#000",
            "line-width": 2,
          },
        });
      }
    });

    return () => {
      map.current?.remove();
    };
  }, [geometry]);

  return <div ref={mapContainer} style={{ width: "100%", height: "400px" }} />;
}

function calculateCenter(geometry: Geometry): [number, number] {
  // Extraer primera coordenada según tipo
  if (geometry.type === "Point") {
    return geometry.coordinates as [number, number];
  } else if (geometry.type === "LineString") {
    const coords = geometry.coordinates as Array<[number, number]>;
    return coords[Math.floor(coords.length / 2)];
  } else if (geometry.type === "Polygon") {
    const coords = geometry.coordinates[0] as Array<[number, number]>;
    return coords[Math.floor(coords.length / 2)];
  }
  // Fallback
  return [-76.5225, 3.4372]; // Centro de Cali
}
```

---

## Validación de Geometrías

Para validar que todas las geometrías sean compatibles:

```bash
# Verificar geometrías en Firebase
python verify_geometry_compatibility.py --limit 100
```

## Referencias

- [GeoJSON Specification RFC 7946](https://tools.ietf.org/html/rfc7946)
- [Mapbox GL JS Documentation](https://docs.mapbox.com/mapbox-gl-js/)
- [Leaflet GeoJSON Documentation](https://leafletjs.com/reference.html#geojson)
- [Firebase Data Types](https://firebase.google.com/docs/firestore/manage-data/data-types)
