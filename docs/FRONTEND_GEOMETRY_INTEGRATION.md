# Guía de Integración: Geometrías Firebase con Next.js

## Problema Actual

El mapa muestra "0 con ubicación" porque el frontend no está parseando correctamente las coordenadas que están en formato JSON string para LineString y otros tipos complejos.

## Estructura de Datos en Firebase

### Point (1561 documentos)

```json
{
  "geometry": {
    "type": "Point",
    "coordinates": [-76.520562, 3.4418833] // Array directo
  },
  "geometry_type": "Point",
  "has_geometry": true
}
```

### LineString (226 documentos) y MultiLineString (4 documentos)

```json
{
  "geometry": {
    "type": "LineString",
    "coordinates": "[[-76.53901129,3.42810854],[-76.5376278,3.426295]]" // JSON string
  },
  "geometry_type": "LineString",
  "has_geometry": true
}
```

## Solución: Parser de Geometrías

### Función Utility (TypeScript)

Crear archivo: `utils/geometryParser.ts`

```typescript
export interface GeoJSONGeometry {
  type:
    | "Point"
    | "LineString"
    | "Polygon"
    | "MultiPoint"
    | "MultiLineString"
    | "MultiPolygon";
  coordinates: any;
}

export interface ParsedGeometry extends GeoJSONGeometry {
  coordinates: number[] | number[][] | number[][][];
}

/**
 * Parse geometry from Firebase format to standard GeoJSON
 * Handles both array (Point) and JSON string (LineString, Polygon, etc.)
 */
export function parseGeometry(geometry: any): ParsedGeometry | null {
  if (!geometry || !geometry.type) {
    return null;
  }

  let coordinates = geometry.coordinates;

  // Si coordinates es string, parsear JSON
  if (typeof coordinates === "string") {
    try {
      coordinates = JSON.parse(coordinates);
    } catch (e) {
      console.error("Error parsing geometry coordinates:", e);
      return null;
    }
  }

  // Validar que coordinates sea array
  if (!Array.isArray(coordinates)) {
    console.error("Invalid coordinates format:", typeof coordinates);
    return null;
  }

  return {
    type: geometry.type,
    coordinates: coordinates,
  };
}

/**
 * Calculate center point from any geometry type
 */
export function getGeometryCenter(
  geometry: ParsedGeometry
): [number, number] | null {
  try {
    switch (geometry.type) {
      case "Point":
        return geometry.coordinates as [number, number];

      case "LineString":
        const lineCoords = geometry.coordinates as number[][];
        const midIndex = Math.floor(lineCoords.length / 2);
        return lineCoords[midIndex] as [number, number];

      case "Polygon":
        const polyCoords = (geometry.coordinates as number[][][])[0];
        const polyMidIndex = Math.floor(polyCoords.length / 2);
        return polyCoords[polyMidIndex] as [number, number];

      case "MultiPoint":
        const multiPoints = geometry.coordinates as number[][];
        return multiPoints[0] as [number, number];

      case "MultiLineString":
        const multiLines = geometry.coordinates as number[][][];
        const firstLine = multiLines[0];
        const multiMidIndex = Math.floor(firstLine.length / 2);
        return firstLine[multiMidIndex] as [number, number];

      case "MultiPolygon":
        const multiPolys = geometry.coordinates as number[][][][];
        const firstPoly = multiPolys[0][0];
        const multiPolyMidIndex = Math.floor(firstPoly.length / 2);
        return firstPoly[multiPolyMidIndex] as [number, number];

      default:
        return null;
    }
  } catch (e) {
    console.error("Error calculating geometry center:", e);
    return null;
  }
}

/**
 * Create GeoJSON Feature from Firebase document
 */
export function createGeoJSONFeature(doc: any) {
  const parsedGeometry = parseGeometry(doc.geometry);

  if (!parsedGeometry) {
    return null;
  }

  return {
    type: "Feature",
    geometry: parsedGeometry,
    properties: {
      upid: doc.upid,
      nombre_up: doc.nombre_up,
      estado: doc.estado,
      tipo_equipamiento: doc.tipo_equipamiento,
      comuna_corregimiento: doc.comuna_corregimiento,
      barrio_vereda: doc.barrio_vereda,
      // Agregar más propiedades según necesidad
    },
  };
}
```

### Uso en Componente de Mapa

```typescript
// components/MapComponent.tsx
import { useEffect, useState } from "react";
import { collection, getDocs, query, where } from "firebase/firestore";
import { db } from "@/lib/firebase";
import {
  parseGeometry,
  getGeometryCenter,
  createGeoJSONFeature,
} from "@/utils/geometryParser";

export default function MapComponent() {
  const [features, setFeatures] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadFeatures();
  }, []);

  const loadFeatures = async () => {
    try {
      // Cargar solo documentos con geometría
      const q = query(
        collection(db, "unidades_proyecto"),
        where("has_geometry", "==", true)
      );

      const snapshot = await getDocs(q);
      const docs = snapshot.docs.map((doc) => ({
        id: doc.id,
        ...doc.data(),
      }));

      console.log(`Loaded ${docs.length} documents with geometry`);

      // Parsear geometrías y crear features
      const parsedFeatures = docs
        .map((doc) => createGeoJSONFeature(doc))
        .filter((feature) => feature !== null);

      console.log(`Parsed ${parsedFeatures.length} valid features`);

      setFeatures(parsedFeatures);
      setLoading(false);
    } catch (error) {
      console.error("Error loading features:", error);
      setLoading(false);
    }
  };

  // Renderizar mapa con features parseados
  return (
    <div>
      {loading ? (
        <p>Cargando geometrías...</p>
      ) : (
        <MapView features={features} />
      )}
    </div>
  );
}
```

### Uso con Mapbox GL JS

```typescript
// components/MapboxMap.tsx
import { useEffect, useRef } from "react";
import mapboxgl from "mapbox-gl";
import { parseGeometry } from "@/utils/geometryParser";

export default function MapboxMap({ documents }: { documents: any[] }) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<mapboxgl.Map | null>(null);

  useEffect(() => {
    if (!mapContainer.current) return;

    mapboxgl.accessToken = process.env.NEXT_PUBLIC_MAPBOX_TOKEN!;

    map.current = new mapboxgl.Map({
      container: mapContainer.current,
      style: "mapbox://styles/mapbox/streets-v12",
      center: [-76.5225, 3.4372], // Centro de Cali
      zoom: 12,
    });

    map.current.on("load", () => {
      addFeaturesToMap(documents);
    });

    return () => {
      map.current?.remove();
    };
  }, []);

  const addFeaturesToMap = (docs: any[]) => {
    if (!map.current) return;

    // Crear GeoJSON FeatureCollection
    const geojson: GeoJSON.FeatureCollection = {
      type: "FeatureCollection",
      features: docs
        .map((doc) => {
          const geometry = parseGeometry(doc.geometry);
          if (!geometry) return null;

          return {
            type: "Feature" as const,
            geometry: geometry,
            properties: {
              upid: doc.upid,
              nombre: doc.nombre_up,
              estado: doc.estado,
              tipo: doc.tipo_equipamiento,
            },
          };
        })
        .filter((f) => f !== null) as GeoJSON.Feature[],
    };

    console.log(`Adding ${geojson.features.length} features to map`);

    // Agregar fuente
    map.current!.addSource("projects", {
      type: "geojson",
      data: geojson,
    });

    // Agregar capa de puntos
    map.current!.addLayer({
      id: "points",
      type: "circle",
      source: "projects",
      filter: ["==", ["geometry-type"], "Point"],
      paint: {
        "circle-radius": 6,
        "circle-color": "#007cbf",
        "circle-stroke-width": 2,
        "circle-stroke-color": "#ffffff",
      },
    });

    // Agregar capa de líneas
    map.current!.addLayer({
      id: "lines",
      type: "line",
      source: "projects",
      filter: ["==", ["geometry-type"], "LineString"],
      paint: {
        "line-width": 3,
        "line-color": "#ff6b35",
      },
    });

    // Agregar popup al hacer click
    map.current!.on("click", ["points", "lines"], (e) => {
      if (!e.features || e.features.length === 0) return;

      const feature = e.features[0];
      const props = feature.properties;

      new mapboxgl.Popup()
        .setLngLat(e.lngLat)
        .setHTML(
          `
          <div style="padding: 8px;">
            <h3 style="margin: 0 0 8px 0;">${props.nombre}</h3>
            <p style="margin: 4px 0;"><strong>Estado:</strong> ${props.estado}</p>
            <p style="margin: 4px 0;"><strong>Tipo:</strong> ${props.tipo}</p>
            <p style="margin: 4px 0;"><strong>UPID:</strong> ${props.upid}</p>
          </div>
        `
        )
        .addTo(map.current!);
    });

    // Cambiar cursor al pasar sobre features
    map.current!.on("mouseenter", ["points", "lines"], () => {
      map.current!.getCanvas().style.cursor = "pointer";
    });

    map.current!.on("mouseleave", ["points", "lines"], () => {
      map.current!.getCanvas().style.cursor = "";
    });
  };

  return <div ref={mapContainer} style={{ width: "100%", height: "100%" }} />;
}
```

## Verificación

Para verificar que el parsing funciona correctamente:

```typescript
// Test en consola del navegador
const testDoc = {
  geometry: {
    type: "LineString",
    coordinates: "[[-76.53901129,3.42810854],[-76.5376278,3.426295]]",
  },
};

const parsed = parseGeometry(testDoc.geometry);
console.log("Parsed:", parsed);
// Debe mostrar: { type: "LineString", coordinates: [[-76.53901129,3.42810854],[-76.5376278,3.426295]] }

const center = getGeometryCenter(parsed);
console.log("Center:", center);
// Debe mostrar: [-76.5376278, 3.426295] (o el punto medio)
```

## Resumen de Cambios Necesarios

1. ✅ **Backend (Python/Firebase)**: Ya está correcto

   - Point: coordinates como array
   - LineString/Polygon/etc: coordinates como JSON string

2. ⚠️ **Frontend (Next.js)**: Necesita actualización

   - Agregar función `parseGeometry()`
   - Usar el parser antes de renderizar en el mapa
   - Manejar ambos formatos (array y string)

3. 🎯 **Resultado Esperado**:
   - Mapa muestra "1791 elementos con ubicación" (1561 Point + 230 LineString/Multi)
   - Todos los tipos de geometría son visualizables
   - Click en features muestra información
