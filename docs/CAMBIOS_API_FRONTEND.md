# 📋 CAMBIOS NECESARIOS EN API Y FRONTEND

## 🎯 Resumen de Cambios Estructurales

### Antes (Estructura Antigua)

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [lon, lat]},
      "properties": {
        "intervencion_id": "...",
        "upid": "UNP-1",
        "nombre_up": "...",
        "direccion": "...",
        "lat": 3.464202,
        "lon": -76.538661,
        "estado": "En ejecución",
        "presupuesto_base": 1000000,
        "avance_obra": 50.0,
        "frente_activo": "Frente activo",
        ...
      }
    }
  ]
}
```

- **1,695 features** (una por intervención)
- Geometry duplicada para intervenciones en la misma ubicación
- Campos `lat` y `lon` individuales

### Después (Nueva Estructura)

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [lon, lat]},
      "properties": {
        "upid": "UNP-439",
        "nombre_up": "Centro Cultural de Cali",
        "nombre_up_detalle": "Principal",
        "direccion": "Calle 8 # 6-23",
        "tipo_equipamiento": "Bibliotecas",
        "clase_up": "Obras equipamientos",
        "barrio_vereda": "San Nicolás",
        "comuna_corregimiento": "COMUNA 3",
        "n_intervenciones": 11,
        "intervenciones": [
          {
            "intervencion_id": "INT-001",
            "ano": 2024,
            "estado": "En ejecución",
            "presupuesto_base": 1000000,
            "avance_obra": 50.0,
            "frente_activo": "Frente activo",
            "tipo_intervencion": "Obras",
            "fuente_financiacion": "Presupuesto Participativo",
            "fecha_inicio": "2024-01-15T00:00:00",
            "fecha_fin": "2024-12-31T00:00:00",
            "bpin": "2024...",
            "referencia_proceso": "CCC-001",
            "referencia_contrato": "CON-001",
            "url_proceso": "https://..."
          },
          {
            "intervencion_id": "INT-002",
            ...
          }
        ]
      }
    }
  ]
}
```

- **1,573 features** (una por unidad de proyecto)
- Geometry única por ubicación física
- Campos `lat` y `lon` eliminados → usar `geometry.coordinates`
- Array `intervenciones` con múltiples intervenciones por unidad

---

## 🔧 CAMBIOS EN LA API

### 1. Endpoints que Devuelven GeoJSON

#### ✅ Endpoint: `GET /api/unidades-proyecto`

**Sin cambios** - Retorna el GeoJSON completo

#### 🔄 Endpoint: `GET /api/unidades-proyecto/:upid`

**CAMBIO**: Ahora retorna UNA feature (unidad) con su array de intervenciones

**Antes**:

```javascript
// Retornaba todas las features con ese upid
return features.filter((f) => f.properties.upid === upid);
```

**Después**:

```javascript
// Retorna UNA feature con ese upid
const unidad = features.find((f) => f.properties.upid === upid);
return unidad;
```

**Respuesta esperada**:

```json
{
  "type": "Feature",
  "geometry": {"type": "Point", "coordinates": [-76.538661, 3.464202]},
  "properties": {
    "upid": "UNP-439",
    "nombre_up": "Centro Cultural de Cali",
    "n_intervenciones": 11,
    "intervenciones": [...]
  }
}
```

---

#### 🆕 Endpoint: `GET /api/intervenciones/:intervencionId`

**NUEVO** - Buscar una intervención específica dentro de las unidades

```javascript
router.get("/intervenciones/:intervencionId", (req, res) => {
  const { intervencionId } = req.params;

  // Buscar en todas las unidades
  for (const feature of geojsonData.features) {
    const intervencion = feature.properties.intervenciones.find(
      (i) => i.intervencion_id === intervencionId
    );

    if (intervencion) {
      return res.json({
        unidad: {
          upid: feature.properties.upid,
          nombre_up: feature.properties.nombre_up,
          direccion: feature.properties.direccion,
          geometry: feature.geometry,
        },
        intervencion: intervencion,
      });
    }
  }

  return res.status(404).json({ error: "Intervención no encontrada" });
});
```

---

#### 🔄 Endpoint: `GET /api/intervenciones?estado=En ejecución`

**CAMBIO**: Filtrar intervenciones, no features

**Antes**:

```javascript
// Filtraba features por estado
return features.filter((f) => f.properties.estado === estado);
```

**Después**:

```javascript
router.get("/intervenciones", (req, res) => {
  const { estado, tipo_intervencion, ano } = req.query;
  const resultados = [];

  // Iterar sobre todas las unidades
  for (const feature of geojsonData.features) {
    const intervencionesFiltradas = feature.properties.intervenciones.filter(
      (interv) => {
        let match = true;

        if (estado && interv.estado !== estado) match = false;
        if (tipo_intervencion && interv.tipo_intervencion !== tipo_intervencion)
          match = false;
        if (ano && interv.ano !== parseInt(ano)) match = false;

        return match;
      }
    );

    // Si hay intervenciones que coinciden, agregar al resultado
    if (intervencionesFiltradas.length > 0) {
      resultados.push({
        type: "Feature",
        geometry: feature.geometry,
        properties: {
          ...feature.properties,
          intervenciones: intervencionesFiltradas,
        },
      });
    }
  }

  return res.json({
    type: "FeatureCollection",
    features: resultados,
  });
});
```

---

#### 🔄 Endpoint: `GET /api/frentes-activos`

**CAMBIO**: Filtrar intervenciones con `frente_activo: "Frente activo"`

```javascript
router.get("/frentes-activos", (req, res) => {
  const frentesActivos = [];

  for (const feature of geojsonData.features) {
    const intervencionesFrentes = feature.properties.intervenciones.filter(
      (i) => i.frente_activo === "Frente activo"
    );

    if (intervencionesFrentes.length > 0) {
      frentesActivos.push({
        type: "Feature",
        geometry: feature.geometry,
        properties: {
          upid: feature.properties.upid,
          nombre_up: feature.properties.nombre_up,
          direccion: feature.properties.direccion,
          barrio_vereda: feature.properties.barrio_vereda,
          comuna_corregimiento: feature.properties.comuna_corregimiento,
          n_frentes_activos: intervencionesFrentes.length,
          intervenciones: intervencionesFrentes,
        },
      });
    }
  }

  return res.json({
    type: "FeatureCollection",
    features: frentesActivos,
    total_frentes: frentesActivos.reduce(
      (sum, f) => sum + f.properties.n_frentes_activos,
      0
    ),
  });
});
```

---

#### 🔄 Endpoint: `GET /api/estadisticas`

**CAMBIO**: Calcular estadísticas sobre intervenciones, no features

```javascript
router.get("/estadisticas", (req, res) => {
  let totalUnidades = geojsonData.features.length;
  let totalIntervenciones = 0;
  let presupuestoTotal = 0;
  let estadisticas = {
    por_estado: {},
    por_tipo: {},
    por_ano: {},
    frentes_activos: 0,
  };

  // Iterar sobre todas las unidades
  for (const feature of geojsonData.features) {
    for (const interv of feature.properties.intervenciones) {
      totalIntervenciones++;
      presupuestoTotal += interv.presupuesto_base || 0;

      // Por estado
      estadisticas.por_estado[interv.estado] =
        (estadisticas.por_estado[interv.estado] || 0) + 1;

      // Por tipo
      estadisticas.por_tipo[interv.tipo_intervencion] =
        (estadisticas.por_tipo[interv.tipo_intervencion] || 0) + 1;

      // Por año
      estadisticas.por_ano[interv.ano] =
        (estadisticas.por_ano[interv.ano] || 0) + 1;

      // Frentes activos
      if (interv.frente_activo === "Frente activo") {
        estadisticas.frentes_activos++;
      }
    }
  }

  return res.json({
    total_unidades: totalUnidades,
    total_intervenciones: totalIntervenciones,
    presupuesto_total: presupuestoTotal,
    promedio_intervenciones_por_unidad: (
      totalIntervenciones / totalUnidades
    ).toFixed(2),
    estadisticas: estadisticas,
  });
});
```

---

### 2. Cambios en Modelos de Datos

#### Antes: `Intervencion` model

```typescript
interface Intervencion {
  intervencion_id: string;
  upid: string;
  nombre_up: string;
  direccion: string;
  lat: number;
  lon: number;
  estado: string;
  presupuesto_base: number;
  // ... otros campos
}
```

#### Después: `UnidadProyecto` + `Intervencion` models

```typescript
interface UnidadProyecto {
  upid: string;
  nombre_up: string;
  nombre_up_detalle: string;
  direccion: string;
  tipo_equipamiento: string;
  clase_up: string;
  barrio_vereda: string;
  barrio_vereda_2?: string;
  comuna_corregimiento: string;
  n_intervenciones: number;
  intervenciones: Intervencion[];
  // NO tiene lat/lon - usar geometry
}

interface Intervencion {
  intervencion_id: string;
  ano: number;
  estado: string;
  tipo_intervencion: string;
  presupuesto_base: number;
  avance_obra: number;
  frente_activo: string; // "Frente activo" | "Inactivo" | "No aplica"
  fuente_financiacion: string;
  fecha_inicio: string;
  fecha_inicio_std: string;
  fecha_fin: string;
  fecha_fin_std: string;
  bpin?: string;
  referencia_proceso?: string;
  referencia_contrato?: string;
  url_proceso?: string;
  // NO tiene lat/lon - heredado de la unidad
}

interface GeoJSONFeature {
  type: "Feature";
  geometry: {
    type: "Point";
    coordinates: [number, number]; // [lon, lat]
  };
  properties: UnidadProyecto;
}
```

---

## 🎨 CAMBIOS EN EL FRONTEND

### 1. Mapa (Leaflet/Mapbox)

#### ✅ Renderizado de Marcadores

**Sin cambios mayores** - Cada feature sigue siendo un marcador

```javascript
// Renderizar marcadores
geojsonData.features.forEach((feature) => {
  const { geometry, properties } = feature;

  if (!geometry) return; // Manejar unidades sin geometría

  const [lon, lat] = geometry.coordinates;

  // Crear marcador
  L.marker([lat, lon])
    .bindPopup(
      `
      <strong>${properties.nombre_up}</strong><br>
      Dirección: ${properties.direccion}<br>
      Intervenciones: ${properties.n_intervenciones}<br>
      <button onclick="verDetalle('${properties.upid}')">Ver detalle</button>
    `
    )
    .addTo(map);
});
```

#### 🔄 Color de Marcador por Estado

**CAMBIO**: Determinar color según intervenciones

```javascript
function getMarkerColor(unidad) {
  const estados = unidad.intervenciones.map((i) => i.estado);

  // Si tiene al menos una "En ejecución"
  if (estados.includes("En ejecución")) {
    return "green";
  }

  // Si todas están "Terminado"
  if (estados.every((e) => e === "Terminado")) {
    return "blue";
  }

  // Si tiene al menos una "Suspendido"
  if (estados.includes("Suspendido")) {
    return "red";
  }

  // Resto (En alistamiento)
  return "orange";
}

// Uso
const color = getMarkerColor(feature.properties);
L.marker([lat, lon], {
  icon: getColoredIcon(color),
}).addTo(map);
```

---

### 2. Panel de Detalles

#### 🔄 Vista de Detalle de Unidad

**CAMBIO**: Mostrar información de unidad + lista de intervenciones

```jsx
function DetalleUnidad({ upid }) {
  const [unidad, setUnidad] = useState(null);

  useEffect(() => {
    fetch(`/api/unidades-proyecto/${upid}`)
      .then((res) => res.json())
      .then((data) => setUnidad(data.properties));
  }, [upid]);

  if (!unidad) return <Spinner />;

  return (
    <div className="detalle-unidad">
      <h2>{unidad.nombre_up}</h2>
      <p>
        <strong>Dirección:</strong> {unidad.direccion}
      </p>
      <p>
        <strong>Tipo:</strong> {unidad.tipo_equipamiento}
      </p>
      <p>
        <strong>Barrio:</strong> {unidad.barrio_vereda}
      </p>
      <p>
        <strong>Comuna:</strong> {unidad.comuna_corregimiento}
      </p>

      <h3>Intervenciones ({unidad.n_intervenciones})</h3>

      {unidad.intervenciones.map((interv) => (
        <div key={interv.intervencion_id} className="card-intervencion">
          <h4>{interv.tipo_intervencion}</h4>
          <div className="estado-badge">{interv.estado}</div>

          {interv.frente_activo === "Frente activo" && (
            <span className="badge badge-success">🚧 Frente Activo</span>
          )}

          <p>
            <strong>Presupuesto:</strong> $
            {formatNumber(interv.presupuesto_base)}
          </p>
          <p>
            <strong>Avance:</strong> {interv.avance_obra}%
          </p>
          <p>
            <strong>Año:</strong> {interv.ano}
          </p>

          <ProgressBar value={interv.avance_obra} />

          <div className="fechas">
            <span>Inicio: {formatDate(interv.fecha_inicio_std)}</span>
            <span>Fin: {formatDate(interv.fecha_fin_std)}</span>
          </div>

          {interv.url_proceso && (
            <a href={interv.url_proceso} target="_blank">
              Ver proceso en SECOP
            </a>
          )}
        </div>
      ))}
    </div>
  );
}
```

---

### 3. Filtros y Búsqueda

#### 🔄 Filtro por Estado

**CAMBIO**: Filtrar por estado de intervenciones

```javascript
function filtrarPorEstado(geojsonData, estadoSeleccionado) {
  return {
    type: "FeatureCollection",
    features: geojsonData.features
      .map((feature) => {
        // Filtrar intervenciones por estado
        const intervencionesFiltradas =
          feature.properties.intervenciones.filter(
            (i) => i.estado === estadoSeleccionado
          );

        // Si no hay intervenciones que coinciden, excluir la feature
        if (intervencionesFiltradas.length === 0) return null;

        return {
          ...feature,
          properties: {
            ...feature.properties,
            intervenciones: intervencionesFiltradas,
            n_intervenciones: intervencionesFiltradas.length,
          },
        };
      })
      .filter((f) => f !== null), // Eliminar nulls
  };
}
```

#### 🆕 Filtro por Frentes Activos

**NUEVO**: Mostrar solo unidades con frentes activos

```javascript
function filtrarFrentesActivos(geojsonData) {
  return {
    type: "FeatureCollection",
    features: geojsonData.features
      .map((feature) => {
        const frentesActivos = feature.properties.intervenciones.filter(
          (i) => i.frente_activo === "Frente activo"
        );

        if (frentesActivos.length === 0) return null;

        return {
          ...feature,
          properties: {
            ...feature.properties,
            intervenciones: frentesActivos,
            n_intervenciones: frentesActivos.length,
          },
        };
      })
      .filter((f) => f !== null),
  };
}
```

---

### 4. Dashboard y Estadísticas

#### 🔄 Cards de Resumen

**CAMBIO**: Mostrar diferenciación entre unidades e intervenciones

```jsx
function Dashboard() {
  const [stats, setStats] = useState(null);

  useEffect(() => {
    fetch("/api/estadisticas")
      .then((res) => res.json())
      .then((data) => setStats(data));
  }, []);

  if (!stats) return <Spinner />;

  return (
    <div className="dashboard">
      <div className="card">
        <h3>Unidades de Proyecto</h3>
        <div className="numero">{stats.total_unidades}</div>
      </div>

      <div className="card">
        <h3>Total Intervenciones</h3>
        <div className="numero">{stats.total_intervenciones}</div>
      </div>

      <div className="card">
        <h3>Promedio Intervenciones/Unidad</h3>
        <div className="numero">{stats.promedio_intervenciones_por_unidad}</div>
      </div>

      <div className="card">
        <h3>Frentes Activos</h3>
        <div className="numero">{stats.estadisticas.frentes_activos}</div>
      </div>

      <div className="card">
        <h3>Presupuesto Total</h3>
        <div className="numero">${formatNumber(stats.presupuesto_total)}</div>
      </div>

      {/* Gráficos */}
      <ChartEstados data={stats.estadisticas.por_estado} />
      <ChartTipos data={stats.estadisticas.por_tipo} />
      <ChartAnos data={stats.estadisticas.por_ano} />
    </div>
  );
}
```

---

### 5. Listado de Intervenciones

#### 🔄 Tabla de Intervenciones

**CAMBIO**: Iterar sobre unidades → intervenciones

```jsx
function TablaIntervenciones({ filtros }) {
  const [intervenciones, setIntervenciones] = useState([]);

  useEffect(() => {
    // Construir query string
    const params = new URLSearchParams(filtros).toString();

    fetch(`/api/intervenciones?${params}`)
      .then((res) => res.json())
      .then((data) => {
        // Aplanar intervenciones con info de unidad
        const aplanadas = data.features.flatMap((feature) =>
          feature.properties.intervenciones.map((interv) => ({
            ...interv,
            upid: feature.properties.upid,
            nombre_up: feature.properties.nombre_up,
            direccion: feature.properties.direccion,
            barrio_vereda: feature.properties.barrio_vereda,
            geometry: feature.geometry,
          }))
        );

        setIntervenciones(aplanadas);
      });
  }, [filtros]);

  return (
    <table className="tabla-intervenciones">
      <thead>
        <tr>
          <th>Unidad</th>
          <th>Dirección</th>
          <th>Tipo Intervención</th>
          <th>Estado</th>
          <th>Frente Activo</th>
          <th>Presupuesto</th>
          <th>Avance</th>
          <th>Acciones</th>
        </tr>
      </thead>
      <tbody>
        {intervenciones.map((interv) => (
          <tr key={interv.intervencion_id}>
            <td>
              <a href={`/unidades/${interv.upid}`}>{interv.nombre_up}</a>
            </td>
            <td>{interv.direccion}</td>
            <td>{interv.tipo_intervencion}</td>
            <td>
              <span className={`estado ${interv.estado}`}>{interv.estado}</span>
            </td>
            <td>
              {interv.frente_activo === "Frente activo" && (
                <span className="badge-frente">🚧</span>
              )}
            </td>
            <td>${formatNumber(interv.presupuesto_base)}</td>
            <td>
              <ProgressBar value={interv.avance_obra} />
              {interv.avance_obra}%
            </td>
            <td>
              <button onClick={() => verEnMapa(interv.geometry)}>
                Ver en mapa
              </button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
```

---

## 📊 RESUMEN DE IMPACTO

### Ventajas de la Nueva Estructura

1. **✅ Menos Duplicación**:

   - 1,695 features → 1,573 features (7% reducción)
   - Geometry única por ubicación física
   - Archivo GeoJSON más eficiente

2. **✅ Mejor Agrupación Lógica**:

   - Todas las intervenciones en un mismo lugar físico están juntas
   - Ejemplo: Centro Cultural de Cali con 11 intervenciones

3. **✅ Campos Más Semánticos**:

   - `frente_activo` a nivel de intervención (donde tiene sentido)
   - Información de unidad separada de información de intervención

4. **✅ Mejor para Agregaciones**:
   - Calcular presupuesto total por unidad
   - Ver historial de intervenciones en un mismo lugar

### Desventajas / Consideraciones

1. **⚠️ Más Complejidad en Filtros**:

   - Necesitas iterar sobre arrays anidados
   - Filtros deben aplicarse a nivel de intervención

2. **⚠️ Renderizado en Tablas**:

   - Si muestras una tabla plana de intervenciones, necesitas "aplanar" el GeoJSON
   - Usar `flatMap()` para convertir estructura jerárquica a plana

3. **⚠️ Búsquedas**:
   - Buscar una intervención específica requiere iterar sobre todas las unidades

---

## 🚀 ESTRATEGIA DE MIGRACIÓN

### Opción 1: Big Bang (Recomendada)

1. Actualizar API completa
2. Actualizar frontend completo
3. Probar exhaustivamente
4. Desplegar todo junto

### Opción 2: Incremental (Más Segura)

1. **Fase 1**: API soporta ambas estructuras

   - Detectar versión del cliente
   - Endpoint `/api/v1/unidades-proyecto` (antigua)
   - Endpoint `/api/v2/unidades-proyecto` (nueva)

2. **Fase 2**: Frontend consume nueva API

   - Actualizar componentes uno por uno
   - Probar en staging

3. **Fase 3**: Deprecar API antigua
   - Remover `/api/v1/` después de 1-2 meses

---

## 📝 CHECKLIST DE IMPLEMENTACIÓN

### API

- [ ] Actualizar modelos de datos (TypeScript interfaces)
- [ ] Modificar endpoint `GET /api/unidades-proyecto/:upid`
- [ ] Crear endpoint `GET /api/intervenciones/:intervencionId`
- [ ] Actualizar endpoint `GET /api/intervenciones` (con filtros)
- [ ] Crear endpoint `GET /api/frentes-activos`
- [ ] Actualizar endpoint `GET /api/estadisticas`
- [ ] Actualizar tests unitarios
- [ ] Actualizar documentación API (Swagger/OpenAPI)

### Frontend

- [ ] Actualizar modelos TypeScript/PropTypes
- [ ] Modificar renderizado de marcadores en mapa
- [ ] Actualizar lógica de colores de marcadores
- [ ] Modificar componente `DetalleUnidad`
- [ ] Actualizar filtros (estado, tipo, año)
- [ ] Implementar filtro de frentes activos
- [ ] Modificar Dashboard y estadísticas
- [ ] Actualizar tabla de intervenciones
- [ ] Actualizar búsquedas
- [ ] Probar casos edge: unidades sin geometry, sin intervenciones
- [ ] Actualizar tests de componentes

### Testing

- [ ] Test de integración API + Frontend
- [ ] Test de performance (comparar con estructura antigua)
- [ ] Test de UX (navegación, filtros, búsqueda)
- [ ] Test de accesibilidad

---

## 💡 EJEMPLOS DE CASOS DE USO

### Caso 1: Ver todas las intervenciones "En ejecución"

```javascript
// Frontend
fetch("/api/intervenciones?estado=En ejecución")
  .then((res) => res.json())
  .then((data) => {
    // data.features = unidades con solo intervenciones "En ejecución"
    renderMarkers(data.features);
  });
```

### Caso 2: Ver detalle del Centro Cultural de Cali

```javascript
// Frontend
fetch("/api/unidades-proyecto/UNP-439")
  .then((res) => res.json())
  .then((feature) => {
    // feature.properties.intervenciones = array con 11 intervenciones
    renderDetalle(feature.properties);
  });
```

### Caso 3: Filtrar frentes activos en el mapa

```javascript
// Frontend
fetch('/api/frentes-activos')
  .then(res => res.json())
  .then(data => {
    // data.features = unidades con solo intervenciones de frente activo
    // Renderizar con íconos especiales
    data.features.forEach(feature => {
      const marker = L.marker([...], {
        icon: iconFrenteActivo
      });
      marker.addTo(map);
    });
  });
```

---

## 📞 CONTACTO Y SOPORTE

Si tienes preguntas durante la implementación:

1. Revisar este documento
2. Revisar los archivos de ejemplo generados en `app_outputs/`
3. Consultar la documentación del clustering en `transformation_app/geospatial_clustering.py`

**Archivos de referencia**:

- GeoJSON generado: `app_outputs/unidades_proyecto_transformed.geojson`
- Script de prueba: `scripts/test_pipeline_completo_sin_carga.py`
- Verificación: `scripts/verify_complete_output.py`
