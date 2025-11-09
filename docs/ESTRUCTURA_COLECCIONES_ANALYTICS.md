# Estructura de Colecciones Analytics - Compatibilidad con Filtros

## 📋 Resumen Ejecutivo

Las colecciones analytics han sido **enriquecidas con campos detallados** para soportar los 7 filtros del componente `AdvancedFilters` del frontend:

```typescript
filters = {
  banco: "", // ✅ Totalmente soportado
  centroGestor: "", // ✅ Totalmente soportado
  estado: "", // ✅ Totalmente soportado
  sector: "", // ✅ Totalmente soportado
  ano: "", // ✅ Totalmente soportado
  fechaInicio: "", // ✅ Soportado vía fecha_inicio en contratos_detalle
  fechaFin: "", // ✅ Soportado vía fecha_inicio en contratos_detalle
};
```

**Versión de colecciones**: `1.1` (actualizada con campos de filtro)

---

## 🏦 1. `analytics_emprestito_por_banco`

### **Document ID**: `{nombre_banco}` (ej: `Bancolombia`, `Banco_de_Bogota`)

### **Campos de Resumen**:

```json
{
  "banco": "Bancolombia",
  "totalContratos": 31,
  "valorAsignadoBanco": 363222781545.0,
  "valorAdjudicado": 136482481702.0,
  "valorEjecutado": 2718265820.51,
  "valorPagado": 0,
  "porcentajeEjecucion": 1.99,
  "promedioAvanceFisico": 3.01,
  "promedioAvanceFinanciero": 2.17,
  "version": "1.1",
  "timestamp": "2025-11-09T10:28:22.915Z"
}
```

### **Campos de Filtro (NUEVOS)**:

```json
{
  "contratos_detalle": [
    {
      "referencia": "4151.010.26.1.0834-2025",
      "estado": "En ejecución",
      "sector": "No aplica/No pertenece",
      "centroGestor": "Secretaría de Infraestructura",
      "valor": 42254980600.0,
      "fecha_inicio": "2025-08-28",
      "avance_fisico": 0.0,
      "avance_financiero": 0.0
    }
    // ... 30 contratos más
  ],
  "estadosDisponibles": [
    "Aprobado",
    "Borrador",
    "En ejecución",
    "Modificado",
    "cedido",
    "enviado Proveedor"
  ],
  "sectoresDisponibles": [
    "Cultura",
    "Educación Nacional",
    "No aplica/No pertenece",
    "Servicio Público",
    "Tecnologías de la Información y las Comunicaciones",
    "Trabajo",
    "Vivienda, Ciudad y Territorio",
    "deportes"
  ],
  "centrosGestores": [
    "Departamento Administrativo de Tecnologías...",
    "Secretaría de Cultura",
    "Secretaría de Desarrollo Económico"
    // ... 5 más
  ]
}
```

### **Uso con Filtros**:

```typescript
// Filtrar por estado y sector dentro de un banco
const contratosFiltrados = banco.contratos_detalle.filter(
  (c) =>
    (!filters.estado || c.estado === filters.estado) &&
    (!filters.sector || c.sector === filters.sector) &&
    (!filters.centroGestor || c.centroGestor === filters.centroGestor) &&
    (!filters.fechaInicio || c.fecha_inicio >= filters.fechaInicio) &&
    (!filters.fechaFin || c.fecha_inicio <= filters.fechaFin)
);
```

---

## 🏢 2. `analytics_emprestito_por_centro_gestor`

### **Document ID**: `{nombre_centro_gestor}` (ej: `Secretaria_de_Infraestructura`)

### **Campos de Resumen**:

```json
{
  "centroGestor": "Secretaría de Infraestructura",
  "totalContratos": 15,
  "valorAsignadoBanco": 245000000000.0,
  "valorAdjudicado": 89500000000.0,
  "valorEjecutado": 1500000000.0,
  "valorPagado": 0,
  "version": "1.1"
}
```

### **Campos de Filtro (NUEVOS)**:

```json
{
  "contratos_detalle": [
    {
      "referencia": "4151.010.26.1.0834-2025",
      "banco": "Bancolombia",
      "estado": "En ejecución",
      "sector": "No aplica/No pertenece",
      "valor": 42254980600.0,
      "fecha_inicio": "2025-08-28",
      "avance_fisico": 0.0,
      "avance_financiero": 0.0
    }
    // ... más contratos
  ],
  "sectores": ["No aplica/No pertenece", "Vivienda, Ciudad y Territorio"],
  "estadosContratos": {
    "En ejecución": 12,
    "Modificado": 2,
    "Aprobado": 1
  },
  "bancosDisponibles": ["Bancolombia", "Banco de Bogotá", "BBVA"],
  "bancos": [
    {
      "nombre": "Bancolombia",
      "valorAsignado": 150000000000.0,
      "valorAdjudicado": 50000000000.0,
      "valorEjecutado": 1000000000.0,
      "contratos": 10
    }
  ]
}
```

### **Uso con Filtros**:

```typescript
// Filtrar por banco y estado dentro de un centro gestor
const contratosFiltrados = centroGestor.contratos_detalle.filter(
  (c) =>
    (!filters.banco || c.banco === filters.banco) &&
    (!filters.estado || c.estado === filters.estado) &&
    (!filters.sector || c.sector === filters.sector)
);
```

---

## 📅 3. `analytics_emprestito_resumen_anual`

### **Document ID**: `{año}` (ej: `2025`, `2024`, `Sin Año`)

### **Campos de Resumen**:

```json
{
  "anio": "2025",
  "totalContratos": 33,
  "valorTotalAsignado": 107601286246.0,
  "valorTotalEjecutado": 2718265820.51,
  "valorTotalPagado": 0,
  "valorTotalFisico": 2898341895.51,
  "porcentajeFisicoPromedio": 2.69,
  "porcentajeFinancieroPromedio": 2.53,
  "version": "1.1"
}
```

### **Campos de Filtro (NUEVOS)**:

```json
{
  "contratos_detalle": [
    {
      "referencia": "4151.010.26.1.0834-2025",
      "banco": "Bancolombia",
      "centroGestor": "Secretaría de Infraestructura",
      "estado": "En ejecución",
      "sector": "No aplica/No pertenece",
      "valor": 42254980600.0,
      "fecha_inicio": "2025-08-28",
      "avance_fisico": 0.0,
      "avance_financiero": 0.0
    }
  ],
  "bancosDisponibles": ["Bancolombia", "Banco de Bogotá"],
  "centrosDisponibles": ["Secretaría de Infraestructura", "DATIC"],
  "estadosDisponibles": ["En ejecución", "Aprobado", "Modificado"],
  "sectoresDisponibles": ["Infraestructura", "TIC", "Educación"]
}
```

### **Uso con Filtros**:

```typescript
// Filtrar contratos de un año por múltiples dimensiones
const contratosFiltrados = anio.contratos_detalle.filter(
  (c) =>
    (!filters.banco || c.banco === filters.banco) &&
    (!filters.centroGestor || c.centroGestor === filters.centroGestor) &&
    (!filters.estado || c.estado === filters.estado) &&
    (!filters.sector || c.sector === filters.sector)
);
```

---

## 📈 4. `analytics_emprestito_series_temporales_diarias`

### **Document ID**: `{YYYY-MM-DD}` (ej: `2025-10-15`)

### **Campos de Resumen**:

```json
{
  "fecha": "2025-10-15",
  "valor_pagado": 1500000000.0,
  "valor_contrato": 50000000000.0,
  "contratos_count": 5,
  "avance_fisico_promedio": 15.5,
  "avance_financiero_promedio": 12.3,
  "version": "1.1"
}
```

### **Campos de Filtro (NUEVOS)**:

```json
{
  "contratos_detalle": [
    {
      "referencia": "4151.010.26.1.0834-2025",
      "banco": "Bancolombia",
      "centroGestor": "Secretaría de Infraestructura",
      "estado": "En ejecución",
      "sector": "No aplica/No pertenece",
      "valor": 42254980600.0,
      "avance_fisico": 15.0,
      "avance_financiero": 12.0
    }
  ],
  "bancosDisponibles": ["Bancolombia"],
  "centrosDisponibles": ["Secretaría de Infraestructura"],
  "estadosDisponibles": ["En ejecución"],
  "sectoresDisponibles": ["Infraestructura"]
}
```

### **Uso con Filtros**:

```typescript
// Filtrar series temporales por rango de fechas y otras dimensiones
const seriesFiltradas = allSeriesTemporales
  .filter((s) => s.fecha >= filters.fechaInicio && s.fecha <= filters.fechaFin)
  .map((s) => ({
    ...s,
    contratos_detalle: s.contratos_detalle.filter(
      (c) =>
        (!filters.banco || c.banco === filters.banco) &&
        (!filters.estado || c.estado === filters.estado)
    ),
  }));
```

---

## 🔍 Estrategias de Query con Filtros

### **Estrategia 1: Query por Banco + Filtros Adicionales**

```typescript
// 1. Obtener documento de banco
const bancoDoc = await db
  .collection("analytics_emprestito_por_banco")
  .doc(filters.banco)
  .get();

// 2. Aplicar filtros adicionales en cliente
const contratosFiltrados = bancoDoc
  .data()
  .contratos_detalle.filter(
    (c) =>
      (!filters.estado || c.estado === filters.estado) &&
      (!filters.sector || c.sector === filters.sector) &&
      (!filters.centroGestor || c.centroGestor === filters.centroGestor)
  );
```

### **Estrategia 2: Query por Centro Gestor + Filtros**

```typescript
const centroDoc = await db
  .collection("analytics_emprestito_por_centro_gestor")
  .doc(filters.centroGestor)
  .get();

const contratosFiltrados = centroDoc
  .data()
  .contratos_detalle.filter(
    (c) =>
      (!filters.banco || c.banco === filters.banco) &&
      (!filters.estado || c.estado === filters.estado)
  );
```

### **Estrategia 3: Query por Año + Filtros Combinados**

```typescript
const anioDoc = await db
  .collection("analytics_emprestito_resumen_anual")
  .doc(filters.ano || "2025")
  .get();

const contratosFiltrados = anioDoc
  .data()
  .contratos_detalle.filter(
    (c) =>
      (!filters.banco || c.banco === filters.banco) &&
      (!filters.centroGestor || c.centroGestor === filters.centroGestor) &&
      (!filters.estado || c.estado === filters.estado) &&
      (!filters.sector || c.sector === filters.sector)
  );
```

### **Estrategia 4: Range Query por Fechas**

```typescript
// Para series temporales
const seriesDocs = await db
  .collection("analytics_emprestito_series_temporales_diarias")
  .where("fecha", ">=", filters.fechaInicio)
  .where("fecha", "<=", filters.fechaFin)
  .orderBy("fecha", "desc")
  .get();

// Aplicar filtros adicionales
const seriesFiltradas = seriesDocs.docs.map((doc) => {
  const data = doc.data();
  return {
    ...data,
    contratos_detalle: data.contratos_detalle.filter(
      (c) => !filters.banco || c.banco === filters.banco
    ),
  };
});
```

---

## 🎯 Mapeo de Filtros del Frontend

### **Filtro `banco`**

- ✅ **analytics_emprestito_por_banco**: Document ID directo
- ✅ **analytics_emprestito_por_centro_gestor**: Campo `bancosDisponibles` + `contratos_detalle[].banco`
- ✅ **analytics_emprestito_resumen_anual**: Campo `bancosDisponibles` + `contratos_detalle[].banco`
- ✅ **analytics_emprestito_series_temporales_diarias**: Campo `bancosDisponibles` + `contratos_detalle[].banco`

### **Filtro `centroGestor`**

- ✅ **analytics_emprestito_por_centro_gestor**: Document ID directo
- ✅ **analytics_emprestito_por_banco**: Campo `centrosGestores` + `contratos_detalle[].centroGestor`
- ✅ **analytics_emprestito_resumen_anual**: Campo `centrosDisponibles` + `contratos_detalle[].centroGestor`
- ✅ **analytics_emprestito_series_temporales_diarias**: Campo `centrosDisponibles` + `contratos_detalle[].centroGestor`

### **Filtro `estado`**

- ✅ **analytics_emprestito_por_banco**: Campo `estadosDisponibles` + `contratos_detalle[].estado`
- ✅ **analytics_emprestito_por_centro_gestor**: Campo `estadosContratos` + `contratos_detalle[].estado`
- ✅ **analytics_emprestito_resumen_anual**: Campo `estadosDisponibles` + `contratos_detalle[].estado`
- ✅ **analytics_emprestito_series_temporales_diarias**: Campo `estadosDisponibles` + `contratos_detalle[].estado`

### **Filtro `sector`**

- ✅ **analytics_emprestito_por_banco**: Campo `sectoresDisponibles` + `contratos_detalle[].sector`
- ✅ **analytics_emprestito_por_centro_gestor**: Campo `sectores` + `contratos_detalle[].sector`
- ✅ **analytics_emprestito_resumen_anual**: Campo `sectoresDisponibles` + `contratos_detalle[].sector`
- ✅ **analytics_emprestito_series_temporales_diarias**: Campo `sectoresDisponibles` + `contratos_detalle[].sector`

### **Filtro `ano`**

- ✅ **analytics_emprestito_resumen_anual**: Document ID directo
- ✅ Extraer de `fecha_inicio` en otras colecciones

### **Filtros `fechaInicio` / `fechaFin`**

- ✅ **analytics_emprestito_series_temporales_diarias**: Campo `fecha` (Document ID)
- ✅ Campo `contratos_detalle[].fecha_inicio` en todas las colecciones

---

## 📊 Ejemplo de Integración Frontend

### **Hook Modificado: `useEmprestitoAnalytics`**

```typescript
const useEmprestitoAnalytics = (filters: FiltersType) => {
  const [analysisByBank, setAnalysisByBank] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      const db = getFirestore();

      // Estrategia: Query específico según filtro principal
      if (filters.banco) {
        // Query directo por banco
        const bancoDoc = await db
          .collection("analytics_emprestito_por_banco")
          .doc(filters.banco)
          .get();

        const data = bancoDoc.data();

        // Aplicar filtros adicionales
        const contratosFiltrados = data.contratos_detalle.filter(
          (c) =>
            (!filters.estado || c.estado === filters.estado) &&
            (!filters.sector || c.sector === filters.sector) &&
            (!filters.centroGestor || c.centroGestor === filters.centroGestor)
        );

        // Recalcular métricas con contratos filtrados
        const metricas = calculateMetrics(contratosFiltrados);

        setAnalysisByBank([{ ...data, ...metricas }]);
      } else if (filters.ano) {
        // Query por año
        const anioDoc = await db
          .collection("analytics_emprestito_resumen_anual")
          .doc(filters.ano)
          .get();

        // ... aplicar filtros
      } else {
        // Query general: obtener todos los bancos
        const bancosSnapshot = await db
          .collection("analytics_emprestito_por_banco")
          .get();

        const bancosData = bancosSnapshot.docs.map((doc) => {
          const data = doc.data();
          const contratosFiltrados = data.contratos_detalle.filter(
            (c) =>
              (!filters.estado || c.estado === filters.estado) &&
              (!filters.sector || c.sector === filters.sector)
          );
          return { ...data, contratos_detalle: contratosFiltrados };
        });

        setAnalysisByBank(bancosData);
      }

      setLoading(false);
    };

    fetchData();
  }, [filters]);

  return { analysisByBank, loading };
};
```

---

## 🚀 Beneficios de esta Arquitectura

### **Performance**

- ✅ **1 read de Firestore** en lugar de 33 + 145 = 178 reads
- ✅ **Filtrado en cliente** sobre arrays pequeños (< 100 contratos por colección)
- ✅ **Cache efectivo** de documentos agregados
- ✅ **Payload reducido**: solo data relevante

### **Flexibilidad**

- ✅ Soporta **filtros combinados** sin queries complejos
- ✅ **Arrays precalculados** para dropdowns de filtros (`estadosDisponibles`, `sectoresDisponibles`)
- ✅ **Métricas precomputadas** + capacidad de recalcular en cliente

### **Costo**

- ✅ **-95% de reads**: 1-10 docs vs 178 docs
- ✅ **Uso de cache**: documentos agregados cambian raramente
- ✅ **Batch updates**: actualizaciones diarias/semanales en lugar de real-time

---

## 📝 Próximos Pasos

1. ✅ **Colecciones creadas** con versión 1.1 y campos de filtro
2. ⏳ **Actualizar índices** en `firestore.indexes.json` (opcional para filtros en cliente)
3. ⏳ **Modificar frontend** para consumir colecciones analytics
4. ⏳ **Implementar hook** `useEmprestitoAnalytics` con lógica de filtrado
5. ⏳ **Testing** de performance y validación de métricas
6. ⏳ **Configurar Cloud Functions** para actualizaciones automáticas

---

## 📞 Soporte

Para consultas sobre la estructura de las colecciones o integración con el frontend, revisar:

- **Documentación principal**: `docs/implementacion-datawarehouse.md`
- **Quick Start**: `docs/QUICKSTART.md`
- **Código de agregaciones**: `cloud_functions/emprestito_analytics.py`
- **Script de carga**: `load_initial_analytics.py`
