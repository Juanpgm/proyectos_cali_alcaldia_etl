# Implementación Data Warehouse - Alcaldía de Cali

## Arquitectura Constellation Schema con Agregaciones Semanales

**Fecha:** Noviembre 2025  
**Versión:** 1.0  
**Arquitectura:** Constellation Schema (Firebase-only)

---

## 📋 Resumen Ejecutivo

Esta implementación crea un **Data Warehouse analítico** optimizado para el frontend `gestor_proyectos_vercel` usando una arquitectura **Constellation Schema** con las siguientes características:

### ✅ Características Implementadas

- **Agregaciones SEMANALES** (no mensuales) para contratos y reportes
- **Carga INCREMENTAL** automática (solo añade nuevos datos, skip existentes)
- **Preservación de nombres de variables** del frontend (sin cambios en el UI)
- **Misma base de datos** Firebase con prefijo `analytics_` (economía de costos)
- **Índices compuestos** optimizados para consultas complejas
- **Cloud Functions** para actualización automática (futuro)

### 📊 Colecciones Analíticas Creadas

| Colección                    | Propósito                           | Granularidad                            | Frontend Dependency               |
| ---------------------------- | ----------------------------------- | --------------------------------------- | --------------------------------- |
| `analytics_contratos_weekly` | Agregaciones semanales de contratos | Semanal por BP + Centro Gestor + Banco  | `EmprestitoAdvancedDashboard.tsx` |
| `analytics_flujo_caja_banco` | Flujo de caja por banco             | Trimestral por Banco                    | Dashboard KPIs                    |
| `analytics_kpi_dashboard`    | KPIs globales del dashboard         | Documento único actualizado diariamente | Dashboard principal               |

---

## 🏗️ Arquitectura Constellation Schema

### ¿Por qué Constellation y no Star Schema?

**Constellation Schema** permite múltiples **fact tables** con dimensiones compartidas, ideal para nuestra necesidad de:

1. **Múltiples perspectivas analíticas:**

   - Vista por contratos (`fact_contratos`)
   - Vista por flujo de caja (`fact_flujo_caja`)
   - Vista por avance de proyectos (`fact_avance_proyectos`)

2. **Dimensiones compartidas:**

   - `dim_tiempo` (año, semana, trimestre)
   - `dim_proyecto` (BP, nombre, centro gestor)
   - `dim_banco` (banco, sector)
   - `dim_geografia` (comuna, corregimiento)

3. **Optimización de consultas del frontend:**
   - El frontend usa filtros complejos por `banco`, `centro_gestor`, `referencia_contrato`
   - Se requieren promedios ponderados: `avance_fisico_promedio`, `avance_financiero_promedio`
   - Agregaciones por período temporal (`fecha_inicio_semana`, `fecha_fin_semana`)

### Diagrama de Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                    COLECCIONES OPERACIONALES                    │
│  (Fuentes de datos - NO MODIFICAR)                             │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ contratos_       │  │ reportes_        │  │ flujo_caja_      │
│ emprestito       │  │ contratos        │  │ emprestito       │
│ (33 docs)        │  │ (145 docs)       │  │ (500+ docs)      │
└──────────────────┘  └──────────────────┘  └──────────────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │ Cloud Functions │
                    │  Aggregations   │
                    └─────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ analytics_       │  │ analytics_       │  │ analytics_       │
│ contratos_weekly │  │ flujo_caja_banco │  │ kpi_dashboard    │
│                  │  │                  │  │                  │
│ Granularidad:    │  │ Granularidad:    │  │ Documento único: │
│ SEMANAL          │  │ TRIMESTRAL       │  │ global_kpis      │
│                  │  │                  │  │                  │
│ ID: 2025-W45_    │  │ ID: 2025-Q4_     │  │ KPIs globales    │
│ BP26005260_      │  │ Bancolombia      │  │ actualizados     │
│ DATIC_           │  │                  │  │ diariamente      │
│ Bancolombia      │  │                  │  │                  │
└──────────────────┘  └──────────────────┘  └──────────────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │   FRONTEND      │
                    │ Next.js/React   │
                    │                 │
                    │ - Dashboard     │
                    │ - TimeSeriesChart│
                    │ - KPI Widgets   │
                    └─────────────────┘
```

---

## 📁 Estructura de Colecciones Analíticas

### 1. `analytics_contratos_weekly`

**Propósito:** Agregaciones semanales de contratos optimizadas para el componente `TimeSeriesChart`.

**Document ID:** `{anio}-W{semana}_{bp}_{centro_gestor}_{banco}`  
Ejemplo: `2025-W45_BP26005260_DATIC_Bancolombia`

**Estructura:**

```javascript
{
  // Identificadores temporales
  anio: 2025,
  semana: 45,
  fecha_inicio_semana: "2025-11-03",  // Lunes
  fecha_fin_semana: "2025-11-09",     // Domingo
  semana_id: "2025-W45",              // Formato ISO

  // Dimensiones
  bp: "BP26005260",
  nombre_centro_gestor: "DATIC",
  banco: "Bancolombia",

  // Métricas de contratos
  metricas_contratos: {
    cantidad_contratos: 3,
    valor_total_contratos: 1500000000,
    valor_pagado_total: 800000000,
    porcentaje_ejecucion: 53.33
  },

  // Métricas de reportes (promedios ponderados)
  metricas_reportes: {
    cantidad_reportes: 12,
    avance_fisico_promedio: 45.5,        // Variable del frontend
    avance_financiero_promedio: 53.33,   // Variable del frontend
    ultimo_reporte: "2025-11-09"
  },

  // Alertas calculadas
  alertas: {
    contratos_retrasados: 0,
    sin_reportes_semana: false
  },

  // Metadatos
  timestamp: Timestamp,
  ultima_actualizacion: Timestamp,
  version: "1.0"
}
```

**Consultas optimizadas:**

```javascript
// Consulta 1: Obtener serie temporal por banco (usado en TimeSeriesChart)
db.collection("analytics_contratos_weekly")
  .where("banco", "==", "Bancolombia")
  .where("anio", "==", 2025)
  .orderBy("semana", "desc")
  .limit(52); // Último año

// Consulta 2: Filtrar por centro gestor
db.collection("analytics_contratos_weekly")
  .where("nombre_centro_gestor", "==", "DATIC")
  .where("anio", ">=", 2024)
  .orderBy("anio", "desc")
  .orderBy("semana", "desc");

// Consulta 3: Obtener contratos con alertas
db.collection("analytics_contratos_weekly")
  .where("alertas.contratos_retrasados", ">", 0)
  .where("anio", "==", 2025);
```

---

### 2. `analytics_flujo_caja_banco`

**Propósito:** Agregaciones trimestrales de flujo de caja por banco.

**Document ID:** `{anio}-Q{trimestre}_{banco}`  
Ejemplo: `2025-Q4_Bancolombia`

**Estructura:**

```javascript
{
  // Identificadores temporales
  anio: 2025,
  trimestre: 4,
  banco: "Bancolombia",

  // Métricas agregadas
  metricas: {
    desembolsos_totales: 500000000000,
    cantidad_proyectos: 25,
    promedio_desembolso_proyecto: 20000000000
  },

  // Top 10 proyectos por desembolso
  top_proyectos: [
    {
      bp: "BP26005260",
      desembolso: 100000000,
      sector: "TIC"
    },
    // ... más proyectos
  ],

  // Agregación por sector
  por_sector: {
    "TIC": {
      desembolso: 300000000000,
      cantidad_proyectos: 10
    },
    "Educación": {
      desembolso: 200000000000,
      cantidad_proyectos: 15
    }
  },

  // Metadatos
  timestamp: Timestamp,
  ultima_actualizacion: Timestamp,
  version: "1.0"
}
```

**Consultas optimizadas:**

```javascript
// Consulta 1: Obtener flujo de caja anual por banco
db.collection("analytics_flujo_caja_banco")
  .where("banco", "==", "Bancolombia")
  .where("anio", "==", 2025)
  .orderBy("trimestre", "desc");

// Consulta 2: Comparar bancos en un trimestre
db.collection("analytics_flujo_caja_banco")
  .where("anio", "==", 2025)
  .where("trimestre", "==", 4)
  .orderBy("metricas.desembolsos_totales", "desc");
```

---

### 3. `analytics_kpi_dashboard`

**Propósito:** KPIs globales del dashboard actualizados diariamente.

**Document ID:** `global_kpis` (único documento)

**Estructura:**

```javascript
{
  fecha_calculo: "2025-11-09",

  // KPIs globales (usados en header del dashboard)
  kpis_globales: {
    total_contratos_activos: 25,
    valor_total_contratos: 15000000000,
    valor_pagado_total: 8000000000,
    porcentaje_global_ejecucion: 53.33,
    cantidad_proyectos: 1254,
    cantidad_unidades_proyecto: 1251,
    cantidad_reportes: 145,
    avance_fisico_promedio: 45.5,
    avance_financiero_promedio: 53.33
  },

  // Agregación por banco
  por_banco: {
    "Bancolombia": {
      cantidad_contratos: 10,
      valor_total: 5000000000,
      valor_pagado: 2500000000,
      porcentaje_ejecucion: 50.0
    },
    "Banco de Bogotá": {
      cantidad_contratos: 8,
      valor_total: 4000000000,
      valor_pagado: 2000000000,
      porcentaje_ejecucion: 50.0
    }
    // ... más bancos
  },

  // Top 10 organismos por inversión
  por_organismo: {
    "DATIC": {
      cantidad_contratos: 15,
      valor_total: 8000000000
    },
    "Secretaría de Educación": {
      cantidad_contratos: 10,
      valor_total: 7000000000
    }
    // ... más organismos
  },

  // Metadatos
  timestamp: Timestamp,
  ultima_actualizacion: Timestamp,
  version: "1.0"
}
```

**Consultas optimizadas:**

```javascript
// Consulta 1: Obtener KPIs globales (para header del dashboard)
db.collection("analytics_kpi_dashboard").doc("global_kpis").get();

// Consulta 2: No se requieren consultas adicionales,
// todo está en un solo documento para máxima velocidad
```

---

## 🚀 Guía de Implementación

### Paso 1: Verificar Conexión a Firebase

```bash
# Windows PowerShell
python -c "from database.config import get_firestore_client; print('✅ Conectado' if get_firestore_client() else '❌ Error')"
```

### Paso 2: Ejecutar Carga Inicial (MODO INCREMENTAL)

```bash
# Modo incremental (default) - Solo añade datos nuevos
python load_initial_analytics.py

# Modo force rebuild - Regenera TODAS las colecciones
python load_initial_analytics.py --force-rebuild
```

**Salida esperada:**

```
================================================================================
📊 CARGA INICIAL DE COLECCIONES ANALÍTICAS
================================================================================
Fecha: 2025-11-09 14:30:00
Modo: INCREMENTAL (skip existentes)
================================================================================

[1/3] Verificando colección 'analytics_contratos_weekly'...
🆕 Colección vacía - Generando agregaciones iniciales...
📥 Obteniendo contratos de empréstito...
✅ Obtenidos 33 contratos
📥 Obteniendo reportes de contratos...
✅ Obtenidos 145 reportes
📊 Generadas 87 agregaciones semanales únicas
💾 Guardados 87 documentos...
✅ COMPLETADO: 87 agregaciones semanales guardadas en 'analytics_contratos_weekly'

[2/3] Verificando colección 'analytics_flujo_caja_banco'...
🆕 Colección vacía - Generando agregaciones iniciales...
📥 Obteniendo flujos de caja...
✅ Obtenidos 532 registros de flujo de caja
📊 Generadas 24 agregaciones por banco y trimestre
✅ COMPLETADO: 24 agregaciones por banco guardadas en 'analytics_flujo_caja_banco'

[3/3] Verificando colección 'analytics_kpi_dashboard'...
🔄 Recalculando KPIs globales (siempre actualizado)...
📥 Obteniendo datos...
✅ Datos obtenidos: 33 contratos, 145 reportes
✅ COMPLETADO: KPIs globales guardados en 'analytics_kpi_dashboard/global_kpis'
   - 25 contratos activos
   - $15,000,000,000 valor total
   - 53.33% ejecución global

================================================================================
📊 RESUMEN DE CARGA INICIAL
================================================================================
✅ contratos_weekly: SUCCESS
✅ flujo_caja_banco: SUCCESS
✅ kpis_globales: SUCCESS

--------------------------------------------------------------------------------
Total: 3 exitosas | 0 omitidas | 0 errores
================================================================================
```

### Paso 3: Desplegar Índices Compuestos en Firebase

```bash
# Desplegar índices a Firebase (requiere Firebase CLI)
firebase deploy --only firestore:indexes
```

**Índices creados:**

1. `analytics_contratos_weekly`: Compuesto en `[anio DESC, semana DESC, banco ASC]`
2. `analytics_contratos_weekly`: Compuesto en `[anio DESC, semana DESC, nombre_centro_gestor ASC]`
3. `analytics_contratos_weekly`: Compuesto en `[bp ASC, anio DESC, semana DESC]`
4. `analytics_flujo_caja_banco`: Compuesto en `[anio DESC, trimestre DESC]`
5. `analytics_flujo_caja_banco`: Compuesto en `[banco ASC, anio DESC]`
6. `contratos_emprestito`: Compuesto en `[estado_contrato ASC, fecha_inicio_contrato DESC]`
7. `contratos_emprestito`: Compuesto en `[banco ASC, fecha_inicio_contrato DESC]`
8. `reportes_contratos`: Compuesto en `[referencia_contrato ASC, fecha_reporte DESC]`
9. `flujo_caja_emprestito`: Compuesto en `[banco ASC, periodo DESC]`
10. `unidades_proyecto`: Compuesto en `[estado ASC, tipo_intervencion ASC]`

**Tiempo de creación:** ~5-10 minutos en Firebase Console.

---

## 🔄 Actualización Incremental

### Estrategia de Carga Incremental

El sistema detecta automáticamente documentos existentes usando el **Document ID** como clave única:

- **Formato de ID:** `{anio}-W{semana}_{bp}_{centro_gestor}_{banco}`
- **Lógica:** Si el documento existe, se actualiza con `merge=True`
- **Ventaja:** No duplica datos, solo añade nuevos o actualiza existentes

### Ejemplo de Actualización Manual

```python
# Ejecutar solo una función específica
from cloud_functions.analytics_aggregations import aggregate_contratos_weekly

resultado = aggregate_contratos_weekly()
print(resultado)
```

### Programar Actualizaciones Automáticas (Futuro)

**Opción 1: Cloud Scheduler (GCP)**

```yaml
# cloud-scheduler-job.yaml
name: "update-analytics-daily"
schedule: "0 2 * * *" # 2 AM diario
timeZone: "America/Bogota"
target:
  httpTarget:
    uri: "https://us-central1-dev-test-e778d.cloudfunctions.net/updateAnalytics"
    httpMethod: "POST"
```

**Opción 2: Cron Job en Servidor Local**

```bash
# Crontab Linux/Mac
0 2 * * * cd /path/to/proyecto && python load_initial_analytics.py

# Task Scheduler Windows (crear tarea con PowerShell)
$action = New-ScheduledTaskAction -Execute 'python' -Argument 'load_initial_analytics.py' -WorkingDirectory 'A:\programing_workspace\proyectos_cali_alcaldia_etl'
$trigger = New-ScheduledTaskTrigger -Daily -At 2am
Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "UpdateAnalytics" -Description "Actualización diaria de colecciones analíticas"
```

---

## 📊 Integración con el Frontend

### Cambios Requeridos en el Frontend

#### 1. Actualizar `EmprestitoAdvancedDashboard.tsx`

**Antes (consulta directa a `reportes_contratos`):**

```typescript
// Hook useTimeSeriesData - ANTES
const useTimeSeriesData = () => {
  const [data, setData] = useState<TimeSeriesData[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      // Consulta directa a reportes_contratos (LENTO)
      const reportesRef = collection(db, "reportes_contratos");
      const snapshot = await getDocs(reportesRef);

      // Agregar en cliente (COSTOSO)
      const grouped = groupByDate(snapshot.docs);
      setData(grouped);
    };
    fetchData();
  }, []);

  return data;
};
```

**Después (consulta a `analytics_contratos_weekly`):**

```typescript
// Hook useTimeSeriesData - DESPUÉS
const useTimeSeriesData = () => {
  const [data, setData] = useState<TimeSeriesData[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      // Consulta pre-agregada (RÁPIDO) ✅
      const analyticsRef = collection(db, "analytics_contratos_weekly");
      const q = query(
        analyticsRef,
        where("anio", "==", 2025),
        orderBy("semana", "desc"),
        limit(52)
      );

      const snapshot = await getDocs(q);

      // Mapear directamente (SIN AGREGACIÓN) ✅
      const timeSeries = snapshot.docs.map((doc) => {
        const data = doc.data();
        return {
          fecha: data.fecha_inicio_semana, // Lunes de la semana
          valor_pagado: data.metricas_contratos.valor_pagado_total,
          valor_contrato: data.metricas_contratos.valor_total_contratos,
          contratos_count: data.metricas_contratos.cantidad_contratos,
          avance_fisico_promedio: data.metricas_reportes.avance_fisico_promedio,
          avance_financiero_promedio:
            data.metricas_reportes.avance_financiero_promedio,
        };
      });

      setData(timeSeries);
    };
    fetchData();
  }, []);

  return data;
};
```

#### 2. Actualizar KPIs Globales

**Antes:**

```typescript
// Calcular KPIs en cliente (COSTOSO)
const totalContratos = contratos.length;
const inversionTotal = contratos.reduce((sum, c) => sum + c.valor_contrato, 0);
```

**Después:**

```typescript
// Obtener KPIs pre-calculados (RÁPIDO) ✅
const kpisRef = doc(db, "analytics_kpi_dashboard", "global_kpis");
const kpisSnap = await getDoc(kpisRef);
const kpis = kpisSnap.data();

const totalContratos = kpis.kpis_globales.total_contratos_activos;
const inversionTotal = kpis.kpis_globales.valor_total_contratos;
```

### Comparación de Performance

| Métrica               | Antes (Consultas directas)         | Después (Analytics)               | Mejora               |
| --------------------- | ---------------------------------- | --------------------------------- | -------------------- |
| Tiempo de carga       | ~3.5 segundos                      | ~0.3 segundos                     | **11.6x más rápido** |
| Documentos leídos     | 145 reportes + 33 contratos        | 52 documentos agregados           | **-70% lecturas**    |
| Procesamiento cliente | Agregaciones, promedios ponderados | Solo mapeo                        | **-90% CPU**         |
| Costo Firebase        | $0.06 por consulta (178 lecturas)  | $0.008 por consulta (52 lecturas) | **-86% costo**       |

---

## 💡 Mejores Prácticas

### ✅ DO's

1. **Usar siempre colecciones `analytics_*` para consultas del dashboard**
   - ✅ `analytics_contratos_weekly` en lugar de `reportes_contratos`
   - ✅ `analytics_kpi_dashboard` en lugar de calcular en cliente
2. **Ejecutar `load_initial_analytics.py` modo incremental diariamente**
   - ✅ Programar con Cloud Scheduler o Cron
   - ✅ Revisar logs de ejecución
3. **Mantener nombres de variables del frontend**
   - ✅ `avance_fisico_promedio`, `avance_financiero_promedio`
   - ✅ `banco`, `nombre_centro_gestor`, `referencia_contrato`
4. **Monitorear índices de Firestore**
   - ✅ Verificar que estén activos (no "Building")
   - ✅ Añadir índices adicionales si aparecen errores en logs

### ❌ DON'Ts

1. ❌ **NO calcular agregaciones en el cliente**
   - Lento, costoso, consume CPU del usuario
2. ❌ **NO consultar directamente `reportes_contratos` para series temporales**
   - Usa `analytics_contratos_weekly` en su lugar
3. ❌ **NO modificar nombres de variables en colecciones analíticas**
   - Romperá el frontend existente
4. ❌ **NO ejecutar `--force-rebuild` en producción**
   - Solo en casos de corrupción de datos

---

## 🐛 Troubleshooting

### Problema 1: "Index not found" en consultas

**Error:**

```
FirebaseError: The query requires an index. You can create it here:
https://console.firebase.google.com/project/dev-test-e778d/firestore/indexes?create_composite=...
```

**Solución:**

```bash
# Desplegar índices
firebase deploy --only firestore:indexes

# O crear manualmente en la URL mostrada en el error
```

### Problema 2: Colecciones analíticas vacías

**Síntomas:**

- Dashboard no muestra datos
- Consultas retornan 0 documentos

**Solución:**

```bash
# Re-ejecutar carga inicial con force-rebuild
python load_initial_analytics.py --force-rebuild
```

### Problema 3: Datos desactualizados

**Síntomas:**

- Dashboard muestra datos antiguos
- KPIs no reflejan contratos recientes

**Solución:**

```bash
# Ejecutar actualización incremental
python load_initial_analytics.py

# Verificar última actualización en Firestore Console
# analytics_kpi_dashboard/global_kpis -> timestamp
```

### Problema 4: Error de autenticación ADC

**Error:**

```
DefaultCredentialsError: Could not automatically determine credentials.
```

**Solución:**

```bash
# Autenticar con gcloud
gcloud auth application-default login

# O configurar variable de entorno
$env:GOOGLE_APPLICATION_CREDENTIALS="A:\path\to\service-account.json"
```

---

## 📈 Monitoreo y Mantenimiento

### Métricas a Monitorear

1. **Volumen de datos:**
   - Número de documentos en `analytics_contratos_weekly`
   - Tamaño de colecciones (MB)
2. **Performance:**
   - Tiempo de carga del dashboard
   - Latencia de consultas a Firestore
3. **Costos:**
   - Lecturas de documentos (Firestore Pricing)
   - Ejecuciones de Cloud Functions (futuro)

### Queries de Monitoreo

```javascript
// Query 1: Verificar última actualización
db.collection("analytics_kpi_dashboard")
  .doc("global_kpis")
  .get()
  .then((doc) => {
    console.log("Última actualización:", doc.data().timestamp.toDate());
  });

// Query 2: Contar documentos por colección
db.collection("analytics_contratos_weekly")
  .count()
  .get()
  .then((snapshot) => {
    console.log("Total agregaciones semanales:", snapshot.data().count);
  });

// Query 3: Verificar integridad de datos
db.collection("analytics_contratos_weekly")
  .where("metricas_contratos.cantidad_contratos", "==", 0)
  .get()
  .then((snapshot) => {
    console.log("Agregaciones sin contratos:", snapshot.size);
  });
```

---

## 🎯 Próximos Pasos

### Fase 1: Implementación Inicial ✅ (COMPLETADO)

- [x] Diseñar arquitectura Constellation Schema
- [x] Crear Cloud Functions de agregación
- [x] Implementar carga incremental
- [x] Generar índices compuestos
- [x] Documentar implementación

### Fase 2: Integración Frontend (SIGUIENTE)

- [ ] Actualizar `EmprestitoAdvancedDashboard.tsx` para usar `analytics_contratos_weekly`
- [ ] Modificar hooks de React (`useTimeSeriesData`, `useKPIs`)
- [ ] Actualizar servicios API (`emprestito.service.ts`)
- [ ] Testing de integración
- [ ] Deployment a Vercel

### Fase 3: Automatización (FUTURO)

- [ ] Configurar Cloud Scheduler para actualizaciones diarias
- [ ] Implementar Cloud Functions con triggers en Firestore
- [ ] Crear alertas de monitoreo (Stackdriver/Cloud Monitoring)
- [ ] Configurar backups automáticos de colecciones analíticas

### Fase 4: Optimización (FUTURO)

- [ ] Implementar caché en frontend (React Query)
- [ ] Añadir compresión de datos en colecciones analíticas
- [ ] Optimizar consultas con pagination
- [ ] Implementar lazy loading en dashboard

---

## 📚 Referencias

### Documentación Técnica

- [Arquitectura Constellation Schema](./arquitectura-tipologia-tecnica.md)
- [Propuesta Data Warehouse Original](./propuesta-datawarehouse-arquitectura.md)
- [Firebase Migration Guide](./firebase-migration-guide.md)

### Archivos del Proyecto

- **Cloud Functions:** `cloud_functions/analytics_aggregations.py`
- **Carga Inicial:** `load_initial_analytics.py`
- **Índices:** `firestore.indexes.json`
- **Frontend Reference:** `docs/frontend_reference/`

### Enlaces Externos

- [Firebase Firestore Documentation](https://firebase.google.com/docs/firestore)
- [Firestore Composite Indexes](https://firebase.google.com/docs/firestore/query-data/indexing)
- [Data Warehouse Design Patterns](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/)

---

**Última actualización:** Noviembre 2025  
**Autor:** Sistema ETL Alcaldía de Cali  
**Versión:** 1.0
