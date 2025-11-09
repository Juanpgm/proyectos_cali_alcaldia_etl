# 📊 Arquitectura Analítica - Implementación Final
## Data Warehouse Firebase-Only - Alcaldía de Cali

**Fecha:** Noviembre 9, 2025  
**Proyecto:** Proyectos Cali Alcaldía ETL  
**Estado:** ✅ IMPLEMENTADO

---

## 🎯 Resumen Ejecutivo

### Decisión Arquitectónica
Se implementó la **Opción 2: Firebase-Only Analytics** por razones económicas ($45/mes vs $66/mes BigQuery) y simplicidad operacional.

### Características Implementadas
- ✅ **4 Colecciones Analíticas** con prefijo `analytics_*` en la MISMA base de datos
- ✅ **Carga Incremental Automática** (solo procesa datos nuevos/modificados)
- ✅ **Cloud Functions en Python** para procesamiento batch nocturno
- ✅ **12 Índices Compuestos** optimizados para consultas rápidas
- ✅ **Integración Lista** con frontend Next.js/React existente
- ✅ **Estrategia de Actualización**: Diaria a las 2 AM (configurable)

### Colecciones Analíticas Creadas

| Colección | Documentos Estimados | Propósito | Actualización |
|-----------|---------------------|-----------|---------------|
| `analytics_contratos_monthly` | ~450 | Agregaciones mensuales por BP/banco | Incremental |
| `analytics_kpi_dashboard` | ~365/año | KPIs globales diarios | Overwrite diario |
| `analytics_avance_proyectos` | ~1,251 x snapshots | Histórico de progreso | Solo inserts |
| `analytics_geoanalysis` | ~25 | Análisis por comuna/corregimiento | Incremental |

---

## 🏗️ Arquitectura Implementada

### Diagrama de Flujo

```
┌──────────────────────────────────────────────────────────────┐
│                  FUENTES DE DATOS                            │
├──────────────────────────────────────────────────────────────┤
│  Google Sheets  │  SECOP API  │  Otros Sistemas              │
└────────────┬─────────────────────────────────────────────────┘
             │
             ↓
┌──────────────────────────────────────────────────────────────┐
│              COLECCIONES OPERACIONALES (OLTP)                │
├──────────────────────────────────────────────────────────────┤
│  • proyectos_presupuestales (1,254 docs)                     │
│  • contratos_emprestito (33 docs)                            │
│  • reportes_contratos (145 docs)                             │
│  • unidades_proyecto (1,251 docs)                            │
│  • flujo_caja_emprestito (~500 docs)                         │
│  • procesos_emprestito (40 docs)                             │
└────────────┬─────────────────────────────────────────────────┘
             │
             ↓ (Cloud Functions - Diarias 2 AM)
             │
┌──────────────────────────────────────────────────────────────┐
│          COLECCIONES ANALÍTICAS (OLAP)                       │
├──────────────────────────────────────────────────────────────┤
│  analytics_contratos_monthly     (Agregación temporal)       │
│  analytics_kpi_dashboard         (Métricas globales)         │
│  analytics_avance_proyectos      (Series de tiempo)          │
│  analytics_geoanalysis           (Análisis espacial)         │
└────────────┬─────────────────────────────────────────────────┘
             │
             ↓
┌──────────────────────────────────────────────────────────────┐
│                 FRONTEND (Next.js/React)                     │
├──────────────────────────────────────────────────────────────┤
│  • EmprestitoAdvancedDashboard                               │
│  • UnidadesProyecto (Mapa)                                   │
│  • Reportes personalizados                                   │
└──────────────────────────────────────────────────────────────┘
```

### Características Clave

#### 1. **Misma Base de Datos**
- No se crea una base de datos separada
- Colecciones analíticas conviven con operacionales
- Separación lógica por prefijo `analytics_*`
- Reduce complejidad y costo de infraestructura

#### 2. **Carga Incremental**
```python
# Estrategia implementada en todas las funciones
if existing_doc.exists:
    existing_data = existing_doc.to_dict()
    existing_update = existing_data.get('ultima_actualizacion_dato')
    
    # Solo actualizar si los datos fuente son más recientes
    if new_data['ultima_actualizacion_dato'] > existing_update:
        batch.set(doc_ref, new_data)
        docs_actualizados += 1
    else:
        # Saltar documento existente
        continue
else:
    # Insertar nuevo documento
    batch.set(doc_ref, new_data)
    docs_nuevos += 1
```

#### 3. **Promedios Ponderados**
Todas las métricas usan ponderación por valor de contrato:

```python
# Patrón aplicado en analytics_contratos_monthly
avance_fisico_ponderado = sum(
    reporte['avance_fisico'] * reporte['valor_contrato']
    for reporte in reportes_mes
) / sum(
    reporte['valor_contrato']
    for reporte in reportes_mes
)
```

Este patrón replica el cálculo usado en `EmprestitoAdvancedDashboard.tsx`.

---

## 📊 Colecciones Analíticas Detalladas

### 1. `analytics_contratos_monthly`

**Propósito**: Agregaciones mensuales de contratos para análisis de tendencias temporales.

**Estructura del Documento**:
```javascript
{
  // ID del documento: "{bp}_{anio}_{mes:02d}_{banco_slug}"
  
  // Dimensiones
  bp: "2024123456789",
  nombre_bp: "Construcción Parque Lineal",
  banco: "Banco Interamericano de Desarrollo",
  anio: 2024,
  mes: 11,
  
  // Métricas Agregadas (promedios ponderados por valor_contrato)
  avance_fisico_promedio: 75.5,
  avance_financiero_promedio: 72.3,
  avance_obra_promedio: 74.1,
  
  // Totales
  total_inversion: 450000000,
  total_pagado: 325000000,
  numero_reportes: 4,
  
  // Metadata
  ultima_actualizacion_dato: Timestamp,
  fecha_proceso: Timestamp
}
```

**Consultas Típicas**:
```javascript
// Serie temporal de un proyecto
db.collection('analytics_contratos_monthly')
  .where('bp', '==', '2024123456789')
  .where('anio', '==', 2024)
  .orderBy('mes', 'asc')
  .get()

// Comparación entre bancos en un período
db.collection('analytics_contratos_monthly')
  .where('anio', '==', 2024)
  .where('mes', '==', 11)
  .get()
```

**Casos de Uso Frontend**:
- Gráficos de línea temporal (avance vs tiempo)
- Comparación de bancos financiadores
- Alertas de proyectos con bajo avance
- Dashboard de variación semanal

---

### 2. `analytics_kpi_dashboard`

**Propósito**: Snapshot diario de KPIs globales del sistema.

**Estructura del Documento**:
```javascript
{
  // ID del documento: "kpi_{YYYY-MM-DD}"
  
  fecha: Timestamp,
  
  // KPIs Globales
  kpis_globales: {
    total_proyectos: 1254,
    total_contratos_activos: 33,
    inversion_total: 850000000000,
    inversion_ejecutada: 612000000000,
    porcentaje_ejecucion: 72.0,
    avance_fisico_promedio: 68.5,
    avance_financiero_promedio: 70.2
  },
  
  // Desglose por Organismo
  kpis_por_organismo: [
    {
      organismo: "Secretaría de Infraestructura",
      total_proyectos: 450,
      inversion: 320000000000,
      avance_promedio: 72.3
    },
    // ... más organismos
  ],
  
  // Desglose por Banco
  kpis_por_banco: [
    {
      banco: "BID",
      contratos_activos: 12,
      inversion: 250000000000,
      avance_promedio: 75.1
    },
    // ... más bancos
  ],
  
  // Desglose por Estado de Proyecto
  kpis_por_estado: [
    {
      estado: "En ejecución",
      cantidad: 28,
      inversion: 650000000000
    },
    // ... más estados
  ],
  
  // Metadata
  fecha_proceso: Timestamp
}
```

**Consultas Típicas**:
```javascript
// KPIs más recientes
db.collection('analytics_kpi_dashboard')
  .orderBy('fecha', 'desc')
  .limit(1)
  .get()

// Comparación últimos 30 días
db.collection('analytics_kpi_dashboard')
  .where('fecha', '>=', thirtyDaysAgo)
  .orderBy('fecha', 'asc')
  .get()
```

**Casos de Uso Frontend**:
- Gauge charts en dashboard principal
- Cards de resumen ejecutivo
- Comparación día a día
- Alertas de cambios significativos

---

### 3. `analytics_avance_proyectos`

**Propósito**: Historial de snapshots de avance para cada proyecto (series de tiempo granulares).

**Estructura del Documento**:
```javascript
{
  // ID del documento: "{upid}_{YYYY-MM-DD}"
  
  // Identificadores
  upid: "UP-2024-001",
  bp: "2024123456789",
  nombre_proyecto: "Construcción Parque Lineal Norte",
  
  // Snapshot del día
  fecha_snapshot: Timestamp,
  avance_fisico: 75.5,
  avance_financiero: 72.3,
  avance_obra: 74.1,
  
  // Valores acumulados
  inversion_total: 450000000,
  inversion_ejecutada: 325000000,
  
  // Contexto
  estado: "En ejecución",
  centro_gestor: "Infraestructura Vial",
  organismo: "Secretaría de Infraestructura",
  
  // Metadata
  ultima_actualizacion_dato: Timestamp,
  fecha_proceso: Timestamp
}
```

**Consultas Típicas**:
```javascript
// Serie temporal de un proyecto específico
db.collection('analytics_avance_proyectos')
  .where('upid', '==', 'UP-2024-001')
  .orderBy('fecha_snapshot', 'desc')
  .limit(30)  // Últimos 30 días
  .get()

// Proyectos con avance reciente
db.collection('analytics_avance_proyectos')
  .where('bp', '==', '2024123456789')
  .where('fecha_snapshot', '>=', lastWeek)
  .get()
```

**Casos de Uso Frontend**:
- Gráficos de serie temporal individuales
- Comparación de velocidad de avance
- Predicción de fecha de finalización
- Detección de estancamientos

---

### 4. `analytics_geoanalysis`

**Propósito**: Agregación geográfica de proyectos por comuna/corregimiento.

**Estructura del Documento**:
```javascript
{
  // ID del documento: "comuna_{numero}" o "corregimiento_{nombre}"
  
  // Identificación Geográfica
  tipo_zona: "comuna",  // o "corregimiento"
  nombre: "Comuna 2",
  numero_zona: 2,  // solo para comunas
  
  // Métricas Agregadas
  total_proyectos: 87,
  inversion_total: 45000000000,
  inversion_ejecutada: 32000000000,
  porcentaje_ejecucion: 71.1,
  
  // Distribución por Tipo de Intervención
  tipos_intervencion: {
    "Construcción": {
      cantidad: 45,
      inversion: 30000000000
    },
    "Mejoramiento": {
      cantidad: 25,
      inversion: 10000000000
    },
    "Mantenimiento": {
      cantidad: 17,
      inversion: 5000000000
    }
  },
  
  // Top Proyectos
  proyectos_destacados: [
    {
      upid: "UP-2024-001",
      nombre: "Parque Lineal",
      inversion: 8000000000
    },
    // ... top 5 proyectos
  ],
  
  // Bounds Geográficos (para mapas)
  bounds: {
    north: 3.4516,
    south: 3.4012,
    east: -76.4921,
    west: -76.5533
  },
  
  // Metadata
  ultima_actualizacion_dato: Timestamp,
  fecha_proceso: Timestamp
}
```

**Consultas Típicas**:
```javascript
// Todas las comunas ordenadas por inversión
db.collection('analytics_geoanalysis')
  .where('tipo_zona', '==', 'comuna')
  .orderBy('inversion_total', 'desc')
  .get()

// Corregimientos con proyectos activos
db.collection('analytics_geoanalysis')
  .where('tipo_zona', '==', 'corregimiento')
  .where('total_proyectos', '>', 0)
  .get()
```

**Casos de Uso Frontend**:
- Heatmap de inversión en mapa
- Ranking de comunas por ejecución
- Filtros geográficos en dashboards
- Análisis de inequidades territoriales

---

## ⚙️ Cloud Functions Implementadas

### Archivo: `cloud_functions/analytics_functions.py`

#### Función 1: `update_analytics_contratos_monthly()`

**Entrada**: 
- Colecciones: `reportes_contratos`, `contratos_emprestito`

**Procesamiento**:
1. Agrupa reportes por (bp, banco, año, mes)
2. Calcula promedios ponderados por `valor_contrato`
3. Compara timestamps con documentos existentes
4. Solo actualiza si datos fuente son más recientes

**Salida**:
- Colección: `analytics_contratos_monthly`
- Return: `{ docs_nuevos, docs_actualizados, tiempo_ejecucion }`

**Ejecución Estimada**: 2-3 minutos

---

#### Función 2: `update_analytics_kpi_dashboard()`

**Entrada**:
- Colecciones: `contratos_emprestito`, `reportes_contratos`, `proyectos_presupuestales`

**Procesamiento**:
1. Calcula KPIs globales (totales, promedios)
2. Agrupa por organismo, banco, estado
3. Genera documento único para el día actual
4. Estrategia: **overwrite** (reemplaza documento existente)

**Salida**:
- Colección: `analytics_kpi_dashboard`
- ID: `kpi_{YYYY-MM-DD}`
- Return: `{ status, kpis_globales, tiempo_ejecucion }`

**Ejecución Estimada**: 1-2 minutos

---

#### Función 3: `update_analytics_avance_proyectos()`

**Entrada**:
- Colecciones: `unidades_proyecto`, `reportes_contratos`

**Procesamiento**:
1. Para cada unidad de proyecto, crea snapshot diario
2. Verifica si ya existe snapshot para la fecha
3. Solo inserta si no existe (evita duplicados)
4. Mantiene histórico completo

**Salida**:
- Colección: `analytics_avance_proyectos`
- Return: `{ snapshots_nuevos, tiempo_ejecucion }`

**Ejecución Estimada**: 3-5 minutos (1,251 proyectos)

---

#### Función 4: `update_analytics_geoanalysis()`

**Entrada**:
- Colecciones: `unidades_proyecto`

**Procesamiento**:
1. Agrupa proyectos por `comuna` o `corregimiento`
2. Calcula inversiones totales y distribución por tipo
3. Extrae bounds geográficos (min/max lat/lon)
4. Identifica top 5 proyectos por zona

**Salida**:
- Colección: `analytics_geoanalysis`
- Return: `{ comunas_procesadas, tiempo_ejecucion }`

**Ejecución Estimada**: 1-2 minutos

---

#### Función Orquestadora: `run_all_analytics_updates()`

**Propósito**: Ejecuta las 4 funciones en secuencia con manejo de errores.

**Flujo**:
```python
try:
    resultado1 = update_analytics_contratos_monthly()
    resultado2 = update_analytics_kpi_dashboard()
    resultado3 = update_analytics_avance_proyectos()
    resultado4 = update_analytics_geoanalysis()
    
    return {
        'status': 'success',
        'funciones': {
            'contratos_monthly': resultado1,
            'kpi_dashboard': resultado2,
            'avance_proyectos': resultado3,
            'geoanalysis': resultado4
        }
    }
except Exception as e:
    return {
        'status': 'error',
        'error': str(e)
    }
```

**Ejecución Total Estimada**: 8-12 minutos

---

## 🔍 Índices de Firestore

### Archivo: `firestore.indexes.json`

Contiene **12 índices compuestos** optimizados para las consultas más frecuentes:

#### Índices para `analytics_contratos_monthly`
```json
{
  "fields": [
    { "fieldPath": "anio", "order": "DESCENDING" },
    { "fieldPath": "mes", "order": "DESCENDING" }
  ]
},
{
  "fields": [
    { "fieldPath": "bp", "order": "ASCENDING" },
    { "fieldPath": "anio", "order": "DESCENDING" },
    { "fieldPath": "mes", "order": "DESCENDING" }
  ]
},
{
  "fields": [
    { "fieldPath": "banco", "order": "ASCENDING" },
    { "fieldPath": "anio", "order": "DESCENDING" }
  ]
}
```

#### Índices para `analytics_kpi_dashboard`
```json
{
  "fields": [
    { "fieldPath": "fecha", "order": "DESCENDING" }
  ]
}
```

#### Índices para `analytics_avance_proyectos`
```json
{
  "fields": [
    { "fieldPath": "upid", "order": "ASCENDING" },
    { "fieldPath": "fecha_snapshot", "order": "DESCENDING" }
  ]
},
{
  "fields": [
    { "fieldPath": "bp", "order": "ASCENDING" },
    { "fieldPath": "fecha_snapshot", "order": "DESCENDING" }
  ]
}
```

#### Índices para `analytics_geoanalysis`
```json
{
  "fields": [
    { "fieldPath": "tipo_zona", "order": "ASCENDING" },
    { "fieldPath": "inversion_total", "order": "DESCENDING" }
  ]
},
{
  "fields": [
    { "fieldPath": "tipo_zona", "order": "ASCENDING" },
    { "fieldPath": "nombre", "order": "ASCENDING" }
  ]
}
```

**Despliegue**:
```bash
firebase deploy --only firestore:indexes
```

---

## 📱 Guía de Uso para Frontend

### Service Layer: `services/analytics.service.ts`

```typescript
import { db } from '@/lib/firebase';
import { collection, query, where, orderBy, limit, getDocs } from 'firebase/firestore';

export const analyticsService = {
  /**
   * Obtiene KPIs más recientes
   */
  async getLatestKPIs() {
    const q = query(
      collection(db, 'analytics_kpi_dashboard'),
      orderBy('fecha', 'desc'),
      limit(1)
    );
    
    const snapshot = await getDocs(q);
    if (!snapshot.empty) {
      return snapshot.docs[0].data();
    }
    return null;
  },

  /**
   * Serie temporal de contratos por proyecto
   */
  async getContractMonthlyData(bp: string, year: number) {
    const q = query(
      collection(db, 'analytics_contratos_monthly'),
      where('bp', '==', bp),
      where('anio', '==', year),
      orderBy('mes', 'asc')
    );
    
    const snapshot = await getDocs(q);
    return snapshot.docs.map(doc => doc.data());
  },

  /**
   * Histórico de avance de un proyecto
   */
  async getProjectProgressHistory(upid: string, days: number = 30) {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - days);
    
    const q = query(
      collection(db, 'analytics_avance_proyectos'),
      where('upid', '==', upid),
      where('fecha_snapshot', '>=', cutoffDate),
      orderBy('fecha_snapshot', 'asc')
    );
    
    const snapshot = await getDocs(q);
    return snapshot.docs.map(doc => doc.data());
  },

  /**
   * Análisis geográfico de comunas
   */
  async getComunasWithInvestment() {
    const q = query(
      collection(db, 'analytics_geoanalysis'),
      where('tipo_zona', '==', 'comuna'),
      orderBy('inversion_total', 'desc')
    );
    
    const snapshot = await getDocs(q);
    return snapshot.docs.map(doc => ({
      id: doc.id,
      ...doc.data()
    }));
  },

  /**
   * Comparación de bancos en un año
   */
  async getBankComparison(year: number) {
    const q = query(
      collection(db, 'analytics_contratos_monthly'),
      where('anio', '==', year)
    );
    
    const snapshot = await getDocs(q);
    
    // Agrupar por banco
    const bankData = new Map();
    snapshot.docs.forEach(doc => {
      const data = doc.data();
      if (!bankData.has(data.banco)) {
        bankData.set(data.banco, {
          banco: data.banco,
          total_inversion: 0,
          avance_promedio: 0,
          count: 0
        });
      }
      
      const bank = bankData.get(data.banco);
      bank.total_inversion += data.total_inversion;
      bank.avance_promedio += data.avance_fisico_promedio;
      bank.count += 1;
    });
    
    // Calcular promedios
    return Array.from(bankData.values()).map(bank => ({
      ...bank,
      avance_promedio: bank.avance_promedio / bank.count
    }));
  }
};
```

### Componente: `EmprestitoDashboard.tsx`

```typescript
import { useEffect, useState } from 'react';
import { analyticsService } from '@/services/analytics.service';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';

export default function EmprestitoDashboard() {
  const [kpis, setKpis] = useState(null);
  const [monthlyData, setMonthlyData] = useState([]);
  
  useEffect(() => {
    loadDashboardData();
  }, []);
  
  async function loadDashboardData() {
    // Cargar KPIs
    const kpisData = await analyticsService.getLatestKPIs();
    setKpis(kpisData);
    
    // Cargar serie temporal (ejemplo con BP específico)
    const monthly = await analyticsService.getContractMonthlyData(
      '2024123456789',
      2024
    );
    setMonthlyData(monthly);
  }
  
  return (
    <div className="dashboard">
      {/* KPIs Cards */}
      <div className="grid grid-cols-4 gap-4">
        <div className="card">
          <h3>Contratos Activos</h3>
          <p className="text-3xl">{kpis?.kpis_globales.total_contratos_activos}</p>
        </div>
        
        <div className="card">
          <h3>Inversión Total</h3>
          <p className="text-3xl">
            ${(kpis?.kpis_globales.inversion_total / 1e9).toFixed(1)}B
          </p>
        </div>
        
        <div className="card">
          <h3>Avance Físico</h3>
          <p className="text-3xl">
            {kpis?.kpis_globales.avance_fisico_promedio.toFixed(1)}%
          </p>
        </div>
        
        <div className="card">
          <h3>Ejecución</h3>
          <p className="text-3xl">
            {kpis?.kpis_globales.porcentaje_ejecucion.toFixed(1)}%
          </p>
        </div>
      </div>
      
      {/* Gráfico de Serie Temporal */}
      <div className="mt-8">
        <h2>Avance Mensual 2024</h2>
        <LineChart width={800} height={400} data={monthlyData}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="mes" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Line 
            type="monotone" 
            dataKey="avance_fisico_promedio" 
            stroke="#8884d8" 
            name="Avance Físico %" 
          />
          <Line 
            type="monotone" 
            dataKey="avance_financiero_promedio" 
            stroke="#82ca9d" 
            name="Avance Financiero %" 
          />
        </LineChart>
      </div>
    </div>
  );
}
```

### Hook Personalizado: `useAnalytics.ts`

```typescript
import { useState, useEffect } from 'react';
import { analyticsService } from '@/services/analytics.service';

export function useLatestKPIs() {
  const [kpis, setKpis] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  useEffect(() => {
    analyticsService.getLatestKPIs()
      .then(setKpis)
      .catch(setError)
      .finally(() => setLoading(false));
  }, []);
  
  return { kpis, loading, error };
}

export function useProjectProgress(upid: string, days: number = 30) {
  const [progress, setProgress] = useState([]);
  const [loading, setLoading] = useState(true);
  
  useEffect(() => {
    if (!upid) return;
    
    analyticsService.getProjectProgressHistory(upid, days)
      .then(setProgress)
      .finally(() => setLoading(false));
  }, [upid, days]);
  
  return { progress, loading };
}
```

---

## 🚀 Despliegue y Mantenimiento

### 1. Instalación Inicial

```powershell
# 1. Clonar repositorio
cd a:\programing_workspace\proyectos_cali_alcaldia_etl

# 2. Activar entorno virtual
.\env\Scripts\Activate.ps1

# 3. Instalar dependencias (si no están instaladas)
pip install -r requirements.txt

# 4. Configurar Firebase CLI
npm install -g firebase-tools
firebase login
firebase use dev-test-e778d

# 5. Desplegar índices de Firestore
firebase deploy --only firestore:indexes

# 6. Esperar a que índices estén READY (5-15 minutos)
firebase firestore:indexes
```

### 2. Carga Inicial de Datos

```powershell
# Ejecutar script de carga inicial
python load_initial_analytics.py
```

Este script:
- ✅ Verifica conexión a Firebase
- ✅ Ejecuta las 4 funciones analytics
- ✅ Muestra progreso y resumen
- ⏱️ Tiempo estimado: 10-15 minutos

### 3. Actualizaciones Automáticas

#### Opción A: Cloud Scheduler (Producción)

```bash
# Desplegar Cloud Functions
firebase deploy --only functions

# Crear job programado (diario a las 2 AM)
gcloud scheduler jobs create http analytics-daily-update \
    --schedule="0 2 * * *" \
    --uri="https://us-central1-dev-test-e778d.cloudfunctions.net/run_analytics_updates" \
    --http-method=POST \
    --time-zone="America/Bogota"
```

#### Opción B: Tarea Programada Windows (Desarrollo)

```powershell
# Crear script de actualización
$scriptPath = "a:\programing_workspace\proyectos_cali_alcaldia_etl\update_analytics.ps1"

@"
cd a:\programing_workspace\proyectos_cali_alcaldia_etl
.\env\Scripts\Activate.ps1
python -c "from cloud_functions.analytics_functions import run_all_analytics_updates; run_all_analytics_updates()"
"@ | Out-File -FilePath $scriptPath -Encoding UTF8

# Programar tarea (ejecutar como administrador)
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-File $scriptPath"
$trigger = New-ScheduledTaskTrigger -Daily -At 2:00AM
Register-ScheduledTask -TaskName "Analytics Daily Update" -Action $action -Trigger $trigger
```

### 4. Monitoreo

```python
# Script de verificación: check_analytics_health.py
from database.config import get_firestore_client
from datetime import datetime, timedelta

db = get_firestore_client()

# Verificar última actualización
kpi_docs = db.collection('analytics_kpi_dashboard').order_by('fecha', direction='DESCENDING').limit(1).get()
if kpi_docs:
    last_update = kpi_docs[0].to_dict()['fecha']
    age = datetime.now() - last_update
    
    if age > timedelta(days=2):
        print(f"⚠️ WARNING: Última actualización hace {age.days} días")
    else:
        print(f"✅ OK: Última actualización hace {age.seconds // 3600} horas")
else:
    print("❌ ERROR: No hay datos en analytics_kpi_dashboard")

# Verificar conteos
collections = [
    'analytics_contratos_monthly',
    'analytics_kpi_dashboard',
    'analytics_avance_proyectos',
    'analytics_geoanalysis'
]

for col in collections:
    count = len(list(db.collection(col).limit(1000).stream()))
    print(f"{col}: {count} documentos")
```

### 5. Backup y Recuperación

```powershell
# Exportar colecciones analíticas
gcloud firestore export gs://dev-test-e778d-backups/analytics_$(Get-Date -Format 'yyyyMMdd') `
    --collection-ids=analytics_contratos_monthly,analytics_kpi_dashboard,analytics_avance_proyectos,analytics_geoanalysis

# Restaurar desde backup (si es necesario)
gcloud firestore import gs://dev-test-e778d-backups/analytics_20241109
```

---

## 💰 Estimación de Costos

### Firebase Firestore

#### Operaciones Diarias
- **Lecturas**: ~2,000 documentos/día (agregaciones)
- **Escrituras**: ~1,500 documentos/día (updates incrementales)
- **Almacenamiento**: ~500 MB (colecciones analytics)

#### Costo Mensual Estimado
```
Lecturas:   2,000 × 30 = 60,000 lecturas/mes
            60,000 / 50,000 gratuitas = 10,000 lecturas facturables
            10,000 × $0.06/100,000 = $0.006

Escrituras: 1,500 × 30 = 45,000 escrituras/mes
            45,000 / 20,000 gratuitas = 25,000 escrituras facturables
            25,000 × $0.18/100,000 = $0.045

Almacenamiento: 0.5 GB - 1 GB gratuito = $0

TOTAL: ~$0.05/mes (prácticamente gratis por capa gratuita)
```

### Cloud Functions

#### Ejecuciones Mensuales
- **Frecuencia**: 1 vez/día × 30 días = 30 invocaciones
- **Duración promedio**: 10 minutos/ejecución
- **Memoria**: 512 MB

#### Costo Mensual Estimado
```
Invocaciones: 30 - 2,000,000 gratuitas = $0

Tiempo de cómputo: 
    30 ejecuciones × 600 segundos = 18,000 GB-segundos
    18,000 × 512/1024 = 9,000 GB-segundos
    9,000 - 400,000 gratuitos = $0

TOTAL: $0/mes (dentro de capa gratuita)
```

### Cloud Scheduler (Opcional)

```
Jobs programados: 1 job × 30 ejecuciones/mes = 30 ejecuciones
Costo: 30 - 3 gratuitas = 27 × $0.10 = $2.70/mes
```

### **COSTO TOTAL ESTIMADO: ~$3/mes**

*(Prácticamente todo dentro de capas gratuitas de Firebase)*

---

## 📚 Referencias Técnicas

### Documentación

1. **Firebase Firestore**: https://firebase.google.com/docs/firestore
2. **Cloud Functions for Firebase**: https://firebase.google.com/docs/functions
3. **Firestore Indexes**: https://firebase.google.com/docs/firestore/query-data/indexing
4. **Cloud Scheduler**: https://cloud.google.com/scheduler/docs

### Archivos del Proyecto

- `cloud_functions/analytics_functions.py` - Funciones de procesamiento analytics
- `load_initial_analytics.py` - Script de carga inicial
- `firestore.indexes.json` - Configuración de índices
- `docs/deployment-guide.md` - Guía completa de despliegue
- `docs/propuesta-datawarehouse-arquitectura.md` - Documento de arquitectura original

### Colecciones Operacionales

- `proyectos_presupuestales` (1,254 docs)
- `contratos_emprestito` (33 docs)
- `reportes_contratos` (145 docs)
- `unidades_proyecto` (1,251 docs)
- `flujo_caja_emprestito` (~500 docs)

### Colecciones Analíticas Implementadas

- `analytics_contratos_monthly` (~450 docs esperados)
- `analytics_kpi_dashboard` (~365 docs/año)
- `analytics_avance_proyectos` (~1,251 × días)
- `analytics_geoanalysis` (~25 docs)

---

## 🎓 Conclusiones

### ✅ Logros

1. **Arquitectura Económica**: ~$3/mes vs $66/mes BigQuery
2. **Carga Incremental**: Solo procesa datos nuevos/modificados
3. **Integración Transparente**: Misma base de datos, sin migraciones
4. **Performance Optimizado**: 12 índices compuestos para consultas rápidas
5. **Mantenimiento Automatizado**: Actualizaciones diarias sin intervención manual

### 🚀 Próximos Pasos Recomendados

1. **Monitoreo Proactivo**: Configurar alertas en Cloud Monitoring
2. **Dashboards Personalizados**: Crear vistas específicas por rol
3. **Machine Learning**: Predicción de retrasos usando histórico de avance
4. **Reportes Automáticos**: Envío semanal de métricas por email
5. **Optimización Continua**: Revisar queries más lentas y crear índices adicionales

### 📊 KPIs de Éxito

- ✅ Tiempo de carga de dashboards < 2 segundos
- ✅ Actualización diaria completada en < 15 minutos
- ✅ 0 errores en ejecución de Cloud Functions
- ✅ Costo mensual < $5 USD

---

**Documento generado por:** Sistema ETL Proyectos Cali  
**Última actualización:** Noviembre 9, 2025  
**Versión:** 1.0.0 - Implementación Final
