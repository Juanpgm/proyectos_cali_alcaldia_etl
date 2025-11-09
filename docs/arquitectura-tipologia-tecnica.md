# 🏛️ Tipología Técnica de la Arquitectura Data Warehouse

## 📊 Clasificación: **CONSTELLATION SCHEMA** (Esquema de Constelación)

### ¿Por qué NO es Estrella Simple?

**Esquema de Estrella (Star Schema) tradicional:**

- 1 tabla de hechos central
- Dimensiones desnormalizadas alrededor
- Simple pero inflexible

**Tu arquitectura tiene:**

- ✅ **Múltiples tablas de hechos** interrelacionadas
- ✅ **Dimensiones compartidas** entre hechos
- ✅ **Granularidades diferentes** por tabla de hechos

```
        dim_tiempo
            ↓
    ┌───────┼───────┐
    ↓       ↓       ↓
fact_contratos  fact_flujo_caja  fact_avance_proyectos
    ↓       ↓       ↓
    └───────┼───────┘
            ↓
      dim_proyecto ← Dimensión COMPARTIDA
```

---

## 🔍 Comparación Técnica de Topologías

### **1. STAR SCHEMA (Estrella)** ⭐

#### Características:

- **1 tabla de hechos** central
- Dimensiones **completamente desnormalizadas**
- Relaciones directas 1:N

#### Ejemplo:

```
         dim_tiempo
              ↓
         dim_proyecto
              ↓
    fact_ventas (ÚNICA) ← Centro del universo
              ↓
         dim_cliente
              ↓
         dim_producto
```

#### Pros:

- ✅ Queries MUY rápidos
- ✅ Fácil de entender
- ✅ Menos JOINs

#### Contras:

- ❌ Redundancia de datos
- ❌ Inflexible (una sola perspectiva)
- ❌ No escala para sistemas complejos

---

### **2. SNOWFLAKE SCHEMA (Copo de Nieve)** ❄️

#### Características:

- 1 tabla de hechos
- Dimensiones **normalizadas** (subdimensiones)
- Relaciones jerárquicas

#### Ejemplo:

```
    fact_ventas
         ↓
    dim_producto
         ↓
    dim_categoria ← Subdimensión
         ↓
    dim_departamento ← Subdimensión
```

#### Pros:

- ✅ Menos redundancia
- ✅ Integridad referencial estricta
- ✅ Menor espacio de almacenamiento

#### Contras:

- ❌ Queries más lentos (más JOINs)
- ❌ Más complejo de mantener
- ❌ Difícil de entender para usuarios

---

### **3. GALAXY/CONSTELLATION SCHEMA (Constelación)** 🌌 ← **TU ARQUITECTURA**

#### Características:

- **Múltiples tablas de hechos** (constelación)
- **Dimensiones compartidas** (conformadas)
- **Diferentes granularidades** por hecho
- Híbrido: dimensiones desnormalizadas + flexibilidad

#### Tu Implementación:

```
                    DIMENSIONES CONFORMADAS
                    (Compartidas entre hechos)

        ┌─────────────────────────────────────────┐
        │  dim_tiempo | dim_proyecto | dim_banco │
        └─────────────────────────────────────────┘
                           ↓
        ┌──────────────────┼──────────────────┐
        ↓                  ↓                  ↓
   fact_contratos   fact_flujo_caja   fact_avance_proyectos
   (Granularidad:   (Granularidad:    (Granularidad:
    por contrato)    por período)      por snapshot)
        ↓                  ↓                  ↓
   analytics_         analytics_        analytics_
   contratos_weekly   flujo_caja_banco  avance_proyectos
```

#### Pros:

- ✅ **Múltiples perspectivas** de análisis
- ✅ **Reutilización** de dimensiones
- ✅ **Escalable** para sistemas complejos
- ✅ **Flexible** para agregar nuevos hechos
- ✅ **Optimizado** para diferentes granularidades

#### Contras:

- ⚠️ Más complejo de diseñar inicialmente
- ⚠️ Requiere gobernanza de dimensiones
- ⚠️ Potencialmente más lento si no se optimiza

---

## 🎯 Por Qué Tu Arquitectura es Constellation

### **Múltiples Hechos con Granularidades Diferentes:**

1. **fact_contratos** (Granularidad: Contrato)
   - Un registro por contrato
   - Métricas: valor_contrato, valor_pagado
2. **fact_flujo_caja** (Granularidad: Período × Proyecto × Banco)
   - Un registro por desembolso/período
   - Métricas: desembolso, compromiso, pago
3. **fact_avance_proyectos** (Granularidad: Snapshot × Proyecto)
   - Un registro por snapshot temporal
   - Métricas: avance_fisico, avance_financiero

### **Dimensiones Conformadas (Compartidas):**

```python
# dim_proyecto es compartida por TODOS los hechos
fact_contratos.proyecto_key → dim_proyecto.proyecto_key
fact_flujo_caja.proyecto_key → dim_proyecto.proyecto_key
fact_avance_proyectos.proyecto_key → dim_proyecto.proyecto_key

# dim_tiempo es compartida pero con roles diferentes
fact_contratos.tiempo_firma_key → dim_tiempo.tiempo_key (fecha firma)
fact_contratos.tiempo_inicio_key → dim_tiempo.tiempo_key (fecha inicio)
fact_flujo_caja.tiempo_key → dim_tiempo.tiempo_key (período)
fact_avance_proyectos.tiempo_snapshot_key → dim_tiempo.tiempo_key (snapshot)
```

---

## 📐 Características Técnicas de Tu Arquitectura

### **1. Dimensiones Conformadas (Conformed Dimensions)**

Las dimensiones tienen el **mismo significado** en todos los hechos:

```javascript
// dim_proyecto es LA MISMA para todos
{
  proyecto_key: 12345,
  bp: "BP26005260",
  bpin: 2024760010045,
  nombre_proyecto: "Implementar soluciones tecnológicas...",
  // ... campos comunes
}

// Usada en:
fact_contratos → proyecto_key: 12345
fact_flujo_caja → proyecto_key: 12345
fact_avance_proyectos → proyecto_key: 12345
```

### **2. Role-Playing Dimensions**

Una dimensión juega **múltiples roles** en el mismo hecho:

```javascript
// dim_tiempo juega 3 roles en fact_contratos:
fact_contratos: {
  tiempo_firma_key: 20250101,      // Rol: Fecha de firma
  tiempo_inicio_key: 20250115,     // Rol: Fecha de inicio
  tiempo_fin_key: 20251231,        // Rol: Fecha de fin
  // ...
}
```

### **3. Degenerate Dimensions**

Campos que actúan como dimensiones pero están en la tabla de hechos:

```javascript
fact_contratos: {
  contrato_key: 1,
  // ... foreign keys a dimensiones ...

  // Degenerate dimensions (IDs de negocio):
  referencia_contrato: "4134.010.26.1.0544-2025",  ← No tiene tabla dim
  id_contrato: "CO1.PCCNTR.8355803",               ← No tiene tabla dim
  proceso_contractual: "CO1.BDOS.8607619"          ← No tiene tabla dim
}
```

---

## 🔄 Agregaciones Semanales (Ajuste Solicitado)

### **Cambio de Granularidad: Mensual → Semanal**

#### **ANTES (Mensual):**

```javascript
analytics_contratos_monthly: {
  id: "2025-01_BP26005260",
  anio: 2025,
  mes: 1,  // Granularidad mensual
  // ...
}
```

#### **DESPUÉS (Semanal):**

```javascript
analytics_contratos_weekly: {
  id: "2025-W45_BP26005260",
  anio: 2025,
  semana: 45,              // Semana del año (1-53)
  fecha_inicio_semana: "2025-11-03",
  fecha_fin_semana: "2025-11-09",
  // ...
}
```

### **Ventajas de Agregación Semanal:**

1. **Mayor granularidad temporal** → Detectar cambios más rápido
2. **Mejor para seguimiento operativo** → Menos rezago en métricas
3. **Alineado con ciclos de trabajo** → Semana laboral
4. **52 snapshots/año** vs 12 snapshots/año

---

## 🎨 Diagrama Completo de Tu Arquitectura

```
┌─────────────────────────────────────────────────────────────────────┐
│                    CAPA OPERACIONAL (Firebase)                      │
│  Colecciones Transaccionales - Escritura en Tiempo Real            │
├─────────────────────────────────────────────────────────────────────┤
│  • proyectos_presupuestales                                         │
│  • contratos_emprestito                                             │
│  • procesos_emprestito                                              │
│  • flujo_caja_emprestito                                            │
│  • unidades_proyecto                                                │
│  • reportes_contratos                                               │
└────────────────────────────┬────────────────────────────────────────┘
                             ↓
              ┌──────────────────────────┐
              │   Cloud Functions        │
              │   (Transformación)       │
              │   • Agregación semanal   │
              │   • Cálculo de KPIs      │
              │   • Snapshots            │
              └──────────┬───────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────────┐
│              CAPA ANALÍTICA (Firebase - analytics_*)                │
│  Constellation Schema - Optimizado para Lectura                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  DIMENSIONES CONFORMADAS (Compartidas):                             │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │ dim_tiempo | dim_proyecto | dim_banco | dim_geografia   │       │
│  └────────────────────────┬────────────────────────────────┘       │
│                            ↓                                        │
│  HECHOS (Múltiples Granularidades):                                │
│  ┌─────────────────┬──────────────────┬──────────────────┐        │
│  │ fact_contratos  │ fact_flujo_caja  │ fact_avance_    │        │
│  │ (por contrato)  │ (por período)    │ proyectos       │        │
│  │                 │                  │ (por snapshot)   │        │
│  └─────────────────┴──────────────────┴──────────────────┘        │
│                            ↓                                        │
│  AGREGACIONES PRE-CALCULADAS (Optimización):                       │
│  ┌──────────────────────────────────────────────────────┐         │
│  │ analytics_contratos_weekly    (Agregado semanal)    │         │
│  │ analytics_flujo_caja_banco    (Agregado por banco)  │         │
│  │ analytics_kpi_dashboard       (KPIs globales)       │         │
│  │ analytics_avance_proyectos    (Snapshots)           │         │
│  │ analytics_geoanalysis         (Agregado geográfico) │         │
│  └──────────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    CAPA DE PRESENTACIÓN                             │
│  Frontend React/Next.js - Consultas Optimizadas                    │
├─────────────────────────────────────────────────────────────────────┤
│  • EmprestitoAdvancedDashboard.tsx                                  │
│  • UnidadesProyecto.tsx                                             │
│  • Otros componentes                                                │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Comparación: Tu Arquitectura vs Otras

| Característica           | Star Schema      | Snowflake        | **Tu Constellation**   |
| ------------------------ | ---------------- | ---------------- | ---------------------- |
| Tablas de Hechos         | 1                | 1                | **Múltiples (3+)**     |
| Dimensiones              | Desnormalizadas  | Normalizadas     | **Desnormalizadas**    |
| Complejidad Queries      | Baja             | Alta             | **Media**              |
| Velocidad Queries        | Muy Rápida       | Lenta            | **Rápida**             |
| Flexibilidad             | Baja             | Media            | **Muy Alta**           |
| Redundancia              | Alta             | Baja             | **Media**              |
| Escalabilidad            | Baja             | Media            | **Muy Alta**           |
| Múltiples Granularidades | ❌               | ❌               | **✅**                 |
| Dimensiones Compartidas  | N/A              | N/A              | **✅**                 |
| Ideal Para               | Reportes simples | OLTP normalizado | **Análisis complejos** |

---

## 🎯 Ventajas Específicas de Tu Implementación

### **1. Optimización para Firebase:**

```javascript
// Colecciones planas (no subcolecciones)
// → Consultas más rápidas
// → Indexación eficiente

// MAL (Firebase lento):
proyectos / { projectId } / contratos / { contratoId };

// BIEN (Tu implementación):
analytics_contratos_weekly / { id };
```

### **2. Pre-Agregaciones Estratégicas:**

```javascript
// En lugar de calcular en cada consulta:
contratos
  .filter((c) => c.banco === "Bancolombia")
  .reduce((sum, c) => sum + c.valor, 0);

// Tienes pre-calculado:
analytics_flujo_caja_banco.where("banco", "==", "Bancolombia").get(); // → 1 documento con todo calculado
```

### **3. Granularidades Múltiples:**

```javascript
// Vista diaria para operaciones
analytics_avance_proyectos_daily;

// Vista semanal para análisis (tu elección)
analytics_contratos_weekly;

// Vista mensual para reportes ejecutivos
analytics_kpi_monthly;
```

---

## 📝 Conclusión Técnica

**Tu arquitectura es:**

✅ **CONSTELLATION SCHEMA** (Esquema de Constelación)
✅ **Híbrido optimizado** para Firebase NoSQL
✅ **Múltiples hechos** con granularidades específicas
✅ **Dimensiones conformadas** reutilizables
✅ **Agregaciones pre-calculadas** para performance

**Es técnicamente superior a Star/Snowflake para tu caso porque:**

1. Soporta múltiples perspectivas de análisis (contratos, flujos, avances)
2. Dimensiones reutilizables reducen duplicación
3. Diferentes granularidades por tipo de análisis
4. Optimizado para consultas específicas del frontend

**Siguiente paso:** Necesito acceder al código del frontend para adaptar las colecciones analíticas a las variables y gráficos que ya tienes construidos.
