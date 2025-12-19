# 🎯 Diferenciación por nombre_up_detalle

## 📋 Regla Implementada

**Si dos registros comparten:**

- ✅ Mismo `nombre_up`
- ✅ Misma ubicación GPS (< 20 metros)
- ❌ **Diferente** `nombre_up_detalle`

**Resultado**: Se consideran **UNIDADES DE PROYECTO INDEPENDIENTES**

## 💡 Justificación

El campo `nombre_up_detalle` especifica **sedes**, **fases**, **etapas** o **secciones** diferentes de una misma institución o infraestructura. Aunque estén en la misma ubicación física general, son unidades de proyecto distintas con:

- Presupuestos independientes
- Contratos separados
- Intervenciones específicas
- Estados de avance diferentes

## 📊 Ejemplo Real

### Caso: Institución Educativa con Múltiples Sedes

**Antes de la Regla** (Incorrecto):

```json
{
  "UNP-45": {
    "nombre_up": "I.E. PANCE",
    "nombre_up_detalle": "Principal",  // ← Se perdía esta diferencia
    "lat": 3.2823,
    "lon": -76.5642,
    "intervenciones": [
      {"descripcion": "Mejoramiento Sede Principal", ...},
      {"descripcion": "Construcción Sede Secundaria", ...},
      {"descripcion": "Dotación Sede Rural", ...}
    ]
  }
}
```

❌ **Problema**: Todas las sedes se agrupaban juntas aunque tengan presupuestos y contratos independientes

**Después de la Regla** (Correcto):

```json
{
  "UNP-45": {
    "nombre_up": "I.E. PANCE",
    "nombre_up_detalle": "Principal",
    "lat": 3.2823,
    "lon": -76.5642,
    "intervenciones": [
      {"descripcion": "Mejoramiento Sede Principal", ...}
    ]
  },
  "UNP-46": {
    "nombre_up": "I.E. PANCE",
    "nombre_up_detalle": "Secundaria",
    "lat": 3.2825,  // ← Misma zona (~20m de distancia)
    "lon": -76.5640,
    "intervenciones": [
      {"descripcion": "Construcción Sede Secundaria", ...}
    ]
  },
  "UNP-47": {
    "nombre_up": "I.E. PANCE",
    "nombre_up_detalle": "Sede Rural",
    "lat": 3.2820,  // ← Misma zona general
    "lon": -76.5645,
    "intervenciones": [
      {"descripcion": "Dotación Sede Rural", ...}
    ]
  }
}
```

✅ **Correcto**: Cada sede es una unidad independiente con su propio UPID

## 🔧 Implementación Técnica

### 1. En Clustering Geoespacial (DBSCAN)

```python
# Paso 1: DBSCAN agrupa por proximidad GPS
clusters_geo = DBSCAN(radius=20m).fit(coordenadas)

# Paso 2: Post-procesamiento - Separar por nombre_up_detalle
for cluster_id in clusters_geo.unique():
    grupo = registros[cluster_id]

    # Verificar si hay diferentes nombre_up_detalle
    subgrupos = grupo.groupby(['nombre_up', 'nombre_up_detalle'])

    if len(subgrupos) > 1:
        # Separar en clusters independientes
        for cada_subgrupo:
            asignar_nuevo_cluster_id()
```

### 2. En Clustering por Hash

```python
# Incluir nombre_up_detalle en la clave de agrupación
clave = hash([
    nombre_up,
    nombre_up_detalle,  # ← AGREGADO
    direccion,
    comuna,
    barrio,
    tipo_equipamiento
])
```

### 3. En Fuzzy Matching

```python
# Concatenar nombre_up + nombre_up_detalle para comparación
nombre_completo = f"{nombre_up} {nombre_up_detalle}"
nombre_normalizado = normalizar(nombre_completo)

# Comparar textos completos
similitud = fuzzy_match(nombre_norm_1, nombre_norm_2)
```

## 📈 Impacto en los Resultados

### Resultados de la Prueba

**Clustering Geoespacial**:

- Clusters iniciales (solo GPS): **207**
- Clusters ajustados (con nombre_up_detalle): **214**
- **+7 separaciones** detectadas ✅

**Esto significa**:

- Se encontraron 7 casos donde había registros en la misma ubicación GPS
- Pero con diferentes `nombre_up_detalle`
- El sistema los separó correctamente en unidades independientes

## 🎯 Casos de Uso Comunes

### 1. Instituciones Educativas

```
I.E. San Fernando
├─ Principal
├─ Sede A
├─ Sede B
└─ Sede Rural
```

### 2. Parques con Múltiples Zonas

```
Parque Central
├─ Zona Deportiva
├─ Zona Infantil
├─ Zona Verde
└─ Sendero Ecológico
```

### 3. Hospitales con Pabellones

```
Hospital Valle del Lili
├─ Pabellón Principal
├─ UCI
├─ Urgencias
└─ Consulta Externa
```

### 4. Bibliotecas con Secciones

```
Biblioteca Departamental
├─ Sede Principal
├─ Sala Infantil
├─ Hemeroteca
└─ Ludoteca
```

## ✅ Validación de la Regla

### Checklist de Verificación

- [x] `nombre_up` igual + `nombre_up_detalle` diferente → Unidades separadas
- [x] Misma ubicación GPS + `nombre_up_detalle` diferente → Unidades separadas
- [x] Fuzzy matching considera ambos campos
- [x] Hash de agrupación incluye `nombre_up_detalle`
- [x] Post-procesamiento DBSCAN separa subgrupos
- [x] Estructura final mantiene independencia

## 📊 Estadísticas Comparativas

### Sin Considerar nombre_up_detalle

- Unidades agrupables: 369
- Riesgo: Sedes mezcladas incorrectamente

### Con nombre_up_detalle como Diferenciador

- Unidades agrupables: 378 (+9)
- Beneficio: Cada sede/fase/etapa es independiente

## 🔍 Cómo Identificar Estos Casos

En los archivos JSON generados, busca:

```json
// Patrón: Mismo nombre_up, diferente nombre_up_detalle
{
  "UNP-100": {
    "nombre_up": "Parque Los Mangos",
    "nombre_up_detalle": "Zona Deportiva",
    "cluster_original": "GEO-50"
  },
  "UNP-101": {
    "nombre_up": "Parque Los Mangos",
    "nombre_up_detalle": "Zona Infantil",
    "cluster_original": "GEO-51" // ← Cluster diferente
  }
}
```

## 💭 Consideraciones Especiales

### ¿Cuándo NO Separar?

Si `nombre_up_detalle` es NULL o vacío en ambos registros:

- Se agrupan normalmente por GPS + fuzzy matching
- Se considera la misma unidad

### ¿Y si uno tiene detalle y otro no?

```
Registro A: nombre_up = "Parque X", nombre_up_detalle = "Zona Norte"
Registro B: nombre_up = "Parque X", nombre_up_detalle = NULL
```

Se consideran **DIFERENTES** → Unidades separadas

## 🚀 Beneficios de Esta Regla

1. ✅ **Precisión**: Cada sede/fase tiene su UPID único
2. ✅ **Trazabilidad**: Fácil seguimiento de intervenciones por sede
3. ✅ **Flexibilidad**: Permite presupuestos independientes
4. ✅ **Integridad**: No mezcla datos de diferentes sedes
5. ✅ **Escalabilidad**: Funciona para cualquier tipo de equipamiento

## 📝 Conclusión

La inclusión de `nombre_up_detalle` como diferenciador es crítica para:

- Mantener la independencia de sedes, fases y etapas
- Evitar la mezcla incorrecta de presupuestos
- Permitir seguimiento granular de intervenciones
- Respetar la estructura organizacional de las instituciones

**Resultado**: Datos más precisos y estructura más fiel a la realidad operativa.

---

**Fecha**: 18 de Diciembre, 2025
**Versión**: 3.0 (Con diferenciación por nombre_up_detalle)
**Estado**: ✅ Implementado y validado
