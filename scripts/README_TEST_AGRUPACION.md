# 🧪 Prueba de Agrupación: Unidades de Proyecto e Intervenciones

## 📋 Resumen

Este script prueba un **nuevo modelo de datos** que organiza la información en dos niveles jerárquicos:

1. **Unidades de Proyecto (UP)**: Nivel superior que representa la ubicación física e infraestructura
2. **Intervenciones**: Nivel de detalle que representa contratos, proyectos y trabajos específicos en esa ubicación

## 🎯 Objetivo

Reducir la redundancia en los datos agrupando múltiples intervenciones (contratos/proyectos) que ocurren en la misma ubicación física bajo una única "Unidad de Proyecto".

## 📊 Modelo de Datos Propuesto

### Unidad de Proyecto (UP)

Representa **dónde** está la infraestructura:

```json
{
  "upid": "UNP-155",
  "nombre_up": "Parque Central",
  "nombre_up_detalle": "Parque recreativo zona centro",
  "comuna_corregimiento": "Comuna 3",
  "barrio_vereda": "San Fernando",
  "direccion": "Calle 12 # 45-67",
  "tipo_equipamiento": "Parques",
  "created_at": "2024-12-18T10:00:00",
  "updated_at": "2024-12-18T10:00:00",
  "intervenciones": [...]
}
```

### Intervención

Representa **qué** se está haciendo en esa ubicación:

```json
{
  "intervencion_id": "UNP-155-01",
  "intervencion_num": 1,
  "referencia_proceso": "SECOP-12345",
  "referencia_contrato": "CT-2024-001",
  "bpin": "2023000123456",
  "identificador": "PRY-001",
  "fuente_financiacion": "Recursos propios",
  "tipo_intervencion": "Mejoramiento",
  "unidad": "M2",
  "cantidad": 500,
  "estado": "En ejecución",
  "presupuesto_base": 500000000,
  "avance_obra": "65%",
  "ano": 2024,
  "fecha_inicio": "2024-01-15",
  "fecha_fin": "2024-12-31",
  "geometry": {...},
  "plataforma": "SECOP II",
  "url_proceso": "https://...",
  "clase_up": "Infraestructura deportiva",
  "nombre_centro_gestor": "Secretaría de Deporte"
}
```

## 🔑 Criterios de Agrupación

Los registros se agrupan en la misma Unidad de Proyecto cuando comparten:

1. **nombre_up** (mismo nombre de lugar)
2. **direccion** (misma dirección)
3. **comuna_corregimiento** (misma comuna)
4. **barrio_vereda** (mismo barrio)
5. **tipo_equipamiento** (mismo tipo de infraestructura)

## 🏷️ Sistema de IDs

### UPIDs (Unidades de Proyecto)

- Formato: `UNP-###`
- Ejemplos: `UNP-1`, `UNP-155`, `UNP-2024`
- Se asignan secuencialmente
- Únicos por unidad de proyecto

### IDs de Intervención

- Formato: `UNP-###-##`
- Ejemplos: `UNP-155-01`, `UNP-155-02`, `UNP-155-03`
- El primer número es el UPID de la unidad padre
- El segundo número es secuencial dentro de cada unidad
- Formato con cero padding (01, 02, ..., 99)

## 📈 Ventajas del Modelo

✅ **Reduce redundancia**: Datos de ubicación se guardan una sola vez  
✅ **Histórico claro**: Todas las intervenciones en un lugar están juntas  
✅ **Consultas eficientes**: Firebase puede consultar unidades completas o intervenciones específicas  
✅ **Escalable**: Fácil agregar nuevas intervenciones a unidades existentes  
✅ **Estructura lógica**: Refleja la realidad (múltiples proyectos en un mismo lugar)

## 🚀 Cómo Ejecutar la Prueba

### Requisitos

- Python 3.8+
- Entorno virtual configurado
- Credenciales de Google Sheets configuradas

### Ejecución

```bash
# Desde la raíz del proyecto
python scripts/test_agrupacion_unidades_intervenciones.py
```

### Salidas Generadas

El script genera 3 archivos JSON en `app_outputs/test_agrupacion/`:

1. **`unidades_agrupadas_YYYYMMDD_HHMMSS.json`**

   - Estructura completa con todas las unidades e intervenciones
   - Formato listo para Firebase

2. **`estadisticas_agrupacion_YYYYMMDD_HHMMSS.json`**

   - Métricas sobre la agrupación
   - Distribuciones y promedios
   - Unidad con más intervenciones

3. **`ejemplo_estructura_YYYYMMDD_HHMMSS.json`**
   - Primeras 5 unidades como ejemplo
   - Más fácil de revisar manualmente

## 📊 Estadísticas que Genera

- **Total de unidades de proyecto**: Cuántas ubicaciones únicas hay
- **Total de intervenciones**: Cuántos contratos/proyectos hay
- **Factor de compresión**: Promedio de intervenciones por unidad
- **Intervenciones por unidad**: Máximo, mínimo, promedio
- **Distribución por tipo de equipamiento**: Top 10 tipos
- **Distribución por comuna**: Top 10 comunas
- **Unidad con más intervenciones**: La ubicación más activa

## 🔍 Ejemplo de Salida en Consola

```
============================================================
📊 ESTADÍSTICAS DE AGRUPACIÓN
============================================================

🔢 Totales:
   • Unidades de Proyecto: 250
   • Intervenciones: 1500
   • Factor de Compresión: 6.0x
     (promedio de intervenciones por unidad)

📈 Intervenciones por Unidad:
   • Máximo: 25
   • Mínimo: 1
   • Promedio: 6.0

🏆 Unidad con más intervenciones:
   • UPID: UNP-42
   • Nombre: Parque Principal
   • Total intervenciones: 25

============================================================
📋 EJEMPLOS DE UNIDADES CON INTERVENCIONES
============================================================

────────────────────────────────────────────────────────────
📍 Ejemplo 1: UNP-42
────────────────────────────────────────────────────────────

🏢 UNIDAD DE PROYECTO:
   • Nombre: Parque Principal
   • Dirección: Calle 5 # 12-34
   • Comuna/Corregimiento: Comuna 2
   • Tipo Equipamiento: Parques

🔧 INTERVENCIONES (25):

   ├─ UNP-42-01:
   │  • Contrato: CT-2020-001
   │  • Tipo: Construcción
   │  • Estado: Terminado
   │  • Año: 2020

   ├─ UNP-42-02:
   │  • Contrato: CT-2021-045
   │  • Tipo: Mejoramiento
   │  • Estado: Terminado
   │  • Año: 2021

   ... (más intervenciones)
```

## ⚠️ Nota Importante

**Este script NO afecta la ETL actual**. Solo:

- Lee los datos de Google Sheets
- Prueba la lógica de agrupación
- Genera archivos JSON de ejemplo
- Muestra estadísticas

No modifica Firebase ni ningún archivo del pipeline actual.

## 🔄 Próximos Pasos

Si el modelo funciona bien:

1. ✅ Revisar los archivos JSON generados
2. ✅ Validar que la agrupación sea correcta
3. ✅ Confirmar que los IDs son únicos
4. 🔲 Integrar la lógica en el pipeline de transformación
5. 🔲 Actualizar el módulo de carga a Firebase
6. 🔲 Migrar datos existentes (si es necesario)
7. 🔲 Actualizar queries del frontend

## 📝 Archivos Relacionados

- **Script de prueba**: `scripts/test_agrupacion_unidades_intervenciones.py`
- **Pipeline actual**: `pipelines/unidades_proyecto_pipeline.py`
- **Transformación**: `transformation_app/data_transformation_unidades_proyecto.py`
- **Carga a Firebase**: `load_app/data_loading_unidades_proyecto.py`

## 🤔 Preguntas Frecuentes

### ¿Por qué agrupar los datos?

Actualmente, cada fila del Google Sheets es un registro independiente en Firebase. Si hay 5 contratos diferentes en el mismo parque, tenemos 5 registros con la misma ubicación repetida 5 veces. El nuevo modelo elimina esta redundancia.

### ¿Cómo afecta esto a las consultas?

Firebase puede:

- Listar todas las unidades de proyecto
- Buscar una unidad específica por UPID
- Consultar intervenciones dentro de una unidad
- Filtrar por campos de la unidad o de las intervenciones

### ¿Se pierden datos?

No. Todos los datos actuales se preservan, solo se reorganizan en una estructura más lógica.

### ¿Qué pasa con las geometrías?

Las geometrías quedan en el nivel de **intervención** porque cada contrato/proyecto puede tener su propia área de cobertura, incluso si están en la misma ubicación general.

## 📞 Soporte

Si tienes dudas o encuentras problemas:

1. Revisa la salida en consola
2. Verifica los archivos JSON generados
3. Consulta el código fuente con comentarios detallados
