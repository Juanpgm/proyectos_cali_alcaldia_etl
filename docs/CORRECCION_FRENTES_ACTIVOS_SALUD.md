# Corrección: Detección de Frentes Activos para Secretaría de Salud Pública

## 🔍 Problema Identificado

Los registros de **Secretaría de Salud Pública** no se detectaban como frentes activos porque:

- Los datos de origen (Google Sheets/Excel) tienen campos `clase_up` y `tipo_equipamiento` vacíos
- La lógica de frentes activos requiere `clase_up` en categorías válidas
- Sin estos valores, todos se clasificaban como **"No aplica"**

## ✅ Solución Implementada

### Nueva Función: `infer_missing_categorical_values`

Ubicación: [`transformation_app/data_transformation_unidades_proyecto.py`](../transformation_app/data_transformation_unidades_proyecto.py)

**Se ejecuta en Fase 5.4** (antes de calcular frente_activo)

### Reglas de Inferencia

#### Regla 1: Detección por nombre "IPS"

```python
Si nombre_up contiene "IPS" (case-insensitive):
  → tipo_equipamiento = "IPS"
  → clase_up = "Obras equipamientos" (si también está vacío)
```

#### Regla 2: Secretaría de Salud Pública

```python
Si nombre_centro_gestor = "Secretaría de Salud Pública":
  → clase_up = "Obras equipamientos" (si está vacío)
  → tipo_equipamiento = "IPS" (si está vacío y no se aplicó Regla 1)
```

## 📊 Lógica de Frentes Activos

Una vez inferidos los valores, se aplica la lógica estándar:

### Condiciones Base (deben cumplirse TODAS):

1. ✅ `clase_up` en ['Obras equipamientos', 'Obra vial', 'Espacio Público']
2. ✅ `tipo_equipamiento` NO en ['Vivienda mejoramiento', 'Vivienda nueva', 'Adquisición de predios', 'Señalización vial']
3. ✅ `tipo_intervencion` NO en ['Mantenimiento', 'Estudios y diseños', 'Transferencia directa']

### Clasificación Final:

- **"Frente activo"**: Condiciones base + `estado = 'En ejecución'`
- **"Inactivo"**: Condiciones base + `estado = 'Suspendido'`
- **"No aplica"**: Cualquier otro caso

## ✅ Por qué "IPS" es válido

- "IPS" está en la lista oficial de categorías estándar: [`unidades_proyecto_std_categories.json`](../app_inputs/unidades_proyecto_input/defaults/unidades_proyecto_std_categories.json)
- No está en la lista de exclusión de `add_frente_activo`
- Representa Instituciones Prestadoras de Servicios de Salud (infraestructura física real)

## 🧪 Ejemplo de Aplicación

### Antes de la inferencia:

```json
{
  "nombre_up": "IPS - Union de Vivienda Popular",
  "nombre_centro_gestor": "Secretaría de Salud Pública",
  "clase_up": null,
  "tipo_equipamiento": null,
  "estado": "En ejecución",
  "tipo_intervencion": "Obra nueva"
}
→ Resultado: frente_activo = "No aplica" ❌
```

### Después de la inferencia:

```json
{
  "nombre_up": "IPS - Union de Vivienda Popular",
  "nombre_centro_gestor": "Secretaría de Salud Pública",
  "clase_up": "Obras equipamientos",  ← Inferido
  "tipo_equipamiento": "IPS",          ← Inferido
  "estado": "En ejecución",
  "tipo_intervencion": "Obra nueva"
}
→ Resultado: frente_activo = "Frente activo" ✅
```

## 🔄 Para Aplicar los Cambios

Ejecuta el pipeline completo para regenerar los datos:

```bash
python pipelines/unidades_proyecto_pipeline.py --transform --load
```

O ejecuta el test de validación:

```bash
python test_inferencia_directa.py
```

## 📝 Archivos Modificados

1. **transformation_app/data_transformation_unidades_proyecto.py**

   - Agregada función `infer_missing_categorical_values()` (línea ~1621)
   - Integrada en Phase 5.4 del pipeline (línea ~2399)

2. **Scripts de prueba creados:**
   - `test_inferencia_directa.py` - Prueba la función de inferencia
   - `test_salud_publica_frente_activo.py` - Valida resultados finales
   - `diagnostico_salud_tipo_equipamiento.py` - Diagnóstico de datos

## ✅ Resultado Esperado

Con los cambios aplicados, todos los registros de Secretaría de Salud Pública que:

- Tienen `estado = 'En ejecución'`
- Y `tipo_intervencion` no excluido (no "Mantenimiento", "Estudios y diseños", etc.)

Ahora se detectarán correctamente como **"Frente activo"** en lugar de "No aplica".
