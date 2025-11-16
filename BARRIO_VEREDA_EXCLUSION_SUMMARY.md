# Intersección Espacial con Exclusión para Barrio/Vereda

## Resumen de Funcionalidad

Se ha implementado la intersección espacial para crear la columna `barrio_vereda_val` con las siguientes características:

### 1. Intersección Espacial

- **Archivo GeoJSON**: `basemaps/barrios_veredas.geojson` (433 features)
- **Campo origen**: `barrio_vereda` (del GeoJSON)
- **Campo destino**: `barrio_vereda_val` (nuevo en el DataFrame)
- **Algoritmo**: Point-in-polygon usando ray casting

### 2. Exclusión de Infraestructura Vial

Los registros con `tipo_equipamiento` igual a "Infraestructura vial" son **excluidos** de la intersección:

- La comparación es **case-insensitive** (no distingue mayúsculas/minúsculas)
- Estos registros mantienen el valor `"REVISAR"` en `barrio_vereda_val`
- No se realiza procesamiento geoespacial para estos registros

### 3. Comportamiento de Valores

- **Con intersección exitosa**: Se asigna el nombre del barrio/vereda del GeoJSON
- **Sin geometría**: Se asigna `"REVISAR"`
- **Sin intersección encontrada**: Se asigna `"REVISAR"`
- **Infraestructura vial**: Se asigna `"REVISAR"` (excluido)

## Orden de Procesamiento en el Pipeline

1. Generación de UPID
2. Columnas computadas
3. Limpieza de tipos de datos
4. Procesamiento geoespacial
5. **Intersección comuna/corregimiento** (sin exclusiones)
6. **Intersección barrio/vereda** (con exclusión de Infraestructura vial)
7. Imputación de unidad/cantidad

## Código de Implementación

### Función Principal

```python
def perform_spatial_intersection(
    df: pd.DataFrame,
    geojson_path: str,
    target_field: str,
    output_column: str,
    exclude_condition: Optional[callable] = None
) -> pd.DataFrame
```

### Uso en Pipeline

```python
lambda df: perform_spatial_intersection(
    df,
    'basemaps/barrios_veredas.geojson',
    'barrio_vereda',
    'barrio_vereda_val',
    exclude_condition=lambda row: str(row.get('tipo_equipamiento', '')).strip().lower() == 'infraestructura vial'
)
```

## Tests Implementados

### 1. test_barrio_vereda_exclusion.py

- Verifica que la exclusión funciona correctamente
- Prueba case-insensitivity
- Valida que otros tipos se procesan normalmente
- **Resultado**: ✓ Todos los tests pasaron

### 2. test_full_pipeline_exclusion.py

- Test de integración completo
- Verifica el pipeline end-to-end
- Valida ambas columnas: comuna_corregimiento_val y barrio_vereda_val
- **Resultado**: ✓ Test de integración pasado

## Ejemplos de Resultados

### Registros de Infraestructura Vial (Excluidos)

```
Vía Principal Norte (Infraestructura vial)
  → comuna_corregimiento_val: COMUNA 04
  → barrio_vereda_val: REVISAR (excluido)

Corredor Vial Sur (INFRAESTRUCTURA VIAL)
  → comuna_corregimiento_val: COMUNA 03
  → barrio_vereda_val: REVISAR (excluido)
```

### Otros Tipos de Equipamiento (Procesados)

```
Parque El Recreo (Espacio público)
  → comuna_corregimiento_val: COMUNA 17
  → barrio_vereda_val: Lili

Centro Deportivo (Equipamiento deportivo)
  → comuna_corregimiento_val: COMUNA 04
  → barrio_vereda_val: El Troncal

Espacio Público Central (Espacio público)
  → comuna_corregimiento_val: COMUNA 08
  → barrio_vereda_val: Santander
```

## Notas Importantes

1. **Comuna vs Barrio**: La intersección de comunas NO tiene exclusiones, solo la de barrios/veredas
2. **Normalización**: Los valores de barrio/vereda NO se normalizan (solo las comunas 1-9 se normalizan)
3. **Valor por defecto**: Siempre es `"REVISAR"` para consistencia con otras columnas
4. **Performance**: Se procesa registro por registro con early exit al encontrar intersección

## Estadísticas del Test de Integración

```
Total registros: 5
- Infraestructura vial: 2 (excluidos)
- Otros tipos: 3 (procesados)

Resultados barrio_vereda_val:
- REVISAR: 2 (infraestructura vial excluida)
- Lili: 1 (intersección exitosa)
- El Troncal: 1 (intersección exitosa)
- Santander: 1 (intersección exitosa)

Intersecciones exitosas: 3/3 registros procesados (100%)
Registros excluidos por condición: 2
```
