# 📥 Script de Descarga Unificada de Datos de Empréstito

## 🎯 Descripción

El script `download_contratos_emprestito.py` ha sido actualizado para descargar y unificar datos de las tres colecciones de empréstito en Firebase:

1. **contratos_emprestito** - Contratos principales
2. **ordenes_compra_emprestito** - Órdenes de compra
3. **convenios_transferencias_emprestito** - Convenios y transferencias

El script genera una tabla unificada en Excel, similar al endpoint `GET /contratos_emprestito_all` del API.

## ✨ Características

- ✅ Descarga de las tres colecciones automáticamente
- ✅ **Normalización automática al esquema estándar de contratos**
- ✅ Identificación del tipo de registro con campo `tipo_registro`
- ✅ Mapeo inteligente de campos entre colecciones
- ✅ Todas las filas adaptadas al mismo esquema unificado
- ✅ Exportación a Excel con formato y filtros automáticos
- ✅ Conversión automática de timestamps de Firebase
- ✅ Columnas clave priorizadas al inicio
- ✅ Columnas con ancho ajustado automáticamente
- ✅ Timestamp en el nombre del archivo (opcional)
- ✅ Soporte para filtros personalizados
- ✅ Límite configurable de registros

## 📖 Uso

### Uso básico (descargar todo):

```bash
python download_contratos_emprestito.py
```

Esto generará un archivo llamado `emprestito_unificado_YYYYMMDD_HHMMSS.xlsx` con todos los datos.

### Con archivo de salida personalizado:

```bash
python download_contratos_emprestito.py --output emprestito_2024.xlsx
```

### Limitar registros por colección:

```bash
python download_contratos_emprestito.py --limit 50
```

### Sin timestamp en el nombre:

```bash
python download_contratos_emprestito.py --no-timestamp
```

### Con filtros personalizados:

```bash
python download_contratos_emprestito.py --filter nombre_banco:"Banco Mundial"
```

### Combinar opciones:

```bash
python download_contratos_emprestito.py --output datos_emprestito.xlsx --limit 100 --no-timestamp
```

## 📊 Estructura del Archivo Excel

El archivo Excel generado contiene:

- **Hoja**: "Emprestito Unificado"
- **Esquema unificado**: Todos los datos normalizados al esquema estándar de contratos
- **Columnas prioritarias** (al inicio):
  - `tipo_registro` - Tipo de documento (contrato, orden_compra, convenio_transferencia)
  - `referencia_contrato` - Referencia única del contrato/orden/convenio
  - `banco` - Entidad bancaria
  - `nombre_centro_gestor` - Centro gestor responsable
  - `valor_contrato` - Valor monetario
  - `estado_contrato` - Estado actual
  - `fecha_firma_contrato` - Fecha de firma
  - `objeto_contrato` - Descripción del objeto
  - `modalidad_contratacion` - Modalidad
  - `contratista` - Nombre del contratista/proveedor
  - Y más campos clave...
- **Columnas adicionales**: Todos los campos específicos de cada colección
- **Filtros automáticos**: Habilitados en todas las columnas
- **Ancho de columnas**: Ajustado automáticamente (máximo 50 caracteres)

## 🔍 Normalización de Esquemas

### Campo tipo_registro

Cada registro incluye un campo `tipo_registro` que identifica su origen:

- `contrato` - De la colección contratos_emprestito
- `orden_compra` - De la colección ordenes_compra_emprestito
- `convenio_transferencia` - De la colección convenios_transferencias_emprestito

### Mapeo Automático de Campos

El script aplica automáticamente las siguientes conversiones para unificar los esquemas:

#### Órdenes de Compra → Esquema Estándar:

- `numero_orden` / `referencia_orden` → `referencia_contrato`
- `valor_orden` → `valor_contrato`
- `estado_orden` → `estado_contrato`
- `fecha_publicacion_orden` → `fecha_firma_contrato`
- `fecha_vencimiento_orden` → `fecha_fin_contrato`
- `entidad_compradora` → `entidad_contratante`
- `nombre_proveedor` → `contratista`
- `nit_proveedor` → `nit_contratista`
- `descripcion_orden` / `objeto_orden` → `objeto_contrato`
- `tipo_orden` → `modalidad_contratacion`
- `nombre_banco` → `banco`

#### Convenios/Transferencias → Esquema Estándar:

- `valor_convenio` → `valor_contrato`
- `fecha_inicio_contrato` → `fecha_firma_contrato`
- `nombre_banco` → `banco`

#### Contratos:

- `nombre_banco` → `banco` (normalización del nombre)

### Ventajas de la Normalización

✅ **Análisis unificado**: Todos los datos en el mismo formato
✅ **Filtrado consistente**: Mismos nombres de columnas para todas las filas
✅ **Reportes simplificados**: Una sola estructura para trabajar
✅ **Exportación compatible**: Datos listos para análisis y visualización

## 📝 Opciones de Línea de Comandos

| Opción           | Descripción                        | Default                     |
| ---------------- | ---------------------------------- | --------------------------- |
| `--output`       | Nombre del archivo de salida       | `emprestito_unificado.xlsx` |
| `--limit`        | Límite de documentos por colección | Sin límite                  |
| `--no-timestamp` | No agregar timestamp al nombre     | Agrega timestamp            |
| `--filter`       | Filtros en formato `campo:valor`   | Sin filtros                 |

## 🔄 Comparación con el Endpoint API

Este script replica la funcionalidad del endpoint:

```
GET /contratos_emprestito_all
```

Ambos:

- Unifican las tres colecciones de empréstito
- Incluyen el campo `tipo_registro`
- Retornan todos los campos disponibles
- Proporcionan conteo por tipo de registro

## 💡 Ejemplos Avanzados

### Ver ayuda completa:

```bash
python download_contratos_emprestito.py --help
```

### Múltiples filtros:

```bash
python download_contratos_emprestito.py \
  --filter nombre_banco:"Banco Mundial" \
  --filter nombre_centro_gestor:"Secretaría de Salud"
```

### Exportación rápida de muestra:

```bash
python download_contratos_emprestito.py --limit 10 --output muestra.xlsx --no-timestamp
```

## 📋 Requisitos

- Python 3.7+
- pandas
- openpyxl (para exportación a Excel)
- Firebase configurado (credenciales en `database/config.py`)

## ⚠️ Notas Importantes

1. El script requiere conexión a Firebase y credenciales válidas
2. La descarga de grandes volúmenes puede tomar tiempo
3. Los timestamps de Firebase se convierten automáticamente a formato compatible con Excel
4. El archivo Excel no incluye índices (solo datos puros)

## 🐛 Solución de Problemas

### Error de conexión a Firebase

Verifica que las credenciales estén configuradas correctamente en `database/config.py`

### Archivo muy grande

Usa la opción `--limit` para descargar menos registros:

```bash
python download_contratos_emprestito.py --limit 100
```

### Columnas con errores de formato

El script convierte automáticamente los timestamps de Firebase. Si hay problemas, revisa la salida de consola.

## 📅 Fecha de Actualización

Última modificación: 21 de noviembre de 2025
