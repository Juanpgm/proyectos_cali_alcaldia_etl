# Intelligent ETL System for Santiago de Cali Projects

Sistema inteligente de ETL (Extract, Transform, Load) que permite la gestión automática de bases de datos, generación de esquemas y carga de datos sin dependencias de APIs.

## Características Principales

✅ **Programación Funcional**: Arquitectura basada en principios funcionales
✅ **Autodiagnóstico**: Sistema inteligente que detecta y repara problemas automáticamente
✅ **Generación Automática de Esquemas**: Crea tablas basadas en archivos JSON/GeoJSON
✅ **Soporte PostGIS**: Manejo completo de datos geoespaciales
✅ **Sin Dependencias de API**: Carga directa desde archivos locales
✅ **Escalable y Eficiente**: Procesamiento en lotes con gestión inteligente de memoria

## Instalación Rápida

1. **Clonar y navegar al directorio**:

```bash
cd database_management
```

2. **Instalar dependencias**:

```bash
pip install -r requirements.txt
```

3. **Configurar variables de entorno**:

```bash
cp .env.example .env
# Editar .env con tu configuración de base de datos
```

4. **Inicializar el sistema**:

```bash
python main.py --init
```

## Uso

### Comandos Principales

```bash
# Diagnosticar sistema
python main.py --diagnose

# Reparar problemas automáticamente
python main.py --repair

# Cargar datos desde transformation_app/app_outputs
python main.py --load

# Ejecutar proceso ETL completo
python main.py --run

# Ver estado del sistema
python main.py --status
```

### Interfaz de Línea de Comandos (CLI)

```bash
# Usar CLI interactivo
python etl_cli.py diagnose
python etl_cli.py load --data-dir custom/path
python etl_cli.py run
```

## Estructura de Datos Soportada

El sistema procesa automáticamente:

- **JSON**: Archivos de datos tabulares
- **GeoJSON**: Datos geoespaciales con geometrías
- **Subdirectorios**: Organización por tipo de datos

### Ejemplos de archivos soportados:

```
transformation_app/app_outputs/
├── contratos_secop_outputs/
│   ├── contratos_proyectos.json
│   └── contratos_proyectos_index.json
├── ejecucion_presupuestal_outputs/
│   ├── ejecucion_presupuestal.json
│   └── movimientos_presupuestales.json
├── unidades_proyecto_outputs/
│   ├── equipamientos.geojson
│   └── infraestructura_vial.geojson
└── emprestito_outputs/
    ├── emp_contratos.json
    └── emp_procesos.json
```

## Funcionalidades Avanzadas

### Autodiagnóstico y Reparación

```python
from core import create_etl_system

etl = create_etl_system()

# Diagnóstico completo
diagnosis = etl.diagnose_system()

# Reparación automática
repair_result = etl.repair_system()
```

### Carga Programática

```python
from pathlib import Path
from core import load_data_functional, create_database_manager

# Crear gestor de BD
db_manager = create_database_manager(config)

# Cargar datos desde directorio
result = load_data_functional(
    db_manager,
    Path("transformation_app/app_outputs")
)
```

### Generación de Esquemas

```python
from core import generate_schema_from_data

# Generar esquema desde datos JSON
schema = generate_schema_from_data(json_data, "mi_tabla")

# Crear tabla en BD
db_manager.create_table_from_schema(schema)
```

## Configuración

### Variables de Entorno (.env)

```env
# Base de datos PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_proyectos_cali
DB_USER=postgres
DB_PASSWORD=mi_password

# O usar DATABASE_URL completa
DATABASE_URL=postgresql://user:pass@host:port/db

# PostGIS (datos geoespaciales)
ENABLE_POSTGIS=true

# Configuración de rendimiento
DB_POOL_SIZE=5
BATCH_SIZE=1000
```

## Arquitectura del Sistema

```
database_management/
├── core/                          # Módulos principales
│   ├── config.py                  # Configuración inmutable
│   ├── database_manager.py        # Gestión de BD con autodiagnóstico
│   ├── schema_generator.py        # Generación automática de esquemas
│   ├── data_loader.py            # Carga eficiente de datos
│   ├── model_generator.py        # Generación de modelos SQLAlchemy
│   └── etl_system.py             # Orquestador principal
├── main.py                       # Script principal
├── etl_cli.py                    # Interfaz de línea de comandos
├── requirements.txt              # Dependencias Python
└── .env.example                  # Configuración de ejemplo
```

## Ejemplos de Uso

### Caso 1: Carga Inicial Completa

```bash
# 1. Verificar sistema
python main.py --diagnose

# 2. Reparar si es necesario
python main.py --repair

# 3. Ejecutar ETL completo
python main.py --run
```

### Caso 2: Carga de Datos Específicos

```bash
# Cargar solo datos de contratos
python etl_cli.py load --data-dir transformation_app/app_outputs/contratos_secop_outputs
```

### Caso 3: Monitoreo del Sistema

```bash
# Estado actual
python main.py --status

# Diagnóstico detallado
python etl_cli.py diagnose --verbose
```

## Manejo de Errores

El sistema incluye:

- **Reintentos automáticos** para operaciones de BD
- **Validación de datos** antes de la carga
- **Logs detallados** para debugging
- **Recuperación inteligente** ante fallos

## Optimizaciones de Rendimiento

- **Carga en lotes**: Procesamiento eficiente de grandes volúmenes
- **Pool de conexiones**: Gestión óptima de conexiones BD
- **Índices automáticos**: Creación inteligente de índices
- **Transformaciones funcionales**: Sin efectos secundarios

## Soporte para Datos Geoespaciales

- **PostGIS automático**: Instalación y configuración automática
- **Geometrías múltiples**: Point, Polygon, LineString, etc.
- **Proyecciones**: Soporte para SRID 4326 (WGS84)
- **Índices espaciales**: Optimización automática con GIST

## Logging y Monitoreo

```bash
# Ver logs del sistema
tail -f etl_main.log

# Logs de la aplicación
tail -f etl_system.log
```

## Troubleshooting

### Problema: No se puede conectar a PostgreSQL

```bash
# Verificar conexión
python main.py --diagnose

# Revisar configuración
python etl_cli.py export-config
```

### Problema: PostGIS no disponible

```bash
# Reparar automáticamente
python main.py --repair
```

### Problema: Archivos de datos no encontrados

```bash
# Verificar directorio
python main.py --status

# Especificar directorio custom
python main.py --load --data-dir /ruta/custom
```

## Contribución

El sistema está diseñado para ser extensible:

1. **Nuevos transformadores**: Agregar en `data_loader.py`
2. **Tipos de datos**: Extender `schema_generator.py`
3. **Comandos CLI**: Añadir en `etl_cli.py`

## Licencia

Sistema desarrollado para la Alcaldía de Santiago de Cali.
