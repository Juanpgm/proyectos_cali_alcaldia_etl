# 🎭 Orquestador ETL - Cali Alcaldía

Un sistema completo de orquestación para coordinar y ejecutar procesos ETL (Extract, Transform, Load) del proyecto de la Alcaldía de Cali.

## 🌟 Características Principales

- **🔧 Orquestación Programática**: Define y ejecuta tareas desde código Python
- **📁 Configuración JSON**: Gestiona workflows complejos con archivos de configuración
- **💻 CLI Completo**: Interfaz de línea de comandos para todas las operaciones
- **🌐 API REST**: Servidor FastAPI para integración web y monitoreo
- **📊 Reportes Detallados**: Seguimiento completo de ejecuciones y resultados
- **⚡ Ejecución Paralela**: Soporte para paralelización respetando dependencias
- **🔄 Workflows Predefinidos**: Conjuntos de tareas organizados por categorías
- **📝 Logging Avanzado**: Registro detallado de todas las actividades
- **🧪 Modo Testing**: Funciones mock para desarrollo y testing

## 📁 Estructura de Archivos

```
orchestrator/
├── etl_orchestrator.py      # 🎯 Módulo principal del orquestador
├── cli.py                   # 💻 Interfaz de línea de comandos
├── api.py                   # 🌐 Servidor API REST
├── examples.py              # 📚 Ejemplos de uso
├── etl_config.json          # ⚙️ Configuración principal
├── etl_config_testing.json  # 🧪 Configuración para testing
└── README.md               # 📖 Esta documentación

Directorios generados:
├── orchestrator_logs/       # 📝 Logs de ejecución
└── orchestrator_reports/    # 📊 Reportes detallados
```

## 🚀 Instalación y Configuración

### Requisitos

- Python 3.8+
- FastAPI y Uvicorn (para API)
- Pandas (para procesamiento de datos)
- Los módulos ETL existentes del proyecto

### Configuración inicial

```bash
# 1. Navegar al directorio del proyecto
cd a:\programing_workspace\proyectos_cali_alcaldia_etl

# 2. Activar entorno virtual
.\env\Scripts\activate

# 3. Instalar dependencias adicionales si es necesario
pip install fastapi uvicorn

# 4. Verificar estructura del orquestador
ls orchestrator/
```

## 💻 Uso del CLI

### Comandos Básicos

#### Listar tareas disponibles

```bash
python orchestrator/cli.py list-tasks
python orchestrator/cli.py list-tasks --category extraction
```

#### Listar workflows

```bash
python orchestrator/cli.py list-workflows
```

#### Ejecutar workflows

```bash
# ETL completo
python orchestrator/cli.py run --workflow full_etl

# Solo extracción
python orchestrator/cli.py run --workflow extraction_only

# Solo transformación
python orchestrator/cli.py run --workflow transformation_only

# Solo procesos críticos
python orchestrator/cli.py run --workflow critical_only
```

#### Ejecutar tareas específicas

```bash
# Una tarea
python orchestrator/cli.py run --tasks extract_contratos_emprestito

# Múltiples tareas
python orchestrator/cli.py run --tasks extract_contratos_emprestito transform_contratos_secop load_data_complete
```

#### Ejecución en paralelo

```bash
# Paralelo con 4 workers
python orchestrator/cli.py run --workflow full_etl --parallel --max-workers 4

# Continuar aunque fallen tareas no críticas
python orchestrator/cli.py run --workflow full_etl --continue-on-failure
```

### Monitoreo y Reportes

#### Ver estado de ejecución

```bash
# Última ejecución
python orchestrator/cli.py status --latest

# Ejecución específica
python orchestrator/cli.py status --execution-id 20240918_143022

# Listar todas las ejecuciones
python orchestrator/cli.py status
```

#### Validar configuración

```bash
python orchestrator/cli.py validate
python orchestrator/cli.py validate --config orchestrator/etl_config_testing.json
```

## 🌐 API REST

### Iniciar el servidor API

```bash
# Opción 1: Directamente
python orchestrator/api.py

# Opción 2: Con uvicorn
cd orchestrator
uvicorn api:app --host 0.0.0.0 --port 8001 --reload
```

### Endpoints principales

#### Información general

- `GET /` - Información básica del servicio
- `GET /health` - Health check
- `GET /tasks` - Listar tareas disponibles
- `GET /workflows` - Listar workflows

#### Ejecución

- `POST /execute` - Ejecutar tareas o workflows
- `GET /executions` - Listar ejecuciones
- `GET /executions/{execution_id}` - Estado de ejecución específica
- `DELETE /executions/{execution_id}` - Cancelar/limpiar ejecución

#### Reportes

- `GET /reports` - Listar reportes disponibles
- `GET /reports/{execution_id}` - Obtener reporte específico

### Ejemplos de uso de la API

#### Ejecutar workflow completo

```bash
curl -X POST "http://localhost:8001/execute" \
     -H "Content-Type: application/json" \
     -d '{"workflow": "full_etl", "parallel": true}'
```

#### Ejecutar tareas específicas

```bash
curl -X POST "http://localhost:8001/execute" \
     -H "Content-Type: application/json" \
     -d '{"tasks": ["extract_contratos_emprestito", "transform_contratos_secop"]}'
```

#### Ver estado de ejecución

```bash
curl "http://localhost:8001/executions/exec_20240918_143022_a1b2c3d4"
```

## 🧪 Modo Testing

### Configuración de testing

El orquestador incluye un modo de testing con funciones mock que simulan el comportamiento de los módulos ETL reales sin ejecutar procesamiento pesado.

#### Usar configuración de testing

```bash
# CLI con testing
python orchestrator/cli.py run --workflow test_basic --config orchestrator/etl_config_testing.json

# Workflows de testing disponibles
python orchestrator/cli.py list-workflows --config orchestrator/etl_config_testing.json
```

#### Workflows de testing

- `test_basic`: ETL básico con funciones simples
- `test_mock_etl`: Simulación completa usando mocks de módulos reales
- `test_simple`: Solo funciones básicas
- `test_with_long_task`: Incluye proceso de larga duración
- `test_dependencies`: Testing de manejo de dependencias

## 🔧 Uso Programático

### Ejemplo básico

```python
import asyncio
from orchestrator.etl_orchestrator import ETLOrchestrator, Task

# Crear orquestador
orchestrator = ETLOrchestrator()

# Registrar tarea
orchestrator.register_task(Task(
    task_id="mi_tarea",
    name="Mi Tarea ETL",
    description="Descripción de la tarea",
    module_path="mi_modulo",
    function_name="mi_funcion",
    category="extraction"
))

# Ejecutar
async def main():
    await orchestrator.execute_all()

asyncio.run(main())
```

### Ejemplo con configuración

```python
from orchestrator.etl_orchestrator import create_orchestrator

# Crear con configuración
orchestrator = create_orchestrator("orchestrator/etl_config.json")

# Ejecutar workflow específico
async def main():
    await orchestrator.execute_all(parallel=True, max_workers=4)

asyncio.run(main())
```

## ⚙️ Configuración JSON

### Estructura del archivo de configuración

```json
{
  "orchestrator_config": {
    "name": "Nombre del sistema",
    "version": "1.0.0",
    "description": "Descripción",
    "default_timeout": 3600,
    "max_parallel_workers": 3
  },
  "tasks": [
    {
      "task_id": "id_unico",
      "name": "Nombre de la tarea",
      "description": "Descripción detallada",
      "module_path": "ruta.al.modulo",
      "function_name": "nombre_funcion",
      "dependencies": ["tarea_prerequisito"],
      "parameters": { "param1": "valor1" },
      "timeout": 1800,
      "retry_count": 2,
      "critical": true,
      "category": "extraction|transformation|load|validation|utility"
    }
  ],
  "workflows": {
    "nombre_workflow": {
      "name": "Nombre del workflow",
      "description": "Descripción del workflow",
      "tasks": ["tarea1", "tarea2", "tarea3"]
    }
  }
}
```

### Campos de configuración

#### Tarea (Task)

- `task_id`: Identificador único
- `name`: Nombre descriptivo
- `description`: Descripción detallada
- `module_path`: Ruta del módulo Python (ej: `extraction_app.data_extraction_contratos_emprestito`)
- `function_name`: Nombre de la función a ejecutar (ej: `main`)
- `dependencies`: Lista de task_ids que deben completarse antes
- `parameters`: Parámetros a pasar a la función
- `timeout`: Tiempo límite en segundos
- `retry_count`: Número de reintentos en caso de fallo
- `critical`: Si es true, el fallo detiene la ejecución
- `category`: Categoría para organización

## 📊 Reportes y Logging

### Logs

Los logs se guardan en:

- **Consola**: Logs en tiempo real durante ejecución
- **Archivo**: `orchestrator_logs/orchestrator_YYYYMMDD_HHMMSS.log`

### Reportes

Los reportes detallados se guardan en:

- **Directorio**: `orchestrator_reports/`
- **Formato**: `execution_report_YYYYMMDD_HHMMSS.json`

### Contenido del reporte

```json
{
  "execution_id": "20240918_143022",
  "start_time": "2024-09-18T14:30:22",
  "end_time": "2024-09-18T14:35:45",
  "total_duration": 323.45,
  "execution_order": ["tarea1", "tarea2", "tarea3"],
  "results": {
    "tarea1": {
      "task_id": "tarea1",
      "status": "completed",
      "start_time": "2024-09-18T14:30:22",
      "end_time": "2024-09-18T14:32:15",
      "duration": 113.2,
      "output": "Resultado de la ejecución",
      "error": null
    }
  }
}
```

## 🔄 Workflows Predefinidos

### ETL Completo (`full_etl`)

Ejecuta todo el pipeline ETL desde extracción hasta validación:

1. Extracción de datos (contratos, procesos, ejecución presupuestal)
2. Transformación de todos los datasets
3. Carga completa a base de datos
4. Validación de calidad de datos
5. Generación de reportes

### Solo Extracción (`extraction_only`)

Ejecuta únicamente los procesos de extracción de datos desde fuentes externas.

### Solo Transformación (`transformation_only`)

Ejecuta únicamente los procesos de transformación y limpieza de datos.

### Solo Críticos (`critical_only`)

Ejecuta únicamente las tareas marcadas como críticas para el funcionamiento del sistema.

## 🚨 Manejo de Errores

### Tipos de errores

- **Errores de importación**: Módulo o función no encontrada
- **Errores de ejecución**: Excepción durante la ejecución de la tarea
- **Timeouts**: Tarea excede el tiempo límite
- **Dependencias**: Falla en tareas prerequisito

### Estrategias de recuperación

- **Reintentos**: Configurables por tarea
- **Tareas críticas vs no críticas**: Control de flujo de ejecución
- **Logs detallados**: Para diagnóstico y debugging
- **Reportes de error**: Información completa en reportes JSON

## 🎯 Casos de Uso

### 1. Desarrollo y Testing

```bash
# Testing rápido con mocks
python orchestrator/cli.py run --workflow test_basic --config orchestrator/etl_config_testing.json
```

### 2. Ejecución Manual

```bash
# ETL completo en producción
python orchestrator/cli.py run --workflow full_etl --parallel
```

### 3. Procesamiento Parcial

```bash
# Solo actualizar contratos
python orchestrator/cli.py run --tasks extract_contratos_emprestito transform_contratos_secop
```

### 4. Monitoreo Automatizado

```bash
# API para integración con sistemas de monitoreo
curl "http://localhost:8001/health"
curl "http://localhost:8001/executions"
```

### 5. Integración con Scheduler

```python
# Cron job o scheduler
import schedule
import asyncio
from orchestrator.etl_orchestrator import create_orchestrator

async def daily_etl():
    orchestrator = create_orchestrator()
    await orchestrator.execute_all()

schedule.every().day.at("02:00").do(lambda: asyncio.run(daily_etl()))
```

## 🔧 Personalización

### Agregar nuevas tareas

1. Editar `orchestrator/etl_config.json`
2. Agregar nueva entrada en `tasks`
3. Configurar dependencias si es necesario
4. Validar configuración: `python orchestrator/cli.py validate`

### Crear workflows personalizados

1. Agregar entrada en `workflows` en el archivo de configuración
2. Especificar las tareas a incluir
3. Ejecutar: `python orchestrator/cli.py run --workflow mi_workflow`

### Funciones personalizadas

Las funciones ETL deben seguir esta estructura:

```python
def mi_funcion_etl(**kwargs):
    """
    Función ETL personalizada

    Args:
        **kwargs: Parámetros de configuración

    Returns:
        dict: Resultado de la ejecución
    """
    # Lógica de procesamiento
    return {"status": "success", "message": "Completado"}
```

## 🤝 Integración con el Proyecto

### Con el servidor de desarrollo

```bash
# Terminal 1: Servidor principal
.\start_dev_server.bat

# Terminal 2: API del orquestador
python orchestrator/api.py
```

### Con módulos existentes

El orquestador puede ejecutar directamente:

- `extraction_app/*`: Módulos de extracción
- `transformation_app/*`: Módulos de transformación
- `load_app/*`: Módulos de carga
- Cualquier función Python personalizada

## 📞 Soporte y Troubleshooting

### Problemas comunes

#### Error de importación de módulo

```bash
# Verificar configuración
python orchestrator/cli.py validate

# Verificar que el módulo existe
python -c "import extraction_app.data_extraction_contratos_emprestito"
```

#### Timeout en tareas

- Aumentar el valor `timeout` en la configuración
- Verificar que la función no esté en bucle infinito
- Usar modo testing para debugging

#### Dependencias circulares

```bash
# Validar configuración detecta dependencias circulares
python orchestrator/cli.py validate
```

### Logs para debugging

```bash
# Logs detallados
python orchestrator/cli.py run --workflow test_basic --log-level DEBUG

# Ver último log
ls orchestrator_logs/ | tail -1
```

## 🚀 Próximos Pasos

1. **Integrar con sistema de notificaciones**: Envío de alertas por email/Slack
2. **Dashboard web**: Interfaz gráfica para monitoreo
3. **Scheduler integrado**: Ejecución automática programada
4. **Métricas avanzadas**: Estadísticas de rendimiento y uso de recursos
5. **Integración con CI/CD**: Automatización en pipelines de deployment

---

## 📝 Changelog

### v1.0.0 (2024-09-18)

- 🎉 Versión inicial
- ✅ Orquestador programático completo
- ✅ CLI con todos los comandos
- ✅ API REST con FastAPI
- ✅ Configuración JSON flexible
- ✅ Modo testing con mocks
- ✅ Reportes y logging detallados
- ✅ Soporte para ejecución paralela
- ✅ Manejo avanzado de dependencias

---

**¡El Orquestador ETL está listo para coordinar todos tus procesos de datos! 🎭✨**
