# ETL Cali Alcaldía para Railway

## Configuración para Railway

Esta aplicación está configurada para deployarse automáticamente en Railway usando la variable de entorno `DATABASE_URL`.

### Variables de entorno requeridas en Railway:

```bash
DATABASE_URL=postgresql://user:password@host:port/database
PORT=8000  # Railway se encarga de esto automáticamente
```

### Proceso de deployment:

1. **Automático desde GitHub:**

   - Railway detecta cambios en el repositorio
   - Construye la imagen usando Nixpacks
   - Despliega usando el comando definido en `railway.json`

2. **Base de datos:**
   - Railway PostgreSQL se conecta automáticamente vía `DATABASE_URL`
   - Las tablas se crean automáticamente en el primer deploy

### Estructura de archivos para Railway:

- `railway.json` - Configuración de deployment
- `requirements.txt` - Dependencias Python
- `fastapi_project/main.py` - API principal
- `load_app/bulk_load_data.py` - Cargador de datos
- `database_management/core/` - Modelos y configuración

### Comandos de carga de datos:

```bash
# Cargar todos los datos (después del deploy)
python load_app/bulk_load_data.py --data-type all

# Cargar tipo específico
python load_app/bulk_load_data.py --data-type paa_dacp
```
