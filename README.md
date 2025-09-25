# �️ ETL Pipeline - Proyectos Cali Alcaldía

Pipeline automatizado de **Extracción, Transformación y Carga (ETL)** para datos de unidades de proyecto de la Alcaldía de Cali. Implementa programación funcional, carga incremental y ejecución automatizada con GitHub Actions.

## 🎯 Características Principales

- **🔄 Automatización Completa**: Ejecución automática 2 veces al día (8:00 AM y 4:00 PM COT)
- **⚡ Carga Incremental**: Solo procesa datos nuevos o modificados
- **🔐 Seguridad**: Credenciales encriptadas con GitHub Secrets
- **📊 Monitoreo**: Health checks automáticos y reportes detallados
- **🎮 Control Manual**: Ejecutar pipeline manualmente cuando sea necesario
- **🏗️ Programación Funcional**: Código limpio, eficiente y reutilizable

## 🚀 Inicio Rápido

### 1. Configuración Inicial

```bash
# Clonar repositorio
git clone https://github.com/Juanpgm/proyectos_cali_alcaldia_etl.git
cd proyectos_cali_alcaldia_etl

# Instalar dependencias
pip install -r requirements.txt

# Configurar repositorio
python fix_repository.py
```

### 2. Configurar Secrets

Sigue la guía detallada en [`.github/SECRETS_SETUP.md`](.github/SECRETS_SETUP.md) para configurar:

- Service Account de Google Cloud
- Acceso a Firebase Firestore
- Permisos de Google Sheets

### 3. Ejecutar Pipeline Local

````bash
# Ejecutar pipeline completo
cd pipelines
python unidades_proyecto_pipeline.py

```bash
# Los datos están en Firebase Firestore
# Proyecto: dev-test-e778d
# Colección: proyectos_presupuestales
````

## 📚 Documentación

### [📖 Documentación Completa](./docs/)

- [🔐 Configuración Firebase con Workload Identity](./docs/firebase-workload-identity-setup.md)
- [⚡ Setup Rápido](./docs/quick-setup.md)

## 🏗️ Estructura del Proyecto

```
├── database/               # Configuración de Firebase
│   └── config.py          # Setup con Workload Identity Federation
├── load_app/              # Carga de datos
│   └── data_loading_bp.py # Carga de proyectos presupuestales
├── transformation_app/    # Transformación de datos
├── extraction_app/       # Extracción de datos
├── docs/                 # Documentación
└── requirements.txt      # Dependencias
```

## 🔧 Tecnologías

- **Base de datos:** Firebase Firestore
- **Autenticación:** Workload Identity Federation
- **Lenguaje:** Python 3.12+
- **Cloud:** Google Cloud Platform

## 📊 Estado del Proyecto

- ✅ Configuración Firebase con Workload Identity Federation
- ✅ Carga de proyectos presupuestales (1,254 registros)
- ✅ Verificación automática de datos
- 🔄 Extracción y transformación de datos (en desarrollo)

## 🛠️ Configuración Local

1. **Clonar repositorio**
2. **Instalar dependencias:** `pip install -r requirements.txt`
3. **Configurar Firebase:** Ver [documentación](./docs/firebase-workload-identity-setup.md)
4. **Probar configuración:** `python database/config.py`

## 🔐 Seguridad

Este proyecto usa **Workload Identity Federation** en lugar de archivos de claves de cuenta de servicio, siguiendo las mejores prácticas de seguridad de Google Cloud.

## 📈 Datos Disponibles

- **Proyectos Presupuestales:** 1,254 registros
- **Campos:** BPIN, nombre, centro gestor, programa, etc.
- **Actualización:** En tiempo real via ETL

## 🆘 Soporte

- **Configuración:** Ver `/docs/`
- **Issues:** Crear issue en GitHub
- **Contacto:** Equipo de desarrollo
