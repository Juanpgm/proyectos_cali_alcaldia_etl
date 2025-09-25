# 🏛️ Proyectos Cali Alcaldía ETL

Sistema ETL para gestión de datos de proyectos presupuestales de la Alcaldía de Santiago de Cali.

## 🚀 Inicio Rápido

### 1. Configurar Firebase

```bash
# Seguir la guía rápida
python database/config.py
```

### 2. Cargar datos

```bash
# Cargar proyectos presupuestales
python load_app/data_loading_bp.py
```

### 3. Verificar

```bash
# Los datos están en Firebase Firestore
# Proyecto: dev-test-e778d
# Colección: proyectos_presupuestales
```

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
