# 📊 Transformation & Extraction Apps - Alcaldía de Cali

Sistema de transformación y extracción de datos para el dashboard de la Alcaldía de Santiago de Cali. **Proyecto limpio sin FastAPI**, enfocado únicamente en el procesamiento y transformación de datos gubernamentales.

## 🎯 Descripción

Este proyecto contiene las herramientas necesarias para procesar y transformar datos de:

- Contratos SECOP I y II
- Ejecución presupuestal
- Plan Anual de Adquisiciones (PAA)
- Seguimiento de proyectos
- Unidades de proyecto e infraestructura
- Centros de gravedad geoespaciales

## 🏗️ Estructura del Proyecto

```
api_dashboard_cali/
├── extraction_app/          # 🔍 Aplicaciones de extracción
│   ├── EP_DAPM/             # Extracción datos EP DAPM
│   └── SECOP/               # Extracción datos SECOP
├── transformation_app/      # 🔄 Aplicaciones de transformación
│   ├── app_inputs/          # 📁 Archivos de entrada
│   ├── app_outputs/         # 📤 Archivos procesados
│   ├── data_transformation_contratos_secop.py
│   ├── data_transformation_procesos_secop.py
│   ├── data_transformation_ejecucion_presupuestal.py
│   ├── data_transformation_emprestito.py
│   ├── data_transformation_paa.py
│   ├── data_transformation_seguimiento_pa.py
│   ├── data_transformation_unidades_proyecto.py
│   ├── data_trasnformation_centros_gravedad.py
│   └── emprestito.ipynb
├── requirements.txt         # 📋 Dependencias (sin FastAPI)
├── test_modules.py         # 🧪 Script de pruebas
└── README.md               # 📖 Este archivo
```

## 🚀 Instalación Rápida

1. **Clonar repositorio:**

   ```bash
   git clone <repository-url>
   cd api_dashboard_cali
   ```

2. **Crear entorno virtual:**

   ```bash
   python -m venv env
   env\Scripts\activate  # Windows
   # source env/bin/activate  # Linux/Mac
   ```

3. **Instalar dependencias:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Verificar instalación:**
   ```bash
   python test_modules.py
   ```

## ✅ Verificación del Sistema

El script `test_modules.py` verifica que todos los módulos funcionen correctamente:

```bash
python test_modules.py
```

**Salida esperada:**

```
🧪 INICIANDO PRUEBAS DE MÓDULOS DE TRANSFORMATION_APP
============================================================

=== PRUEBAS DE IMPORTACIÓN ===
✅ data_transformation_procesos_secop: Importación exitosa
✅ data_transformation_contratos_secop: Importación exitosa
✅ data_transformation_ejecucion_presupuestal: Importación exitosa
✅ data_transformation_emprestito: Importación exitosa
✅ data_transformation_paa: Importación exitosa
✅ data_transformation_seguimiento_pa: Importación exitosa
✅ data_transformation_unidades_proyecto: Importación exitosa
✅ data_trasnformation_centros_gravedad: Importación exitosa

🎉 ¡TODOS LOS MÓDULOS FUNCIONAN CORRECTAMENTE!
```

## 📊 Módulos de Transformación

### 🔄 Contratos y Procesos SECOP

- `data_transformation_contratos_secop.py` - Procesa contratos SECOP I y II
- `data_transformation_procesos_secop.py` - Procesa procesos de contratación

### 💰 Gestión Presupuestal

- `data_transformation_ejecucion_presupuestal.py` - Ejecución presupuestal
- `data_transformation_emprestito.py` - Datos de empréstitos

### 📋 Planificación y Seguimiento

- `data_transformation_paa.py` - Plan Anual de Adquisiciones
- `data_transformation_seguimiento_pa.py` - Seguimiento de Plan de Acción

### 🏗️ Infraestructura y Territorio

- `data_transformation_unidades_proyecto.py` - Unidades de proyecto
- `data_trasnformation_centros_gravedad.py` - Análisis geoespacial

## 💻 Uso de los Módulos

### Preparación de Datos

1. **Colocar archivos** de entrada en `transformation_app/app_inputs/[nombre_modulo]_input/`
2. **Verificar formato** de archivos (Excel, CSV según módulo)

### Ejecución

```bash
cd transformation_app

# Ejemplo: Procesar contratos SECOP
python data_transformation_contratos_secop.py

# Ejemplo: Procesar ejecución presupuestal
python data_transformation_ejecucion_presupuestal.py
```

### Resultados

- Los archivos procesados se guardan en `transformation_app/app_outputs/[nombre_modulo]_outputs/`
- Formatos de salida: JSON, CSV, Excel según módulo

## 🔧 Dependencias Principales

```
pandas==2.2.2           # Procesamiento de datos
numpy==1.26.4           # Operaciones numéricas
openpyxl==3.1.5         # Archivos Excel
geopandas==1.1.1        # Datos geoespaciales
shapely==2.1.1          # Geometría
tqdm==4.66.5            # Barras de progreso
selenium==4.21.0        # Automatización web
requests==2.32.3        # HTTP requests
beautifulsoup4==4.12.2  # Web scraping
python-dotenv==0.21.0   # Variables de entorno
```

## 📁 Estructura de Datos

### Archivos de Entrada (`app_inputs/`)

```
app_inputs/
├── contratos_input/          # Archivos de contratos SECOP
├── procesos_input/           # Archivos de procesos SECOP
├── ejecucion_presupuestal_input/  # Datos presupuestales
├── paa_input/               # Plan Anual de Adquisiciones
├── seguimiento_pa_input/    # Seguimiento proyectos
└── unidades_proyecto_input/ # Infraestructura y equipamientos
```

### Archivos de Salida (`app_outputs/`)

```
app_outputs/
├── contratos_outputs/       # Contratos procesados
├── procesos_outputs/        # Procesos procesados
├── ejecucion_outputs/       # Ejecución presupuestal
├── paa_outputs/            # PAA procesado
├── seguimiento_outputs/    # Seguimiento procesado
└── unidades_outputs/       # Unidades procesadas
```

## 🛠️ Desarrollo

### Añadir Nuevo Módulo

1. Crear archivo `data_transformation_nuevo_modulo.py` en `transformation_app/`
2. Seguir estructura de módulos existentes
3. Añadir dependencias a `requirements.txt`
4. Actualizar `test_modules.py`

### Estructura de Módulo Típica

```python
import pandas as pd
import os
from datetime import datetime

def clean_data(df):
    """Función de limpieza"""
    pass

def transform_data(df):
    """Función de transformación"""
    pass

def save_outputs(df, output_dir):
    """Función de guardado"""
    pass

def main():
    """Función principal"""
    pass

if __name__ == "__main__":
    main()
```

## 📝 Notas Importantes

- ✅ **Sin FastAPI**: Proyecto limpio enfocado solo en procesamiento
- ✅ **Autocontenido**: Cada módulo es independiente
- ✅ **Geoespacial**: Soporte completo para datos geográficos
- ✅ **Escalable**: Fácil añadir nuevos módulos
- ✅ **Probado**: Todos los módulos verificados

## 🤝 Contribuir

1. Fork del proyecto
2. Crear rama feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -m 'Añadir nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request

## 📄 Licencia

[Especificar licencia del proyecto]

---

**Proyecto limpio y optimizado para procesamiento de datos gubernamentales** 🏛️✨
