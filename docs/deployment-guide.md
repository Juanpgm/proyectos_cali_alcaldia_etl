# Guía de Despliegue - Analytics Data Warehouse

## 📋 Prerequisitos

Antes de iniciar el despliegue, asegúrate de tener:

- [x] Python 3.9+ instalado
- [x] Cuenta Firebase con proyecto `dev-test-e778d` configurado
- [x] Credenciales ADC configuradas (Application Default Credentials)
- [x] Permisos de escritura en Firestore
- [x] Firebase CLI instalado: `npm install -g firebase-tools`

## 🚀 Despliegue Paso a Paso

### 1. Configurar Índices de Firestore

Los índices compuestos son **CRÍTICOS** para el rendimiento de las consultas analíticas.

```powershell
# Autenticarse con Firebase
firebase login

# Seleccionar proyecto
firebase use dev-test-e778d

# Desplegar índices (toma 5-15 minutos en crear)
firebase deploy --only firestore:indexes

# Verificar estado de índices
firebase firestore:indexes
```

**⚠️ IMPORTANTE**: Espera a que todos los índices estén en estado `READY` antes de continuar. Puedes verificar el estado en:
- Firebase Console → Firestore Database → Indexes
- O ejecutando: `firebase firestore:indexes`

### 2. Activar Entorno Virtual

```powershell
# Navegar al directorio del proyecto
cd a:\programing_workspace\proyectos_cali_alcaldia_etl

# Activar entorno virtual
.\env\Scripts\Activate.ps1

# Verificar instalación de dependencias
pip list | Select-String "firebase|google"
```

### 3. Verificar Conexión a Firebase

```powershell
# Ejecutar script de verificación
python -c "from database.config import setup_firebase; print('✅ Conexión OK' if setup_firebase() else '❌ Error')"
```

Si hay error, verifica:
- Las credenciales ADC: `gcloud auth application-default login`
- El proyecto activo: `gcloud config get-value project`

### 4. Ejecutar Carga Inicial de Analytics

```powershell
# Ejecutar script de carga inicial (puede tomar 10-30 minutos)
python load_initial_analytics.py
```

Este script creará y poblará:
- ✅ `analytics_contratos_monthly` - Agregaciones mensuales de contratos
- ✅ `analytics_kpi_dashboard` - KPIs globales del día
- ✅ `analytics_avance_proyectos` - Snapshots históricos de avance
- ✅ `analytics_geoanalysis` - Análisis geográfico por comuna/corregimiento

**Resultado esperado:**
```
✅ 4/4 funciones completadas exitosamente

✅ contratos_monthly:
    - Documentos nuevos: 450
    - Documentos actualizados: 0

✅ kpi_dashboard:
    - Contratos activos: 145
    - Inversión total: $850,000,000

✅ avance_proyectos:
    - Snapshots nuevos: 1251

✅ geoanalysis:
    - Comunas procesadas: 22
```

### 5. Verificar Colecciones Creadas

```powershell
# Listar colecciones
python -c "from database.config import get_firestore_client; db = get_firestore_client(); print([col.id for col in db.collections() if 'analytics' in col.id])"
```

Debe mostrar:
```
['analytics_contratos_monthly', 'analytics_kpi_dashboard', 'analytics_avance_proyectos', 'analytics_geoanalysis']
```

### 6. Configurar Actualizaciones Automáticas

#### Opción A: Cloud Scheduler + Cloud Functions (Recomendado)

```powershell
# Desplegar Cloud Functions
firebase deploy --only functions

# Crear job en Cloud Scheduler
gcloud scheduler jobs create http analytics-daily-update \
    --schedule="0 2 * * *" \
    --uri="https://us-central1-dev-test-e778d.cloudfunctions.net/run_analytics_updates" \
    --http-method=POST \
    --time-zone="America/Bogota" \
    --description="Actualización diaria de analytics a las 2 AM"
```

#### Opción B: Cron Job Local (Desarrollo)

```powershell
# Crear script de actualización
# Ubicación: a:\programing_workspace\proyectos_cali_alcaldia_etl\update_analytics.ps1

$scriptContent = @"
cd a:\programing_workspace\proyectos_cali_alcaldia_etl
.\env\Scripts\Activate.ps1
python -c "from cloud_functions.analytics_functions import run_all_analytics_updates; run_all_analytics_updates()"
"@

$scriptContent | Out-File -FilePath .\update_analytics.ps1 -Encoding UTF8

# Configurar tarea programada (ejecutar como administrador)
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-File a:\programing_workspace\proyectos_cali_alcaldia_etl\update_analytics.ps1"
$trigger = New-ScheduledTaskTrigger -Daily -At 2:00AM
Register-ScheduledTask -TaskName "Analytics Daily Update" -Action $action -Trigger $trigger -Description "Actualización diaria de analytics warehouse"
```

## 🧪 Testing

### Prueba de Carga Incremental

Ejecuta este test para verificar que la carga incremental funciona correctamente:

```powershell
python -c @"
from cloud_functions.analytics_functions import update_analytics_contratos_monthly

# Primera ejecución
print('--- Primera ejecución ---')
resultado1 = update_analytics_contratos_monthly()
print(f'Docs nuevos: {resultado1["docs_nuevos"]}')

# Segunda ejecución (debe saltar documentos existentes)
print('\n--- Segunda ejecución (debe ser 0/0) ---')
resultado2 = update_analytics_contratos_monthly()
print(f'Docs nuevos: {resultado2["docs_nuevos"]}')
print(f'Docs actualizados: {resultado2["docs_actualizados"]}')
"@
```

**Resultado esperado:**
```
--- Primera ejecución ---
Docs nuevos: 450

--- Segunda ejecución (debe ser 0/0) ---
Docs nuevos: 0
Docs actualizados: 0
```

### Consultas de Verificación

```python
from database.config import get_firestore_client

db = get_firestore_client()

# Test 1: Verificar datos en analytics_contratos_monthly
docs = db.collection('analytics_contratos_monthly').limit(5).stream()
print("Top 5 contratos mensuales:")
for doc in docs:
    data = doc.to_dict()
    print(f"  BP: {data['bp']}, Mes: {data['mes']}/{data['anio']}, Inversión: ${data['total_inversion']:,.0f}")

# Test 2: Verificar KPI dashboard
kpi_doc = db.collection('analytics_kpi_dashboard').order_by('fecha', direction='DESCENDING').limit(1).get()
if kpi_doc:
    kpis = kpi_doc[0].to_dict()
    print(f"\nKPIs actuales:")
    print(f"  Contratos activos: {kpis['kpis_globales']['total_contratos_activos']}")
    print(f"  Inversión total: ${kpis['kpis_globales']['inversion_total']:,.0f}")

# Test 3: Verificar análisis geográfico
geo_docs = db.collection('analytics_geoanalysis').where('tipo_zona', '==', 'comuna').limit(3).stream()
print("\nTop 3 comunas por inversión:")
for doc in geo_docs:
    data = doc.to_dict()
    print(f"  {data['nombre']}: ${data['inversion_total']:,.0f}")
```

## 🔍 Monitoreo

### Métricas Clave

Monitorea estas métricas para asegurar el correcto funcionamiento:

1. **Latencia de Actualización**: Debe completarse en < 5 minutos
2. **Documentos Procesados**: ~450 mensuales, ~1251 snapshots, ~25 zonas geográficas
3. **Errores**: 0 errores en ejecución normal
4. **Uso de Cuota Firestore**: ~2,000 lecturas + ~1,500 escrituras por día

### Logs

```powershell
# Ver logs de Cloud Functions
gcloud functions logs read run_analytics_updates --limit=50

# Filtrar errores
gcloud functions logs read run_analytics_updates --limit=50 | Select-String "ERROR"
```

## 🐛 Troubleshooting

### Error: "Missing indexes"

**Causa**: Los índices de Firestore no están creados o no están listos.

**Solución**:
```powershell
firebase deploy --only firestore:indexes
firebase firestore:indexes  # Verificar estado
```

### Error: "Permission denied"

**Causa**: Credenciales ADC no configuradas o sin permisos.

**Solución**:
```powershell
gcloud auth application-default login
gcloud projects add-iam-policy-binding dev-test-e778d \
    --member="user:tu-email@example.com" \
    --role="roles/datastore.user"
```

### Error: "Timeout" durante carga inicial

**Causa**: Demasiados documentos para procesar de una vez.

**Solución**: Ejecuta las funciones individualmente:
```python
from cloud_functions.analytics_functions import *

# Ejecutar una por una
update_analytics_contratos_monthly()
update_analytics_kpi_dashboard()
update_analytics_avance_proyectos()
update_analytics_geoanalysis()
```

### Documentos duplicados

**Causa**: Clave de documento no única o timestamps incorrectos.

**Solución**: Ejecuta script de limpieza:
```python
from database.config import get_firestore_client

db = get_firestore_client()

# Limpiar colección específica
collection = db.collection('analytics_contratos_monthly')
docs = collection.stream()

for doc in docs:
    doc.reference.delete()
    print(f"Deleted: {doc.id}")

# Re-ejecutar carga inicial
```

## 📊 Consultas de Ejemplo para Frontend

### Dashboard de KPIs

```javascript
// services/analytics.service.ts
export async function getLatestKPIs() {
  const snapshot = await db
    .collection('analytics_kpi_dashboard')
    .orderBy('fecha', 'desc')
    .limit(1)
    .get();
  
  if (!snapshot.empty) {
    return snapshot.docs[0].data();
  }
  return null;
}
```

### Series de Tiempo de Contratos

```javascript
export async function getContractMonthlyData(bp: string, year: number) {
  const snapshot = await db
    .collection('analytics_contratos_monthly')
    .where('bp', '==', bp)
    .where('anio', '==', year)
    .orderBy('mes', 'asc')
    .get();
  
  return snapshot.docs.map(doc => doc.data());
}
```

### Análisis Geográfico

```javascript
export async function getComunasWithInvestment() {
  const snapshot = await db
    .collection('analytics_geoanalysis')
    .where('tipo_zona', '==', 'comuna')
    .orderBy('inversion_total', 'desc')
    .get();
  
  return snapshot.docs.map(doc => ({
    id: doc.id,
    ...doc.data()
  }));
}
```

## 📝 Mantenimiento

### Limpieza de Datos Antiguos (Opcional)

Si deseas mantener solo los últimos 2 años de snapshots:

```python
from datetime import datetime, timedelta
from database.config import get_firestore_client

db = get_firestore_client()
cutoff_date = datetime.now() - timedelta(days=730)  # 2 años

# Limpiar snapshots antiguos
collection = db.collection('analytics_avance_proyectos')
old_docs = collection.where('fecha_snapshot', '<', cutoff_date).stream()

batch = db.batch()
count = 0

for doc in old_docs:
    batch.delete(doc.reference)
    count += 1
    
    if count >= 500:
        batch.commit()
        batch = db.batch()
        count = 0

if count > 0:
    batch.commit()

print(f"Deleted {count} old snapshots")
```

### Reindexación Completa

Si los datos están corruptos o desactualizados:

```powershell
# 1. Respaldar datos actuales
python -c "from database.config import get_firestore_client; import json; db = get_firestore_client(); [json.dump(doc.to_dict(), open(f'backup_{doc.id}.json', 'w')) for doc in db.collection('analytics_contratos_monthly').stream()]"

# 2. Limpiar colecciones analytics
# (Ejecutar script de limpieza arriba)

# 3. Re-ejecutar carga inicial
python load_initial_analytics.py
```

## 🎯 Checklist de Producción

Antes de ir a producción, verifica:

- [ ] Índices de Firestore desplegados y en estado `READY`
- [ ] Carga inicial completada exitosamente (4/4 funciones)
- [ ] Tests de carga incremental pasados (0 docs nuevos en segunda ejecución)
- [ ] Cloud Scheduler configurado para actualizaciones diarias
- [ ] Alertas configuradas en Cloud Monitoring para errores
- [ ] Documentación de endpoints entregada a equipo frontend
- [ ] Permisos de Firestore configurados correctamente
- [ ] Logs de Cloud Functions revisados (0 errores)
- [ ] Consultas de prueba ejecutadas exitosamente
- [ ] Plan de respaldo y recuperación documentado

## 🆘 Soporte

Si encuentras problemas durante el despliegue:

1. Revisa los logs: `gcloud functions logs read --limit=100`
2. Verifica el estado de índices: `firebase firestore:indexes`
3. Consulta la documentación de Firebase: https://firebase.google.com/docs/firestore
4. Revisa el archivo `docs/propuesta-datawarehouse-arquitectura.md` para detalles de diseño

---

**Última actualización**: 2025-11-09  
**Versión**: 1.0.0  
**Autor**: Sistema ETL Proyectos Cali
