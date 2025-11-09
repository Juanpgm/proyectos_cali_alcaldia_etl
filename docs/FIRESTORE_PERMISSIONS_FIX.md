# 🚨 Error de Permisos en Firestore - Solución Rápida

## Problema Detectado

La migración falló con el error: `403 Missing or insufficient permissions`

Esto significa que las **reglas de seguridad de Firestore** están bloqueando las escrituras.

---

## ✅ SOLUCIÓN RÁPIDA (5 minutos)

### Paso 1: Abrir la Consola de Firebase

1. Ve a: https://console.firebase.google.com/project/calitrack-44403/firestore/rules
2. Asegúrate de estar autenticado con **juanp.gzmz@gmail.com**

### Paso 2: Actualizar Reglas de Seguridad (Temporalmente)

En la consola de Firebase, ve a **Firestore Database** → **Rules** y reemplaza las reglas actuales con:

```javascript
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    // ⚠️ REGLAS TEMPORALES PARA MIGRACIÓN
    // Estas reglas permiten lectura/escritura total
    // IMPORTANTE: Actualizar después de la migración
    match /{document=**} {
      allow read, write: if true;
    }
  }
}
```

### Paso 3: Publicar las Reglas

1. Clic en **"Publish"** o **"Publicar"**
2. Espera unos 10-30 segundos para que se propaguen

### Paso 4: Re-ejecutar la Migración

```powershell
python migrate_firestore.py
```

---

## 🔐 DESPUÉS DE LA MIGRACIÓN (IMPORTANTE)

### Restaurar Reglas de Seguridad Apropiadas

Una vez completada la migración, **DEBES** actualizar las reglas a algo más seguro:

#### Opción 1: Modo Desarrollo (Solo para testing)

```javascript
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    match /{document=**} {
      allow read, write: if request.time < timestamp.date(2025, 12, 31);
    }
  }
}
```

#### Opción 2: Reglas con Autenticación (Recomendado)

```javascript
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    // Regla general: Solo usuarios autenticados
    match /{document=**} {
      allow read, write: if request.auth != null;
    }

    // Usuarios: Solo pueden leer/escribir su propio documento
    match /users/{userId} {
      allow read, write: if request.auth != null && request.auth.uid == userId;
    }

    // Otras colecciones: Solo lectura para autenticados
    match /proyectos_presupuestales/{docId} {
      allow read: if request.auth != null;
      allow write: if request.auth != null &&
                      get(/databases/$(database)/documents/users/$(request.auth.uid)).data.role == 'admin';
    }
  }
}
```

#### Opción 3: Copiar Reglas del Proyecto Origen

Si el proyecto `unidad-cumplimiento-aa245` ya tiene reglas configuradas:

1. Ve a: https://console.firebase.google.com/project/unidad-cumplimiento-aa245/firestore/rules
2. Copia todas las reglas
3. Pégalas en: https://console.firebase.google.com/project/calitrack-44403/firestore/rules
4. Publica

---

## 🔄 Alternativa: Usar Firebase CLI

Si prefieres usar la línea de comandos:

### Instalar Firebase CLI

```powershell
npm install -g firebase-tools
```

### Iniciar sesión con juanp.gzmz@gmail.com

```powershell
firebase login
```

### Inicializar proyecto

```powershell
firebase init firestore
# Selecciona calitrack-44403
```

### Editar firestore.rules

Crea o edita `firestore.rules` con las reglas temporales:

```javascript
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    match /{document=**} {
      allow read, write: if true;
    }
  }
}
```

### Desplegar reglas

```powershell
firebase deploy --only firestore:rules --project calitrack-44403
```

---

## 📝 Resumen del Proceso

1. ✅ Actualizar reglas de Firestore (permitir escritura)
2. ✅ Esperar 10-30 segundos
3. ✅ Ejecutar: `python migrate_firestore.py`
4. ✅ Verificar migración en Firebase Console
5. ⚠️ **CRÍTICO**: Restaurar reglas de seguridad apropiadas

---

## ⏱️ Tiempo Estimado

- Actualizar reglas: 2 minutos
- Propagación: 30 segundos
- Migración: 5-15 minutos (dependiendo del tamaño de datos)
- Verificación: 3 minutos
- **Total: ~10-20 minutos**

---

## 🆘 Si Continúa Fallando

### Verificar Permisos IAM

```powershell
gcloud projects get-iam-policy calitrack-44403
```

Debes tener al menos uno de estos roles:

- `roles/owner` ✅ (Ya lo tienes)
- `roles/editor`
- `roles/datastore.owner`
- `roles/datastore.user`

### Verificar APIs Habilitadas

```powershell
gcloud services list --enabled --project=calitrack-44403 | Select-String firestore
```

Debe mostrar:

- `firestore.googleapis.com`

---

## 📞 Siguiente Paso

**➡️ Ve a la consola de Firebase ahora y actualiza las reglas:**
https://console.firebase.google.com/project/calitrack-44403/firestore/rules

Después ejecuta:

```powershell
python migrate_firestore.py
```
