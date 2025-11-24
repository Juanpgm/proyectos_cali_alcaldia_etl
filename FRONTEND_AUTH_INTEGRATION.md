# 🎨 Integración de Autenticación en Frontend (NextJS)

**Proyecto**: Gestor de Proyectos Cali  
**API**: https://gestorproyectoapi-production.up.railway.app  
**Fecha**: 24 de Noviembre 2025

---

## 📋 Tabla de Contenidos

1. [Configuración Inicial](#1-configuración-inicial)
2. [Context Provider de Autenticación](#2-context-provider-de-autenticación)
3. [Componentes de UI](#3-componentes-de-ui)
4. [Hooks Personalizados](#4-hooks-personalizados)
5. [Protección de Rutas](#5-protección-de-rutas)
6. [Llamadas a la API](#6-llamadas-a-la-api)
7. [Manejo de Permisos](#7-manejo-de-permisos)
8. [Ejemplos Completos](#8-ejemplos-completos)

---

## 1. Configuración Inicial

### **1.1 Instalar Dependencias**

```bash
npm install firebase
# o
yarn add firebase
```

### **1.2 Configurar Firebase en el Frontend**

Crea `lib/firebase.ts`:

```typescript
// lib/firebase.ts
import { initializeApp, getApps, FirebaseApp } from "firebase/app";
import { getAuth, Auth } from "firebase/auth";

// Configuración de Firebase (valores públicos)
const firebaseConfig = {
  apiKey: process.env.NEXT_PUBLIC_FIREBASE_API_KEY,
  authDomain: process.env.NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN,
  projectId: process.env.NEXT_PUBLIC_FIREBASE_PROJECT_ID,
};

// Inicializar Firebase
let app: FirebaseApp;
if (!getApps().length) {
  app = initializeApp(firebaseConfig);
} else {
  app = getApps()[0];
}

export const auth = getAuth(app);
export default app;
```

### **1.3 Variables de Entorno**

Crea `.env.local`:

```env
# Firebase Configuration
NEXT_PUBLIC_FIREBASE_API_KEY=tu_api_key
NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN=tu_proyecto.firebaseapp.com
NEXT_PUBLIC_FIREBASE_PROJECT_ID=tu_proyecto_id

# API Backend
NEXT_PUBLIC_API_URL=https://gestorproyectoapi-production.up.railway.app
```

---

## 2. Context Provider de Autenticación

### **2.1 Tipos de TypeScript**

Crea `types/auth.ts`:

```typescript
// types/auth.ts

export interface User {
  uid: string;
  email: string;
  full_name: string;
  roles: string[];
  permissions: string[];
  centro_gestor_assigned?: string;
  email_verified: boolean;
  phone_verified: boolean;
  is_active: boolean;
  created_at: string;
  last_login_at?: string;
}

export interface AuthContextType {
  user: User | null;
  loading: boolean;
  error: string | null;

  // Métodos de autenticación
  login: (email: string, password: string) => Promise<void>;
  loginWithGoogle: () => Promise<void>;
  logout: () => Promise<void>;
  register: (data: RegisterData) => Promise<void>;

  // Verificación de permisos
  hasPermission: (permission: string) => boolean;
  hasRole: (role: string) => boolean;
  hasAnyRole: (roles: string[]) => boolean;

  // Actualización de datos
  refreshUser: () => Promise<void>;
}

export interface RegisterData {
  email: string;
  password: string;
  full_name: string;
  phone_number?: string;
  centro_gestor?: string;
}
```

### **2.2 Auth Context**

Crea `contexts/AuthContext.tsx`:

```typescript
// contexts/AuthContext.tsx
"use client";

import React, { createContext, useContext, useState, useEffect } from "react";
import {
  signInWithEmailAndPassword,
  signInWithPopup,
  GoogleAuthProvider,
  signOut,
  onAuthStateChanged,
  User as FirebaseUser,
} from "firebase/auth";
import { auth } from "@/lib/firebase";
import { User, AuthContextType, RegisterData } from "@/types/auth";

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const API_URL = process.env.NEXT_PUBLIC_API_URL;

  // Función auxiliar para obtener datos completos del usuario desde la API
  const fetchUserData = async (
    firebaseUser: FirebaseUser
  ): Promise<User | null> => {
    try {
      const idToken = await firebaseUser.getIdToken();

      const response = await fetch(`${API_URL}/auth/validate-session`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${idToken}`,
        },
        body: JSON.stringify({ id_token: idToken }),
      });

      if (!response.ok) {
        throw new Error("Error al validar sesión");
      }

      const data = await response.json();

      if (data.success && data.user) {
        return data.user as User;
      }

      return null;
    } catch (error) {
      console.error("Error fetching user data:", error);
      return null;
    }
  };

  // Escuchar cambios de autenticación de Firebase
  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, async (firebaseUser) => {
      if (firebaseUser) {
        const userData = await fetchUserData(firebaseUser);
        setUser(userData);
      } else {
        setUser(null);
      }
      setLoading(false);
    });

    return () => unsubscribe();
  }, []);

  // Login con email/password
  const login = async (email: string, password: string) => {
    try {
      setError(null);
      setLoading(true);

      // Autenticarse con Firebase
      const userCredential = await signInWithEmailAndPassword(
        auth,
        email,
        password
      );
      const idToken = await userCredential.user.getIdToken();

      // Validar con el backend
      const response = await fetch(`${API_URL}/auth/login`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ email, password }),
      });

      if (!response.ok) {
        throw new Error("Credenciales inválidas");
      }

      const data = await response.json();

      if (data.success && data.user) {
        setUser(data.user);
      }
    } catch (error: any) {
      setError(error.message);
      throw error;
    } finally {
      setLoading(false);
    }
  };

  // Login con Google
  const loginWithGoogle = async () => {
    try {
      setError(null);
      setLoading(true);

      const provider = new GoogleAuthProvider();
      provider.setCustomParameters({
        hd: "cali.gov.co", // Restringir a dominio @cali.gov.co
      });

      const result = await signInWithPopup(auth, provider);
      const idToken = await result.user.getIdToken();

      // Validar con el backend
      const response = await fetch(`${API_URL}/auth/google`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ google_token: idToken }),
      });

      if (!response.ok) {
        throw new Error("Error en autenticación con Google");
      }

      const data = await response.json();

      if (data.success && data.user) {
        setUser(data.user);
      }
    } catch (error: any) {
      setError(error.message);
      throw error;
    } finally {
      setLoading(false);
    }
  };

  // Registro de nuevo usuario
  const register = async (data: RegisterData) => {
    try {
      setError(null);
      setLoading(true);

      const response = await fetch(`${API_URL}/auth/register`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(data),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || "Error en el registro");
      }

      const responseData = await response.json();

      // Después del registro, hacer login automático
      await login(data.email, data.password);
    } catch (error: any) {
      setError(error.message);
      throw error;
    } finally {
      setLoading(false);
    }
  };

  // Logout
  const logout = async () => {
    try {
      await signOut(auth);
      setUser(null);
    } catch (error: any) {
      setError(error.message);
      throw error;
    }
  };

  // Verificar si el usuario tiene un permiso específico
  const hasPermission = (permission: string): boolean => {
    if (!user || !user.permissions) return false;

    // Super admin tiene todos los permisos
    if (user.permissions.includes("*")) return true;

    // Verificar permiso exacto
    if (user.permissions.includes(permission)) return true;

    // Verificar wildcard (ej: "read:*" cubre "read:proyectos")
    const [action, resource] = permission.split(":");
    const wildcardPermission = `${action}:*`;

    return user.permissions.includes(wildcardPermission);
  };

  // Verificar si el usuario tiene un rol específico
  const hasRole = (role: string): boolean => {
    if (!user || !user.roles) return false;
    return user.roles.includes(role);
  };

  // Verificar si el usuario tiene alguno de los roles
  const hasAnyRole = (roles: string[]): boolean => {
    if (!user || !user.roles) return false;
    return roles.some((role) => user.roles.includes(role));
  };

  // Refrescar datos del usuario
  const refreshUser = async () => {
    if (auth.currentUser) {
      const userData = await fetchUserData(auth.currentUser);
      setUser(userData);
    }
  };

  const value: AuthContextType = {
    user,
    loading,
    error,
    login,
    loginWithGoogle,
    logout,
    register,
    hasPermission,
    hasRole,
    hasAnyRole,
    refreshUser,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
```

### **2.3 Usar el Provider en la App**

En `app/layout.tsx`:

```typescript
// app/layout.tsx
import { AuthProvider } from "@/contexts/AuthContext";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="es">
      <body>
        <AuthProvider>{children}</AuthProvider>
      </body>
    </html>
  );
}
```

---

## 3. Componentes de UI

### **3.1 Formulario de Login**

```typescript
// components/LoginForm.tsx
"use client";

import { useState } from "react";
import { useAuth } from "@/contexts/AuthContext";
import { useRouter } from "next/navigation";

export default function LoginForm() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const { login, loginWithGoogle, loading, error } = useAuth();
  const router = useRouter();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await login(email, password);
      router.push("/dashboard");
    } catch (error) {
      console.error("Error en login:", error);
    }
  };

  const handleGoogleLogin = async () => {
    try {
      await loginWithGoogle();
      router.push("/dashboard");
    } catch (error) {
      console.error("Error en Google login:", error);
    }
  };

  return (
    <div className="max-w-md mx-auto p-6">
      <h2 className="text-2xl font-bold mb-6">Iniciar Sesión</h2>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
          {error}
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label className="block text-sm font-medium mb-1">Email</label>
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            className="w-full px-3 py-2 border rounded-md"
            required
          />
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">Contraseña</label>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="w-full px-3 py-2 border rounded-md"
            required
          />
        </div>

        <button
          type="submit"
          disabled={loading}
          className="w-full bg-blue-600 text-white py-2 rounded-md hover:bg-blue-700 disabled:bg-gray-400"
        >
          {loading ? "Iniciando sesión..." : "Iniciar Sesión"}
        </button>
      </form>

      <div className="mt-4">
        <button
          onClick={handleGoogleLogin}
          disabled={loading}
          className="w-full bg-white border border-gray-300 text-gray-700 py-2 rounded-md hover:bg-gray-50 flex items-center justify-center gap-2"
        >
          <svg className="w-5 h-5" viewBox="0 0 24 24">
            {/* Google icon SVG */}
          </svg>
          Continuar con Google
        </button>
      </div>
    </div>
  );
}
```

### **3.2 Componente de Usuario Logueado**

```typescript
// components/UserMenu.tsx
"use client";

import { useAuth } from "@/contexts/AuthContext";
import { useRouter } from "next/navigation";

export default function UserMenu() {
  const { user, logout } = useAuth();
  const router = useRouter();

  if (!user) return null;

  const handleLogout = async () => {
    await logout();
    router.push("/login");
  };

  return (
    <div className="flex items-center gap-4">
      <div className="text-right">
        <p className="font-medium">{user.full_name}</p>
        <p className="text-sm text-gray-600">{user.email}</p>
        <p className="text-xs text-gray-500">{user.roles.join(", ")}</p>
      </div>
      <button
        onClick={handleLogout}
        className="px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700"
      >
        Cerrar Sesión
      </button>
    </div>
  );
}
```

### **3.3 Componente Condicional por Permiso**

```typescript
// components/PermissionGate.tsx
"use client";

import { useAuth } from "@/contexts/AuthContext";

interface PermissionGateProps {
  permission?: string;
  role?: string;
  roles?: string[];
  children: React.ReactNode;
  fallback?: React.ReactNode;
}

export default function PermissionGate({
  permission,
  role,
  roles,
  children,
  fallback = null,
}: PermissionGateProps) {
  const { hasPermission, hasRole, hasAnyRole } = useAuth();

  let hasAccess = false;

  if (permission) {
    hasAccess = hasPermission(permission);
  } else if (role) {
    hasAccess = hasRole(role);
  } else if (roles) {
    hasAccess = hasAnyRole(roles);
  }

  if (!hasAccess) {
    return <>{fallback}</>;
  }

  return <>{children}</>;
}
```

**Uso:**

```typescript
<PermissionGate permission="write:proyectos">
  <button>Crear Proyecto</button>
</PermissionGate>

<PermissionGate role="super_admin" fallback={<p>Acceso denegado</p>}>
  <AdminPanel />
</PermissionGate>
```

---

## 4. Hooks Personalizados

### **4.1 Hook para llamadas a la API**

```typescript
// hooks/useApi.ts
import { useState, useCallback } from "react";
import { auth } from "@/lib/firebase";

interface ApiOptions extends RequestInit {
  requiresAuth?: boolean;
}

export function useApi() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const API_URL = process.env.NEXT_PUBLIC_API_URL;

  const callApi = useCallback(
    async <T = any>(endpoint: string, options: ApiOptions = {}): Promise<T> => {
      setLoading(true);
      setError(null);

      try {
        const { requiresAuth = true, ...fetchOptions } = options;

        const headers: HeadersInit = {
          "Content-Type": "application/json",
          ...fetchOptions.headers,
        };

        // Agregar token de autenticación si es requerido
        if (requiresAuth && auth.currentUser) {
          const token = await auth.currentUser.getIdToken();
          headers["Authorization"] = `Bearer ${token}`;
        }

        const response = await fetch(`${API_URL}${endpoint}`, {
          ...fetchOptions,
          headers,
        });

        if (!response.ok) {
          const errorData = await response.json().catch(() => ({}));
          throw new Error(errorData.detail || `Error: ${response.status}`);
        }

        const data = await response.json();
        return data;
      } catch (err: any) {
        setError(err.message);
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [API_URL]
  );

  return { callApi, loading, error };
}
```

### **4.2 Hook para operaciones CRUD**

```typescript
// hooks/useProyectos.ts
import { useState, useCallback } from "react";
import { useApi } from "./useApi";

interface Proyecto {
  id: string;
  nombre_proyecto: string;
  bpin: string;
  nombre_centro_gestor: string;
  // ... otros campos
}

export function useProyectos() {
  const { callApi, loading, error } = useApi();
  const [proyectos, setProyectos] = useState<Proyecto[]>([]);

  const getAll = useCallback(async () => {
    const data = await callApi<{ success: boolean; data: Proyecto[] }>(
      "/proyectos-presupuestales/all"
    );
    if (data.success) {
      setProyectos(data.data);
    }
    return data.data;
  }, [callApi]);

  const getByBpin = useCallback(
    async (bpin: string) => {
      const data = await callApi<{ success: boolean; data: Proyecto[] }>(
        `/proyectos-presupuestales/bpin/${bpin}`
      );
      return data.data;
    },
    [callApi]
  );

  const getByCentroGestor = useCallback(
    async (centroGestor: string) => {
      const data = await callApi<{ success: boolean; data: Proyecto[] }>(
        `/proyectos-presupuestales/centro-gestor/${encodeURIComponent(
          centroGestor
        )}`
      );
      return data.data;
    },
    [callApi]
  );

  const uploadJson = useCallback(
    async (file: File, updateMode: string = "merge") => {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("update_mode", updateMode);

      const data = await callApi("/proyectos-presupuestales/cargar-json", {
        method: "POST",
        body: formData,
        headers: {}, // Dejar que el browser establezca Content-Type con boundary
      });

      return data;
    },
    [callApi]
  );

  return {
    proyectos,
    loading,
    error,
    getAll,
    getByBpin,
    getByCentroGestor,
    uploadJson,
  };
}
```

---

## 5. Protección de Rutas

### **5.1 Middleware de Autenticación**

```typescript
// middleware.ts (en la raíz del proyecto)
import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

export function middleware(request: NextRequest) {
  // Rutas públicas que no requieren autenticación
  const publicPaths = ["/login", "/register", "/"];

  const isPublicPath = publicPaths.some((path) =>
    request.nextUrl.pathname.startsWith(path)
  );

  // Si es ruta pública, permitir acceso
  if (isPublicPath) {
    return NextResponse.next();
  }

  // Para rutas protegidas, verificar si hay sesión
  // Nota: Next.js middleware no tiene acceso a Firebase Auth directamente
  // La validación real se hace en el cliente con useAuth

  return NextResponse.next();
}

export const config = {
  matcher: ["/((?!api|_next/static|_next/image|favicon.ico).*)"],
};
```

### **5.2 Componente de Ruta Protegida**

```typescript
// components/ProtectedRoute.tsx
"use client";

import { useEffect } from "react";
import { useAuth } from "@/contexts/AuthContext";
import { useRouter } from "next/navigation";

interface ProtectedRouteProps {
  children: React.ReactNode;
  requiredPermission?: string;
  requiredRole?: string;
  requiredRoles?: string[];
}

export default function ProtectedRoute({
  children,
  requiredPermission,
  requiredRole,
  requiredRoles,
}: ProtectedRouteProps) {
  const { user, loading, hasPermission, hasRole, hasAnyRole } = useAuth();
  const router = useRouter();

  useEffect(() => {
    if (!loading && !user) {
      router.push("/login");
      return;
    }

    if (user && requiredPermission && !hasPermission(requiredPermission)) {
      router.push("/unauthorized");
      return;
    }

    if (user && requiredRole && !hasRole(requiredRole)) {
      router.push("/unauthorized");
      return;
    }

    if (user && requiredRoles && !hasAnyRole(requiredRoles)) {
      router.push("/unauthorized");
      return;
    }
  }, [user, loading, requiredPermission, requiredRole, requiredRoles]);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Cargando...</p>
        </div>
      </div>
    );
  }

  if (!user) {
    return null;
  }

  return <>{children}</>;
}
```

**Uso en páginas:**

```typescript
// app/dashboard/page.tsx
import ProtectedRoute from "@/components/ProtectedRoute";

export default function DashboardPage() {
  return (
    <ProtectedRoute>
      <div>
        <h1>Dashboard</h1>
        {/* Contenido del dashboard */}
      </div>
    </ProtectedRoute>
  );
}

// app/admin/page.tsx
export default function AdminPage() {
  return (
    <ProtectedRoute requiredRole="super_admin">
      <div>
        <h1>Panel de Administración</h1>
        {/* Solo super_admin puede ver esto */}
      </div>
    </ProtectedRoute>
  );
}
```

---

## 6. Llamadas a la API

### **6.1 Ejemplo: Cargar Proyectos**

```typescript
// components/ProyectosList.tsx
"use client";

import { useEffect, useState } from "react";
import { useProyectos } from "@/hooks/useProyectos";
import PermissionGate from "./PermissionGate";

export default function ProyectosList() {
  const { proyectos, loading, error, getAll } = useProyectos();

  useEffect(() => {
    getAll();
  }, [getAll]);

  if (loading) return <div>Cargando proyectos...</div>;
  if (error) return <div>Error: {error}</div>;

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-2xl font-bold">Proyectos Presupuestales</h2>

        <PermissionGate permission="write:proyectos">
          <button className="px-4 py-2 bg-blue-600 text-white rounded">
            Nuevo Proyecto
          </button>
        </PermissionGate>
      </div>

      <div className="grid gap-4">
        {proyectos.map((proyecto) => (
          <div key={proyecto.id} className="border p-4 rounded">
            <h3 className="font-bold">{proyecto.nombre_proyecto}</h3>
            <p className="text-sm text-gray-600">BPIN: {proyecto.bpin}</p>
            <p className="text-sm text-gray-600">
              Centro Gestor: {proyecto.nombre_centro_gestor}
            </p>
          </div>
        ))}
      </div>
    </div>
  );
}
```

### **6.2 Ejemplo: Upload de Archivo**

```typescript
// components/UploadProyectosForm.tsx
"use client";

import { useState } from "react";
import { useProyectos } from "@/hooks/useProyectos";

export default function UploadProyectosForm() {
  const [file, setFile] = useState<File | null>(null);
  const [updateMode, setUpdateMode] = useState("merge");
  const { uploadJson, loading, error } = useProyectos();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!file) {
      alert("Selecciona un archivo");
      return;
    }

    try {
      const result = await uploadJson(file, updateMode);
      alert("Archivo cargado exitosamente");
      console.log("Resultado:", result);
    } catch (error) {
      console.error("Error al cargar:", error);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div>
        <label className="block text-sm font-medium mb-1">Archivo JSON</label>
        <input
          type="file"
          accept=".json"
          onChange={(e) => setFile(e.target.files?.[0] || null)}
          className="w-full"
        />
      </div>

      <div>
        <label className="block text-sm font-medium mb-1">
          Modo de Actualización
        </label>
        <select
          value={updateMode}
          onChange={(e) => setUpdateMode(e.target.value)}
          className="w-full px-3 py-2 border rounded"
        >
          <option value="merge">Merge (actualizar existentes)</option>
          <option value="replace">Replace (reemplazar todo)</option>
          <option value="append">Append (solo agregar nuevos)</option>
        </select>
      </div>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded">
          {error}
        </div>
      )}

      <button
        type="submit"
        disabled={loading || !file}
        className="w-full bg-blue-600 text-white py-2 rounded hover:bg-blue-700 disabled:bg-gray-400"
      >
        {loading ? "Cargando..." : "Cargar Archivo"}
      </button>
    </form>
  );
}
```

---

## 7. Manejo de Permisos

### **7.1 Tabla de Permisos por Funcionalidad**

| Funcionalidad      | Permiso Requerido  | Componente                  |
| ------------------ | ------------------ | --------------------------- |
| Ver proyectos      | `read:proyectos`   | `<ProyectosList />`         |
| Crear proyecto     | `write:proyectos`  | `<CreateProyectoForm />`    |
| Editar proyecto    | `write:proyectos`  | `<EditProyectoForm />`      |
| Eliminar proyecto  | `delete:proyectos` | `<DeleteProyectoButton />`  |
| Cargar GeoJSON     | `write:unidades`   | `<UploadGeoJSONForm />`     |
| Descargar GeoJSON  | `download:geojson` | `<DownloadGeoJSONButton />` |
| Ver contratos      | `read:contratos`   | `<ContratosList />`         |
| Crear reporte      | `write:contratos`  | `<CreateReporteForm />`     |
| Gestionar usuarios | `manage:users`     | `<AdminUsersPanel />`       |
| Ver logs           | `view:audit_logs`  | `<AuditLogsTable />`        |

### **7.2 Ejemplo: Botones Condicionales**

```typescript
// components/ActionButtons.tsx
import PermissionGate from "./PermissionGate";

export default function ActionButtons({ proyectoId }: { proyectoId: string }) {
  return (
    <div className="flex gap-2">
      <PermissionGate permission="read:proyectos">
        <button className="px-3 py-1 bg-blue-500 text-white rounded">
          Ver
        </button>
      </PermissionGate>

      <PermissionGate permission="write:proyectos">
        <button className="px-3 py-1 bg-green-500 text-white rounded">
          Editar
        </button>
      </PermissionGate>

      <PermissionGate permission="delete:proyectos">
        <button className="px-3 py-1 bg-red-500 text-white rounded">
          Eliminar
        </button>
      </PermissionGate>
    </div>
  );
}
```

---

## 8. Ejemplos Completos

### **8.1 Página de Login Completa**

```typescript
// app/login/page.tsx
"use client";

import { useState } from "react";
import { useAuth } from "@/contexts/AuthContext";
import { useRouter } from "next/navigation";
import Link from "next/link";

export default function LoginPage() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const { login, loginWithGoogle, loading, error } = useAuth();
  const router = useRouter();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await login(email, password);
      router.push("/dashboard");
    } catch (error) {
      // Error manejado por el contexto
    }
  };

  const handleGoogleLogin = async () => {
    try {
      await loginWithGoogle();
      router.push("/dashboard");
    } catch (error) {
      // Error manejado por el contexto
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100">
      <div className="max-w-md w-full bg-white rounded-lg shadow-md p-8">
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold text-gray-900">
            Gestor de Proyectos Cali
          </h1>
          <p className="text-gray-600 mt-2">Inicia sesión en tu cuenta</p>
        </div>

        {error && (
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Correo Electrónico
            </label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              placeholder="tu-email@cali.gov.co"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Contraseña
            </label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              placeholder="••••••••"
              required
            />
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-blue-600 text-white py-2 rounded-md hover:bg-blue-700 disabled:bg-gray-400 transition-colors"
          >
            {loading ? "Iniciando sesión..." : "Iniciar Sesión"}
          </button>
        </form>

        <div className="mt-6">
          <div className="relative">
            <div className="absolute inset-0 flex items-center">
              <div className="w-full border-t border-gray-300"></div>
            </div>
            <div className="relative flex justify-center text-sm">
              <span className="px-2 bg-white text-gray-500">
                O continúa con
              </span>
            </div>
          </div>

          <button
            onClick={handleGoogleLogin}
            disabled={loading}
            className="mt-4 w-full bg-white border border-gray-300 text-gray-700 py-2 rounded-md hover:bg-gray-50 flex items-center justify-center gap-2 transition-colors"
          >
            <svg className="w-5 h-5" viewBox="0 0 24 24">
              <path
                fill="#4285F4"
                d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"
              />
              <path
                fill="#34A853"
                d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"
              />
              <path
                fill="#FBBC05"
                d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"
              />
              <path
                fill="#EA4335"
                d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"
              />
            </svg>
            Google
          </button>
        </div>

        <div className="mt-6 text-center text-sm">
          <span className="text-gray-600">¿No tienes cuenta? </span>
          <Link href="/register" className="text-blue-600 hover:underline">
            Regístrate aquí
          </Link>
        </div>
      </div>
    </div>
  );
}
```

### **8.2 Dashboard con Datos Reales**

```typescript
// app/dashboard/page.tsx
"use client";

import { useEffect } from "react";
import { useAuth } from "@/contexts/AuthContext";
import { useProyectos } from "@/hooks/useProyectos";
import ProtectedRoute from "@/components/ProtectedRoute";
import UserMenu from "@/components/UserMenu";
import PermissionGate from "@/components/PermissionGate";
import Link from "next/link";

export default function DashboardPage() {
  const { user } = useAuth();
  const { proyectos, loading, getAll } = useProyectos();

  useEffect(() => {
    getAll();
  }, [getAll]);

  return (
    <ProtectedRoute>
      <div className="min-h-screen bg-gray-100">
        {/* Header */}
        <header className="bg-white shadow">
          <div className="max-w-7xl mx-auto px-4 py-4 sm:px-6 lg:px-8">
            <div className="flex justify-between items-center">
              <h1 className="text-2xl font-bold text-gray-900">
                Dashboard - Gestor de Proyectos
              </h1>
              <UserMenu />
            </div>
          </div>
        </header>

        {/* Main Content */}
        <main className="max-w-7xl mx-auto px-4 py-6 sm:px-6 lg:px-8">
          {/* Stats Cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
            <div className="bg-white p-6 rounded-lg shadow">
              <h3 className="text-lg font-semibold text-gray-700">
                Total Proyectos
              </h3>
              <p className="text-3xl font-bold text-blue-600 mt-2">
                {loading ? "..." : proyectos.length}
              </p>
            </div>

            <div className="bg-white p-6 rounded-lg shadow">
              <h3 className="text-lg font-semibold text-gray-700">
                Tu Centro Gestor
              </h3>
              <p className="text-xl font-bold text-green-600 mt-2">
                {user?.centro_gestor_assigned || "No asignado"}
              </p>
            </div>

            <div className="bg-white p-6 rounded-lg shadow">
              <h3 className="text-lg font-semibold text-gray-700">Tu Rol</h3>
              <p className="text-xl font-bold text-purple-600 mt-2">
                {user?.roles.join(", ")}
              </p>
            </div>
          </div>

          {/* Quick Actions */}
          <div className="bg-white p-6 rounded-lg shadow mb-8">
            <h2 className="text-xl font-bold mb-4">Acciones Rápidas</h2>
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <PermissionGate permission="read:proyectos">
                <Link
                  href="/proyectos"
                  className="p-4 border rounded-lg hover:bg-gray-50 text-center"
                >
                  <div className="text-3xl mb-2">📊</div>
                  <div className="font-medium">Ver Proyectos</div>
                </Link>
              </PermissionGate>

              <PermissionGate permission="write:proyectos">
                <Link
                  href="/proyectos/upload"
                  className="p-4 border rounded-lg hover:bg-gray-50 text-center"
                >
                  <div className="text-3xl mb-2">📤</div>
                  <div className="font-medium">Cargar Datos</div>
                </Link>
              </PermissionGate>

              <PermissionGate permission="read:contratos">
                <Link
                  href="/contratos"
                  className="p-4 border rounded-lg hover:bg-gray-50 text-center"
                >
                  <div className="text-3xl mb-2">📋</div>
                  <div className="font-medium">Contratos</div>
                </Link>
              </PermissionGate>

              <PermissionGate permission="manage:users">
                <Link
                  href="/admin/users"
                  className="p-4 border rounded-lg hover:bg-gray-50 text-center"
                >
                  <div className="text-3xl mb-2">👥</div>
                  <div className="font-medium">Administrar</div>
                </Link>
              </PermissionGate>
            </div>
          </div>

          {/* Recent Projects */}
          <div className="bg-white p-6 rounded-lg shadow">
            <h2 className="text-xl font-bold mb-4">Proyectos Recientes</h2>
            {loading ? (
              <div className="text-center py-8">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
                <p className="mt-4 text-gray-600">Cargando proyectos...</p>
              </div>
            ) : (
              <div className="space-y-3">
                {proyectos.slice(0, 5).map((proyecto) => (
                  <div
                    key={proyecto.id}
                    className="border p-4 rounded hover:bg-gray-50"
                  >
                    <h3 className="font-bold">{proyecto.nombre_proyecto}</h3>
                    <div className="text-sm text-gray-600 mt-1">
                      <span>BPIN: {proyecto.bpin}</span>
                      <span className="mx-2">•</span>
                      <span>{proyecto.nombre_centro_gestor}</span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </main>
      </div>
    </ProtectedRoute>
  );
}
```

---

## 🔑 Resumen de Configuración

### **Checklist de Implementación:**

- [ ] Instalar Firebase SDK
- [ ] Configurar variables de entorno (`.env.local`)
- [ ] Crear `lib/firebase.ts`
- [ ] Crear tipos en `types/auth.ts`
- [ ] Crear `AuthContext` en `contexts/AuthContext.tsx`
- [ ] Envolver app con `<AuthProvider>` en `layout.tsx`
- [ ] Crear hooks personalizados (`useApi`, `useProyectos`, etc.)
- [ ] Crear componentes de UI (`LoginForm`, `UserMenu`, `PermissionGate`)
- [ ] Crear `ProtectedRoute` para rutas protegidas
- [ ] Implementar páginas de login/dashboard
- [ ] Testear flujo completo de autenticación

### **Flujo de Autenticación:**

```
1. Usuario ingresa credenciales → LoginForm
2. LoginForm llama useAuth().login()
3. AuthContext autentica con Firebase
4. AuthContext obtiene token de Firebase
5. AuthContext valida con backend (/auth/validate-session)
6. Backend retorna datos completos del usuario (roles, permisos)
7. AuthContext guarda user en estado
8. Redirect a /dashboard
9. ProtectedRoute valida que user existe
10. Dashboard muestra datos según permisos
```

### **Endpoints de la API que usarás:**

| Endpoint                        | Método | Uso en Frontend                     |
| ------------------------------- | ------ | ----------------------------------- |
| `/auth/login`                   | POST   | Login con email/password            |
| `/auth/google`                  | POST   | Login con Google                    |
| `/auth/register`                | POST   | Registro de nuevo usuario           |
| `/auth/validate-session`        | POST   | Obtener datos completos del usuario |
| `/auth/config`                  | GET    | Configuración de Firebase (público) |
| `/proyectos-presupuestales/all` | GET    | Listar proyectos                    |
| `/unidades-proyecto/geometry`   | GET    | Datos GeoJSON para mapa             |
| `/auth/users`                   | GET    | Admin: listar usuarios              |
| `/auth/users/{uid}/roles`       | POST   | Admin: asignar roles                |

---

**Versión:** 1.0  
**Última Actualización:** 24 de Noviembre 2025  
**Autor**: Sistema de Auth para Gestor de Proyectos Cali
