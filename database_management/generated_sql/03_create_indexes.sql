-- Índices básicos ultra seguros
-- Generado: 2025-09-19 14:32:10


-- Índices para contratos_proyectos
CREATE INDEX IF NOT EXISTS idx_contratos_proyectos_created_at ON contratos_proyectos(created_at);
CREATE INDEX IF NOT EXISTS idx_contratos_proyectos_updated_at ON contratos_proyectos(updated_at);
CREATE INDEX IF NOT EXISTS idx_contratos_proyectos_is_active ON contratos_proyectos(is_active);

-- Índices para datos_caracteristicos_proyectos
CREATE INDEX IF NOT EXISTS idx_datos_caracteristicos_proyectos_created_at ON datos_caracteristicos_proyectos(created_at);
CREATE INDEX IF NOT EXISTS idx_datos_caracteristicos_proyectos_updated_at ON datos_caracteristicos_proyectos(updated_at);
CREATE INDEX IF NOT EXISTS idx_datos_caracteristicos_proyectos_is_active ON datos_caracteristicos_proyectos(is_active);

-- Índices para ejecucion_presupuestal
CREATE INDEX IF NOT EXISTS idx_ejecucion_presupuestal_created_at ON ejecucion_presupuestal(created_at);
CREATE INDEX IF NOT EXISTS idx_ejecucion_presupuestal_updated_at ON ejecucion_presupuestal(updated_at);
CREATE INDEX IF NOT EXISTS idx_ejecucion_presupuestal_is_active ON ejecucion_presupuestal(is_active);

-- Índices para movimientos_presupuestales
CREATE INDEX IF NOT EXISTS idx_movimientos_presupuestales_created_at ON movimientos_presupuestales(created_at);
CREATE INDEX IF NOT EXISTS idx_movimientos_presupuestales_updated_at ON movimientos_presupuestales(updated_at);
CREATE INDEX IF NOT EXISTS idx_movimientos_presupuestales_is_active ON movimientos_presupuestales(is_active);

-- Índices para emp_contratos_index
CREATE INDEX IF NOT EXISTS idx_emp_contratos_index_created_at ON emp_contratos_index(created_at);
CREATE INDEX IF NOT EXISTS idx_emp_contratos_index_updated_at ON emp_contratos_index(updated_at);
CREATE INDEX IF NOT EXISTS idx_emp_contratos_index_is_active ON emp_contratos_index(is_active);

-- Índices para emp_procesos
CREATE INDEX IF NOT EXISTS idx_emp_procesos_created_at ON emp_procesos(created_at);
CREATE INDEX IF NOT EXISTS idx_emp_procesos_updated_at ON emp_procesos(updated_at);
CREATE INDEX IF NOT EXISTS idx_emp_procesos_is_active ON emp_procesos(is_active);

-- Índices para emp_procesos_index
CREATE INDEX IF NOT EXISTS idx_emp_procesos_index_created_at ON emp_procesos_index(created_at);
CREATE INDEX IF NOT EXISTS idx_emp_procesos_index_updated_at ON emp_procesos_index(updated_at);
CREATE INDEX IF NOT EXISTS idx_emp_procesos_index_is_active ON emp_procesos_index(is_active);

-- Índices para emp_proyectos
CREATE INDEX IF NOT EXISTS idx_emp_proyectos_created_at ON emp_proyectos(created_at);
CREATE INDEX IF NOT EXISTS idx_emp_proyectos_updated_at ON emp_proyectos(updated_at);
CREATE INDEX IF NOT EXISTS idx_emp_proyectos_is_active ON emp_proyectos(is_active);

-- Índices para procesos_secop
CREATE INDEX IF NOT EXISTS idx_procesos_secop_created_at ON procesos_secop(created_at);
CREATE INDEX IF NOT EXISTS idx_procesos_secop_updated_at ON procesos_secop(updated_at);
CREATE INDEX IF NOT EXISTS idx_procesos_secop_is_active ON procesos_secop(is_active);

-- Índices para unidad_proyecto_infraestructura_equipamientos
CREATE INDEX IF NOT EXISTS idx_unidad_proyecto_infraestructura_equipamientos_created_at ON unidad_proyecto_infraestructura_equipamientos(created_at);
CREATE INDEX IF NOT EXISTS idx_unidad_proyecto_infraestructura_equipamientos_updated_at ON unidad_proyecto_infraestructura_equipamientos(updated_at);
CREATE INDEX IF NOT EXISTS idx_unidad_proyecto_infraestructura_equipamientos_is_active ON unidad_proyecto_infraestructura_equipamientos(is_active);

-- Índices para unidad_proyecto_infraestructura_vial
CREATE INDEX IF NOT EXISTS idx_unidad_proyecto_infraestructura_vial_created_at ON unidad_proyecto_infraestructura_vial(created_at);
CREATE INDEX IF NOT EXISTS idx_unidad_proyecto_infraestructura_vial_updated_at ON unidad_proyecto_infraestructura_vial(updated_at);
CREATE INDEX IF NOT EXISTS idx_unidad_proyecto_infraestructura_vial_is_active ON unidad_proyecto_infraestructura_vial(is_active);
