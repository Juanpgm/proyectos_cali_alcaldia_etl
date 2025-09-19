-- Triggers para updated_at ultra seguros
-- Generado: 2025-09-19 14:32:10

-- Función genérica para updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';


-- Trigger para contratos_proyectos
DROP TRIGGER IF EXISTS update_contratos_proyectos_modtime ON contratos_proyectos;
CREATE TRIGGER update_contratos_proyectos_modtime
    BEFORE UPDATE ON contratos_proyectos
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger para datos_caracteristicos_proyectos
DROP TRIGGER IF EXISTS update_datos_caracteristicos_proyectos_modtime ON datos_caracteristicos_proyectos;
CREATE TRIGGER update_datos_caracteristicos_proyectos_modtime
    BEFORE UPDATE ON datos_caracteristicos_proyectos
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger para ejecucion_presupuestal
DROP TRIGGER IF EXISTS update_ejecucion_presupuestal_modtime ON ejecucion_presupuestal;
CREATE TRIGGER update_ejecucion_presupuestal_modtime
    BEFORE UPDATE ON ejecucion_presupuestal
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger para movimientos_presupuestales
DROP TRIGGER IF EXISTS update_movimientos_presupuestales_modtime ON movimientos_presupuestales;
CREATE TRIGGER update_movimientos_presupuestales_modtime
    BEFORE UPDATE ON movimientos_presupuestales
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger para emp_contratos_index
DROP TRIGGER IF EXISTS update_emp_contratos_index_modtime ON emp_contratos_index;
CREATE TRIGGER update_emp_contratos_index_modtime
    BEFORE UPDATE ON emp_contratos_index
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger para emp_procesos
DROP TRIGGER IF EXISTS update_emp_procesos_modtime ON emp_procesos;
CREATE TRIGGER update_emp_procesos_modtime
    BEFORE UPDATE ON emp_procesos
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger para emp_procesos_index
DROP TRIGGER IF EXISTS update_emp_procesos_index_modtime ON emp_procesos_index;
CREATE TRIGGER update_emp_procesos_index_modtime
    BEFORE UPDATE ON emp_procesos_index
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger para emp_proyectos
DROP TRIGGER IF EXISTS update_emp_proyectos_modtime ON emp_proyectos;
CREATE TRIGGER update_emp_proyectos_modtime
    BEFORE UPDATE ON emp_proyectos
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger para procesos_secop
DROP TRIGGER IF EXISTS update_procesos_secop_modtime ON procesos_secop;
CREATE TRIGGER update_procesos_secop_modtime
    BEFORE UPDATE ON procesos_secop
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger para unidad_proyecto_infraestructura_equipamientos
DROP TRIGGER IF EXISTS update_unidad_proyecto_infraestructura_equipamientos_modtime ON unidad_proyecto_infraestructura_equipamientos;
CREATE TRIGGER update_unidad_proyecto_infraestructura_equipamientos_modtime
    BEFORE UPDATE ON unidad_proyecto_infraestructura_equipamientos
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger para unidad_proyecto_infraestructura_vial
DROP TRIGGER IF EXISTS update_unidad_proyecto_infraestructura_vial_modtime ON unidad_proyecto_infraestructura_vial;
CREATE TRIGGER update_unidad_proyecto_infraestructura_vial_modtime
    BEFORE UPDATE ON unidad_proyecto_infraestructura_vial
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
