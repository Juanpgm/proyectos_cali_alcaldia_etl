-- Creación de Tablas Ultra Seguras
-- Generado: 2025-09-19 14:32:10
-- Total: 11 tablas


-- Tabla: contratos_proyectos
-- Generado: 2025-09-19 14:32:10
-- Campos: 80 + 5 sistema
CREATE TABLE IF NOT EXISTS contratos_proyectos (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    version INTEGER DEFAULT 1,    is_active BOOLEAN DEFAULT true,    anno_bpin TEXT,    bpin BIGINT,    codigo_categoria_principal VARCHAR(100),    codigo_entidad VARCHAR(100),    codigo_proveedor TEXT,    contrato_puede_ser_prorrogado TEXT,    descripcion_proceso VARCHAR(2000),    destino_gasto VARCHAR(100),    dias_adicionados TEXT,    documento_proveedor VARCHAR(100),    domicilio_representante_legal VARCHAR(500),    duraci_n_contrato VARCHAR(100),    entidad_centralizada VARCHAR(100),    es_grupo TEXT,    es_pyme TEXT,    espostconflicto TEXT,    estado_bpin VARCHAR(100),    estado_contrato VARCHAR(100),    fecha_fin_contrato VARCHAR(100),    fecha_fin_ejecucion TEXT,    fecha_fin_liquidacion VARCHAR(100),    fecha_firma VARCHAR(100),    fecha_inicio_contrato VARCHAR(100),    fecha_inicio_ejecucion TEXT,    fecha_inicio_liquidacion VARCHAR(100),    fecha_notificaci_n_prorrogaci_n DATE,    g_nero_representante_legal VARCHAR(100),    habilita_pago_adelantado TEXT,    id_contrato VARCHAR(100),    identificaci_n_representante_legal VARCHAR(100),    justificacion_modalidad_contratacion VARCHAR(500),    liquidaci_n TEXT,    modalidad_contratacion VARCHAR(500),    nacionalidad_representante_legal VARCHAR(100),    nombre_banco VARCHAR(100),    nombre_entidad VARCHAR(500),    nombre_ordenador_gasto VARCHAR(100),    nombre_ordenador_pago TEXT,    nombre_representante_legal VARCHAR(500),    nombre_supervisor VARCHAR(100),    n_mero_cuenta VARCHAR(100),    n_mero_documento_ordenador_gasto VARCHAR(100),    n_mero_documento_ordenador_pago TEXT,    n_mero_documento_supervisor VARCHAR(100),    objeto_contrato VARCHAR(2000),    obligaciones_postconsumo TEXT,    obligaci_n_ambiental TEXT,    origen_recursos VARCHAR(100),    pilares_acuerdo TEXT,    presupuesto_general_nacion_pgn BIGINT,    proceso_compra VARCHAR(100),    proveedor_adjudicado VARCHAR(500),    puntos_acuerdo TEXT,    recursos_credito TEXT,    recursos_propios BIGINT,    recursos_propios_alcald_as_gobernaciones_resguardos_ind_genas BIGINT,    referencia_contrato VARCHAR(100),    reversion TEXT,    saldo_cdp BIGINT,    saldo_vigencia BIGINT,    sector VARCHAR(100),    sistema_general_participaciones BIGINT,    sistema_general_regal_as TEXT,    tipo_contrato VARCHAR(100),    tipo_cuenta VARCHAR(100),    tipo_documento_ordenador_gasto VARCHAR(100),    tipo_documento_ordenador_pago TEXT,    tipo_documento_supervisor VARCHAR(100),    tipo_identificaci_n_representante_legal VARCHAR(100),    tipodocproveedor VARCHAR(100),    ultima_actualizacion VARCHAR(100),    urlproceso VARCHAR(500),    valor_amortizado TEXT,    valor_contrato BIGINT,    valor_facturado TEXT,    valor_pagado TEXT,    valor_pago_adelantado TEXT,    valor_pendiente_amortizacion TEXT,    valor_pendiente_ejecucion BIGINT,    valor_pendiente_pago BIGINT
);


-- Tabla: datos_caracteristicos_proyectos
-- Generado: 2025-09-19 14:32:10
-- Campos: 20 + 5 sistema
CREATE TABLE IF NOT EXISTS datos_caracteristicos_proyectos (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    version INTEGER DEFAULT 1,    is_active BOOLEAN DEFAULT true,    anio TEXT,    bp VARCHAR(100),    bpin BIGINT,    clasificacion_fondo VARCHAR(100),    cod_producto TEXT,    cod_sector TEXT,    comuna VARCHAR(100),    nombre_actividad VARCHAR(2000),    nombre_area_funcional VARCHAR(2000),    nombre_centro_gestor VARCHAR(500),    nombre_dimension TEXT,    nombre_fondo VARCHAR(500),    nombre_linea_estrategica TEXT,    nombre_pospre VARCHAR(500),    nombre_programa VARCHAR(500),    nombre_proyecto VARCHAR(2000),    origen VARCHAR(100),    programa_presupuestal TEXT,    tipo_gasto VARCHAR(100),    validador_cuipo TEXT
);


-- Tabla: ejecucion_presupuestal
-- Generado: 2025-09-19 14:32:10
-- Campos: 11 + 5 sistema
CREATE TABLE IF NOT EXISTS ejecucion_presupuestal (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    version INTEGER DEFAULT 1,    is_active BOOLEAN DEFAULT true,    archivo_origen VARCHAR(500),    bpin BIGINT,    dataframe_origen VARCHAR(100),    ejecucion BIGINT,    pagos BIGINT,    periodo_corte VARCHAR(100),    ppto_disponible BIGINT,    saldos_cdp BIGINT,    total_acumul_obligac BIGINT,    total_acumulado_cdp BIGINT,    total_acumulado_rpc BIGINT
);


-- Tabla: movimientos_presupuestales
-- Generado: 2025-09-19 14:32:10
-- Campos: 12 + 5 sistema
CREATE TABLE IF NOT EXISTS movimientos_presupuestales (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    version INTEGER DEFAULT 1,    is_active BOOLEAN DEFAULT true,    adiciones TEXT,    aplazamiento TEXT,    archivo_origen VARCHAR(500),    bpin BIGINT,    contracreditos TEXT,    creditos TEXT,    dataframe_origen VARCHAR(100),    desaplazamiento TEXT,    periodo_corte VARCHAR(100),    ppto_inicial BIGINT,    ppto_modificado BIGINT,    reducciones TEXT
);


-- Tabla: emp_contratos_index
-- Generado: 2025-09-19 14:32:10
-- Campos: 5 + 5 sistema
CREATE TABLE IF NOT EXISTS emp_contratos_index (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    version INTEGER DEFAULT 1,    is_active BOOLEAN DEFAULT true,    banco VARCHAR(100),    fecha_extraccion TIMESTAMP,    id INTEGER,    referencia_contrato VARCHAR(100),    referencia_proceso VARCHAR(100)
);


-- Tabla: emp_procesos
-- Generado: 2025-09-19 14:32:10
-- Campos: 17 + 5 sistema
CREATE TABLE IF NOT EXISTS emp_procesos (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    version INTEGER DEFAULT 1,    is_active BOOLEAN DEFAULT true,    archivo_origen VARCHAR(100),    banco VARCHAR(100),    descripcion TEXT,    estado_proceso_secop VARCHAR(100),    fecha_procesamiento TIMESTAMP,    id INTEGER,    modalidad VARCHAR(100),    numero_contacto TEXT,    objeto VARCHAR(2000),    observaciones VARCHAR(2000),    planeado DATE,    referencia_contato VARCHAR(100),    referencia_proceso VARCHAR(100),    urlestadorealproceso VARCHAR(500),    urlproceso VARCHAR(500),    valor_plataforma TEXT,    valor_total TEXT
);


-- Tabla: emp_procesos_index
-- Generado: 2025-09-19 14:32:10
-- Campos: 6 + 5 sistema
CREATE TABLE IF NOT EXISTS emp_procesos_index (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    version INTEGER DEFAULT 1,    is_active BOOLEAN DEFAULT true,    banco VARCHAR(100),    fecha_procesamiento TIMESTAMP,    id INTEGER,    proceso_compra VARCHAR(100),    referencia_proceso VARCHAR(100),    urlestadorealproceso VARCHAR(500)
);


-- Tabla: emp_proyectos
-- Generado: 2025-09-19 14:32:10
-- Campos: 5 + 5 sistema
CREATE TABLE IF NOT EXISTS emp_proyectos (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    version INTEGER DEFAULT 1,    is_active BOOLEAN DEFAULT true,    banco VARCHAR(100),    bp VARCHAR(100),    bpin BIGINT,    fecha_procesamiento TIMESTAMP,    nombre_comercial VARCHAR(500)
);


-- Tabla: procesos_secop
-- Generado: 2025-09-19 14:32:10
-- Campos: 53 + 5 sistema
CREATE TABLE IF NOT EXISTS procesos_secop (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    version INTEGER DEFAULT 1,    is_active BOOLEAN DEFAULT true,    adjudicado TEXT,    ciudad_proveedor VARCHAR(100),    ciudad_unidad_contrataci_n VARCHAR(100),    codigo_principal_categoria VARCHAR(100),    codigoproveedor TEXT,    conteo_respuestas_ofertas INTEGER,    departamento_proveedor VARCHAR(100),    descripci_n_procedimiento VARCHAR(2000),    duracion INTEGER,    entidad VARCHAR(500),    entidad_centralizada VARCHAR(100),    estado_apertura_proceso VARCHAR(100),    estado_procedimiento VARCHAR(100),    estado_resumen VARCHAR(100),    fase VARCHAR(100),    fecha_adjudicacion VARCHAR(100),    fecha_apertura_efectiva VARCHAR(100),    fecha_apertura_respuesta VARCHAR(100),    fecha_publicacion_fase_borrador DATE,    fecha_publicacion_fase_planeacion_precalificacion TEXT,    fecha_publicacion_fase_seleccion VARCHAR(100),    fecha_publicacion_fase_seleccion_precalificacion TEXT,    fecha_publicacion_manifestacion_interes DATE,    fecha_publicacion_proceso VARCHAR(100),    fecha_recepcion_respuestas VARCHAR(100),    fecha_ultima_publicaci_n VARCHAR(100),    id_adjudicacion VARCHAR(100),    id_estado_procedimiento INTEGER,    id_proceso VARCHAR(100),    justificaci_n_modalidad_contrataci_n VARCHAR(500),    modalidad_contratacion VARCHAR(100),    nit_proveedor_adjudicado VARCHAR(100),    nombre_adjudicador VARCHAR(100),    nombre_procedimiento VARCHAR(500),    nombre_proveedor_adjudicado VARCHAR(500),    nombre_unidad_contrataci_n VARCHAR(500),    numero_lotes INTEGER,    pci INTEGER,    precio_base TEXT,    proceso_compra VARCHAR(100),    proveedores_invitacion_directa INTEGER,    proveedores_invitados DECIMAL(18,4),    proveedores_que_manifestaron_interes INTEGER,    proveedores_unicos_respuestas INTEGER,    referencia_proceso VARCHAR(500),    respuestas_al_procedimiento INTEGER,    respuestas_externas INTEGER,    subtipo_contrato TEXT,    tipo_contrato VARCHAR(100),    unidad_duracion VARCHAR(100),    urlproceso VARCHAR(500),    valor_total_adjudicacion TEXT,    visualizaciones_procedimiento INTEGER
);


-- Tabla: unidad_proyecto_infraestructura_equipamientos
-- Generado: 2025-09-19 14:32:10
-- Campos: 25 + 5 sistema
CREATE TABLE IF NOT EXISTS unidad_proyecto_infraestructura_equipamientos (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    version INTEGER DEFAULT 1,    is_active BOOLEAN DEFAULT true,    avance_f_sico_obra DECIMAL(18,4),    barrio_vereda VARCHAR(100),    bpin BIGINT,    clase_obra VARCHAR(100),    cod_fuente_financiamiento VARCHAR(100),    comuna_corregimiento VARCHAR(100),    dataframe VARCHAR(500),    descripcion_intervencion VARCHAR(2000),    direccion VARCHAR(500),    ejecucion_financiera_obra DECIMAL(18,4),    es_centro_gravedad VARCHAR(100),    estado_unidad_proyecto VARCHAR(100),    fecha_fin_planeado TEXT,    fecha_fin_real TEXT,    fecha_inicio_planeado TEXT,    fecha_inicio_real TEXT,    geom VARCHAR(500),    identificador VARCHAR(500),    nickname VARCHAR(500),    nickname_detalle VARCHAR(100),    pagos_realizados DECIMAL(18,4),    ppto_base DECIMAL(18,4),    subclase_obra VARCHAR(100),    tipo_intervencion VARCHAR(100),    usuarios_beneficiarios DECIMAL(18,4)
);


-- Tabla: unidad_proyecto_infraestructura_vial
-- Generado: 2025-09-19 14:32:10
-- Campos: 29 + 5 sistema
CREATE TABLE IF NOT EXISTS unidad_proyecto_infraestructura_vial (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    version INTEGER DEFAULT 1,    is_active BOOLEAN DEFAULT true,    avance_f_sico_obra DECIMAL(18,4),    barrio_vereda VARCHAR(100),    bpin BIGINT,    clase_obra VARCHAR(100),    cod_fuente_financiamiento VARCHAR(100),    comuna_corregimiento VARCHAR(100),    dataframe VARCHAR(100),    descripcion_intervencion VARCHAR(500),    direccion TEXT,    ejecucion_financiera_obra DECIMAL(18,4),    es_centro_gravedad VARCHAR(100),    estado_unidad_proyecto VARCHAR(100),    fecha_fin_planeado TEXT,    fecha_fin_real TEXT,    fecha_inicio_planeado TEXT,    fecha_inicio_real TEXT,    geom TEXT,    id_via VARCHAR(100),    identificador VARCHAR(100),    longitud_ejecutada DECIMAL(18,4),    longitud_proyectada DECIMAL(18,4),    nickname VARCHAR(500),    nickname_detalle TEXT,    pagos_realizados DECIMAL(18,4),    ppto_base DECIMAL(18,4),    subclase_obra TEXT,    tipo_intervencion VARCHAR(100),    unidad_medicion VARCHAR(100),    usuarios_beneficiarios DECIMAL(18,4)
);
