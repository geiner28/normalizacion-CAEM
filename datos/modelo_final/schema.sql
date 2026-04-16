-- ============================================================
-- SCHEMA: Modelo de datos de embargos/oficios (v3 - enriquecido)
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_departamentos (
    departamento_id  INTEGER PRIMARY KEY,
    nombre           VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_municipios (
    municipio_id     INTEGER PRIMARY KEY,
    nombre           VARCHAR(150) NOT NULL,
    departamento_id  INTEGER NOT NULL,
    FOREIGN KEY (departamento_id) REFERENCES dim_departamentos(departamento_id)
);
CREATE INDEX idx_municipios_depto ON dim_municipios(departamento_id);

CREATE TABLE IF NOT EXISTS dim_entidades (
    entidad_id          INTEGER PRIMARY KEY,
    nombre_normalizado  VARCHAR(500) NOT NULL,
    nombre_real         VARCHAR(500),
    tipo                VARCHAR(50),
    subtipo             VARCHAR(50),
    categoria           VARCHAR(20),
    nit                 VARCHAR(50),
    cod_institucion     VARCHAR(50),
    email               VARCHAR(200),
    direccion           VARCHAR(500),
    telefono            VARCHAR(100),
    ciudad              VARCHAR(150),
    municipio_id        INTEGER,
    departamento_id     INTEGER,
    orden               VARCHAR(50),
    sector              VARCHAR(100),
    naturaleza_juridica VARCHAR(100),
    estado              VARCHAR(30),
    representante       VARCHAR(200),
    total_registros     INTEGER DEFAULT 0,
    num_variantes       INTEGER DEFAULT 0,
    FOREIGN KEY (municipio_id)    REFERENCES dim_municipios(municipio_id),
    FOREIGN KEY (departamento_id) REFERENCES dim_departamentos(departamento_id)
);
CREATE INDEX idx_entidades_tipo  ON dim_entidades(tipo);
CREATE INDEX idx_entidades_cat   ON dim_entidades(categoria);
CREATE INDEX idx_entidades_muni  ON dim_entidades(municipio_id);
CREATE INDEX idx_entidades_depto ON dim_entidades(departamento_id);

CREATE TABLE IF NOT EXISTS dim_entidades_coactivas (
    entidad_id          INTEGER PRIMARY KEY,
    nombre_extraido     VARCHAR(500) NOT NULL,
    nombre_real         VARCHAR(500),
    ciudad              VARCHAR(150),
    email_extraido      VARCHAR(200),
    email_real          VARCHAR(200),
    nit                 VARCHAR(50),
    cod_institucion     VARCHAR(50),
    orden               VARCHAR(50),
    sector              VARCHAR(100),
    naturaleza_juridica VARCHAR(100),
    tipo_institucion    VARCHAR(100),
    direccion           VARCHAR(500),
    telefono            VARCHAR(100),
    pagina_web          VARCHAR(200),
    estado              VARCHAR(30),
    representante       VARCHAR(200),
    cargo_representante VARCHAR(200),
    total_registros     INTEGER DEFAULT 0,
    FOREIGN KEY (entidad_id) REFERENCES dim_entidades(entidad_id)
);

CREATE TABLE IF NOT EXISTS dim_entidades_judiciales (
    entidad_id          INTEGER PRIMARY KEY,
    nombre_extraido     VARCHAR(500) NOT NULL,
    nombre_real         VARCHAR(500),
    ciudad              VARCHAR(150),
    email_extraido      VARCHAR(200),
    email_real          VARCHAR(200),
    codigo_despacho     VARCHAR(20),
    numero_despacho     VARCHAR(20),
    jurisdiccion        VARCHAR(50),
    distrito            VARCHAR(100),
    circuito            VARCHAR(100),
    juez                VARCHAR(200),
    direccion           VARCHAR(500),
    telefono            VARCHAR(100),
    area                VARCHAR(50),
    total_registros     INTEGER DEFAULT 0,
    FOREIGN KEY (entidad_id) REFERENCES dim_entidades(entidad_id)
);

CREATE TABLE IF NOT EXISTS dim_variantes (
    variante_id         INTEGER PRIMARY KEY,
    entidad_id          INTEGER NOT NULL,
    nombre_normalizado  VARCHAR(500),
    variante_original   VARCHAR(500) NOT NULL,
    conteo              INTEGER DEFAULT 0,
    FOREIGN KEY (entidad_id) REFERENCES dim_entidades(entidad_id)
);
CREATE INDEX idx_variantes_entidad ON dim_variantes(entidad_id);

CREATE TABLE IF NOT EXISTS fact_oficios (
    oficio_id              VARCHAR(20) PRIMARY KEY,
    entidad_remitente_id   INTEGER,
    entidad_bancaria_id    INTEGER,
    estado                 VARCHAR(30),
    numero_oficio          VARCHAR(100),
    fecha_oficio           DATE,
    fecha_recepcion        DATE,
    titulo_embargo         VARCHAR(50),
    titulo_orden           VARCHAR(50),
    monto                  DECIMAL(18,2),
    monto_a_embargar       DECIMAL(18,2),
    nombre_demandado       VARCHAR(300),
    id_demandado           VARCHAR(30),
    tipo_id_demandado      VARCHAR(20),
    direccion_remitente    VARCHAR(500),
    correo_remitente       VARCHAR(200),
    nombre_funcionario     VARCHAR(200),
    municipio_id           INTEGER,
    departamento_id        INTEGER,
    fuente_ubicacion       VARCHAR(30),
    referencia             VARCHAR(200),
    expediente             VARCHAR(200),
    created_at             DATETIME,
    confirmed_at           DATETIME,
    processed_at           DATETIME,
    FOREIGN KEY (entidad_remitente_id) REFERENCES dim_entidades(entidad_id),
    FOREIGN KEY (municipio_id)         REFERENCES dim_municipios(municipio_id),
    FOREIGN KEY (departamento_id)      REFERENCES dim_departamentos(departamento_id)
);
CREATE INDEX idx_oficios_entidad ON fact_oficios(entidad_remitente_id);
CREATE INDEX idx_oficios_estado  ON fact_oficios(estado);
CREATE INDEX idx_oficios_muni    ON fact_oficios(municipio_id);
CREATE INDEX idx_oficios_depto   ON fact_oficios(departamento_id);
CREATE INDEX idx_oficios_fecha   ON fact_oficios(fecha_oficio);

-- ============================================================
