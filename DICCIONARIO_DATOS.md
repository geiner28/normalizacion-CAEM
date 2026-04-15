# Desarrollo de software A-GD-04
## Diccionario de Datos — VERSION 02
### ABR-2026

---

# Diccionario de Datos — Modelo Dimensional de Embargos CAEM

## Tabla de contenido

1. [GENERALIDADES](#1-generalidades)
2. [dim_departamentos](#2-dim_departamentos)
3. [dim_municipios](#3-dim_municipios)
4. [dim_entidades](#4-dim_entidades)
5. [dim_variantes](#5-dim_variantes)
6. [fact_oficios](#6-fact_oficios)
7. [dim_entidades_judiciales](#7-dim_entidades_judiciales)
8. [dim_entidades_coactivas](#8-dim_entidades_coactivas)
9. [RELACIONES ENTRE TABLAS](#9-relaciones-entre-tablas)
10. [ÍNDICES](#10-índices)
11. [VALORES VÁLIDOS Y DOMINIOS](#11-valores-válidos-y-dominios)

---

## Historial de versiones

| Fecha | Descripción | Autor |
|---|---|---|
| 2026-04-06 | Creación inicial del modelo dimensional (5 tablas) | Geiner Martínez |
| 2026-04-07 | Actualización post-deduplicación y validación de calidad | Geiner Martínez |
| 2026-04-09 | Documentación formal del diccionario de datos | Geiner Martínez |
| 2026-04-14 | v02 — Nuevos campos en fact_oficios (referencia, expediente, created_at, confirmed_at, processed_at). División del maestro de entidades en judiciales y coactivas | Geiner Martínez |

---

## 1. GENERALIDADES

| Atributo | Valor |
|---|---|
| Motor de base de datos | MySQL 8.x (InnoDB) |
| Charset | utf8mb4 |
| Esquema | ETL |
| Tipo de modelo | Dimensional estrella (star schema) |
| Tablas dimensionales | 4 (dim_departamentos, dim_municipios, dim_entidades, dim_variantes) + 2 tablas derivadas (dim_entidades_judiciales, dim_entidades_coactivas) |
| Tablas de hechos | 1 (fact_oficios) |
| Total de registros (hechos) | 916,425 |
| DDL de referencia | `datos/modelo_final/schema.sql` |

---

## 2. dim_departamentos

**Descripción:** Dimensión geográfica de nivel superior. Contiene los 32 departamentos de Colombia más Bogotá D.C. como distrito capital.

**Registros:** 33  
**Campos:** 2

| # | Campo | Tipo | Nulo | Clave | Descripción |
|---|---|---|---|---|---|
| 1 | `departamento_id` | INTEGER | NO | PK | Identificador único secuencial del departamento |
| 2 | `nombre` | VARCHAR(100) | NO | UNIQUE | Nombre oficial del departamento según DANE (ej: "Antioquia", "Bogotá, D.C.") |

**Ejemplo de registros:**

| departamento_id | nombre |
|---|---|
| 1 | Amazonas |
| 2 | Antioquia |
| 11 | Cundinamarca |

---

## 3. dim_municipios

**Descripción:** Dimensión geográfica de nivel inferior. Cada municipio pertenece a exactamente un departamento.

**Registros:** 1,104  
**Campos:** 3

| # | Campo | Tipo | Nulo | Clave | Descripción |
|---|---|---|---|---|---|
| 1 | `municipio_id` | INTEGER | NO | PK | Identificador único secuencial del municipio |
| 2 | `nombre` | VARCHAR(150) | NO | — | Nombre oficial del municipio según DANE (ej: "Medellín", "Cartagena de Indias") |
| 3 | `departamento_id` | INTEGER | NO | FK → dim_departamentos | Departamento al que pertenece el municipio |

**Ejemplo de registros:**

| municipio_id | nombre | departamento_id |
|---|---|---|
| 1 | Leticia | 1 |
| 2 | Puerto Nariño | 1 |
| 60 | Medellín | 2 |

---

## 4. dim_entidades

**Descripción:** Dimensión principal del modelo. Contiene las entidades remitentes de oficios de embargo, normalizadas y clasificadas por tipo, subtipo y ubicación geográfica. Resultado de reducir 38,289 variantes textuales a 8,548 entidades únicas.

**Registros:** 8,548  
**Campos:** 8

| # | Campo | Tipo | Nulo | Clave | Descripción |
|---|---|---|---|---|---|
| 1 | `entidad_id` | INTEGER | NO | PK | Identificador único secuencial de la entidad normalizada |
| 2 | `nombre_normalizado` | VARCHAR(500) | NO | — | Nombre canónico de la entidad tras normalización (ej: "JUZGADO 1 CIVIL MUNICIPAL DE MEDELLIN") |
| 3 | `tipo` | VARCHAR(50) | SÍ | IDX | Categoría principal de la entidad. Valores: JUZGADO, ALCALDIA, GOBERNACION, DIAN, DATT, SENA, SUPERINTENDENCIA, SECRETARIA, MINISTERIO, MUNICIPIO, CAR, TRIBUNAL, ESP, FISCALIA, CONTRALORIA, IDU, EMCALI, COLPENSIONES, POLICIA, CORTE, PERSONERIA, RAMA_JUDICIAL, UGPP, TRANSITO, OTRO |
| 4 | `subtipo` | VARCHAR(50) | SÍ | — | Subcategoría. Para JUZGADO: CIVIL_MUNICIPAL, PROMISCUO_MUNICIPAL, PEQUENAS_CAUSAS, CIVIL_CIRCUITO, LABORAL_CIRCUITO, ADMINISTRATIVO, EJECUCION_CIVIL, FAMILIA, etc. Para otros tipos puede ser vacío |
| 5 | `municipio_id` | INTEGER | SÍ | FK → dim_municipios | Municipio de jurisdicción. NULL para entidades nacionales (DIAN, SENA, Superintendencias) o cuando no se pudo extraer del nombre |
| 6 | `departamento_id` | INTEGER | SÍ | FK → dim_departamentos | Departamento de jurisdicción. NULL cuando no se pudo determinar ubicación geográfica |
| 7 | `total_registros` | INTEGER | NO | — | Cantidad de oficios de embargo asociados a esta entidad en la tabla de hechos |
| 8 | `num_variantes` | INTEGER | NO | — | Cantidad de variantes textuales originales que fueron agrupadas en esta entidad |

**Ejemplo de registros:**

| entidad_id | nombre_normalizado | tipo | subtipo | municipio_id | departamento_id | total_registros | num_variantes |
|---|---|---|---|---|---|---|---|
| 1 | DEPARTAMENTO ADMINISTRATIVO DE TRANSITO Y TRANSPORTE DE CARTAGENA - DATT | DATT | TRANSITO | 167 | 6 | 109,036 | 167 |
| 2 | DIAN | DIAN | | | | 38,027 | 3 |
| 3 | JUZGADO 1 CIVIL MUNICIPAL DE MEDELLIN | JUZGADO | CIVIL_MUNICIPAL | 60 | 2 | 5,234 | 12 |

**Notas:**
- Las Gobernaciones tienen `municipio_id = NULL` por diseño (su jurisdicción es departamental, no municipal).
- El campo `total_registros` es denormalizado para facilitar consultas de ranking sin JOINs.

---

## 5. dim_variantes

**Descripción:** Tabla puente que mapea cada variante textual original del campo `entidad_remitente` al ID de la entidad normalizada. Permite la trazabilidad desde el dato crudo hasta la entidad canónica.

**Registros:** 38,289  
**Campos:** 5

| # | Campo | Tipo | Nulo | Clave | Descripción |
|---|---|---|---|---|---|
| 1 | `variante_id` | INTEGER | NO | PK | Identificador único secuencial de la variante |
| 2 | `entidad_id` | INTEGER | NO | FK → dim_entidades | Entidad normalizada a la que pertenece esta variante |
| 3 | `nombre_normalizado` | VARCHAR(500) | SÍ | — | Nombre normalizado de la entidad (denormalizado para consulta rápida) |
| 4 | `variante_original` | VARCHAR(500) | NO | — | Texto exacto como aparece en los datos fuente de embargos |
| 5 | `conteo` | INTEGER | NO | — | Número de veces que esta variante exacta aparece en los datos originales |

**Ejemplo de registros:**

| variante_id | entidad_id | nombre_normalizado | variante_original | conteo |
|---|---|---|---|---|
| 1 | 1 | DATT CARTAGENA | DEPARTAMENTO ADMINISTRATIVO DE TRANSITO Y TRANSPORTE DE CARTAGENA - DATT | 101,854 |
| 2 | 1 | DATT CARTAGENA | DEPARTAMENTO ADMINISTRATIVO DE TRANSITO Y TRANSPORTE DE CARTAGENA DATT | 2,613 |
| 3 | 2 | DIAN | DIAN | 37,500 |

**Uso principal:** Cuando llega un nuevo oficio con un texto de entidad remitente, se busca en `variante_original` para obtener el `entidad_id` normalizado.

---

## 6. fact_oficios

**Descripción:** Tabla de hechos central del modelo. Cada registro representa un oficio de embargo único, con claves foráneas a las dimensiones geográficas y de entidades.

**Registros:** 916,425 (post-deduplicación)  
**Campos:** 25

| # | Campo | Tipo | Nulo | Clave | Descripción |
|---|---|---|---|---|---|
| 1 | `oficio_id` | VARCHAR(20) | NO | PK | Identificador único del oficio de embargo (corresponde al ID original del sistema fuente) |
| 2 | `entidad_remitente_id` | INTEGER | SÍ | FK → dim_entidades | Entidad que emitió el oficio de embargo |
| 3 | `entidad_bancaria_id` | INTEGER | SÍ | — | Entidad bancaria destinataria del embargo (1=CITIBANK, 2=SANTANDER, 3=COOPCENTRAL, 4=COLPATRIA, 5=FALABELLA) |
| 4 | `estado` | VARCHAR(30) | SÍ | IDX | Estado de procesamiento del oficio. Valores: PROCESADO, RECONFIRMADO, CONFIRMADO, PROCESADO_CON_ERRORES, EN_PROCESO |
| 5 | `numero_oficio` | VARCHAR(250) | SÍ | — | Número de referencia del oficio asignado por la entidad remitente |
| 6 | `fecha_oficio` | DATE | SÍ | IDX | Fecha en que fue emitido el oficio de embargo |
| 7 | `fecha_recepcion` | DATE | SÍ | — | Fecha en que fue recibido el oficio por la entidad bancaria |
| 8 | `titulo_embargo` | VARCHAR(50) | SÍ | — | Tipo de embargo (ej: "EMBARGO", "RETENCION") |
| 9 | `titulo_orden` | VARCHAR(50) | SÍ | — | Tipo de orden judicial (ej: "OFICIO", "ORDEN") |
| 10 | `monto` | DECIMAL(20,2) | SÍ | — | Monto del embargo en pesos colombianos |
| 11 | `monto_a_embargar` | DECIMAL(20,2) | SÍ | — | Monto específico a embargar (puede diferir del monto total) |
| 12 | `nombre_demandado` | VARCHAR(300) | SÍ | — | Nombre completo de la persona o entidad demandada |
| 13 | `id_demandado` | VARCHAR(50) | SÍ | — | Número de identificación del demandado (CC, NIT, CE, etc.) |
| 14 | `tipo_id_demandado` | VARCHAR(30) | SÍ | — | Tipo de documento del demandado (CC, NIT, CE, PASAPORTE, TI) |
| 15 | `direccion_remitente` | VARCHAR(500) | SÍ | — | Dirección física de la entidad remitente |
| 16 | `correo_remitente` | VARCHAR(200) | SÍ | — | Correo electrónico de contacto del remitente |
| 17 | `nombre_funcionario` | VARCHAR(200) | SÍ | — | Nombre del funcionario que firma el oficio |
| 18 | `municipio_id` | INTEGER | SÍ | FK → dim_municipios | Municipio de origen del oficio (derivado de la entidad remitente o del campo ciudad) |
| 19 | `departamento_id` | INTEGER | SÍ | FK → dim_departamentos | Departamento de origen del oficio |
| 20 | `fuente_ubicacion` | VARCHAR(30) | SÍ | — | Cómo se determinó la ubicación: "entidad" (del nombre de la entidad) o "ciudad" (del campo ciudad del embargo original) |
| 21 | `referencia` | VARCHAR(200) | SÍ | — | Número de referencia interna del embargo en el sistema fuente (campo `referencia` de la tabla embargos) |
| 22 | `expediente` | VARCHAR(200) | SÍ | — | Número de expediente judicial asociado al demandado (campo `expediente` de la tabla demandado) |
| 23 | `created_at` | DATETIME | SÍ | — | Fecha y hora de creación del registro de embargo en el sistema fuente (campo `create_at` de la tabla embargos) |
| 24 | `confirmed_at` | DATETIME | SÍ | — | Fecha y hora de confirmación del embargo (campo `confirmed_at` de la tabla embargos) |
| 25 | `processed_at` | DATETIME | SÍ | — | Fecha y hora de procesamiento del embargo (campo `processed_at` de la tabla embargos) |

**Notas:**
- Los campos `monto` y `monto_a_embargar` usan DECIMAL(20,2) para precisión monetaria.
- Los campos `referencia`, `expediente`, `created_at`, `confirmed_at` y `processed_at` fueron incorporados en la versión 02 para trazabilidad completa con las tablas originales del sistema fuente.
- `referencia` proviene directamente de la tabla `embargos` (col 27 del CSV fuente).
- `expediente` proviene de la tabla `demandado` (col 5 del CSV fuente), vinculado por `embargo_id`.
- `created_at`, `confirmed_at` y `processed_at` provienen de la tabla `embargos` (cols 6, 3, 25 respectivamente).

---

## 7. dim_entidades_judiciales

**Descripción:** Vista derivada del maestro de entidades que contiene únicamente las entidades pertenecientes a la Rama Judicial. Generada por el script `split_entidades.py`. Permite cruce con el directorio oficial SIERJU para validar nombres reales y correos electrónicos institucionales.

**Archivo:** `datos/modelo_final/dim_entidades_judiciales.csv`  
**Registros:** ~6,400 (estimado: JUZGADO + TRIBUNAL + CORTE + RAMA_JUDICIAL + FISCALIA + OFICINA_APOYO + CENTRO_SERVICIOS + DIRECCION_EJECUTIVA)  
**Campos:** 8

| # | Campo | Tipo | Nulo | Descripción |
|---|---|---|---|---|
| 1 | `entidad_id` | INTEGER | NO | Identificador interno de la entidad (PK en dim_entidades) |
| 2 | `nombre_real` | VARCHAR(500) | SÍ | Nombre oficial del despacho según el directorio SIERJU de la Rama Judicial. Vacío si no se encontró coincidencia |
| 3 | `nombre_extraido` | VARCHAR(500) | NO | Nombre normalizado extraído de nuestra base de datos (campo `nombre_normalizado` de dim_entidades) |
| 4 | `ciudad` | VARCHAR(150) | SÍ | Municipio/ciudad de jurisdicción del despacho (resuelto desde `municipio_id`) |
| 5 | `email_real` | VARCHAR(200) | SÍ | Correo electrónico institucional según directorio SIERJU (formato: `{codigo_despacho}@cendoj.ramajudicial.gov.co`). Vacío si no se encontró coincidencia |
| 6 | `email_extraido` | VARCHAR(200) | SÍ | Correo electrónico más frecuente asociado a esta entidad en los oficios de embargo de nuestra BD |
| 7 | `numero_despacho` | VARCHAR(10) | SÍ | Número ordinal del despacho extraído del nombre (ej: "3" de "JUZGADO 3 CIVIL MUNICIPAL") |
| 8 | `total_registros` | INTEGER | NO | Cantidad de oficios de embargo asociados a esta entidad |

**Tipos de entidad incluidos:** JUZGADO, TRIBUNAL, CORTE, RAMA_JUDICIAL, FISCALIA, OFICINA_APOYO, CENTRO_SERVICIOS, DIRECCION_EJECUTIVA

**Fuente de validación:** [Directorio Judicial SIERJU](https://directoriojudicial.ramajudicial.gov.co) — Exportar Excel y colocar como `datos/fuentes/directorio_sierju.xlsx`

**Ejemplo de registros:**

| entidad_id | nombre_real | nombre_extraido | ciudad | email_real | email_extraido | numero_despacho | total_registros |
|---|---|---|---|---|---|---|---|
| 3 | Juzgado Primero Civil Municipal de Medellín | JUZGADO 1 CIVIL MUNICIPAL DE MEDELLIN | Medellín | 050013103001@cendoj.ramajudicial.gov.co | j01cmlmed@cendoj.ramajudicial.gov.co | 1 | 5,234 |
| 45 | | JUZGADO 2 LABORAL DEL CIRCUITO DE BOGOTA | Bogotá D.C. | | j02labctobog@notificacionesrj.gov.co | 2 | 1,890 |

---

## 8. dim_entidades_coactivas

**Descripción:** Vista derivada del maestro de entidades que contiene las entidades coactivas y administrativas (no judiciales). Incluye entidades como DIAN, DATT, alcaldías, gobernaciones, secretarías, etc.

**Archivo:** `datos/modelo_final/dim_entidades_coactivas.csv`  
**Registros:** ~2,100 (estimado: todos los tipos no judiciales)  
**Campos:** 8

| # | Campo | Tipo | Nulo | Descripción |
|---|---|---|---|---|
| 1 | `entidad_id` | INTEGER | NO | Identificador interno de la entidad (PK en dim_entidades) |
| 2 | `nombre_real` | VARCHAR(500) | SÍ | Nombre oficial según directorio externo. Vacío: requiere fuente de validación de entidades coactivas |
| 3 | `nombre_extraido` | VARCHAR(500) | NO | Nombre normalizado extraído de nuestra base de datos |
| 4 | `ciudad` | VARCHAR(150) | SÍ | Municipio/ciudad de la entidad |
| 5 | `email_real` | VARCHAR(200) | SÍ | Correo electrónico oficial según directorio externo. Vacío: requiere fuente de validación |
| 6 | `email_extraido` | VARCHAR(200) | SÍ | Correo electrónico más frecuente asociado a esta entidad en los oficios de nuestra BD |
| 7 | `nit` | VARCHAR(20) | SÍ | Número de Identificación Tributaria de la entidad (extraído del nombre si está disponible) |
| 8 | `total_registros` | INTEGER | NO | Cantidad de oficios de embargo asociados a esta entidad |

**Tipos de entidad incluidos:** DIAN, DATT, ALCALDIA, GOBERNACION, SECRETARIA, UGPP, SENA, CAR, SUPERINTENDENCIA, MINISTERIO, MUNICIPIO, ESP, CONTRALORIA, TRANSITO, INSTITUTO_MOVILIDAD, IDU, COLPENSIONES, POLICIA, EMCALI, PERSONERIA, OTRO

**Ejemplo de registros:**

| entidad_id | nombre_real | nombre_extraido | ciudad | email_real | email_extraido | nit | total_registros |
|---|---|---|---|---|---|---|---|
| 1 | | DATT CARTAGENA | Cartagena de Indias | | datt@transitocartagena.gov.co | | 109,036 |
| 2 | | DIAN | | | notificaciones@dian.gov.co | | 38,027 |

---

## 9. RELACIONES ENTRE TABLAS

```
dim_departamentos (PK: departamento_id)
       │
       ├──── dim_municipios.departamento_id (FK, NOT NULL)
       ├──── dim_entidades.departamento_id (FK, nullable)
       └──── fact_oficios.departamento_id (FK, nullable)

dim_municipios (PK: municipio_id)
       │
       ├──── dim_entidades.municipio_id (FK, nullable)
       └──── fact_oficios.municipio_id (FK, nullable)

dim_entidades (PK: entidad_id)
       │
       ├──── dim_variantes.entidad_id (FK, NOT NULL)
       ├──── fact_oficios.entidad_remitente_id (FK, nullable)
       ├──── dim_entidades_judiciales.entidad_id (subset, tipos judiciales)
       └──── dim_entidades_coactivas.entidad_id (subset, tipos coactivos)
```

### Cardinalidad:

| Relación | Tipo | Descripción |
|---|---|---|
| dim_departamentos → dim_municipios | 1:N | Un departamento tiene muchos municipios |
| dim_departamentos → dim_entidades | 1:N | Un departamento tiene muchas entidades |
| dim_municipios → dim_entidades | 1:N | Un municipio tiene muchas entidades |
| dim_entidades → dim_variantes | 1:N | Una entidad tiene muchas variantes textuales |
| dim_entidades → fact_oficios | 1:N | Una entidad emite muchos oficios |
| dim_municipios → fact_oficios | 1:N | Un municipio origina muchos oficios |
| dim_departamentos → fact_oficios | 1:N | Un departamento origina muchos oficios |
| dim_entidades → dim_entidades_judiciales | 1:1 | Subconjunto: entidades de la Rama Judicial |
| dim_entidades → dim_entidades_coactivas | 1:1 | Subconjunto: entidades coactivas y administrativas |

---

## 10. ÍNDICES

| Tabla | Índice | Campos | Propósito |
|---|---|---|---|
| dim_departamentos | PK | `departamento_id` | Clave primaria |
| dim_departamentos | UNIQUE | `nombre` | Evitar departamentos duplicados |
| dim_municipios | PK | `municipio_id` | Clave primaria |
| dim_municipios | idx_municipios_depto | `departamento_id` | Consultas por departamento |
| dim_entidades | PK | `entidad_id` | Clave primaria |
| dim_entidades | idx_entidades_tipo | `tipo` | Filtro por tipo de entidad |
| dim_entidades | idx_entidades_muni | `municipio_id` | Consultas por municipio |
| dim_entidades | idx_entidades_depto | `departamento_id` | Consultas por departamento |
| dim_variantes | PK | `variante_id` | Clave primaria |
| dim_variantes | idx_variantes_entidad | `entidad_id` | JOIN con dim_entidades |
| fact_oficios | PK | `oficio_id` | Clave primaria |
| fact_oficios | idx_oficios_entidad | `entidad_remitente_id` | Consultas por entidad |
| fact_oficios | idx_oficios_estado | `estado` | Filtro por estado de procesamiento |
| fact_oficios | idx_oficios_muni | `municipio_id` | Consultas geográficas |
| fact_oficios | idx_oficios_depto | `departamento_id` | Consultas geográficas |
| fact_oficios | idx_oficios_fecha | `fecha_oficio` | Consultas temporales |

---

## 11. VALORES VÁLIDOS Y DOMINIOS

### 11.1 `dim_entidades.tipo` — Tipos de entidad (25 valores)

| Valor | Descripción | Cantidad |
|---|---|---|
| JUZGADO | Juzgados de todas las ramas y especialidades | 6,253 |
| OTRO | Entidades no clasificadas en las categorías anteriores | 873 |
| ALCALDIA | Alcaldías municipales y distritales | 330 |
| SECRETARIA | Secretarías de gobierno (tránsito, hacienda, etc.) | 208 |
| MUNICIPIO | Municipios como entidad administrativa | 153 |
| MINISTERIO | Ministerios del gobierno nacional | 65 |
| SUPERINTENDENCIA | Superintendencias nacionales | 62 |
| RAMA_JUDICIAL | Direcciones y seccionales de la Rama Judicial | 56 |
| CAR | Corporaciones Autónomas Regionales | 52 |
| TRIBUNAL | Tribunales Superiores y Administrativos | 39 |
| GOBERNACION | Gobernaciones departamentales | 34 |
| ESP | Empresas de Servicios Públicos | 25 |
| CONTRALORIA | Contralorías departamentales y municipales | 25 |
| TRANSITO | Secretarías y oficinas de tránsito | 20 |
| SENA | Servicio Nacional de Aprendizaje | 18 |
| DIAN | Dirección de Impuestos y Aduanas Nacionales | 11 |
| IDU | Instituto de Desarrollo Urbano | 6 |
| FISCALIA | Fiscalía General de la Nación | 5 |
| POLICIA | Policía Nacional | 4 |
| CORTE | Corte Suprema y Corte Constitucional | 3 |
| COLPENSIONES | Administradora Colombiana de Pensiones | 3 |
| PERSONERIA | Personerías municipales | 3 |
| UGPP | Unidad de Gestión Pensional y Parafiscales | 2 |
| DATT | Departamento Administrativo de Tránsito y Transporte | 1 |
| EMCALI | Empresas Municipales de Cali | 1 |

### 11.2 `fact_oficios.estado` — Estados de procesamiento

| Valor | Descripción |
|---|---|
| PROCESADO | Oficio procesado exitosamente |
| RECONFIRMADO | Oficio reconfirmado tras revisión |
| CONFIRMADO | Oficio confirmado pero pendiente de procesamiento |
| PROCESADO_CON_ERRORES | Procesado con observaciones o errores menores |
| EN_PROCESO | En trámite de procesamiento |

### 11.3 `fact_oficios.entidad_bancaria_id` — Entidades bancarias

| ID | Nombre |
|---|---|
| 1 | CITIBANK |
| 2 | SANTANDER |
| 3 | COOPCENTRAL |
| 4 | COLPATRIA |
| 5 | FALABELLA |

### 11.4 `fact_oficios.tipo_id_demandado` — Tipos de documento

| Valor | Descripción |
|---|---|
| CC | Cédula de Ciudadanía |
| NIT | Número de Identificación Tributaria |
| CE | Cédula de Extranjería |
| PASAPORTE | Pasaporte |
| TI | Tarjeta de Identidad |
