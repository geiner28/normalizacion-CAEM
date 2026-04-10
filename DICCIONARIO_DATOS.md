# Desarrollo de software A-GD-04
## Diccionario de Datos — VERSION 01
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
7. [RELACIONES ENTRE TABLAS](#7-relaciones-entre-tablas)
8. [ÍNDICES](#8-índices)
9. [VALORES VÁLIDOS Y DOMINIOS](#9-valores-válidos-y-dominios)

---

## Historial de versiones

| Fecha | Descripción | Autor |
|---|---|---|
| 2026-04-06 | Creación inicial del modelo dimensional (5 tablas) | Geiner Martínez |
| 2026-04-07 | Actualización post-deduplicación y validación de calidad | Geiner Martínez |
| 2026-04-09 | Documentación formal del diccionario de datos | Geiner Martínez |

---

## 1. GENERALIDADES

| Atributo | Valor |
|---|---|
| Motor de base de datos | MySQL 8.x (InnoDB) |
| Charset | utf8mb4 |
| Esquema | ETL |
| Tipo de modelo | Dimensional estrella (star schema) |
| Tablas dimensionales | 4 (dim_departamentos, dim_municipios, dim_entidades, dim_variantes) |
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
**Campos:** 19

| # | Campo | Tipo | Nulo | Clave | Descripción |
|---|---|---|---|---|---|
| 1 | `oficio_id` | VARCHAR(20) | NO | PK | Identificador único del oficio de embargo (corresponde al ID original del sistema fuente) |
| 2 | `entidad_remitente_id` | INTEGER | SÍ | FK → dim_entidades | Entidad que emitió el oficio de embargo |
| 3 | `entidad_bancaria_id` | INTEGER | SÍ | — | Entidad bancaria destinataria del embargo (1=CITIBANK, 2=SANTANDER, 3=COOPCENTRAL, 4=COLPATRIA, 5=FALABELLA) |
| 4 | `estado` | VARCHAR(30) | SÍ | IDX | Estado de procesamiento del oficio. Valores: PROCESADO, RECONFIRMADO, CONFIRMADO, PROCESADO_CON_ERRORES, EN_PROCESO |
| 5 | `numero_oficio` | VARCHAR(100) | SÍ | — | Número de referencia del oficio asignado por la entidad remitente |
| 6 | `fecha_oficio` | DATE | SÍ | IDX | Fecha en que fue emitido el oficio de embargo |
| 7 | `fecha_recepcion` | DATE | SÍ | — | Fecha en que fue recibido el oficio por la entidad bancaria |
| 8 | `titulo_embargo` | VARCHAR(50) | SÍ | — | Tipo de embargo (ej: "EMBARGO", "RETENCION") |
| 9 | `titulo_orden` | VARCHAR(50) | SÍ | — | Tipo de orden judicial (ej: "OFICIO", "ORDEN") |
| 10 | `monto` | DECIMAL(18,2) | SÍ | — | Monto del embargo en pesos colombianos |
| 11 | `monto_a_embargar` | DECIMAL(18,2) | SÍ | — | Monto específico a embargar (puede diferir del monto total) |
| 12 | `nombre_demandado` | VARCHAR(300) | SÍ | — | Nombre completo de la persona o entidad demandada |
| 13 | `id_demandado` | VARCHAR(30) | SÍ | — | Número de identificación del demandado (CC, NIT, CE, etc.) |
| 14 | `tipo_id_demandado` | VARCHAR(20) | SÍ | — | Tipo de documento del demandado (CC, NIT, CE, PASAPORTE, TI) |
| 15 | `direccion_remitente` | VARCHAR(500) | SÍ | — | Dirección física de la entidad remitente |
| 16 | `correo_remitente` | VARCHAR(200) | SÍ | — | Correo electrónico de contacto del remitente |
| 17 | `nombre_funcionario` | VARCHAR(200) | SÍ | — | Nombre del funcionario que firma el oficio |
| 18 | `municipio_id` | INTEGER | SÍ | FK → dim_municipios | Municipio de origen del oficio (derivado de la entidad remitente o del campo ciudad) |
| 19 | `departamento_id` | INTEGER | SÍ | FK → dim_departamentos | Departamento de origen del oficio |

**Notas:**
- El campo `fuente_ubicacion` (VARCHAR(30)) indica cómo se determinó la ubicación: `"entidad"` (del nombre de la entidad) o `"ciudad"` (del campo ciudad del embargo original). Este campo está presente en el CSV pero puede omitirse en la DDL según necesidad.
- Los campos `monto` y `monto_a_embargar` usan DECIMAL(18,2) para precisión monetaria.

---

## 7. RELACIONES ENTRE TABLAS

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
       └──── fact_oficios.entidad_remitente_id (FK, nullable)
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

---

## 8. ÍNDICES

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

## 9. VALORES VÁLIDOS Y DOMINIOS

### 9.1 `dim_entidades.tipo` — Tipos de entidad (25 valores)

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

### 9.2 `fact_oficios.estado` — Estados de procesamiento

| Valor | Descripción |
|---|---|
| PROCESADO | Oficio procesado exitosamente |
| RECONFIRMADO | Oficio reconfirmado tras revisión |
| CONFIRMADO | Oficio confirmado pero pendiente de procesamiento |
| PROCESADO_CON_ERRORES | Procesado con observaciones o errores menores |
| EN_PROCESO | En trámite de procesamiento |

### 9.3 `fact_oficios.entidad_bancaria_id` — Entidades bancarias

| ID | Nombre |
|---|---|
| 1 | CITIBANK |
| 2 | SANTANDER |
| 3 | COOPCENTRAL |
| 4 | COLPATRIA |
| 5 | FALABELLA |

### 9.4 `fact_oficios.tipo_id_demandado` — Tipos de documento

| Valor | Descripción |
|---|---|
| CC | Cédula de Ciudadanía |
| NIT | Número de Identificación Tributaria |
| CE | Cédula de Extranjería |
| PASAPORTE | Pasaporte |
| TI | Tarjeta de Identidad |
