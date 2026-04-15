# Desarrollo de software A-GD-04
## Metodología, Objetivos y Tareas Ejecutadas — VERSION 02
### ABR-2026

---

# Metodología, Objetivos y Tareas Ejecutadas — Normalización CAEM

## Tabla de contenido

1. [OBJETIVO GENERAL](#1-objetivo-general)
2. [OBJETIVOS ESPECÍFICOS](#2-objetivos-específicos)
3. [METODOLOGÍA](#3-metodología)
4. [TAREAS EJECUTADAS](#4-tareas-ejecutadas)
5. [RESULTADOS OBTENIDOS](#5-resultados-obtenidos)
6. [LIMITACIONES IDENTIFICADAS](#6-limitaciones-identificadas)

---

## Historial de versiones

| Fecha | Descripción | Autor |
|---|---|---|
| 2026-04-09 | Documentación inicial de metodología y tareas | Geiner Martínez |
| 2026-04-14 | v02 — Inclusión de 5 nuevos campos en pipeline, reestructuración del maestro de entidades (judiciales/coactivas), actualización del modelo en línea | Geiner Martínez |

---

## 1. OBJETIVO GENERAL

Diseñar e implementar un pipeline ETL que transforme los datos crudos de oficios de embargo del sistema CAEM (~929,690 registros) en un modelo dimensional estrella normalizado, resolviendo la problemática de duplicación y ambigüedad en los nombres de entidades remitentes (38,289 variantes textuales) para producir un maestro de 8,548 entidades únicas, clasificadas por tipo jurisdiccional y geolocalizadas a nivel municipal y departamental, con integridad referencial completa y cargado en base de datos MySQL.

---

## 2. OBJETIVOS ESPECÍFICOS

| # | Objetivo | Estado |
|---|---|---|
| 1 | Normalizar las cadenas textuales de entidades remitentes (limpieza, unificación de ordinales, eliminación de caracteres especiales) | ✅ Completado |
| 2 | Clasificar cada entidad en tipo y subtipo según taxonomía institucional colombiana (JUZGADO, ALCALDIA, GOBERNACION, DIAN, etc.) | ✅ Completado |
| 3 | Extraer la ubicación geográfica (municipio y departamento) del nombre de la entidad, usando la base DANE de 1,104 municipios como referencia | ✅ Completado |
| 4 | Agrupar variantes textuales que refieren a la misma entidad real mediante fingerprinting y distancia de Levenshtein | ✅ Completado |
| 5 | Construir un modelo dimensional estrella con tablas de dimensiones (departamentos, municipios, entidades, variantes) y hechos (oficios) | ✅ Completado |
| 6 | Validar la integridad referencial del modelo y comparar conteos con la base de datos original | ✅ Completado |
| 7 | Cargar el modelo final a MySQL y ejecutar deduplicación de registros | ✅ Completado |
| 8 | Incluir los campos referencia, expediente, created_at, confirmed_at y processed_at en el pipeline de normalización y modelo dimensional | ✅ Completado (v02) |
| 9 | Reestructurar el maestro de entidades dividiéndolo en Entidades Judiciales y Entidades Coactivas con esquemas diferenciados | ✅ Completado (v02) |
| 10 | Actualizar el modelo en línea (MySQL) con los nuevos campos y datos re-procesados | ✅ Completado (v02) |

---

## 3. METODOLOGÍA

### 3.1 Enfoque general

Se adoptó un enfoque de **ETL por lotes (batch pipeline)** implementado en Python puro, donde cada etapa produce archivos CSV intermedios que alimentan la siguiente. Este enfoque fue elegido por:

- **Trazabilidad:** cada archivo intermedio permite auditar el resultado de cada paso.
- **Reproducibilidad:** el pipeline se puede re-ejecutar completo ante nuevos datos.
- **Simplicidad:** no requiere infraestructura de streaming ni colas de mensajes.

### 3.2 Técnicas de normalización de texto

| Técnica | Aplicación | Justificación |
|---|---|---|
| **Uppercase + strip** | Todas las cadenas | Eliminar variaciones triviales de capitalización |
| **Remoción de acentos** (preservando Ñ) | Todas las cadenas | Unificar "Bogotá" / "BOGOTA" / "bogota" |
| **Conversión de ordinales** | Nombres de juzgados | Unificar "PRIMERO" / "1ERO" / "1" / "PRIMER" → "1" |
| **Eliminación de paréntesis** | Nombres de entidades | Remover NITs, comentarios, aclaraciones |
| **Fingerprinting** (tokens ordenados) | Agrupación inicial | Agrupar cadenas con las mismas palabras en diferente orden |
| **Levenshtein** (distancia de edición) | Agrupación fuzzy | Fusionar variantes con errores tipográficos menores |
| **TF-IDF weighting** | Grupos OTRO | Ponderar tokens informativos vs. comunes para mejorar agrupación |

### 3.3 Técnicas de resolución geográfica

| Técnica | Aplicación | Justificación |
|---|---|---|
| **Two-pass extraction** | Todas las entidades | Primero extraer departamento, luego municipio en contexto del departamento encontrado |
| **Diccionario DANE** (1,104 municipios) | Matching exacto | Fuente oficial de referencia geográfica |
| **Aliases manuales** (170+ entradas) | Ciudades principales | Resolver variantes comunes: "SANTAFE DE BOGOTA", "SANTIAGO DE CALI", "SAN JOSE DE CUCUTA" |
| **Tabla de ambigüedades** | Municipios homónimos | FLORENCIA → Caquetá (no Cauca), MOSQUERA → Cundinamarca (no Nariño) |
| **Fallback por campo ciudad** | Embargos sin entidad georeferenciada | Usar el campo `ciudad` del registro de embargo como segunda fuente |

### 3.4 Técnicas de clasificación

| Técnica | Aplicación |
|---|---|
| **Regex pattern matching** (28 patrones ordenados por especificidad) | Clasificación en tipo/subtipo |
| **Reglas de negocio por tipo** | Gobernaciones agrupadas por departamento (no municipio), Juzgados distinguidos por número ordinal + municipio |

### 3.5 Motor de base de datos

| Aspecto | Decisión |
|---|---|
| Motor | MySQL 8.x con InnoDB |
| Charset | utf8mb4 (soporte completo de caracteres unicode) |
| Conexión | Cloud SQL Proxy (localhost:3306) |
| Carga | INSERT batch con manejo de errores por fila |
| Credenciales | Variables de entorno (DB_HOST, DB_USER, DB_PASSWORD) |

---

## 4. TAREAS EJECUTADAS

### Tarea 1: Extracción y análisis exploratorio de datos

| Atributo | Detalle |
|---|---|
| **Script** | `normalize_v4.py` (sección de carga) |
| **Entrada** | `embargos.csv` (929,690 registros) |
| **Salida** | 38,289 cadenas únicas de entidades remitentes con frecuencia |
| **Actividades** | Lectura del CSV fuente, extracción del campo `entidad_remitente`, conteo de frecuencias, identificación de patrones de variación textual |
| **Hallazgos** | Se identificaron errores de digitación, abreviaturas inconsistentes, caracteres especiales (tsheg tibetano, comillas tipográficas), ordinales en múltiples formatos, y municipios escritos con errores |

### Tarea 2: Normalización textual y conversión de ordinales

| Atributo | Detalle |
|---|---|
| **Script** | `normalize_v4.py` — funciones `clean_text()`, `normalize_ordinals()` |
| **Entrada** | 38,289 cadenas únicas |
| **Salida** | Cadenas limpias con ordinales numéricos |
| **Actividades** | Implementación de pipeline de limpieza (uppercase, remoción de acentos, eliminación de paréntesis, colapso de espacios), mapeo de 85+ ordinales compuestos y simples, manejo de abreviaturas ("1ERO", "2DO", "3ER") |

### Tarea 3: Clasificación por tipo y subtipo de entidad

| Atributo | Detalle |
|---|---|
| **Script** | `normalize_v4.py` — función `classify_entity()` |
| **Entrada** | Cadenas normalizadas |
| **Salida** | Tupla (tipo, subtipo) para cada entidad |
| **Actividades** | Definición de 28 patrones regex ordenados por especificidad, implementación de subtipos para juzgados (18 especialidades), secretarías (6), superintendencias (5), ministerios (5), CARs (15) |
| **Resultado** | 89.5% de entidades clasificadas con tipo específico (7,652 de 8,548), cubriendo 97.64% de los registros |

### Tarea 4: Extracción geográfica en dos pasadas

| Atributo | Detalle |
|---|---|
| **Script** | `normalize_v4.py` — función `extract_location()` |
| **Entrada** | Cadenas normalizadas + base DANE (JSON) |
| **Salida** | (municipio, departamento) para cada entidad |
| **Actividades** | Pasada 1: búsqueda de departamentos en el texto. Pasada 2: búsqueda de municipios con contexto del departamento encontrado. Manejo de ambigüedades con tabla de resolución. Implementación de 170+ aliases de ciudades principales |
| **Resultado** | 59% de entidades con municipio (71.92% de registros), 65.9% con departamento (79.25% de registros) |

### Tarea 5: Agrupación por similitud (fingerprinting + Levenshtein)

| Atributo | Detalle |
|---|---|
| **Script** | `normalize_v4.py` — funciones `fingerprint()`, `levenshtein_distance()` |
| **Entrada** | Entidades clasificadas y georreferenciadas |
| **Salida** | Grupos de variantes fusionados |
| **Actividades** | Agrupación inicial por fingerprint (tokens ordenados), fusión fuzzy con Levenshtein (umbral estricto), TF-IDF para grupos OTRO, respeto de regla: juzgado en diferente ciudad = entidad diferente |
| **Resultado** | Reducción de 38,289 variantes a 8,548 entidades (merge ratio: 77.68%) |

### Tarea 6: Enriquecimiento geográfico de embargos

| Atributo | Detalle |
|---|---|
| **Script** | `cruce_municipios.py` |
| **Entrada** | `embargos.csv` + `entidades.csv` + `colombia_municipios.json` |
| **Salida** | Embargos con campos `municipio` y `departamento` enriquecidos |
| **Actividades** | Cruce primario: ubicación de la entidad remitente. Cruce secundario (fallback): campo `ciudad` del embargo original. Mapeo de aliases de ciudades a municipios DANE |

### Tarea 7: Reestructuración y limpieza de la tabla de hechos

| Atributo | Detalle |
|---|---|
| **Script** | `restructure_embargos.py` |
| **Entrada** | Embargos enriquecidos |
| **Salida** | `embargos_final.csv` |
| **Actividades** | Eliminación de registros SIN_CONFIRMAR, eliminación de columnas 100% vacías (nombre_demandante, id_demandante, tipo_id_demandante), eliminación de nombre_remitente (denormalizado, resuelto via FK), validación de FK a entidades |

### Tarea 8: Construcción del modelo dimensional

| Atributo | Detalle |
|---|---|
| **Script** | `build_modelo.py` |
| **Entrada** | `embargos_final.csv` + `entidades.csv` + `variantes_entidades.csv` + `colombia_municipios.json` |
| **Salida** | 5 CSVs: dim_departamentos, dim_municipios, dim_entidades, dim_variantes, fact_oficios + schema.sql |
| **Actividades** | Generación de IDs secuenciales, resolución de FK geográficas (nombres a IDs), validación de integridad referencial interna, generación de DDL SQL |

### Tarea 9: Carga a base de datos MySQL

| Atributo | Detalle |
|---|---|
| **Script** | `upload_to_mysql.py` |
| **Entrada** | 5 CSVs del modelo dimensional |
| **Salida** | 5 tablas en esquema ETL de MySQL |
| **Actividades** | Ejecución de DDL (DROP + CREATE con FK e índices), carga batch (INSERT) tabla por tabla en orden de dependencias, manejo de errores por registro |

### Tarea 10: Inclusión de nuevos campos en el pipeline (v02)

| Atributo | Detalle |
|---|---|
| **Scripts modificados** | `normalize_v4.py`, `build_modelo.py`, `upload_to_mysql.py` |
| **Campos agregados** | referencia, expediente, created_at, confirmed_at, processed_at |
| **Origen de datos** | referencia: embargos.csv col 27; expediente: demandado.csv col 5; created_at: embargos.csv col 6; confirmed_at: embargos.csv col 3; processed_at: embargos.csv col 25 |
| **Actividades** | Mapeo de columnas por índice en archivos fuente, extracción durante normalización en normalize_v4.py (header de 21→26 columnas), propagación a través del pipeline (DictReader transparente), ampliación de fact_oficios de 20 a 25 campos en build_modelo.py, actualización de DDL e INSERT en upload_to_mysql.py |
| **Resultado** | 916,425 registros con los 5 nuevos campos disponibles en el modelo dimensional y en MySQL |

### Tarea 11: Reestructuración del maestro de entidades (v02)

| Atributo | Detalle |
|---|---|
| **Script creado** | `split_entidades.py` (nuevo, ~310 líneas) |
| **Entrada** | `dim_entidades.csv` (8,548 entidades) + `fact_oficios.csv` (916,425 oficios) |
| **Salida** | `dim_entidades_judiciales.csv` (6,578) + `dim_entidades_coactivas.csv` (1,970) |
| **Criterio de clasificación** | Tipos judiciales: JUZGADO, TRIBUNAL, CORTE, RAMA_JUDICIAL, FISCALIA, OFICINA_APOYO, CENTRO_SERVICIOS, DIRECCION_EJECUTIVA. Todo lo demás: coactiva |
| **Campos judiciales** | id_entidad, nombre_normalizado, tipo, subtipo, municipio, departamento, codigo_sierju, nombre_sierju, especialidad, email |
| **Campos coactivas** | id_entidad, nombre_normalizado, tipo, subtipo, municipio, departamento, email, categoria_funcional |
| **Enriquecimiento** | Extracción de emails del campo `email_juzgado` de fact_oficios (7,545 entidades con email), extracción de `numero_despacho` del nombre normalizado, matching fuzzy contra directorio SIERJU (threshold 0.7) |
| **Resultado** | 6,578 judiciales (76.9%) + 1,970 coactivas (23.1%). 0 registros sin clasificar |

### Tarea 12: Actualización del modelo en línea MySQL (v02)

| Atributo | Detalle |
|---|---|
| **Script** | `upload_to_mysql.py` (modificado) |
| **Conexión** | Cloud SQL Proxy → `caem-embargos-test:us-east4:pyc-embargos-bd-prod` vía localhost:3306 |
| **Actividades** | DDL actualizado con 5 nuevos campos (referencia VARCHAR(200), expediente VARCHAR(200), created_at/confirmed_at/processed_at DATETIME), re-creación de tablas, carga completa del modelo re-procesado |
| **Resultado** | dim_departamentos: 33, dim_municipios: 1,104, dim_entidades: 8,548, dim_variantes: 38,289, fact_oficios: 916,425. Carga exitosa sin errores |

### Tarea 13: Deduplicación de oficios

| Atributo | Detalle |
|---|---|
| **Script** | `dedup_oficios.py` |
| **Entrada** | Tabla fact_oficios en MySQL |
| **Salida** | Tabla fact_oficios deduplicada |
| **Actividades** | Agrupación por clave compuesta de 6 campos, priorización por estado (PROCESADO > RECONFIRMADO > CONFIRMADO), eliminación de duplicados conservando el registro más reciente con mejor estado |

### Tarea 14: Diagnóstico de calidad y validación

| Atributo | Detalle |
|---|---|
| **Script** | `diagnostico_calidad.py` |
| **Entrada** | BD ETL + BD pyc_embargos (original) |
| **Salida** | `reportes/DIAGNOSTICO_CALIDAD.txt` |
| **Actividades** | Comparación de conteos (ETL vs original), validación de integridad referencial de todas las FK (8 relaciones), identificación de registros faltantes, generación de reporte automatizado |
| **Resultado** | 0 FK rotas, varianza de 0.25% en conteos (aceptable) |

---

## 5. RESULTADOS OBTENIDOS

### 5.1 Métricas de normalización

| Métrica | Valor |
|---|---|
| Variantes textuales de entrada | 38,289 |
| Entidades normalizadas | 8,548 |
| Tasa de reducción | 77.68% |
| Entidades con tipo identificado | 89.5% (97.64% de registros) |
| Entidades con municipio | 59.0% (71.92% de registros) |
| Entidades con departamento | 65.9% (79.25% de registros) |
| Cobertura municipal DANE | 894 de 1,104 (81.0%) |
| Cobertura departamental DANE | 32 de 32 (100%) |

### 5.2 Métricas de calidad del modelo

| Métrica | Valor |
|---|---|
| Integridad referencial | 100% (0 FK rotas en 8 relaciones) |
| Varianza ETL vs original | 0.25% |
| Registros en fact_oficios | 916,425 |
| Registros deduplicados | 0 duplicados restantes |

### 5.3 Métricas v02 — Nuevos campos y reestructuración

| Métrica | Valor |
|---|---|
| Campos agregados a fact_oficios | 5 (referencia, expediente, created_at, confirmed_at, processed_at) |
| Total de campos en fact_oficios | 25 |
| Entidades judiciales | 6,578 (76.9%) |
| Entidades coactivas | 1,970 (23.1%) |
| Entidades con email extraído | 7,545 de 8,548 (88.3%) |
| Registros re-cargados a MySQL | 916,425 fact_oficios + 48,974 dimensiones |
| Errores de carga | 0 |

### 5.4 Entregables producidos

| Entregable | Ubicación |
|---|---|
| Modelo dimensional (5 tablas CSV + DDL) | `datos/modelo_final/` |
| Tablas derivadas de entidades (judiciales + coactivas) | `datos/procesados/` |
| Datos intermedios (entidades, variantes, embargos procesados) | `datos/procesados/` |
| Datos fuente (DANE, municipios) | `datos/fuentes/` |
| Scripts del pipeline (11 scripts) | `scripts/` |
| Metodología de normalización | `reportes/METODOLOGIA_NORMALIZACION.md` |
| Diagnóstico de calidad | `reportes/DIAGNOSTICO_CALIDAD.txt` |
| Informe de normalización | `reportes/INFORME_NORMALIZACION.txt` |
| Sugerencias de duplicados | `reportes/sugerencias_normalizacion.csv` |
| Documentación del código (A-GD-04) v02 | `DOCUMENTACION_CODIGO.md` |
| Diccionario de datos v02 | `DICCIONARIO_DATOS.md` |
| Metodología y objetivos v02 | `METODOLOGIA_OBJETIVOS_TAREAS.md` |

---

## 6. LIMITACIONES IDENTIFICADAS

| # | Limitación | Impacto | Mitigación |
|---|---|---|---|
| 1 | 41% de entidades sin municipio asignado | Análisis geográfico incompleto a nivel municipal | Se usa campo `ciudad` del embargo como fallback. Entidades nacionales (DIAN, SENA) no requieren municipio. |
| 2 | Juzgados genéricos sin mención de ciudad | No se pueden geolocalizar | Requiere enriquecimiento con fuente externa (SIERJU, Rama Judicial) |
| 3 | 10.5% de entidades tipo OTRO (sin clasificar) | Entidades no categorizadas | Representan solo 2.36% de registros. Se puede ampliar la taxonomía con patrones adicionales |
| 4 | Ordinales compuestos con errores de digitación | Algunos no se normalizan correctamente | Se cubrieron 85+ variantes, pero pueden existir combinaciones no contempladas |
| 5 | Municipios homónimos entre departamentos | Posible asignación incorrecta | Se implementó tabla de resolución de ambigüedades para los casos más frecuentes |
| 6 | CSV grandes no versionados en Git | fact_oficios.csv y embargos.csv excluidos del repo | Se incluye schema.sql para regenerar la estructura y los scripts para reproducir los datos |

---

## 7. CAMBIOS IMPLEMENTADOS EN v02

### 7.1 Requerimiento 1 — Inclusión de 5 nuevos campos

**Solicitud:** Incluir los campos `referencia` (tabla embargos), `expediente` (tabla demandado), `created_at` (tabla embargos), `confirmed_at` (tabla embargos) y `processed_at` (tabla embargos) en el pipeline de normalización y en el modelo dimensional.

**Implementación:**

| Componente | Cambio realizado |
|---|---|
| `normalize_v4.py` | Se identificaron las columnas por índice en los CSV fuente: referencia=col27, created_at=col6, confirmed_at=col3, processed_at=col25 (embargos.csv), expediente=col5 (demandado.csv). Se agregó la extracción durante la fase de normalización, expandiendo el header de salida de 21 a 26 columnas |
| `build_modelo.py` | Se actualizó `build_oficios()` para propagar los 5 campos con `row.get()` (seguro ante valores ausentes). El fieldnames del CSV writer pasó de 20 a 25 campos. El DDL en schema.sql se amplió con `referencia VARCHAR(200)`, `expediente VARCHAR(200)`, `created_at DATETIME`, `confirmed_at DATETIME`, `processed_at DATETIME` |
| `upload_to_mysql.py` | Se actualizó el DDL de `fact_oficios` con las 5 columnas nuevas, el INSERT con 25 placeholders `%s`, y la construcción de la tupla de valores para incluir los nuevos campos |
| `restructure_embargos.py` | No requirió cambios — usa `DictReader`/`DictWriter` por lo que los campos nuevos fluyen automáticamente |
| `cruce_municipios.py` | No requirió cambios — mismo diseño basado en diccionarios |

**Verificación:** Los 5 campos están presentes en `fact_oficios.csv` (25 columnas) y en la tabla MySQL `fact_oficios` (916,425 filas con 25 columnas).

### 7.2 Requerimiento 2 — Reestructuración del maestro de entidades

**Solicitud:** Dividir `dim_entidades` en dos tablas derivadas:
- **Entidades Judiciales** (despachos del poder judicial) con campos: id_entidad, nombre_normalizado, tipo, subtipo, municipio, departamento, codigo_sierju, nombre_sierju, especialidad, email
- **Entidades Coactivas** (entidades administrativas con facultad de cobro coactivo) con campos: id_entidad, nombre_normalizado, tipo, subtipo, municipio, departamento, email, categoria_funcional

**Implementación:**

| Componente | Cambio realizado |
|---|---|
| `split_entidades.py` (NUEVO) | Script de ~310 líneas que: (1) clasifica cada entidad como judicial o coactiva según su `tipo`, (2) extrae emails del campo `email_juzgado` de fact_oficios mediante conteo de frecuencia (el email más común por entidad), (3) intenta matching fuzzy contra directorio SIERJU (openpyxl), (4) extrae `numero_despacho` del nombre normalizado |
| Criterio de clasificación | Tipos judiciales: JUZGADO, TRIBUNAL, CORTE, RAMA_JUDICIAL, FISCALIA, OFICINA_APOYO, CENTRO_SERVICIOS, DIRECCION_EJECUTIVA |

**Resultados:**
- 6,578 entidades judiciales (76.9%)
- 1,970 entidades coactivas (23.1%)
- 7,545 entidades con email extraído (88.3%)
- 0 entidades sin clasificar

### 7.3 Requerimiento 3 — Actualización del modelo en línea

**Solicitud:** Re-ejecutar el pipeline completo y actualizar la base de datos MySQL en producción.

**Ejecución del pipeline:**

| Paso | Script | Resultado |
|---|---|---|
| 1 | `normalize_v4.py` | 929,690 filas procesadas → 8,548 entidades, 38,289 variantes, 894 municipios |
| 2 | `restructure_embargos.py` | 929,690 → 916,425 (eliminados 13,265 SIN_CONFIRMAR) |
| 3 | `cruce_municipios.py` | 916,425 oficios enriquecidos (72.5% por entidad, 27.3% por ciudad, 0.2% sin resolver) |
| 4 | `build_modelo.py` | 5 CSVs generados: 33 deptos, 1,104 municipios, 8,548 entidades, 38,289 variantes, 916,425 oficios (25 cols) |
| 5 | `split_entidades.py` | 6,578 judiciales + 1,970 coactivas |
| 6 | `upload_to_mysql.py` | Carga exitosa a MySQL: 964,399 registros totales, 0 errores |

**Verificación MySQL:**

| Tabla | Filas cargadas |
|---|---|
| dim_departamentos | 33 |
| dim_municipios | 1,104 |
| dim_entidades | 8,548 |
| dim_variantes | 38,289 |
| fact_oficios | 916,425 |
| **Total** | **964,399** |
