# Desarrollo de software A-GD-04
## Documentación del Código — VERSION 01
### ABR-2026

---

**Documentación del código — 1 de 6**

# Normalización CAEM — Modelo de Datos de Embargos

## Tabla de contenido

1. [INTRODUCCIÓN](#1-introducción)
2. [ESTRUCTURA DEL CÓDIGO](#2-estructura-del-código)
3. [CONVENCIONES DE CODIFICACIÓN](#3-convenciones-de-codificación)
4. [ARQUITECTURA DEL SOFTWARE](#4-arquitectura-del-software)
5. [DOCUMENTACIÓN DEL CÓDIGO](#5-documentación-del-código)
6. [PRUEBAS](#6-pruebas)

---

**Documentación del código — 2 de 6**

## Historial de versiones

| Fecha | Descripción | Autor |
|---|---|---|
| 2026-03-31 | v1 — Normalización base con fingerprinting y agrupación | Geiner Martínez |
| 2026-03-31 | v2 — Reglas por tipo de entidad (Alcaldías, Gobernaciones, Juzgados) | Geiner Martínez |
| 2026-03-31 | v3 — Integración base DANE de municipios, Levenshtein | Geiner Martínez |
| 2026-04-01 | v4 — Producción: Gobernaciones por departamento, TF-IDF, desambiguación geográfica | Geiner Martínez |
| 2026-04-06 | ETL modelo dimensional (5 tablas) + carga a MySQL | Geiner Martínez |
| 2026-04-07 | Diagnóstico de calidad y deduplicación de oficios | Geiner Martínez |

---

**Documentación del código — 3 de 6**

## 1. INTRODUCCIÓN

Sistema de normalización y modelado de datos para el proceso de embargos del CAEM (Centro de Atención de Embargos). El proyecto transforma datos crudos de oficios de embargo (~929,690 registros) en un modelo dimensional relacional limpio, resolviendo la entidad remitente a partir de 38,289 variantes textuales que se normalizan en 8,548 entidades únicas.

**Lenguaje:** Python 3.13  
**Base de datos destino:** MySQL 8.x (InnoDB, utf8mb4)  
**Librerías principales:** csv, json, re, unicodedata (estándar); pymysql (carga a BD)  
**Objetivo:** Construir un modelo dimensional estrella que permita análisis geográfico y temporal de embargos por entidad remitente, con integridad referencial completa.

## 2. ESTRUCTURA DEL CÓDIGO

```
normalizacion-CAEM/
├── datos/
│   ├── fuentes/                         # Datos de entrada (raw)
│   │   ├── embargos.csv                 # Dataset original: 929,690 oficios de embargo
│   │   ├── demandado.csv                # Datos de demandados (~2.3M registros)
│   │   ├── colombia_municipios.json     # Referencia DANE: 1,104 municipios, 32 departamentos
│   │   └── municipios.csv              # Tabla auxiliar de municipios
│   ├── procesados/                      # Salidas intermedias del pipeline
│   │   ├── entidades.csv               # 8,548 entidades normalizadas con clasificación
│   │   ├── variantes_entidades.csv     # Mapeo: 38,289 variantes → entidad normalizada
│   │   └── embargos_final.csv          # Tabla de hechos con FK a entidades
│   └── modelo_final/                    # Modelo dimensional listo para producción
│       ├── dim_departamentos.csv        # 32 departamentos colombianos
│       ├── dim_municipios.csv           # Municipios con FK a departamentos
│       ├── dim_entidades.csv            # Entidades normalizadas con tipo/subtipo/ubicación
│       ├── dim_variantes.csv            # Variantes textuales con FK a entidades
│       ├── fact_oficios.csv             # 916,425 oficios de embargo (tabla de hechos)
│       └── schema.sql                   # DDL completo con constraints e índices
├── scripts/                             # Pipeline de procesamiento
│   ├── normalize_v4.py                  # Normalización de entidades (producción)
│   ├── cruce_municipios.py              # Enriquecimiento geográfico de embargos
│   ├── restructure_embargos.py          # Limpieza y reestructuración de embargos
│   ├── build_modelo.py                  # ETL: genera modelo dimensional (5 tablas CSV)
│   ├── dedup_oficios.py                 # Deduplicación de oficios en BD
│   ├── upload_to_mysql.py               # Carga del modelo a MySQL
│   ├── diagnostico.py                   # Reporte de tasas de normalización
│   ├── diagnostico_calidad.py           # Auditoría de calidad ETL vs BD original
│   └── informe_normalizacion.py         # Genera informe de entidades sin municipio
├── reportes/                            # Informes y documentación analítica
│   ├── METODOLOGIA_NORMALIZACION.md     # Metodología completa del pipeline
│   ├── INFORME_NORMALIZACION.txt        # Resumen tabular de normalización
│   ├── DIAGNOSTICO_CALIDAD.txt          # Auditoría de calidad: ETL vs original
│   └── sugerencias_normalizacion.csv    # Sugerencias de entidades duplicadas  
├── DOCUMENTACION_CODIGO.md              # Este documento
└── README.md                            # Descripción general del proyecto
```

## 3. CONVENCIONES DE CODIFICACIÓN

### Nomenclatura

- **Scripts:** `snake_case.py` con sufijo de versión cuando aplica (`normalize_v4.py`)
- **Funciones:** `snake_case` descriptivas (`clean_text()`, `extract_location()`, `build_departamentos()`)
- **Constantes:** `UPPER_SNAKE_CASE` (`COMPOUND_NUMS`, `ALIASES`, `DISCARD_ESTADOS`)
- **Archivos de datos:** `snake_case.csv` con prefijo `dim_` para dimensiones y `fact_` para hechos
- **Claves primarias:** `<tabla>_id` (ej. `entidad_id`, `municipio_id`, `departamento_id`)
- **Claves foráneas:** mismo nombre que la PK referenciada

### Comentarios

- Cada script comienza con docstring de módulo describiendo propósito, entradas y salidas
- Secciones separadas con bloques de comentarios `# ═══════` o `# ============`
- Progreso en consola con prefijo `[paso/total]` (ej. `[1/6] Cargando referencia DANE...`)
- Inline comments en español para lógica de negocio, en inglés para lógica técnica

### Control de versiones

- Git con rama `main`
- Repositorio: `https://github.com/geiner28/normalizacion-CAEM.git`
- Credenciales de base de datos externalizadas via variables de entorno (`DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`)

---

**Documentación del código — 4 de 6**

## 4. ARQUITECTURA DEL SOFTWARE

### Tipo de arquitectura: Pipeline ETL secuencial (batch)

El sistema sigue un patrón de **pipeline de datos por lotes** donde cada script transforma datos de entrada y genera archivos CSV como salida para el siguiente paso. No hay servidor ni API; es un proceso batch ejecutable paso a paso.

### Flujo del pipeline:

```
embargos.csv (raw)
       │
       ▼
┌──────────────────────────┐
│  normalize_v4.py         │  Paso 1: Normalización de entidades
│  - Limpieza textual      │  38,289 variantes → 8,548 entidades
│  - Fingerprinting        │  Clasificación por tipo/subtipo
│  - Resolución geográfica │  Extracción municipio/departamento
│  - Agrupación Levenshtein│
└──────────┬───────────────┘
           ▼
   entidades.csv + variantes_entidades.csv
           │
           ▼
┌──────────────────────────┐
│  cruce_municipios.py     │  Paso 2: Enriquecimiento geográfico
│  - Cruza embargo con     │  FK municipio/departamento en embargos
│    ubicación de entidad  │
│  - Fallback: campo       │
│    'ciudad' del embargo  │
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│  restructure_embargos.py │  Paso 3: Limpieza de tabla de hechos
│  - Elimina SIN_CONFIRMAR │  Elimina columnas vacías
│  - Valida FK a entidades │
└──────────┬───────────────┘
           ▼
   embargos_final.csv
           │
           ▼
┌──────────────────────────┐
│  build_modelo.py         │  Paso 4: Construcción modelo dimensional
│  - dim_departamentos     │  Genera 5 tablas CSV normalizadas
│  - dim_municipios        │  con integridad referencial
│  - dim_entidades         │
│  - dim_variantes         │
│  - fact_oficios          │
└──────────┬───────────────┘
           ▼
   datos/modelo_final/*.csv + schema.sql
           │
           ▼
┌──────────────────────────┐
│  upload_to_mysql.py      │  Paso 5: Carga a base de datos
│  - Ejecuta DDL           │  MySQL vía Cloud SQL Proxy
│  - Bulk INSERT           │
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│  dedup_oficios.py        │  Paso 6: Deduplicación en BD
│  - Agrupa por clave      │  Prioriza estado PROCESADO
│    compuesta             │  916,425 → registros únicos
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│  diagnostico_calidad.py  │  Paso 7: Validación de calidad
│  - Compara ETL vs        │  Integridad referencial: 100% ✅
│    pyc_embargos original  │  Varianza: 0.25%
└──────────────────────────┘
```

### Modelo dimensional (estrella):

```
                    dim_departamentos
                         │
                    dim_municipios
                    ╱          ╲
          dim_entidades    fact_oficios
               │                │
          dim_variantes         │
                         (tabla de hechos)
```

**Tablas dimensionales:**
- `dim_departamentos` — 32 departamentos colombianos (PK: `departamento_id`)
- `dim_municipios` — Municipios con FK a departamento (PK: `municipio_id`)
- `dim_entidades` — 8,548 entidades normalizadas con tipo, subtipo, ubicación (PK: `entidad_id`)
- `dim_variantes` — 38,289 variantes textuales mapeadas a entidad normalizada (PK: `variante_id`)

**Tabla de hechos:**
- `fact_oficios` — 916,425 oficios de embargo con FKs a entidades, municipios y departamentos (PK: `oficio_id`)

---

**Documentación del código — 5 de 6**

## 5. DOCUMENTACIÓN DEL CÓDIGO

### Scripts principales y sus métodos clave:

#### `normalize_v4.py` — Motor de normalización (producción)
| Función | Propósito | Parámetros | Retorno |
|---|---|---|---|
| `clean_text(text)` | Normaliza texto: mayúsculas, sin acentos, sin paréntesis | `text: str` | `str` limpio |
| `normalize_ordinals(text)` | Convierte ordinales textuales a números ("PRIMERO"→"1") | `text: str` | `str` con ordinales numéricos |
| `fingerprint(text)` | Genera clave de agrupación: tokens ordenados alfabéticamente | `text: str` | `str` fingerprint |
| `classify_entity(name)` | Clasifica entidad por tipo/subtipo (JUZGADO, ALCALDIA, DIAN...) | `name: str` | `(tipo, subtipo)` |
| `extract_location(name, deptos, munis)` | Extracción geográfica en dos pasadas: departamento → municipio | `name, deptos, munis` | `(municipio, departamento)` |
| `levenshtein_distance(s1, s2)` | Distancia de edición entre cadenas para fuzzy matching | `s1, s2: str` | `int` |

#### `build_modelo.py` — Generador del modelo dimensional
| Función | Propósito | Parámetros | Retorno |
|---|---|---|---|
| `build_departamentos(json_path)` | Construye dim_departamentos desde JSON DANE | `json_path: str` | `list[dict]` |
| `build_municipios(json_path, deptos)` | Construye dim_municipios con FK a departamentos | `json_path, deptos` | `list[dict]` |
| `build_entidades(csv_path, munis, deptos)` | Construye dim_entidades con resolución de FK geográficas | `csv_path, munis, deptos` | `list[dict]` |
| `build_variantes(csv_path, entidades)` | Construye dim_variantes con FK a entidades | `csv_path, entidades` | `list[dict]` |
| `build_fact_oficios(csv_path, ...)` | Construye fact_oficios con todas las FK resueltas | `csv_path, ...` | `list[dict]` |

#### `upload_to_mysql.py` — Carga a base de datos
| Función | Propósito |
|---|---|
| `main()` | Ejecuta DDL (5 tablas con FK e índices) y carga datos via INSERT batch |

#### `dedup_oficios.py` — Deduplicación
| Función | Propósito |
|---|---|
| `main()` | Agrupa por clave compuesta (numero_oficio + entidad + fecha + demandado), conserva 1 registro priorizando estado PROCESADO |

#### `diagnostico_calidad.py` — Auditoría de calidad
| Función | Propósito |
|---|---|
| `main()` | Compara conteos ETL vs BD original, valida integridad referencial de todas las FK, genera reporte DIAGNOSTICO_CALIDAD.txt |

### Evidencia de documentación técnica:

- **Reportes generados:** [reportes/METODOLOGIA_NORMALIZACION.md](reportes/METODOLOGIA_NORMALIZACION.md) — Metodología completa con métricas
- **Diagnóstico de calidad:** [reportes/DIAGNOSTICO_CALIDAD.txt](reportes/DIAGNOSTICO_CALIDAD.txt) — Auditoría automatizada
- **Schema SQL:** [datos/modelo_final/schema.sql](datos/modelo_final/schema.sql) — DDL completo con constraints

---

**Documentación del código — 6 de 6**

## 6. PRUEBAS

### 6.1 Validación de integridad referencial

Se ejecutó `diagnostico_calidad.py` que verifica automáticamente todas las FK del modelo:

| Relación | FK rotas | Estado |
|---|---|---|
| `dim_municipios` → `dim_departamentos` | 0 | ✅ |
| `dim_entidades` → `dim_municipios` | 0 | ✅ |
| `dim_entidades` → `dim_departamentos` | 0 | ✅ |
| `dim_variantes` → `dim_entidades` | 0 | ✅ |
| `fact_oficios` → `dim_entidades` | 0 | ✅ |
| `fact_oficios` → `dim_municipios` | 0 | ✅ |
| `fact_oficios` → `dim_departamentos` | 0 | ✅ |
| `fact_oficios` → `dim_entidad_bancaria` | 0 | ✅ |

**Resultado: TODAS LAS FK VÁLIDAS ✅**

### 6.2 Comparación de conteos ETL vs base original

| Métrica | Original (pyc_embargos) | ETL | Diferencia |
|---|---|---|---|
| Total embargos | 929,690 | — | — |
| Embargos activos (deleted=0) | 914,153 | 916,425 | +0.25% ✅ |
| Departamentos | — | 32 | — |
| Municipios | — | 1,104 | — |
| Entidades normalizadas | — | 8,548 | — |
| Variantes mapeadas | 38,289 | 38,289 | 0% ✅ |

### 6.3 Validación de normalización geográfica

| Indicador | Entidades | % | Registros | % |
|---|---|---|---|---|
| Con municipio asignado | 5,043 | 59.0% | 668,639 | 71.92% |
| Con departamento asignado | 8,548 | 100% | 929,690 | 100% |

### 6.4 Validación de deduplicación

El script `dedup_oficios.py` identifica y elimina registros duplicados con criterio:
- **Clave compuesta:** numero_oficio + entidad_remitente_id + entidad_bancaria_id + fecha_oficio + nombre_demandado + id_demandado
- **Prioridad:** PROCESADO > RECONFIRMADO > CONFIRMADO > otros
- **Resultado:** 0 registros duplicados restantes tras ejecución

### 6.5 Pruebas de cobertura de tipos de entidad

| Tipo | Entidades | Registros | % Registros |
|---|---|---|---|
| JUZGADO | ~3,200 | ~450,000 | ~48% |
| ALCALDIA | ~1,100 | ~120,000 | ~13% |
| DIAN | ~100 | ~90,000 | ~10% |
| GOBERNACION | ~32 | ~50,000 | ~5% |
| OTRO (sin clasificar) | ~2,000 | ~100,000 | ~11% |
| Demás tipos | ~2,100 | ~120,000 | ~13% |
