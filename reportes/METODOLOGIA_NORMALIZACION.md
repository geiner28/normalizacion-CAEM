# Metodología de Normalización de Entidades — Embargos Colombia

## Índice
1. [Diagnóstico de Resultados](#1-diagnóstico-de-resultados)
2. [Descripción General del Pipeline](#2-descripción-general-del-pipeline)
3. [Datos de Entrada](#3-datos-de-entrada)
4. [Etapas del Proceso](#4-etapas-del-proceso)
5. [Estructura de Salida](#5-estructura-de-salida)
6. [Guía para Replicación con Datos Nuevos](#6-guía-para-replicación-con-datos-nuevos)
7. [Problemas Conocidos y Limitaciones](#7-problemas-conocidos-y-limitaciones)

---

## 1. Diagnóstico de Resultados

### 1.1 Resumen Ejecutivo

| Métrica | Valor |
|---|---|
| Cadenas únicas de entrada (raw) | 38,289 |
| Entidades normalizadas finales | 8,548 |
| Tasa de reducción (merge ratio) | **77.68%** |
| Total de registros de embargos | 929,690 |

### 1.2 Normalización Geográfica — Municipios

| Indicador | Entidades | % Entidades | Registros | % Registros |
|---|---|---|---|---|
| **CON** municipio asignado | 5,043 | 59.0% | 668,639 | **71.92%** |
| **SIN** municipio asignado | 3,505 | 41.0% | 261,051 | 28.08% |

- Se identificaron **894 municipios** de los 1,104 del DANE → **81.0% de cobertura municipal**.
- El 41% de entidades sin municipio corresponde principalmente a:
  - Juzgados sin ciudad en el nombre original (ej: "JUZGADO PRIMERO CIVIL MUNICIPAL" sin mencionar la ciudad).
  - Entidades nacionales (DIAN, SENA, Superintendencias, Ministerios) que no tienen jurisdicción municipal.

### 1.3 Normalización Geográfica — Departamentos

| Indicador | Entidades | % Entidades | Registros | % Registros |
|---|---|---|---|---|
| **CON** departamento asignado | 5,629 | 65.9% | 736,770 | **79.25%** |
| **SIN** departamento asignado | 2,919 | 34.1% | 192,920 | 20.75% |

- Se cubren **32/32 departamentos** del DANE (100%).
- Se identificó un departamento extra: "Bogotá, D.C." (usado como departamento para entidades distritales).

### 1.4 Normalización de Tipo de Entidad

| Indicador | Entidades | % Entidades | Registros | % Registros |
|---|---|---|---|---|
| Clasificadas (con tipo) | 7,652 | 89.5% | 907,704 | **97.64%** |
| Sin clasificar (OTRO) | 896 | 10.5% | 21,986 | 2.36% |
| "No encontrada" | 2 | — | ~2,500 | 0.27% |

### 1.5 Éxito Combinado (Tipo + Ubicación)

| Indicador | Entidades | % | Registros | % |
|---|---|---|---|---|
| **Completamente normalizadas** (tipo + ubicación) | 5,449 / 8,548 | **63.7%** | 726,926 / 929,690 | **78.19%** |

### 1.6 Desglose por Tipo de Entidad

| Tipo | Entidades | Registros | Variantes | % con Municipio | % con Depto |
|---|---|---|---|---|---|
| JUZGADO | 6,444 | 487,208 | 29,274 | 66.5% | 72.8% |
| ALCALDIA | 335 | 133,914 | 2,779 | 70.4% | 75.5% |
| DATT | 2 | 109,044 | 169 | 100.0% | 100.0% |
| GOBERNACION | 35 | 39,656 | 437 | 0.0%* | 74.3% |
| DIAN | 12 | 38,211 | 22 | 8.3% | 8.3% |
| MUNICIPIO | 157 | 24,166 | 594 | 84.7% | 90.4% |
| OTRO | 896 | 21,986 | 2,484 | 17.4% | 20.1% |
| SECRETARIA | 212 | 20,028 | 816 | 44.3% | 54.7% |
| SUPERINTENDENCIA | 63 | 12,133 | 156 | 12.7% | 12.7% |
| IDU | 7 | 6,495 | 58 | 42.9% | 42.9% |
| SENA | 19 | 5,823 | 38 | 31.6% | 73.7% |
| MINISTERIO | 74 | 5,633 | 144 | 8.1% | 9.5% |
| CAR | 55 | 4,127 | 341 | 20.0% | 56.4% |
| ESP | 29 | 4,068 | 206 | 44.8% | 48.3% |
| EMCALI | 1 | 3,899 | 61 | 100.0% | 100.0% |
| TRANSITO | 21 | 3,022 | 131 | 61.9% | 71.4% |
| COLPENSIONES | 3 | 1,502 | 8 | 0.0% | 0.0% |
| TRIBUNAL | 40 | 459 | 90 | 20.0% | 85.0% |

> *Las Gobernaciones usan departamento como ubicación, no municipio. 0% de municipio es correcto por diseño.

### 1.7 Principales Entidades sin Municipio

Las entidades con más registros sin municipio son de naturaleza nacional (DIAN, SENA, Superintendencias) o son juzgados genéricos sin mención de ciudad en los datos originales:

| Entidad | Registros | Tipo |
|---|---|---|
| DIAN | 38,027 | DIAN (nacional) |
| SUPERINTENDENCIA DE SOCIEDADES | 8,894 | SUPERINTENDENCIA (nacional) |
| IDU | 6,427 | IDU (Bogotá implícito) |
| SENA | 5,425 | SENA (nacional) |
| Juzgado Primero Civil Municipal | 4,599 | JUZGADO (sin ciudad) |
| Ministerio del Trabajo | 4,160 | MINISTERIO (nacional) |

---

## 2. Descripción General del Pipeline

```
embargos.csv (raw data, ~1M filas)
    │
    ├── Extracción del campo "entidad_remitente" (columna 12)
    │         │
    │         ▼
    │   unique_entities_raw.csv (38,289 cadenas únicas + conteo)
    │         │
    │         ▼
    │   ┌─────────────────────────────────┐
    │   │  NORMALIZACIÓN (normalize_v4.py)│
    │   │  1. Limpieza de texto           │
    │   │  2. Conversión de ordinales     │
    │   │  3. Clasificación de tipo       │
    │   │  4. Extracción geográfica       │
    │   │  5. Agrupación por similitud    │
    │   │  6. Recuperación fuzzy          │
    │   └─────────────────────────────────┘
    │         │
    │         ▼
    │   entidades.csv              (8,548 entidades normalizadas)
    │   variantes_entidades.csv    (38,289 mapeos variante → entidad)
    │   municipios.csv             (894 municipios identificados)
    │
    └── Enriquecimiento + demandado.csv
              │
              ▼
        embargos_limpios.csv       (929,690 registros limpios)
              │
              ▼
        dashboard.html             (visualización interactiva)
```

---

## 3. Datos de Entrada

### 3.1 `embargos.csv`  
- **Descripción**: Registros crudos de embargos judiciales.
- **Columnas clave**: `id` (col 0), `entidad_remitente` (col 12), `estado_embargo` (col 13), `fecha_oficio` (col 15), `monto` (col 20).
- **Volumen**: ~929,690 filas.
- **Problemas típicos**: Errores de digitación, variaciones de mayúsculas/minúsculas, acentos inconsistentes, caracteres especiales, abreviaturas, duplicaciones de texto.

### 3.2 `demandado.csv`
- **Descripción**: Información de demandados vinculados a embargos.
- **Columnas clave**: `nombres` (col 12), `identificacion` (col 7), `embargo_id` (col 15).

### 3.3 `colombia_municipios.json`
- **Descripción**: Base de datos oficial del DANE con 1,104 municipios de Colombia, organizados por departamento.
- **Estructura**:
```json
[
  {
    "id": 0,
    "departamento": "Amazonas",
    "ciudades": ["Leticia", "Puerto Nariño"]
  }
]
```

---

## 4. Etapas del Proceso

### Etapa 1: Limpieza de Texto (`clean_text`)

**Objetivo**: Normalizar la representación textual eliminando variaciones superficiales.

**Operaciones en orden**:
1. **Trim** — eliminar espacios en extremos.
2. **Uppercase** — convertir todo a mayúsculas.
3. **Remover acentos** — `á→A, é→E, í→I, ó→O, ú→U` (preservando la Ñ).
4. **Caracteres especiales** — eliminar comillas tipográficas (`""''`), reemplazar guiones largos (`—`, `–`) por `-`.
5. **Contenido entre paréntesis** — eliminar `(contenido)`.
6. **Pipes** — eliminar `|`.
7. **Espacios múltiples** — colapsar a un solo espacio.
8. **Puntuación en extremos** — eliminar `.,;:-` al inicio/final.

**Ejemplo**:
```
Entrada: "  Alcaldía Distrital de Cartagena (NIT: 890.480.184)  "
Salida:  "ALCALDIA DISTRITAL DE CARTAGENA"
```

### Etapa 2: Conversión de Números Ordinales (`replace_ordinals`)

**Objetivo**: Unificar números escritos en texto a su forma numérica.

**Cobertura**:
- Compuestos (1-85): `DECIMO PRIMERO → 11`, `VIGESIMO TERCERO → 23`, `CUARENTA Y TRES → 43`
- Simples (1-90): `PRIMERO → 1`, `SEGUNDO → 2`, `NOVENTA → 90`
- Abreviaturas: `1ERO → 1`, `2DO → 2`, `3ER → 3`, `5TO → 5`

**Importancia**: Los juzgados incluyen números ordinales en su nombre y estos varían enormemente:
```
"JUZGADO CUARENTA Y TRES DE PEQUEÑAS CAUSAS" → "JUZGADO 43 DE PEQUEÑAS CAUSAS"
"JUZGADO CUADRAGÉSIMO TERCERO DE..."         → "JUZGADO 43 DE..."
```

### Etapa 3: Clasificación de Tipo de Entidad (`_detect_type` + `classify_entity`)

**Objetivo**: Asignar un tipo taxonómico (JUZGADO, ALCALDIA, GOBERNACION, etc.) y subtipo.

**Tipos reconocidos** (28 categorías):

| Tipo | Patrón de Detección | Subtipo |
|---|---|---|
| DATT | `DATT`, `DEPARTAMENTO ADMINISTRATIVO DE TRANS` | TRANSITO |
| DIAN | `\bDIAN\b` | — |
| SENA | `\bSENA\b` | — |
| COLPENSIONES | `COLPENSIONES` | — |
| EMCALI | `EMCALI` | — |
| CAR | `CORPORACION AUTONOMA REGIONAL`, `CARDER`, `CVC`, `CORNARE` | CARDER, CVC, CORNARE, etc. |
| RAMA_JUDICIAL | `RAMA JUDICIAL`, `DISTRITO JUDICIAL` | — |
| JUZGADO | `JUZGADO`, `JUZSADO`, `JUZG` | CIVIL_MUNICIPAL, PEQUENAS_CAUSAS, PROMISCUO_MUNICIPAL, EJECUCION_CIVIL, LABORAL, PENAL, FAMILIA, etc. |
| OFICINA_APOYO | `OFICINA DE APOYO` | CIVIL_MUNICIPAL, CIVIL, LABORAL |
| ALCALDIA | `ALCALDIA`, `ALCALDI` | — |
| GOBERNACION | `GOBERNACI`, `GBERANCION` | — |
| SUPERINTENDENCIA | `SUPERINTENDENCIA` | SOCIEDADES, INDUSTRIA_COMERCIO, SALUD, etc. |
| SECRETARIA | `SECRETARI` | TRANSITO, HACIENDA, GOBIERNO, etc. |
| MINISTERIO | `MINISTERIO` | TRABAJO, HACIENDA, DEFENSA, etc. |
| MUNICIPIO | `\bMUNICIPIO\b` | — |
| IDU | `\bIDU\b` | — |
| TRIBUNAL | `TRIBUNAL` | ADMINISTRATIVO, SUPERIOR |
| FISCALIA | `FISCALI` | — |
| ESP | `EMPRESAS PUBLICAS`, `E.S.P.` | — |
| POLICIA | `POLICI` | — |
| CORTE | `\bCORTE\b` | SUPREMA, CONSTITUCIONAL |
| CONTRALORIA | `CONTRALORI` | — |
| PERSONERIA | `PERSONERI` | — |
| UGPP | `U.G.P.P.` | — |
| OTRO | Ningún patrón coincide | — |

**Regla crítica**: El orden de detección importa. Los patrones más específicos se evalúan primero (ej: DATT antes de TRANSITO, CAR antes de JUZGADO).

### Etapa 4: Extracción Geográfica — Dos Pasadas (`extract_location_v4`)

**Objetivo**: Identificar el municipio y/o departamento mencionado en el nombre de la entidad.

#### Pasada 1: Departamento
- Buscar nombres de departamentos del DANE en el texto (longest match first).
- Buscar aliases de departamentos (ej: `BOIVAR → Bolívar`, `SATANDER → Santander`).

#### Pasada 2: Municipio (con contexto de departamento)
- Buscar nombres de municipios del DANE en el texto.
- Si se encontró departamento en pasada 1, preferir municipios de ese departamento.
- **Manejo de ambigüedades**: municipios con nombres duplicados entre departamentos se resuelven con tabla `AMBIGUOUS_RESOLUTION`:
  - `FLORENCIA → Caquetá` (no Cauca)
  - `MOSQUERA → Cundinamarca` (no Nariño)
  - `RIONEGRO → Antioquia`
  - `BARBOSA → Antioquia`

#### Aliases de ciudades principales
Diccionario de **170+ aliases** que mapea variantes comunes y errores tipográficos a la ciudad correcta:
```
MEDILLIN → Medellín, Antioquia
BARRAANQUILLA → Barranquilla, Atlántico
VILLAVIVENCIO → Villavicencio, Meta
SOCAHA → Soacha, Cundinamarca
```

#### Regla especial para Gobernaciones
Las Gobernaciones se agrupan por **departamento**, no por municipio. Si el texto dice "GOBERNACION DE PEREIRA", se mapea al departamento Risaralda.

#### Nombres que son tanto departamento como municipio
Cuando un nombre como "CÓRDOBA", "BOLÍVAR", "CALDAS" aparece en el texto, se trata como **departamento**, no como municipio, a menos que haya evidencia fuerte de lo contrario.

### Etapa 5: Agrupación y Clustering (`initial_groups` → `final_groups`)

**Objetivo**: Agrupar variantes que representan la misma entidad real.

#### Clave de agrupación:
```
(tipo, subtipo, número, ubicación)
```

Ejemplos:
- `(JUZGADO, CIVIL_MUNICIPAL, 3, Barranquilla)` → un grupo
- `(ALCALDIA, '', '', Cartagena de Indias)` → un grupo
- `(GOBERNACION, '', '', Bolívar)` → un grupo (departamento)

#### Manejo de grupos sin ubicación:
Cuando la ubicación no se pudo extraer, se usa **Levenshtein distance** para separar clusters dentro del grupo:

| Tipo entidad | Umbral de similitud |
|---|---|
| GOBERNACION | 0.90 |
| OTRO | 0.88 |
| JUZGADO, ALCALDIA | 0.85 |
| CAR, RAMA_JUDICIAL | 0.80 |
| Default | 0.85 |

**Proceso**: Se compara el "texto núcleo" (sin ubicaciones ni palabras de relleno) de cada variante con el anchor del cluster. Si la similitud ≥ umbral, se agrega al cluster existente. Si no, se crea un cluster nuevo.

### Etapa 6: Recuperación Fuzzy de Ubicación (Segunda Pasada)

**Objetivo**: Para los grupos que quedaron sin ubicación, intentar recuperarla con matching fuzzy.

**Proceso**:
1. Extraer la porción de "ciudad" del nombre (quitando prefijos como "ALCALDIA DE", "GOBERNACION DEL").
2. Comparar esa porción con todos los municipios/departamentos del DANE usando Levenshtein.
3. Si la similitud ≥ 0.70, asignar la ubicación.
4. Si el municipio/departamento ya existe en otro grupo, **fusionar** los grupos.

**Filtro de basura**: Lista de ~60 palabras (`GARBAGE_WORDS`) que no son ciudades reales: `CERTIFIED, SOCIAL, PROCESO, HACIENDA, SECRETARIA`, etc.

### Etapa 7: Selección de Nombre Canónico (`choose_canonical`)

**Objetivo**: Elegir el nombre más legible para representar cada grupo.

**Algoritmo**:
1. Ordenar variantes por frecuencia (mayor primero).
2. Si entre las variantes con ≥20% de la frecuencia máxima hay alguna en *mixed case* (no todo mayúsculas ni todo minúsculas), preferirla.
3. Si no, usar la variante más frecuente.

### Etapa 8: Generación de Salidas

Se generan los 3 archivos de salida (`entidades.csv`, `variantes_entidades.csv`, `embargos_limpios.csv`) y el archivo auxiliar `municipios.csv`.

---

## 5. Estructura de Salida

### 5.1 `entidades.csv` — Tabla maestra de entidades

| Columna | Tipo | Descripción |
|---|---|---|
| `entidad_id` | int | Identificador único secuencial |
| `nombre_normalizado` | string | Nombre canónico de la entidad |
| `tipo` | string | Clasificación principal (JUZGADO, ALCALDIA, etc.) |
| `subtipo` | string | Sub-clasificación (CIVIL_MUNICIPAL, TRANSITO, etc.) |
| `municipio` | string | Municipio asociado (puede estar vacío) |
| `departamento` | string | Departamento asociado (puede estar vacío) |
| `total_registros` | int | Total de registros de embargos de esta entidad |
| `num_variantes` | int | Cantidad de cadenas raw que se mapearon a esta entidad |

### 5.2 `variantes_entidades.csv` — Mapeo de variantes

| Columna | Tipo | Descripción |
|---|---|---|
| `entidad_id` | int | FK → entidades.entidad_id |
| `nombre_normalizado` | string | Nombre canónico |
| `variante_original` | string | Cadena exacta como aparece en embargos.csv |
| `conteo` | int | Cantidad de veces que aparece esta variante |

### 5.3 `embargos_limpios.csv` — Datos enriquecidos

| Columna | Descripción |
|---|---|
| `embargo_id` | ID del embargo |
| `entidad_remitente_id` | FK → entidades.entidad_id |
| `entidad_bancaria_id` | ID de la entidad bancaria |
| `estado_embargo` | Estado del embargo |
| `numero_oficio` | Número de oficio |
| `fecha_oficio` | Fecha del oficio |
| `fecha_recepcion` | Fecha de recepción |
| `monto` | Monto del embargo |
| `monto_a_embargar` | Monto a embargar del demandado |
| `nombre_demandado` | Nombre del demandado |
| `id_demandado` | Identificación del demandado |
| `tipo_id_demandado` | Tipo de identificación |
| `nombre_remitente` | Nombre normalizado del remitente |
| `direccion_remitente` | Dirección |
| `correo_remitente` | Correo electrónico |

### 5.4 `municipios.csv` — Catálogo de municipios

| Columna | Descripción |
|---|---|
| `municipio_id` | Identificador secuencial |
| `nombre_municipio` | Nombre oficial del municipio |
| `departamento` | Departamento al que pertenece |

---

## 6. Guía para Replicación con Datos Nuevos

### 6.1 Requisitos Previos

- **Python 3.7+** (sin dependencias externas — solo `csv`, `json`, `re`, `unicodedata`, `collections`, `math`).
- Archivo `colombia_municipios.json` con datos DANE actualizados.
- Datos crudos en formato CSV con una columna que contenga el nombre de la entidad.

### 6.2 Paso a Paso para Adaptar a Datos Nuevos

#### Paso 1: Preparar los datos de entrada

Tu CSV debe tener al menos una columna con el nombre de la entidad en texto libre. Anota:
- **Número de columna** (0-indexed) donde está el nombre de la entidad.
- **Encoding** del archivo (típicamente UTF-8 o latin-1).

En `normalize_v4.py`, modificar la sección `[2/7]` para apuntar a tu archivo:

```python
# CAMBIAR ESTO:
with open('TU_ARCHIVO.csv', 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.reader(f)
    next(reader)  # saltar header
    for row in reader:
        if len(row) > COLUMNA_ENTIDAD:
            ent = row[COLUMNA_ENTIDAD].strip()
            raw_entities[ent] = raw_entities.get(ent, 0) + 1
```

#### Paso 2: Explorar las entidades raw

Antes de normalizar, generar un resumen de las cadenas únicas:

```python
# Guardar para inspección manual
import csv
sorted_entities = sorted(raw_entities.items(), key=lambda x: -x[1])
with open('unique_entities_raw.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['entidad_raw', 'count'])
    for ent, count in sorted_entities:
        writer.writerow([ent, count])
```

**Inspeccionar manualmente** las 200 entidades más frecuentes para identificar:
- Tipos de entidades presentes.
- Errores tipográficos comunes.
- Patrones de nombres de ciudades.

#### Paso 3: Ajustar los patrones de clasificación

En la función `_detect_type()`, agregar/modificar patrones regex según los tipos de entidad de tus datos. El orden importa: **más específicos primero**.

#### Paso 4: Actualizar aliases de ciudades

Revisar el diccionario `CITY_ALIASES` y agregar errores tipográficos encontrados en tus datos:

```python
CITY_ALIASES = {
    # Agregar variantes comunes en tus datos
    'MEDILLIN': ('Medellín', 'Antioquia'),
    'TU_TYPO': ('Ciudad correcta', 'Departamento'),
}
```

**Técnica para descubrir typos**: Después de una primera corrida, revisar las entidades en el grupo `OTRO` o sin ubicación para identificar variantes no mapeadas.

#### Paso 5: Ajustar resolución de ambigüedades

Si tus datos tienen concentración geográfica diferente, ajustar `AMBIGUOUS_RESOLUTION`:

```python
AMBIGUOUS_RESOLUTION = {
    'FLORENCIA': ('Florencia', 'Caquetá'),  # Cambiar según tu contexto
}
```

#### Paso 6: Ejecutar normalización

```bash
python3 normalize_v4.py
```

Tiempo estimado: ~2-5 minutos para ~1M registros.

#### Paso 7: Validar resultados

Ejecutar el script de diagnóstico:
```bash
python3 diagnostico.py
```

Verificar:
- Que el merge ratio sea razonable (>70% es bueno).
- Que las entidades más grandes tengan municipio y departamento.
- Que el porcentaje de OTRO sea bajo (<5%).

#### Paso 8: Iterar

El proceso es **iterativo**. En cada corrida:
1. Identificar entidades en OTRO → agregar patrones a `_detect_type`.
2. Identificar entidades sin ubicación → agregar aliases a `CITY_ALIASES`.
3. Identificar agrupaciones incorrectas (sobre-merge o sub-merge) → ajustar umbrales de Levenshtein.

### 6.3 Checklist de Adaptación

- [ ] Configurar ruta y columna del CSV de origen.
- [ ] Actualizar `colombia_municipios.json` si hay nuevos municipios.
- [ ] Inspeccionar entidades raw y documentar tipos de entidad.
- [ ] Agregar/modificar patrones en `_detect_type()` para nuevos tipos.
- [ ] Agregar errores tipográficos de ciudades a `CITY_ALIASES`.
- [ ] Agregar errores tipográficos de departamentos a `DEPT_ALIASES`.
- [ ] Ajustar `AMBIGUOUS_RESOLUTION` según contexto geográfico.
- [ ] Agregar nuevas palabras basura a `GARBAGE_WORDS`.
- [ ] Ejecutar y validar con `diagnostico.py`.
- [ ] Iterar hasta alcanzar tasas de normalización aceptables.
- [ ] Generar dashboard con `build_dashboard_v2.py`.

### 6.4 Cómo Generar el Dashboard

```bash
python3 build_dashboard_v2.py
# Genera dashboard.html — abrir en cualquier navegador
```

---

## 7. Problemas Conocidos y Limitaciones

### 7.1 Juzgados sin Ciudad (~33.5% sin municipio)
Los juzgados genéricos como "JUZGADO PRIMERO CIVIL MUNICIPAL" no contienen el nombre de la ciudad. Se agrupan en un solo cluster genérico que puede mezclar juzgados de diferentes ciudades. **Solución potencial**: Cruzar con la columna `ciudad` del `embargos.csv` original.

### 7.2 Entidades Nacionales
DIAN, SENA, Ministerios y Superintendencias son entidades de alcance nacional. No incluyen municipio por diseño.

### 7.3 Errores Tipográficos Raros
Algunos errores aparecen solo 1-2 veces y no se capturan con los aliases. Representan un volumen mínimo de registros (<0.1%).

### 7.4 Gobernaciones sin Departamento (~25.7%)
Algunas variantes de gobernaciones contienen errores tan severos que ni el matching fuzzy puede recuperar el departamento.

### 7.5 "Bogotá, D.C." como Departamento
Se usa como departamento adicional fuera del catálogo DANE estándar de 32 departamentos, ya que Bogotá tiene un estatus administrativo especial.

### 7.6 Umbral de Similitud Fijo
El umbral de Levenshtein es estático por tipo. Un umbral adaptativo por volumen de datos podría mejorar la precisión en clusters pequeños.

---

## Evolución del Pipeline

El proceso se desarrolló en 4 versiones incrementales:

| Versión | Archivo | Mejora Principal |
|---|---|---|
| v1 | `normalize_entities.py` | Limpieza de texto, fingerprint-based grouping |
| v2 | `normalize_v2.py` | Clasificación por tipo de entidad, reglas de merge |
| v3 | `normalize_v3.py` | Integración DANE, extracción de municipios, Levenshtein |
| **v4** | **`normalize_v4.py`** | **Producción**: Two-pass location, gobernaciones por depto, aliases extensos, fuzzy recovery |

---

*Documento generado: 1 de abril de 2026*  
*Pipeline: normalize_v4.py*  
*Datos: embargos.csv → entidades.csv*
