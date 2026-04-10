# Desarrollo de software A-GD-04
## Documentación de Datasets Maestros — VERSION 01
### ABR-2026

---

# Documentación de Datasets Maestros — Modelo CAEM

## Tabla de contenido

1. [VISIÓN GENERAL](#1-visión-general)
2. [DATASET: dim_departamentos](#2-dataset-dim_departamentos)
3. [DATASET: dim_municipios](#3-dataset-dim_municipios)
4. [DATASET: dim_entidades](#4-dataset-dim_entidades)
5. [DATASET: dim_variantes](#5-dataset-dim_variantes)
6. [DATASET: fact_oficios](#6-dataset-fact_oficios)
7. [DATASETS DE SOPORTE](#7-datasets-de-soporte)
8. [GUÍA DE USO Y CONSULTAS FRECUENTES](#8-guía-de-uso-y-consultas-frecuentes)

---

## Historial de versiones

| Fecha | Descripción | Autor |
|---|---|---|
| 2026-04-09 | Documentación inicial de datasets maestros | Geiner Martínez |

---

## 1. VISIÓN GENERAL

El modelo de datos está compuesto por **5 datasets principales** (4 dimensiones + 1 tabla de hechos) y **3 datasets de soporte** (fuentes y procesados intermedios). Todos se entregan en formato CSV (delimitador `,`, encoding UTF-8).

### Resumen de datasets

| Dataset | Tipo | Registros | Campos | Tamaño aprox. | Ubicación |
|---|---|---|---|---|---|
| dim_departamentos.csv | Dimensión | 33 | 2 | 1 KB | datos/modelo_final/ |
| dim_municipios.csv | Dimensión | 1,104 | 3 | 30 KB | datos/modelo_final/ |
| dim_entidades.csv | Dimensión (maestro) | 8,548 | 8 | 750 KB | datos/modelo_final/ |
| dim_variantes.csv | Dimensión (puente) | 38,289 | 5 | 4 MB | datos/modelo_final/ |
| fact_oficios.csv | Hechos | 916,425 | 19 | ~200 MB | datos/modelo_final/ |
| entidades.csv | Procesado (maestro plano) | 8,548 | 8 | 700 KB | datos/procesados/ |
| variantes_entidades.csv | Procesado (mapeo plano) | 38,289 | 4 | 3.5 MB | datos/procesados/ |
| colombia_municipios.json | Fuente (referencia DANE) | 1,104 municipios | 3 | 50 KB | datos/fuentes/ |

---

## 2. DATASET: dim_departamentos

### Información general

| Atributo | Valor |
|---|---|
| **Archivo** | `datos/modelo_final/dim_departamentos.csv` |
| **Propósito** | Maestro de departamentos de Colombia. Nivel superior de la jerarquía geográfica. |
| **Fuente de origen** | Base de datos DANE (`colombia_municipios.json`) |
| **Registros** | 33 (32 departamentos + Bogotá D.C.) |
| **Campos** | 2 |
| **Encoding** | UTF-8 |
| **Delimitador** | Coma (`,`) |
| **Frecuencia de actualización** | Estático (solo cambia si el DANE modifica la división político-administrativa) |

### Estructura de campos

| # | Campo | Tipo | Tamaño max | Obligatorio | Clave | Descripción |
|---|---|---|---|---|---|---|
| 1 | departamento_id | Entero | — | Sí | PK | ID secuencial (1-33) |
| 2 | nombre | Texto | 100 chars | Sí | UNIQUE | Nombre oficial del departamento |

### Modo de uso

- **JOIN principal:** Se usa como tabla padre en JOINs con `dim_municipios`, `dim_entidades` y `fact_oficios` por el campo `departamento_id`.
- **Agrupación:** Permite agrupar oficios de embargo por departamento para análisis geográfico de nivel macro.
- **No modificar:** Los IDs son referenciados por las demás tablas. Cambiar un ID rompe la integridad referencial.

### Muestra de datos

```csv
departamento_id,nombre
1,Amazonas
2,Antioquia
3,Arauca
4,Atlántico
5,Bolívar
6,Boyacá
```

---

## 3. DATASET: dim_municipios

### Información general

| Atributo | Valor |
|---|---|
| **Archivo** | `datos/modelo_final/dim_municipios.csv` |
| **Propósito** | Maestro de municipios de Colombia. Nivel inferior de la jerarquía geográfica, vinculado a un departamento. |
| **Fuente de origen** | Base de datos DANE (`colombia_municipios.json`) |
| **Registros** | 1,104 |
| **Campos** | 3 |
| **Dependencia** | Requiere `dim_departamentos` (FK: departamento_id) |

### Estructura de campos

| # | Campo | Tipo | Tamaño max | Obligatorio | Clave | Descripción |
|---|---|---|---|---|---|---|
| 1 | municipio_id | Entero | — | Sí | PK | ID secuencial (1-1104) |
| 2 | nombre | Texto | 150 chars | Sí | — | Nombre oficial del municipio según DANE |
| 3 | departamento_id | Entero | — | Sí | FK → dim_departamentos | Departamento al que pertenece |

### Modo de uso

- **JOIN geográfico:** Permite vincular entidades y oficios a su municipio y, a través de la FK, al departamento.
- **Filtros:** Ideal para filtrar oficios por ciudad o municipio específico.
- **Jerarquía:** Para obtener el departamento de un municipio: `JOIN dim_departamentos USING (departamento_id)`.

### Muestra de datos

```csv
municipio_id,nombre,departamento_id
1,Leticia,1
2,Puerto Nariño,1
3,Abejorral,2
60,Medellín,2
167,Cartagena de Indias,6
```

---

## 4. DATASET: dim_entidades

### Información general

| Atributo | Valor |
|---|---|
| **Archivo** | `datos/modelo_final/dim_entidades.csv` |
| **Propósito** | **Dataset maestro principal.** Contiene las entidades remitentes de oficios de embargo, normalizadas, clasificadas y geolocalizadas. Es el resultado central del proceso de normalización. |
| **Fuente de origen** | Generado por `normalize_v4.py` + `build_modelo.py` a partir de embargos.csv |
| **Registros** | 8,548 |
| **Campos** | 8 |
| **Dependencias** | FK a dim_municipios y dim_departamentos |

### Estructura de campos

| # | Campo | Tipo | Tamaño max | Obligatorio | Clave | Descripción | Ejemplo |
|---|---|---|---|---|---|---|---|
| 1 | entidad_id | Entero | — | Sí | PK | Identificador único de la entidad | 1 |
| 2 | nombre_normalizado | Texto | 500 chars | Sí | — | Nombre canónico tras normalización | JUZGADO 1 CIVIL MUNICIPAL DE MEDELLIN |
| 3 | tipo | Texto | 50 chars | No | IDX | Categoría institucional (25 valores posibles) | JUZGADO |
| 4 | subtipo | Texto | 50 chars | No | — | Subcategoría (varía según tipo) | CIVIL_MUNICIPAL |
| 5 | municipio_id | Entero | — | No | FK → dim_municipios | Municipio de jurisdicción (NULL si nacional o no determinado) | 60 |
| 6 | departamento_id | Entero | — | No | FK → dim_departamentos | Departamento de jurisdicción | 2 |
| 7 | total_registros | Entero | — | Sí | — | Cantidad de oficios que emitió esta entidad | 5234 |
| 8 | num_variantes | Entero | — | Sí | — | Cantidad de variantes textuales agrupadas en esta entidad | 12 |

### Distribución por tipo

| Tipo | Registros | % del total | Con municipio | Con departamento |
|---|---|---|---|---|
| JUZGADO | 6,253 | 73.2% | 66.5% | 72.8% |
| OTRO | 873 | 10.2% | 17.4% | 20.1% |
| ALCALDIA | 330 | 3.9% | 70.4% | 75.5% |
| SECRETARIA | 208 | 2.4% | 44.3% | 54.7% |
| MUNICIPIO | 153 | 1.8% | 84.7% | 90.4% |
| MINISTERIO | 65 | 0.8% | 8.1% | 9.5% |
| SUPERINTENDENCIA | 62 | 0.7% | 12.7% | 12.7% |
| Demás tipos | 604 | 7.1% | Variable | Variable |

### Modo de uso

- **Búsqueda por nombre:** Consultar entidades por `nombre_normalizado` (LIKE o FULLTEXT).
- **Filtro por tipo:** Filtrar por `tipo` para obtener solo juzgados, o solo alcaldías, etc.
- **Ranking:** Ordenar por `total_registros DESC` para obtener entidades con más actividad.
- **Geográfico:** JOIN con dim_municipios/dim_departamentos para análisis por ubicación.
- **Resolución de variantes:** Usar dim_variantes para encontrar qué entidad corresponde a un texto crudo.

### Consideraciones especiales

- Las **Gobernaciones** tienen `municipio_id = NULL` de forma intencional (su jurisdicción es departamental).
- Las **entidades nacionales** (DIAN, SENA, Superintendencias) pueden tener tanto `municipio_id` como `departamento_id` en NULL.
- El campo `total_registros` es **denormalizado**: reproducible con `SELECT COUNT(*) FROM fact_oficios WHERE entidad_remitente_id = X`.

---

## 5. DATASET: dim_variantes

### Información general

| Atributo | Valor |
|---|---|
| **Archivo** | `datos/modelo_final/dim_variantes.csv` |
| **Propósito** | Tabla puente de trazabilidad. Mapea cada variante textual original (tal como fue escrita en el sistema fuente) a la entidad normalizada correspondiente. |
| **Fuente de origen** | Generado por `normalize_v4.py` + `build_modelo.py` |
| **Registros** | 38,289 |
| **Campos** | 5 |
| **Dependencia** | FK a dim_entidades (entidad_id) |

### Estructura de campos

| # | Campo | Tipo | Tamaño max | Obligatorio | Clave | Descripción | Ejemplo |
|---|---|---|---|---|---|---|---|
| 1 | variante_id | Entero | — | Sí | PK | ID secuencial de la variante | 1 |
| 2 | entidad_id | Entero | — | Sí | FK → dim_entidades | Entidad normalizada a la que pertenece | 1 |
| 3 | nombre_normalizado | Texto | 500 chars | No | — | Nombre normalizado (denormalizado para consulta rápida) | DATT CARTAGENA |
| 4 | variante_original | Texto | 500 chars | Sí | — | Texto exacto como fue ingresado en el sistema fuente | DEPARTAMENTO ADMINISTRATIVO DE TRANSITO Y TRANSPORTE DE CARTAGENA - DATT |
| 5 | conteo | Entero | — | Sí | — | Número de ocurrencias de esta variante en los datos fuente | 101854 |

### Modo de uso

- **Resolución de entidad:** Cuando llega un nuevo oficio con texto de entidad remitente, buscar en `variante_original` para obtener el `entidad_id`:
  ```sql
  SELECT entidad_id FROM dim_variantes 
  WHERE variante_original = 'TEXTO DEL OFICIO';
  ```
- **Auditoría:** Ver todas las variantes de una entidad:
  ```sql
  SELECT variante_original, conteo FROM dim_variantes 
  WHERE entidad_id = 1 ORDER BY conteo DESC;
  ```
- **Estadísticas de calidad:** El campo `conteo` permite identificar las variantes más frecuentes y las más raras (posibles errores de digitación).

### Distribución

| Métrica | Valor |
|---|---|
| Total variantes | 38,289 |
| Total entidades referenciadas | 8,548 |
| Promedio variantes por entidad | 4.5 |
| Máximo variantes por entidad | 167 (DATT Cartagena) |
| Mínimo variantes por entidad | 1 |

---

## 6. DATASET: fact_oficios

### Información general

| Atributo | Valor |
|---|---|
| **Archivo** | `datos/modelo_final/fact_oficios.csv` |
| **Propósito** | **Tabla de hechos central.** Cada fila es un oficio de embargo único procesado por la entidad bancaria. Contiene todas las métricas y claves foráneas a las dimensiones. |
| **Fuente de origen** | Generado por `build_modelo.py` a partir de `embargos_final.csv` |
| **Registros** | 916,425 (post-deduplicación: 916,425 únicos) |
| **Campos** | 19 |
| **Tamaño** | ~200 MB |
| **Dependencias** | FK a dim_entidades, dim_municipios, dim_departamentos |
| **Nota** | Este archivo está excluido del repositorio Git por su tamaño. Se regenera con `build_modelo.py`. |

### Estructura de campos

| # | Campo | Tipo | Tamaño max | Obligatorio | Clave | Descripción |
|---|---|---|---|---|---|---|
| 1 | oficio_id | Texto | 20 chars | Sí | PK | ID único del oficio (del sistema fuente) |
| 2 | entidad_remitente_id | Entero | — | No | FK → dim_entidades | Entidad que emitió el embargo |
| 3 | entidad_bancaria_id | Entero | — | No | — | Banco destinatario (1-5) |
| 4 | estado | Texto | 30 chars | No | IDX | Estado de procesamiento |
| 5 | numero_oficio | Texto | 100 chars | No | — | Referencia del oficio |
| 6 | fecha_oficio | Fecha | — | No | IDX | Fecha de emisión (YYYY-MM-DD) |
| 7 | fecha_recepcion | Fecha | — | No | — | Fecha de recepción en el banco |
| 8 | titulo_embargo | Texto | 50 chars | No | — | Tipo de embargo |
| 9 | titulo_orden | Texto | 50 chars | No | — | Tipo de orden judicial |
| 10 | monto | Decimal | 18,2 | No | — | Monto del embargo (COP) |
| 11 | monto_a_embargar | Decimal | 18,2 | No | — | Monto específico a embargar (COP) |
| 12 | nombre_demandado | Texto | 300 chars | No | — | Nombre del demandado |
| 13 | id_demandado | Texto | 30 chars | No | — | Número de identificación |
| 14 | tipo_id_demandado | Texto | 20 chars | No | — | Tipo de documento (CC, NIT, CE, TI, PASAPORTE) |
| 15 | direccion_remitente | Texto | 500 chars | No | — | Dirección de la entidad remitente |
| 16 | correo_remitente | Texto | 200 chars | No | — | Email de contacto del remitente |
| 17 | nombre_funcionario | Texto | 200 chars | No | — | Funcionario firmante |
| 18 | municipio_id | Entero | — | No | FK → dim_municipios | Municipio de origen del oficio |
| 19 | departamento_id | Entero | — | No | FK → dim_departamentos | Departamento de origen |

### Métricas clave

| Métrica | Valor |
|---|---|
| Total oficios | 916,425 |
| Oficios con entidad_remitente_id | ~99.7% |
| Oficios con municipio_id | ~72% |
| Oficios con departamento_id | ~79% |
| Rango de fechas | Variable (según datos fuente) |
| Montos | Pesos colombianos (COP), DECIMAL(18,2) |

### Modo de uso

- **Análisis temporal:**
  ```sql
  SELECT DATE_FORMAT(fecha_oficio, '%Y-%m') as mes, COUNT(*) as oficios
  FROM fact_oficios GROUP BY mes ORDER BY mes;
  ```
- **Top entidades por volumen:**
  ```sql
  SELECT e.nombre_normalizado, e.tipo, COUNT(*) as total
  FROM fact_oficios f JOIN dim_entidades e ON f.entidad_remitente_id = e.entidad_id
  GROUP BY e.entidad_id ORDER BY total DESC LIMIT 20;
  ```
- **Distribución geográfica:**
  ```sql
  SELECT d.nombre as departamento, COUNT(*) as oficios
  FROM fact_oficios f JOIN dim_departamentos d ON f.departamento_id = d.departamento_id
  GROUP BY d.departamento_id ORDER BY oficios DESC;
  ```
- **Montos por tipo de entidad:**
  ```sql
  SELECT e.tipo, SUM(f.monto) as monto_total, AVG(f.monto) as monto_promedio
  FROM fact_oficios f JOIN dim_entidades e ON f.entidad_remitente_id = e.entidad_id
  WHERE f.monto IS NOT NULL
  GROUP BY e.tipo ORDER BY monto_total DESC;
  ```

---

## 7. DATASETS DE SOPORTE

### 7.1 entidades.csv (procesado intermedio)

| Atributo | Valor |
|---|---|
| **Archivo** | `datos/procesados/entidades.csv` |
| **Propósito** | Versión plana (denormalizada) del maestro de entidades. Contiene nombres de municipio y departamento en texto en lugar de IDs. |
| **Registros** | 8,548 |
| **Campos** | 8: entidad_id, nombre_normalizado, tipo, subtipo, municipio, departamento, total_registros, num_variantes |

**Diferencia con dim_entidades.csv:** En este archivo los campos geográficos son texto (`municipio = "Medellín"`, `departamento = "Antioquia"`), mientras que en dim_entidades son IDs numéricos (FK). Útil para inspección visual rápida sin necesidad de JOINs.

### 7.2 variantes_entidades.csv (procesado intermedio)

| Atributo | Valor |
|---|---|
| **Archivo** | `datos/procesados/variantes_entidades.csv` |
| **Propósito** | Versión simplificada del mapeo de variantes (sin variante_id ni nombre_normalizado redundante). |
| **Registros** | 38,289 |
| **Campos** | 4: entidad_id, nombre_normalizado, variante_original, conteo |

**Diferencia con dim_variantes.csv:** No tiene `variante_id` propio (se puede generar con ROW_NUMBER). Mismo contenido pero formato más compacto.

### 7.3 colombia_municipios.json (fuente DANE)

| Atributo | Valor |
|---|---|
| **Archivo** | `datos/fuentes/colombia_municipios.json` |
| **Propósito** | Base de referencia oficial del DANE con la división político-administrativa de Colombia. |
| **Estructura** | Array de objetos JSON, cada uno con: `id` (int), `departamento` (string), `ciudades` (array de strings) |
| **Registros** | 33 departamentos, 1,104 municipios |

```json
[
  {"id": 0, "departamento": "Amazonas", "ciudades": ["Leticia", "Puerto Nariño"]},
  {"id": 1, "departamento": "Antioquia", "ciudades": ["Abejorral", "Abriaquí", "Medellín", ...]}
]
```

### 7.4 municipios.csv (fuente auxiliar)

| Atributo | Valor |
|---|---|
| **Archivo** | `datos/fuentes/municipios.csv` |
| **Propósito** | Tabla auxiliar de municipios generada durante la normalización. |
| **Registros** | 894 |
| **Campos** | 3: municipio_id, nombre_municipio, departamento |

---

## 8. GUÍA DE USO Y CONSULTAS FRECUENTES

### 8.1 ¿Cómo cargar los datos en MySQL?

```bash
# Configurar variables de entorno
export DB_HOST=127.0.0.1
export DB_USER=producto
export DB_PASSWORD=<contraseña>
export DB_NAME=ETL

# Ejecutar carga
python scripts/upload_to_mysql.py
```

### 8.2 ¿Cómo encontrar la entidad normalizada de un texto crudo?

```sql
-- Búsqueda exacta
SELECT v.entidad_id, e.nombre_normalizado, e.tipo
FROM dim_variantes v
JOIN dim_entidades e ON v.entidad_id = e.entidad_id
WHERE v.variante_original = 'JUZGADO PRIMERO CIVIL MUNICIPAL DE MEDELLIN';

-- Búsqueda parcial
SELECT DISTINCT e.entidad_id, e.nombre_normalizado, e.tipo
FROM dim_variantes v
JOIN dim_entidades e ON v.entidad_id = e.entidad_id
WHERE v.variante_original LIKE '%MEDELLIN%' AND e.tipo = 'JUZGADO';
```

### 8.3 ¿Cómo obtener todas las variantes de una entidad?

```sql
SELECT variante_original, conteo
FROM dim_variantes
WHERE entidad_id = 1
ORDER BY conteo DESC;
```

### 8.4 ¿Cómo generar un reporte de oficios por departamento y tipo?

```sql
SELECT d.nombre AS departamento, e.tipo, COUNT(*) AS total_oficios, SUM(f.monto) AS monto_total
FROM fact_oficios f
JOIN dim_entidades e ON f.entidad_remitente_id = e.entidad_id
JOIN dim_departamentos d ON f.departamento_id = d.departamento_id
GROUP BY d.nombre, e.tipo
ORDER BY d.nombre, total_oficios DESC;
```

### 8.5 ¿Cómo regenerar el modelo desde los datos fuente?

```bash
# Ejecutar el pipeline completo en orden
python scripts/normalize_v4.py           # Paso 1: Normaliza entidades
python scripts/cruce_municipios.py       # Paso 2: Enriquece geográficamente
python scripts/restructure_embargos.py   # Paso 3: Limpia tabla de hechos
python scripts/build_modelo.py           # Paso 4: Genera modelo dimensional
```
