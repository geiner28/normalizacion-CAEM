# normalizacion-CAEM

Pipeline ETL de normalización de entidades y construcción del modelo dimensional de embargos para el CAEM.

## Descripción

Transforma ~929,690 registros de oficios de embargo con 38,289 variantes textuales de entidades remitentes en un modelo dimensional estrella con 8,548 entidades normalizadas, clasificadas por tipo/subtipo y geolocalizadas a nivel municipal.

## Modelo de datos

| Tabla | Registros | Descripción |
|---|---|---|
| `dim_departamentos` | 32 | Departamentos colombianos |
| `dim_municipios` | 1,104 | Municipios con FK a departamento |
| `dim_entidades` | 8,548 | Entidades normalizadas (tipo, subtipo, ubicación) |
| `dim_variantes` | 38,289 | Variantes textuales → entidad normalizada |
| `fact_oficios` | 916,425 | Oficios de embargo (tabla de hechos) |

## Pipeline de ejecución

```bash
# 1. Normalización de entidades
python scripts/normalize_v4.py

# 2. Enriquecimiento geográfico
python scripts/cruce_municipios.py

# 3. Reestructuración de embargos
python scripts/restructure_embargos.py

# 4. Construcción del modelo dimensional
python scripts/build_modelo.py

# 5. Carga a MySQL
export DB_HOST=127.0.0.1 DB_USER=producto DB_PASSWORD=<password> DB_NAME=ETL
python scripts/upload_to_mysql.py

# 6. Deduplicación
python scripts/dedup_oficios.py

# 7. Diagnóstico de calidad
python scripts/diagnostico_calidad.py
```

## Estructura

```
datos/fuentes/          → Datos de entrada (embargos.csv, demandado.csv, DANE)
datos/procesados/       → Salidas intermedias (entidades normalizadas)
datos/modelo_final/     → Modelo dimensional (dims + fact + schema.sql)
scripts/                → Pipeline de procesamiento
reportes/               → Informes de metodología y calidad
```

## Requisitos

- Python 3.10+
- PyMySQL (para carga a BD)
