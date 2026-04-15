# Informe de Exploración: Correlación Embargo / Desembargo

## Desarrollo de software A-GD-04 — ABR-2026

---

## Tabla de contenido

1. [Objetivo](#1-objetivo)
2. [Fuentes de datos utilizadas](#2-fuentes-de-datos-utilizadas)
3. [Volumetría del sistema](#3-volumetría-del-sistema)
4. [Hipótesis evaluadas](#4-hipótesis-evaluadas)
5. [Método de correlación ejecutado](#5-método-de-correlación-ejecutado)
6. [Resultados obtenidos](#6-resultados-obtenidos)
7. [Observaciones y descubrimientos](#7-observaciones-y-descubrimientos)
8. [Conclusiones](#8-conclusiones)
9. [Recomendaciones](#9-recomendaciones)

---

## 1. Objetivo

Validar e identificar la regla de negocio y de base de datos para conectar los eventos de embargo y desembargo dentro de la BD espejo (`pyc_embargos`), utilizando los datos crudos exportados en CSV.

---

## 2. Fuentes de datos utilizadas

| Archivo | Registros | Descripción |
|---|---|---|
| `datos/fuentes/embargos.csv` | 929,690 | Tabla principal con metadata de eventos (embargos, desembargos, requerimientos) |
| `datos/fuentes/demandado.csv` | 920,691 | Tabla de personas/entidades afectadas, vinculada por `embargo_id` |

### Campos clave explorados

**Tabla embargos (44 columnas):**
- `id` — Identificador único del registro
- `tipo_documento` — Tipo de evento: EMBARGO, DESEMBARGO, REQUERIMIENTO, NO_PROCESABLE
- `secondary_id` — Campo candidato a llave de autorreferencia
- `entidad_remitente` — Entidad que origina el oficio
- `referencia` — Número de referencia o radicado del proceso
- `oficio` — Número del oficio
- `radicado_banco` — Radicado interno del banco
- `embargo_path` — Ruta al documento PDF del oficio
- `response_path` — Ruta al documento JSON de respuesta
- `tipo_embargo` — JUDICIAL o COACTIVO
- `create_at` — Fecha de creación del registro

**Tabla demandado (19 columnas):**
- `embargo_id` — Llave foránea hacia `embargos.id`
- `identificacion` — Cédula o NIT del demandado
- `nombres` — Nombre del demandado
- `expediente` — Número de expediente procesal

---

## 3. Volumetría del sistema

| Tipo de documento | Registros | Porcentaje |
|---|---|---|
| DESEMBARGO | 460,088 | 49.5% |
| EMBARGO | 412,124 | 44.3% |
| REQUERIMIENTO | 47,554 | 5.1% |
| NO_PROCESABLE | 9,924 | 1.1% |
| **Total** | **929,690** | **100%** |

---

## 4. Hipótesis evaluadas

### 4.1 Por Resolución/Oficio (`oficio` o `referencia`)

**Descripción:** Correlacionar eventos buscando coincidencias exactas en el número de oficio o referencia.

**Resultado:** Descartada. Campos de texto libre con alta variabilidad en la digitación. No garantiza unicidad ni consistencia.

### 4.2 Por Identificación del demandado (`identificacion`)

**Descripción:** Agrupar por cédula/NIT del demandado para buscar personas con embargo y desembargo.

**Resultado:** Descartada. Una misma persona puede tener múltiples procesos simultáneos, generando falsos positivos al emparejar.

### 4.3 Por combinación compuesta (Fecha + Entidad + Persona/Expediente)

**Descripción:** Regla compuesta cruzando `identificacion`, `expediente` y `entidad_remitente`.

**Resultado:** Válida conceptualmente para casos aislados, pero computacionalmente costosa y sin garantía de unicidad en el histórico completo.

### 4.4 Por autorreferencia vía `secondary_id` (hipótesis principal)

**Descripción:** El campo `secondary_id` en un registro de tipo DESEMBARGO almacena el `id` del EMBARGO original, creando un modelo de autorreferencia padre-hijo.

**Resultado:** Se ejecutó la validación completa. Los resultados refutan la hipótesis como llave de negocio embargo→desembargo. Ver sección 7.

---

## 5. Método de correlación ejecutado

### 5.1 Proceso

Se implementó un self-join en Python sobre los datos crudos (`embargos.csv`):

1. **Carga de embargos:** Se indexaron los 412,124 registros de tipo `EMBARGO` por su campo `id` en un diccionario.
2. **Carga de desembargos:** Se recopilaron los 143,288 registros de tipo `DESEMBARGO` que tienen `secondary_id` no vacío.
3. **Correlación:** Para cada desembargo, se buscó si su `secondary_id` apuntaba a un `id` existente en el diccionario de embargos.
4. **Enriquecimiento:** Se cruzó con la tabla `demandado.csv` para obtener nombre, identificación y expediente del demandado asociado a cada embargo.

### 5.2 Campos incluidos en el output

Para cada par correlacionado se extrajeron:

| Lado Embargo | Lado Desembargo | Demandado |
|---|---|---|
| embargo_id | desembargo_id | demandado_nombre |
| tipo_embargo | entidad_desembargo | demandado_identificacion |
| entidad_embargo | referencia_desembargo | demandado_expediente |
| referencia_embargo | fecha_desembargo | |
| fecha_embargo | embargo_path_desembargo | |
| embargo_path_embargo | radicado_banco_desembargo | |
| radicado_banco_embargo | response_path_desembargo | |
| response_path_embargo | oficio_desembargo | |
| oficio_embargo | | |

### 5.3 Output generado

- **Archivo:** `datos/procesados/correlacion_embargo_desembargo_30_casos.csv`
- **Registros:** 30 pares embargo-desembargo correlacionados
- **Columnas:** 21

---

## 6. Resultados obtenidos

### 6.1 Cobertura del campo `secondary_id`

| Métrica | Valor |
|---|---|
| Total de desembargos | 460,088 |
| Desembargos con `secondary_id` poblado | 143,288 (31.1%) |
| Desembargos sin `secondary_id` | 316,800 (68.9%) |

### 6.2 Resultado del cruce

| Métrica | Valor |
|---|---|
| Desembargos con `secondary_id` que apunta a un EMBARGO existente | 62,700 (43.8% de los 143,288) |
| Desembargos con `secondary_id` que apunta a un registro que NO es EMBARGO | ~80,588 (56.2%) |

Esto significa que `secondary_id` referencia otros tipos de eventos (REQUERIMIENTO, otros DESEMBARGO), confirmando que es una llave de ciclo procesal genérica, no exclusiva de la relación embargo→desembargo.

### 6.3 Cobertura total

| Nivel | Desembargos cubiertos | % del total (460,088) |
|---|---|---|
| Con `secondary_id` → EMBARGO válido | 62,700 | 13.6% |
| Con `secondary_id` → otro tipo | ~80,588 | 17.5% |
| Sin `secondary_id` | 316,800 | 68.9% |

---

## 7. Observaciones y descubrimientos

### 7.1 HALLAZGO CRÍTICO: Las entidades no coinciden entre embargo y desembargo

En los 30 casos correlacionados, **la entidad remitente del embargo y la del desembargo son siempre diferentes**. Ejemplos representativos:

| Caso | Entidad del Embargo | Entidad del Desembargo |
|---|---|---|
| 1 | Juzgado 35 Civil Municipal | Juzgado 1 Civil Municipal |
| 3 | Juzgado 20 Pequeñas Causas | Dpto. Tránsito Cartagena |
| 8 | Juzgado 4 Civil del Circuito | DIAN |
| 11 | Juzgado 5 Civil del Circuito | Alcaldía de Aguazul |
| 22 | Alcaldía de Cartagena | Dpto. Tránsito Cartagena |
| 24 | Alcaldía de Palmira | DIAN |
| 27 | DIAN | Juzgado 2 Pequeñas Causas |
| 30 | Juzgado 54 Civil Municipal Bogotá | Superintendencia de Sociedades |

**Interpretación:** Si `secondary_id` fuera una llave de negocio real (embargo padre → desembargo hijo del mismo proceso), la entidad remitente debería coincidir o al menos ser del mismo despacho judicial. El hecho de que **nunca coincida** indica que `secondary_id` **no vincula eventos del mismo proceso judicial/administrativo**.

### 7.2 Patrón de IDs consecutivos

Los IDs de embargo y desembargo correlacionados son casi siempre consecutivos:

| Embargo ID | Desembargo ID | Diferencia |
|---|---|---|
| 23063000008 | 23063000009 | +1 |
| 23063000019 | 23063000020 | +1 |
| 23063000068 | 23063000069 | +1 |
| 23063000153 | 23063000154 | +1 |

**Interpretación:** El sistema asigna IDs secuenciales durante el procesamiento por lotes. El `secondary_id` parece referenciar al **registro inmediatamente anterior en la cola de ingesta**, no al embargo generador real. Es un artefacto del orden de procesamiento del sistema, no una referencia procesal.

### 7.3 Misma fecha de creación

Todos los 30 casos tienen la misma fecha de creación para embargo y desembargo (`2023-06-30`). Esto refuerza la hipótesis de procesamiento por lotes: los documentos fueron cargados en la misma sesión batch, y `secondary_id` simplemente apunta al registro previo en la cola.

### 7.4 Demandados distintos entre embargo y desembargo vinculados

Al cruzar con la tabla `demandado`, los nombres e identificaciones de los demandados del embargo y del desembargo vinculado por `secondary_id` corresponden a personas completamente diferentes, confirmando que no hay relación procesal real.

### 7.5 El campo `secondary_id` sí tiene uso real — pero no para esta correlación

El campo `secondary_id` se usa para vincular **todo el ciclo procesal** de forma genérica (Requerimiento → Embargo, Embargo → otro Embargo, etc.), pero su población es inconsistente (solo 31% de desembargos lo tienen) y no garantiza que apunte específicamente al embargo padre.

### 7.6 Impacto en el modelo actual (`fact_oficios`)

El modelo dimensional construido en el ETL **no preservó** los campos `secondary_id` ni `referencia` durante la transformación. Esto significa que incluso si se encontrara una regla de correlación válida, el modelo actual no tiene los campos necesarios para implementarla.

| Campo original | ¿Existe en `fact_oficios`? |
|---|---|
| `secondary_id` | No |
| `referencia` | No |
| `tipo_documento` | Sí (como `titulo_orden`) |
| `id` | Sí (como `oficio_id`) |

---

## 8. Conclusiones

1. **La hipótesis de correlación vía `secondary_id` no es válida** como llave de negocio embargo→desembargo. El campo es un artefacto del sistema de ingesta por lotes, no una referencia procesal.

2. **No existe una llave explícita** en los datos exportados que vincule directamente un desembargo con su embargo original de forma determinista.

3. **El modelo actual no soporta correlación** porque descartó `secondary_id` y `referencia` durante el ETL.

4. **La cobertura máxima teórica** del campo `secondary_id` (incluso si fuera válido) sería del 13.6% del total de desembargos, insuficiente para un análisis completo.

---

## 9. Recomendaciones

### 9.1 Corto plazo — Modelo en cascada (Fallback)

Para correlacionar embargo-desembargo sin llave directa, implementar un cruce progresivo:

| Nivel | Criterio | Cobertura esperada |
|---|---|---|
| 1 | `identificacion` + `referencia` + `entidad_remitente` (exacto) | Alta precisión, baja cobertura |
| 2 | `identificacion` + `entidad_remitente` (sin referencia) | Media precisión |
| 3 | `identificacion` + fuzzy match en `referencia` | Baja precisión, mayor cobertura |

### 9.2 Mediano plazo — Enriquecer el modelo

Agregar a `fact_oficios` los campos `secondary_id` y `referencia` para habilitar análisis futuros sin depender de los CSVs originales.

### 9.3 Largo plazo — Validación con el sistema fuente

Consultar con el equipo de desarrollo del sistema CAEM para confirmar:
- ¿Existe una tabla de relaciones procesales no exportada?
- ¿El campo `secondary_id` tiene una semántica documentada?
- ¿Hay un campo de radicado único que vincule embargo y desembargo del mismo proceso?

---

## Anexo: Archivo de evidencia

| Archivo | Descripción |
|---|---|
| `datos/procesados/correlacion_embargo_desembargo_30_casos.csv` | 30 pares correlacionados con 21 columnas de detalle |
