#!/usr/bin/env python3
"""
ETL Final — Modelo de datos relacional para embargos
=====================================================
Genera las tablas normalizadas:

  1. dim_departamentos.csv   (departamento_id, nombre)
  2. dim_municipios.csv      (municipio_id, nombre, departamento_id)
  3. dim_entidades.csv       (entidad_id, nombre_normalizado, tipo, subtipo, municipio_id, departamento_id)
  4. dim_variantes.csv       (variante_id, entidad_id, variante_original, conteo)
  5. fact_oficios.csv        (oficio_id=embargo_id, entidad_remitente_id, entidad_bancaria_id,
                              estado, numero_oficio, fecha_oficio, fecha_recepcion,
                              titulo_embargo, titulo_orden, monto, monto_a_embargar,
                              nombre_demandado, id_demandado, tipo_id_demandado,
                              direccion_remitente, correo_remitente, nombre_funcionario,
                              municipio_id, departamento_id, fuente_ubicacion)

Todas las tablas van a  modelo_final/
"""

import csv
import json
import re
import unicodedata
import os
import sys
from collections import defaultdict

OUTPUT_DIR = 'modelo_final'

# ============================================================
# TEXT UTILS
# ============================================================

def remove_accents(text):
    result = []
    for char in text:
        if char in ('ñ', 'Ñ'):
            result.append(char)
        else:
            nfd = unicodedata.normalize('NFD', char)
            result.append(''.join(c for c in nfd if unicodedata.category(c) != 'Mn'))
    return ''.join(result)


def norm(text):
    """Normalize text for comparison: uppercase, no accents, single spaces."""
    if not text:
        return ''
    t = text.strip().upper()
    t = remove_accents(t)
    t = re.sub(r'[^A-Z0-9Ñ\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


# ============================================================
# PASO 1: Construir dim_departamentos
# ============================================================

def build_departamentos(colombia_json_path):
    """Build department table from official JSON + Bogotá D.C."""
    with open(colombia_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    deptos = {}
    for item in data:
        nombre = item['departamento']
        deptos[norm(nombre)] = nombre

    # Ensure Bogotá is included
    if norm('Bogotá, D.C.') not in deptos and norm('BOGOTA') not in deptos:
        deptos[norm('Bogotá, D.C.')] = 'Bogotá, D.C.'

    # Sort and assign IDs
    sorted_names = sorted(deptos.values())
    result = []
    for i, nombre in enumerate(sorted_names, 1):
        result.append({'departamento_id': i, 'nombre': nombre})

    return result


def build_depto_lookup(deptos_table):
    """Create lookup: norm(name) -> departamento_id with aliases."""
    lookup = {}
    for d in deptos_table:
        lookup[norm(d['nombre'])] = d['departamento_id']

    # Common aliases
    aliases = {
        'BOGOTA': 'Bogotá, D.C.', 'BOGOTA D C': 'Bogotá, D.C.',
        'BOGOTA DC': 'Bogotá, D.C.', 'BOGOTA D.C.': 'Bogotá, D.C.',
        'BOGOTA  D.C.': 'Bogotá, D.C.', 'BOGOTA D.C': 'Bogotá, D.C.',
        'SAN ANDRES': 'San Andrés y Providencia',
        'SAN ANDRES Y PROVIDENCIA': 'San Andrés y Providencia',
        'SAN ANDRES ISLAS': 'San Andrés y Providencia',
        'SAN ANDRES  PROVIDENCIA Y SANTA CATALINA': 'San Andrés y Providencia',
        'NORTE DE SANTANDER': 'Norte de Santander',
        'VALLE DEL CAUCA': 'Valle del Cauca',
        'VALLE': 'Valle del Cauca',
    }
    for alias_raw, real_name in aliases.items():
        key = norm(alias_raw)
        real_key = norm(real_name)
        if real_key in lookup:
            lookup[key] = lookup[real_key]

    return lookup


# ============================================================
# PASO 2: Construir dim_municipios
# ============================================================

def build_municipios(colombia_json_path, deptos_table, depto_lookup):
    """Build municipality table from official JSON."""
    with open(colombia_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    municipios = []
    seen = set()
    mid = 0

    for item in data:
        depto_name = item['departamento']
        depto_id = depto_lookup.get(norm(depto_name))
        if not depto_id:
            print(f"  WARN: Departamento '{depto_name}' no encontrado en lookup")
            continue

        for ciudad in item['ciudades']:
            key = (norm(ciudad), depto_id)
            if key in seen:
                continue
            seen.add(key)
            mid += 1
            municipios.append({
                'municipio_id': mid,
                'nombre': ciudad,
                'departamento_id': depto_id,
            })

    # Add Bogotá D.C. as municipality
    bogota_depto_id = depto_lookup.get(norm('Bogotá, D.C.'))
    if bogota_depto_id:
        bogota_keys = [
            (norm('Bogotá D.C.'), bogota_depto_id),
            (norm('Bogotá'), bogota_depto_id),
        ]
        for bk in bogota_keys:
            if bk not in seen:
                seen.add(bk)
                mid += 1
                municipios.append({
                    'municipio_id': mid,
                    'nombre': 'Bogotá D.C.',
                    'departamento_id': bogota_depto_id,
                })
                break

    return municipios


def build_muni_lookup(municipios_table, deptos_table, depto_lookup):
    """Create lookup: (norm(muni_name), departamento_id) -> municipio_id
       and also norm(muni_name) -> municipio_id for unique names."""
    by_name_depto = {}
    by_name_only = defaultdict(list)

    for m in municipios_table:
        key = (norm(m['nombre']), m['departamento_id'])
        by_name_depto[key] = m['municipio_id']
        by_name_only[norm(m['nombre'])].append(m['municipio_id'])

    # For unique names (only one municipio with that name), allow lookup without depto
    unique = {}
    for name_norm, ids in by_name_only.items():
        if len(ids) == 1:
            unique[name_norm] = ids[0]

    return by_name_depto, unique


def resolve_municipio(muni_str, depto_str, by_name_depto, unique_muni, depto_lookup):
    """Resolve a municipality string to its ID."""
    if not muni_str or not muni_str.strip():
        return None

    muni_norm = norm(muni_str)

    # Common Bogotá aliases
    bogota_aliases = {'BOGOTA', 'BOGOTA D C', 'BOGOTA DC', 'BOGOTA D.C.', 'BOGOTA D.C',
                      'SANTAFE DE BOGOTA', 'SANTA FE DE BOGOTA'}
    if muni_norm in bogota_aliases or 'BOGOTA' in muni_norm:
        muni_norm = norm('Bogotá D.C.')

    # Cartagena alias
    if 'CARTAGENA' in muni_norm:
        muni_norm = norm('Cartagena de Indias')

    # Mompox / Mompós → Mompós (official name in json)
    if 'MOMPOX' in muni_norm or 'MOMPOS' in muni_norm:
        muni_norm = norm('Mompós')

    # "SOCORRO SANTANDER" → Socorro
    if 'SOCORRO' in muni_norm:
        muni_norm = norm('Socorro')

    # "FUENTEDEORO META" → Fuente de Oro
    if 'FUENTEDEORO' in muni_norm or 'FUENTE DE ORO' in muni_norm:
        muni_norm = norm('Fuente de Oro')

    # "Dorada Caldas" → La Dorada
    if 'DORADA' in muni_norm:
        muni_norm = norm('La Dorada')

    # Skip non-location values
    skip_values = {'NO ENCONTRADO', 'NO ENCONTRADA', 'NO REGISTRA', 'SIN INFORMACION',
                   'BANCO SANTANDER', 'CREDIFINANCIERA SANTANDER',
                   'CONSEJO SECCIONAL DE LA JUDICATURA DEL ATLANTICO'}
    if muni_norm in skip_values:
        return None

    # "Socorro" / "SOCORRO SANTANDER" → El Socorro
    if 'SOCORRO' in muni_norm and 'BANCO' not in muni_norm and 'CREDI' not in muni_norm:
        muni_norm = norm('El Socorro')

    # Pure department names NOT also municipalities → NULL
    pure_dept_only = {'ATLANTICO', 'MAGDALENA', 'VALLE DEL CAUCA', 'HUILA',
                      'CUNDINAMARCA', 'ANTIOQUIA', 'SANTANDER', 'CASANARE',
                      'NORTE DE SANTANDER', 'TOLIMA',
                      'QUINDIO', 'CHOCO', 'PUTUMAYO', 'CAQUETA', 'GUAVIARE',
                      'VAUPES', 'GUAINIA', 'AMAZONAS', 'VICHADA',
                      'SAN ANDRES Y PROVIDENCIA', 'SOCORRO', 'META', 'CESAR',
                      'CORDOBA', 'SUCRE'}
    if muni_norm in pure_dept_only:
        return None

    # Try with depto
    if depto_str and depto_str.strip():
        depto_id = depto_lookup.get(norm(depto_str))
        if depto_id:
            key = (muni_norm, depto_id)
            if key in by_name_depto:
                return by_name_depto[key]

    # Try unique name
    if muni_norm in unique_muni:
        return unique_muni[muni_norm]

    # Try without accents/flexing
    for key_norm, mid in unique_muni.items():
        if key_norm == muni_norm:
            return mid

    return None


def resolve_departamento(depto_str, depto_lookup):
    """Resolve a department string to its ID."""
    if not depto_str or not depto_str.strip():
        return None
    depto_norm = norm(depto_str)
    if depto_norm in depto_lookup:
        return depto_lookup[depto_norm]
    # Try partial match
    for key, did in depto_lookup.items():
        if depto_norm in key or key in depto_norm:
            return did
    return None


# ============================================================
# PASO 3: Construir dim_entidades con FK a municipio/depto
# ============================================================

def build_entidades(entidades_path, depto_lookup, by_name_depto, unique_muni):
    """Build entities table with foreign keys to municipio and departamento."""
    entidades = []
    with open(entidades_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            depto_id = resolve_departamento(row['departamento'], depto_lookup)
            muni_id = resolve_municipio(row['municipio'], row['departamento'],
                                        by_name_depto, unique_muni, depto_lookup)
            entidades.append({
                'entidad_id': int(row['entidad_id']),
                'nombre_normalizado': row['nombre_normalizado'],
                'tipo': row['tipo'],
                'subtipo': row['subtipo'],
                'municipio_id': muni_id or '',
                'departamento_id': depto_id or '',
                'total_registros': int(row['total_registros']),
                'num_variantes': int(row['num_variantes']),
            })
    return entidades


# ============================================================
# PASO 4: Construir dim_variantes
# ============================================================

def build_variantes(variantes_path):
    """Build variants table with auto-generated PK."""
    variantes = []
    vid = 0
    with open(variantes_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            vid += 1
            variantes.append({
                'variante_id': vid,
                'entidad_id': int(row['entidad_id']),
                'nombre_normalizado': row['nombre_normalizado'],
                'variante_original': row['variante_original'],
                'conteo': int(row['conteo']),
            })
    return variantes


# ============================================================
# PASO 5: Construir fact_oficios (embargos con FKs)
# ============================================================

def build_oficios(embargos_path, depto_lookup, by_name_depto, unique_muni):
    """Build oficios/embargos fact table with FK to municipio and departamento."""
    oficios = []
    unresolved_munis = defaultdict(int)
    unresolved_deptos = defaultdict(int)
    total = 0
    resolved_muni = 0
    resolved_depto = 0

    with open(embargos_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1

            depto_id = resolve_departamento(row['departamento'], depto_lookup)
            muni_id = resolve_municipio(row['municipio'], row['departamento'],
                                        by_name_depto, unique_muni, depto_lookup)

            if muni_id:
                resolved_muni += 1
            elif row['municipio'].strip():
                unresolved_munis[row['municipio'].strip()] += 1

            if depto_id:
                resolved_depto += 1
            elif row['departamento'].strip():
                unresolved_deptos[row['departamento'].strip()] += 1

            oficios.append({
                'oficio_id': row['embargo_id'],
                'entidad_remitente_id': row['entidad_remitente_id'],
                'entidad_bancaria_id': row['entidad_bancaria_id'],
                'estado': row['estado_embargo'],
                'numero_oficio': row['numero_oficio'],
                'fecha_oficio': row['fecha_oficio'],
                'fecha_recepcion': row['fecha_recepcion'],
                'titulo_embargo': row['titulo_embargo'],
                'titulo_orden': row['titulo_orden'],
                'monto': row['monto'],
                'monto_a_embargar': row['monto_a_embargar'],
                'nombre_demandado': row['nombre_demandado'],
                'id_demandado': row['id_demandado'],
                'tipo_id_demandado': row['tipo_id_demandado'],
                'direccion_remitente': row['direccion_remitente'],
                'correo_remitente': row['correo_remitente'],
                'nombre_funcionario': row['nombre_personal_remitente'],
                'municipio_id': muni_id or '',
                'departamento_id': depto_id or '',
                'fuente_ubicacion': row['fuente_ubicacion'],
            })

            if total % 100000 == 0:
                print(f"  ... {total:,} registros procesados")

    print(f"\n  Oficios totales:      {total:,}")
    print(f"  Municipio resuelto:   {resolved_muni:,} ({resolved_muni/total*100:.1f}%)")
    print(f"  Departamento resuelto:{resolved_depto:,} ({resolved_depto/total*100:.1f}%)")

    if unresolved_munis:
        print(f"\n  Top 20 municipios no resueltos:")
        for m, c in sorted(unresolved_munis.items(), key=lambda x: -x[1])[:20]:
            print(f"    {c:>8,}  {m}")

    if unresolved_deptos:
        print(f"\n  Top 10 departamentos no resueltos:")
        for d, c in sorted(unresolved_deptos.items(), key=lambda x: -x[1])[:10]:
            print(f"    {c:>8,}  {d}")

    return oficios, unresolved_munis, unresolved_deptos


# ============================================================
# WRITE CSV helpers
# ============================================================

def write_csv(filepath, rows, fieldnames):
    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  -> {filepath}  ({len(rows):,} filas)")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("ETL — Modelo de Datos Final")
    print("=" * 70)

    # ---- DEPARTAMENTOS ----
    print("\n[1/5] Construyendo dim_departamentos...")
    deptos = build_departamentos('colombia_municipios.json')

    # Ensure Bogotá, D.C. is in the list
    bogota_exists = any(norm(d['nombre']) == norm('Bogotá, D.C.') for d in deptos)
    if not bogota_exists:
        max_id = max(d['departamento_id'] for d in deptos)
        deptos.append({'departamento_id': max_id + 1, 'nombre': 'Bogotá, D.C.'})
        deptos.sort(key=lambda x: x['nombre'])
        # Reassign IDs
        for i, d in enumerate(deptos, 1):
            d['departamento_id'] = i

    depto_lookup = build_depto_lookup(deptos)
    write_csv(f'{OUTPUT_DIR}/dim_departamentos.csv', deptos,
              ['departamento_id', 'nombre'])

    # ---- MUNICIPIOS ----
    print("\n[2/5] Construyendo dim_municipios...")
    municipios = build_municipios('colombia_municipios.json', deptos, depto_lookup)
    by_name_depto, unique_muni = build_muni_lookup(municipios, deptos, depto_lookup)
    write_csv(f'{OUTPUT_DIR}/dim_municipios.csv', municipios,
              ['municipio_id', 'nombre', 'departamento_id'])

    # ---- ENTIDADES ----
    print("\n[3/5] Construyendo dim_entidades...")
    entidades = build_entidades('entidades.csv', depto_lookup, by_name_depto, unique_muni)

    # Stats
    with_muni = sum(1 for e in entidades if e['municipio_id'])
    with_depto = sum(1 for e in entidades if e['departamento_id'])
    print(f"  Entidades con municipio_id: {with_muni:,}/{len(entidades):,}")
    print(f"  Entidades con departamento_id: {with_depto:,}/{len(entidades):,}")

    write_csv(f'{OUTPUT_DIR}/dim_entidades.csv', entidades,
              ['entidad_id', 'nombre_normalizado', 'tipo', 'subtipo',
               'municipio_id', 'departamento_id', 'total_registros', 'num_variantes'])

    # ---- VARIANTES ----
    print("\n[4/5] Construyendo dim_variantes...")
    variantes = build_variantes('variantes_entidades.csv')
    write_csv(f'{OUTPUT_DIR}/dim_variantes.csv', variantes,
              ['variante_id', 'entidad_id', 'nombre_normalizado', 'variante_original', 'conteo'])

    # ---- OFICIOS ----
    print("\n[5/5] Construyendo fact_oficios (esto puede tardar)...")
    oficios, unresolved_m, unresolved_d = build_oficios(
        'embargos_final.csv', depto_lookup, by_name_depto, unique_muni)

    write_csv(f'{OUTPUT_DIR}/fact_oficios.csv', oficios,
              ['oficio_id', 'entidad_remitente_id', 'entidad_bancaria_id',
               'estado', 'numero_oficio', 'fecha_oficio', 'fecha_recepcion',
               'titulo_embargo', 'titulo_orden', 'monto', 'monto_a_embargar',
               'nombre_demandado', 'id_demandado', 'tipo_id_demandado',
               'direccion_remitente', 'correo_remitente', 'nombre_funcionario',
               'municipio_id', 'departamento_id', 'fuente_ubicacion'])

    # ---- RESUMEN ----
    print("\n" + "=" * 70)
    print("RESUMEN DEL MODELO")
    print("=" * 70)
    print(f"  dim_departamentos:  {len(deptos):>10,} filas")
    print(f"  dim_municipios:     {len(municipios):>10,} filas")
    print(f"  dim_entidades:      {len(entidades):>10,} filas")
    print(f"  dim_variantes:      {len(variantes):>10,} filas")
    print(f"  fact_oficios:       {len(oficios):>10,} filas")
    print(f"\n  Archivos en: {OUTPUT_DIR}/")

    # ---- SQL SCHEMA ----
    schema_sql = """-- ============================================================
-- SCHEMA: Modelo de datos de embargos/oficios
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
    tipo                VARCHAR(50),
    subtipo             VARCHAR(50),
    municipio_id        INTEGER,
    departamento_id     INTEGER,
    total_registros     INTEGER DEFAULT 0,
    num_variantes       INTEGER DEFAULT 0,
    FOREIGN KEY (municipio_id)    REFERENCES dim_municipios(municipio_id),
    FOREIGN KEY (departamento_id) REFERENCES dim_departamentos(departamento_id)
);
CREATE INDEX idx_entidades_tipo ON dim_entidades(tipo);
CREATE INDEX idx_entidades_muni ON dim_entidades(municipio_id);
CREATE INDEX idx_entidades_depto ON dim_entidades(departamento_id);

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
    FOREIGN KEY (entidad_remitente_id) REFERENCES dim_entidades(entidad_id),
    FOREIGN KEY (municipio_id)         REFERENCES dim_municipios(municipio_id),
    FOREIGN KEY (departamento_id)      REFERENCES dim_departamentos(departamento_id)
);
CREATE INDEX idx_oficios_entidad ON fact_oficios(entidad_remitente_id);
CREATE INDEX idx_oficios_estado ON fact_oficios(estado);
CREATE INDEX idx_oficios_muni ON fact_oficios(municipio_id);
CREATE INDEX idx_oficios_depto ON fact_oficios(departamento_id);
CREATE INDEX idx_oficios_fecha ON fact_oficios(fecha_oficio);
"""

    with open(f'{OUTPUT_DIR}/schema.sql', 'w', encoding='utf-8') as f:
        f.write(schema_sql)
    print(f"  -> {OUTPUT_DIR}/schema.sql")
    print("\nETL completado.")


if __name__ == '__main__':
    main()
