#!/usr/bin/env python3
"""
cruce_entidades.py
──────────────────
Cruza las entidades del modelo (dim_entidades_coactivas + dim_entidades_judiciales)
con las fuentes externas:
  1. Universo_de_entidades_20260415.csv  → entidades coactivas/gubernamentales (NIT, dirección, email, etc.)
  2. Despachos_20260415_151620.xls       → despachos judiciales (código, dirección, teléfono, juez)

Genera:
  - datos/modelo_final/dim_entidades_coactivas.csv  (enriquecido)
  - datos/modelo_final/dim_entidades_judiciales.csv (enriquecido)
  - datos/modelo_final/dim_entidades.csv            (unificado, todo MAYÚSCULAS)
  - datos/modelo_final/schema.sql                   (actualizado)

Estrategia de cruce:
  - Normaliza nombres a MAYÚSCULAS, sin tildes, sin espacios extra
  - Fuzzy matching por similitud de texto cuando no hay match exacto
  - Para judiciales: cruza con Despachos por nombre de despacho
  - Para coactivas: cruza con Universo por nombre de entidad
"""

import csv
import os
import re
import unicodedata
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
REPO = BASE / "entidades-repo"
MODELO = BASE / "datos" / "modelo_final"

# ───────────────────── Utilidades ─────────────────────

def strip_accents(s):
    """Quita tildes/diacríticos."""
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))

def normalize(s):
    """Normaliza nombre para comparaciones: MAYÚSCULAS, sin tildes, sin espacios extra."""
    if not s:
        return ''
    s = strip_accents(str(s).upper().strip())
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def extract_numbers(s):
    """Extrae todos los números del string."""
    return set(re.findall(r'\d+', s))

def similarity(a, b):
    """Similitud basada en tokens comunes (Jaccard) con penalización si los números difieren."""
    if not a or not b:
        return 0.0
    ta = set(a.split())
    tb = set(b.split())
    if not ta or not tb:
        return 0.0
    inter = ta & tb
    union = ta | tb
    score = len(inter) / len(union)

    # Penalizar si los números del nombre no coinciden
    nums_a = extract_numbers(a)
    nums_b = extract_numbers(b)
    if nums_a and nums_b and not (nums_a & nums_b):
        score *= 0.5  # Penalización fuerte

    return score

def parse_direccion(raw):
    """Extrae dirección legible de JSON o string."""
    if not raw:
        return ''
    raw = raw.strip()
    if raw.startswith('{'):
        try:
            import json
            d = json.loads(raw)
            addr = d.get('direccionGenerada', d.get('complemento', ''))
            addr = str(addr).strip()
            if addr and addr.upper() != 'VACIA':
                return addr.upper()
            return ''
        except Exception:
            m = re.search(r'direccionGenerada[":\s]+([^"]+)', raw)
            if m:
                return m.group(1).strip().upper()
    return raw.upper()

def best_match(name_norm, candidates, index, threshold=0.60):
    """Encuentra el mejor match usando índice invertido por tokens."""
    if not name_norm:
        return None

    # Exact match via dict
    if name_norm in candidates:
        return (1.0, candidates[name_norm])

    # Use inverted index to find candidates with shared tokens
    tokens = name_norm.split()
    scored = {}
    for tok in tokens:
        for norm_key in index.get(tok, []):
            if norm_key not in scored:
                scored[norm_key] = similarity(name_norm, norm_key)

    if not scored:
        return None

    best_key = max(scored, key=scored.get)
    best_score = scored[best_key]
    if best_score >= threshold:
        return (best_score, candidates[best_key])
    return None

def build_index(items):
    """Construye dict exacto + índice invertido por tokens."""
    exact = {}
    inv = {}
    for norm_key, row in items:
        if norm_key not in exact:
            exact[norm_key] = row
        for tok in norm_key.split():
            inv.setdefault(tok, []).append(norm_key)
    return exact, inv

def to_upper(val):
    """Convierte a MAYÚSCULAS o retorna vacío."""
    if not val or str(val).strip().upper() in ('NULL', 'NONE', 'NAN', ''):
        return ''
    return str(val).strip().upper()

def clean_null(val):
    """Limpia valores NULL."""
    if not val or str(val).strip().upper() in ('NULL', 'NONE', 'NAN'):
        return ''
    return str(val).strip()

# ───────────────────── Cargar Universo (coactivas) ─────────────────────

def load_universo():
    """Carga Universo_de_entidades CSV → list of (nombre_norm, dict)."""
    path = REPO / "Universo_de_entidades_20260415.csv"
    data = []
    with open(path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            nombre = clean_null(row.get('NOMBRE', ''))
            norm = normalize(nombre)
            if norm:
                data.append((norm, {
                    'cod_institucion': clean_null(row.get('DM_INSTITUCION_COD_INSTITUCION', '')),
                    'nombre_real': to_upper(nombre),
                    'nit': clean_null(row.get('CCB_NIT_INST', '')),
                    'orden': to_upper(row.get('ORDEN', '')),
                    'sector': to_upper(row.get('SECTOR', '')),
                    'naturaleza_juridica': to_upper(row.get('NATURALEZA_JURIDICA', '')),
                    'tipo_institucion': to_upper(row.get('TIPO_INSTITUCION', '')),
                    'municipio_univ': to_upper(row.get('MUNICIPIO', '')),
                    'departamento_univ': to_upper(row.get('DEPARTAMENTO', '')),
                    'direccion': parse_direccion(row.get('DIRECCION', '')),
                    'email': clean_null(row.get('EMAIL', '')),
                    'telefono': clean_null(row.get('CCB_TELEFONO', '')),
                    'pagina_web': clean_null(row.get('CCB_PAGINA_WEB', '')),
                    'estado': to_upper(row.get('ESTADO_INS', '')),
                    'representante': to_upper(row.get('NOMBRE_REPRESENTANTE', '')),
                    'cargo_representante': to_upper(row.get('CARGO_REPRESENTANTE', '')),
                }))
    print(f"  Universo cargado: {len(data)} entidades")
    return data

# ───────────────────── Cargar Despachos (judiciales) ─────────────────────

def load_despachos():
    """Carga Despachos XLS → list of (nombre_norm, dict)."""
    try:
        import xlrd
    except ImportError:
        print("ERROR: xlrd no instalado. pip install xlrd")
        return []

    path = REPO / "Despachos_20260415_151620.xls"
    wb = xlrd.open_workbook(str(path))
    sh = wb.sheet_by_index(0)

    # Find header row (row 10 based on exploration)
    header_row = None
    for r in range(min(15, sh.nrows)):
        val = str(sh.cell_value(r, 0)).strip().upper()
        if 'JURISDI' in val:
            header_row = r
            break
    if header_row is None:
        print("ERROR: No se encontró header en Despachos XLS")
        return []

    headers = [str(sh.cell_value(header_row, c)).strip() for c in range(sh.ncols)]

    data = []
    for r in range(header_row + 1, sh.nrows):
        row = {}
        for c, h in enumerate(headers):
            row[h] = str(sh.cell_value(r, c)).strip()

        nombre = clean_null(row.get('NOMBRE DESPACHO', ''))
        norm = normalize(nombre)
        if norm:
            # Parse dirección JSON-like o string
            direccion_raw = clean_null(row.get('DIRECCIÓN DESPACHO', ''))

            data.append((norm, {
                'codigo_despacho': clean_null(row.get('CODIGO DESPACHO', '')),
                'nombre_real': to_upper(nombre),
                'jurisdiccion': to_upper(row.get('JURISDICCIÓN', '')),
                'distrito': to_upper(row.get('DISTRITO', '')),
                'circuito': to_upper(row.get('CIRCUITO', '')),
                'departamento_desp': to_upper(row.get('DEPARTAMENTO', '')),
                'municipio_desp': to_upper(row.get('MUNICIPIO', '')),
                'juez': to_upper(row.get('JUEZ/MAGISTRADO DESPACHO', '')),
                'direccion': to_upper(direccion_raw),
                'telefono': clean_null(row.get('TELÉFONO', '')),
                'area': to_upper(row.get('ÁREA', '')),
            }))
    print(f"  Despachos cargado: {len(data)} despachos judiciales")
    return data

# ───────────────────── Cruce Coactivas ─────────────────────

def cruce_coactivas(universo_list):
    """Cruza dim_entidades_coactivas con Universo."""
    exact, inv = build_index(universo_list)
    path_in = MODELO / "dim_entidades_coactivas.csv"
    rows_out = []
    matched = 0
    total = 0

    with open(path_in, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            nombre_ext = row.get('nombre_extraido', '')
            nombre_norm = normalize(nombre_ext)

            result = best_match(nombre_norm, exact, inv, threshold=0.60)

            out = {
                'entidad_id': row['entidad_id'],
                'nombre_extraido': to_upper(nombre_ext),
                'nombre_real': '',
                'ciudad': to_upper(row.get('ciudad', '')),
                'email_extraido': to_upper(row.get('email_extraido', '')),
                'email_real': '',
                'nit': '',
                'cod_institucion': '',
                'orden': '',
                'sector': '',
                'naturaleza_juridica': '',
                'tipo_institucion': '',
                'direccion': '',
                'telefono': '',
                'pagina_web': '',
                'estado': '',
                'representante': '',
                'cargo_representante': '',
                'total_registros': row.get('total_registros', '0'),
            }

            if result:
                matched += 1
                score, info = result
                out['nombre_real'] = info['nombre_real']
                out['nit'] = info['nit']
                out['cod_institucion'] = info['cod_institucion']
                out['email_real'] = to_upper(info['email'])
                out['orden'] = info['orden']
                out['sector'] = info['sector']
                out['naturaleza_juridica'] = info['naturaleza_juridica']
                out['tipo_institucion'] = info['tipo_institucion']
                out['direccion'] = to_upper(info['direccion'])
                out['telefono'] = info['telefono']
                out['pagina_web'] = info['pagina_web']
                out['estado'] = info['estado']
                out['representante'] = info['representante']
                out['cargo_representante'] = info['cargo_representante']

                # Si no tenía email, usar el del universo
                if not out['email_extraido'] and out['email_real']:
                    out['email_extraido'] = out['email_real']

            rows_out.append(out)

    print(f"  Coactivas: {matched}/{total} cruzadas ({100*matched/total:.1f}%)")
    return rows_out

# ───────────────────── Cruce Judiciales ─────────────────────

def cruce_judiciales(despachos_list):
    """Cruza dim_entidades_judiciales con Despachos."""
    exact, inv = build_index(despachos_list)
    path_in = MODELO / "dim_entidades_judiciales.csv"
    rows_out = []
    matched = 0
    total = 0

    with open(path_in, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            nombre_ext = row.get('nombre_extraido', '')
            nombre_norm = normalize(nombre_ext)

            result = best_match(nombre_norm, exact, inv, threshold=0.60)

            out = {
                'entidad_id': row['entidad_id'],
                'nombre_extraido': to_upper(nombre_ext),
                'nombre_real': '',
                'ciudad': to_upper(row.get('ciudad', '')),
                'email_extraido': to_upper(row.get('email_extraido', '')),
                'email_real': '',
                'codigo_despacho': '',
                'numero_despacho': to_upper(row.get('numero_despacho', '')),
                'jurisdiccion': '',
                'distrito': '',
                'circuito': '',
                'juez': '',
                'direccion': '',
                'telefono': '',
                'area': '',
                'total_registros': row.get('total_registros', '0'),
            }

            if result:
                matched += 1
                score, info = result
                out['nombre_real'] = info['nombre_real']
                out['codigo_despacho'] = info['codigo_despacho']
                out['jurisdiccion'] = info['jurisdiccion']
                out['distrito'] = info['distrito']
                out['circuito'] = info['circuito']
                out['juez'] = info['juez']
                out['direccion'] = info['direccion']
                out['telefono'] = info['telefono']
                out['area'] = info['area']

                # Llenar ciudad si estaba vacía
                if not out['ciudad'] and info['municipio_desp']:
                    out['ciudad'] = info['municipio_desp']

            rows_out.append(out)

    print(f"  Judiciales: {matched}/{total} cruzadas ({100*matched/total:.1f}%)")
    return rows_out

# ───────────────────── Generar dim_entidades unificado ─────────────────────

def build_dim_entidades(coactivas, judiciales):
    """Genera dim_entidades.csv unificado con todos los campos en MAYÚSCULAS."""
    # Leer dim_entidades original para obtener tipo, subtipo, municipio_id, departamento_id
    orig = {}
    with open(MODELO / "dim_entidades.csv", encoding='utf-8') as f:
        for row in csv.DictReader(f):
            orig[row['entidad_id']] = row

    rows = []
    for ent in coactivas:
        eid = ent['entidad_id']
        o = orig.get(eid, {})
        rows.append({
            'entidad_id': eid,
            'nombre_normalizado': to_upper(o.get('nombre_normalizado', ent['nombre_extraido'])),
            'nombre_real': ent['nombre_real'],
            'tipo': to_upper(o.get('tipo', '')),
            'subtipo': to_upper(o.get('subtipo', '')),
            'categoria': 'COACTIVA',
            'nit': ent['nit'],
            'cod_institucion': ent['cod_institucion'],
            'email': ent['email_extraido'] or ent['email_real'],
            'direccion': ent['direccion'],
            'telefono': ent['telefono'],
            'ciudad': ent['ciudad'],
            'municipio_id': o.get('municipio_id', ''),
            'departamento_id': o.get('departamento_id', ''),
            'orden': ent['orden'],
            'sector': ent['sector'],
            'naturaleza_juridica': ent['naturaleza_juridica'],
            'estado': ent['estado'],
            'representante': ent['representante'],
            'total_registros': ent['total_registros'],
            'num_variantes': o.get('num_variantes', '0'),
        })

    for ent in judiciales:
        eid = ent['entidad_id']
        o = orig.get(eid, {})
        rows.append({
            'entidad_id': eid,
            'nombre_normalizado': to_upper(o.get('nombre_normalizado', ent['nombre_extraido'])),
            'nombre_real': ent['nombre_real'],
            'tipo': to_upper(o.get('tipo', '')),
            'subtipo': to_upper(o.get('subtipo', '')),
            'categoria': 'JUDICIAL',
            'nit': '',
            'cod_institucion': ent.get('codigo_despacho', ''),
            'email': ent['email_extraido'],
            'direccion': ent['direccion'],
            'telefono': ent['telefono'],
            'ciudad': ent['ciudad'],
            'municipio_id': o.get('municipio_id', ''),
            'departamento_id': o.get('departamento_id', ''),
            'orden': ent.get('jurisdiccion', ''),
            'sector': ent.get('area', ''),
            'naturaleza_juridica': '',
            'estado': '',
            'representante': ent.get('juez', ''),
            'total_registros': ent['total_registros'],
            'num_variantes': o.get('num_variantes', '0'),
        })

    # Sort by entidad_id
    rows.sort(key=lambda x: int(x['entidad_id']))
    return rows

# ───────────────────── Escribir CSVs ─────────────────────

def write_csv(path, rows, fieldnames):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Escrito: {path.name} ({len(rows)} filas)")

# ───────────────────── Schema SQL ─────────────────────

def write_schema():
    schema = MODELO / "schema.sql"
    sql = """-- ============================================================
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
"""
    with open(schema, 'w', encoding='utf-8') as f:
        f.write(sql)
    print(f"  Escrito: schema.sql (v3 enriquecido)")

# ───────────────────── Main ─────────────────────

def main():
    print("=" * 60)
    print("CRUCE DE ENTIDADES CON FUENTES EXTERNAS")
    print("=" * 60)

    print("\n1. Cargando fuentes externas...")
    universo = load_universo()
    despachos = load_despachos()

    print("\n2. Cruzando entidades coactivas con Universo...")
    coactivas = cruce_coactivas(universo)

    print("\n3. Cruzando entidades judiciales con Despachos...")
    judiciales = cruce_judiciales(despachos)

    print("\n4. Construyendo dim_entidades unificado (MAYÚSCULAS)...")
    dim_ent = build_dim_entidades(coactivas, judiciales)

    print("\n5. Escribiendo archivos...")

    # Coactivas
    write_csv(MODELO / "dim_entidades_coactivas.csv", coactivas, [
        'entidad_id', 'nombre_extraido', 'nombre_real', 'ciudad',
        'email_extraido', 'email_real', 'nit', 'cod_institucion',
        'orden', 'sector', 'naturaleza_juridica', 'tipo_institucion',
        'direccion', 'telefono', 'pagina_web', 'estado',
        'representante', 'cargo_representante', 'total_registros',
    ])

    # Judiciales
    write_csv(MODELO / "dim_entidades_judiciales.csv", judiciales, [
        'entidad_id', 'nombre_extraido', 'nombre_real', 'ciudad',
        'email_extraido', 'email_real', 'codigo_despacho', 'numero_despacho',
        'jurisdiccion', 'distrito', 'circuito', 'juez',
        'direccion', 'telefono', 'area', 'total_registros',
    ])

    # Unificado
    write_csv(MODELO / "dim_entidades.csv", dim_ent, [
        'entidad_id', 'nombre_normalizado', 'nombre_real', 'tipo', 'subtipo',
        'categoria', 'nit', 'cod_institucion', 'email', 'direccion', 'telefono',
        'ciudad', 'municipio_id', 'departamento_id', 'orden', 'sector',
        'naturaleza_juridica', 'estado', 'representante',
        'total_registros', 'num_variantes',
    ])

    # Schema
    write_schema()

    print("\n" + "=" * 60)
    print("CRUCE COMPLETADO")
    print("=" * 60)

if __name__ == '__main__':
    main()
