#!/usr/bin/env python3
"""
DIAGNÓSTICO DE CALIDAD DE DATOS
Compara el modelo ETL vs la base original pyc_embargos
y genera un informe completo de confiabilidad.
"""

import pymysql
import os
from collections import defaultdict
import sys
from datetime import datetime

# ── Configuración ──────────────────────────────────────────────
HOST = os.environ.get("DB_HOST", "127.0.0.1")
PORT = int(os.environ.get("DB_PORT", 3306))
USER = os.environ.get("DB_USER", "producto")
PASS = os.environ.get("DB_PASSWORD", "")
DB_ORIGINAL = "pyc_embargos"
DB_ETL = "ETL"

report_lines = []

def log(msg=""):
    print(msg, flush=True)
    report_lines.append(msg)

def header(title):
    log()
    log("=" * 70)
    log(f"  {title}")
    log("=" * 70)

def subheader(title):
    log()
    log(f"--- {title} ---")

def result(label, value, ok=None):
    status = ""
    if ok is True:
        status = " ✅"
    elif ok is False:
        status = " ❌"
    elif ok == "warn":
        status = " ⚠️"
    log(f"  {label}: {value}{status}")


def get_conn(db):
    return pymysql.connect(
        host=HOST, port=PORT, user=USER, password=PASS,
        database=db, charset="utf8mb4"
    )


# ═══════════════════════════════════════════════════════════════
# 0. CREAR dim_entidad_bancaria SI NO EXISTE
# ═══════════════════════════════════════════════════════════════

def create_dim_entidad_bancaria():
    header("0. CREAR dim_entidad_bancaria EN ETL")

    conn_orig = get_conn(DB_ORIGINAL)
    cur_orig = conn_orig.cursor()
    cur_orig.execute("SELECT id, nombre, descripcion FROM entidad_bancaria ORDER BY id")
    bancarias = cur_orig.fetchall()
    cur_orig.close()
    conn_orig.close()

    log(f"  Entidades bancarias en pyc_embargos: {len(bancarias)}")
    for b in bancarias:
        log(f"    id={b[0]}, nombre={b[1]}")

    conn_etl = get_conn(DB_ETL)
    cur_etl = conn_etl.cursor()

    # Verificar si ya existe
    cur_etl.execute("SHOW TABLES LIKE 'dim_entidad_bancaria'")
    exists = cur_etl.fetchone()

    if not exists:
        cur_etl.execute("""
            CREATE TABLE dim_entidad_bancaria (
                entidad_bancaria_id INT PRIMARY KEY,
                nombre              VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        for b in bancarias:
            cur_etl.execute(
                "INSERT INTO dim_entidad_bancaria (entidad_bancaria_id, nombre) VALUES (%s, %s)",
                (b[0], b[1])
            )
        conn_etl.commit()
        log("  ✅ Tabla dim_entidad_bancaria creada y poblada")

        # Agregar FK en fact_oficios
        try:
            cur_etl.execute("""
                ALTER TABLE fact_oficios
                ADD CONSTRAINT fk_oficios_bancaria
                FOREIGN KEY (entidad_bancaria_id) REFERENCES dim_entidad_bancaria(entidad_bancaria_id)
            """)
            conn_etl.commit()
            log("  ✅ Foreign key fact_oficios → dim_entidad_bancaria creada")
        except Exception as e:
            log(f"  ⚠️ No se pudo crear FK (posible dato sin referencia): {e}")
            conn_etl.rollback()

            # Ver qué IDs faltan
            cur_etl.execute("""
                SELECT DISTINCT f.entidad_bancaria_id
                FROM fact_oficios f
                LEFT JOIN dim_entidad_bancaria d ON f.entidad_bancaria_id = d.entidad_bancaria_id
                WHERE f.entidad_bancaria_id IS NOT NULL AND d.entidad_bancaria_id IS NULL
            """)
            missing = cur_etl.fetchall()
            if missing:
                log(f"  IDs bancarios en fact_oficios sin referencia: {[m[0] for m in missing]}")
    else:
        log("  Tabla dim_entidad_bancaria ya existe")

    cur_etl.close()
    conn_etl.close()


# ═══════════════════════════════════════════════════════════════
# 1. COMPARACIÓN DE CONTEOS
# ═══════════════════════════════════════════════════════════════

def test_conteos():
    header("1. COMPARACIÓN DE CONTEOS")

    conn_orig = get_conn(DB_ORIGINAL)
    cur_orig = conn_orig.cursor()

    conn_etl = get_conn(DB_ETL)
    cur_etl = conn_etl.cursor()

    # Embargos originales (no eliminados)
    cur_orig.execute("SELECT COUNT(*) FROM embargos WHERE deleted = 0")
    orig_total = cur_orig.fetchone()[0]

    cur_orig.execute("SELECT COUNT(*) FROM embargos")
    orig_all = cur_orig.fetchone()[0]

    cur_etl.execute("SELECT COUNT(*) FROM fact_oficios")
    etl_total = cur_etl.fetchone()[0]

    subheader("Embargos / Oficios")
    result("Original (todos)", f"{orig_all:,}")
    result("Original (deleted=0)", f"{orig_total:,}")
    result("ETL fact_oficios", f"{etl_total:,}")
    diff = etl_total - orig_total
    result("Diferencia vs no-eliminados", f"{diff:,} ({diff/orig_total*100:+.2f}%)",
           ok=True if abs(diff/orig_total) < 0.01 else "warn")

    # Demandados
    cur_orig.execute("SELECT COUNT(*) FROM demandado")
    orig_demandados = cur_orig.fetchone()[0]
    result("Demandados en original", f"{orig_demandados:,}")

    # Demandantes
    cur_orig.execute("SELECT COUNT(*) FROM demandante")
    orig_demandantes = cur_orig.fetchone()[0]
    result("Demandantes en original", f"{orig_demandantes:,}")

    cur_orig.close()
    conn_orig.close()
    cur_etl.close()
    conn_etl.close()


# ═══════════════════════════════════════════════════════════════
# 2. VERIFICACIÓN DE INTEGRIDAD REFERENCIAL EN ETL
# ═══════════════════════════════════════════════════════════════

def test_integridad_referencial():
    header("2. INTEGRIDAD REFERENCIAL EN ETL")

    conn = get_conn(DB_ETL)
    cur = conn.cursor()

    checks = [
        ("dim_municipios → dim_departamentos",
         "SELECT COUNT(*) FROM dim_municipios m LEFT JOIN dim_departamentos d ON m.departamento_id = d.departamento_id WHERE d.departamento_id IS NULL"),
        ("dim_entidades → dim_municipios",
         "SELECT COUNT(*) FROM dim_entidades e LEFT JOIN dim_municipios m ON e.municipio_id = m.municipio_id WHERE e.municipio_id IS NOT NULL AND m.municipio_id IS NULL"),
        ("dim_entidades → dim_departamentos",
         "SELECT COUNT(*) FROM dim_entidades e LEFT JOIN dim_departamentos d ON e.departamento_id = d.departamento_id WHERE e.departamento_id IS NOT NULL AND d.departamento_id IS NULL"),
        ("dim_variantes → dim_entidades",
         "SELECT COUNT(*) FROM dim_variantes v LEFT JOIN dim_entidades e ON v.entidad_id = e.entidad_id WHERE e.entidad_id IS NULL"),
        ("fact_oficios → dim_entidades",
         "SELECT COUNT(*) FROM fact_oficios f LEFT JOIN dim_entidades e ON f.entidad_remitente_id = e.entidad_id WHERE f.entidad_remitente_id IS NOT NULL AND e.entidad_id IS NULL"),
        ("fact_oficios → dim_municipios",
         "SELECT COUNT(*) FROM fact_oficios f LEFT JOIN dim_municipios m ON f.municipio_id = m.municipio_id WHERE f.municipio_id IS NOT NULL AND m.municipio_id IS NULL"),
        ("fact_oficios → dim_departamentos",
         "SELECT COUNT(*) FROM fact_oficios f LEFT JOIN dim_departamentos d ON f.departamento_id = d.departamento_id WHERE f.departamento_id IS NOT NULL AND d.departamento_id IS NULL"),
        ("fact_oficios → dim_entidad_bancaria",
         "SELECT COUNT(*) FROM fact_oficios f LEFT JOIN dim_entidad_bancaria b ON f.entidad_bancaria_id = b.entidad_bancaria_id WHERE f.entidad_bancaria_id IS NOT NULL AND b.entidad_bancaria_id IS NULL"),
    ]

    all_ok = True
    for label, sql in checks:
        try:
            cur.execute(sql)
            broken = cur.fetchone()[0]
            ok = broken == 0
            if not ok:
                all_ok = False
            result(label, f"{broken:,} rotas", ok=ok)
        except Exception as e:
            result(label, f"Error: {e}", ok=False)
            all_ok = False

    log()
    result("INTEGRIDAD REFERENCIAL GENERAL",
           "TODAS LAS FK VÁLIDAS" if all_ok else "HAY PROBLEMAS",
           ok=all_ok)

    cur.close()
    conn.close()


# ═══════════════════════════════════════════════════════════════
# 3. CRUCE DE IDs: OFICIOS ETL vs EMBARGOS ORIGINALES
# ═══════════════════════════════════════════════════════════════

def test_cruce_ids():
    header("3. CRUCE DE IDs ENTRE ETL Y ORIGINAL")

    conn_orig = get_conn(DB_ORIGINAL)
    cur_orig = conn_orig.cursor()
    conn_etl = get_conn(DB_ETL)
    cur_etl = conn_etl.cursor()

    # Cargar todos los IDs en memoria para cruzar (evitar TEMP TABLE por GTID)
    cur_etl.execute("SELECT oficio_id FROM fact_oficios")
    etl_ids = set(r[0] for r in cur_etl.fetchall())
    log(f"  IDs en ETL: {len(etl_ids):,}")

    cur_orig.execute("SELECT id, deleted FROM embargos")
    orig_active_ids = set()
    orig_deleted_ids = set()
    for r in cur_orig.fetchall():
        if r[1] == b'\x00':
            orig_active_ids.add(r[0])
        else:
            orig_deleted_ids.add(r[0])

    etl_not_in_orig = etl_ids - orig_active_ids - orig_deleted_ids
    result("IDs ETL que NO existen en original", f"{len(etl_not_in_orig):,}",
           ok=len(etl_not_in_orig) == 0)

    orig_not_in_etl = orig_active_ids - etl_ids
    result("IDs originales (activos) que NO están en ETL", f"{len(orig_not_in_etl):,}",
           ok="warn" if len(orig_not_in_etl) > 0 else True)

    if orig_not_in_etl:
        sample_missing = list(orig_not_in_etl)[:10]
        placeholders = ",".join(["%s"] * len(sample_missing))
        cur_orig.execute(f"""
            SELECT id, entidad_remitente, estado_embargo, fecha_oficio
            FROM embargos WHERE id IN ({placeholders})
        """, sample_missing)
        log("  Ejemplos de IDs faltantes en ETL:")
        for r in cur_orig.fetchall():
            log(f"    {r}")

    deleted_in_etl = etl_ids & orig_deleted_ids
    result("IDs eliminados que están en ETL", f"{len(deleted_in_etl):,}",
           ok="warn" if len(deleted_in_etl) > 0 else True)
    
    cur_orig.close()
    conn_orig.close()
    cur_etl.close()
    conn_etl.close()


# ═══════════════════════════════════════════════════════════════
# 4. VERIFICACIÓN DE CAMPOS CLAVE (MUESTREO)
# ═══════════════════════════════════════════════════════════════

def test_campos_clave():
    header("4. VERIFICACIÓN DE CAMPOS CLAVE (MUESTREO 10,000 REGISTROS)")

    conn_orig = get_conn(DB_ORIGINAL)
    cur_orig = conn_orig.cursor(pymysql.cursors.DictCursor)
    conn_etl = get_conn(DB_ETL)
    cur_etl = conn_etl.cursor(pymysql.cursors.DictCursor)

    # Obtener muestra aleatoria de IDs
    cur_etl.execute("SELECT oficio_id FROM fact_oficios ORDER BY RAND() LIMIT 10000")
    sample_ids = [r["oficio_id"] for r in cur_etl.fetchall()]

    # Obtener datos ETL para la muestra
    placeholders = ",".join(["%s"] * len(sample_ids))
    cur_etl.execute(f"""
        SELECT f.oficio_id, f.entidad_remitente_id, e.nombre_normalizado as entidad_etl,
               f.entidad_bancaria_id, f.estado, f.numero_oficio,
               f.fecha_oficio, f.monto, f.municipio_id,
               m.nombre as municipio_etl, d.nombre as departamento_etl,
               f.nombre_demandado, f.id_demandado
        FROM fact_oficios f
        LEFT JOIN dim_entidades e ON f.entidad_remitente_id = e.entidad_id
        LEFT JOIN dim_municipios m ON f.municipio_id = m.municipio_id
        LEFT JOIN dim_departamentos d ON f.departamento_id = d.departamento_id
        WHERE f.oficio_id IN ({placeholders})
    """, sample_ids)
    etl_data = {r["oficio_id"]: r for r in cur_etl.fetchall()}

    # Obtener datos originales
    cur_orig.execute(f"""
        SELECT e.id, e.entidad_remitente, e.entidad_bancaria_id,
               e.estado_embargo, e.oficio, e.fecha_oficio, e.monto, e.ciudad
        FROM embargos e
        WHERE e.id IN ({placeholders})
    """, sample_ids)
    orig_data = {r["id"]: r for r in cur_orig.fetchall()}

    # Comparar campo por campo
    errors = defaultdict(int)
    matches = defaultdict(int)
    total_compared = 0
    mismatch_examples = defaultdict(list)

    for oid in sample_ids:
        if oid not in etl_data or oid not in orig_data:
            continue
        total_compared += 1
        e = etl_data[oid]
        o = orig_data[oid]

        # Estado
        if e["estado"] and o["estado_embargo"]:
            if e["estado"].strip().upper() == o["estado_embargo"].strip().upper():
                matches["estado"] += 1
            else:
                errors["estado"] += 1
                if len(mismatch_examples["estado"]) < 3:
                    mismatch_examples["estado"].append(
                        f"ID={oid}: ETL='{e['estado']}' vs ORIG='{o['estado_embargo']}'"
                    )

        # Entidad bancaria ID
        if e["entidad_bancaria_id"] is not None and o["entidad_bancaria_id"] is not None:
            if int(e["entidad_bancaria_id"]) == int(o["entidad_bancaria_id"]):
                matches["entidad_bancaria_id"] += 1
            else:
                errors["entidad_bancaria_id"] += 1
                if len(mismatch_examples["entidad_bancaria_id"]) < 3:
                    mismatch_examples["entidad_bancaria_id"].append(
                        f"ID={oid}: ETL={e['entidad_bancaria_id']} vs ORIG={o['entidad_bancaria_id']}"
                    )

        # Fecha oficio
        e_fecha = str(e["fecha_oficio"]) if e["fecha_oficio"] else None
        o_fecha = str(o["fecha_oficio"]) if o["fecha_oficio"] else None
        if e_fecha and o_fecha:
            if e_fecha == o_fecha:
                matches["fecha_oficio"] += 1
            else:
                errors["fecha_oficio"] += 1
                if len(mismatch_examples["fecha_oficio"]) < 3:
                    mismatch_examples["fecha_oficio"].append(
                        f"ID={oid}: ETL='{e_fecha}' vs ORIG='{o_fecha}'"
                    )

        # Monto
        e_monto = float(e["monto"]) if e["monto"] else None
        o_monto = float(o["monto"]) if o["monto"] else None
        if e_monto is not None and o_monto is not None:
            if abs(e_monto - o_monto) < 0.01:
                matches["monto"] += 1
            else:
                errors["monto"] += 1
                if len(mismatch_examples["monto"]) < 3:
                    mismatch_examples["monto"].append(
                        f"ID={oid}: ETL={e_monto} vs ORIG={o_monto}"
                    )

        # Ciudad vs municipio (comparación normalizada)
        if e["municipio_etl"] and o["ciudad"]:
            e_city = e["municipio_etl"].strip().upper()
            o_city = o["ciudad"].strip().upper()
            # Normalizar para comparar
            if e_city in o_city or o_city in e_city or e_city == o_city:
                matches["ciudad_municipio"] += 1
            else:
                errors["ciudad_municipio"] += 1
                if len(mismatch_examples["ciudad_municipio"]) < 5:
                    mismatch_examples["ciudad_municipio"].append(
                        f"ID={oid}: ETL='{e['municipio_etl']}' vs ORIG='{o['ciudad']}'"
                    )

    subheader("Resultados por campo")
    for field in ["estado", "entidad_bancaria_id", "fecha_oficio", "monto", "ciudad_municipio"]:
        m = matches.get(field, 0)
        e = errors.get(field, 0)
        total = m + e
        if total > 0:
            pct = m / total * 100
            ok = True if pct >= 99 else ("warn" if pct >= 95 else False)
            result(field, f"{m:,}/{total:,} coinciden ({pct:.2f}%)", ok=ok)
            if field in mismatch_examples and mismatch_examples[field]:
                for ex in mismatch_examples[field]:
                    log(f"      Ejemplo: {ex}")
        else:
            result(field, "Sin datos comparables", ok="warn")

    log(f"\n  Total registros comparados: {total_compared:,}")

    cur_orig.close()
    conn_orig.close()
    cur_etl.close()
    conn_etl.close()


# ═══════════════════════════════════════════════════════════════
# 5. VERIFICACIÓN DE ENTIDAD REMITENTE (NOMBRE)
# ═══════════════════════════════════════════════════════════════

def test_entidad_remitente():
    header("5. VERIFICACIÓN DE NORMALIZACIÓN DE ENTIDAD REMITENTE")

    conn_orig = get_conn(DB_ORIGINAL)
    cur_orig = conn_orig.cursor()
    conn_etl = get_conn(DB_ETL)
    cur_etl = conn_etl.cursor()

    # Muestra de 5000 para verificar que la entidad normalizada coincide razonablemente
    cur_etl.execute("""
        SELECT f.oficio_id, e.nombre_normalizado
        FROM fact_oficios f
        JOIN dim_entidades e ON f.entidad_remitente_id = e.entidad_id
        ORDER BY RAND() LIMIT 5000
    """)
    etl_sample = {r[0]: r[1] for r in cur_etl.fetchall()}

    ids = list(etl_sample.keys())
    placeholders = ",".join(["%s"] * len(ids))
    cur_orig.execute(f"""
        SELECT id, entidad_remitente FROM embargos WHERE id IN ({placeholders})
    """, ids)
    orig_map = {r[0]: r[1] for r in cur_orig.fetchall()}

    # Comparar: el nombre normalizado debe ser substring/contenido en el original
    match_count = 0
    mismatch_count = 0
    mismatch_examples = []

    import unicodedata
    import re

    def normalize_text(s):
        if not s:
            return ""
        s = s.strip().upper()
        s = unicodedata.normalize("NFD", s)
        s = "".join(c for c in s if unicodedata.category(c) != "Mn" or c in "ñÑ")
        s = re.sub(r"[^A-Z0-9ÑÁÉÍÓÚ ]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    for oid in ids:
        if oid not in orig_map or not orig_map[oid]:
            continue
        etl_name = normalize_text(etl_sample[oid])
        orig_name = normalize_text(orig_map[oid])

        # Extraer palabras clave (> 3 chars) del nombre normalizado
        etl_words = set(w for w in etl_name.split() if len(w) > 3)
        orig_words = set(w for w in orig_name.split() if len(w) > 3)

        if not etl_words:
            continue

        overlap = len(etl_words & orig_words) / len(etl_words)
        if overlap >= 0.5:
            match_count += 1
        else:
            mismatch_count += 1
            if len(mismatch_examples) < 5:
                mismatch_examples.append(
                    f"ID={oid}: ETL='{etl_sample[oid]}' ← ORIG='{orig_map[oid].strip()}'"
                )

    total = match_count + mismatch_count
    if total > 0:
        pct = match_count / total * 100
        result("Entidades cuyo nombre normalizado coincide con original",
               f"{match_count:,}/{total:,} ({pct:.2f}%)",
               ok=True if pct >= 95 else ("warn" if pct >= 90 else False))
    if mismatch_examples:
        log("  Ejemplos de posible discrepancia:")
        for ex in mismatch_examples:
            log(f"    {ex}")

    cur_orig.close()
    conn_orig.close()
    cur_etl.close()
    conn_etl.close()


# ═══════════════════════════════════════════════════════════════
# 6. VERIFICACIÓN DE DEMANDADOS
# ═══════════════════════════════════════════════════════════════

def test_demandados():
    header("6. VERIFICACIÓN DE DEMANDADOS EN OFICIOS")

    conn_orig = get_conn(DB_ORIGINAL)
    cur_orig = conn_orig.cursor()
    conn_etl = get_conn(DB_ETL)
    cur_etl = conn_etl.cursor()

    # Muestra aleatoria
    cur_etl.execute("""
        SELECT oficio_id, nombre_demandado, id_demandado, tipo_id_demandado, monto_a_embargar
        FROM fact_oficios
        WHERE nombre_demandado IS NOT NULL
        ORDER BY RAND() LIMIT 5000
    """)
    etl_sample = {r[0]: {"nombre": r[1], "id": r[2], "tipo": r[3], "monto": r[4]} for r in cur_etl.fetchall()}

    ids = list(etl_sample.keys())
    placeholders = ",".join(["%s"] * len(ids))
    cur_orig.execute(f"""
        SELECT d.embargo_id, d.nombres, d.identificacion, d.tipo_identificacion_tipo, d.montoaembargar
        FROM demandado d
        WHERE d.embargo_id IN ({placeholders})
    """, ids)

    orig_demandados = defaultdict(list)
    for r in cur_orig.fetchall():
        orig_demandados[r[0]].append({
            "nombre": r[1], "id": r[2], "tipo": r[3], "monto": r[4]
        })

    match_nombre = 0
    match_id = 0
    total_checked = 0
    mismatch_examples = []

    for oid in ids:
        if oid not in orig_demandados:
            continue
        total_checked += 1
        etl_d = etl_sample[oid]
        # Check if any demandado in original matches
        found_name = False
        found_id = False
        for orig_d in orig_demandados[oid]:
            if etl_d["nombre"] and orig_d["nombre"]:
                if etl_d["nombre"].strip().upper()[:20] in orig_d["nombre"].strip().upper() or \
                   orig_d["nombre"].strip().upper()[:20] in etl_d["nombre"].strip().upper():
                    found_name = True
            if etl_d["id"] and orig_d["id"]:
                if etl_d["id"].strip() == orig_d["id"].strip():
                    found_id = True
        if found_name:
            match_nombre += 1
        if found_id:
            match_id += 1
        if not found_name and not found_id and len(mismatch_examples) < 3:
            mismatch_examples.append(
                f"ID={oid}: ETL nombre='{etl_d['nombre']}', id='{etl_d['id']}' "
                f"vs ORIG={[(d['nombre'], d['id']) for d in orig_demandados[oid][:2]]}"
            )

    subheader("Coincidencia de demandados")
    if total_checked > 0:
        pct_n = match_nombre / total_checked * 100
        pct_i = match_id / total_checked * 100
        result("Nombre demandado coincide", f"{match_nombre:,}/{total_checked:,} ({pct_n:.2f}%)",
               ok=True if pct_n >= 95 else ("warn" if pct_n >= 90 else False))
        result("ID demandado coincide", f"{match_id:,}/{total_checked:,} ({pct_i:.2f}%)",
               ok=True if pct_i >= 95 else ("warn" if pct_i >= 90 else False))
    if mismatch_examples:
        for ex in mismatch_examples:
            log(f"    {ex}")

    cur_orig.close()
    conn_orig.close()
    cur_etl.close()
    conn_etl.close()


# ═══════════════════════════════════════════════════════════════
# 7. DISTRIBUCIÓN DE ESTADOS
# ═══════════════════════════════════════════════════════════════

def test_distribucion_estados():
    header("7. COMPARACIÓN DE DISTRIBUCIÓN DE ESTADOS")

    conn_orig = get_conn(DB_ORIGINAL)
    cur_orig = conn_orig.cursor()
    conn_etl = get_conn(DB_ETL)
    cur_etl = conn_etl.cursor()

    cur_orig.execute("""
        SELECT estado_embargo, COUNT(*) FROM embargos
        WHERE deleted = 0
        GROUP BY estado_embargo ORDER BY COUNT(*) DESC
    """)
    orig_states = dict(cur_orig.fetchall())

    cur_etl.execute("""
        SELECT estado, COUNT(*) FROM fact_oficios
        GROUP BY estado ORDER BY COUNT(*) DESC
    """)
    etl_states = dict(cur_etl.fetchall())

    all_states = set(list(orig_states.keys()) + list(etl_states.keys()))

    log(f"\n  {'Estado':<30} {'Original':>12} {'ETL':>12} {'Diff':>10} {'%Diff':>8}")
    log(f"  {'-'*30} {'-'*12} {'-'*12} {'-'*10} {'-'*8}")

    total_orig = sum(orig_states.values())
    total_etl = sum(etl_states.values())

    for state in sorted(all_states):
        o = orig_states.get(state, 0)
        e = etl_states.get(state, 0)
        diff = e - o
        pct = (diff / o * 100) if o > 0 else 0
        log(f"  {str(state):<30} {o:>12,} {e:>12,} {diff:>+10,} {pct:>+7.1f}%")

    log(f"  {'TOTAL':<30} {total_orig:>12,} {total_etl:>12,} {total_etl - total_orig:>+10,}")

    cur_orig.close()
    conn_orig.close()
    cur_etl.close()
    conn_etl.close()


# ═══════════════════════════════════════════════════════════════
# 8. DISTRIBUCIÓN POR ENTIDAD BANCARIA
# ═══════════════════════════════════════════════════════════════

def test_distribucion_bancaria():
    header("8. COMPARACIÓN POR ENTIDAD BANCARIA")

    conn_orig = get_conn(DB_ORIGINAL)
    cur_orig = conn_orig.cursor()
    conn_etl = get_conn(DB_ETL)
    cur_etl = conn_etl.cursor()

    cur_orig.execute("""
        SELECT eb.nombre, COUNT(*)
        FROM embargos e
        JOIN entidad_bancaria eb ON e.entidad_bancaria_id = eb.id
        WHERE e.deleted = 0
        GROUP BY eb.nombre ORDER BY COUNT(*) DESC
    """)
    orig_bank = dict(cur_orig.fetchall())

    cur_etl.execute("""
        SELECT b.nombre, COUNT(*)
        FROM fact_oficios f
        JOIN dim_entidad_bancaria b ON f.entidad_bancaria_id = b.entidad_bancaria_id
        GROUP BY b.nombre ORDER BY COUNT(*) DESC
    """)
    etl_bank = dict(cur_etl.fetchall())

    all_banks = set(list(orig_bank.keys()) + list(etl_bank.keys()))

    log(f"\n  {'Entidad Bancaria':<20} {'Original':>12} {'ETL':>12} {'Diff':>10} {'%Diff':>8}")
    log(f"  {'-'*20} {'-'*12} {'-'*12} {'-'*10} {'-'*8}")

    for bank in sorted(all_banks):
        o = orig_bank.get(bank, 0)
        e = etl_bank.get(bank, 0)
        diff = e - o
        pct = (diff / o * 100) if o > 0 else 0
        log(f"  {str(bank):<20} {o:>12,} {e:>12,} {diff:>+10,} {pct:>+7.1f}%")

    cur_orig.close()
    conn_orig.close()
    cur_etl.close()
    conn_etl.close()


# ═══════════════════════════════════════════════════════════════
# 9. VERIFICACIÓN DE MONTOS AGREGADOS
# ═══════════════════════════════════════════════════════════════

def test_montos():
    header("9. COMPARACIÓN DE MONTOS AGREGADOS")

    conn_orig = get_conn(DB_ORIGINAL)
    cur_orig = conn_orig.cursor()
    conn_etl = get_conn(DB_ETL)
    cur_etl = conn_etl.cursor()

    # Montos en original (embargos no eliminados)
    cur_orig.execute("""
        SELECT SUM(monto), AVG(monto), MIN(monto), MAX(monto),
               COUNT(CASE WHEN monto IS NOT NULL AND monto > 0 THEN 1 END)
        FROM embargos WHERE deleted = 0
    """)
    o = cur_orig.fetchone()

    # Montos en ETL
    cur_etl.execute("""
        SELECT SUM(monto), AVG(monto), MIN(monto), MAX(monto),
               COUNT(CASE WHEN monto IS NOT NULL AND monto > 0 THEN 1 END)
        FROM fact_oficios
    """)
    e = cur_etl.fetchone()

    subheader("Monto (embargo)")
    log(f"  {'Métrica':<30} {'Original':>25} {'ETL':>25}")
    log(f"  {'-'*30} {'-'*25} {'-'*25}")
    labels = ["SUM", "AVG", "MIN", "MAX", "COUNT (>0)"]
    for i, label in enumerate(labels):
        ov = f"{o[i]:,.2f}" if o[i] is not None else "NULL"
        ev = f"{e[i]:,.2f}" if e[i] is not None else "NULL"
        log(f"  {label:<30} {ov:>25} {ev:>25}")

    # Monto a embargar (de demandados en original vs ETL)
    cur_orig.execute("""
        SELECT SUM(d.montoaembargar)
        FROM demandado d
        JOIN embargos e ON d.embargo_id = e.id
        WHERE e.deleted = 0
    """)
    orig_monto_embargar = cur_orig.fetchone()[0]

    cur_etl.execute("SELECT SUM(monto_a_embargar) FROM fact_oficios")
    etl_monto_embargar = cur_etl.fetchone()[0]

    subheader("Monto a embargar (agregado)")
    result("Original (SUM demandado.montoaembargar)",
           f"{orig_monto_embargar:,.2f}" if orig_monto_embargar else "NULL")
    result("ETL (SUM fact_oficios.monto_a_embargar)",
           f"{etl_monto_embargar:,.2f}" if etl_monto_embargar else "NULL")

    if orig_monto_embargar and etl_monto_embargar:
        diff_pct = abs(float(etl_monto_embargar) - float(orig_monto_embargar)) / float(orig_monto_embargar) * 100
        result("Diferencia porcentual", f"{diff_pct:.4f}%",
               ok=True if diff_pct < 1 else ("warn" if diff_pct < 5 else False))

    cur_orig.close()
    conn_orig.close()
    cur_etl.close()
    conn_etl.close()


# ═══════════════════════════════════════════════════════════════
# 10. COBERTURA DE DIMENSIONES
# ═══════════════════════════════════════════════════════════════

def test_cobertura():
    header("10. COBERTURA DE DIMENSIONES EN ETL")

    conn = get_conn(DB_ETL)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM fact_oficios")
    total = cur.fetchone()[0]

    checks = [
        ("entidad_remitente_id IS NOT NULL", "Con entidad remitente"),
        ("entidad_bancaria_id IS NOT NULL", "Con entidad bancaria"),
        ("municipio_id IS NOT NULL", "Con municipio"),
        ("departamento_id IS NOT NULL", "Con departamento"),
        ("nombre_demandado IS NOT NULL", "Con nombre demandado"),
        ("fecha_oficio IS NOT NULL", "Con fecha oficio"),
        ("monto IS NOT NULL AND monto > 0", "Con monto > 0"),
        ("numero_oficio IS NOT NULL", "Con número oficio"),
    ]

    for condition, label in checks:
        cur.execute(f"SELECT COUNT(*) FROM fact_oficios WHERE {condition}")
        count = cur.fetchone()[0]
        pct = count / total * 100
        result(label, f"{count:,}/{total:,} ({pct:.1f}%)")

    cur.close()
    conn.close()


# ═══════════════════════════════════════════════════════════════
# 11. RESUMEN FINAL
# ═══════════════════════════════════════════════════════════════

def resumen_final():
    header("RESUMEN EJECUTIVO")
    log()
    log("  Base original: pyc_embargos (embargos, demandante, demandado, entidad_bancaria)")
    log("  Modelo ETL: ETL (dim_departamentos, dim_municipios, dim_entidades,")
    log("              dim_variantes, dim_entidad_bancaria, fact_oficios)")
    log()
    log("  Este diagnóstico verifica:")
    log("    1. Conteos entre original y ETL")
    log("    2. Integridad referencial (todas las FK)")
    log("    3. Cruce de IDs 1:1 entre embargos y fact_oficios")
    log("    4. Coincidencia de campos clave (muestreo)")
    log("    5. Normalización correcta de entidades remitentes")
    log("    6. Preservación de datos de demandados")
    log("    7-8. Distribuciones consistentes (estados, bancos)")
    log("    9. Montos agregados consistentes")
    log("    10. Cobertura de dimensiones")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    log(f"DIAGNÓSTICO DE CALIDAD DE DATOS")
    log(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Original: {DB_ORIGINAL}  →  ETL: {DB_ETL}")

    create_dim_entidad_bancaria()
    test_conteos()
    test_integridad_referencial()
    test_cruce_ids()
    test_campos_clave()
    test_entidad_remitente()
    test_demandados()
    test_distribucion_estados()
    test_distribucion_bancaria()
    test_montos()
    test_cobertura()
    resumen_final()

    # Guardar reporte
    report_path = "/Users/geinermartinezmoscoso/Desktop/entidades/modelo_final/DIAGNOSTICO_CALIDAD.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    log(f"\n📄 Reporte guardado en: {report_path}")


if __name__ == "__main__":
    main()
