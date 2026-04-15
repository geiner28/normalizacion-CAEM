#!/usr/bin/env python3
"""
Carga el modelo de datos (5 tablas) a MySQL via Cloud SQL Proxy.
Requiere: PyMySQL
"""

import csv
import sys
import os
import pymysql

# ── Configuración ──────────────────────────────────────────────
HOST = os.environ.get("DB_HOST", "127.0.0.1")
PORT = int(os.environ.get("DB_PORT", 3306))
USER = os.environ.get("DB_USER", "producto")
PASSWORD = os.environ.get("DB_PASSWORD", "")
DATABASE = os.environ.get("DB_NAME", "ETL")
BASE_DIR = os.environ.get("MODEL_DIR", os.path.join(os.path.dirname(__file__), "..", "datos", "modelo_final"))

# ── DDL ────────────────────────────────────────────────────────
DDL_STATEMENTS = [
    "DROP TABLE IF EXISTS fact_oficios",
    "DROP TABLE IF EXISTS dim_variantes",
    "DROP TABLE IF EXISTS dim_entidades",
    "DROP TABLE IF EXISTS dim_municipios",
    "DROP TABLE IF EXISTS dim_departamentos",
    """
    CREATE TABLE dim_departamentos (
        departamento_id  INT PRIMARY KEY,
        nombre           VARCHAR(100) NOT NULL UNIQUE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE dim_municipios (
        municipio_id     INT PRIMARY KEY,
        nombre           VARCHAR(150) NOT NULL,
        departamento_id  INT NOT NULL,
        FOREIGN KEY (departamento_id) REFERENCES dim_departamentos(departamento_id),
        INDEX idx_municipios_depto (departamento_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE dim_entidades (
        entidad_id          INT PRIMARY KEY,
        nombre_normalizado  VARCHAR(500) NOT NULL,
        tipo                VARCHAR(50),
        subtipo             VARCHAR(50),
        municipio_id        INT,
        departamento_id     INT,
        total_registros     INT DEFAULT 0,
        num_variantes       INT DEFAULT 0,
        FOREIGN KEY (municipio_id)    REFERENCES dim_municipios(municipio_id),
        FOREIGN KEY (departamento_id) REFERENCES dim_departamentos(departamento_id),
        INDEX idx_entidades_tipo (tipo),
        INDEX idx_entidades_muni (municipio_id),
        INDEX idx_entidades_depto (departamento_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE dim_variantes (
        variante_id         INT PRIMARY KEY,
        entidad_id          INT NOT NULL,
        nombre_normalizado  VARCHAR(500),
        variante_original   VARCHAR(500) NOT NULL,
        conteo              INT DEFAULT 0,
        FOREIGN KEY (entidad_id) REFERENCES dim_entidades(entidad_id),
        INDEX idx_variantes_entidad (entidad_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE fact_oficios (
        oficio_id              VARCHAR(20) PRIMARY KEY,
        entidad_remitente_id   INT,
        entidad_bancaria_id    INT,
        estado                 VARCHAR(30),
        numero_oficio          VARCHAR(250),
        fecha_oficio           DATE,
        fecha_recepcion        DATE,
        titulo_embargo         VARCHAR(50),
        titulo_orden           VARCHAR(50),
        monto                  DECIMAL(20,2),
        monto_a_embargar       DECIMAL(20,2),
        nombre_demandado       VARCHAR(300),
        id_demandado           VARCHAR(50),
        tipo_id_demandado      VARCHAR(30),
        direccion_remitente    VARCHAR(500),
        correo_remitente       VARCHAR(200),
        nombre_funcionario     VARCHAR(200),
        municipio_id           INT,
        departamento_id        INT,
        fuente_ubicacion       VARCHAR(30),
        referencia             VARCHAR(200),
        expediente             VARCHAR(200),
        created_at             DATETIME,
        confirmed_at           DATETIME,
        processed_at           DATETIME,
        FOREIGN KEY (entidad_remitente_id) REFERENCES dim_entidades(entidad_id),
        FOREIGN KEY (municipio_id)         REFERENCES dim_municipios(municipio_id),
        FOREIGN KEY (departamento_id)      REFERENCES dim_departamentos(departamento_id),
        INDEX idx_oficios_entidad (entidad_remitente_id),
        INDEX idx_oficios_estado (estado),
        INDEX idx_oficios_muni (municipio_id),
        INDEX idx_oficios_depto (departamento_id),
        INDEX idx_oficios_fecha (fecha_oficio)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
]

# ── Helpers ────────────────────────────────────────────────────

def safe_int(val):
    if val is None or val == "":
        return None
    return int(val)

def safe_float(val):
    if val is None or val == "":
        return None
    return float(val)

def safe_date(val):
    if val is None or val == "":
        return None
    return val  # ya viene en formato YYYY-MM-DD

def safe_str(val):
    if val is None or val == "":
        return None
    return val

def load_csv(filename):
    path = f"{BASE_DIR}/{filename}"
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def insert_batch(cursor, sql, rows, batch_size=5000):
    """Inserta filas en lotes."""
    total = len(rows)
    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(sql, batch)
        done = min(i + batch_size, total)
        print(f"  ... {done:,}/{total:,} filas insertadas", flush=True)


# ── Carga por tabla ───────────────────────────────────────────

def load_departamentos(cursor):
    print("\n📦 Cargando dim_departamentos...", flush=True)
    rows = load_csv("dim_departamentos.csv")
    data = [(safe_int(r["departamento_id"]), r["nombre"]) for r in rows]
    sql = "INSERT INTO dim_departamentos (departamento_id, nombre) VALUES (%s, %s)"
    insert_batch(cursor, sql, data)
    print(f"  ✅ {len(data)} departamentos cargados")


def load_municipios(cursor):
    print("\n📦 Cargando dim_municipios...", flush=True)
    rows = load_csv("dim_municipios.csv")
    data = [(safe_int(r["municipio_id"]), r["nombre"], safe_int(r["departamento_id"])) for r in rows]
    sql = "INSERT INTO dim_municipios (municipio_id, nombre, departamento_id) VALUES (%s, %s, %s)"
    insert_batch(cursor, sql, data)
    print(f"  ✅ {len(data)} municipios cargados")


def load_entidades(cursor):
    print("\n📦 Cargando dim_entidades...", flush=True)
    rows = load_csv("dim_entidades.csv")
    data = [
        (
            safe_int(r["entidad_id"]),
            r["nombre_normalizado"],
            safe_str(r.get("tipo")),
            safe_str(r.get("subtipo")),
            safe_int(r.get("municipio_id")),
            safe_int(r.get("departamento_id")),
            safe_int(r.get("total_registros")),
            safe_int(r.get("num_variantes")),
        )
        for r in rows
    ]
    sql = """INSERT INTO dim_entidades
             (entidad_id, nombre_normalizado, tipo, subtipo,
              municipio_id, departamento_id, total_registros, num_variantes)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    insert_batch(cursor, sql, data)
    print(f"  ✅ {len(data)} entidades cargadas")


def load_variantes(cursor):
    print("\n📦 Cargando dim_variantes...", flush=True)
    rows = load_csv("dim_variantes.csv")
    data = [
        (
            safe_int(r["variante_id"]),
            safe_int(r["entidad_id"]),
            safe_str(r.get("nombre_normalizado")),
            r["variante_original"],
            safe_int(r.get("conteo")),
        )
        for r in rows
    ]
    sql = """INSERT INTO dim_variantes
             (variante_id, entidad_id, nombre_normalizado, variante_original, conteo)
             VALUES (%s, %s, %s, %s, %s)"""
    insert_batch(cursor, sql, data)
    print(f"  ✅ {len(data)} variantes cargadas")


def load_oficios(cursor):
    print("\n📦 Cargando fact_oficios...", flush=True)
    path = f"{BASE_DIR}/fact_oficios.csv"
    sql = """INSERT INTO fact_oficios
             (oficio_id, entidad_remitente_id, entidad_bancaria_id, estado,
              numero_oficio, fecha_oficio, fecha_recepcion, titulo_embargo,
              titulo_orden, monto, monto_a_embargar, nombre_demandado,
              id_demandado, tipo_id_demandado, direccion_remitente,
              correo_remitente, nombre_funcionario, municipio_id,
              departamento_id, fuente_ubicacion,
              referencia, expediente, created_at, confirmed_at, processed_at)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    batch = []
    batch_size = 10000
    total = 0

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            row = (
                r["oficio_id"],
                safe_int(r.get("entidad_remitente_id")),
                safe_int(r.get("entidad_bancaria_id")),
                safe_str(r.get("estado")),
                safe_str(r.get("numero_oficio")),
                safe_date(r.get("fecha_oficio")),
                safe_date(r.get("fecha_recepcion")),
                safe_str(r.get("titulo_embargo")),
                safe_str(r.get("titulo_orden")),
                safe_float(r.get("monto")),
                safe_float(r.get("monto_a_embargar")),
                safe_str(r.get("nombre_demandado")),
                safe_str(r.get("id_demandado")),
                safe_str(r.get("tipo_id_demandado")),
                safe_str(r.get("direccion_remitente")),
                safe_str(r.get("correo_remitente")),
                safe_str(r.get("nombre_funcionario")),
                safe_int(r.get("municipio_id")),
                safe_int(r.get("departamento_id")),
                safe_str(r.get("fuente_ubicacion")),
                safe_str(r.get("referencia")),
                safe_str(r.get("expediente")),
                safe_str(r.get("created_at")),
                safe_str(r.get("confirmed_at")),
                safe_str(r.get("processed_at")),
            )
            batch.append(row)
            if len(batch) >= batch_size:
                cursor.executemany(sql, batch)
                total += len(batch)
                print(f"  ... {total:,} filas insertadas", flush=True)
                batch = []

    if batch:
        cursor.executemany(sql, batch)
        total += len(batch)

    print(f"  ✅ {total:,} oficios cargados")


# ── Main ──────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("CARGA DE MODELO DE DATOS A MYSQL")
    print("=" * 60)

    print(f"\nConectando a {HOST}:{PORT} como {USER}...")
    conn = pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=30,
    )
    print("✅ Conexión establecida")

    cursor = conn.cursor()

    # Crear tablas
    print("\n" + "=" * 60)
    print("CREANDO TABLAS...")
    print("=" * 60)
    for stmt in DDL_STATEMENTS:
        cursor.execute(stmt.strip())
    conn.commit()
    print("✅ Tablas creadas exitosamente")

    # Cargar datos
    print("\n" + "=" * 60)
    print("CARGANDO DATOS...")
    print("=" * 60)

    try:
        load_departamentos(cursor)
        conn.commit()

        load_municipios(cursor)
        conn.commit()

        load_entidades(cursor)
        conn.commit()

        load_variantes(cursor)
        conn.commit()

        load_oficios(cursor)
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error durante la carga: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    # Verificación final
    print("\n" + "=" * 60)
    print("VERIFICACIÓN FINAL")
    print("=" * 60)

    conn = pymysql.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD,
        database=DATABASE, charset="utf8mb4"
    )
    cursor = conn.cursor()

    tables = [
        "dim_departamentos", "dim_municipios", "dim_entidades",
        "dim_variantes", "fact_oficios"
    ]
    for t in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {t}")
        count = cursor.fetchone()[0]
        print(f"  {t}: {count:,} filas")

    cursor.close()
    conn.close()

    print("\n✅ CARGA COMPLETADA EXITOSAMENTE")


if __name__ == "__main__":
    main()
