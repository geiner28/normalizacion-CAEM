#!/usr/bin/env python3
"""
Deduplicación de fact_oficios en la BD ETL.

Criterio: cuando hay múltiples registros con la misma combinación de:
  - numero_oficio + entidad_remitente_id + entidad_bancaria_id + 
    fecha_oficio + nombre_demandado + id_demandado

Se conserva 1 solo registro, priorizando:
  1. Estado más avanzado: PROCESADO > RECONFIRMADO > CONFIRMADO > otros
  2. oficio_id más reciente (mayor)
"""

import pymysql
import os
from collections import defaultdict

HOST = os.environ.get("DB_HOST", "127.0.0.1")
PORT = int(os.environ.get("DB_PORT", 3306))
USER = os.environ.get("DB_USER", "producto")
PASS = os.environ.get("DB_PASSWORD", "")
DB = "ETL"

ESTADO_PRIORIDAD = {
    "PROCESADO": 1,
    "RECONFIRMADO": 2,
    "CONFIRMADO": 3,
    "PROCESADO_CON_ERRORES": 4,
    "EN_PROCESO": 5,
    "SIN_CONFIRMAR": 6,
}


def main():
    conn = pymysql.connect(
        host=HOST, port=PORT, user=USER, password=PASS,
        database=DB, charset="utf8mb4"
    )
    cur = conn.cursor()

    # Conteo inicial
    cur.execute("SELECT COUNT(*) FROM fact_oficios")
    total_antes = cur.fetchone()[0]
    print(f"Total registros antes: {total_antes:,}")

    # ── Paso 1: Cargar todos los registros con numero_oficio en memoria ──
    print("\nCargando registros para deduplicación...", flush=True)
    cur.execute("""
        SELECT oficio_id, numero_oficio, entidad_remitente_id, entidad_bancaria_id,
               fecha_oficio, nombre_demandado, id_demandado, estado
        FROM fact_oficios
        WHERE numero_oficio IS NOT NULL
    """)

    # Agrupar por clave de deduplicación
    groups = defaultdict(list)
    for row in cur:
        oficio_id = row[0]
        key = (
            row[1],  # numero_oficio
            row[2],  # entidad_remitente_id
            row[3],  # entidad_bancaria_id
            str(row[4]) if row[4] else None,  # fecha_oficio
            (row[5] or "").strip().upper(),    # nombre_demandado
            (row[6] or "").strip().upper(),    # id_demandado
        )
        estado = row[7] or ""
        groups[key].append((oficio_id, estado))

    print(f"Grupos únicos: {len(groups):,}")

    # ── Paso 2: Determinar cuáles eliminar ──
    ids_to_delete = []
    for key, records in groups.items():
        if len(records) <= 1:
            continue

        # Ordenar: mejor estado primero, luego oficio_id más alto
        records.sort(key=lambda r: (
            ESTADO_PRIORIDAD.get(r[1], 99),
            # Negamos para que el mayor ID quede primero
        ))
        # De los que tienen el mejor estado, quedarnos con el oficio_id mayor
        best_estado_priority = ESTADO_PRIORIDAD.get(records[0][1], 99)
        best_candidates = [r for r in records if ESTADO_PRIORIDAD.get(r[1], 99) == best_estado_priority]
        # Del grupo con mejor estado, conservar el oficio_id más reciente
        best_candidates.sort(key=lambda r: r[0], reverse=True)
        keeper = best_candidates[0][0]

        for oid, _ in records:
            if oid != keeper:
                ids_to_delete.append(oid)

    print(f"Registros a eliminar: {len(ids_to_delete):,}")
    print(f"Registros proyectados después: {total_antes - len(ids_to_delete):,}")

    if not ids_to_delete:
        print("\nNo hay duplicados para eliminar.")
        return

    # ── Paso 3: Eliminar en lotes ──
    print("\nEliminando duplicados...", flush=True)
    batch_size = 5000
    deleted = 0
    for i in range(0, len(ids_to_delete), batch_size):
        batch = ids_to_delete[i:i + batch_size]
        placeholders = ",".join(["%s"] * len(batch))
        cur.execute(f"DELETE FROM fact_oficios WHERE oficio_id IN ({placeholders})", batch)
        conn.commit()
        deleted += len(batch)
        print(f"  ... {deleted:,}/{len(ids_to_delete):,} eliminados", flush=True)

    # ── Paso 4: Verificación ──
    cur.execute("SELECT COUNT(*) FROM fact_oficios")
    total_despues = cur.fetchone()[0]
    print(f"\n{'='*60}")
    print(f"RESULTADO DE DEDUPLICACIÓN")
    print(f"{'='*60}")
    print(f"  Antes:     {total_antes:,}")
    print(f"  Eliminados: {total_antes - total_despues:,}")
    print(f"  Después:   {total_despues:,}")
    print(f"  Reducción:  {(total_antes - total_despues)/total_antes*100:.1f}%")

    # Verificar que no quedaron duplicados
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT 1 FROM fact_oficios
            WHERE numero_oficio IS NOT NULL
            GROUP BY numero_oficio, entidad_remitente_id, entidad_bancaria_id,
                     fecha_oficio, nombre_demandado, id_demandado
            HAVING COUNT(*) > 1
        ) t
    """)
    remaining_dups = cur.fetchone()[0]
    if remaining_dups == 0:
        print(f"\n  ✅ 0 duplicados restantes")
    else:
        print(f"\n  ⚠️ {remaining_dups:,} grupos aún duplicados")

    # Distribución de estados post-dedup
    print(f"\nDistribución de estados post-dedup:")
    cur.execute("SELECT estado, COUNT(*) FROM fact_oficios GROUP BY estado ORDER BY COUNT(*) DESC")
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]:,}")

    cur.close()
    conn.close()
    print("\n✅ Deduplicación completada")


if __name__ == "__main__":
    main()
