"""
Reestructuración de embargos_limpios.csv:
1. Elimina oficios SIN_CONFIRMAR (sin procesar, no sirven)
2. Elimina columnas 100% vacías: nombre_demandante, id_demandante, tipo_id_demandante
3. Elimina nombre_remitente (desnormalizado, se obtiene via entidad_remitente_id → entidades.csv)
4. Genera embargos_final.csv (tabla de hechos) con FK a entidades.csv

Modelo relacional resultante:
  entidades.csv        (entidad_id PK)
  embargos_final.csv   (embargo_id PK, entidad_remitente_id FK, entidad_bancaria_id FK)
"""

import csv
import sys

INPUT = "embargos_limpios.csv"
OUTPUT = "embargos_final.csv"
ENTIDADES = "entidades.csv"

# Columnas a eliminar
DROP_COLS = {
    "nombre_demandante",   # 100% vacía
    "id_demandante",       # 100% vacía
    "tipo_id_demandante",  # 100% vacía
    "nombre_remitente",    # desnormalizado → usar entidad_remitente_id
}

# Estados a descartar
DISCARD_ESTADOS = {"SIN_CONFIRMAR"}


def main():
    # Verificar entidades.csv existe y cargar IDs
    entidad_ids = set()
    with open(ENTIDADES, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entidad_ids.add(row["entidad_id"])
    print(f"[INFO] Entidades cargadas: {len(entidad_ids)}")

    # Procesar embargos
    total = 0
    descartados = 0
    escritos = 0
    fk_missing = 0

    with open(INPUT, "r", encoding="utf-8") as fin, \
         open(OUTPUT, "w", encoding="utf-8", newline="") as fout:

        reader = csv.DictReader(fin)
        out_fields = [f for f in reader.fieldnames if f not in DROP_COLS]
        writer = csv.DictWriter(fout, fieldnames=out_fields)
        writer.writeheader()

        for row in reader:
            total += 1

            # Descartar oficios sin procesar
            if row["estado_embargo"] in DISCARD_ESTADOS:
                descartados += 1
                continue

            # Validar FK
            if row["entidad_remitente_id"] not in entidad_ids:
                fk_missing += 1

            # Escribir fila sin columnas eliminadas
            out_row = {k: row[k] for k in out_fields}
            writer.writerow(out_row)
            escritos += 1

    print(f"\n{'='*50}")
    print(f"RESUMEN DE REESTRUCTURACIÓN")
    print(f"{'='*50}")
    print(f"Total registros leídos:       {total:>10,}")
    print(f"Descartados (SIN_CONFIRMAR):  {descartados:>10,}")
    print(f"Registros escritos:           {escritos:>10,}")
    print(f"FK huérfanas (remitente):     {fk_missing:>10,}")
    print(f"Columnas eliminadas:          {', '.join(sorted(DROP_COLS))}")
    print(f"\nArchivos generados:")
    print(f"  → {OUTPUT}  (tabla de hechos)")
    print(f"  → {ENTIDADES}  (tabla de entidades, sin cambios)")
    print(f"\nRelaciones:")
    print(f"  embargos_final.entidad_remitente_id → entidades.entidad_id")
    print(f"  embargos_final.entidad_bancaria_id  → entidades.entidad_id")


if __name__ == "__main__":
    main()
