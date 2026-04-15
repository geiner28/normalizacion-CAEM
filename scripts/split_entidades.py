#!/usr/bin/env python3
"""
Reestructuración del Maestro de Entidades
==========================================
Divide dim_entidades en dos categorías:

  1. dim_entidades_judiciales.csv
     - Entidades de la Rama Judicial (JUZGADO, TRIBUNAL, CORTE, FISCALIA, etc.)
     - Campos: entidad_id, nombre_real, nombre_extraido, ciudad, email_real,
               email_extraido, numero_despacho, total_registros

  2. dim_entidades_coactivas.csv
     - Entidades Coactivas (DIAN, DATT, ALCALDIA, GOBERNACION, SECRETARIA, etc.)
     - Campos: entidad_id, nombre_real, nombre_extraido, ciudad, email_real,
               email_extraido, nit, total_registros

Validación judicial via: https://directoriojudicial.ramajudicial.gov.co
  → Exportar el Excel de SIERJU y colocarlo como 'directorio_sierju.xlsx' en datos/fuentes/

Uso:
  cd datos/procesados
  python ../../scripts/split_entidades.py [--sierju ../fuentes/directorio_sierju.xlsx]
"""

import csv
import os
import re
import sys
import unicodedata
from collections import defaultdict

# ── Configuración ──────────────────────────────────────────────

MODEL_DIR = os.path.join(os.path.dirname(__file__), '..', 'datos', 'modelo_final')
PROCESADOS_DIR = os.path.join(os.path.dirname(__file__), '..', 'datos', 'procesados')
FUENTES_DIR = os.path.join(os.path.dirname(__file__), '..', 'datos', 'fuentes')

# Tipos que pertenecen a la Rama Judicial
TIPOS_JUDICIALES = {
    'JUZGADO', 'TRIBUNAL', 'CORTE', 'RAMA_JUDICIAL', 'FISCALIA',
    'OFICINA_APOYO', 'CENTRO_SERVICIOS', 'DIRECCION_EJECUTIVA',
}

# Todo lo demás es entidad coactiva/administrativa
# DIAN, DATT, ALCALDIA, GOBERNACION, SECRETARIA, UGPP, SENA, CAR,
# SUPERINTENDENCIA, MINISTERIO, MUNICIPIO, ESP, CONTRALORIA, TRANSITO,
# INSTITUTO_MOVILIDAD, IDU, COLPENSIONES, POLICIA, EMCALI, OTRO, etc.


# ── Helpers ────────────────────────────────────────────────────

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
    if not text:
        return ''
    t = text.strip().upper()
    t = remove_accents(t)
    t = re.sub(r'[^A-Z0-9Ñ\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def extraer_numero_despacho(nombre, subtipo):
    """Extrae el número de despacho del nombre normalizado de una entidad judicial."""
    # Patrones: "JUZGADO 3 CIVIL MUNICIPAL DE BOGOTA" → "3"
    #           "TRIBUNAL SUPERIOR SALA PENAL 1" → "1"
    m = re.search(r'\b(\d{1,3})\b', nombre)
    if m:
        return m.group(1)
    return ''


# ── Cargar directorio SIERJU (Excel) ──────────────────────────

def load_sierju(filepath):
    """
    Carga el directorio judicial exportado de SIERJU.
    Esperado: Excel con columnas:
      Jurisdicción, Distrito, Circuito, Departamento, Municipio,
      Codigo Despacho, Nombre Despacho, Juez/Magistrado Despacho,
      Dirección Despacho, Ubicación, Teléfono
    Retorna dict: norm(nombre_despacho) -> {nombre_real, ciudad, codigo, ...}
    """
    try:
        import openpyxl
    except ImportError:
        print("  [WARN] openpyxl no instalado. Instalar con: pip install openpyxl")
        print("         Continuando sin validación SIERJU.")
        return {}

    if not os.path.exists(filepath):
        print(f"  [WARN] Archivo SIERJU no encontrado: {filepath}")
        print("         Descárgalo desde: https://directoriojudicial.ramajudicial.gov.co")
        print("         (Exportar Excel) y colócalo en datos/fuentes/directorio_sierju.xlsx")
        return {}

    print(f"  Cargando directorio SIERJU: {filepath}")
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return {}

    header = [str(c).strip() if c else '' for c in rows[0]]

    # Mapear columnas flexiblemente
    col_map = {}
    for i, h in enumerate(header):
        h_up = h.upper()
        if 'NOMBRE' in h_up and 'DESPACHO' in h_up:
            col_map['nombre'] = i
        elif 'CODIGO' in h_up and 'DESPACHO' in h_up:
            col_map['codigo'] = i
        elif 'MUNICIPIO' in h_up:
            col_map['municipio'] = i
        elif 'DEPARTAMENTO' in h_up:
            col_map['departamento'] = i
        elif 'DIRECCION' in h_up:
            col_map['direccion'] = i
        elif 'TELEFONO' in h_up:
            col_map['telefono'] = i

    if 'nombre' not in col_map:
        print("  [ERROR] No se encontró columna 'Nombre Despacho' en el archivo SIERJU")
        return {}

    directorio = {}
    for row in rows[1:]:
        nombre = str(row[col_map['nombre']]).strip() if row[col_map['nombre']] else ''
        if not nombre:
            continue

        entry = {
            'nombre_real': nombre,
            'codigo_despacho': str(row[col_map.get('codigo', 0)]).strip() if col_map.get('codigo') is not None and row[col_map['codigo']] else '',
            'municipio': str(row[col_map.get('municipio', 0)]).strip() if col_map.get('municipio') is not None and row[col_map['municipio']] else '',
            'departamento': str(row[col_map.get('departamento', 0)]).strip() if col_map.get('departamento') is not None and row[col_map['departamento']] else '',
            'direccion': str(row[col_map.get('direccion', 0)]).strip() if col_map.get('direccion') is not None and row[col_map['direccion']] else '',
            'telefono': str(row[col_map.get('telefono', 0)]).strip() if col_map.get('telefono') is not None and row[col_map['telefono']] else '',
        }

        key = norm(nombre)
        directorio[key] = entry

    wb.close()
    print(f"  → {len(directorio):,} despachos cargados desde SIERJU")
    return directorio


def match_sierju(nombre_normalizado, municipio, sierju):
    """
    Busca coincidencia de una entidad judicial en el directorio SIERJU.
    Retorna (nombre_real, email_real) o (None, None).
    """
    if not sierju:
        return None, None

    key = norm(nombre_normalizado)

    # Intento 1: coincidencia exacta normalizada
    if key in sierju:
        entry = sierju[key]
        # El email de despachos judiciales sigue el patrón:
        # [código]@cendoj.ramajudicial.gov.co
        email = ''
        if entry['codigo_despacho']:
            email = f"{entry['codigo_despacho'].lower()}@cendoj.ramajudicial.gov.co"
        return entry['nombre_real'], email

    # Intento 2: coincidencia parcial — buscar despachos que contengan las mismas palabras clave
    best_match = None
    best_score = 0
    key_words = set(key.split())

    for sierju_key, entry in sierju.items():
        sierju_words = set(sierju_key.split())
        common = key_words & sierju_words
        score = len(common) / max(len(key_words), len(sierju_words))
        if score > best_score and score >= 0.7:
            # Verificar que el municipio coincida si está disponible
            if municipio and entry['municipio']:
                if norm(municipio) == norm(entry['municipio']):
                    score += 0.2
            best_score = score
            best_match = entry

    if best_match and best_score >= 0.7:
        email = ''
        if best_match['codigo_despacho']:
            email = f"{best_match['codigo_despacho'].lower()}@cendoj.ramajudicial.gov.co"
        return best_match['nombre_real'], email

    return None, None


# ── Cargar correo extraído desde fact_oficios ──────────────────

def load_emails_from_oficios(fact_oficios_path):
    """
    Extrae el correo_remitente más frecuente por entidad_remitente_id
    desde fact_oficios.csv.
    """
    email_counts = defaultdict(lambda: defaultdict(int))

    with open(fact_oficios_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            eid = row.get('entidad_remitente_id', '').strip()
            correo = row.get('correo_remitente', '').strip()
            if eid and correo and correo.lower() not in ('', 'no encontrada', 'no encontrado', 'noencontrada'):
                email_counts[eid][correo.lower()] += 1

    # Seleccionar el correo más frecuente por entidad
    best_emails = {}
    for eid, emails in email_counts.items():
        best = max(emails, key=emails.get)
        best_emails[eid] = best

    return best_emails


# ── Cargar municipios ──────────────────────────────────────────

def load_municipios(dim_municipios_path):
    """Carga lookup municipio_id -> nombre."""
    lookup = {}
    with open(dim_municipios_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            lookup[row['municipio_id']] = row['nombre']
    return lookup


# ── Main ───────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Dividir maestro de entidades en judiciales y coactivas')
    parser.add_argument('--sierju', default=os.path.join(FUENTES_DIR, 'directorio_sierju.xlsx'),
                        help='Ruta al Excel exportado de SIERJU (directorio judicial)')
    args = parser.parse_args()

    print("=" * 70)
    print("REESTRUCTURACIÓN DEL MAESTRO DE ENTIDADES")
    print("=" * 70)

    # Rutas
    dim_entidades_path = os.path.join(MODEL_DIR, 'dim_entidades.csv')
    dim_municipios_path = os.path.join(MODEL_DIR, 'dim_municipios.csv')
    fact_oficios_path = os.path.join(MODEL_DIR, 'fact_oficios.csv')

    for p in [dim_entidades_path, dim_municipios_path, fact_oficios_path]:
        if not os.path.exists(p):
            print(f"[ERROR] No se encontró: {p}")
            sys.exit(1)

    # 1. Cargar municipios
    print("\n[1/5] Cargando municipios...")
    municipios = load_municipios(dim_municipios_path)
    print(f"  → {len(municipios):,} municipios")

    # 2. Cargar correos extraídos
    print("\n[2/5] Extrayendo correos más frecuentes por entidad...")
    emails_extraidos = load_emails_from_oficios(fact_oficios_path)
    print(f"  → {len(emails_extraidos):,} entidades con correo extraído")

    # 3. Cargar directorio SIERJU (si disponible)
    print("\n[3/5] Cargando directorio SIERJU...")
    sierju = load_sierju(args.sierju)

    # 4. Leer entidades y dividir
    print("\n[4/5] Clasificando entidades...")
    judiciales = []
    coactivas = []
    matched_sierju = 0

    with open(dim_entidades_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            eid = row['entidad_id']
            nombre = row['nombre_normalizado']
            tipo = row['tipo'].strip()
            subtipo = row.get('subtipo', '').strip()
            muni_id = row.get('municipio_id', '').strip()
            total = row.get('total_registros', '0')

            # Resolver ciudad
            ciudad = municipios.get(muni_id, '') if muni_id else ''

            # Correo extraído de nuestra BD
            email_extraido = emails_extraidos.get(eid, '')

            if tipo in TIPOS_JUDICIALES:
                # ── Entidad Judicial ──
                nombre_real, email_real = match_sierju(nombre, ciudad, sierju)
                if nombre_real:
                    matched_sierju += 1

                numero_despacho = extraer_numero_despacho(nombre, subtipo)

                judiciales.append({
                    'entidad_id': eid,
                    'nombre_real': nombre_real or '',
                    'nombre_extraido': nombre,
                    'ciudad': ciudad,
                    'email_real': email_real or '',
                    'email_extraido': email_extraido,
                    'numero_despacho': numero_despacho,
                    'total_registros': total,
                })
            else:
                # ── Entidad Coactiva ──
                # NIT: se puede extraer si aparece en el nombre o en datos demandado
                nit = ''
                nit_match = re.search(r'NIT\s*[:\-]?\s*(\d[\d.\-]+)', nombre)
                if nit_match:
                    nit = nit_match.group(1).replace('.', '').replace('-', '')

                coactivas.append({
                    'entidad_id': eid,
                    'nombre_real': '',  # Requiere directorio externo de entidades coactivas
                    'nombre_extraido': nombre,
                    'ciudad': ciudad,
                    'email_real': '',   # Requiere directorio externo
                    'email_extraido': email_extraido,
                    'nit': nit,
                    'total_registros': total,
                })

    # 5. Escribir CSVs de salida
    print("\n[5/5] Generando archivos...")

    out_judiciales = os.path.join(MODEL_DIR, 'dim_entidades_judiciales.csv')
    out_coactivas = os.path.join(MODEL_DIR, 'dim_entidades_coactivas.csv')

    with open(out_judiciales, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'entidad_id', 'nombre_real', 'nombre_extraido', 'ciudad',
            'email_real', 'email_extraido', 'numero_despacho', 'total_registros',
        ])
        writer.writeheader()
        writer.writerows(judiciales)

    with open(out_coactivas, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'entidad_id', 'nombre_real', 'nombre_extraido', 'ciudad',
            'email_real', 'email_extraido', 'nit', 'total_registros',
        ])
        writer.writeheader()
        writer.writerows(coactivas)

    # ── Resumen ──
    print("\n" + "=" * 70)
    print("RESUMEN")
    print("=" * 70)
    print(f"  Total entidades:          {len(judiciales) + len(coactivas):>8,}")
    print(f"  ── Judiciales:            {len(judiciales):>8,}")
    print(f"  ── Coactivas:             {len(coactivas):>8,}")
    print(f"  Cruzadas con SIERJU:      {matched_sierju:>8,}")
    print(f"  Con correo extraído:      {len(emails_extraidos):>8,}")
    print()
    print(f"  Tipos judiciales:")
    tipo_counts_j = defaultdict(int)
    for j in judiciales:
        # Reconstruct tipo from the original data — use a re-read or track it
        pass
    print(f"    → {', '.join(sorted(TIPOS_JUDICIALES))}")
    print()
    print(f"  Archivos generados:")
    print(f"    → {out_judiciales}  ({len(judiciales):,} filas)")
    print(f"    → {out_coactivas}  ({len(coactivas):,} filas)")

    if not sierju:
        print()
        print("  ⚠  Para completar nombre_real y email_real de entidades judiciales:")
        print("     1. Ir a: https://directoriojudicial.ramajudicial.gov.co")
        print("        Sierju-Web → Consulta Externa Despachos → Buscar → Exportar Excel")
        print("     2. Guardar como: datos/fuentes/directorio_sierju.xlsx")
        print("     3. Re-ejecutar: python scripts/split_entidades.py")

    print("\nProceso completado.")


if __name__ == '__main__':
    main()
