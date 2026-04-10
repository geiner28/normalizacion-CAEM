#!/usr/bin/env python3
"""
Informe de Normalización y Diagnóstico de Entidades
====================================================
1. Entidades sin municipio y/o departamento
2. Entidades huérfanas que podrían ser variantes de una entidad padre existente
"""

import csv
import re
import unicodedata
from collections import defaultdict
from difflib import SequenceMatcher

# ============================================================
# UTILIDADES DE TEXTO
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


def normalize_text(text):
    if not text:
        return ''
    t = text.strip().upper()
    t = remove_accents(t)
    t = re.sub(r'[^A-Z0-9ÑÁÉÍÓÚ\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


# ============================================================
# ORDINALS - para distinguir juzgados
# ============================================================

ORDINAL_MAP = {
    'PRIMERO': 1, 'PRIMERA': 1, 'PRIMER': 1, '1': 1, '01': 1, '001': 1,
    'SEGUNDO': 2, 'SEGUNDA': 2, '2': 2, '02': 2, '002': 2,
    'TERCERO': 3, 'TERCERA': 3, 'TERCER': 3, '3': 3, '03': 3, '003': 3, 'TRES': 3,
    'CUARTO': 4, 'CUARTA': 4, '4': 4, '04': 4, '004': 4,
    'QUINTO': 5, 'QUINTA': 5, '5': 5, '05': 5, '005': 5,
    'SEXTO': 6, 'SEXTA': 6, '6': 6, '06': 6, '006': 6,
    'SEPTIMO': 7, 'SEPTIMA': 7, 'SETIMO': 7, '7': 7, '07': 7, '007': 7,
    'OCTAVO': 8, 'OCTAVA': 8, '8': 8, '08': 8, '008': 8,
    'NOVENO': 9, 'NOVENA': 9, '9': 9, '09': 9, '009': 9,
    'DECIMO': 10, 'DECIMA': 10, '10': 10, '010': 10,
    'ONCE': 11, '11': 11, 'UNDECIMO': 11,
    'DOCE': 12, '12': 12,
    'TRECE': 13, '13': 13,
    'CATORCE': 14, '14': 14,
    'QUINCE': 15, '15': 15,
    'DIECISEIS': 16, 'DIECISEIS': 16, '16': 16,
    'DIECISIETE': 17, '17': 17,
    'DIECIOCHO': 18, '18': 18,
    'DIECINUEVE': 19, '19': 19,
    'VEINTE': 20, '20': 20,
    'VEINTIUNO': 21, 'VEINTIUN': 21, '21': 21,
    'VEINTIDOS': 22, '22': 22,
    'VEINTITRES': 23, '23': 23,
    'VEINTICUATRO': 24, '24': 24,
    'VEINTICINCO': 25, '25': 25,
    'VEINTISEIS': 26, '26': 26,
    'VEINTISIETE': 27, '27': 27,
    'VEINTIOCHO': 28, '28': 28,
    'VEINTINUEVE': 29, '29': 29,
    'TREINTA': 30, '30': 30,
    'CUARENTA': 40, '40': 40,
    'CINCUENTA': 50, '50': 50,
    'SESENTA': 60, '60': 60,
    'SETENTA': 70, '70': 70,
    'OCHENTA': 80, '80': 80,
    'NOVENTA': 90, '90': 90,
}

# Two-word compound ordinals
COMPOUND_ORDINALS = {
    'TREINTA Y UNO': 31, 'TREINTA Y UN': 31, 'TREINTA Y DOS': 32,
    'TREINTA Y TRES': 33, 'TREINTA Y CUATRO': 34, 'TREINTA Y CINCO': 35,
    'TREINTA Y SEIS': 36, 'TREINTA Y SIETE': 37, 'TREINTA Y OCHO': 38,
    'TREINTA Y NUEVE': 39,
    'CUARENTA Y UNO': 41, 'CUARENTA Y UN': 41, 'CUARENTA Y DOS': 42,
    'CUARENTA Y TRES': 43, 'CUARENTA Y CUATRO': 44, 'CUARENTA Y CINCO': 45,
    'CUARENTA Y SEIS': 46, 'CUARENTA Y SIETE': 47, 'CUARENTA Y OCHO': 48,
    'CUARENTA Y NUEVE': 49,
    'CINCUENTA Y UNO': 51, 'CINCUENTA Y UN': 51, 'CINCUENTA Y DOS': 52,
    'CINCUENTA Y TRES': 53, 'CINCUENTA Y CUATRO': 54, 'CINCUENTA Y CINCO': 55,
    'CINCUENTA Y SEIS': 56, 'CINCUENTA Y SIETE': 57, 'CINCUENTA Y OCHO': 58,
    'CINCUENTA Y NUEVE': 59,
    'SESENTA Y UNO': 61, 'SESENTA Y UN': 61, 'SESENTA Y DOS': 62,
    'SESENTA Y TRES': 63, 'SESENTA Y CUATRO': 64, 'SESENTA Y CINCO': 65,
    'SESENTA Y SEIS': 66, 'SESENTA Y SIETE': 67, 'SESENTA Y OCHO': 68,
    'SESENTA Y NUEVE': 69,
    'SETENTA Y UNO': 71, 'SETENTA Y DOS': 72, 'SETENTA Y TRES': 73,
    'SETENTA Y CUATRO': 74, 'SETENTA Y CINCO': 75, 'SETENTA Y SEIS': 76,
    'SETENTA Y SIETE': 77, 'SETENTA Y OCHO': 78, 'SETENTA Y NUEVE': 79,
    'OCHENTA Y UNO': 81, 'OCHENTA Y DOS': 82, 'OCHENTA Y TRES': 83,
    'DECIMO CUARTO': 14, 'DECIMO QUINTO': 15, 'DECIMO SEXTO': 16,
    'DECIMO SEPTIMO': 17, 'DECIMO OCTAVO': 18, 'DECIMO NOVENO': 19,
    'VIGESIMO PRIMERO': 21, 'VIGESIMO SEGUNDO': 22, 'VIGESIMO TERCERO': 23,
    'VIGESIMO CUARTO': 24, 'VIGESIMO QUINTO': 25, 'VIGESIMO SEXTO': 26,
    'VIGESIMO SEPTIMO': 27, 'VIGESIMO OCTAVO': 28, 'VIGESIMO NOVENO': 29,
}


def extract_juzgado_ordinal(name):
    """Extract the ordinal number from a juzgado name. Returns int or None."""
    text = normalize_text(name)
    # Remove "JUZGADO" prefix
    text = re.sub(r'^JUZGADO\s+', '', text)

    # Try compound ordinals first (multi-word) - sorted by length desc to match longest first
    for pattern, num in sorted(COMPOUND_ORDINALS.items(), key=lambda x: -len(x[0])):
        pat_norm = remove_accents(pattern.upper())
        if pat_norm in text:
            return num

    # Try single ordinals (first few words after JUZGADO)
    words = text.split()
    for w in words[:3]:
        w_clean = remove_accents(w.upper())
        if w_clean in ORDINAL_MAP:
            return ORDINAL_MAP[w_clean]

    # Try extracting pure numbers
    nums = re.findall(r'\b(\d{1,3})\b', text)
    if nums:
        return int(nums[0])

    return None


def extract_juzgado_subtipo_key(name):
    """Extract the subtipo words (CIVIL MUNICIPAL, PROMISCUO, etc.) ignoring ordinal and city."""
    text = normalize_text(name)
    # Remove JUZGADO
    text = re.sub(r'^JUZGADO\s+', '', text)
    # Remove ordinal words
    ordinal_words = set()
    for k in list(ORDINAL_MAP.keys()) + list(COMPOUND_ORDINALS.keys()):
        for w in k.split():
            ordinal_words.add(w)
    # Remove city names and common suffixes
    text = re.sub(r'\bDE\s+(BOGOTA|CALI|BARRANQUILLA|CARTAGENA|MEDELLIN|BUCARAMANGA|CUCUTA|IBAGUE|NEIVA|SANTA\s+MARTA|MONTERIA|VILLAVICENCIO|MANIZALES|PEREIRA|VALLEDUPAR|SOLEDAD|TURBACO|PALMIRA)\b.*', '', text)
    text = re.sub(r'\b(D\s*C|BOGOTA|CUNDINAMARCA|BOLIVAR|ATLANTICO|TOLIMA|SANTANDER|HUILA|META|VALLE)\b', '', text)

    words = text.split()
    key_words = [w for w in words if w not in ordinal_words and len(w) > 1 and w not in ('DE', 'DEL', 'LA', 'LAS', 'LOS', 'EL', 'EN', 'Y', 'A')]
    return ' '.join(key_words)


# ============================================================
# CARGAR DATOS
# ============================================================

def load_entidades():
    entidades = []
    with open('entidades.csv', 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            row['total_registros'] = int(row['total_registros'])
            row['num_variantes'] = int(row['num_variantes'])
            row['entidad_id'] = int(row['entidad_id'])
            entidades.append(row)
    return entidades


def load_variantes():
    variantes = []
    with open('variantes_entidades.csv', 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            row['entidad_id'] = int(row['entidad_id'])
            row['conteo'] = int(row['conteo'])
            variantes.append(row)
    return variantes


# ============================================================
# PARTE 1: INFORME GEOGRÁFICO
# ============================================================

def informe_geografico(entidades):
    sin_muni = [e for e in entidades if not e['municipio'].strip()]
    sin_dept = [e for e in entidades if not e['departamento'].strip()]
    sin_ambos = [e for e in entidades if not e['municipio'].strip() and not e['departamento'].strip()]
    con_dept_sin_muni = [e for e in entidades if e['departamento'].strip() and not e['municipio'].strip()]
    con_muni_sin_dept = [e for e in entidades if e['municipio'].strip() and not e['departamento'].strip()]

    total_recs = sum(e['total_registros'] for e in entidades)
    recs_sin_ambos = sum(e['total_registros'] for e in sin_ambos)
    recs_sin_muni = sum(e['total_registros'] for e in sin_muni)

    lines = []
    lines.append("=" * 80)
    lines.append("INFORME DE ENTIDADES SIN MUNICIPIO / DEPARTAMENTO")
    lines.append("=" * 80)
    lines.append(f"Total entidades:                      {len(entidades):>6,}")
    lines.append(f"Total registros:                      {total_recs:>10,}")
    lines.append(f"")
    lines.append(f"Sin municipio:                        {len(sin_muni):>6,}  ({len(sin_muni)/len(entidades)*100:.1f}%)")
    lines.append(f"Sin departamento:                     {len(sin_dept):>6,}  ({len(sin_dept)/len(entidades)*100:.1f}%)")
    lines.append(f"Sin ambos (muni + depto):             {len(sin_ambos):>6,}  ({len(sin_ambos)/len(entidades)*100:.1f}%)")
    lines.append(f"Con depto pero sin municipio:         {len(con_dept_sin_muni):>6,}")
    lines.append(f"Con municipio pero sin depto:         {len(con_muni_sin_dept):>6,}")
    lines.append(f"")
    lines.append(f"Registros sin ambos:                  {recs_sin_ambos:>10,}  ({recs_sin_ambos/total_recs*100:.1f}%)")
    lines.append(f"Registros sin municipio:              {recs_sin_muni:>10,}  ({recs_sin_muni/total_recs*100:.1f}%)")
    lines.append("")

    # Desglose por tipo de las que no tienen ni municipio ni departamento
    tipo_counts = defaultdict(lambda: {'count': 0, 'recs': 0})
    for e in sin_ambos:
        tipo_counts[e['tipo']]['count'] += 1
        tipo_counts[e['tipo']]['recs'] += e['total_registros']

    lines.append("-" * 80)
    lines.append("DESGLOSE POR TIPO (entidades sin municipio NI departamento)")
    lines.append("-" * 80)
    lines.append(f"  {'Tipo':<30} {'Cantidad':>10} {'Registros':>12}")
    lines.append(f"  {'-'*30} {'-'*10} {'-'*12}")
    for tipo, data in sorted(tipo_counts.items(), key=lambda x: -x[1]['recs']):
        lines.append(f"  {tipo:<30} {data['count']:>10,} {data['recs']:>12,}")
    lines.append("")

    # Top 50 entidades sin ambos por registros
    lines.append("-" * 80)
    lines.append("TOP 50 ENTIDADES SIN MUNICIPIO NI DEPARTAMENTO (por registros)")
    lines.append("-" * 80)
    for e in sorted(sin_ambos, key=lambda x: -x['total_registros'])[:50]:
        lines.append(f"  ID {e['entidad_id']:>5}  [{e['tipo']:>25}]  {e['total_registros']:>8,} recs  |  {e['nombre_normalizado']}")
    lines.append("")

    # Entidades con depto pero sin municipio (top 30)
    lines.append("-" * 80)
    lines.append("ENTIDADES CON DEPARTAMENTO PERO SIN MUNICIPIO (top 30)")
    lines.append("-" * 80)
    for e in sorted(con_dept_sin_muni, key=lambda x: -x['total_registros'])[:30]:
        lines.append(f"  ID {e['entidad_id']:>5}  [{e['tipo']:>25}]  depto={e['departamento']:>22}  {e['total_registros']:>8,} recs  |  {e['nombre_normalizado'][:70]}")
    lines.append("")

    return lines


# ============================================================
# PARTE 2: DETECCIÓN DE ENTIDADES HUÉRFANAS NORMALIZABLES
# ============================================================

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


def extract_key_words(text):
    """Extract meaningful words, removing common filler words."""
    stop = {'DE', 'DEL', 'LA', 'LAS', 'LOS', 'EL', 'EN', 'Y', 'A', 'CON', 'POR', 'PARA', 'D', 'C', 'E', 'S', 'P'}
    words = normalize_text(text).split()
    return [w for w in words if w not in stop and len(w) > 1]


def build_ngram_index(entities, n=3):
    """Build character n-gram index for fast candidate lookup."""
    index = defaultdict(set)
    for i, e in enumerate(entities):
        text = normalize_text(e['nombre_normalizado'])
        for k in range(len(text) - n + 1):
            index[text[k:k+n]].add(i)
    return index


def find_orphan_matches(entidades):
    """
    Para cada entidad, buscar si su nombre_normalizado podría ser variante
    de otra entidad padre con más registros.
    Optimizado: solo compara dentro del mismo tipo, usa n-gram index,
    y para JUZGADO agrupa por subtipo+ubicación para reducir comparaciones.
    """
    results = []

    by_tipo = defaultdict(list)
    for e in entidades:
        by_tipo[e['tipo']].append(e)

    for tipo in by_tipo:
        by_tipo[tipo].sort(key=lambda x: -x['total_registros'])

    processed = set()
    total_tipos = len([t for t in by_tipo if t not in ('OTRO',)])

    for tipo_idx, (tipo, entities) in enumerate(by_tipo.items()):
        if tipo in ('OTRO',):
            continue

        print(f"  Procesando tipo {tipo} ({len(entities)} entidades)... [{tipo_idx+1}/{total_tipos}]")

        # For JUZGADO: group by subtipo to reduce search space
        if tipo == 'JUZGADO' and len(entities) > 200:
            subgroups = defaultdict(list)
            for e in entities:
                subgroups[e.get('subtipo', '')].append(e)
            groups = list(subgroups.values())
        else:
            groups = [entities]

        for group in groups:
            if len(group) < 2:
                continue

            # Only compare top 500 biggest as potential parents
            # and check the rest as potential children
            parent_pool = group[:min(500, len(group))]
            
            for i, child in enumerate(group):
                child_id = child['entidad_id']
                if child_id in processed:
                    continue

                child_norm = normalize_text(child['nombre_normalizado'])
                child_words = set(extract_key_words(child['nombre_normalizado']))

                if len(child_words) < 2:
                    continue

                best_match = None
                best_score = 0

                for parent in parent_pool:
                    if parent['entidad_id'] == child_id:
                        continue
                    if parent['total_registros'] <= child['total_registros']:
                        continue

                    parent_words = set(extract_key_words(parent['nombre_normalizado']))

                    overlap = len(child_words & parent_words) / max(len(child_words), len(parent_words)) if parent_words else 0
                    if overlap < 0.5:
                        continue

                    child_muni = child['municipio'].strip()
                    parent_muni = parent['municipio'].strip()
                    child_dept = child['departamento'].strip()
                    parent_dept = parent['departamento'].strip()

                    if child_muni and parent_muni and normalize_text(child_muni) != normalize_text(parent_muni):
                        continue
                    if child_dept and parent_dept and normalize_text(child_dept) != normalize_text(parent_dept):
                        continue

                    parent_norm = normalize_text(parent['nombre_normalizado'])

                    if tipo == 'JUZGADO':
                        child_nums = set(re.findall(r'\d+', child_norm))
                        parent_nums = set(re.findall(r'\d+', parent_norm))
                        if child_nums and parent_nums and child_nums != parent_nums:
                            continue
                        # Compare ordinal words (PRIMERO vs SEGUNDO etc.)
                        child_ord = extract_juzgado_ordinal(child['nombre_normalizado'])
                        parent_ord = extract_juzgado_ordinal(parent['nombre_normalizado'])
                        if child_ord is not None and parent_ord is not None and child_ord != parent_ord:
                            continue
                        # Also compare subtipo key (CIVIL MUNICIPAL vs PROMISCUO etc.)
                        child_subkey = extract_juzgado_subtipo_key(child['nombre_normalizado'])
                        parent_subkey = extract_juzgado_subtipo_key(parent['nombre_normalizado'])
                        if child_subkey and parent_subkey:
                            subkey_sim = similarity(child_subkey, parent_subkey)
                            if subkey_sim < 0.7:
                                continue
                        threshold = 0.88
                    elif tipo in ('ALCALDIA', 'MUNICIPIO', 'GOBERNACION'):
                        threshold = 0.80
                    else:
                        threshold = 0.82

                    # Quick length check before expensive similarity
                    len_ratio = min(len(child_norm), len(parent_norm)) / max(len(child_norm), len(parent_norm))
                    if len_ratio < threshold * 0.85:
                        continue

                    score = similarity(child_norm, parent_norm)

                    if score > best_score and score >= threshold:
                        best_score = score
                        best_match = parent

                if best_match:
                    results.append({
                        'child_id': child['entidad_id'],
                        'child_name': child['nombre_normalizado'],
                        'child_tipo': child['tipo'],
                        'child_recs': child['total_registros'],
                        'child_muni': child['municipio'],
                        'child_dept': child['departamento'],
                        'parent_id': best_match['entidad_id'],
                        'parent_name': best_match['nombre_normalizado'],
                        'parent_recs': best_match['total_registros'],
                        'parent_muni': best_match['municipio'],
                        'parent_dept': best_match['departamento'],
                        'score': best_score,
                    })
                    processed.add(child_id)

    results.sort(key=lambda x: -x['child_recs'])
    return results


def informe_normalizacion(entidades):
    lines = []
    lines.append("=" * 80)
    lines.append("INFORME DE NORMALIZACIÓN: ENTIDADES POSIBLEMENTE DUPLICADAS")
    lines.append("=" * 80)
    lines.append("Entidades que podrían ser variantes de otra entidad padre existente.")
    lines.append("Se comparan solo entidades del mismo tipo. Las variantes existentes NO se tocan.")
    lines.append("")

    matches = find_orphan_matches(entidades)

    if not matches:
        lines.append("No se encontraron entidades huérfanas que coincidan con un padre.")
        return lines

    total_recs_recuperables = sum(m['child_recs'] for m in matches)
    lines.append(f"Posibles duplicados encontrados: {len(matches)}")
    lines.append(f"Registros recuperables:          {total_recs_recuperables:,}")
    lines.append("")

    # Confidence categories
    high = [m for m in matches if m['score'] >= 0.92]
    medium = [m for m in matches if 0.85 <= m['score'] < 0.92]
    low = [m for m in matches if m['score'] < 0.85]

    lines.append(f"  Alta confianza (>=0.92):      {len(high):>5}  ({sum(m['child_recs'] for m in high):>8,} recs)")
    lines.append(f"  Media confianza (0.85-0.92):  {len(medium):>5}  ({sum(m['child_recs'] for m in medium):>8,} recs)")
    lines.append(f"  Baja confianza (<0.85):       {len(low):>5}  ({sum(m['child_recs'] for m in low):>8,} recs)")
    lines.append("")

    # Detail by confidence level
    for label, group in [("ALTA CONFIANZA", high), ("MEDIA CONFIANZA", medium), ("BAJA CONFIANZA", low)]:
        if not group:
            continue
        lines.append("-" * 80)
        lines.append(f"  {label}")
        lines.append("-" * 80)
        for m in group:
            lines.append(f"  Similitud: {m['score']:.3f}  |  Tipo: {m['child_tipo']}")
            lines.append(f"    HIJO  ID {m['child_id']:>5}  ({m['child_recs']:>6,} recs)  {m['child_name']}")
            geo_child = f"muni={m['child_muni'] or '---'}, dept={m['child_dept'] or '---'}"
            lines.append(f"          {geo_child}")
            lines.append(f"    PADRE ID {m['parent_id']:>5}  ({m['parent_recs']:>6,} recs)  {m['parent_name']}")
            geo_parent = f"muni={m['parent_muni'] or '---'}, dept={m['parent_dept'] or '---'}"
            lines.append(f"          {geo_parent}")
            lines.append("")

    return lines, matches


# ============================================================
# GENERAR CSV DE NORMALIZACIONES SUGERIDAS
# ============================================================

def export_suggestions(matches):
    with open('sugerencias_normalizacion.csv', 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'similitud', 'child_id', 'child_nombre', 'child_tipo',
            'child_registros', 'child_municipio', 'child_departamento',
            'parent_id', 'parent_nombre', 'parent_registros',
            'parent_municipio', 'parent_departamento'
        ])
        writer.writeheader()
        for m in matches:
            writer.writerow({
                'similitud': f"{m['score']:.3f}",
                'child_id': m['child_id'],
                'child_nombre': m['child_name'],
                'child_tipo': m['child_tipo'],
                'child_registros': m['child_recs'],
                'child_municipio': m['child_muni'],
                'child_departamento': m['child_dept'],
                'parent_id': m['parent_id'],
                'parent_nombre': m['parent_name'],
                'parent_registros': m['parent_recs'],
                'parent_municipio': m['parent_muni'],
                'parent_departamento': m['parent_dept'],
            })


# ============================================================
# MAIN
# ============================================================

def main():
    print("Cargando datos...")
    entidades = load_entidades()

    output = []

    # Parte 1: Informe geográfico
    output.extend(informe_geografico(entidades))

    # Parte 2: Normalización
    print("Buscando entidades posiblemente duplicadas (esto puede tardar)...")
    norm_lines, matches = informe_normalizacion(entidades)
    output.extend(norm_lines)

    # Guardar informe
    report = '\n'.join(output)
    with open('INFORME_NORMALIZACION.txt', 'w', encoding='utf-8') as f:
        f.write(report)

    print(report)

    # Exportar sugerencias a CSV
    if matches:
        export_suggestions(matches)
        print(f"\n>>> Sugerencias exportadas a sugerencias_normalizacion.csv ({len(matches)} filas)")

    print(f"\n>>> Informe guardado en INFORME_NORMALIZACION.txt")


if __name__ == '__main__':
    main()
