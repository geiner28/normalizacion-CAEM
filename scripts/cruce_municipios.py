#!/usr/bin/env python3
"""
Cruce de municipio/departamento para embargos.
Fuente 1: entidades.csv (municipio de la entidad remitente)
Fuente 2: embargos.csv campo 'ciudad' (fallback cuando entidad no tiene municipio)

Genera: embargos_final.csv actualizado con columnas municipio, departamento
"""

import csv, json, unicodedata, re
from collections import Counter, defaultdict

# ═══════════════════════════════════════════
# Normalización de texto
# ═══════════════════════════════════════════
def norm(s):
    s = s.strip().upper()
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    s = re.sub(r'[.,;:\-/]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# ═══════════════════════════════════════════
# 1. Cargar referencia DANE
# ═══════════════════════════════════════════
print("[1/6] Cargando referencia DANE...")
with open('colombia_municipios.json', 'r') as f:
    dane = json.load(f)

city_db = {}   # norm -> (pretty, dept)
dept_set = {}  # norm -> pretty

for d in dane:
    dept = d['departamento']
    dept_set[norm(dept)] = dept
    for c in d['ciudades']:
        cn = norm(c)
        if cn not in city_db:
            city_db[cn] = (c, dept)

# ═══════════════════════════════════════════
# 2. Aliases manuales extensos
# ═══════════════════════════════════════════
ALIASES = {
    # Bogotá
    'BOGOTA': ('Bogotá D.C.', 'Cundinamarca'), 'BOGOTA DC': ('Bogotá D.C.', 'Cundinamarca'),
    'BOGOTA D.C.': ('Bogotá D.C.', 'Cundinamarca'), 'BOGOTA D.C': ('Bogotá D.C.', 'Cundinamarca'),
    'BOGOTA D C': ('Bogotá D.C.', 'Cundinamarca'), 'BOGOTA D. C.': ('Bogotá D.C.', 'Cundinamarca'),
    'BOGOTA, D.C.': ('Bogotá D.C.', 'Cundinamarca'), 'SANTAFE DE BOGOTA': ('Bogotá D.C.', 'Cundinamarca'),
    'BOGOTA D C BOGOTA': ('Bogotá D.C.', 'Cundinamarca'),
    # Cali
    'SANTIAGO DE CALI': ('Cali', 'Valle del Cauca'), 'CALI': ('Cali', 'Valle del Cauca'),
    'CALI VALLE': ('Cali', 'Valle del Cauca'), 'CALI VALLE DEL CAUCA': ('Cali', 'Valle del Cauca'),
    # Cartagena
    'CARTAGENA': ('Cartagena de Indias', 'Bolívar'), 'CARTAGENA DE INDIAS': ('Cartagena de Indias', 'Bolívar'),
    'CARTAGENA DE INDIAS D.T Y C': ('Cartagena de Indias', 'Bolívar'),
    'CARTAGENA DE INDIAS D.T. Y C.': ('Cartagena de Indias', 'Bolívar'),
    'CARTAGENA D.T. Y C.': ('Cartagena de Indias', 'Bolívar'),
    'CARTAGENA BOLIVAR': ('Cartagena de Indias', 'Bolívar'),
    'CARTAGENA DE INDIAS D.T': ('Cartagena de Indias', 'Bolívar'),
    'CARTAGENA DE INDIAS D T Y C': ('Cartagena de Indias', 'Bolívar'),
    'CARTAGENA DE INDIAS D T': ('Cartagena de Indias', 'Bolívar'),
    # Cúcuta
    'CUCUTA': ('Cúcuta', 'Norte de Santander'), 'SAN JOSE DE CUCUTA': ('Cúcuta', 'Norte de Santander'),
    'CUCUTA NORTE DE SANTANDER': ('Cúcuta', 'Norte de Santander'),
    # Santa Marta
    'SANTA MARTA': ('Santa Marta', 'Magdalena'), 'SANTA MARTA MAGDALENA': ('Santa Marta', 'Magdalena'),
    # Medellín
    'MEDELLIN': ('Medellín', 'Antioquia'), 'MEDELLIN ANTIOQUIA': ('Medellín', 'Antioquia'),
    # Ibagué
    'IBAGUE': ('Ibagué', 'Tolima'), 'IBAGUE TOLIMA': ('Ibagué', 'Tolima'),
    # Pasto
    'SAN JUAN DE PASTO': ('Pasto', 'Nariño'), 'PASTO NARINO': ('Pasto', 'Nariño'),
    # Girón
    'GIRON': ('Girón', 'Santander'), 'GIRON SANTANDER': ('Girón', 'Santander'),
    'GIRON, SANTANDER': ('Girón', 'Santander'), 'SAN JUAN DE GIRON': ('Girón', 'Santander'),
    # Buenaventura
    'BUENAVENTURA': ('Buenaventura', 'Valle del Cauca'),
    'BUENAVENTURA VALLE DEL CAUCA': ('Buenaventura', 'Valle del Cauca'),
    'BUENAVENTURA VALLE': ('Buenaventura', 'Valle del Cauca'),
    # Dosquebradas
    'DOSQUEBRADAS': ('Dosquebradas', 'Risaralda'), 'DOS QUEBRADAS': ('Dosquebradas', 'Risaralda'),
    'DOSQUEBRADAS RISARALDA': ('Dosquebradas', 'Risaralda'),
    # Buga
    'GUADALAJARA DE BUGA': ('Buga', 'Valle del Cauca'),
    'GUADALAJARA DE BUGA VALLE DEL CAUCA': ('Buga', 'Valle del Cauca'),
    'BUGA VALLE DEL CAUCA': ('Buga', 'Valle del Cauca'),
    'BUGA VALLE': ('Buga', 'Valle del Cauca'),
    # Espinal
    'ESPINAL': ('El Espinal', 'Tolima'), 'EL ESPINAL': ('El Espinal', 'Tolima'),
    'ESPINAL TOLIMA': ('El Espinal', 'Tolima'), 'EL ESPINAL TOLIMA': ('El Espinal', 'Tolima'),
    # Villa del Rosario
    'VILLA DEL ROSARIO': ('Villa del Rosario', 'Norte de Santander'),
    'VILLA DE ROSARIO': ('Villa del Rosario', 'Norte de Santander'),
    'VILLA DEL ROSARIO SANTANDER': ('Villa del Rosario', 'Norte de Santander'),
    'VILLA DEL ROSARIO NORTE DE SANTANDER': ('Villa del Rosario', 'Norte de Santander'),
    # La Ceja
    'LA CEJA': ('La Ceja', 'Antioquia'), 'LA CEJA DEL TAMBO': ('La Ceja', 'Antioquia'),
    'LA CEJA ANTIOQUIA': ('La Ceja', 'Antioquia'),
    # El Carmen de Bolívar
    'CARMEN DE BOLIVAR': ('El Carmen de Bolívar', 'Bolívar'),
    'EL CARMEN DE BOLIVAR': ('El Carmen de Bolívar', 'Bolívar'),
    'CARMEN DE BOLIVAR BOLIVAR': ('El Carmen de Bolívar', 'Bolívar'),
    # San Gil
    'SAN GIL': ('San Gil', 'Santander'), 'SAN GIL SANTANDER': ('San Gil', 'Santander'),
    # La Dorada
    'LA DORADA': ('La Dorada', 'Caldas'), 'LA DORADA CALDAS': ('La Dorada', 'Caldas'),
    # Clemencia
    'CLEMENCIA': ('Clemencia', 'Bolívar'), 'CLEMENCIA BOLIVAR': ('Clemencia', 'Bolívar'),
    # Santa Rosa de Cabal
    'SANTA ROSA DE CABAL': ('Santa Rosa de Cabal', 'Risaralda'),
    'SANTA ROSA DE CABAL RISARALDA': ('Santa Rosa de Cabal', 'Risaralda'),
    # Planeta Rica
    'PLANETA RICA': ('Planeta Rica', 'Córdoba'), 'PLANETA RICA CORDOBA': ('Planeta Rica', 'Córdoba'),
    # Paz de Ariporo
    'PAZ DE ARIPORO': ('Paz de Ariporo', 'Casanare'), 'PAZ DE ARIPORO CASANARE': ('Paz de Ariporo', 'Casanare'),
    # Santo Tomás
    'SANTO TOMAS': ('Santo Tomás', 'Atlántico'), 'SANTO TOMAS ATLANTICO': ('Santo Tomás', 'Atlántico'),
    # Puerto Colombia
    'PUERTO COLOMBIA': ('Puerto Colombia', 'Atlántico'), 'PUERTO COLOMBIA ATLANTICO': ('Puerto Colombia', 'Atlántico'),
}

# Merge aliases into city_db
for k, v in ALIASES.items():
    kn = norm(k)
    city_db[kn] = v

# Dept norm set for stripping
dept_norms = set(dept_set.keys())

def resolve_city(raw):
    """Resolve raw city name to (municipio, departamento) or None."""
    cn = norm(raw)
    if not cn:
        return None

    # 1. Direct lookup (includes aliases)
    if cn in city_db:
        return city_db[cn]

    # 2. Strip D.C./D.T. suffixes
    cleaned = re.sub(r'\bD\s*\.?\s*C\s*\.?\s*$', '', cn).strip()
    cleaned = re.sub(r'\bD\s*\.?\s*T\s*\.?\s*(Y\s*C\s*\.?)?\s*$', '', cleaned).strip()
    if cleaned and cleaned in city_db:
        return city_db[cleaned]

    # 3. Try splitting off department suffix (various separators)
    for sep_pat in [r'\s*-\s*', r'\s*,\s*', r'\s+']:
        parts = re.split(sep_pat, cn)
        if len(parts) >= 2:
            # Try first N-1 words as city, last as dept
            for split_point in range(len(parts)-1, 0, -1):
                candidate_city = ' '.join(parts[:split_point]).strip()
                candidate_dept = ' '.join(parts[split_point:]).strip()
                is_dept = candidate_dept in dept_norms or any(candidate_dept in dn for dn in dept_norms)
                if is_dept and candidate_city in city_db:
                    return city_db[candidate_city]

    # 4. Try first word if 4+ chars
    words = cn.split()
    if len(words) >= 2:
        first = words[0]
        if len(first) >= 4 and first in city_db:
            return city_db[first]
        # Try first two words
        first2 = ' '.join(words[:2])
        if first2 in city_db:
            return city_db[first2]
        # Try first three words
        if len(words) >= 3:
            first3 = ' '.join(words[:3])
            if first3 in city_db:
                return city_db[first3]

    return None

# ═══════════════════════════════════════════
# 3. Pre-compute city resolution cache
# ═══════════════════════════════════════════
print("[2/6] Pre-resolviendo ciudades únicas...")

# Get all unique ciudades
unique_cities = set()
with open('embargos.csv', 'r', encoding='utf-8-sig') as f:
    for r in csv.DictReader(f):
        c = r.get('ciudad', '').strip()
        if c:
            unique_cities.add(c)

city_cache = {}
for raw in unique_cities:
    city_cache[raw] = resolve_city(raw)

resolved = sum(1 for v in city_cache.values() if v)
print(f"  {len(unique_cities):,} ciudades únicas → {resolved:,} resueltas ({resolved/len(unique_cities)*100:.1f}%)")

# ═══════════════════════════════════════════
# 4. Cargar entidades y embargo->ciudad
# ═══════════════════════════════════════════
print("[3/6] Cargando entidades y ciudades...")

entidades = {}
with open('entidades.csv', 'r') as f:
    for r in csv.DictReader(f):
        entidades[r['entidad_id']] = {
            'municipio': r['municipio'].strip(),
            'departamento': r['departamento'].strip(),
        }

embargo_ciudad = {}
with open('embargos.csv', 'r', encoding='utf-8-sig') as f:
    for r in csv.DictReader(f):
        embargo_ciudad[r['id']] = r.get('ciudad', '').strip()

# ═══════════════════════════════════════════
# 5. Generar embargos_final.csv con municipio/depto
# ═══════════════════════════════════════════
print("[4/6] Generando embargos_final.csv con municipio/departamento...")

INPUT = 'embargos_final.csv'
OUTPUT = 'embargos_final_v2.csv'

stats = Counter()
source_stats = Counter()

with open(INPUT, 'r', encoding='utf-8') as fin, \
     open(OUTPUT, 'w', encoding='utf-8', newline='') as fout:

    reader = csv.DictReader(fin)
    out_fields = reader.fieldnames + ['municipio', 'departamento', 'fuente_ubicacion']
    writer = csv.DictWriter(fout, fieldnames=out_fields)
    writer.writeheader()

    for row in reader:
        eid = row['entidad_remitente_id']
        ent = entidades.get(eid, {})
        ent_mun = ent.get('municipio', '')
        ent_dep = ent.get('departamento', '')

        municipio = ''
        departamento = ''
        fuente = ''

        # Fuente 1: Entidad tiene municipio
        if ent_mun:
            municipio = ent_mun
            departamento = ent_dep
            fuente = 'entidad'
        else:
            # Fuente 2: Campo ciudad del CSV original
            ciudad_raw = embargo_ciudad.get(row['embargo_id'], '')
            if ciudad_raw:
                result = city_cache.get(ciudad_raw)
                if result:
                    municipio, departamento = result
                    fuente = 'ciudad_original'
                else:
                    # No se pudo resolver, guardar raw
                    municipio = ciudad_raw
                    departamento = ent_dep if ent_dep else ''
                    fuente = 'ciudad_sin_resolver'
            else:
                fuente = 'sin_dato'

        row['municipio'] = municipio
        row['departamento'] = departamento
        row['fuente_ubicacion'] = fuente
        source_stats[fuente] += 1
        writer.writerow(row)

total = sum(source_stats.values())
print(f"\n  Total oficios escritos: {total:,}")
print(f"  Fuente ubicación:")
for src, cnt in source_stats.most_common():
    print(f"    {src:<25} {cnt:>10,} ({cnt/total*100:.1f}%)")

# ═══════════════════════════════════════════
# 6. Verificación final
# ═══════════════════════════════════════════
print("\n[5/6] Verificando resultado...")

con_mun = 0
sin_mun = 0
con_dep = 0
sin_dep = 0
total_v = 0

with open(OUTPUT, 'r') as f:
    for r in csv.DictReader(f):
        total_v += 1
        if r['municipio'].strip():
            con_mun += 1
        else:
            sin_mun += 1
        if r['departamento'].strip():
            con_dep += 1
        else:
            sin_dep += 1

print(f"  Total: {total_v:,}")
print(f"  Con municipio:     {con_mun:,} ({con_mun/total_v*100:.1f}%)")
print(f"  Sin municipio:     {sin_mun:,} ({sin_mun/total_v*100:.1f}%)")
print(f"  Con departamento:  {con_dep:,} ({con_dep/total_v*100:.1f}%)")
print(f"  Sin departamento:  {sin_dep:,} ({sin_dep/total_v*100:.1f}%)")

print("\n[6/6] Listo. Archivo: embargos_final_v2.csv")
