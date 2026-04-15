#!/usr/bin/env python3
"""
Dashboard v4 — Entidades + Embargos limpios.
- Datos de embargos_final.csv (916,425 sin SIN_CONFIRMAR)
- DIAN desglosada por municipio usando campo 'ciudad' del CSV original
- Relacional: embargos.entidad_remitente_id → entidades.entidad_id
"""

import csv, json, unicodedata, re
from collections import Counter, defaultdict

print("Cargando datos...")

# ──────────────────────────────────────────
# 1. Entidades
# ──────────────────────────────────────────
entidades = []
entidades_by_id = {}
with open('entidades.csv', 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        e = {
            'id': int(row['entidad_id']),
            'nombre': row['nombre_normalizado'],
            'tipo': row['tipo'],
            'subtipo': row['subtipo'],
            'municipio': row['municipio'],
            'departamento': row['departamento'],
            'total': int(row['total_registros']),
            'variantes': int(row['num_variantes']),
        }
        entidades.append(e)
        entidades_by_id[e['id']] = e

# ──────────────────────────────────────────
# 2. Variantes
# ──────────────────────────────────────────
variantes = {}
with open('variantes_entidades.csv', 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        eid = int(row['entidad_id'])
        if eid not in variantes:
            variantes[eid] = []
        variantes[eid].append({'v': row['variante_original'], 'c': int(row['conteo'])})

# ──────────────────────────────────────────
# 3. Embargos finales — stats
# ──────────────────────────────────────────
embargo_by_estado = Counter()
embargo_by_tipo = Counter()
embargo_by_remitente = Counter()
embargo_by_bancaria = Counter()
monto_total = 0.0
embargos_con_monto = 0
total_embargos = 0

with open('embargos_final.csv', 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        total_embargos += 1
        embargo_by_estado[row['estado_embargo']] += 1
        embargo_by_tipo[row['titulo_embargo']] += 1
        embargo_by_remitente[int(row['entidad_remitente_id'])] += 1
        embargo_by_bancaria[int(row['entidad_bancaria_id'])] += 1
        m = row['monto_a_embargar'].strip()
        if m:
            try:
                monto_total += float(m)
                embargos_con_monto += 1
            except ValueError:
                pass

# Nombre de bancarias
bancaria_names = {}
for bid, cnt in embargo_by_bancaria.items():
    e = entidades_by_id.get(bid)
    bancaria_names[bid] = e['nombre'] if e else f'ID {bid}'

# Top entidades por embargos
top_remitentes = embargo_by_remitente.most_common(20)

# ──────────────────────────────────────────
# 4. DIAN por municipio (usando campo ciudad del CSV original)
# ──────────────────────────────────────────
print("Procesando DIAN por municipio...")

dian_ids = set()
for e in entidades:
    if e['tipo'] == 'DIAN':
        dian_ids.add(e['id'])

# Cargar embargo_id -> ciudad del CSV original
embargo_ciudad = {}
with open('embargos.csv', 'r', encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        c = row.get('ciudad', '').strip()
        if c:
            embargo_ciudad[row['id']] = c

# Normalizar nombres de ciudad
def normalize_city(name):
    """Normaliza nombre de ciudad: UPPER, sin tildes."""
    name = name.strip().upper()
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = re.sub(r'\s+', ' ', name).strip()
    return name

# Mapear ciudades normalizadas a nombre bonito
city_canonical = {}
city_counts = Counter()
with open('embargos_limpios.csv', 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        if row['entidad_remitente_id'] not in {str(d) for d in dian_ids}:
            continue
        if row['estado_embargo'] == 'SIN_CONFIRMAR':
            continue
        ciudad = embargo_ciudad.get(row['embargo_id'], '')
        if not ciudad:
            ciudad = 'SIN CIUDAD'
        norm = normalize_city(ciudad)
        city_counts[norm] += 1
        if norm not in city_canonical or len(ciudad) > len(city_canonical[norm]):
            city_canonical[norm] = ciudad

# Agrupar ciudades similares
CITY_ALIASES = {
    'BOGOTA': 'BOGOTA', 'BOGOTA D.C.': 'BOGOTA', 'BOGOTA D.C': 'BOGOTA',
    'SANTIAGO DE CALI': 'CALI', 'CUCUTA': 'CUCUTA',
    'SAN JOSE DE CUCUTA': 'CUCUTA', 'SANTA MARTA': 'SANTA MARTA',
}

merged = Counter()
for norm, cnt in city_counts.items():
    key = CITY_ALIASES.get(norm, norm)
    merged[key] += cnt

# Build DIAN por municipio list
dian_by_city = []
for city, cnt in merged.most_common():
    dian_by_city.append({'ciudad': city_canonical.get(city, city.title()), 'normalizado': city, 'embargos': cnt})

print(f"  DIAN ciudades únicas: {len(dian_by_city)}")

# ──────────────────────────────────────────
# 5. Compute dashboard stats
# ──────────────────────────────────────────
total_entities = len(entidades)
total_variants = sum(e['variantes'] for e in entidades)
total_records = sum(e['total'] for e in entidades)

# Type distribution
type_counts = Counter()
type_records = Counter()
for e in entidades:
    type_counts[e['tipo']] += 1
    type_records[e['tipo']] += e['total']

# Department distribution
dept_records = Counter()
dept_entities = Counter()
for e in entidades:
    d = e['departamento'] or 'Sin departamento'
    dept_records[d] += e['total']
    dept_entities[d] += 1

munis = set(e['municipio'] for e in entidades if e['municipio'])
all_types = sorted(set(e['tipo'] for e in entidades))
all_depts = sorted(set(e['departamento'] for e in entidades if e['departamento']))

# Charts data
type_chart = sorted(type_records.items(), key=lambda x: -x[1])
type_labels = json.dumps([t[0] for t in type_chart], ensure_ascii=False)
type_values = json.dumps([t[1] for t in type_chart])
type_entity_counts = json.dumps([type_counts[t[0]] for t in type_chart])

dept_chart = sorted(dept_records.items(), key=lambda x: -x[1])[:15]
dept_labels = json.dumps([d[0] for d in dept_chart], ensure_ascii=False)
dept_values = json.dumps([d[1] for d in dept_chart])

estado_chart = sorted(embargo_by_estado.items(), key=lambda x: -x[1])
estado_labels = json.dumps([e[0] for e in estado_chart], ensure_ascii=False)
estado_values = json.dumps([e[1] for e in estado_chart])

bancaria_chart = sorted(embargo_by_bancaria.items(), key=lambda x: -x[1])
bancaria_labels_list = [bancaria_names[b[0]] for b in bancaria_chart]
bancaria_labels = json.dumps(bancaria_labels_list, ensure_ascii=False)
bancaria_values = json.dumps([b[1] for b in bancaria_chart])

# DIAN top 30 cities chart
dian_chart = dian_by_city[:30]
dian_labels = json.dumps([d['ciudad'] for d in dian_chart], ensure_ascii=False)
dian_values = json.dumps([d['embargos'] for d in dian_chart])

entidades_json = json.dumps(entidades, ensure_ascii=False)
variantes_json = json.dumps(variantes, ensure_ascii=False)
types_json = json.dumps(all_types, ensure_ascii=False)
depts_json = json.dumps(all_depts, ensure_ascii=False)
dian_by_city_json = json.dumps(dian_by_city, ensure_ascii=False)

print(f"Entidades: {total_entities:,}  Variantes: {total_variants:,}  Embargos: {total_embargos:,}")

# ──────────────────────────────────────────
# 6. Generate HTML
# ──────────────────────────────────────────
html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dashboard v4 — Entidades & Embargos Limpios</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --bg: #0f1117; --surface: #1a1d27; --surface2: #232734; --border: #2d3348;
    --text: #e1e4ed; --text2: #8b90a0; --accent: #6366f1; --accent2: #818cf8;
    --green: #22c55e; --orange: #f59e0b; --red: #ef4444; --blue: #3b82f6; --teal: #14b8a6;
    --pink: #ec4899; --purple: #8b5cf6;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--bg); color: var(--text); line-height: 1.5; }}
  .container {{ max-width: 1600px; margin: 0 auto; padding: 20px; }}
  h1 {{ font-size: 1.5rem; font-weight: 600; margin-bottom: 4px; }}
  .subtitle {{ color: var(--text2); font-size: 0.85rem; margin-bottom: 24px; }}

  /* Tabs */
  .tabs {{ display: flex; gap: 2px; margin-bottom: 20px; background: var(--surface); border-radius: 12px; padding: 4px; border: 1px solid var(--border); }}
  .tab {{ padding: 10px 20px; border-radius: 8px; cursor: pointer; font-size: 0.85rem; font-weight: 500; color: var(--text2); transition: all 0.2s; }}
  .tab:hover {{ color: var(--text); }}
  .tab.active {{ background: var(--accent); color: white; }}
  .tab-content {{ display: none; }}
  .tab-content.active {{ display: block; }}

  .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(155px, 1fr)); gap: 12px; margin-bottom: 24px; }}
  .stat-card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 16px 20px; }}
  .stat-card .label {{ color: var(--text2); font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.05em; }}
  .stat-card .value {{ font-size: 1.5rem; font-weight: 700; margin-top: 2px; }}
  .stat-card .value.accent {{ color: var(--accent2); }}
  .stat-card .value.green {{ color: var(--green); }}
  .stat-card .value.orange {{ color: var(--orange); }}
  .stat-card .value.blue {{ color: var(--blue); }}
  .stat-card .value.teal {{ color: var(--teal); }}
  .stat-card .value.pink {{ color: var(--pink); }}
  .stat-card .value.red {{ color: var(--red); }}
  .stat-card .detail {{ color: var(--text2); font-size: 0.7rem; margin-top: 2px; }}

  .charts {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }}
  .charts.three {{ grid-template-columns: 1fr 1fr 1fr; }}
  .chart-card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }}
  .chart-card.full {{ grid-column: 1 / -1; }}
  .chart-card h3 {{ font-size: 0.85rem; margin-bottom: 12px; color: var(--text2); }}

  .filters {{
    background: var(--surface); border: 1px solid var(--border); border-radius: 12px;
    padding: 14px 18px; margin-bottom: 14px;
    display: flex; gap: 10px; align-items: center; flex-wrap: wrap;
  }}
  .filters input, .filters select {{
    background: var(--surface2); border: 1px solid var(--border);
    color: var(--text); padding: 7px 12px; border-radius: 8px; font-size: 0.82rem; outline: none;
  }}
  .filters input:focus, .filters select:focus {{ border-color: var(--accent); }}
  .filters input {{ flex: 1; min-width: 200px; }}
  .filters label {{ color: var(--text2); font-size: 0.75rem; white-space: nowrap; }}
  .result-count {{ color: var(--text2); font-size: 0.78rem; margin-left: auto; }}

  .table-wrap {{ background: var(--surface); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; }}
  table {{ width: 100%; border-collapse: collapse; }}
  thead th {{
    background: var(--surface2); padding: 10px 14px; text-align: left;
    font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.05em;
    color: var(--text2); cursor: pointer; user-select: none;
    border-bottom: 1px solid var(--border); white-space: nowrap;
  }}
  thead th:hover {{ color: var(--accent2); }}
  tbody tr {{ border-bottom: 1px solid var(--border); cursor: pointer; transition: background 0.15s; }}
  tbody tr:hover {{ background: var(--surface2); }}
  tbody tr.expanded {{ background: rgba(99,102,241,0.08); }}
  td {{ padding: 8px 14px; font-size: 0.82rem; }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  td.id {{ color: var(--text2); font-size: 0.78rem; }}
  td.loc {{ font-size: 0.78rem; color: var(--teal); }}
  td.dept {{ font-size: 0.78rem; color: var(--orange); }}

  .badge {{
    display: inline-block; padding: 2px 8px; border-radius: 6px;
    font-size: 0.68rem; font-weight: 500; white-space: nowrap;
  }}
  .badge.JUZGADO {{ background: rgba(59,130,246,0.15); color: #60a5fa; }}
  .badge.ALCALDIA {{ background: rgba(34,197,94,0.15); color: #4ade80; }}
  .badge.GOBERNACION {{ background: rgba(245,158,11,0.15); color: #fbbf24; }}
  .badge.DATT {{ background: rgba(239,68,68,0.15); color: #f87171; }}
  .badge.SECRETARIA {{ background: rgba(168,85,247,0.15); color: #c084fc; }}
  .badge.OFICINA_APOYO {{ background: rgba(20,184,166,0.15); color: #2dd4bf; }}
  .badge.SUPERINTENDENCIA {{ background: rgba(236,72,153,0.15); color: #f472b6; }}
  .badge.MINISTERIO {{ background: rgba(251,146,60,0.15); color: #fb923c; }}
  .badge.DIAN {{ background: rgba(99,102,241,0.2); color: #a5b4fc; }}
  .badge.OTRO {{ background: rgba(139,144,160,0.12); color: #8b90a0; }}
  .badge.MUNICIPIO {{ background: rgba(132,204,22,0.15); color: #a3e635; }}
  .badge.default {{ background: rgba(139,144,160,0.12); color: #8b90a0; }}

  .badge-estado {{ display: inline-block; padding: 2px 8px; border-radius: 6px; font-size: 0.68rem; font-weight: 500; }}
  .badge-estado.CONFIRMADO {{ background: rgba(34,197,94,0.15); color: #4ade80; }}
  .badge-estado.PROCESADO {{ background: rgba(59,130,246,0.15); color: #60a5fa; }}
  .badge-estado.RECONFIRMADO {{ background: rgba(245,158,11,0.15); color: #fbbf24; }}
  .badge-estado.PROCESADO_CON_ERRORES {{ background: rgba(239,68,68,0.15); color: #f87171; }}
  .badge-estado.EN_PROCESO {{ background: rgba(168,85,247,0.15); color: #c084fc; }}

  .variants-row td {{ padding: 0; }}
  .variants-panel {{
    background: var(--bg); padding: 10px 18px 10px 50px;
    max-height: 280px; overflow-y: auto;
  }}
  .variants-panel table {{ width: 100%; }}
  .variants-panel th {{
    padding: 5px 10px; font-size: 0.68rem; color: var(--text2);
    text-transform: uppercase; border-bottom: 1px solid var(--border);
    text-align: left; background: transparent; cursor: default;
  }}
  .variants-panel td {{ padding: 4px 10px; font-size: 0.78rem; border-bottom: 1px solid rgba(45,51,72,0.4); }}
  .variants-panel tr:last-child td {{ border-bottom: none; }}
  .variants-panel .bar {{ height: 3px; background: var(--accent); border-radius: 2px; min-width: 2px; }}

  .pagination {{ display: flex; justify-content: center; align-items: center; gap: 8px; padding: 14px; }}
  .pagination button {{
    background: var(--surface2); border: 1px solid var(--border);
    color: var(--text); padding: 5px 12px; border-radius: 6px; cursor: pointer; font-size: 0.78rem;
  }}
  .pagination button:hover {{ background: var(--border); }}
  .pagination button:disabled {{ opacity: 0.3; cursor: default; }}
  .pagination span {{ color: var(--text2); font-size: 0.78rem; }}

  /* DIAN section */
  .dian-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 8px; margin-top: 12px; }}
  .dian-city {{
    background: var(--surface2); border: 1px solid var(--border); border-radius: 8px;
    padding: 10px 14px; display: flex; justify-content: space-between; align-items: center;
  }}
  .dian-city .city-name {{ font-size: 0.82rem; }}
  .dian-city .city-count {{ font-size: 0.85rem; font-weight: 600; color: var(--accent2); font-variant-numeric: tabular-nums; }}

  @media (max-width: 1100px) {{ .charts, .charts.three {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
<div class="container">
  <h1>Dashboard v4 — Entidades & Embargos</h1>
  <p class="subtitle">Modelo relacional limpio: {total_embargos:,} embargos → {total_entities:,} entidades normalizadas | Sin oficios SIN_CONFIRMAR</p>

  <div class="tabs">
    <div class="tab active" onclick="switchTab('overview')">Resumen General</div>
    <div class="tab" onclick="switchTab('entidades')">Entidades ({total_entities:,})</div>
    <div class="tab" onclick="switchTab('embargos')">Embargos ({total_embargos:,})</div>
    <div class="tab" onclick="switchTab('dian')">DIAN por Municipio</div>
  </div>

  <!-- ═══════════ TAB: OVERVIEW ═══════════ -->
  <div id="tab-overview" class="tab-content active">
    <div class="stats">
      <div class="stat-card"><div class="label">Embargos Limpios</div><div class="value blue">{total_embargos:,}</div><div class="detail">Sin SIN_CONFIRMAR</div></div>
      <div class="stat-card"><div class="label">Entidades Normalizadas</div><div class="value accent">{total_entities:,}</div></div>
      <div class="stat-card"><div class="label">Variantes Originales</div><div class="value orange">{total_variants:,}</div></div>
      <div class="stat-card"><div class="label">Monto Total</div><div class="value green">${{monto_total/1e9:,.1f}}B</div><div class="detail">{embargos_con_monto:,} con monto</div></div>
      <div class="stat-card"><div class="label">Municipios</div><div class="value teal">{len(munis):,}</div></div>
      <div class="stat-card"><div class="label">Departamentos</div><div class="value green">{len(all_depts)}</div></div>
      <div class="stat-card"><div class="label">Reducción Variantes</div><div class="value pink">{(1 - total_entities/total_variants)*100:.1f}%</div></div>
      <div class="stat-card"><div class="label">Descartados</div><div class="value red">13,265</div><div class="detail">SIN_CONFIRMAR eliminados</div></div>
    </div>

    <div class="charts">
      <div class="chart-card">
        <h3>Embargos por Estado</h3>
        <canvas id="chartEstado"></canvas>
      </div>
      <div class="chart-card">
        <h3>Embargos por Entidad Bancaria</h3>
        <canvas id="chartBancaria"></canvas>
      </div>
      <div class="chart-card">
        <h3>Registros por Tipo de Entidad</h3>
        <canvas id="chartType"></canvas>
      </div>
      <div class="chart-card">
        <h3>Top 15 Departamentos</h3>
        <canvas id="chartDept"></canvas>
      </div>
    </div>
  </div>

  <!-- ═══════════ TAB: ENTIDADES ═══════════ -->
  <div id="tab-entidades" class="tab-content">
    <div class="charts three">
      <div class="chart-card">
        <h3>Entidades por Tipo</h3>
        <canvas id="chartTypeCount"></canvas>
      </div>
      <div class="chart-card">
        <h3>Registros por Tipo</h3>
        <canvas id="chartTypeRec"></canvas>
      </div>
      <div class="chart-card">
        <h3>Top 15 Departamentos</h3>
        <canvas id="chartDeptEnt"></canvas>
      </div>
    </div>

    <div class="filters">
      <input type="text" id="searchInput" placeholder="Buscar por nombre, ID, municipio...">
      <div><label>Tipo:</label>
        <select id="typeFilter"><option value="">Todos</option></select>
      </div>
      <div><label>Depto:</label>
        <select id="deptFilter"><option value="">Todos</option></select>
      </div>
      <div><label>Ordenar:</label>
        <select id="sortSelect">
          <option value="total-desc">Mayor registros</option>
          <option value="total-asc">Menor registros</option>
          <option value="variantes-desc">Más variantes</option>
          <option value="nombre-asc">Nombre A-Z</option>
          <option value="municipio-asc">Municipio A-Z</option>
          <option value="id-asc">ID ↑</option>
        </select>
      </div>
      <span class="result-count" id="resultCount"></span>
    </div>

    <div class="table-wrap">
      <table>
        <thead><tr>
          <th style="width:50px">ID</th>
          <th>Nombre Normalizado</th>
          <th style="width:90px">Tipo</th>
          <th style="width:130px">Municipio</th>
          <th style="width:120px">Departamento</th>
          <th style="width:80px">Registros</th>
          <th style="width:70px">Variantes</th>
        </tr></thead>
        <tbody id="tableBody"></tbody>
      </table>
      <div class="pagination" id="pagination"></div>
    </div>
  </div>

  <!-- ═══════════ TAB: EMBARGOS ═══════════ -->
  <div id="tab-embargos" class="tab-content">
    <div class="stats">
      <div class="stat-card"><div class="label">Total Embargos</div><div class="value blue">{total_embargos:,}</div></div>
      <div class="stat-card"><div class="label">Confirmados</div><div class="value green">{embargo_by_estado.get('CONFIRMADO',0):,}</div></div>
      <div class="stat-card"><div class="label">Procesados</div><div class="value accent">{embargo_by_estado.get('PROCESADO',0):,}</div></div>
      <div class="stat-card"><div class="label">Reconfirmados</div><div class="value orange">{embargo_by_estado.get('RECONFIRMADO',0):,}</div></div>
      <div class="stat-card"><div class="label">Con Errores</div><div class="value red">{embargo_by_estado.get('PROCESADO_CON_ERRORES',0):,}</div></div>
      <div class="stat-card"><div class="label">Monto Total</div><div class="value green">${{monto_total/1e9:,.1f}}B</div></div>
    </div>

    <div class="charts">
      <div class="chart-card">
        <h3>Top 20 Entidades Remitentes por Embargos</h3>
        <canvas id="chartTopRemitentes"></canvas>
      </div>
      <div class="chart-card">
        <h3>Embargos por Tipo (Judicial / Coactivo)</h3>
        <canvas id="chartTipoEmbargo"></canvas>
      </div>
    </div>
  </div>

  <!-- ═══════════ TAB: DIAN ═══════════ -->
  <div id="tab-dian" class="tab-content">
    <div class="stats">
      <div class="stat-card"><div class="label">Embargos DIAN Total</div><div class="value accent">{sum(d['embargos'] for d in dian_by_city):,}</div></div>
      <div class="stat-card"><div class="label">Ciudades Identificadas</div><div class="value teal">{len([d for d in dian_by_city if d['normalizado'] != 'SIN CIUDAD']):,}</div></div>
      <div class="stat-card"><div class="label">Entidades DIAN</div><div class="value orange">{len(dian_ids)}</div></div>
    </div>
    <div class="charts">
      <div class="chart-card full">
        <h3>Top 30 Ciudades — Embargos DIAN</h3>
        <canvas id="chartDian"></canvas>
      </div>
    </div>
    <div class="chart-card" style="margin-top:16px">
      <h3>Todas las ciudades DIAN</h3>
      <input type="text" id="dianSearch" placeholder="Buscar ciudad..." style="background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:7px 12px;border-radius:8px;font-size:0.82rem;width:100%;margin-bottom:12px;outline:none;">
      <div class="dian-grid" id="dianGrid"></div>
    </div>
  </div>
</div>

<script>
const E = {entidades_json};
const V = {variantes_json};
const TYPES = {types_json};
const DEPTS = {depts_json};
const DIAN_CITIES = {dian_by_city_json};

const PAGE = 50;
let filtered = [...E];
let page = 1;
let expanded = null;

// Tab switching
function switchTab(name) {{
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  event.target.classList.add('active');
  if (name === 'dian') renderDianGrid();
}}

// Populate filters
const tf = document.getElementById('typeFilter');
TYPES.forEach(t => {{ const o = document.createElement('option'); o.value=t; o.textContent=t; tf.appendChild(o); }});
const df = document.getElementById('deptFilter');
DEPTS.forEach(d => {{ const o = document.createElement('option'); o.value=d; o.textContent=d; df.appendChild(o); }});

function fmt(n) {{ return n.toLocaleString('es-CO'); }}
function esc(s) {{ const d=document.createElement('div'); d.textContent=s; return d.innerHTML; }}
function badgeCls(t) {{
  const known = ['JUZGADO','ALCALDIA','GOBERNACION','DATT','SECRETARIA','OFICINA_APOYO','SUPERINTENDENCIA','MINISTERIO','DIAN','OTRO','MUNICIPIO'];
  return known.includes(t) ? t : 'default';
}}

function applyFilters() {{
  const q = document.getElementById('searchInput').value.toLowerCase().trim();
  const tipo = tf.value;
  const dept = df.value;
  const sort = document.getElementById('sortSelect').value;
  filtered = E.filter(e => {{
    if (tipo && e.tipo !== tipo) return false;
    if (dept && e.departamento !== dept) return false;
    if (q) {{
      const match = e.id.toString() === q ||
        e.nombre.toLowerCase().includes(q) ||
        (e.municipio && e.municipio.toLowerCase().includes(q)) ||
        (e.departamento && e.departamento.toLowerCase().includes(q)) ||
        (V[e.id] || []).some(v => v.v.toLowerCase().includes(q));
      if (!match) return false;
    }}
    return true;
  }});
  const [field, dir] = sort.split('-');
  const m = dir === 'asc' ? 1 : -1;
  filtered.sort((a,b) => {{
    if (field==='total') return (a.total-b.total)*m;
    if (field==='variantes') return (a.variantes-b.variantes)*m;
    if (field==='nombre') return a.nombre.localeCompare(b.nombre)*m;
    if (field==='municipio') return (a.municipio||'zzz').localeCompare(b.municipio||'zzz')*m;
    if (field==='id') return (a.id-b.id)*m;
    return 0;
  }});
  page = 1; expanded = null; render();
}}

function render() {{
  const body = document.getElementById('tableBody');
  const pages = Math.ceil(filtered.length/PAGE);
  const start = (page-1)*PAGE;
  const slice = filtered.slice(start, start+PAGE);
  document.getElementById('resultCount').textContent = fmt(filtered.length) + ' entidades';
  let h = '';
  slice.forEach(e => {{
    const exp = expanded === e.id;
    h += `<tr class="${{exp?'expanded':''}}" onclick="toggle(${{e.id}})">
      <td class="id">${{e.id}}</td>
      <td>${{esc(e.nombre)}}</td>
      <td><span class="badge ${{badgeCls(e.tipo)}}">${{e.tipo}}</span></td>
      <td class="loc">${{esc(e.municipio||'—')}}</td>
      <td class="dept">${{esc(e.departamento||'—')}}</td>
      <td class="num">${{fmt(e.total)}}</td>
      <td class="num">${{fmt(e.variantes)}}</td>
    </tr>`;
    if (exp) {{
      const vars = V[e.id] || [];
      const mx = vars.length>0 ? vars[0].c : 1;
      h += `<tr class="variants-row"><td colspan="7"><div class="variants-panel">
        <table><thead><tr><th>Variante Original</th><th style="width:80px">Conteo</th><th style="width:100px">%</th></tr></thead><tbody>`;
      vars.forEach(v => {{
        const p = v.c/mx*100;
        h += `<tr><td>${{esc(v.v)}}</td><td class="num">${{fmt(v.c)}}</td>
          <td><div class="bar" style="width:${{Math.max(p,1)}}%"></div></td></tr>`;
      }});
      h += `</tbody></table></div></td></tr>`;
    }}
  }});
  body.innerHTML = h;
  const pg = document.getElementById('pagination');
  if (pages<=1) {{ pg.innerHTML=''; return; }}
  pg.innerHTML = `<button onclick="go(${{page-1}})" ${{page===1?'disabled':''}}>← Anterior</button>
    <span>Página ${{page}} de ${{pages}}</span>
    <button onclick="go(${{page+1}})" ${{page===pages?'disabled':''}}>Siguiente →</button>`;
}}

function toggle(id) {{ expanded = expanded===id ? null : id; render(); }}
function go(p) {{ page=p; expanded=null; render(); document.querySelector('.table-wrap').scrollIntoView({{behavior:'smooth'}}); }}
function debounce(fn,ms) {{ let t; return (...a) => {{ clearTimeout(t); t=setTimeout(()=>fn(...a),ms); }}; }}

document.getElementById('searchInput').addEventListener('input', debounce(applyFilters, 250));
tf.addEventListener('change', applyFilters);
df.addEventListener('change', applyFilters);
document.getElementById('sortSelect').addEventListener('change', applyFilters);

// DIAN grid
function renderDianGrid(q='') {{
  const grid = document.getElementById('dianGrid');
  const cities = q ? DIAN_CITIES.filter(d => d.ciudad.toLowerCase().includes(q.toLowerCase())) : DIAN_CITIES;
  grid.innerHTML = cities.map(d =>
    `<div class="dian-city"><span class="city-name">${{esc(d.ciudad)}}</span><span class="city-count">${{fmt(d.embargos)}}</span></div>`
  ).join('');
}}
document.getElementById('dianSearch').addEventListener('input', debounce(e => renderDianGrid(e.target.value), 200));

// ═══════════ Charts ═══════════
const C = ['#6366f1','#3b82f6','#22c55e','#f59e0b','#ef4444','#ec4899','#8b5cf6','#14b8a6','#f97316','#06b6d4','#84cc16','#e11d48','#a855f7','#10b981','#eab308','#64748b','#f43f5e','#0ea5e9','#d946ef','#78716c'];
const barOpts = (h) => ({{
  indexAxis:'y', responsive:true, maintainAspectRatio:false,
  plugins: {{ legend:{{display:false}}, tooltip:{{ callbacks:{{ label: c=>fmt(c.raw) }} }} }},
  scales: {{ x:{{grid:{{color:'rgba(45,51,72,0.5)'}}, ticks:{{color:'#8b90a0', callback:v=>v>=1e6?(v/1e6)+'M':v>=1000?(v/1000)+'K':v}} }}, y:{{grid:{{display:false}}, ticks:{{color:'#e1e4ed',font:{{size:10}} }} }} }}
}});

// Estado chart
new Chart(document.getElementById('chartEstado'), {{
  type: 'doughnut',
  data: {{ labels: {estado_labels}, datasets: [{{ data: {estado_values}, backgroundColor: ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6'], borderWidth:0, hoverOffset:8 }}] }},
  options: {{ responsive:true, maintainAspectRatio:false, cutout:'55%',
    plugins: {{ legend:{{position:'right',labels:{{color:'#8b90a0',font:{{size:10}},padding:6,boxWidth:10}} }}, tooltip:{{callbacks:{{label:c=>c.label+': '+fmt(c.raw)}} }} }} }}
}});
document.getElementById('chartEstado').parentElement.style.height = '300px';

// Bancaria chart
new Chart(document.getElementById('chartBancaria'), {{
  type: 'doughnut',
  data: {{ labels: {bancaria_labels}, datasets: [{{ data: {bancaria_values}, backgroundColor: C.slice(0,{len(bancaria_chart)}), borderWidth:0, hoverOffset:8 }}] }},
  options: {{ responsive:true, maintainAspectRatio:false, cutout:'55%',
    plugins: {{ legend:{{position:'right',labels:{{color:'#8b90a0',font:{{size:10}},padding:6,boxWidth:10}} }}, tooltip:{{callbacks:{{label:c=>c.label+': '+fmt(c.raw)}} }} }} }}
}});
document.getElementById('chartBancaria').parentElement.style.height = '300px';

// Type records
new Chart(document.getElementById('chartType'), {{
  type: 'bar',
  data: {{ labels: {type_labels}, datasets: [{{ data: {type_values}, backgroundColor: C.slice(0,{len(type_chart)}), borderRadius:5, borderSkipped:false }}] }},
  options: barOpts()
}});
document.getElementById('chartType').parentElement.style.height = ({len(type_chart)}*30+40)+'px';

// Dept
new Chart(document.getElementById('chartDept'), {{
  type: 'bar',
  data: {{ labels: {dept_labels}, datasets: [{{ data: {dept_values}, backgroundColor: C.slice(0,15), borderRadius:5, borderSkipped:false }}] }},
  options: barOpts()
}});
document.getElementById('chartDept').parentElement.style.height = '380px';

// Entidades tab: type count donut
new Chart(document.getElementById('chartTypeCount'), {{
  type: 'doughnut',
  data: {{ labels: {type_labels}, datasets: [{{ data: {type_entity_counts}, backgroundColor: C.slice(0,{len(type_chart)}), borderWidth:0, hoverOffset:8 }}] }},
  options: {{ responsive:true, maintainAspectRatio:false, cutout:'55%',
    plugins: {{ legend:{{position:'bottom',labels:{{color:'#8b90a0',font:{{size:9}},padding:4,boxWidth:8}} }}, tooltip:{{callbacks:{{label:c=>c.label+': '+fmt(c.raw)+' entidades'}} }} }} }}
}});
document.getElementById('chartTypeCount').parentElement.style.height = '350px';

// Entidades tab: type records bar
new Chart(document.getElementById('chartTypeRec'), {{
  type: 'bar',
  data: {{ labels: {type_labels}, datasets: [{{ data: {type_values}, backgroundColor: C.slice(0,{len(type_chart)}), borderRadius:5, borderSkipped:false }}] }},
  options: barOpts()
}});
document.getElementById('chartTypeRec').parentElement.style.height = ({len(type_chart)}*28+40)+'px';

// Entidades tab: dept bar
new Chart(document.getElementById('chartDeptEnt'), {{
  type: 'bar',
  data: {{ labels: {dept_labels}, datasets: [{{ data: {dept_values}, backgroundColor: C.slice(0,15), borderRadius:5, borderSkipped:false }}] }},
  options: barOpts()
}});
document.getElementById('chartDeptEnt').parentElement.style.height = '350px';

// Embargos tab: Top remitentes
const topRemLabels = {json.dumps([entidades_by_id[r[0]]['nombre'][:45] for r in top_remitentes], ensure_ascii=False)};
const topRemValues = {json.dumps([r[1] for r in top_remitentes])};
new Chart(document.getElementById('chartTopRemitentes'), {{
  type: 'bar',
  data: {{ labels: topRemLabels, datasets: [{{ data: topRemValues, backgroundColor: C.slice(0,20), borderRadius:5, borderSkipped:false }}] }},
  options: barOpts()
}});
document.getElementById('chartTopRemitentes').parentElement.style.height = (20*28+40)+'px';

// Embargos tab: tipo embargo donut
const tipoEmbLabels = {json.dumps([t[0] for t in sorted(embargo_by_tipo.items(), key=lambda x: -x[1])], ensure_ascii=False)};
const tipoEmbValues = {json.dumps([t[1] for t in sorted(embargo_by_tipo.items(), key=lambda x: -x[1])])};
new Chart(document.getElementById('chartTipoEmbargo'), {{
  type: 'doughnut',
  data: {{ labels: tipoEmbLabels, datasets: [{{ data: tipoEmbValues, backgroundColor: C, borderWidth:0, hoverOffset:8 }}] }},
  options: {{ responsive:true, maintainAspectRatio:false, cutout:'55%',
    plugins: {{ legend:{{position:'right',labels:{{color:'#8b90a0',font:{{size:10}},padding:6,boxWidth:10}} }}, tooltip:{{callbacks:{{label:c=>c.label+': '+fmt(c.raw)}} }} }} }}
}});
document.getElementById('chartTipoEmbargo').parentElement.style.height = '400px';

// DIAN chart
new Chart(document.getElementById('chartDian'), {{
  type: 'bar',
  data: {{ labels: {dian_labels}, datasets: [{{ data: {dian_values}, backgroundColor: C.concat(C).slice(0,30), borderRadius:5, borderSkipped:false }}] }},
  options: barOpts()
}});
document.getElementById('chartDian').parentElement.style.height = (30*26+40)+'px';

applyFilters();
</script>
</body>
</html>"""

with open('dashboard.html', 'w', encoding='utf-8') as f:
    f.write(html)

print(f"✓ dashboard.html generado ({len(html)/1024:.0f} KB)")
