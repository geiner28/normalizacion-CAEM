#!/usr/bin/env python3
"""Diagnostic report for normalization success rates."""
import csv, json

# Load data
entidades = []
with open('entidades.csv', 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        entidades.append(row)

total_entities = len(entidades)
total_registros = sum(int(r['total_registros']) for r in entidades)
total_variantes = sum(int(r['num_variantes']) for r in entidades)

with_municipio = [r for r in entidades if r['municipio'].strip()]
without_municipio = [r for r in entidades if not r['municipio'].strip()]
with_dept = [r for r in entidades if r['departamento'].strip()]
without_dept = [r for r in entidades if not r['departamento'].strip()]

registros_with_muni = sum(int(r['total_registros']) for r in with_municipio)
registros_without_muni = sum(int(r['total_registros']) for r in without_municipio)
registros_with_dept = sum(int(r['total_registros']) for r in with_dept)
registros_without_dept = sum(int(r['total_registros']) for r in without_dept)

tipos = {}
for r in entidades:
    t = r['tipo']
    if t not in tipos:
        tipos[t] = {'count': 0, 'registros': 0, 'variantes': 0, 'with_muni': 0, 'with_dept': 0}
    tipos[t]['count'] += 1
    tipos[t]['registros'] += int(r['total_registros'])
    tipos[t]['variantes'] += int(r['num_variantes'])
    if r['municipio'].strip():
        tipos[t]['with_muni'] += 1
    if r['departamento'].strip():
        tipos[t]['with_dept'] += 1

raw_count = sum(1 for _ in open('unique_entities_raw.csv', encoding='utf-8')) - 1
var_count = sum(1 for _ in open('variantes_entidades.csv', encoding='utf-8')) - 1

otros = [r for r in entidades if r['tipo'] == 'OTRO']
no_encontrada = [r for r in entidades if 'no encontrada' in r['nombre_normalizado'].lower()]
registros_otros = sum(int(r['total_registros']) for r in otros)
registros_no_enc = sum(int(r['total_registros']) for r in no_encontrada)

print('=' * 80)
print('DIAGNOSTIC REPORT: ENTITY & MUNICIPALITY NORMALIZATION')
print('=' * 80)

print(f'\n--- GENERAL OVERVIEW ---')
print(f'  Raw unique entity strings:       {raw_count:>10,}')
print(f'  Variants mapped (variantes.csv): {var_count:>10,}')
print(f'  Final normalized entities:       {total_entities:>10,}')
print(f'  Total records (embargos):        {total_registros:>10,}')
print(f'  Merge ratio:                     {(1 - total_entities / var_count) * 100:.2f}%')

print(f'\n--- MUNICIPALITY NORMALIZATION ---')
print(f'  Entities WITH municipio:         {len(with_municipio):>10,} ({len(with_municipio) / total_entities * 100:.1f}%)')
print(f'  Entities WITHOUT municipio:      {len(without_municipio):>10,} ({len(without_municipio) / total_entities * 100:.1f}%)')
print(f'  Records WITH municipio:          {registros_with_muni:>10,} ({registros_with_muni / total_registros * 100:.2f}%)')
print(f'  Records WITHOUT municipio:       {registros_without_muni:>10,} ({registros_without_muni / total_registros * 100:.2f}%)')

print(f'\n--- DEPARTMENT NORMALIZATION ---')
print(f'  Entities WITH departamento:      {len(with_dept):>10,} ({len(with_dept) / total_entities * 100:.1f}%)')
print(f'  Entities WITHOUT departamento:   {len(without_dept):>10,} ({len(without_dept) / total_entities * 100:.1f}%)')
print(f'  Records WITH departamento:       {registros_with_dept:>10,} ({registros_with_dept / total_registros * 100:.2f}%)')
print(f'  Records WITHOUT departamento:    {registros_without_dept:>10,} ({registros_without_dept / total_registros * 100:.2f}%)')

print(f'\n--- ENTITY TYPE CLASSIFICATION ---')
print(f'  Typed entities (not OTRO):       {total_entities - len(otros):>10,} ({(total_entities - len(otros)) / total_entities * 100:.1f}%)')
print(f'  OTRO (unclassified):             {len(otros):>10,} ({len(otros) / total_entities * 100:.1f}%)')
print(f'  No encontrada entries:           {len(no_encontrada):>10,}')
print(f'  Records typed (not OTRO):        {total_registros - registros_otros:>10,} ({(total_registros - registros_otros) / total_registros * 100:.2f}%)')
print(f'  Records OTRO:                    {registros_otros:>10,} ({registros_otros / total_registros * 100:.2f}%)')

print(f'\n--- BREAKDOWN BY ENTITY TYPE ---')
print(f'  {"TYPE":<25} {"ENTS":>6} {"RECORDS":>10} {"VARS":>7} {"W/MUNI":>8} {"W/DEPT":>8} {"MUNI%":>7} {"DEPT%":>7}')
print(f'  {"-"*25} {"-"*6} {"-"*10} {"-"*7} {"-"*8} {"-"*8} {"-"*7} {"-"*7}')
for t in sorted(tipos.keys(), key=lambda x: -tipos[x]['registros']):
    d = tipos[t]
    mp = d['with_muni'] / d['count'] * 100 if d['count'] else 0
    dp = d['with_dept'] / d['count'] * 100 if d['count'] else 0
    print(f'  {t:<25} {d["count"]:>6,} {d["registros"]:>10,} {d["variantes"]:>7,} {d["with_muni"]:>8,} {d["with_dept"]:>8,} {mp:>6.1f}% {dp:>6.1f}%')

print(f'\n--- TOP 20 ENTITIES WITHOUT MUNICIPIO ---')
for r in sorted(without_municipio, key=lambda x: -int(x['total_registros']))[:20]:
    print(f'  ID {r["entidad_id"]:>5}: {int(r["total_registros"]):>8,} recs | [{r["tipo"]:>20}] {r["nombre_normalizado"][:70]}')

print(f'\n--- TOP 15 ENTITIES WITHOUT DEPARTAMENTO ---')
for r in sorted(without_dept, key=lambda x: -int(x['total_registros']))[:15]:
    print(f'  ID {r["entidad_id"]:>5}: {int(r["total_registros"]):>8,} recs | [{r["tipo"]:>20}] {r["nombre_normalizado"][:70]}')

muni_csv_count = sum(1 for _ in open('municipios.csv', encoding='utf-8')) - 1
with open('colombia_municipios.json', 'r', encoding='utf-8') as f:
    dane = json.load(f)
dane_depts = set(d['departamento'] for d in dane)
our_depts = set(r['departamento'] for r in entidades if r['departamento'].strip())

print(f'\n--- MUNICIPALITIES & DEPARTMENTS ---')
print(f'  Unique municipios in output:     {muni_csv_count}')
print(f'  DANE municipios total:           1,104')
print(f'  Municipio coverage:              {muni_csv_count / 1104 * 100:.1f}%')
print(f'  DANE departments:                {len(dane_depts)}')
print(f'  Our departments:                 {len(our_depts)}')
match = len(our_depts & dane_depts)
print(f'  Department coverage:             {match}/{len(dane_depts)} ({match / len(dane_depts) * 100:.1f}%)')
extra = our_depts - dane_depts
if extra:
    print(f'  Extra depts (not in DANE):       {extra}')

print(f'\n--- GEOGRAPHIC NORMALIZATION (weighted by records) ---')
either = sum(int(r['total_registros']) for r in entidades if r['municipio'].strip() or r['departamento'].strip())
print(f'  Municipio identified:            {registros_with_muni / total_registros * 100:.2f}%')
print(f'  Department identified:           {registros_with_dept / total_registros * 100:.2f}%')
print(f'  Either muni or dept:             {either / total_registros * 100:.2f}%')

print(f'\n--- ENTITY TYPE SUCCESS (weighted by records) ---')
print(f'  Classified (not OTRO):           {(total_registros - registros_otros) / total_registros * 100:.2f}%')
print(f'  OTRO residual:                   {registros_otros / total_registros * 100:.2f}%')
print(f'  No encontrada:                   {registros_no_enc / total_registros * 100:.2f}%')

print(f'\n--- OVERALL SUCCESS ---')
fully = [r for r in entidades if r['tipo'] != 'OTRO' and (r['municipio'].strip() or r['departamento'].strip())]
fr = sum(int(r['total_registros']) for r in fully)
print(f'  Fully normalized (typed+located):')
print(f'    Entities: {len(fully):>6,} / {total_entities:>6,} ({len(fully) / total_entities * 100:.1f}%)')
print(f'    Records:  {fr:>10,} / {total_registros:>10,} ({fr / total_registros * 100:.2f}%)')
