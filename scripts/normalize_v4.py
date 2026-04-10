#!/usr/bin/env python3
"""
Entity Normalization v4 — Production-grade entity resolution
============================================================
Fixes from v3:
1. Gobernaciones grouped by DEPARTMENT (not municipality)
2. Department-aware location extraction (department vs municipality disambiguation)
3. More entity type categories (CORPORACION_AUTONOMA, RAMA_JUDICIAL, etc.)
4. Ambiguous municipality resolution (Florencia→Caquetá, Mosquera→Cundinamarca)
5. Two-pass location: extract department first, then municipality within that context
6. Stricter Levenshtein threshold + TF-IDF weighting for OTRO groups
7. Juzgado Promiscuo in small towns resolved per municipality
"""

import csv
import json
import re
import unicodedata
from collections import defaultdict, Counter
import math

# ============================================================
# TEXT NORMALIZATION
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


def clean_text(text):
    if not text:
        return ''
    t = text.strip()
    t = t.upper()
    t = remove_accents(t)
    t = t.replace('\u0f0b', ' ')  # Tibetan tsheg (found in data)
    t = t.replace('"', '').replace('\u201c', '').replace('\u201d', '').replace("'", '').replace('\u2018', '').replace('\u2019', '')
    t = re.sub(r'[\u2013\u2014]', '-', t)
    t = re.sub(r'\([^)]*\)', '', t)
    t = re.sub(r'\|', '', t)
    t = re.sub(r'\s+', ' ', t)
    t = re.sub(r'^[.,;:\-\s]+', '', t)
    t = re.sub(r'[.,;:\-\s]+$', '', t)
    return t.strip()


# ============================================================
# ORDINAL NUMBERS
# ============================================================

COMPOUND_NUMS = {
    'DECIMO PRIMERO': '11', 'DECIMO PRIMERA': '11',
    'DECIMO SEGUNDO': '12', 'DECIMO SEGUNDA': '12',
    'DECIMO TERCERO': '13', 'DECIMO TERCERA': '13',
    'DECIMO CUARTO': '14', 'DECIMO CUARTA': '14',
    'DECIMO QUINTO': '15', 'DECIMO QUINTA': '15',
    'DECIMO SEXTO': '16', 'DECIMO SEXTA': '16',
    'DECIMO SEPTIMO': '17', 'DECIMO SEPTIMA': '17',
    'DECIMO OCTAVO': '18', 'DECIMO OCTAVA': '18',
    'DECIMO NOVENO': '19', 'DECIMO NOVENA': '19',
    'VIGESIMO PRIMERO': '21', 'VIGESIMO PRIMERA': '21',
    'VIGESIMO SEGUNDO': '22', 'VIGESIMO SEGUNDA': '22',
    'VIGESIMO TERCERO': '23', 'VIGESIMO TERCERA': '23',
    'VIGESIMO CUARTO': '24', 'VIGESIMO CUARTA': '24',
    'VIGESIMO QUINTO': '25', 'VIGESIMO QUINTA': '25',
    'VIGESIMO SEXTO': '26', 'VIGESIMO SEXTA': '26',
    'VIGESIMO SEPTIMO': '27', 'VIGESIMO SEPTIMA': '27',
    'VIGESIMO OCTAVO': '28', 'VIGESIMO OCTAVA': '28',
    'VIGESIMO NOVENO': '29', 'VIGESIMO NOVENA': '29',
    'TREINTA Y UNO': '31', 'TREINTA Y UN': '31',
    'TREINTA Y DOS': '32', 'TREINTA Y TRES': '33',
    'TREINTA Y CUATRO': '34', 'TREINTA Y CINCO': '35',
    'TREINTA Y SEIS': '36', 'TREINTA Y SIETE': '37',
    'TREINTA Y OCHO': '38', 'TREINTA Y NUEVE': '39',
    'CUARENTA Y UNO': '41', 'CUARENTA Y UN': '41',
    'CUARENTA Y DOS': '42', 'CUARENTA Y TRES': '43',
    'CUARENTA Y CUATRO': '44', 'CUARENTA Y CINCO': '45',
    'CUARENTA Y SEIS': '46', 'CUARENTA Y SIETE': '47',
    'CUARENTA Y OCHO': '48', 'CUARENTA Y NUEVE': '49',
    'CINCUENTA Y UNO': '51', 'CINCUENTA Y UN': '51',
    'CINCUENTA Y DOS': '52', 'CINCUENTA Y TRES': '53',
    'CINCUENTA Y CUATRO': '54', 'CINCUENTA Y CINCO': '55',
    'CINCUENTA Y SEIS': '56', 'CINCUENTA Y SIETE': '57',
    'CINCUENTA Y OCHO': '58', 'CINCUENTA Y NUEVE': '59',
    'SESENTA Y UNO': '61', 'SESENTA Y UN': '61',
    'SESENTA Y DOS': '62', 'SESENTA Y TRES': '63',
    'SESENTA Y CUATRO': '64', 'SESENTA Y CINCO': '65',
    'SESENTA Y SEIS': '66', 'SESENTA Y SIETE': '67',
    'SESENTA Y OCHO': '68', 'SESENTA Y NUEVE': '69',
    'SETENTA Y UNO': '71', 'SETENTA Y UN': '71',
    'SETENTA Y DOS': '72', 'SETENTA Y TRES': '73',
    'SETENTA Y CUATRO': '74', 'SETENTA Y CINCO': '75',
    'SETENTA Y SEIS': '76', 'SETENTA Y SIETE': '77',
    'SETENTA Y OCHO': '78', 'SETENTA Y NUEVE': '79',
    'OCHENTA Y UNO': '81', 'OCHENTA Y UN': '81',
    'OCHENTA Y DOS': '82', 'OCHENTA Y TRES': '83',
    'OCHENTA Y CUATRO': '84', 'OCHENTA Y CINCO': '85',
}

SINGLE_NUMS = {
    'PRIMERO': '1', 'PRIMER': '1', 'PRIMERA': '1', '1ERO': '1', '1ER': '1', '1RO': '1',
    'SEGUNDO': '2', 'SEGUNDA': '2', '2DO': '2', '2DA': '2',
    'TERCERO': '3', 'TERCERA': '3', '3ERO': '3', '3ER': '3', '3RO': '3',
    'CUARTO': '4', 'CUARTA': '4', '4TO': '4',
    'QUINTO': '5', 'QUINTA': '5', '5TO': '5',
    'SEXTO': '6', 'SEXTA': '6', '6TO': '6',
    'SEPTIMO': '7', 'SEPTIMA': '7', '7MO': '7',
    'OCTAVO': '8', 'OCTAVA': '8', '8VO': '8',
    'NOVENO': '9', 'NOVENA': '9', '9NO': '9',
    'DECIMO': '10', 'DECIMA': '10',
    'UNDECIMO': '11', 'UNDECIMA': '11',
    'DUODECIMO': '12', 'DUODECIMA': '12',
    'ONCE': '11', 'DOCE': '12', 'TRECE': '13', 'CATORCE': '14',
    'QUINCE': '15', 'DIECISEIS': '16', 'DIECISIETE': '17',
    'DIECIOCHO': '18', 'DIECINUEVE': '19',
    'VEINTE': '20', 'VEINTIUNO': '21', 'VEINTIUN': '21',
    'VEINTIDOS': '22', 'VEINTITRES': '23', 'VEINTICUATRO': '24',
    'VEINTICINCO': '25', 'VEINTISEIS': '26', 'VEINTISIETE': '27',
    'VEINTIOCHO': '28', 'VEINTINUEVE': '29',
    'TREINTA': '30', 'CUARENTA': '40', 'CINCUENTA': '50',
    'SESENTA': '60', 'SETENTA': '70', 'OCHENTA': '80', 'NOVENTA': '90',
}


def replace_ordinals(text):
    t = text
    for word, digit in sorted(COMPOUND_NUMS.items(), key=lambda x: -len(x[0])):
        t = re.sub(r'\b' + word + r'\b', digit, t)
    for word, digit in sorted(SINGLE_NUMS.items(), key=lambda x: -len(x[0])):
        t = re.sub(r'\b' + word + r'\b', digit, t)
    return t


# ============================================================
# DANE DATABASE
# ============================================================

print("=" * 70)
print("ENTITY NORMALIZATION v4 — Production-Grade Resolution")
print("=" * 70)

print("\n[1/7] Loading DANE database...")

with open('colombia_municipios.json', 'r', encoding='utf-8') as f:
    dane_data = json.load(f)

# municipio_norm -> [(municipio_original, departamento), ...]  (can be ambiguous)
municipio_raw = defaultdict(list)
departamento_db = {}  # dept_norm -> dept_original
dept_to_munis = defaultdict(set)  # dept_norm -> set of muni_norms

for dept_data in dane_data:
    dept_name = dept_data['departamento']
    dept_norm = remove_accents(dept_name.upper())
    departamento_db[dept_norm] = dept_name

    for city in dept_data['ciudades']:
        city_norm = remove_accents(city.upper())
        municipio_raw[city_norm].append((city, dept_name))
        dept_to_munis[dept_norm].add(city_norm)

# Ambiguous municipalities - resolve by most common association in Colombia
# These municipalities exist in multiple departments
AMBIGUOUS_RESOLUTION = {
    # city_norm -> preferred (city, dept) when no department context
    'FLORENCIA': ('Florencia', 'Caquetá'),         # NOT Cauca
    'MOSQUERA': ('Mosquera', 'Cundinamarca'),       # NOT Nariño
    'SAN CARLOS': ('San Carlos', 'Antioquia'),
    'SAN LUIS': ('San Luis', 'Antioquia'),
    'ALBANIA': ('Albania', 'La Guajira'),
    'BELEN': ('Belén', 'Boyacá'),
    'BOLIVAR': ('Bolívar', 'Santander'),            # Many exist
    'COLON': ('Colón', 'Putumayo'),
    'CONCORDIA': ('Concordia', 'Antioquia'),
    'EL CARMEN': ('El Carmen de Bolívar', 'Bolívar'),
    'GUADALUPE': ('Guadalupe', 'Huila'),
    'LA UNION': ('La Unión', 'Valle del Cauca'),
    'PROVIDENCIA': ('Providencia', 'San Andrés y Providencia'),
    'PUERTO RICO': ('Puerto Rico', 'Caquetá'),
    'RIONEGRO': ('Rionegro', 'Antioquia'),
    'SAN ANTONIO': ('San Antonio', 'Tolima'),
    'SAN JOSE': ('San José del Guaviare', 'Guaviare'),
    'SANTA ROSA': ('Santa Rosa de Cabal', 'Risaralda'),
    'BARBOSA': ('Barbosa', 'Antioquia'),            # Also in Santander
    'CALDAS': ('Caldas', 'Antioquia'),              # Also a department
    'GRANADA': ('Granada', 'Meta'),
    'LA PAZ': ('La Paz', 'Cesar'),
    'SUCRE': ('Sucre', 'Sucre'),
}

# City aliases for major cities
CITY_ALIASES = {
    'BOGOTA': ('Bogotá D.C.', 'Cundinamarca'),
    'BOGOTA DC': ('Bogotá D.C.', 'Cundinamarca'),
    'BOGOTA D.C.': ('Bogotá D.C.', 'Cundinamarca'),
    'BOGOTA D.C': ('Bogotá D.C.', 'Cundinamarca'),
    'BOGOTA D C': ('Bogotá D.C.', 'Cundinamarca'),
    'SANTIAGO DE CALI': ('Cali', 'Valle del Cauca'),
    'CALI': ('Cali', 'Valle del Cauca'),
    'CARTAGENA': ('Cartagena de Indias', 'Bolívar'),
    'CARTAGENA DE INDIAS': ('Cartagena de Indias', 'Bolívar'),
    'SANTA MARTA': ('Santa Marta', 'Magdalena'),
    'BARRANQUILLA': ('Barranquilla', 'Atlántico'),
    'BUCARAMANGA': ('Bucaramanga', 'Santander'),
    'CUCUTA': ('Cúcuta', 'Norte de Santander'),
    'SAN JOSE DE CUCUTA': ('Cúcuta', 'Norte de Santander'),
    'MEDELLIN': ('Medellín', 'Antioquia'),
    'IBAGUE': ('Ibagué', 'Tolima'),
    'PASTO': ('Pasto', 'Nariño'),
    'SAN JUAN DE PASTO': ('Pasto', 'Nariño'),
    'NEIVA': ('Neiva', 'Huila'),
    'VILLAVICENCIO': ('Villavicencio', 'Meta'),
    'MANIZALES': ('Manizales', 'Caldas'),
    'PEREIRA': ('Pereira', 'Risaralda'),
    'ARMENIA': ('Armenia', 'Quindío'),
    'MONTERIA': ('Montería', 'Córdoba'),
    'VALLEDUPAR': ('Valledupar', 'Cesar'),
    'SINCELEJO': ('Sincelejo', 'Sucre'),
    'POPAYAN': ('Popayán', 'Cauca'),
    'TUNJA': ('Tunja', 'Boyacá'),
    'RIOHACHA': ('Riohacha', 'La Guajira'),
    'QUIBDO': ('Quibdó', 'Chocó'),
    'FLORENCIA': ('Florencia', 'Caquetá'),
    'MOCOA': ('Mocoa', 'Putumayo'),
    'YOPAL': ('Yopal', 'Casanare'),
    'LETICIA': ('Leticia', 'Amazonas'),
    'INIRIDA': ('Inírida', 'Guainía'),
    'MITU': ('Mitú', 'Vaupés'),
    'PUERTO CARRENO': ('Puerto Carreño', 'Vichada'),
    'SAN JOSE DEL GUAVIARE': ('San José del Guaviare', 'Guaviare'),
    'SAN ANDRES': ('San Andrés', 'San Andrés y Providencia'),
    'ARAUCA': ('Arauca', 'Arauca'),
    'SOLEDAD': ('Soledad', 'Atlántico'),
    'SOACHA': ('Soacha', 'Cundinamarca'),
    'PALMIRA': ('Palmira', 'Valle del Cauca'),
    'BELLO': ('Bello', 'Antioquia'),
    'ITAGUI': ('Itagüí', 'Antioquia'),
    'ENVIGADO': ('Envigado', 'Antioquia'),
    'DOSQUEBRADAS': ('Dosquebradas', 'Risaralda'),
    'DOS QUEBRADAS': ('Dosquebradas', 'Risaralda'),
    'TULUA': ('Tuluá', 'Valle del Cauca'),
    'BARRANCABERMEJA': ('Barrancabermeja', 'Santander'),
    'BUENAVENTURA': ('Buenaventura', 'Valle del Cauca'),
    'FLORIDABLANCA': ('Floridablanca', 'Santander'),
    'GIRON': ('Girón', 'Santander'),
    'PIEDECUESTA': ('Piedecuesta', 'Santander'),
    'TURBACO': ('Turbaco', 'Bolívar'),
    'MAGANGUE': ('Magangué', 'Bolívar'),
    'GIRARDOT': ('Girardot', 'Cundinamarca'),
    'SOGAMOSO': ('Sogamoso', 'Boyacá'),
    'DUITAMA': ('Duitama', 'Boyacá'),
    'CARTAGO': ('Cartago', 'Valle del Cauca'),
    'MOSQUERA': ('Mosquera', 'Cundinamarca'),
    'FUSAGASUGA': ('Fusagasugá', 'Cundinamarca'),
    'FACATATIVA': ('Facatativá', 'Cundinamarca'),
    'ZIPAQUIRA': ('Zipaquirá', 'Cundinamarca'),
    'JAMUNDI': ('Jamundí', 'Valle del Cauca'),
    'YUMBO': ('Yumbo', 'Valle del Cauca'),
    'LORICA': ('Lorica', 'Córdoba'),
    'CERETE': ('Cereté', 'Córdoba'),
    'SAHAGUN': ('Sahagún', 'Córdoba'),
    'PLANETA RICA': ('Planeta Rica', 'Córdoba'),
    'MONTELIBANO': ('Montelíbano', 'Córdoba'),
    'TIERRALTA': ('Tierralta', 'Córdoba'),
    'CAUCASIA': ('Caucasia', 'Antioquia'),
    'CHIGORODO': ('Chigorodó', 'Antioquia'),
    'APARTADO': ('Apartadó', 'Antioquia'),
    'TURBO': ('Turbo', 'Antioquia'),
    'AGUACHICA': ('Aguachica', 'Cesar'),
    'OCANA': ('Ocaña', 'Norte de Santander'),
    'PAMPLONA': ('Pamplona', 'Norte de Santander'),
    'SAN GIL': ('San Gil', 'Santander'),
    'SOCORRO': ('El Socorro', 'Santander'),
    'CHIQUINQUIRA': ('Chiquinquirá', 'Boyacá'),
    'YONDO': ('Yondó', 'Antioquia'),
    'VILLA DEL ROSARIO': ('Villa del Rosario', 'Norte de Santander'),
    'EL CARMEN DE BOLIVAR': ('El Carmen de Bolívar', 'Bolívar'),
    'ACACIAS': ('Acacías', 'Meta'),
    'ESPINAL': ('El Espinal', 'Tolima'),
    'EL ESPINAL': ('El Espinal', 'Tolima'),
    'HONDA': ('Honda', 'Tolima'),
    'LA DORADA': ('La Dorada', 'Caldas'),
    'RIONEGRO': ('Rionegro', 'Antioquia'),
    'MARINILLA': ('Marinilla', 'Antioquia'),
    'SABANETA': ('Sabaneta', 'Antioquia'),
    'LA ESTRELLA': ('La Estrella', 'Antioquia'),
    'COPACABANA': ('Copacabana', 'Antioquia'),
    'LA CEJA': ('La Ceja', 'Antioquia'),
    'COROZAL': ('Corozal', 'Sucre'),
    'ARJONA': ('Arjona', 'Bolívar'),
    'MAICAO': ('Maicao', 'La Guajira'),
    'CIENAGA': ('Ciénaga', 'Magdalena'),
    'FUNDACION': ('Fundación', 'Magdalena'),
    'PLATO': ('Plato', 'Magdalena'),
    'SANTO TOMAS': ('Santo Tomás', 'Atlántico'),
    'MALAMBO': ('Malambo', 'Atlántico'),
    'SABANALARGA': ('Sabanalarga', 'Atlántico'),
    'GALAPA': ('Galapa', 'Atlántico'),
    'LA JAGUA DE IBIRICO': ('La Jagua de Ibirico', 'Cesar'),
    'ORITO': ('Orito', 'Putumayo'),
    'BARRANCAS': ('Barrancas', 'La Guajira'),
    'YACOPI': ('Yacopí', 'Cundinamarca'),
    'LA UNION': ('La Unión', 'Valle del Cauca'),
    'VILLA DE LEYVA': ('Villa de Leyva', 'Boyacá'),
    'GARZON': ('Garzón', 'Huila'),
    'PITALITO': ('Pitalito', 'Huila'),
    'CHAPARRAL': ('Chaparral', 'Tolima'),
    'MELGAR': ('Melgar', 'Tolima'),
    'FLANDES': ('Flandes', 'Tolima'),
    'MARIQUITA': ('Mariquita', 'Tolima'),
    'FRESNO': ('Fresno', 'Tolima'),
    'LIBANO': ('Líbano', 'Tolima'),
    'CALARCA': ('Calarcá', 'Quindío'),
    'CIRCASIA': ('Circasia', 'Quindío'),
    'MONTENEGRO': ('Montenegro', 'Quindío'),
    'LOS PATIOS': ('Los Patios', 'Norte de Santander'),
    'IPIALES': ('Ipiales', 'Nariño'),
    'TUMACO': ('Tumaco', 'Nariño'),
    'TUQUERRES': ('Túquerres', 'Nariño'),
    'BARBOSA': ('Barbosa', 'Antioquia'),
    'SANTA ROSA DE CABAL': ('Santa Rosa de Cabal', 'Risaralda'),
    'GUATICA': ('Guática', 'Risaralda'),
    'QUINCHIA': ('Quinchía', 'Risaralda'),
    'LA CELIA': ('La Celia', 'Risaralda'),
    'MARSELLA': ('Marsella', 'Risaralda'),
    'SANTUARIO': ('Santuario', 'Risaralda'),
    'APIA': ('Apía', 'Risaralda'),
    'BALBOA': ('Balboa', 'Risaralda'),
    'PUEBLO RICO': ('Pueblo Rico', 'Risaralda'),
    'LA VIRGINIA': ('La Virginia', 'Risaralda'),
    'MISTRATO': ('Mistrató', 'Risaralda'),
    'BELEN DE UMBRIA': ('Belén de Umbría', 'Risaralda'),
    'AGUAZUL': ('Aguazul', 'Casanare'),
    'PAZ DE ARIPORO': ('Paz de Ariporo', 'Casanare'),
    'TAURAMENA': ('Tauramena', 'Casanare'),
    'VILLANUEVA': ('Villanueva', 'Casanare'),
    'PUERTO COLOMBIA': ('Puerto Colombia', 'Atlántico'),
    'BARANOA': ('Baranoa', 'Atlántico'),
    'CHINCHINA': ('Chinchiná', 'Caldas'),
    'PUERTO BOYACA': ('Puerto Boyacá', 'Boyacá'),
    'GUAMO': ('Guamo', 'Tolima'),
    'CASTILLA LA NUEVA': ('Castilla la Nueva', 'Meta'),
    'BECERRIL': ('Becerril', 'Cesar'),
    'SITIONUEVO': ('Sitionuevo', 'Magdalena'),
    'ARIGUANI': ('Ariguaní', 'Magdalena'),
    'EL BANCO': ('El Banco', 'Magdalena'),
    'PIVIJAY': ('Pivijay', 'Magdalena'),
    'ZONA BANANERA': ('Zona Bananera', 'Magdalena'),
    'EL RETEN': ('El Retén', 'Magdalena'),
    'CHINU': ('Chinú', 'Córdoba'),
    'SAN PELAYO': ('San Pelayo', 'Córdoba'),
    'CISNEROS': ('Cisneros', 'Antioquia'),
    'EL BAGRE': ('El Bagre', 'Antioquia'),
    'SEGOVIA': ('Segovia', 'Antioquia'),
    'TAMESIS': ('Támesis', 'Antioquia'),
    'TARAZA': ('Tarazá', 'Antioquia'),
    'ZARAGOZA': ('Zaragoza', 'Antioquia'),
    'PUERTO BERRIO': ('Puerto Berrío', 'Antioquia'),
    'CAREPA': ('Carepa', 'Antioquia'),
    'DABEIBA': ('Dabeiba', 'Antioquia'),
    'ITUANGO': ('Ituango', 'Antioquia'),
    'GUARNE': ('Guarne', 'Antioquia'),
    'SONSON': ('Sonsón', 'Antioquia'),
    'URRAO': ('Urrao', 'Antioquia'),
    'YARUMAL': ('Yarumal', 'Antioquia'),
    'SANTA FE DE ANTIOQUIA': ('Santa Fe de Antioquia', 'Antioquia'),
    'CURUMANI': ('Curumaní', 'Cesar'),
    'BOSCONIA': ('Bosconia', 'Cesar'),
    'CODAZZI': ('Agustín Codazzi', 'Cesar'),
    'AGUSTIN CODAZZI': ('Agustín Codazzi', 'Cesar'),
    'SAN ALBERTO': ('San Alberto', 'Cesar'),
    'CHIVOLO': ('Chivoló', 'Magdalena'),
    'ARACATACA': ('Aracataca', 'Magdalena'),
    'CALAMAR': ('Calamar', 'Bolívar'),
    'SAN JACINTO': ('San Jacinto', 'Bolívar'),
    'MARIA LA BAJA': ('María la Baja', 'Bolívar'),
    'MOMPOS': ('Mompox', 'Bolívar'),
    'MOMPOX': ('Mompox', 'Bolívar'),
    'SAN PABLO': ('San Pablo', 'Bolívar'),
    'SIMITI': ('Simití', 'Bolívar'),
    'GARZON': ('Garzón', 'Huila'),
    'LA PLATA': ('La Plata', 'Huila'),
    'CAMPOALEGRE': ('Campoalegre', 'Huila'),
    'LEBRIJA': ('Lebrija', 'Santander'),
    'MALAGA': ('Málaga', 'Santander'),
    'VELEZ': ('Vélez', 'Santander'),
    'BARBOSA SANTANDER': ('Barbosa', 'Santander'),
    'SABANA DE TORRES': ('Sabana de Torres', 'Santander'),
    'PUERTO WILCHES': ('Puerto Wilches', 'Santander'),
    'CIMITARRA': ('Cimitarra', 'Santander'),
    'OIBA': ('Oiba', 'Santander'),
    'CHARALA': ('Charalá', 'Santander'),
    'LOS SANTOS': ('Los Santos', 'Santander'),
    # Common truncations/typos found in second-pass audit
    'ITAG': ('Itagüí', 'Antioquia'),
    'ITAGUI': ('Itagüí', 'Antioquia'),
    'PITAGUI': ('Itagüí', 'Antioquia'),
    'SOCAHA': ('Soacha', 'Cundinamarca'),
    'SOAHA': ('Soacha', 'Cundinamarca'),
    'SOACHA': ('Soacha', 'Cundinamarca'),
    'NELVA': ('Neiva', 'Huila'),
    'NEIBA': ('Neiva', 'Huila'),
    'ACASIAS': ('Acacías', 'Meta'),
    'MONTERA': ('Montería', 'Córdoba'),
    'PEIDECUESTA': ('Piedecuesta', 'Santander'),
    'GIRO': ('Girón', 'Santander'),
    'GIRON': ('Girón', 'Santander'),
    'BARRAANQUILLA': ('Barranquilla', 'Atlántico'),
    'BARRANQU': ('Barranquilla', 'Atlántico'),
    'RANQUILLA': ('Barranquilla', 'Atlántico'),
    'BARRAQUILLA': ('Barranquilla', 'Atlántico'),
    'BARRACABERMEJA': ('Barrancabermeja', 'Santander'),
    'SINCELEJOS': ('Sincelejo', 'Sucre'),
    'SINCEELJO': ('Sincelejo', 'Sucre'),
    'SANTAMARTA': ('Santa Marta', 'Magdalena'),
    'BUCARAMANGA3': ('Bucaramanga', 'Santander'),
    'CARTAGEENA DE INDIAS': ('Cartagena de Indias', 'Bolívar'),
    'CARTAGEENA': ('Cartagena de Indias', 'Bolívar'),
    'VILLAVIVENCIO': ('Villavicencio', 'Meta'),
    'VILPAVICENCIO': ('Villavicencio', 'Meta'),
    'PLORENCIA': ('Florencia', 'Caquetá'),
    'FOLRENCIA': ('Florencia', 'Caquetá'),
    'FLORANCIA': ('Florencia', 'Caquetá'),
    'MANIZALEZ': ('Manizales', 'Caldas'),
    'MANIZALOS': ('Manizales', 'Caldas'),
    'VILLA DE ROSARIO': ('Villa del Rosario', 'Norte de Santander'),
    'VILLA DEL ROSARIO': ('Villa del Rosario', 'Norte de Santander'),
    'AQUAZUL': ('Aguazul', 'Casanare'),
    'AGUZAUL': ('Aguazul', 'Casanare'),
    'AGUAZUI': ('Aguazul', 'Casanare'),
    'USOLEDAD': ('Soledad', 'Atlántico'),
    'BAGUÉ': ('Ibagué', 'Tolima'),
    'BAGUE': ('Ibagué', 'Tolima'),
    'IBAGUE': ('Ibagué', 'Tolima'),
    'TAURAMEN': ('Tauramena', 'Casanare'),
    'VALLADUPAR': ('Valledupar', 'Cesar'),
    'ARIQUANI': ('Ariguaní', 'Magdalena'),
    'DEMOSQUERA': ('Mosquera', 'Cundinamarca'),
    'MODQUERA': ('Mosquera', 'Cundinamarca'),
    'COROZAI': ('Corozal', 'Sucre'),
    'SABANALARGA': ('Sabanalarga', 'Atlántico'),
    'SABANA LARGA': ('Sabanalarga', 'Atlántico'),
    'MEDILLIN': ('Medellín', 'Antioquia'),
    'TURBACAO': ('Turbaco', 'Bolívar'),
    'TOLUVIEJO': ('Tolú Viejo', 'Sucre'),
    'PAIMIRA': ('Palmira', 'Valle del Cauca'),
    'PAZ DE AROPORO': ('Paz de Ariporo', 'Casanare'),
    'JAMUINDI': ('Jamundí', 'Valle del Cauca'),
    'JAMUMI': ('Jamundí', 'Valle del Cauca'),
    'PUERTO COLOMBIS': ('Puerto Colombia', 'Atlántico'),
    'PERERIA': ('Pereira', 'Risaralda'),
    'ROGOTA': ('Bogotá', 'Bogotá, D.C.'),
    'BOGOTA': ('Bogotá', 'Bogotá, D.C.'),
    'CHINQUINA': ('Chinchiná', 'Caldas'),
    'TEBAIDA': ('La Tebaida', 'Quindío'),
    'ESINAL': ('El Espinal', 'Tolima'),
    'ESPINAI': ('El Espinal', 'Tolima'),
    'SN GIL': ('San Gil', 'Santander'),
    'SAN GIL': ('San Gil', 'Santander'),
    'LA DORA': ('La Dorada', 'Caldas'),
    'DOARADA': ('La Dorada', 'Caldas'),
    'COPACABANA': ('Copacabana', 'Antioquia'),
    'COÁPACABANA': ('Copacabana', 'Antioquia'),
    'RIONEGRA': ('Rionegro', 'Antioquia'),
    'YOIPAL': ('Yopal', 'Casanare'),
    'FRIDABLANCA': ('Floridablanca', 'Santander'),
    'SAN NEPOMUCENO': ('San Juan Nepomuceno', 'Bolívar'),
    'NEPOMUCEO': ('San Juan Nepomuceno', 'Bolívar'),
    'JUGUA DE IBIRICO': ('La Jagua de Ibirico', 'Cesar'),
    'LA JUGUA DE IBIRICO': ('La Jagua de Ibirico', 'Cesar'),
    'JAGUA DE SECO': ('La Jagua de Ibirico', 'Cesar'),
    'CARMEN DE BOLIVAR': ('El Carmen de Bolívar', 'Bolívar'),
    'PUEBLOVIEJO': ('Puebloviejo', 'Magdalena'),
    'SABANA DE TORRRES': ('Sabana de Torres', 'Santander'),
    'BARANOAATLANTICO': ('Baranoa', 'Atlántico'),
    'YOMBO': ('Yondó', 'Antioquia'),
    'ZABANETA': ('Sabaneta', 'Antioquia'),
    'DESQUEBRADAS': ('Dosquebradas', 'Risaralda'),
    'CATILLA LA NUEVA': ('Castilla la Nueva', 'Meta'),
    'TANJA': ('Tunja', 'Boyacá'),
    'SAN JERONIMO': ('San Jerónimo', 'Antioquia'),
    'SAN JERÓNIM': ('San Jerónimo', 'Antioquia'),
    'NARIÑPO': ('Nariño', 'Nariño'),
    'JUAN ACOSTA': ('Juan de Acosta', 'Atlántico'),
    'AL CEJA TAMBO': ('La Ceja', 'Antioquia'),
    'CALDAS ANTIOQUIA': ('Caldas', 'Antioquia'),
    'CALDAS': ('Caldas', 'Antioquia'),
}

# Build consolidated municipality lookup
# municipio_norm -> (city_pretty, dept_pretty)
municipio_db = {}

# First, from DANE data (prefer unambiguous)
for city_norm, entries in municipio_raw.items():
    if len(entries) == 1:
        municipio_db[city_norm] = entries[0]
    else:
        # Ambiguous — use resolution table or first entry
        if city_norm in AMBIGUOUS_RESOLUTION:
            municipio_db[city_norm] = AMBIGUOUS_RESOLUTION[city_norm]
        else:
            municipio_db[city_norm] = entries[0]

# Then add aliases (override ambiguous)
for alias, (city, dept) in CITY_ALIASES.items():
    alias_norm = remove_accents(alias.upper())
    municipio_db[alias_norm] = (city, dept)

# Build sorted lists (longest first)
municipio_names_sorted = sorted(municipio_db.keys(), key=len, reverse=True)
departamento_names_sorted = sorted(departamento_db.keys(), key=len, reverse=True)

# Department alias mapping (common alternate names)
DEPT_ALIASES = {
    'NORTE DE SANTANDER': 'Norte de Santander',
    'VALLE DEL CAUCA': 'Valle del Cauca',
    'VALLE': 'Valle del Cauca',
    'SAN ANDRES Y PROVIDENCIA': 'San Andrés y Providencia',
    'SAN ANDRES': 'San Andrés y Providencia',
    'LA GUAJIRA': 'La Guajira',
    'GUAJIRA': 'La Guajira',
    'N DE SANTANDER': 'Norte de Santander',
    'NTE DE SANTANDER': 'Norte de Santander',
    'NTE SANTANDER': 'Norte de Santander',
    # Common department typos from gobernación data
    'BOIVAR': 'Bolívar',
    'SOLIVAR': 'Bolívar',
    'BOLNAR': 'Bolívar',
    'BOLIBAR': 'Bolívar',
    'BOLIVAR': 'Bolívar',
    'BOLIVA': 'Bolívar',
    'DELATLANTICO': 'Atlántico',
    'ALTANTICO': 'Atlántico',
    'ATLANCO': 'Atlántico',
    'ATLÁNTIC': 'Atlántico',
    'SATANDER': 'Santander',
    'CASANARES': 'Casanare',
    'CASANRE': 'Casanare',
    'MADGALENA': 'Magdalena',
    'MAGADALENA': 'Magdalena',
    'HULA': 'Huila',
    'TOLMAGOERNACION': 'Tolima',
    'CLADAS': 'Caldas',
    'CALAS': 'Caldas',
    'PERREIRA': 'Risaralda',
    'PEREI': 'Risaralda',
    'QINDIO': 'Quindío',
    'QUIDIO': 'Quindío',
    'SUERE': 'Sucre',
    'SUCREW': 'Sucre',
    'SANTADER': 'Santander',
    'NORTE DE SANTADER': 'Norte de Santander',
    'NORTE': 'Norte de Santander',
    'CALL': 'Valle del Cauca',
    'CALI': 'Valle del Cauca',
}

print(f"  {len(municipio_db)} municipality entries, {len(departamento_db)} departments")


# ============================================================
# LOCATION EXTRACTION — TWO PASS
# ============================================================

def extract_department(text_na):
    """Extract department name from normalized-accents-removed text."""
    # Try explicit department names (longest first)
    for dept_norm in departamento_names_sorted:
        if re.search(r'\b' + re.escape(dept_norm) + r'\b', text_na):
            return departamento_db[dept_norm]
    # Try aliases
    for alias, dept_pretty in DEPT_ALIASES.items():
        alias_norm = remove_accents(alias.upper())
        if re.search(r'\b' + re.escape(alias_norm) + r'\b', text_na):
            return dept_pretty
    return None


# Names that are BOTH departments and municipalities (in another dept)
# When these appear in text, they're almost always the DEPARTMENT, not the municipality
DEPT_NAME_MUNIS = set()
for dn in departamento_db:
    if dn in municipio_db:
        DEPT_NAME_MUNIS.add(dn)
# Also add common short forms
for alias_norm in [remove_accents(a.upper()) for a in DEPT_ALIASES]:
    if alias_norm in municipio_db:
        DEPT_NAME_MUNIS.add(alias_norm)

def extract_municipality(text_na, dept_hint=None):
    """
    Extract municipality from text. If dept_hint is provided, prefer
    municipalities in that department to resolve ambiguity.
    
    KEY FIX: When a matched word is also a department name (Córdoba, Nariño,
    Boyacá, Caldas, Risaralda, Bolívar...), treat it as department context
    instead of as a municipality match, UNLESS there's strong evidence otherwise.
    """
    found_dept_as_muni = None  # Track if we skip a dept-name match
    
    for muni_norm in municipio_names_sorted:
        if len(muni_norm) < 3:
            continue
        if not re.search(r'\b' + re.escape(muni_norm) + r'\b', text_na):
            continue
            
        city, dept = municipio_db[muni_norm]
        
        # Check if this "municipality" name is actually a department name
        if muni_norm in DEPT_NAME_MUNIS:
            # This is ambiguous — likely the department, not the municipality
            # Save it as fallback department context and keep looking for real municipalities
            if not found_dept_as_muni:
                found_dept_as_muni = departamento_db.get(muni_norm)
            continue
        
        # Normal municipality match
        if dept_hint:
            dept_hint_norm = remove_accents(dept_hint.upper())
            if remove_accents(dept.upper()) == dept_hint_norm:
                return city, dept
            alt_entries = municipio_raw.get(muni_norm, [])
            for alt_city, alt_dept in alt_entries:
                if remove_accents(alt_dept.upper()) == dept_hint_norm:
                    return alt_city, alt_dept
        elif found_dept_as_muni:
            # We have department context from a skipped dept-name match
            # Check if this municipality is in that department
            dept_ctx_norm = remove_accents(found_dept_as_muni.upper())
            if remove_accents(dept.upper()) == dept_ctx_norm:
                return city, dept
            alt_entries = municipio_raw.get(muni_norm, [])
            for alt_city, alt_dept in alt_entries:
                if remove_accents(alt_dept.upper()) == dept_ctx_norm:
                    return alt_city, alt_dept
        
        return city, dept
    
    return None, None


def extract_location_v4(text, entity_type=None):
    """
    Enhanced two-pass location extraction.
    For GOBERNACION: extract department (the location IS the department).
    For others: extract municipality first, then department for context.
    Also handles "GOBERNACION DE [CITY]" which is really erroneous data
    — map city to its department.
    """
    t = clean_text(text)
    t_na = remove_accents(t)

    # Pass 1: Extract department
    dept = extract_department(t_na)

    if entity_type == 'GOBERNACION':
        if dept:
            return None, dept
        # No department found — maybe text says "GOBERNACION DE PEREIRA" 
        # (city instead of dept). Try to find a city and use its department.
        muni, muni_dept = extract_municipality(t_na)
        if muni and muni_dept:
            return None, muni_dept
        return None, None

    # Pass 2: Extract municipality with department context
    muni, muni_dept = extract_municipality(t_na, dept_hint=dept)

    if muni:
        return muni, muni_dept
    elif dept:
        return None, dept
    else:
        return None, None


# ============================================================
# ENTITY TYPE CLASSIFICATION — EXPANDED
# ============================================================

def extract_number(text):
    t = clean_text(text)
    t = remove_accents(t)
    t = replace_ordinals(t)
    nums = re.findall(r'\b(\d+)\b', t)
    return nums[0] if nums else ''


def classify_entity(text):
    """
    Returns (entity_type, sub_type, number, municipio, departamento)
    """
    t = clean_text(text)
    t_na = remove_accents(t)
    num = extract_number(text)

    # ---- Pre-classify to get entity_type for location extraction ----
    entity_type = _detect_type(t_na)
    muni, dept = extract_location_v4(text, entity_type)

    # ---- DATT ----
    if entity_type == 'DATT':
        return ('DATT', 'TRANSITO', '', muni or 'Cartagena de Indias', dept or 'Bolívar')

    # ---- DIAN ----
    if entity_type == 'DIAN':
        return ('DIAN', '', '', muni, dept)

    # ---- SENA ----
    if entity_type == 'SENA':
        return ('SENA', '', '', muni, dept)

    # ---- COLPENSIONES ----
    if entity_type == 'COLPENSIONES':
        return ('COLPENSIONES', '', '', muni, dept)

    # ---- EMCALI ----
    if entity_type == 'EMCALI':
        return ('EMCALI', '', '', muni or 'Cali', dept or 'Valle del Cauca')

    # ---- CORPORACIÓN AUTÓNOMA REGIONAL ----
    if entity_type == 'CAR':
        sub = ''
        if re.search(r'CARDER', t_na): sub = 'CARDER'
        elif re.search(r'CVC', t_na): sub = 'CVC'
        elif re.search(r'CORNARE', t_na): sub = 'CORNARE'
        elif re.search(r'CAS\b', t_na): sub = 'CAS'
        elif re.search(r'CORTOLIMA', t_na): sub = 'CORTOLIMA'
        elif re.search(r'CORPOGUAJIRA', t_na): sub = 'CORPOGUAJIRA'
        elif re.search(r'CORANTIOQUIA', t_na): sub = 'CORANTIOQUIA'
        elif re.search(r'CORPOCESAR', t_na): sub = 'CORPOCESAR'
        elif re.search(r'CORPOAMAZONIA', t_na): sub = 'CORPOAMAZONIA'
        elif re.search(r'CORPOBOYACA', t_na): sub = 'CORPOBOYACA'
        elif re.search(r'CORPOMAG', t_na): sub = 'CORPOMAG'
        elif re.search(r'CDMB', t_na): sub = 'CDMB'
        elif re.search(r'CRA\b', t_na): sub = 'CRA'
        elif re.search(r'CRQ\b', t_na): sub = 'CRQ'
        return ('CAR', sub, '', muni, dept)

    # ---- RAMA JUDICIAL / DISTRITO JUDICIAL ----
    if entity_type == 'RAMA_JUDICIAL':
        return ('RAMA_JUDICIAL', '', '', muni, dept)

    # ---- JUZGADO ----
    if entity_type == 'JUZGADO':
        sub = _classify_juzgado(t_na)
        return ('JUZGADO', sub, num, muni, dept)

    # ---- OFICINA DE APOYO ----
    if entity_type == 'OFICINA_APOYO':
        sub = ''
        if 'CIVIL' in t_na and 'MUNICIPAL' in t_na: sub = 'CIVIL_MUNICIPAL'
        elif 'CIVIL' in t_na: sub = 'CIVIL'
        elif 'LABORAL' in t_na: sub = 'LABORAL'
        return ('OFICINA_APOYO', sub, '', muni, dept)

    # ---- CENTRO DE SERVICIOS JUDICIALES ----
    if entity_type == 'CENTRO_SERVICIOS':
        return ('CENTRO_SERVICIOS', '', '', muni, dept)

    # ---- ALCALDÍA ----
    if entity_type == 'ALCALDIA':
        return ('ALCALDIA', '', '', muni, dept)

    # ---- GOBERNACIÓN ----
    if entity_type == 'GOBERNACION':
        return ('GOBERNACION', '', '', muni, dept)

    # ---- SUPERINTENDENCIA ----
    if entity_type == 'SUPERINTENDENCIA':
        sub = ''
        if 'SOCIEDADES' in t_na: sub = 'SOCIEDADES'
        elif 'INDUSTRIA' in t_na or 'COMERCIO' in t_na: sub = 'INDUSTRIA_COMERCIO'
        elif 'SALUD' in t_na: sub = 'SALUD'
        elif 'FINANCIERA' in t_na: sub = 'FINANCIERA'
        elif 'NOTARIADO' in t_na: sub = 'NOTARIADO'
        elif 'PUERTOS' in t_na or 'TRANSPORTE' in t_na: sub = 'PUERTOS_TRANSPORTE'
        elif 'SUBSIDIO' in t_na: sub = 'SUBSIDIO_FAMILIAR'
        elif 'SERVICIO' in t_na and 'PUBLICO' in t_na: sub = 'SERVICIOS_PUBLICOS'
        elif 'VIGILANCIA' in t_na: sub = 'VIGILANCIA'
        else: sub = 'OTRA'
        return ('SUPERINTENDENCIA', sub, '', muni, dept)

    # ---- SECRETARÍA ----
    if entity_type == 'SECRETARIA':
        sub = ''
        if re.search(r'TRANSITO|TRANSPORTE|MOVILIDAD', t_na): sub = 'TRANSITO'
        elif 'HACIENDA' in t_na: sub = 'HACIENDA'
        elif 'GOBIERNO' in t_na: sub = 'GOBIERNO'
        elif 'EDUCACION' in t_na: sub = 'EDUCACION'
        elif 'SALUD' in t_na: sub = 'SALUD'
        else: sub = 'OTRA'
        return ('SECRETARIA', sub, '', muni, dept)

    # ---- MINISTERIO ----
    if entity_type == 'MINISTERIO':
        sub = ''
        if 'TRABAJO' in t_na: sub = 'TRABAJO'
        elif 'HACIENDA' in t_na: sub = 'HACIENDA'
        elif 'DEFENSA' in t_na: sub = 'DEFENSA'
        elif 'EDUCACION' in t_na: sub = 'EDUCACION'
        elif 'SALUD' in t_na: sub = 'SALUD'
        else:
            idx = t_na.find('MINISTERIO')
            if idx >= 0: sub = t_na[idx+10:].strip()[:25]
        return ('MINISTERIO', sub, '', muni, dept)

    # ---- MUNICIPIO ----
    if entity_type == 'MUNICIPIO':
        return ('MUNICIPIO', '', '', muni, dept)

    # ---- DIRECCIÓN EJECUTIVA ----
    if entity_type == 'DIRECCION_EJECUTIVA':
        return ('DIRECCION_EJECUTIVA', '', '', muni, dept)

    # ---- IDU ----
    if entity_type == 'IDU':
        return ('IDU', '', '', muni, dept)

    # ---- INSTITUTO DE MOVILIDAD ----
    if entity_type == 'INSTITUTO_MOVILIDAD':
        return ('INSTITUTO_MOVILIDAD', '', '', muni, dept)

    # ---- DIRECCIÓN DE TRÁNSITO ----
    if entity_type == 'TRANSITO':
        return ('TRANSITO', '', '', muni, dept)

    # ---- TRIBUNAL ----
    if entity_type == 'TRIBUNAL':
        sub = ''
        if 'ADMINISTRATIVO' in t_na: sub = 'ADMINISTRATIVO'
        elif 'SUPERIOR' in t_na: sub = 'SUPERIOR'
        return ('TRIBUNAL', sub, '', muni, dept)

    # ---- FISCALÍA ----
    if entity_type == 'FISCALIA':
        return ('FISCALIA', '', num, muni, dept)

    # ---- ESP / EMPRESAS PÚBLICAS ----
    if entity_type == 'ESP':
        return ('ESP', '', '', muni, dept)

    # ---- POLICÍA ----
    if entity_type == 'POLICIA':
        return ('POLICIA', '', '', muni, dept)

    # ---- CORTE ----
    if entity_type == 'CORTE':
        sub = ''
        if 'SUPREMA' in t_na: sub = 'SUPREMA'
        elif 'CONSTITUCIONAL' in t_na: sub = 'CONSTITUCIONAL'
        return ('CORTE', sub, '', muni, dept)

    # ---- CONTRALORIA ----
    if entity_type == 'CONTRALORIA':
        return ('CONTRALORIA', '', '', muni, dept)

    # ---- PERSONERIA ----
    if entity_type == 'PERSONERIA':
        return ('PERSONERIA', '', '', muni, dept)

    # ---- UGPP ----
    if entity_type == 'UGPP':
        return ('UGPP', '', '', muni, dept)

    return ('OTRO', '', '', muni, dept)


def _detect_type(t_na):
    """Detect entity type from normalized text."""
    # Order matters — more specific patterns first

    if re.search(r'DATT|DEPARTAMENTO\s+ADMINISTRATIVO\s+DE\s+TRANS', t_na):
        return 'DATT'

    if re.search(r'\bDIAN\b', t_na):
        return 'DIAN'

    if re.search(r'\bSENA\b', t_na) and not re.search(r'SENAMIHI', t_na):
        return 'SENA'

    if re.search(r'COLPENSIONES', t_na):
        return 'COLPENSIONES'

    if re.search(r'EMCALI', t_na):
        return 'EMCALI'

    # CAR before JUZGADO (some CAR text has "JUDICIAL" in it)
    if re.search(r'CORPORACION\s+AUTONOMA\s+REGIONAL|COORPORACION\s+AUTONOMA|CARDER|CORNARE|\bCVC\b|\bCAS\b.*SANTANDER|CORTOLIMA|CORPOGUAJIRA|CORANTIOQUIA|CORPOCESAR|CORPOAMAZONIA|CORPOBOYACA|CORPOMAG|\bCDMB\b', t_na):
        return 'CAR'

    # RAMA JUDICIAL / DISTRITO JUDICIAL before JUZGADO
    if re.search(r'RAMA\s+JUDICIAL|DISTRITO\s+JUDICIAL|CONSEJO\s+SUPERIOR|CONSEJO\s+SECCIONAL', t_na) and not re.search(r'JUZGADO', t_na):
        return 'RAMA_JUDICIAL'

    if re.search(r'JUZGADO|JUZG\b|JUZSADO|JUZG\s|JUEZ\b', t_na):
        return 'JUZGADO'

    if re.search(r'OFICINA\s+DE\s+APOYO', t_na):
        return 'OFICINA_APOYO'

    if re.search(r'CENTRO\s+DE\s+SERVICIOS?\s+JUDICIAL', t_na):
        return 'CENTRO_SERVICIOS'

    if re.search(r'ALCALDI|ALCALDIA', t_na):
        return 'ALCALDIA'

    if re.search(r'GOBERNACI|GOBERNACIÒ|GOBERNACIO\b|GBERANCION|OBERNACION', t_na):
        return 'GOBERNACION'

    if re.search(r'SUPERINTENDENCIA', t_na):
        return 'SUPERINTENDENCIA'

    if re.search(r'SECRETARI', t_na) and not re.search(r'SECRGOBERNACION', t_na):
        return 'SECRETARIA'

    if re.search(r'MINISTERIO|MINHACIENDA', t_na):
        return 'MINISTERIO'

    if re.search(r'\bU\.?G\.?P\.?P\.?\b', t_na):
        return 'UGPP'

    if re.search(r'\bMUNICIPIO\b', t_na):
        return 'MUNICIPIO'

    if re.search(r'DIRECCION\s+EJECUTIVA', t_na):
        return 'DIRECCION_EJECUTIVA'

    if re.search(r'\bIDU\b|INSTITUTO\s+DE\s+DESARROLLO\s+URBANO', t_na):
        return 'IDU'

    if re.search(r'INSTITUTO\s+DE\s+MOVILIDAD', t_na):
        return 'INSTITUTO_MOVILIDAD'

    if re.search(r'DIRECCION\s+DE\s+TRANSITO|SECRETARIA\s+DE\s+TRANSITO|INSTITUTO\s+DE\s+TRANSITO', t_na):
        return 'TRANSITO'

    if re.search(r'TRIBUNAL', t_na):
        return 'TRIBUNAL'

    if re.search(r'FISCALI', t_na):
        return 'FISCALIA'

    if re.search(r'EMPRESAS?\s+PUBLICAS?|E\.?\s*S\.?\s*P\.?\s*$', t_na):
        return 'ESP'

    if re.search(r'POLICI', t_na):
        return 'POLICIA'

    if re.search(r'\bCORTE\b', t_na):
        return 'CORTE'

    if re.search(r'CONTRALORI', t_na):
        return 'CONTRALORIA'

    if re.search(r'PERSONERI', t_na):
        return 'PERSONERIA'

    return 'OTRO'


def _classify_juzgado(t_na):
    """Classify juzgado subtype."""
    if re.search(r'PEQUENAS CAUSAS.*LABORAL|LABORAL.*PEQUENAS CAUSAS', t_na):
        return 'PEQUENAS_CAUSAS_LABORAL'
    if re.search(r'PEQUENAS CAUSAS|COMPETENCIA MULTIPLE', t_na):
        return 'PEQUENAS_CAUSAS'
    if 'PROMISCUO' in t_na:
        if 'FAMILIA' in t_na:
            return 'PROMISCUO_FAMILIA'
        return 'PROMISCUO_MUNICIPAL'
    if 'EJECUCION' in t_na:
        if 'CIVIL' in t_na: return 'EJECUCION_CIVIL'
        if 'PENAL' in t_na: return 'EJECUCION_PENAL'
        return 'EJECUCION'
    if 'FAMILIA' in t_na:
        if 'CIRCUITO' in t_na: return 'FAMILIA_CIRCUITO'
        return 'FAMILIA'
    if 'LABORAL' in t_na:
        if 'CIRCUITO' in t_na: return 'LABORAL_CIRCUITO'
        return 'LABORAL'
    if 'PENAL' in t_na:
        if 'CIRCUITO' in t_na: return 'PENAL_CIRCUITO'
        if 'MUNICIPAL' in t_na: return 'PENAL_MUNICIPAL'
        return 'PENAL'
    if 'ADMINISTRATIVO' in t_na:
        return 'ADMINISTRATIVO'
    if 'CIVIL' in t_na:
        if 'CIRCUITO' in t_na: return 'CIVIL_CIRCUITO'
        if 'MUNICIPAL' in t_na: return 'CIVIL_MUNICIPAL'
        return 'CIVIL'
    if 'MUNICIPAL' in t_na:
        return 'MUNICIPAL'
    return ''


# ============================================================
# LEVENSHTEIN SIMILARITY
# ============================================================

def levenshtein_ratio(s1, s2):
    if not s1 and not s2: return 1.0
    if not s1 or not s2: return 0.0
    len1, len2 = len(s1), len(s2)
    if abs(len1 - len2) / max(len1, len2) > 0.4:
        return 0.0
    if len1 > len2:
        s1, s2 = s2, s1
        len1, len2 = len2, len1
    prev = list(range(len2 + 1))
    for i in range(1, len1 + 1):
        curr = [i] + [0] * len2
        for j in range(1, len2 + 1):
            cost = 0 if s1[i-1] == s2[j-1] else 1
            curr[j] = min(curr[j-1] + 1, prev[j] + 1, prev[j-1] + cost)
        prev = curr
    return 1.0 - (prev[len2] / max(len1, len2))


def make_core_text(text, entity_type=None):
    """
    Strip location info and noise to get the entity's semantic core.
    E.g., "JUZGADO 3 CIVIL MUNICIPAL PEREIRA RISARALDA" → "JUZGADO 3 CIVIL"
    """
    t = clean_text(text)
    t = remove_accents(t)
    t = replace_ordinals(t)

    # Remove municipality/department names
    for muni_norm in municipio_names_sorted:
        t = re.sub(r'\b' + re.escape(muni_norm) + r'\b', '', t)
    for dept_norm in departamento_names_sorted:
        t = re.sub(r'\b' + re.escape(dept_norm) + r'\b', '', t)

    # Remove common filler words
    noise = [
        r'\bDE\b', r'\bDEL\b', r'\bLA\b', r'\bLAS\b', r'\bLOS\b',
        r'\bEL\b', r'\bEN\b', r'\bY\b', r'\bPARA\b', r'\bPOR\b',
        r'\bCON\b', r'\bA\b', r'\bAL\b',
        r'\bD\.?\s*C\.?\b', r'\bDISTRITO\b', r'\bJUDICIAL\b',
        r'\bORALIDAD\b', r'\bSENTENCIAS?\b',
        r'\bCIRCUITO\b', r'\bORAL\b', r'\bSECCIONAL\b', r'\bDISTRITAL\b',
        r'\bMUNICIPAL\b', r'\bTURISTICO\b', r'\bCULTURAL\b',
        r'\bMAYOR\b', r'\bINDIAS\b',
        r'\bPAGINAS?\b', r'\b\d+\s+DE\s+\d+\b',
        r'\bOFICIO\b', r'\bNO\.\b', r'\bNIT\b',
        r'\bREPUBLICA\b', r'\bCOLOMBIA\b', r'\bPODER\b', r'\bPUBLICO\b',
        r'\bNO\s+ENCONTRADA\b', r'\bLEVANTAMIENTO\b', r'\bMEDIDA\b',
        r'\bCAUTELAR\b', r'\bREMITENTE\b', r'\bCERTIFIED\b',
        r'\bBANK\b', r'\bPERDOMO\b', r'\bCANTILLO\b', r'\bMUNOZ\b',
        r'\bTORRES\b', r'\bCAAMANO\b', r'\bMEJIA\b', r'\bURREA\b',
        r'\bGERMAN\b', r'\bMONICA\b', r'\bANDREA\b', r'\bSALGADO\b',
        r'\bDAMARIS\b', r'\bCASTRO\b', r'\bCASTRILLON\b',
        r'\bCATALINA\b', r'\bALVARO\b', r'\bTADEO\b',
        r'\bFUNCIONES\b', r'\bCONTROL\b', r'\bGARANTIAS\b',
        r'\bCONOCIMIENTO\b', r'\bUNICO\b', r'\bUNICA\b',
    ]
    for pattern in noise:
        t = re.sub(pattern, '', t)

    t = re.sub(r'[^A-Z0-9Ñ]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


# ============================================================
# MAIN PROCESSING
# ============================================================

# Step 2: Load raw entities
print("\n[2/7] Loading raw entities from embargos.csv...")
raw_entities = {}
with open('embargos.csv', 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        if len(row) > 12:
            ent = row[12].strip()
            raw_entities[ent] = raw_entities.get(ent, 0) + 1

print(f"  {len(raw_entities):,} unique raw entities")

# Step 3: Classify all entities
print("\n[3/7] Classifying entities with v4 location extraction...")

entity_classifications = {}
for raw_text in raw_entities:
    result = classify_entity(raw_text)
    entity_classifications[raw_text] = result

# Build initial groups
# KEY CHANGE: For GOBERNACION, the grouping key uses DEPARTMENT
# For others, it uses MUNICIPALITY
initial_groups = defaultdict(list)
for raw_text, count in raw_entities.items():
    etype, sub, num, muni, dept = entity_classifications[raw_text]

    if etype == 'GOBERNACION':
        # Group gobernaciones by department
        loc_key = dept or '__NO_DEPT__'
        key = (etype, sub, num, loc_key)
    else:
        # Group others by municipality (or department as fallback)
        if muni:
            loc_key = muni
        elif dept:
            loc_key = f'__DEPT_{dept}__'
        else:
            loc_key = '__NO_LOC__'
        key = (etype, sub, num, loc_key)

    initial_groups[key].append((raw_text, count))

print(f"  {len(initial_groups):,} initial groups")

no_loc = sum(1 for k in initial_groups if '__NO_' in k[3])
print(f"  {no_loc:,} groups without location identification")

# Step 4: Split __NO_LOC__ and __NO_DEPT__ groups
print("\n[4/7] Splitting ambiguous groups using Levenshtein similarity...")

SIMILARITY_THRESHOLDS = {
    'JUZGADO': 0.85,
    'GOBERNACION': 0.90,
    'ALCALDIA': 0.85,
    'OTRO': 0.88,
    'CAR': 0.80,
    'RAMA_JUDICIAL': 0.80,
}
DEFAULT_THRESHOLD = 0.85

final_groups = {}
split_count = 0

for key, variants in initial_groups.items():
    etype = key[0]
    loc_info = key[3]

    if '__NO_' not in loc_info or len(variants) <= 1:
        final_groups[key] = variants
        continue

    threshold = SIMILARITY_THRESHOLDS.get(etype, DEFAULT_THRESHOLD)
    sorted_vars = sorted(variants, key=lambda x: -x[1])
    clusters = []
    core_cache = {}

    for raw_text, count in sorted_vars:
        if raw_text not in core_cache:
            core_cache[raw_text] = make_core_text(raw_text, etype)

    for raw_text, count in sorted_vars:
        core = core_cache[raw_text]
        placed = False

        for cluster in clusters:
            anchor = cluster[0][0]
            anchor_core = core_cache[anchor]

            if not core or not anchor_core:
                ratio = levenshtein_ratio(clean_text(anchor), clean_text(raw_text))
            else:
                ratio = levenshtein_ratio(anchor_core, core)

            if ratio >= threshold:
                cluster.append((raw_text, count))
                placed = True
                break

        if not placed:
            clusters.append([(raw_text, count)])

    for i, cluster in enumerate(clusters):
        new_key = (key[0], key[1], key[2], f'{loc_info}_C{i}')
        final_groups[new_key] = cluster

    if len(clusters) > 1:
        split_count += len(clusters) - 1

print(f"  Split into {split_count} additional clusters")
print(f"  {len(final_groups):,} final groups")

# Step 4.5: Second-pass fuzzy location recovery
print("\n[4.5/7] Second-pass: fuzzy location recovery for orphans...")

def extract_city_portion(text, entity_type):
    """Strip entity-type prefix to isolate the city/department name."""
    t = clean_text(text)
    t_na = remove_accents(t)
    if entity_type == 'ALCALDIA':
        # Strip: ALCALDIA (DISTRITAL|MUNICIPAL|MAYOR) (DE|DEL)
        t_na = re.sub(r'^ALCALDI?A\s*(DISTRITAL|MUNICIPAL|MAYOR)?\s*(DE\s+LA|DEL|DE)?\s*', '', t_na)
    elif entity_type == 'GOBERNACION':
        t_na = re.sub(r'^GOBERNACI[OÒ]N\s*(DEL?\s+DEPARTAMENTO)?\s*(DEL|DE\s+LA|DE)?\s*', '', t_na)
        t_na = re.sub(r'^GBERANCION\s*(DEL|DE)?\s*', '', t_na)
        t_na = re.sub(r'^OBERNACION\s*(DEL|DE)?\s*', '', t_na)
    elif entity_type == 'MUNICIPIO':
        t_na = re.sub(r'^MUNICIPIO\s*(DE|DEL)?\s*', '', t_na)
    # Remove trailing noise
    t_na = re.sub(r'\s+(SECRETARI|DIRECCION|DESPACHO|HACIENDA|PROCESO|RESOLUCION|NIT|OFICIO|GESTION|FECHA).*', '', t_na)
    t_na = re.sub(r'\s*\d+.*$', '', t_na)
    t_na = re.sub(r'[^A-Z0-9Ñ\s]', '', t_na).strip()
    return t_na


def fuzzy_match_municipality(text_fragment):
    """Fuzzy match a text fragment against all known municipalities."""
    if not text_fragment or len(text_fragment) < 3:
        return None, None
    tf = text_fragment.strip()
    if len(tf) < 3:
        return None, None
    best_ratio = 0.0
    best_match = None
    for muni_norm, (city, dept) in municipio_db.items():
        if len(muni_norm) < 3:
            continue
        ratio = levenshtein_ratio(tf, muni_norm)
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = (city, dept)
    if best_ratio >= 0.70:
        return best_match
    return None, None


def fuzzy_match_department(text_fragment):
    """Fuzzy match a text fragment against all known departments."""
    if not text_fragment or len(text_fragment) < 3:
        return None
    tf = text_fragment.strip()
    if len(tf) < 3:
        return None
    best_ratio = 0.0
    best_match = None
    # Check department names
    for dept_norm, dept_pretty in departamento_db.items():
        ratio = levenshtein_ratio(tf, dept_norm)
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = dept_pretty
    # Check aliases
    for alias, dept_pretty in DEPT_ALIASES.items():
        alias_norm = remove_accents(alias.upper())
        ratio = levenshtein_ratio(tf, alias_norm)
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = dept_pretty
    if best_ratio >= 0.70:
        return best_match
    return None


# GARBAGE words — if the city portion is one of these, it's not a real location
GARBAGE_WORDS = {
    'CERTIFIED', 'SOCIAL', 'GFIC', 'GIANGEROMETTA', 'PROCESO', 'SANTOS',
    'TESORERIA', 'PUERTO', 'DEPARTAMENTO', 'GEST', 'TRÁNSITO', 'TRANSITO',
    'MOVILIDAD', 'CONSTRUYENDO', 'CIUDAD', 'ERTIFIE', 'CER',
    'SECRETARIA', 'HACIENDA', 'JAVIER', 'VERDE', 'CAKE', 'ERINTED',
    'TRD', 'RADICADO', 'VERSION', 'NEGRO', 'ALCALDE', 'ALCALDIAS',
    'GRON', 'ST', 'ES', 'RE', 'MP', 'VI', 'IBA', 'BARRA', 'BLOS',
    'MON', 'SAN', 'VILLA', 'MUNICIPIO', 'YOVANNY', 'CRISTO', 'RETEN',
    'GESTION', 'VENTANILLA', 'ZONA', 'DIGDDD', 'ATTO', 'KAPPALJON',
    'ESPINBAL', 'EDUMAS', 'PRESIDENCIA', 'PAZ', 'ESTOS', 'AUDER',
    'GENTE', 'NIT', 'MIT', 'DIRECCIONAMIENTO', 'CALL', 'SU',
    'O 6', 'POSTAL MUNICIPIO',
}

# Index existing located groups: (type, sub, num, muni) -> group_key 
# and (type, sub, num, dept) -> group_key for GOBERNACION
located_index_muni = {}
located_index_dept = {}
for key, variants in final_groups.items():
    etype, sub, num, loc_info = key
    if '__NO_' in loc_info:
        continue
    if etype == 'GOBERNACION':
        located_index_dept[(etype, sub, num, loc_info)] = key
    else:
        located_index_muni[(etype, sub, num, loc_info)] = key

# For merge: also index by (type, sub, num, dept_prefix)
located_index_dept_prefix = {}
for key, variants in final_groups.items():
    etype, sub, num, loc_info = key
    if '__NO_' not in loc_info and loc_info.startswith('__DEPT_'):
        dept_name = loc_info[7:-2]
        located_index_dept_prefix[(etype, sub, num, dept_name)] = key

recovered = 0
merged = 0
no_loc_groups = [k for k in list(final_groups.keys()) if '__NO_' in k[3]]

for key in no_loc_groups:
    etype, sub, num, loc_info = key
    variants = final_groups[key]
    primary = max(variants, key=lambda x: x[1])[0]
    
    if etype == 'GOBERNACION':
        city_part = extract_city_portion(primary, etype)
        if city_part in GARBAGE_WORDS or len(city_part) < 3:
            continue
        dept = fuzzy_match_department(city_part)
        if not dept:
            # Try as city → department mapping
            muni, muni_dept = fuzzy_match_municipality(city_part)
            if muni and muni_dept:
                dept = muni_dept
        if dept:
            # Try merge into existing group
            target = located_index_dept.get((etype, sub, num, dept))
            if target:
                final_groups[target].extend(variants)
                del final_groups[key]
                merged += 1
            else:
                # Create new group with recovered location
                new_key = (etype, sub, num, dept)
                final_groups[new_key] = variants
                del final_groups[key]
                located_index_dept[new_key] = new_key
                recovered += 1
                
    elif etype in ('ALCALDIA', 'MUNICIPIO'):
        city_part = extract_city_portion(primary, etype)
        if city_part in GARBAGE_WORDS or len(city_part) < 3:
            continue
        muni, dept = fuzzy_match_municipality(city_part)
        if muni:
            # Try merge into existing group
            target = located_index_muni.get((etype, sub, num, muni))
            if target:
                final_groups[target].extend(variants)
                del final_groups[key]
                merged += 1
            else:
                new_key = (etype, sub, num, muni)
                final_groups[new_key] = variants
                del final_groups[key]
                located_index_muni[new_key] = new_key
                recovered += 1
                # Update classification cache for these variants
                for raw_text, _ in variants:
                    et, st, n, _, _ = entity_classifications[raw_text]
                    entity_classifications[raw_text] = (et, st, n, muni, dept)
                    
    elif etype in ('SECRETARIA', 'CONTRALORIA', 'PERSONERIA', 'TRANSITO', 'ESP', 'TRIBUNAL'):
        # These may also have city names we can recover
        city_part = extract_city_portion(primary, 'ALCALDIA')  # reuse alcaldia extraction
        if city_part in GARBAGE_WORDS or len(city_part) < 3:
            continue
        muni, dept = fuzzy_match_municipality(city_part)
        if muni:
            target = located_index_muni.get((etype, sub, num, muni))
            if target:
                final_groups[target].extend(variants)
                del final_groups[key]
                merged += 1
            else:
                new_key = (etype, sub, num, muni)
                final_groups[new_key] = variants
                del final_groups[key]
                located_index_muni[new_key] = new_key
                recovered += 1
                for raw_text, _ in variants:
                    et, st, n, _, _ = entity_classifications[raw_text]
                    entity_classifications[raw_text] = (et, st, n, muni, dept)

# Update classifications for recovered entities
for key, variants in final_groups.items():
    etype, sub, num, loc_info = key
    if etype == 'GOBERNACION' and '__NO_' not in loc_info:
        for raw_text, _ in variants:
            et, st, n, m, d = entity_classifications[raw_text]
            if not d:
                entity_classifications[raw_text] = (et, st, n, m, loc_info)

print(f"  Recovered location for {recovered} groups")
print(f"  Merged {merged} groups into existing located groups")
print(f"  {len(final_groups):,} groups after second pass")

# Step 5: Assign IDs and canonical names
print("\n[5/7] Assigning IDs and canonical names...")


def choose_canonical(variants):
    sorted_v = sorted(variants, key=lambda x: -x[1])
    best = sorted_v[0][0]
    top_count = sorted_v[0][1]
    threshold = max(top_count * 0.2, 2)
    for v, c in sorted_v:
        if c >= threshold and not v.isupper() and not v.islower():
            best = v
            break
    return best.strip()


entities_final = []
variant_to_id = {}
entity_id = 1

sorted_groups = sorted(final_groups.items(), key=lambda x: -sum(c for _, c in x[1]))

for key, variants in sorted_groups:
    etype, sub, num, loc_info = key
    canonical = choose_canonical(variants)
    total = sum(c for _, c in variants)

    # Determine location from the primary classified variant
    primary = max(variants, key=lambda x: x[1])[0]
    _, _, _, muni, dept = entity_classifications[primary]

    entities_final.append({
        'id': entity_id,
        'canonical': canonical,
        'variants': variants,
        'total': total,
        'tipo': etype,
        'subtipo': sub,
        'numero': num,
        'municipio': muni or '',
        'departamento': dept or '',
    })

    for raw_text, count in variants:
        variant_to_id[raw_text] = entity_id

    entity_id += 1

print(f"  {len(entities_final):,} normalized entities created")

# Step 6: Write outputs
print("\n[6/7] Writing output files...")

with open('entidades.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'entidad_id', 'nombre_normalizado', 'tipo', 'subtipo',
        'municipio', 'departamento', 'total_registros', 'num_variantes'
    ])
    for e in entities_final:
        writer.writerow([
            e['id'], e['canonical'], e['tipo'], e['subtipo'],
            e['municipio'], e['departamento'], e['total'], len(e['variants'])
        ])
print("  ✓ entidades.csv")

with open('variantes_entidades.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['entidad_id', 'nombre_normalizado', 'variante_original', 'conteo'])
    for e in entities_final:
        for raw_text, count in sorted(e['variants'], key=lambda x: -x[1]):
            writer.writerow([e['id'], e['canonical'], raw_text, count])
print("  ✓ variantes_entidades.csv")

muni_set = set()
for e in entities_final:
    if e['municipio']:
        muni_set.add((e['municipio'], e['departamento']))

with open('municipios.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['municipio_id', 'nombre_municipio', 'departamento'])
    for mid, (muni, dept) in enumerate(sorted(muni_set), 1):
        writer.writerow([mid, muni, dept])
print(f"  ✓ municipios.csv ({len(muni_set)} municipios)")

# embargos_limpios.csv
print("  Generating embargos_limpios.csv...")
id_to_canonical = {e['id']: e['canonical'] for e in entities_final}

demandados = defaultdict(list)
with open('demandado.csv', 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        if len(row) >= 18:
            emb_id = row[15].strip()
            if emb_id:
                demandados[emb_id].append({
                    'nombre': row[12].strip().strip(',').strip(),
                    'identificacion': row[7].strip(),
                    'tipo_id': row[17].strip(),
                    'monto_a_embargar': row[10].strip(),
                })

print(f"    {len(demandados):,} embargos with demandado data")

rows = 0
with open('embargos_limpios.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'embargo_id', 'entidad_remitente_id', 'entidad_bancaria_id',
        'estado_embargo', 'numero_oficio', 'fecha_oficio', 'fecha_recepcion',
        'titulo_embargo', 'titulo_orden', 'monto', 'monto_a_embargar',
        'nombre_demandado', 'id_demandado', 'tipo_id_demandado',
        'nombre_demandante', 'id_demandante', 'tipo_id_demandante',
        'nombre_remitente', 'direccion_remitente', 'correo_remitente',
        'nombre_personal_remitente',
    ])

    with open('embargos.csv', 'r', encoding='utf-8', errors='replace') as ef:
        reader = csv.reader(ef)
        next(reader)
        for row in reader:
            if len(row) < 36:
                continue
            emb_id = row[0].strip()
            ent_raw = row[12].strip()
            ent_id = variant_to_id.get(ent_raw, '')

            ebi = row[35].strip()
            try:
                ebi = str(int(ebi))
            except (ValueError, TypeError):
                ebi = ''

            correo = row[4].strip()
            if correo.lower() in ('noencontrada', 'no encontrada', 'no encontrado', ''):
                correo = ''
            direccion = row[9].strip()
            if direccion.lower() in ('no encontrada', 'no encontrado', ''):
                direccion = ''

            funcionario = row[17].strip()
            nombre_remitente = id_to_canonical.get(ent_id, ent_raw)

            dem_list = demandados.get(emb_id, [])
            if dem_list:
                dem = dem_list[0]
                nombre_dem = dem['nombre']
                id_dem = dem['identificacion']
                tipo_dem = dem['tipo_id']
                monto_emb = dem['monto_a_embargar']
            else:
                nombre_dem = id_dem = tipo_dem = monto_emb = ''

            writer.writerow([
                emb_id, ent_id, ebi, row[13].strip(), row[22].strip(),
                row[15].strip(), row[16].strip(), row[30].strip(), row[29].strip(),
                row[20].strip(), monto_emb, nombre_dem, id_dem, tipo_dem,
                '', '', '', nombre_remitente, direccion, correo, funcionario,
            ])
            rows += 1
            if rows % 200000 == 0:
                print(f"    ...{rows:,} rows")

print(f"  ✓ embargos_limpios.csv ({rows:,} rows)")

# Step 7: Summary
print("\n" + "=" * 70)
print("FINAL SUMMARY v4")
print("=" * 70)
print(f"  Raw unique strings:     {len(raw_entities):>10,}")
print(f"  Normalized entities:    {len(entities_final):>10,}")
print(f"  Merge ratio:            {(1 - len(entities_final)/len(raw_entities))*100:.1f}%")
print(f"  Embargos processed:     {rows:>10,}")
print(f"  Municipalities found:   {len(muni_set):>10,}")

# Verify: Gobernaciones
print("\n--- GOBERNACIONES (all) ---")
gob_count = 0
for e in entities_final:
    if e['tipo'] == 'GOBERNACION':
        loc = f"[dept={e['departamento']}]" if e['departamento'] else '[NO DEPT]'
        print(f"  ID {e['id']:>5}: {e['total']:>8,} recs, {len(e['variants']):>4} vars | {e['canonical']} {loc}")
        gob_count += 1
        if gob_count > 40:
            print("  ... (truncated)")
            break

# Verify: Promiscuo in Risaralda
print("\n--- JUZGADO PROMISCUO MUNICIPAL (Risaralda department) ---")
for e in entities_final:
    if e['tipo'] == 'JUZGADO' and e['subtipo'] == 'PROMISCUO_MUNICIPAL' and e['departamento'] == 'Risaralda':
        loc = f"[{e['municipio'] or 'NO MUNI'}, {e['departamento']}]"
        print(f"  ID {e['id']:>5}: {e['total']:>6,} recs, {len(e['variants']):>3} vars | {e['canonical']} {loc}")

# Verify: Alcaldías Mosquera and Florencia
print("\n--- ALCALDIA Mosquera/Florencia ---")
for e in entities_final:
    if e['tipo'] == 'ALCALDIA' and e['municipio'] in ('Mosquera', 'Florencia'):
        print(f"  ID {e['id']:>5}: {e['total']:>6,} recs | {e['canonical']} [{e['municipio']}, {e['departamento']}]")
