"""
04_extraer_entidades.py
Extracción de entidades de los cuerpos completos:
- Localidades (regiones, provincias, distritos)
- Cultivos y productos agrícolas
- Cifras de afectación (hectáreas, muertos, damnificados, viviendas)
- Instituciones
"""

import json
import re
import pandas as pd
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime

RAW_DIR = Path("data/raw/cuerpos")
STAGING_DIR = Path("data/staging")
OUTPUT_DIR = Path("outputs/tables")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FILE = RAW_DIR / "cuerpos_descargados.jsonl"

print("=" * 70)
print("  EXTRACCIÓN DE ENTIDADES DESDE CUERPOS COMPLETOS")
print("=" * 70)

# ── Cargar cuerpos ─────────────────────────────────────────────
articles = []
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    for line in f:
        articles.append(json.loads(line))

print(f"Artículos cargados: {len(articles)}")

# ── DICCIONARIOS DE EXTRACCIÓN ────────────────────────────────

# Regiones y capitales
regiones_peru = {
    "Amazonas": ["Chachapoyas", "Bagua", "Bongará", "Condorcanqui", "Luya", "Rodríguez de Mendoza", "Utcubamba"],
    "Áncash": ["Huaraz", "Aija", "Antonio Raymondi", "Asunción", "Bolognesi", "Carhuaz", "Carlos Fermín Fitzcarrald", "Casma", "Corongo", "Huari", "Huarmey", "Huaylas", "Mariscal Luzuriaga", "Ocros", "Pallasca", "Pomabamba", "Recuay", "Santa", "Sihuas", "Yungay"],
    "Apurímac": ["Abancay", "Andahuaylas", "Antabamba", "Aymaraes", "Chincheros", "Cotabambas", "Grau"],
    "Arequipa": ["Arequipa", "Camaná", "Caravelí", "Castilla", "Caylloma", "Condesuyos", "Islay", "La Unión"],
    "Ayacucho": ["Huamanga", "Cangallo", "Huanca Sancos", "Huanta", "La Mar", "Lucanas", "Parinacochas", "Páucar del Sara Sara", "Sucre", "Víctor Fajardo", "Vilcas Huamán"],
    "Cajamarca": ["Cajamarca", "Cajabamba", "Celendín", "Chota", "Contumazá", "Cutervo", "Hualgayoc", "Jaén", "San Ignacio", "San Marcos", "San Miguel", "San Pablo", "Santa Cruz"],
    "Cusco": ["Cusco", "Acomayo", "Anta", "Calca", "Canas", "Canchis", "Chumbivilcas", "Espinar", "La Convención", "Paruro", "Paucartambo", "Quispicanchi", "Urubamba", "Santa Teresa", "Quillabamba"],
    "Huancavelica": ["Huancavelica", "Acobamba", "Angaraes", "Castrovirreyna", "Churcampa", "Huaytará", "Tayacaja"],
    "Huánuco": ["Huánuco", "Ambo", "Dos de Mayo", "Huacaybamba", "Huamalíes", "Leoncio Prado", "Marañón", "Pachitea", "Puerto Inca", "Lauricocha", "Yarowilca"],
    "Ica": ["Ica", "Chincha", "Nazca", "Palpa", "Pisco"],
    "Junín": ["Huancayo", "Chanchamayo", "Chupaca", "Concepción", "Jauja", "Junín", "Satipo", "Tarma", "Yauli"],
    "La Libertad": ["Trujillo", "Ascope", "Bolívar", "Chepén", "Julcán", "Otuzco", "Pacasmayo", "Pataz", "Sánchez Carrión", "Santiago de Chuco", "Gran Chimú", "Virú"],
    "Lambayeque": ["Chiclayo", "Ferreñafe", "Lambayeque"],
    "Lima": ["Lima", "Barranca", "Cajatambo", "Canta", "Cañete", "Huaral", "Huarochirí", "Huaura", "Oyón", "Yauyos"],
    "Loreto": ["Iquitos", "Alto Amazonas", "Loreto", "Mariscal Ramón Castilla", "Maynas", "Requena", "Ucayali", "Datem del Marañón", "Putumayo"],
    "Madre de Dios": ["Puerto Maldonado", "Manu", "Tahuamanu", "Tambopata"],
    "Moquegua": ["Moquegua", "General Sánchez Cerro", "Ilo", "Mariscal Nieto"],
    "Pasco": ["Cerro de Pasco", "Daniel Alcides Carrión", "Oxapampa", "Pasco"],
    "Piura": ["Piura", "Ayabaca", "Huancabamba", "Morropón", "Paita", "Sechura", "Sullana", "Talara"],
    "Puno": ["Puno", "Azángaro", "Carabaya", "Chucuito", "El Collao", "Huancané", "Lampa", "Melgar", "Moho", "San Antonio de Putina", "San Román", "Sandia", "Yunguyo", "Juliaca", "Ayaviri", "Ilave", "Juli"],
    "San Martín": ["Moyobamba", "Bellavista", "El Dorado", "Huallaga", "Lamas", "Mariscal Cáceres", "Picota", "Rioja", "San Martín", "Tocache", "Tarapoto"],
    "Tacna": ["Tacna", "Candarave", "Jorge Basadre", "Tarata"],
    "Tumbes": ["Tumbes", "Contralmirante Villar", "Zarumilla"],
    "Ucayali": ["Pucallpa", "Atalaya", "Coronel Portillo", "Padre Abad", "Purús"],
}

# Cultivos y productos agrícolas
cultivos = {
    "papa": r"\bpapa[s]?\b",
    "maíz": r"\bma[ií]z\b",
    "quinua": r"\bquinua\b|\bquinoa\b",
    "arroz": r"\barroz\b",
    "trigo": r"\btrigo\b",
    "cebada": r"\bcebada\b",
    "haba": r"\bhaba[s]?\b",
    "oca": r"\boca[s]?\b(?!\s+de\s+lobo)",
    "olluco": r"\bolluco[s]?\b",
    "café": r"\bcaf[eé]\b",
    "cacao": r"\bcacao\b",
    "caña de azúcar": r"\bca[ñn]a\s+de\s+az[uú]car\b",
    "algodón": r"\balgod[oó]n\b",
    "espárrago": r"\besp[aá]rrago[s]?\b",
    "uva": r"\buva[s]?\b",
    "palta": r"\bpalta[s]?\b|\baguacate[s]?\b",
    "mango": r"\bmango[s]?\b",
    "arándano": r"\bar[aá]ndano[s]?\b",
    "cebolla": r"\bcebolla[s]?\b",
    "ajo": r"\bajo[s]?\b(?!\s+de)",
    "alpaca": r"\balpaca[s]?\b",
    "ovino": r"\bovino[s]?\b|\boveja[s]?\b",
    "vacuno": r"\bvacuno[s]?\b|\bvaca[s]?\b|\bganado\s+vacuno\b",
    "llama": r"\bllama[s]?\b(?!\s+(la|el|al|a\s+la|de))",
    "vicuña": r"\bvicu[ñn]a[s]?\b",
}

# Patrones de cifras de afectación
cifras_patrones = {
    "hectareas_afectadas": [
        r"([\d.,]+)\s*(?:mil\s+)?hect[aá]reas?\s+(?:afectad|da[ñn]ad|destruid|perdid|arrasad)",
        r"afect[oóa]\s+(?:a\s+)?([\d.,]+)\s*(?:mil\s+)?hect[aá]reas?",
        r"(?:pérdida|da[ñn]o)\s+(?:de\s+)?([\d.,]+)\s*(?:mil\s+)?hect[aá]reas?",
    ],
    "fallecidos": [
        r"([\d.,]+)\s+(?:muerto|fallecid|v[ií]ctima|persona).{0,20}(?:muert|fallec)",
        r"(?:muert|fallec).{0,20}([\d.,]+)\s+persona",
        r"(?:cobr[oó]|dej[oó]|caus[oó]).{0,20}([\d.,]+)\s+(?:muerto|fallecid|v[ií]ctima)",
        r"(\d+)\s+muerto[s]?",
        r"(\d+)\s+fallecido[s]?",
    ],
    "damnificados": [
        r"([\d.,]+)\s*(?:mil\s+)?(?:damnificad|afectad)",
        r"(?:damnific|afect).{0,15}([\d.,]+)\s*(?:mil\s+)?(?:persona|familia|habitante)",
    ],
    "viviendas": [
        r"([\d.,]+)\s*(?:mil\s+)?vivienda[s]?\s+(?:afectad|da[ñn]ad|destruid|colapsad|derrumbad|inundad)",
        r"(?:afect|da[ñn]|destru|colaps).{0,15}([\d.,]+)\s*(?:mil\s+)?vivienda",
    ],
    "familias": [
        r"([\d.,]+)\s*(?:mil\s+)?familia[s]?\s+(?:afectad|damnificad)",
    ],
}

# ── EXTRACCIÓN ────────────────────────────────────────────────
print("\nExtrayendo entidades...")

resultados = []

for i, art in enumerate(articles):
    texto = (art.get('title', '') + ' ' + art.get('cuerpo', '')).lower()
    texto_original = art.get('title', '') + ' ' + art.get('cuerpo', '')
    
    registro = {
        'url': art['url'],
        'domain': art['domain'],
        'fecha': art['fecha'],
        'title': art.get('title', ''),
        'temas': art.get('temas', []),
    }
    
    # 1. Detectar regiones y provincias
    regiones_encontradas = []
    provincias_encontradas = []
    
    for region, provincias in regiones_peru.items():
        # Buscar región
        patron_reg = re.compile(r'\b' + re.escape(region.lower()) + r'\b')
        if patron_reg.search(texto):
            regiones_encontradas.append(region)
        
        # Buscar provincias/distritos
        for prov in provincias:
            if len(prov) > 3:  # evitar matches cortos
                patron_prov = re.compile(r'\b' + re.escape(prov.lower()) + r'\b')
                if patron_prov.search(texto):
                    provincias_encontradas.append(f"{prov} ({region})")
    
    registro['regiones'] = list(set(regiones_encontradas))
    registro['provincias'] = list(set(provincias_encontradas))
    
    # 2. Detectar cultivos
    cultivos_encontrados = []
    for cultivo, patron in cultivos.items():
        if re.search(patron, texto):
            cultivos_encontrados.append(cultivo)
    registro['cultivos'] = cultivos_encontrados
    
    # 3. Extraer cifras
    for tipo_cifra, patrones in cifras_patrones.items():
        valores = []
        for patron in patrones:
            matches = re.findall(patron, texto, re.IGNORECASE)
            for m in matches:
                try:
                    num = m.replace(',', '').replace('.', '')
                    val = int(num)
                    if val > 0 and val < 10000000:
                        valores.append(val)
                except:
                    pass
        registro[tipo_cifra] = max(valores) if valores else None
    
    resultados.append(registro)
    
    if (i + 1) % 500 == 0:
        print(f"  Procesados: {i+1}/{len(articles)}")

df_ent = pd.DataFrame(resultados)
print(f"\nExtracción completada: {len(df_ent)} artículos procesados")

# ── ESTADÍSTICAS ──────────────────────────────────────────────
print("\n" + "=" * 70)
print("ESTADÍSTICAS DE EXTRACCIÓN")
print("=" * 70)

# Regiones
print("\n── Regiones mencionadas ──")
all_regiones = Counter()
for regs in df_ent['regiones']:
    for r in regs:
        all_regiones[r] += 1
for reg, n in all_regiones.most_common(25):
    bar = "█" * (n // 20)
    print(f"  {reg:20s} {n:5d} {bar}")

# Provincias/distritos
print("\n── Top 30 provincias/distritos mencionados ──")
all_prov = Counter()
for provs in df_ent['provincias']:
    for p in provs:
        all_prov[p] += 1
for prov, n in all_prov.most_common(30):
    print(f"  {prov:40s} {n:5d}")

# Cultivos
print("\n── Cultivos/productos mencionados ──")
all_cultivos = Counter()
for cults in df_ent['cultivos']:
    for c in cults:
        all_cultivos[c] += 1
for cult, n in all_cultivos.most_common():
    if n > 5:
        bar = "█" * (n // 10)
        print(f"  {cult:20s} {n:5d} {bar}")

# Cifras de afectación
print("\n── Artículos con cifras de afectación ──")
for tipo in ['hectareas_afectadas', 'fallecidos', 'damnificados', 'viviendas', 'familias']:
    n = df_ent[tipo].notna().sum()
    if n > 0:
        vals = df_ent[tipo].dropna()
        print(f"  {tipo:25s} {n:5d} artículos | mediana: {vals.median():.0f} | máx: {vals.max():.0f}")

# Artículos con al menos una entidad
tiene_region = df_ent['regiones'].apply(len) > 0
tiene_provincia = df_ent['provincias'].apply(len) > 0
tiene_cultivo = df_ent['cultivos'].apply(len) > 0
tiene_cifra = df_ent[['hectareas_afectadas', 'fallecidos', 'damnificados', 'viviendas', 'familias']].notna().any(axis=1)

print(f"\n── Cobertura de extracción ──")
print(f"  Con región:     {tiene_region.sum():,} ({tiene_region.mean()*100:.1f}%)")
print(f"  Con provincia:  {tiene_provincia.sum():,} ({tiene_provincia.mean()*100:.1f}%)")
print(f"  Con cultivo:    {tiene_cultivo.sum():,} ({tiene_cultivo.mean()*100:.1f}%)")
print(f"  Con cifra:      {tiene_cifra.sum():,} ({tiene_cifra.mean()*100:.1f}%)")

# ── Cruce: regiones × temas ──────────────────────────────────
print("\n── Top combinaciones región × tema ──")
combos = Counter()
for _, row in df_ent.iterrows():
    for reg in row['regiones']:
        for tema in row['temas']:
            combos[(reg, tema)] += 1
for (reg, tema), n in combos.most_common(20):
    print(f"  {reg:15s} × {tema:30s} {n:4d}")

# ── Cruce: cultivos × temas ──────────────────────────────────
print("\n── Top combinaciones cultivo × tema ──")
combos_cult = Counter()
for _, row in df_ent.iterrows():
    for cult in row['cultivos']:
        for tema in row['temas']:
            combos_cult[(cult, tema)] += 1
for (cult, tema), n in combos_cult.most_common(15):
    print(f"  {cult:15s} × {tema:30s} {n:4d}")

# ── GUARDAR ───────────────────────────────────────────────────
print("\n" + "=" * 70)
print("GUARDANDO RESULTADOS")
print("=" * 70)

# Convertir listas a strings para CSV
df_export = df_ent.copy()
df_export['regiones'] = df_export['regiones'].apply(lambda x: '|'.join(x))
df_export['provincias'] = df_export['provincias'].apply(lambda x: '|'.join(x))
df_export['cultivos'] = df_export['cultivos'].apply(lambda x: '|'.join(x))
df_export['temas'] = df_export['temas'].apply(lambda x: '|'.join(x))

# CSV para revisión
csv_path = OUTPUT_DIR / "entidades_extraidas.csv"
df_export.to_csv(csv_path, index=False, encoding="utf-8-sig")
print(f"Guardado: {csv_path}")

# Parquet para análisis
parquet_path = STAGING_DIR / "entidades_extraidas.parquet"
df_ent.to_parquet(parquet_path, index=False)
print(f"Guardado: {parquet_path}")

# JSON resumen
resumen = {
    "total_articulos": len(df_ent),
    "con_region": int(tiene_region.sum()),
    "con_provincia": int(tiene_provincia.sum()),
    "con_cultivo": int(tiene_cultivo.sum()),
    "con_cifra": int(tiene_cifra.sum()),
    "top_regiones": dict(all_regiones.most_common(10)),
    "top_provincias": dict(all_prov.most_common(10)),
    "top_cultivos": dict(all_cultivos.most_common(10)),
}
resumen_path = OUTPUT_DIR / "resumen_entidades.json"
with open(resumen_path, "w", encoding="utf-8") as f:
    json.dump(resumen, f, ensure_ascii=False, indent=2)
print(f"Guardado: {resumen_path}")

print("\nScript completado.")
