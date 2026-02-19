"""
00b_exploracion_profunda.py
Exploración profunda de GDELT fuera de red CGIAR.
Objetivo: mapear qué términos devuelven más resultados,
qué fuentes cubren Perú, qué idiomas, y qué profundidad histórica hay.
"""

import requests
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
PAUSE = 8  # segundos entre requests
LOG_DIR = Path("data/logs")
RAW_DIR = Path("data/raw")

def gdelt_search(query, start, end, max_records=250):
    """Búsqueda de artículos en GDELT."""
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": max_records,
        "startdatetime": start.strftime("%Y%m%d%H%M%S"),
        "enddatetime": end.strftime("%Y%m%d%H%M%S"),
    }
    for attempt in range(3):
        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
            if r.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"    429 — esperando {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            return data.get("articles", [])
        except Exception as e:
            if attempt < 2:
                time.sleep(15)
            else:
                print(f"    Error: {str(e)[:80]}")
    return []

def gdelt_timeline(query, start, end):
    """Timeline de volumen de cobertura."""
    params = {
        "query": query,
        "mode": "timelinevol",
        "format": "json",
        "startdatetime": start.strftime("%Y%m%d%H%M%S"),
        "enddatetime": end.strftime("%Y%m%d%H%M%S"),
    }
    for attempt in range(3):
        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
            if r.status_code == 429:
                time.sleep(30 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            if "timeline" in data and data["timeline"]:
                return data["timeline"][0].get("data", [])
            return []
        except:
            time.sleep(15)
    return []

# ════════════════════════════════════════════════════════════════
print("=" * 70)
print("  EXPLORACIÓN PROFUNDA — GDELT + CLIMA PERÚ")
print("  Fecha:", datetime.now().strftime("%Y-%m-%d %H:%M"))
print("=" * 70)

end = datetime.today()

# ── BLOQUE 1: MAPEO DE TÉRMINOS ──────────────────────────────
# Probamos TODOS los términos relevantes para encontrar
# cuáles devuelven más resultados
print("\n" + "=" * 70)
print("BLOQUE 1: MAPEO EXHAUSTIVO DE TÉRMINOS (último año)")
print("=" * 70)

start_1y = end - timedelta(days=365)

# Organizados por categoría
categorias = {
    "Eventos hidrometeorológicos": [
        "flood peru",
        "flooding peru",
        "drought peru",
        "landslide peru",
        "mudslide peru",
        "hailstorm peru",
        "frost peru",
        "cold wave peru",
        "heavy rain peru",
        "avalanche peru",
    ],
    "El Niño y variabilidad": [
        "El Nino peru",
        "La Nina peru",
        "ENSO peru",
        "climate change peru",
        "global warming peru",
    ],
    "Impacto agrícola": [
        "crop loss peru",
        "crop damage peru",
        "agriculture peru disaster",
        "food security peru",
        "potato peru frost",
        "quinoa peru climate",
        "livestock peru cold",
        "harvest peru",
    ],
    "Institucional y emergencias": [
        "peru disaster emergency",
        "peru emergency declaration",
        "INDECI peru",
        "SENAMHI peru",
        "humanitarian aid peru",
        "peru disaster relief",
        "peru evacuations",
    ],
    "Regiones específicas": [
        "Cusco flood",
        "Puno frost",
        "Arequipa drought",
        "Junin peru disaster",
        "Huancavelica peru",
        "Ayacucho peru disaster",
        "Apurimac peru",
        "Andes peru climate",
    ],
    "Términos en español": [
        "heladas peru",
        "sequia peru",
        "inundaciones peru",
        "huaico peru",
        "friaje peru",
        "granizada peru",
        "deslizamiento peru",
        "emergencia peru clima",
        "perdida cosecha peru",
        "campaña agricola peru",
    ],
}

todos_resultados = []

for cat, terminos in categorias.items():
    print(f"\n── {cat} ──")
    for q in terminos:
        arts = gdelt_search(q, start_1y, end, max_records=250)
        n = len(arts)

        # Analizar idiomas y fuentes si hay resultados
        langs = {}
        domains = {}
        if arts:
            for a in arts:
                lang = a.get("language", "unknown")
                dom = a.get("domain", "unknown")
                langs[lang] = langs.get(lang, 0) + 1
                domains[dom] = domains.get(dom, 0) + 1

        top_lang = max(langs, key=langs.get) if langs else "-"
        n_spanish = langs.get("Spanish", 0)
        top_domain = max(domains, key=domains.get) if domains else "-"

        todos_resultados.append({
            "categoria": cat,
            "query": q,
            "n_total": n,
            "n_spanish": n_spanish,
            "top_lang": top_lang,
            "top_domain": top_domain,
        })

        lang_str = f"(esp: {n_spanish})" if n_spanish > 0 else ""
        print(f"  {q:35s} -> {n:4d} arts {lang_str:12s} top: {top_domain}")
        time.sleep(PAUSE)

# Guardar resultados del mapeo
df_mapeo = pd.DataFrame(todos_resultados)
df_mapeo.to_csv(RAW_DIR / "gdelt_mapeo_terminos.csv", index=False)
print(f"\nGuardado: data/raw/gdelt_mapeo_terminos.csv")

# ── BLOQUE 2: PROFUNDIDAD HISTÓRICA ─────────────────────────
print("\n" + "=" * 70)
print("BLOQUE 2: PROFUNDIDAD HISTÓRICA (2017-2026)")
print("=" * 70)

# Usamos los 3 mejores términos del bloque anterior
df_mapeo_sorted = df_mapeo.sort_values("n_total", ascending=False)
top_queries = df_mapeo_sorted.head(5)["query"].tolist()

print(f"Top 5 términos: {top_queries}")

historico = []

for q in top_queries:
    print(f"\n  Término: '{q}'")
    for year in range(2017, 2027):
        s = datetime(year, 1, 1)
        e = min(datetime(year, 12, 31), end)
        arts = gdelt_search(q, s, e, max_records=250)
        n = len(arts)
        n_esp = sum(1 for a in arts if a.get("language") == "Spanish")
        historico.append({"query": q, "year": year, "n_total": n, "n_spanish": n_esp})
        print(f"    {year}: {n:4d} total, {n_esp:4d} español")
        time.sleep(PAUSE)

df_hist = pd.DataFrame(historico)
df_hist.to_csv(RAW_DIR / "gdelt_profundidad_historica.csv", index=False)
print(f"\nGuardado: data/raw/gdelt_profundidad_historica.csv")

# ── BLOQUE 3: ANÁLISIS DE FUENTES PERUANAS ──────────────────
print("\n" + "=" * 70)
print("BLOQUE 3: FUENTES PERUANAS — ¿qué medios cubre GDELT?")
print("=" * 70)

# Búsqueda amplia de noticias de Perú sobre desastres
arts_peru = gdelt_search("peru disaster OR flood OR drought OR emergency", start_1y, end, max_records=250)
time.sleep(PAUSE)
arts_peru2 = gdelt_search("peru inundacion OR sequia OR helada OR huaico", start_1y, end, max_records=250)

all_arts = arts_peru + arts_peru2

if all_arts:
    df_arts = pd.DataFrame(all_arts)

    # Fuentes
    print(f"\nTotal artículos recopilados: {len(df_arts)}")
    print(f"\nTop 20 fuentes (dominios):")
    print(df_arts['domain'].value_counts().head(20).to_string())

    # Idiomas
    if 'language' in df_arts.columns:
        print(f"\nDistribución por idioma:")
        print(df_arts['language'].value_counts().to_string())

    # Fuentes peruanas (filtro manual)
    pe_domains = ['elcomercio', 'rpp.pe', 'larepublica', 'gestion.pe', 'andina.pe',
                  'peru21', 'deperu.com', 'exitosa', 'panamericana', 'canal',
                  'elperuano', 'correo', 'ojo', 'trome', 'diario']
    df_arts['es_peruano'] = df_arts['domain'].apply(
        lambda d: any(pe in str(d).lower() for pe in pe_domains)
    )
    n_pe = df_arts['es_peruano'].sum()
    print(f"\nArtículos de medios peruanos detectados: {n_pe}/{len(df_arts)}")
    if n_pe > 0:
        print("Fuentes peruanas encontradas:")
        print(df_arts[df_arts['es_peruano']]['domain'].value_counts().to_string())

    # Guardar muestra
    df_arts.to_csv(RAW_DIR / "gdelt_muestra_peru_clima.csv", index=False)
    print(f"\nGuardado: data/raw/gdelt_muestra_peru_clima.csv")

# ── BLOQUE 4: TIMELINES DE COBERTURA ────────────────────────
print("\n" + "=" * 70)
print("BLOQUE 4: TIMELINES DE COBERTURA (3 meses)")
print("=" * 70)

start_3m = end - timedelta(days=90)
timeline_queries = ["flood peru", "drought peru", "El Nino peru",
                    "landslide peru", "peru disaster"]

all_timelines = {}

for q in timeline_queries:
    tl = gdelt_timeline(q, start_3m, end)
    if tl:
        all_timelines[q] = tl
        print(f"  {q:25s} -> {len(tl)} puntos temporales")
    else:
        print(f"  {q:25s} -> sin datos")
    time.sleep(PAUSE)

if all_timelines:
    with open(RAW_DIR / "gdelt_timelines.json", "w") as f:
        json.dump(all_timelines, f, indent=2)
    print(f"\nGuardado: data/raw/gdelt_timelines.json")

# ── RESUMEN FINAL ─────────────────────────────────────────────
print("\n" + "=" * 70)
print("RESUMEN FINAL")
print("=" * 70)

print("\nTop 15 términos por volumen de artículos:")
print(df_mapeo.sort_values("n_total", ascending=False).head(15)[
    ["query", "n_total", "n_spanish", "top_lang", "top_domain"]
].to_string(index=False))

if not df_hist.empty:
    print("\nCobertura histórica (artículos por año, top queries):")
    pivot = df_hist.pivot(index="year", columns="query", values="n_total")
    print(pivot.to_string())

print("\nArchivos generados:")
print("  data/raw/gdelt_mapeo_terminos.csv")
print("  data/raw/gdelt_profundidad_historica.csv")
print("  data/raw/gdelt_muestra_peru_clima.csv")
print("  data/raw/gdelt_timelines.json")
print("\nExploración completada.")
