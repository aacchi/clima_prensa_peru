"""
01_cosechar_gdelt.py
Cosecha sistemática de GDELT: todos los queries × todos los trimestres × 2017-2026.
Guarda cada página en data/raw/ y consolida en data/staging/.
Diseñado para dejarlo corriendo ~2 horas.
"""

import requests
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ── Configuración ──────────────────────────────────────────────
BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
PAUSE = 10          # pausa normal entre requests
RETRY_WAIT = 35     # espera base ante 429
MAX_RETRIES = 4
MAX_RECORDS = 250

RAW_DIR = Path("data/raw/gdelt_cosecha")
STAGING_DIR = Path("data/staging")
LOG_DIR = Path("data/logs")
RAW_DIR.mkdir(parents=True, exist_ok=True)

TODAY = datetime.today().strftime("%Y%m%d")
LOG_PATH = LOG_DIR / f"cosecha_gdelt_{TODAY}.log"

# ── Queries definitivos ───────────────────────────────────────
QUERIES = [
    # Eventos hidrometeorológicos
    "flood peru",
    "flooding peru",
    "landslide peru",
    "frost peru",
    "cold wave peru",
    "heavy rain peru",
    "drought peru",
    "drought peru agriculture",
    "water shortage peru",
    "hailstorm peru",
    "avalanche peru",
    # El Niño y variabilidad
    "El Nino peru",
    "La Nina peru",
    "climate change peru",
    "global warming peru",
    # Impacto agrícola
    "crop loss peru",
    "crop damage peru",
    "food security peru",
    "harvest peru",
    "agriculture peru disaster",
    "livestock peru cold",
    "potato peru",
    # Institucional
    "SENAMHI peru",
    "peru disaster emergency",
    "peru emergency declaration",
    "peru disaster relief",
    "INDECI peru",
    # Regiones de interés
    "Cusco disaster",
    "Puno frost",
    "Puno flood",
    "Arequipa disaster",
    "Junin peru",
    "Huancavelica peru",
    "Ayacucho peru disaster",
    "Apurimac peru",
    "Ica peru drought",
]

# ── Trimestres: 2017-Q1 hasta 2026-Q1 ────────────────────────
def generar_trimestres(year_start, year_end):
    trimestres = []
    for year in range(year_start, year_end + 1):
        for q in range(1, 5):
            month_start = (q - 1) * 3 + 1
            start = datetime(year, month_start, 1)
            if q < 4:
                end = datetime(year, month_start + 3, 1) - timedelta(days=1)
            else:
                end = datetime(year, 12, 31)
            # No ir más allá de hoy
            if start > datetime.today():
                break
            end = min(end, datetime.today())
            trimestres.append((f"{year}Q{q}", start, end))
    return trimestres

TRIMESTRES = generar_trimestres(2017, 2026)

# ── Logging ────────────────────────────────────────────────────
def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ── Búsqueda con reintentos ───────────────────────────────────
def gdelt_search(query, start, end):
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": MAX_RECORDS,
        "startdatetime": start.strftime("%Y%m%d%H%M%S"),
        "enddatetime": end.strftime("%Y%m%d%H%M%S"),
    }
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
            if r.status_code == 429:
                wait = RETRY_WAIT * (attempt + 1)
                log(f"    429 — esperando {wait}s (intento {attempt+1})")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                log(f"    HTTP {r.status_code}")
                return []
            data = r.json()
            return data.get("articles", [])
        except json.JSONDecodeError:
            log(f"    JSON inválido (intento {attempt+1})")
            time.sleep(15)
        except Exception as e:
            log(f"    Error: {str(e)[:60]} (intento {attempt+1})")
            time.sleep(15)
    return []

# ── Cosecha principal ──────────────────────────────────────────
def cosechar():
    total_queries = len(QUERIES)
    total_trimestres = len(TRIMESTRES)
    total_combinaciones = total_queries * total_trimestres

    log("=" * 70)
    log(f"COSECHA GDELT — {total_queries} queries × {total_trimestres} trimestres = {total_combinaciones} consultas")
    log(f"Pausa entre requests: {PAUSE}s")
    log(f"Tiempo estimado: {total_combinaciones * (PAUSE + 2) // 60} minutos")
    log("=" * 70)

    all_articles = []
    seen_urls = set()  # para deduplicar
    consulta_num = 0
    errores = 0

    for query in QUERIES:
        query_total = 0
        query_spanish = 0
        query_new = 0

        for qname, start, end in TRIMESTRES:
            consulta_num += 1

            # Nombre de archivo para caché
            safe_query = query.replace(" ", "_").replace("/", "_")
            cache_file = RAW_DIR / f"{safe_query}_{qname}.json"

            # Si ya existe el archivo, saltar (permite reanudar)
            if cache_file.exists():
                with open(cache_file, "r", encoding="utf-8") as f:
                    arts = json.load(f)
                new_arts = [a for a in arts if a.get("url") not in seen_urls]
                for a in new_arts:
                    seen_urls.add(a.get("url"))
                    a["_query"] = query
                    a["_trimestre"] = qname
                    all_articles.append(a)
                query_total += len(arts)
                query_new += len(new_arts)
                continue

            # Consultar GDELT
            arts = gdelt_search(query, start, end)

            # Guardar raw
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(arts, f, ensure_ascii=False)

            # Deduplicar y agregar
            new_arts = [a for a in arts if a.get("url") not in seen_urls]
            for a in new_arts:
                seen_urls.add(a.get("url"))
                a["_query"] = query
                a["_trimestre"] = qname
                all_articles.append(a)

            n = len(arts)
            n_new = len(new_arts)
            n_esp = sum(1 for a in arts if a.get("language") == "Spanish")
            query_total += n
            query_spanish += n_esp
            query_new += n_new

            if n > 0:
                log(f"  [{consulta_num}/{total_combinaciones}] {query:30s} {qname}: {n:3d} arts ({n_new} nuevos, {n_esp} esp)")
            
            time.sleep(PAUSE)

        log(f"  >>> {query}: {query_total} total, {query_new} nuevos únicos")
        log("")

    # ── Consolidar y guardar ──────────────────────────────────
    log("=" * 70)
    log(f"COSECHA COMPLETADA")
    log(f"  Artículos únicos totales: {len(all_articles)}")
    log(f"  URLs deduplicadas: {len(seen_urls)}")
    log("=" * 70)

    if all_articles:
        df = pd.DataFrame(all_articles)

        # Limpiar y enriquecer
        if 'seendate' in df.columns:
            df['fecha'] = pd.to_datetime(df['seendate'].str[:8], format='%Y%m%d', errors='coerce')
            df['year'] = df['fecha'].dt.year
            df['month'] = df['fecha'].dt.month
            df['quarter'] = df['fecha'].dt.quarter

        # Guardar
        out_parquet = STAGING_DIR / f"gdelt_clima_peru_{TODAY}.parquet"
        out_csv = STAGING_DIR / f"gdelt_clima_peru_{TODAY}.csv"

        df.to_parquet(out_parquet, index=False)
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")

        log(f"Guardado: {out_parquet}")
        log(f"Guardado: {out_csv}")
        log(f"Columnas: {list(df.columns)}")

        # Estadísticas
        log(f"\nRango de fechas: {df['fecha'].min()} → {df['fecha'].max()}")

        if 'language' in df.columns:
            log(f"\nIdiomas:")
            log(df['language'].value_counts().head(10).to_string())

        if 'domain' in df.columns:
            log(f"\nTop 20 fuentes:")
            log(df['domain'].value_counts().head(20).to_string())

        log(f"\nArtículos por año:")
        log(df['year'].value_counts().sort_index().to_string())

        log(f"\nArtículos por query:")
        log(df['_query'].value_counts().head(20).to_string())

        # Estadísticas de español
        if 'language' in df.columns:
            df_esp = df[df['language'] == 'Spanish']
            log(f"\n--- Solo artículos en español: {len(df_esp)} ---")
            if len(df_esp) > 0:
                log(f"Top fuentes españolas:")
                log(df_esp['domain'].value_counts().head(15).to_string())
                log(f"\nPor año (español):")
                log(df_esp['year'].value_counts().sort_index().to_string())

    else:
        log("Sin artículos para guardar.")

    log("\nProceso terminado.")

if __name__ == "__main__":
    cosechar()
