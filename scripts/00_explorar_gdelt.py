"""
00_explorar_gdelt.py
Exploración directa de GDELT API v2 para noticias climáticas en Perú.
Usa requests directamente (más confiable que gdeltdoc).
"""

import requests
import time
import pandas as pd
from datetime import datetime, timedelta

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

def buscar_gdelt(query, start_date, end_date, max_records=250):
    """Consulta GDELT Doc API directamente."""
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": max_records,
        "startdatetime": start_date.strftime("%Y%m%d%H%M%S"),
        "enddatetime": end_date.strftime("%Y%m%d%H%M%S"),
    }
    try:
        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        articles = data.get("articles", [])
        return pd.DataFrame(articles) if articles else pd.DataFrame()
    except Exception as e:
        print(f"  Error: {e}")
        return pd.DataFrame()

def buscar_timeline(query, start_date, end_date):
    """Consulta GDELT timeline."""
    params = {
        "query": query,
        "mode": "timelinevol",
        "format": "json",
        "startdatetime": start_date.strftime("%Y%m%d%H%M%S"),
        "enddatetime": end_date.strftime("%Y%m%d%H%M%S"),
    }
    try:
        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if "timeline" in data and len(data["timeline"]) > 0:
            series = data["timeline"][0].get("data", [])
            return pd.DataFrame(series)
        return pd.DataFrame()
    except Exception as e:
        print(f"  Error: {e}")
        return pd.DataFrame()

# ── Fechas ─────────────────────────────────────────────────────
end = datetime.today()
start_1m = end - timedelta(days=30)
start_3m = end - timedelta(days=90)

# ── PRUEBA 1: términos individuales (último mes) ──────────────
print("=" * 60)
print("PRUEBA 1: términos climáticos — último mes")
print("=" * 60)

terminos = {
    "helada peru":             "helada peru",
    "sequia peru":             "sequia peru",
    "inundacion peru":         "inundacion peru",
    "huaico peru":             "huaico peru",
    "El Nino peru":            "El Nino peru",
    "friaje peru":             "friaje peru",
    "granizo peru":            "granizo peru",
    "emergencia climatica peru": "emergencia climatica peru",
    "lluvias peru":            "lluvias peru",
    "deslizamiento peru":      "deslizamiento peru",
    "senamhi":                 "senamhi",
    "indeci emergencia":       "indeci emergencia",
}

resultados = []

for nombre, query in terminos.items():
    df = buscar_gdelt(query, start_1m, end)
    n = len(df)
    fuentes = df['domain'].nunique() if n > 0 else 0
    resultados.append({"termino": nombre, "n_articulos": n, "n_fuentes": fuentes})
    print(f"  {nombre:35s} -> {n:4d} articulos, {fuentes:3d} fuentes")
    time.sleep(2)

# ── PRUEBA 2: ver artículos de un término con resultados ──────
print("\n" + "=" * 60)
print("PRUEBA 2: detalle de artículos encontrados")
print("=" * 60)

# Buscar el término con más resultados
df_resumen = pd.DataFrame(resultados)
mejor = df_resumen.loc[df_resumen['n_articulos'].idxmax()]
mejor_query = terminos[mejor['termino']]

print(f"\nMejor término: '{mejor['termino']}' ({mejor['n_articulos']} artículos)")
df_mejor = buscar_gdelt(mejor_query, start_1m, end)

if len(df_mejor) > 0:
    print(f"Columnas disponibles: {list(df_mejor.columns)}")
    print(f"\nPrimeros 10 artículos:")
    for i, row in df_mejor.head(10).iterrows():
        fecha = str(row.get('seendate', ''))[:10]
        titulo = str(row.get('title', 'sin título'))[:80]
        fuente = str(row.get('domain', '?'))
        idioma = str(row.get('language', '?'))
        print(f"  [{fecha}] {titulo}")
        print(f"    {fuente} | idioma: {idioma}")

    print(f"\nTop 10 fuentes:")
    print(df_mejor['domain'].value_counts().head(10).to_string())

    print(f"\nIdiomas:")
    if 'language' in df_mejor.columns:
        print(df_mejor['language'].value_counts().to_string())

time.sleep(3)

# ── PRUEBA 3: timeline 3 meses ───────────────────────────────
print("\n" + "=" * 60)
print("PRUEBA 3: timeline 'lluvias peru' (3 meses)")
print("=" * 60)

tl = buscar_timeline("lluvias peru", start_3m, end)
if len(tl) > 0:
    print(f"Puntos en timeline: {len(tl)}")
    print(f"Columnas: {list(tl.columns)}")
    print(f"\nÚltimos 15 puntos:")
    print(tl.tail(15).to_string())
else:
    print("Sin datos de timeline.")

time.sleep(3)

# ── PRUEBA 4: profundidad histórica ──────────────────────────
print("\n" + "=" * 60)
print("PRUEBA 4: profundidad histórica")
print("=" * 60)

periodos = [
    ("2025", datetime(2025, 6, 1), datetime(2025, 7, 31)),
    ("2024", datetime(2024, 6, 1), datetime(2024, 7, 31)),
    ("2023", datetime(2023, 6, 1), datetime(2023, 7, 31)),
    ("2022", datetime(2022, 6, 1), datetime(2022, 7, 31)),
    ("2020", datetime(2020, 6, 1), datetime(2020, 7, 31)),
    ("2017", datetime(2017, 6, 1), datetime(2017, 7, 31)),
]

for año, s, e in periodos:
    df = buscar_gdelt("helada peru", s, e, max_records=50)
    print(f"  {año} (jun-jul): {len(df):4d} artículos")
    time.sleep(2)

# ── RESUMEN ───────────────────────────────────────────────────
print("\n" + "=" * 60)
print("RESUMEN FINAL")
print("=" * 60)
print(df_resumen.sort_values("n_articulos", ascending=False).to_string(index=False))
print("\nScript completado.")
