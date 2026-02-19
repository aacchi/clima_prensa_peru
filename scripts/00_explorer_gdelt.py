"""
00_explorar_gdelt.py
Exploración inicial de GDELT para noticias climáticas en Perú.
Objetivo: ver qué hay disponible, qué cobertura temporal tiene,
y cuántas noticias devuelve para nuestros términos clave.
"""

from gdeltdoc import GdeltDoc, Filters
from datetime import datetime, timedelta
import pandas as pd
import time

# ── Configuración ──────────────────────────────────────────────
# GDELT Doc API oficialmente cubre los últimos 3 meses
# Probamos con ventanas recientes para ver qué devuelve

gd = GdeltDoc()

# ── Prueba 1: búsqueda simple ─────────────────────────────────
print("=" * 60)
print("PRUEBA 1: heladas + Perú (último mes)")
print("=" * 60)

end_date = datetime.today()
start_date = end_date - timedelta(days=30)

f1 = Filters(
    keyword="helada Perú",
    start_date=start_date.strftime("%Y-%m-%d"),
    end_date=end_date.strftime("%Y-%m-%d"),
    country="PE",
)

try:
    articles = gd.article_search(f1)
    print(f"Artículos encontrados: {len(articles)}")
    if len(articles) > 0:
        print(f"Columnas: {list(articles.columns)}")
        print(f"\nPrimeros 5 títulos:")
        for _, row in articles.head(5).iterrows():
            print(f"  [{row.get('seendate', 'sin fecha')[:10]}] {row.get('title', 'sin título')}")
            print(f"    Fuente: {row.get('domain', '?')} | URL: {row.get('url', '')[:80]}")
        print(f"\nFuentes únicas: {articles['domain'].nunique()}")
        print(f"Top fuentes:")
        print(articles['domain'].value_counts().head(10).to_string())
except Exception as e:
    print(f"Error: {e}")

time.sleep(3)

# ── Prueba 2: varios términos climáticos ──────────────────────
print("\n" + "=" * 60)
print("PRUEBA 2: diferentes términos climáticos (último mes)")
print("=" * 60)

terminos = [
    "helada Perú",
    "sequía Perú",
    "inundación Perú",
    "huaico Perú",
    "El Niño Perú",
    "friaje Perú",
    "granizada Perú",
    "emergencia agraria Perú",
]

resultados = []

for termino in terminos:
    f = Filters(
        keyword=termino,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    )
    try:
        arts = gd.article_search(f)
        n = len(arts)
        fuentes = arts['domain'].nunique() if n > 0 else 0
        resultados.append({
            "termino": termino,
            "n_articulos": n,
            "n_fuentes": fuentes,
        })
        print(f"  {termino:30s} → {n:4d} artículos, {fuentes:3d} fuentes")
    except Exception as e:
        print(f"  {termino:30s} → Error: {e}")
        resultados.append({"termino": termino, "n_articulos": 0, "n_fuentes": 0})
    time.sleep(2)

# ── Prueba 3: volumen temporal ────────────────────────────────
print("\n" + "=" * 60)
print("PRUEBA 3: timeline de volumen — 'clima Perú' (3 meses)")
print("=" * 60)

f3 = Filters(
    keyword="clima Perú",
    start_date=(end_date - timedelta(days=90)).strftime("%Y-%m-%d"),
    end_date=end_date.strftime("%Y-%m-%d"),
    country="PE",
)

try:
    timeline = gd.timeline_search("timelinevol", f3)
    print(f"Filas en timeline: {len(timeline)}")
    if len(timeline) > 0:
        print(f"Columnas: {list(timeline.columns)}")
        print(f"\nÚltimas 10 entradas:")
        print(timeline.tail(10).to_string())
except Exception as e:
    print(f"Error: {e}")

time.sleep(3)

# ── Prueba 4: ¿qué tan atrás llega? ──────────────────────────
print("\n" + "=" * 60)
print("PRUEBA 4: intentar datos históricos (2020)")
print("=" * 60)

f4 = Filters(
    keyword="helada Perú",
    start_date="2020-06-01",
    end_date="2020-07-31",
)

try:
    arts_hist = gd.article_search(f4)
    print(f"Artículos 2020 (helada Perú): {len(arts_hist)}")
    if len(arts_hist) > 0:
        print("¡GDELT devuelve datos históricos para este rango!")
        print(f"Top fuentes:")
        print(arts_hist['domain'].value_counts().head(5).to_string())
    else:
        print("Sin datos para 2020. GDELT limita a los últimos 3 meses para article_search.")
except Exception as e:
    print(f"Error o sin datos: {e}")

# ── Resumen ───────────────────────────────────────────────────
print("\n" + "=" * 60)
print("RESUMEN DE EXPLORACIÓN")
print("=" * 60)
df_resumen = pd.DataFrame(resultados)
print(df_resumen.to_string(index=False))
print("\nScript completado.")