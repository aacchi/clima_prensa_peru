"""
02_explorar_y_detectar_temas.py
Exploración del corpus GDELT y detección temática por palabras clave.
Metodología basada en Bastián Olea (prensa_chile/delincuencia_prensa).
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from collections import Counter

STAGING_DIR = Path("data/staging")
OUTPUT_DIR = Path("outputs/tables")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Cargar datos ───────────────────────────────────────────────
print("=" * 70)
print("  EXPLORACIÓN Y DETECCIÓN TEMÁTICA")
print("=" * 70)

df = pd.read_parquet(STAGING_DIR / "gdelt_clima_peru_20260221.parquet")
print(f"\nRegistros totales: {len(df):,}")
print(f"Columnas: {list(df.columns)}")

# ── PARTE 1: EXPLORACIÓN GENERAL ─────────────────────────────
print("\n" + "=" * 70)
print("PARTE 1: EXPLORACIÓN GENERAL")
print("=" * 70)

# Filtrar solo español
df_esp = df[df['language'] == 'Spanish'].copy()
print(f"\nArtículos en español: {len(df_esp):,} ({len(df_esp)/len(df)*100:.1f}%)")

# Rango temporal
print(f"Rango: {df_esp['fecha'].min()} → {df_esp['fecha'].max()}")

# Por año
print(f"\nArtículos por año (español):")
year_counts = df_esp['year'].value_counts().sort_index()
for year, n in year_counts.items():
    bar = "█" * (n // 500)
    print(f"  {int(year)}: {n:6,} {bar}")

# Por trimestre
print(f"\nTop 10 trimestres con más artículos:")
q_counts = df_esp['_trimestre'].value_counts().head(10)
for q, n in q_counts.items():
    print(f"  {q}: {n:,}")

# Fuentes
print(f"\nTop 15 fuentes en español:")
for dom, n in df_esp['domain'].value_counts().head(15).items():
    print(f"  {dom:30s} {n:6,}")

# Queries que más aportan
print(f"\nTop 15 queries por volumen (español):")
for q, n in df_esp['_query'].value_counts().head(15).items():
    print(f"  {q:35s} {n:6,}")

# ── PARTE 2: DETECCIÓN TEMÁTICA ──────────────────────────────
print("\n" + "=" * 70)
print("PARTE 2: DETECCIÓN TEMÁTICA POR PALABRAS CLAVE")
print("=" * 70)

# Trabajamos sobre títulos (el cuerpo no está disponible en GDELT artlist)
# Los títulos son suficientes para clasificación temática
print("\nNota: clasificación basada en TÍTULOS de noticias.")
print("GDELT artlist no incluye cuerpo completo.\n")

# ── Diccionario de temas ──────────────────────────────────────

temas = {
    "heladas_friaje": [
        r"helad[ao]s?",
        r"friaje",
        r"ola\s+de\s+fr[ií]o",
        r"temperatur\w+\s+(baj|descen|bajo\s+cero)",
        r"nevada",
        r"nev[oó]",
        r"hipotermia",
        r"frost",
        r"cold\s+wave",
        r"freeze",
    ],
    "inundaciones_huaicos": [
        r"inundaci[oó]n",
        r"inundaciones",
        r"huaic[oa]",
        r"desbord[eó]",
        r"crecida",
        r"riada",
        r"anegad",
        r"flood",
        r"flash\s+flood",
        r"overflow",
    ],
    "lluvias_intensas": [
        r"lluvias?\s+(intensa|torrencial|fuerte|extrema)",
        r"precipitaci[oó]n",
        r"tormenta",
        r"aguacero",
        r"diluvio",
        r"heavy\s+rain",
        r"downpour",
        r"rainfall",
    ],
    "deslizamientos": [
        r"deslizamiento",
        r"derrumbe",
        r"alud",
        r"aluvi[oó]n",
        r"avalanch",
        r"landslide",
        r"mudslide",
        r"rockslide",
    ],
    "sequias": [
        r"sequ[ií]a",
        r"d[eé]ficit\s+h[ií]drico",
        r"estr[eé]s\s+h[ií]drico",
        r"escasez\s+de\s+agua",
        r"falta\s+de\s+(agua|lluvia|riego)",
        r"drought",
        r"water\s+(shortage|scarcity|crisis)",
        r"dry\s+spell",
    ],
    "granizadas": [
        r"granizad[ao]",
        r"granizo",
        r"hail",
        r"hailstorm",
        r"hailstone",
    ],
    "el_nino_variabilidad": [
        r"[Ee]l\s+[Nn]i[ñn]o",
        r"[Ll]a\s+[Nn]i[ñn]a",
        r"ENSO",
        r"[Ff]en[oó]meno\s+(del\s+)?[Nn]i[ñn]o",
        r"[Nn]i[ñn]o\s+(costero|global)",
        r"variabilidad\s+clim[aá]tica",
        r"oscilaci[oó]n",
    ],
    "cambio_climatico": [
        r"cambio\s+clim[aá]tico",
        r"calentamiento\s+global",
        r"climate\s+change",
        r"global\s+warming",
        r"efecto\s+invernadero",
        r"greenhouse",
        r"emisi[oó]n.*carbono",
        r"carbon\s+emission",
    ],
    "impacto_agricola": [
        r"p[eé]rdida.*cosecha",
        r"cosecha.*p[eé]rdida",
        r"cultivo.*afectad",
        r"cultivo.*da[ñn]ad",
        r"cultivo.*destru",
        r"hect[aá]rea.*afectad",
        r"hect[aá]rea.*perdid",
        r"campa[ñn]a\s+agr[ií]cola",
        r"emergencia\s+agr[ai]ria",
        r"seguro\s+agr[ai]rio",
        r"plaga",
        r"crop\s+(loss|damage|failure)",
        r"harvest\s+(loss|damage|fail)",
        r"agricultural\s+(disaster|emergency|damage)",
    ],
    "impacto_ganadero": [
        r"ganado\s+(muert|afectad|perdid)",
        r"mortalidad.*ganado",
        r"mortalidad.*alpaca",
        r"mortalidad.*ovino",
        r"alpaca.*muert",
        r"ovino.*muert",
        r"livestock\s+(death|loss|mortality)",
    ],
    "seguridad_alimentaria": [
        r"seguridad\s+alimentaria",
        r"inseguridad\s+alimentaria",
        r"hambre",
        r"hambruna",
        r"desnutrici[oó]n",
        r"food\s+security",
        r"food\s+insecurity",
        r"hunger",
        r"famine",
        r"malnutrition",
    ],
    "emergencias_institucional": [
        r"declaratoria\s+de\s+emergencia",
        r"estado\s+de\s+emergencia",
        r"INDECI",
        r"SENAMHI",
        r"alerta\s+(meteorol[oó]gica|roja|naranja|amarilla)",
        r"damnificad",
        r"evacuaci[oó]n",
        r"evacuad",
        r"albergue",
        r"ayuda\s+humanitaria",
        r"emergency\s+declaration",
        r"disaster\s+relief",
        r"humanitarian\s+aid",
    ],
}

# Regiones para geolocalizacion
regiones = {
    "Lima": r"\bLima\b",
    "Cusco": r"\bCusco\b|\bCuzco\b",
    "Puno": r"\bPuno\b",
    "Arequipa": r"\bArequipa\b",
    "Junin": r"\bJun[ií]n\b",
    "Huancavelica": r"\bHuancavelica\b",
    "Ayacucho": r"\bAyacucho\b",
    "Apurimac": r"\bApur[ií]mac\b",
    "Ica": r"\bIca\b",
    "Ancash": r"\b[AÁ]ncash\b",
    "La Libertad": r"\bLa\s+Libertad\b",
    "Piura": r"\bPiura\b",
    "Cajamarca": r"\bCajamarca\b",
    "Huanuco": r"\bHu[aá]nuco\b",
    "Pasco": r"\bPasco\b",
    "Tacna": r"\bTacna\b",
    "Moquegua": r"\bMoquegua\b",
    "Lambayeque": r"\bLambayeque\b",
    "Loreto": r"\bLoreto\b",
    "Madre de Dios": r"\bMadre\s+de\s+Dios\b",
    "San Martin": r"\bSan\s+Mart[ií]n\b",
    "Ucayali": r"\bUcayali\b",
    "Amazonas": r"\bAmazonas\b",
    "Tumbes": r"\bTumbes\b",
}

# ── Aplicar detección ─────────────────────────────────────────
print("Aplicando detección temática sobre títulos...")

# Convertir títulos a minúsculas para matching
df['title_lower'] = df['title'].fillna('').str.lower()

# Detectar temas
for tema, patrones in temas.items():
    patron_completo = "|".join(patrones)
    df[f'tema_{tema}'] = df['title_lower'].str.contains(patron_completo, regex=True, na=False)
    # Contar coincidencias
    df[f'n_{tema}'] = df['title_lower'].str.count(patron_completo)

# Detectar regiones
for region, patron in regiones.items():
    df[f'reg_{region}'] = df['title'].fillna('').str.contains(patron, regex=True, na=False)

# ── Estadísticas de detección ─────────────────────────────────
print("\n── Detección temática (todos los idiomas) ──")
tema_cols = [c for c in df.columns if c.startswith('tema_')]
for col in tema_cols:
    tema_name = col.replace('tema_', '')
    n = df[col].sum()
    pct = n / len(df) * 100
    print(f"  {tema_name:30s} {n:7,} artículos ({pct:5.2f}%)")

print(f"\n── Detección temática (solo español) ──")
df_esp = df[df['language'] == 'Spanish'].copy()
for col in tema_cols:
    tema_name = col.replace('tema_', '')
    n = df_esp[col].sum()
    pct = n / len(df_esp) * 100
    print(f"  {tema_name:30s} {n:7,} artículos ({pct:5.2f}%)")

# Artículos con al menos un tema detectado
df['tiene_tema'] = df[tema_cols].any(axis=1)
df_esp['tiene_tema'] = df_esp[tema_cols].any(axis=1)
print(f"\nArtículos con ≥1 tema (total): {df['tiene_tema'].sum():,} / {len(df):,} ({df['tiene_tema'].mean()*100:.1f}%)")
print(f"Artículos con ≥1 tema (español): {df_esp['tiene_tema'].sum():,} / {len(df_esp):,} ({df_esp['tiene_tema'].mean()*100:.1f}%)")

# ── Detección de regiones ─────────────────────────────────────
print(f"\n── Menciones de regiones en títulos (español) ──")
reg_cols = [c for c in df.columns if c.startswith('reg_')]
reg_counts = {}
for col in reg_cols:
    region_name = col.replace('reg_', '')
    n = df_esp[col].sum()
    reg_counts[region_name] = n

for region, n in sorted(reg_counts.items(), key=lambda x: -x[1]):
    if n > 0:
        bar = "█" * (n // 200)
        print(f"  {region:20s} {n:6,} {bar}")

# ── PARTE 3: SERIES TEMPORALES POR TEMA ──────────────────────
print("\n" + "=" * 70)
print("PARTE 3: SERIES TEMPORALES POR TEMA (mensual, español)")
print("=" * 70)

df_esp_temas = df_esp[df_esp['tiene_tema']].copy()
df_esp_temas['year_month'] = df_esp_temas['fecha'].dt.to_period('M')

# Total de noticias por mes (para calcular proporción)
total_mensual = df_esp.groupby(df_esp['fecha'].dt.to_period('M')).size()
total_mensual.name = 'total'

# Por cada tema, serie mensual
series_temas = {}
for col in tema_cols:
    tema_name = col.replace('tema_', '')
    serie = df_esp[df_esp[col]].groupby(df_esp[df_esp[col]]['fecha'].dt.to_period('M')).size()
    serie.name = tema_name
    series_temas[tema_name] = serie

df_series = pd.DataFrame(series_temas).fillna(0).astype(int)
df_series['total'] = total_mensual
df_series = df_series.sort_index()

# Mostrar los últimos 12 meses de los temas principales
print("\nÚltimos 12 meses — artículos por tema (español):")
top_temas = ['inundaciones_huaicos', 'deslizamientos', 'heladas_friaje',
             'sequias', 'el_nino_variabilidad', 'impacto_agricola',
             'emergencias_institucional', 'cambio_climatico']
cols_mostrar = [t for t in top_temas if t in df_series.columns]
print(df_series[cols_mostrar + ['total']].tail(12).to_string())

# Guardar serie completa
df_series.to_csv(OUTPUT_DIR / "serie_mensual_temas.csv")
print(f"\nGuardado: outputs/tables/serie_mensual_temas.csv")

# ── PARTE 4: MUESTRA DE TÍTULOS POR TEMA ─────────────────────
print("\n" + "=" * 70)
print("PARTE 4: MUESTRA DE TÍTULOS POR TEMA (verificación)")
print("=" * 70)

for col in tema_cols:
    tema_name = col.replace('tema_', '')
    muestra = df_esp[df_esp[col]].head(5)
    if len(muestra) > 0:
        print(f"\n── {tema_name} ──")
        for _, row in muestra.iterrows():
            print(f"  [{str(row['fecha'])[:10]}] {str(row['title'])[:90]}")
            print(f"    {row['domain']}")

# ── PARTE 5: GUARDAR DATASET ENRIQUECIDO ─────────────────────
print("\n" + "=" * 70)
print("PARTE 5: GUARDANDO DATASET ENRIQUECIDO")
print("=" * 70)

# Guardar versión completa con temas
out_path = STAGING_DIR / "gdelt_clima_peru_temas.parquet"
df.to_parquet(out_path, index=False)
print(f"Guardado: {out_path}")

# Guardar solo español con temas
out_esp = STAGING_DIR / "gdelt_clima_peru_español_temas.parquet"
df_esp_full = df[df['language'] == 'Spanish'].copy()
df_esp_full.to_parquet(out_esp, index=False)
print(f"Guardado: {out_esp}")

# Resumen final
print(f"\n{'='*70}")
print(f"RESUMEN")
print(f"{'='*70}")
print(f"  Artículos totales:     {len(df):,}")
print(f"  Artículos en español:  {len(df_esp):,}")
print(f"  Con tema detectado:    {df_esp['tiene_tema'].sum():,}")
print(f"  Temas definidos:       {len(temas)}")
print(f"  Regiones rastreadas:   {len(regiones)}")
print(f"  Rango temporal:        {df['fecha'].min()} → {df['fecha'].max()}")
print(f"\nArchivos generados:")
print(f"  {out_path}")
print(f"  {out_esp}")
print(f"  outputs/tables/serie_mensual_temas.csv")
print(f"\nScript completado.")
