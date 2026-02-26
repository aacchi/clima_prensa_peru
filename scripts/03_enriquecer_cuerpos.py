"""
03_enriquecer_cuerpos.py
Descarga el cuerpo completo de artículos peruanos con tema detectado.
Usa las URLs ya existentes en el corpus GDELT.
Reanudable: guarda progreso en JSON incremental.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import time
from pathlib import Path
from datetime import datetime

# ── Configuración ──────────────────────────────────────────────
STAGING_DIR = Path("data/staging")
RAW_DIR = Path("data/raw/cuerpos")
LOG_DIR = Path("data/logs")
RAW_DIR.mkdir(parents=True, exist_ok=True)

PAUSE = 3
MAX_RETRIES = 2
TIMEOUT = 12
BATCH_SIZE = 50  # guardar cada N artículos

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'es-PE,es;q=0.9,en;q=0.8',
}

# Dominios accesibles (confirmados)
DOMINIOS_OK = [
    'larepublica.pe', 'deperu.com', 'diariocorreo.pe', 'elcomercio.pe',
    'gestion.pe', 'peru21.pe', 'panamericana.pe', 'tvperu.gob.pe',
    'andina.pe', 'elpopular.pe', 'rpp.pe',
    'radionacional.com.pe', 'servindi.org', 'peru.com',
    'entornointeligente.com',
]

TODAY = datetime.today().strftime("%Y%m%d")
LOG_PATH = LOG_DIR / f"enriquecer_cuerpos_{TODAY}.log"
PROGRESS_FILE = RAW_DIR / "progreso_cuerpos.json"
OUTPUT_FILE = RAW_DIR / "cuerpos_descargados.jsonl"

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def extraer_cuerpo(html, domain):
    """Extrae el texto principal de una página de noticias."""
    soup = BeautifulSoup(html, 'lxml')
    
    # Eliminar scripts, styles, nav, footer, ads
    for tag in soup(['script', 'style', 'nav', 'footer', 'aside', 'iframe']):
        tag.decompose()
    
    # Buscar contenedor principal por clases comunes
    contenedores = [
        ('article', None),
        ('div', 'story-contents'),
        ('div', 'article-body'),
        ('div', 'nota-contenido'),
        ('div', 'news-body'),
        ('div', 'content-body'),
        ('div', 'entry-content'),
        ('div', 'post-content'),
        ('div', 'article-content'),
        ('div', 'cuerpo'),
    ]
    
    for tag, cls in contenedores:
        if cls:
            found = soup.find(tag, class_=lambda c: c and cls in str(c).lower())
        else:
            found = soup.find(tag)
        if found:
            paragraphs = found.find_all('p')
            text = '\n'.join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 30)
            if len(text) > 200:
                return text
    
    # Fallback: todos los párrafos largos de la página
    paragraphs = soup.find_all('p')
    long_p = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 50]
    text = '\n'.join(long_p)
    
    return text if len(text) > 100 else ""

def cargar_progreso():
    """Carga URLs ya procesadas."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            return set(json.load(f))
    return set()

def guardar_progreso(urls_procesadas):
    """Guarda URLs procesadas."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(list(urls_procesadas), f)

def main():
    # Cargar datos
    df = pd.read_parquet(STAGING_DIR / "gdelt_clima_peru_español_temas.parquet")
    tema_cols = [c for c in df.columns if c.startswith('tema_')]
    df_temas = df[df[tema_cols].any(axis=1)].copy()
    
    # Filtrar dominios accesibles
    df_target = df_temas[df_temas['domain'].isin(DOMINIOS_OK)].copy()
    df_target = df_target.sort_values('fecha', ascending=False)
    df_target = df_target.drop_duplicates(subset='url')
    
    log("=" * 70)
    log(f"ENRIQUECIMIENTO DE CUERPOS — {len(df_target)} artículos a procesar")
    log(f"Dominios: {df_target['domain'].nunique()}")
    log(f"Pausa: {PAUSE}s | Timeout: {TIMEOUT}s")
    log("=" * 70)
    
    # Cargar progreso anterior
    urls_procesadas = cargar_progreso()
    pendientes = df_target[~df_target['url'].isin(urls_procesadas)]
    log(f"Ya procesados: {len(urls_procesadas)} | Pendientes: {len(pendientes)}")
    
    # Procesar
    exitos = 0
    errores = 0
    vacios = 0
    batch_count = 0
    
    for idx, (_, row) in enumerate(pendientes.iterrows()):
        url = row['url']
        domain = row['domain']
        
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            
            if r.status_code == 200:
                cuerpo = extraer_cuerpo(r.text, domain)
                
                if cuerpo:
                    # Guardar en JSONL (una línea por artículo)
                    registro = {
                        'url': url,
                        'domain': domain,
                        'title': row.get('title', ''),
                        'fecha': str(row.get('fecha', ''))[:10],
                        'cuerpo': cuerpo,
                        'chars': len(cuerpo),
                        'temas': [c.replace('tema_', '') for c in tema_cols if row.get(c, False)],
                    }
                    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                        f.write(json.dumps(registro, ensure_ascii=False) + "\n")
                    exitos += 1
                else:
                    vacios += 1
            else:
                errores += 1
                
        except Exception as e:
            errores += 1
        
        urls_procesadas.add(url)
        batch_count += 1
        
        # Log cada 50 artículos
        if batch_count % BATCH_SIZE == 0:
            guardar_progreso(urls_procesadas)
            total_proc = exitos + errores + vacios
            log(f"  [{total_proc}/{len(pendientes)}] éxitos: {exitos}, vacíos: {vacios}, errores: {errores} | último: {domain}")
        
        time.sleep(PAUSE)
    
    # Guardar progreso final
    guardar_progreso(urls_procesadas)
    
    # ── Resumen ───────────────────────────────────────────────
    log("\n" + "=" * 70)
    log("RESUMEN")
    log("=" * 70)
    log(f"  Procesados: {exitos + errores + vacios}")
    log(f"  Con cuerpo extraído: {exitos}")
    log(f"  Vacíos (sin texto): {vacios}")
    log(f"  Errores: {errores}")
    log(f"  Tasa de éxito: {exitos/(exitos+errores+vacios)*100:.1f}%" if (exitos+errores+vacios) > 0 else "N/A")
    
    # Estadísticas del archivo de salida
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        log(f"  Total cuerpos en archivo: {len(lines)}")
        
        # Stats por dominio
        domain_counts = {}
        total_chars = 0
        for line in lines:
            d = json.loads(line)
            dom = d['domain']
            domain_counts[dom] = domain_counts.get(dom, 0) + 1
            total_chars += d['chars']
        
        log(f"  Caracteres totales: {total_chars:,}")
        log(f"  Promedio por artículo: {total_chars//len(lines):,} chars")
        log(f"\n  Por fuente:")
        for dom, n in sorted(domain_counts.items(), key=lambda x: -x[1]):
            log(f"    {dom:30s} {n:5d}")
    
    log("\nProceso terminado.")

if __name__ == "__main__":
    main()
