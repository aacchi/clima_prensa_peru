# Clima y Prensa en Perú — Cobertura mediática vs. realidad climática

Proyecto personal para analizar la relación entre la **cobertura mediática de eventos climáticos** en Perú y los **datos reales de variabilidad climática, eventos extremos e impactos agrícolas**, con foco en la zona andina central y sur.

> **Hipótesis:** La cobertura de prensa sobre eventos climáticos no refleja proporcionalmente la magnitud real de los eventos ni sus impactos en la agricultura. Algunos fenómenos (El Niño) reciben atención desproporcionada, mientras que otros (heladas, sequías localizadas) son sistemáticamente subreportados pese a su impacto.

Inspirado en el trabajo de [Bastián Olea](https://github.com/bastianolea/prensa_chile) sobre cobertura de delincuencia vs. estadísticas reales en Chile.

---

## Objetivo

Construir dos bases de datos paralelas — noticias y datos duros — para responder:

1. **Patrones temporales:** ¿Cuándo se habla más de clima en los medios? ¿Hay estacionalidad? ¿Coincide con la ocurrencia real de eventos?
2. **Proporcionalidad:** ¿Los eventos con mayor impacto (muertos, hectáreas perdidas, familias afectadas) reciben más cobertura?
3. **Sesgo temático:** ¿Qué eventos se sobrereportan y cuáles se ignoran? ¿El Niño acapara la cobertura mientras las heladas pasan desapercibidas?
4. **Geografía mediática:** ¿Qué regiones reciben más atención? ¿Lima concentra la cobertura o se cubren las zonas rurales andinas?
5. **Impacto agrícola:** ¿Las pérdidas de cosecha y emergencias agrarias se reflejan en la prensa?

---

## Zona geográfica

Cobertura a **nivel nacional** (todo Perú), con análisis de detalle en:

- **Sierra central:** Junín, Huancavelica, Ayacucho
- **Sierra sur:** Cusco, Puno, Arequipa, Apurímac
- **Costa sur:** Ica, Arequipa (costa)

---

## Estado del proyecto

| Fase | Descripción | Estado |
|------|-------------|--------|
| 0 | Diseño del pipeline y exploración de fuentes | 🔄 En curso |
| 1A | Cosecha GDELT — noticias indexadas | ⬜ Pendiente |
| 1B | Scraping complementario — medios peruanos | ⬜ Pendiente |
| 2 | Detección temática por palabras clave | ⬜ Pendiente |
| 3 | Obtención de datos duros (EM-DAT, INDECI, SENAMHI) | ⬜ Pendiente |
| 4 | Cruce y análisis | ⬜ Pendiente |
| 5 | Visualización y outputs | ⬜ Pendiente |

---

## Arquitectura del pipeline

```
┌─────────────────────────────────────────────────────────┐
│                    FUENTES DE NOTICIAS                   │
│                                                         │
│   GDELT API ──────┐                                    │
│   (noticias        ├──► corpus_noticias (Parquet)       │
│    indexadas)      │    ┌──────────────────────┐        │
│                    │    │ fecha                 │        │
│   Scraping ───────┘    │ fuente                │        │
│   medios peruanos       │ titulo                │        │
│   (complementario)      │ url                   │        │
│                         │ cuerpo (si disponible)│        │
│                         │ idioma                │        │
│                         └──────────────────────┘        │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              DETECCIÓN TEMÁTICA                          │
│                                                         │
│   Palabras clave por categoría (regex)                  │
│   Umbral: ≥ 3 coincidencias por noticia                 │
│   Resultado: noticia clasificada en 1+ temas            │
│                                                         │
│   Conteo diario: noticias_tema / noticias_total = %     │
│   Suavizado: media móvil 21 días                        │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                   DATOS DUROS                            │
│                                                         │
│   EM-DAT ──────────► eventos de desastre (país)         │
│   DesInventar ─────► eventos locales (distrito)         │
│   INDECI ──────────► emergencias y daños                │
│   SENAMHI ─────────► datos meteorológicos (futuro)      │
│   MIDAGRI ─────────► pérdidas agrícolas (futuro)        │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                 CRUCE Y ANÁLISIS                         │
│                                                         │
│   Serie temporal: % cobertura vs. eventos reales        │
│   Correlación por tipo de evento                        │
│   Mapa de cobertura vs. mapa de impacto                 │
│   Brechas: eventos con alto impacto y baja cobertura    │
└─────────────────────────────────────────────────────────┘
```

---

## Fuentes de datos

### Noticias

#### Fuente primaria: GDELT

GDELT (Global Database of Events, Language, and Tone) monitorea medios globales en 100+ idiomas. Su API Doc 2.0 permite buscar artículos por palabra clave, país, idioma y rango de fechas.

- **Cobertura temporal:** desde 2017 (API Doc), desde 2015 (Event Database)
- **Campos disponibles:** url, título, fecha, fuente, país de origen, idioma, tono
- **Limitación:** la API de artículos oficialmente cubre los últimos 3 meses; para datos históricos se usa BigQuery o descargas CSV masivas
- **Librería Python:** `gdeltdoc`

#### Fuente complementaria: scraping de medios peruanos

Para mayor profundidad y cobertura histórica, scraping directo de medios nacionales:

| Medio | Tipo | Archivo histórico | Prioridad |
|-------|------|-------------------|-----------|
| El Comercio | Diario nacional | Por explorar | Alta |
| RPP Noticias | Radio/web | Por explorar | Alta |
| La República | Diario nacional | Por explorar | Alta |
| Andina (agencia estatal) | Agencia | Por explorar | Alta |
| El Peruano | Diario oficial | Por explorar | Media |
| Correo | Diario regional | Por explorar | Media |
| Los Andes (Puno) | Diario regional | Por explorar | Media |
| Diario del Cusco | Diario regional | Por explorar | Media |

> ⚠️ La viabilidad del scraping para cada medio se determinará en la Fase 0 (exploración). Algunos medios bloquean scraping, otros no tienen archivo histórico accesible.

### Datos duros (eventos climáticos reales)

| Fuente | Qué contiene | Acceso | Granularidad |
|--------|-------------|--------|-------------|
| EM-DAT | Desastres globales desde 1900 | API GraphQL + Excel (registro gratuito) | País, por evento |
| DesInventar | Desastres locales Perú | Web + posible descarga | Distrito, por evento |
| INDECI (SINPAD) | Emergencias nacionales | Web, reportes | Distrito, por emergencia |
| SENAMHI | Datos meteorológicos | Web, posible API | Estación, diario |
| MIDAGRI | Estadísticas agrícolas | Web, reportes | Región, por campaña |

> ⚠️ La disponibilidad y formato de los datos duros se confirmará en la Fase 0. SENAMHI y MIDAGRI se incorporarán en una segunda etapa.

---

## Diccionario de palabras clave

Siguiendo la metodología de Bastián Olea: cada noticia se clasifica por **coincidencia de patrones regex** en su título y/o cuerpo. Una noticia debe contener **≥ 3 instancias** de patrones del grupo para clasificarse en ese tema.

### Grupo 1 — Eventos hidrometeorológicos extremos

```python
palabras_eventos_extremos = [
    r"helad[ao]s?",
    r"friaje",
    r"nevada[s]?",
    r"granizad[ao]s?",
    r"huaic[oa]s?",
    r"aluvión",
    r"inundaci[oó]n|inundaciones",
    r"desborde",
    r"deslizamiento",
    r"sequ[ií]a",
    r"veranillo",
    r"lluvias?\s+(intensa|torrencial|fuerte)",
    r"ola\s+de\s+(fr[ií]o|calor)",
    r"tormenta\s+(eléctrica|de\s+granizo)",
    r"vientos?\s+fuerte",
    r"marejada",
]
```

### Grupo 2 — El Niño y variabilidad climática

```python
palabras_enso = [
    r"[Ee]l\s+[Nn]iño",
    r"[Ll]a\s+[Nn]iña",
    r"ENSO",
    r"[Ff]enómeno\s+del\s+[Nn]iño",
    r"[Nn]iño\s+(costero|global)",
    r"variabilidad\s+clim[aá]tica",
    r"cambio\s+clim[aá]tico",
    r"calentamiento\s+(global|del\s+mar)",
    r"anomal[ií]a\s+(térmica|clim[aá]tica)",
]
```

### Grupo 3 — Impacto agrícola

```python
palabras_agro = [
    r"pérdida[s]?\s+de\s+cosecha",
    r"campaña\s+agr[ií]cola",
    r"hectáreas?\s+(afectada|perdida|dañada|destruida)",
    r"emergencia\s+agr[ai]ria",
    r"seguro\s+agr[ai]rio",
    r"cultivos?\s+(afectado|perdido|dañado|destruido)",
    r"ganado\s+(muerto|afectado|perdido)",
    r"mortalidad\s+(de\s+)?(ganado|alpaca|ovino)",
    r"seguridad\s+alimentaria",
    r"precio[s]?\s+de\s+alimento",
    r"escasez\s+de\s+(agua|riego)",
    r"estrés\s+h[ií]drico",
]
```

### Grupo 4 — Institucional y respuesta

```python
palabras_institucional = [
    r"SENAMHI",
    r"INDECI",
    r"declaratoria\s+de\s+emergencia",
    r"estado\s+de\s+emergencia",
    r"alerta\s+(meteorológica|roja|naranja|amarilla)",
    r"zona[s]?\s+de\s+emergencia",
    r"ayuda\s+humanitaria",
    r"damnificado[s]?",
    r"evacuaci[oó]n|evacuados?",
    r"albergue[s]?",
    r"reconstrucción",
]
```

### Grupo 5 — Geográfico (filtro complementario)

```python
# Regiones de interés para filtrado adicional
regiones_foco = [
    r"Jun[ií]n", r"Huancavelica", r"Ayacucho",
    r"Cusco|Cuzco", r"Puno", r"Arequipa", r"Apur[ií]mac",
    r"Ica",
    r"sierra\s+(central|sur)",
    r"altiplano",
    r"cordillera",
    r"zona[s]?\s+andina",
    r"zona[s]?\s+altoandina",
]
```

> ⚠️ Este diccionario es preliminar. Se ajustará tras la cosecha exploratoria (Fase 0), revisando falsos positivos y negativos en una muestra manual.

---

## Métricas de análisis

### Serie temporal de cobertura

Para cada grupo temático, por mes:

- `n_tema`: número de noticias clasificadas en el tema
- `n_total`: número total de noticias del día/mes
- `p_tema`: proporción (`n_tema / n_total`)
- `p_tema_suavizado`: media móvil (ventana 21 días para datos diarios, 3 meses para datos mensuales)

### Cruce con datos duros

- Correlación temporal: ¿los picos de cobertura coinciden con picos de eventos reales?
- Brecha de cobertura: eventos con alto impacto (muertos, hectáreas, damnificados) y baja cobertura mediática
- Índice de proporcionalidad: ratio entre % de cobertura y % de impacto real

### Dimensiones geográficas

- Conteo de menciones por región (regex sobre regiones en título/cuerpo)
- Comparación: regiones más cubiertas vs. regiones más afectadas

---

## Stack técnico

- **Lenguaje:** Python 3.11+
- **IDE:** VSCode con terminal Git Bash
- **Entorno:** venv
- **Librerías principales:**
  - `gdeltdoc` — consultas a GDELT API
  - `requests` + `beautifulsoup4` — scraping de medios
  - `pandas` — manipulación de datos
  - `pyarrow` — lectura/escritura Parquet
  - `sqlite3` — base de datos local
  - `matplotlib` / `plotly` — visualización
  - `re` — detección de patrones regex

---

## Estructura de carpetas

```
clima_prensa_peru/
├── README.md
├── requirements.txt
├── data/
│   ├── raw/                  # datos crudos (GDELT JSON, scraping HTML)
│   ├── staging/              # datos transformados (Parquet)
│   ├── db/                   # SQLite
│   ├── external/             # EM-DAT, DesInventar, INDECI (descargas manuales)
│   └── logs/
├── scripts/
│   ├── 00_explorar_gdelt.py          # exploración inicial
│   ├── 01_cosechar_gdelt.py          # cosecha GDELT por keywords
│   ├── 02_scraping_medios.py         # scraping complementario
│   ├── 03_unificar_corpus.py         # unir fuentes en un solo corpus
│   ├── 04_detectar_temas.py          # clasificación por palabras clave
│   ├── 05_cargar_datos_duros.py      # procesar EM-DAT, INDECI, etc.
│   ├── 06_cruce_analisis.py          # cruzar noticias con datos reales
│   └── 07_visualizar.py              # gráficos y outputs
├── notebooks/                # exploración y análisis ad hoc
│   └── explorar_muestra.ipynb
└── outputs/
    ├── tables/
    ├── figures/
    └── notes/
```

---

## Próximos pasos

- [ ] Explorar GDELT API con una consulta de prueba (heladas + Perú)
- [ ] Evaluar profundidad histórica real de GDELT para noticias peruanas
- [ ] Explorar viabilidad de scraping para El Comercio, RPP, Andina
- [ ] Descargar datos EM-DAT para Perú (crear cuenta gratuita)
- [ ] Ajustar diccionario de palabras clave con muestra real
- [ ] Definir ventana temporal final según disponibilidad de datos

---

## Referencias metodológicas

- Olea, B. (2024). [Cobertura de delincuencia en prensa vs. datos de delincuencia](https://github.com/bastianolea/delincuencia_prensa). GitHub.
- Olea, B. (2024). [Análisis de prensa chilena](https://github.com/bastianolea/prensa_chile). GitHub.
- GDELT Project. [Doc API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/).
- EM-DAT. [The International Disaster Database](https://public.emdat.be/).
- DesInventar. [Disaster Information Management System](https://www.desinventar.net/).

---

*Proyecto en desarrollo. Última actualización: febrero 2026.*
