#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Relatório institucional (HTML+CSS → PDF via Playwright/Chromium) com geração por IA em cada seção.

Versão: v7.4
Principais recursos:
- IA para cada seção + “Arquivos e Saídas…”.
- Capa parametrizável (logo, órgãos/entidades acima da Coordenação, Coordenação, pessoa).
- Layout “livro”; TOC (sumário) clicável opcional.
- Numeração automática de Figura/Tabela via CSS counters.
- Ajustes finos: altura da logo, margem inferior da logo (pode ser negativa), tamanhos e espaçamentos.
- Temas embutidos, CSS local (arquivo) e CSS remoto (URL).
- **Saídas SEMPRE em `docs/`**: salva o `.html` e gera o `.pdf` na pasta `docs`.

Pré-requisitos:
    pip install jinja2 playwright markdown python-dotenv tiktoken openai
    playwright install chromium
    (defina OPENAI_API_KEY no ambiente ou em .env)

Uso (exemplos):
    python3 gerar_relatorio_html_pdf_IA_v7_4.py \
      --ent "República Federativa do Brasil" \
      --ent "Ministério da Gestão e da Inovação em Serviços Públicos" \
      --ent "Secretaria-Executiva" \
      --logo-path misc/logolula.png \
      --dept-text "Coordenação-Geral de Assuntos Corporativos e Disseminação de Conhecimento" \
      --person "Gustavo Magalhães Mendes de Tarso" \
      --logo-h 160px --dept-fs 11pt --brand-gap 0mm --logo-mb=-6mm \
      --ent-gap 0.5mm --ent-lh 1.12 \
      --toc --toc-pagebreak --theme clean

Variáveis de ambiente úteis:
  OPENAI_API_KEY, DOC_MODEL, DOC_SUMMARY_MODEL, DOC_STREAM, DOC_NO_PDF, DOC_POST_POLISH, DOC_POLISH_MODEL,
  DOC_POLISH_STYLE, DOC_FILES_PER_BATCH, DOC_FILE_EXCERPT_CHARS, DOC_BATCH_OUT_TOKENS, DOC_SEC_OUT_TOKENS,
  DOC_MAX_CHARS_FILE, DOC_MAX_PROJECT_CHARS,
  LOGO_PATH, DEPT_TEXT, PERSON_NAME,
  CSS_LOGO_H, CSS_DEPT_FS, CSS_BRAND_GAP, CSS_LOGO_MB,
  DOC_THEME, DOC_CSS_FILE, DOC_CSS_URL,
  ENT_FILE, CSS_ENT_FS, CSS_ENT_GAP, CSS_ENT_LH,
  TOC_TITLE
"""

import os
import re
import argparse
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import urllib.request
import urllib.error

import dotenv
from jinja2 import Environment, BaseLoader, select_autoescape

# ===================== Config padrão =====================
TIPOS_ARQUIVOS = (".py", ".r", ".toml", ".md")
MAX_CARACTERES_POR_ARQ = int(os.getenv("DOC_MAX_CHARS_FILE", "12000"))
MAX_TEXTO_PROJETO       = int(os.getenv("DOC_MAX_PROJECT_CHARS", str(MAX_CARACTERES_POR_ARQ * 10)))

DOC_FILES_PER_BATCH     = int(os.getenv("DOC_FILES_PER_BATCH", "8"))
DOC_FILE_EXCERPT_CHARS  = int(os.getenv("DOC_FILE_EXCERPT_CHARS", "2000"))

DOC_BATCH_OUT_TOKENS = int(os.getenv("DOC_BATCH_OUT_TOKENS", "1200"))
DOC_SEC_OUT_TOKENS   = int(os.getenv("DOC_SEC_OUT_TOKENS", "900"))
DOC_POLISH_OUT_TOKENS = int(os.getenv("DOC_POLISH_OUT_TOKENS", "600"))

DOC_MODEL         = os.getenv("DOC_MODEL", "gpt-4o")
DOC_SUMMARY_MODEL = os.getenv("DOC_SUMMARY_MODEL", "gpt-4o-mini")
USE_STREAM        = os.getenv("DOC_STREAM", "0") not in ("0", "", "false", "False")
NO_PDF            = os.getenv("DOC_NO_PDF", "0") not in ("0", "", "false", "False")
POST_POLISH       = os.getenv("DOC_POST_POLISH", "0") not in ("0", "", "false", "False")
DOC_POLISH_MODEL  = os.getenv("DOC_POLISH_MODEL", "gpt-4o-mini")
DOC_POLISH_STYLE  = os.getenv("DOC_POLISH_STYLE", "").strip()

PROJETO_NOME = os.path.basename(os.path.abspath("."))

# ===================== API Key =====================
if os.path.exists(".env"):
    env = dotenv.dotenv_values(".env")
    OPENAI_API_KEY = env.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
else:
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("Defina OPENAI_API_KEY no ambiente ou no arquivo .env")

# ===================== OpenAI cliente =====================
def _client_openai_v1():
    try:
        from openai import OpenAI
        return OpenAI(api_key=OPENAI_API_KEY)
    except Exception:
        import openai  # compat
        if hasattr(openai, "OpenAI"):
            return openai.OpenAI(api_key=OPENAI_API_KEY)
        raise

# ===================== Tokenização aproximada =====================
def _load_tokenizer():
    try:
        import tiktoken  # type: ignore
        try:
            enc = tiktoken.get_encoding("o200k_base")
        except Exception:
            enc = tiktoken.get_encoding("cl100k_base")
        return ("tiktoken", enc)
    except Exception:
        return ("approx", None)
_TOKENIZER_KIND, _TOKENIZER = _load_tokenizer()

def count_tokens_text(text: str) -> int:
    if _TOKENIZER_KIND == "tiktoken":
        return len(_TOKENIZER.encode(text))
    return max(1, len(text) // 4)

# ===================== FS utils =====================
def ler_texto_caminho(caminho: str, limite: int = MAX_CARACTERES_POR_ARQ) -> str:
    try:
        with open(caminho, encoding="utf-8", errors="ignore") as f:
            return f.read(limite)
    except Exception:
        return ""

def coletar_codigo_do_projeto(raiz="."):
    blocos, total = [], 0
    for root, dirs, files in os.walk(raiz):
        if any(x in root for x in ["logs", "debug_logs", "__pycache__", ".venv", ".git", "node_modules", "exports", "docs"]):
            continue
        for fname in files:
            if fname.lower().endswith(TIPOS_ARQUIVOS):
                caminho = os.path.join(root, fname)
                trecho = ler_texto_caminho(caminho, MAX_CARACTERES_POR_ARQ)
                if trecho:
                    bloco = f"\n### Arquivo: {os.path.relpath(caminho)}\n{trecho}"
                    blocos.append(bloco)
                    total += len(bloco)
                if total > MAX_TEXTO_PROJETO:
                    break
        if total > MAX_TEXTO_PROJETO:
            break
    return "\n".join(blocos)[:MAX_TEXTO_PROJETO]

# ===================== Make-scripts e dependências =====================
def _achar_make_scripts_paths():
    paths = []
    candidatos = [
        "reports/make_report.py",
        "reports/make_kpi_report.py",
        "reports/make_impact_report.py",
        "reports/make_kpi_report_with_impact.py",
        "reports/make_merged_report.py",
    ]
    for c in candidatos:
        if os.path.exists(c):
            paths.append(c)
    for root, _, files in os.walk("."):
        if "reports" not in root:
            continue
        for f in files:
            if f.startswith("make_") and f.endswith(".py"):
                p = os.path.join(root, f)
                if p not in paths:
                    paths.append(p)
    return sorted(paths)

def _resolver_caminho_script_py(nome_ou_caminho: str) -> Optional[str]:
    candidatos = []
    if os.path.isabs(nome_ou_caminho):
        candidatos.append(nome_ou_caminho)
    else:
        candidatos += [
            os.path.join("graphs_and_tables", nome_ou_caminho),
            os.path.join("reports", nome_ou_caminho),
            nome_ou_caminho,
        ]
    for c in candidatos:
        if os.path.exists(c):
            return c
    return None

def _resolver_caminho_script_r(nome_ou_caminho: str) -> Optional[str]:
    candidatos = []
    if os.path.isabs(nome_ou_caminho):
        candidatos.append(nome_ou_caminho)
    else:
        candidatos += [
            os.path.join("r_checks", nome_ou_caminho),
            nome_ou_caminho,
        ]
    for c in candidatos:
        if os.path.exists(c):
            return c
    return None

def extrair_scripts_usados_pelos_makes() -> List[str]:
    usados = set()
    for mp in _achar_make_scripts_paths():
        texto = ler_texto_caminho(mp, 300_000)
        if not texto:
            continue
        usados.add(mp)
        for s in re.findall(r'graphs_and_tables/([A-Za-z0-9_/\-]*?\.py)', texto):
            p = _resolver_caminho_script_py(s);  p and usados.add(p)
        for s in re.findall(r'r_checks/([A-Za-z0-9_\-]*?\.R)', texto):
            p = _resolver_caminho_script_r(s);   p and usados.add(p)
        for s in set(re.findall(r'["\']([A-Za-z0-9_\-]+\.py)["\']', texto)):
            p = _resolver_caminho_script_py(s);  p and usados.add(p)
        for s in set(re.findall(r'["\']([A-Za-z0-9_\-]+\.R)["\']', texto)):
            p = _resolver_caminho_script_r(s);   p and usados.add(p)
    return sorted(usados)

# ===================== OpenAI helpers =====================
def _chat_completion(client, model: str, messages: List[dict], max_tokens: int, stream: bool=False) -> str:
    if stream and not USE_STREAM:
        stream = False
    if stream:
        stream_resp = client.chat.completions.create(
            model=model, messages=messages, temperature=0.13, max_tokens=max_tokens, stream=True,
        )
        partes = []
        for chunk in stream_resp:
            delta = getattr(chunk.choices[0].delta, "content", None)
            if delta:
                print(delta, end="", flush=True); partes.append(delta)
        print()
        return "".join(partes).strip()
    else:
        resp = client.chat.completions.create(
            model=model, messages=messages, temperature=0.13, max_tokens=max_tokens,
        )
        return resp.choices[0].message.content.strip()

def resumir_corpus(client, text: str, alvo_tokens: int) -> str:
    sys_prompt = (
        "Resuma tecnicamente o conteúdo a seguir para servir de CONTEXTO COMPACTO ao redator. "
        "Mantenha nomes de arquivos, métricas, flags CLI e objetivo de alto nível. Parágrafos, sem bullets."
    )
    user = text[:min(len(text), 40000)]
    msgs = [{"role":"system","content":sys_prompt},{"role":"user","content":user}]
    return _chat_completion(client, DOC_SUMMARY_MODEL, msgs, max_tokens=max(512, alvo_tokens//2), stream=False)

SECOES_FIXAS = [
    "Introdução",
    "Contexto do Problema",
    "Soluções Desenvolvidas",
    "Indicadores Estratégicos",
    "Importância para a Gestão Estratégica",
    "Importância do Uso de Inteligência Artificial",
    "Impactos Institucionais",
    "Conclusão",
]

def _strip_ai_meta(texto: str, heading_atual: str) -> str:
    texto = re.sub(r'(?im)^\s*(t[íi]tulo|sistema|texto|se[cç][aã]o)\s*:\s.*$', '', texto)
    def _norm(s: str) -> str:
        return re.sub(r'\s+', ' ', s.strip().lower())
    secset = {_norm(h) for h in SECOES_FIXAS}
    linhas = [l for l in texto.splitlines()]
    while linhas and (_norm(linhas[0]) in secset or _norm(linhas[0]) == _norm(heading_atual)):
        linhas = linhas[1:]
    texto = "\n".join(linhas)
    return re.sub(r'\n{3,}', '\n\n', texto).strip()

def gerar_secao(client, titulo: str, contexto_compacto: str, sistema: str) -> str:
    sys_prompt = (
        "Você é um redator institucional. Escreva APENAS o corpo da seção indicada, em 1–3 parágrafos, "
        "sem títulos, rótulos ou bullets. Não repita o título da seção."
    )
    user = (
        f"Seção: '{titulo}'.\nSistema: {sistema}.\n\n"
        f"CONTEXTO:\n{contexto_compacto}\n\n"
        "Regras:\n- Nada de 'TÍTULO:'/'SISTEMA:'.\n- Só parágrafos.\n- Nomes de arquivos com =inline=.\n"
        "- Evite 'perícia médica'/'pericial'; use 'análises de tarefas'.\n"
    )
    raw = _chat_completion(client, DOC_MODEL,
                           [{"role":"system","content":sys_prompt},{"role":"user","content":user}],
                           max_tokens=DOC_SEC_OUT_TOKENS, stream=False)
    return _strip_ai_meta(raw, titulo)

def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def gerar_paragrafos_arquivos_em_lotes(client, arquivos: List[str], excerto_chars: int) -> str:
    saida: List[str] = []
    for batch in _chunks(arquivos, DOC_FILES_PER_BATCH):
        descritores = []
        for caminho in batch:
            raw = ler_texto_caminho(caminho, excerto_chars)
            descritores.append(f"ARQUIVO: {caminho}\nEXCERTO:\n{raw}\n---")

        sys_prompt = (
            "Redator técnico: para CADA ARQUIVO, escreva UM parágrafo, sem bullets/cabeçalhos. "
            "Comece com o caminho em =inline= (ex.: =reports/make_x.py=). Explique saídas (PNG/CSV/MD), "
            "flags (--start/--end/--top10/--perito/--mode) e finalidade. Ordem inalterada."
        )
        user = "LISTA DE ARQUIVOS (com excertos):\n\n" + "\n".join(descritores)
        texto = _chat_completion(client, DOC_MODEL,
                                 [{"role":"system","content":sys_prompt},{"role":"user","content":user}],
                                 max_tokens=DOC_BATCH_OUT_TOKENS, stream=False)
        paras = [p.strip() for p in re.split(r"\n\s*\n", texto.strip()) if p.strip()]
        paras = [_strip_ai_meta(p, "Arquivos e Saídas Utilizados pelo Relatório") for p in paras]
        saida.append("\n\n".join(paras))
    return "\n\n".join(saida).strip()

def polir_texto(client, titulo: str, corpo: str, sistema: str) -> str:
    estilo_extra = f"\n- Preferências de estilo: {DOC_POLISH_STYLE}\n" if DOC_POLISH_STYLE else ""
    sys_prompt = ("Editor institucional: reescreva com tom homogêneo e claro, sem bullets. "
                  "Devolva apenas parágrafos; preserve =inline code=.")
    user = (f"Título: {titulo}\nSistema: {sistema}\nRegras: sem rótulos/cabeçalhos.{estilo_extra}\n\n"
            f"TEXTO:\n{corpo}\n\nSAÍDA:")
    raw = _chat_completion(client, DOC_POLISH_MODEL,
                           [{"role":"system","content":sys_prompt},{"role":"user","content":user}],
                           max_tokens=DOC_POLISH_OUT_TOKENS, stream=False)
    return _strip_ai_meta(raw, titulo)

# ===================== CSS & HTML =====================
INLINE_EQ = re.compile(r"=(.+?)=")   # =...=
INLINE_TL = re.compile(r"~(.+?)~")   # ~...~

def escape_html(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

def _wrap_img_as_figure(html: str) -> str:
    """
    Converte <p><img alt="Legenda" src="..."></p> em <figure><img ...><figcaption>Legenda</figcaption></figure>
    (se ALT não vazio).
    """
    img_re = re.compile(r'<p>\s*<img([^>]*?)alt="([^"]+)"([^>]*)>\s*</p>', re.IGNORECASE)
    def repl(m):
        before, alt, after = m.group(1), m.group(2), m.group(3)
        return f'<figure><img{before}alt="{escape_html(alt)}"{after}><figcaption>{escape_html(alt)}</figcaption></figure>'
    return img_re.sub(repl, html)

def text_to_html(text: str, allow_markdown: bool = True) -> str:
    def _code_sub(match):
        inner = match.group(1)
        return f"<code>{escape_html(inner)}</code>"
    text2 = INLINE_EQ.sub(_code_sub, text)
    text2 = INLINE_TL.sub(_code_sub, text2)
    out = ""
    try:
        import markdown as md
        if allow_markdown:
            out = md.markdown(text2, extensions=["tables", "fenced_code"])
        else:
            raise RuntimeError()
    except Exception:
        paras = [p.strip() for p in text2.strip().split("\n") if p.strip()]
        out = "\n".join(f"<p>{p}</p>" for p in paras)
    return _wrap_img_as_figure(out)

# Temas embutidos (CSS adicional injetado ao final do <style>)
THEMES: Dict[str, str] = {
    "default": "",
    "clean": """
/* Tema: clean — mais leve */
@page { margin: 20mm 18mm; }
h1 { font-size: 30px; }
h2 { font-size: 20px; }
.section p { font-size: 12pt; }
.brand .dept-line { border-bottom-color: #cbd5e1; }
""",
    "dark": """
/* Tema: dark — fundo escuro e texto claro */
:root { --fg:#e5e7eb; --muted:#cbd5e1; --border:#334155; --accent:#e5e7eb; }
html,body { background:#0b1220; color:var(--fg); }
h2::after { background:#475569; }
code { background:#111827; }
th { background:#111827; }
table, th, td { border-color:#334155; }
.brand .dept-line { border-bottom-color:#475569; }
""",
    "serif": """
/* Tema: serif — estilo livro impresso */
@page { margin: 26mm 24mm; }
h1 { font-size: 34px; letter-spacing: .1px; }
h2 { font-size: 23px; }
.section p { font-size: 13.5pt; line-height: 1.7; }
"""
}

def load_css_file(path: str) -> str:
    p = Path(path or "")
    if not path or not p.exists():
        return ""
    try:
        return "\n/* --- custom css file --- */\n" + p.read_text(encoding="utf-8")
    except Exception:
        return ""

def load_css_url(url: str, timeout: int = 10) -> str:
    u = (url or "").strip()
    if not u or not u.lower().startswith(("http://", "https://")):
        return ""
    try:
        with urllib.request.urlopen(u, timeout=timeout) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            css = resp.read().decode(charset, errors="replace")
            return "\n/* --- custom css url --- */\n" + css
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError):
        return ""

HTML_TEMPLATE = r"""<!doctype html>
<html lang="pt-br">
  <head>
    <meta charset="utf-8"/>
    <title>{{ title }}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <style>
      :root {
        --fg:#0f172a; --muted:#475569; --border:#e5e7eb; --accent:#111827;
        --logo-h: {{ css_logo_h }};
        --dept-fs: {{ css_dept_fs }};
        --brand-gap: {{ css_brand_gap }};
        --logo-mb: {{ css_logo_mb }};
        --ent-fs: {{ css_ent_fs }};
        --ent-gap: {{ css_ent_gap }};
        --ent-lh: {{ css_ent_lh }};
      }
      @page { size: A4; margin: 24mm 22mm 24mm 22mm; }
      html,body{ margin:0;padding:0; color:var(--fg);
                 font-family: "Source Serif Pro", "Noto Serif", Georgia, Cambria, "Times New Roman", Times, serif;
                 line-height:1.6; -webkit-print-color-adjust:exact; print-color-adjust:exact; }
      .container{ max-width:880px; margin:0 auto; padding:0 6mm 14mm; }

      header.cover{ margin:6mm 0 10mm; padding-bottom:8mm; border-bottom:2px solid var(--border); text-align:center; }
      h1{ font-size:32px; letter-spacing:.2px; margin:8mm 0 4mm; }
      .meta{ color:var(--muted); font-size:11.5pt; }

      /* Bloco de marca (logo + textos) */
      .brand { display:flex; flex-direction:column; align-items:center; gap: var(--brand-gap); }
      .brand img.logo {
        height: var(--logo-h); width:auto; display:block; margin: 0 auto var(--logo-mb) auto; image-rendering:auto;
      }
      .brand .ents { display:flex; flex-direction:column; align-items:center; gap: var(--ent-gap); }
      .brand .ents .ent { font-size: var(--ent-fs); color: var(--fg); line-height: var(--ent-lh); }
      .brand .dept { font-size: var(--dept-fs); color:var(--fg); }
      .brand .dept-line { display:inline-block; border-bottom:1.5px solid #94a3b8; padding-bottom:2px; }
      .brand .person { font-size:12.5pt; color:var(--fg); font-weight:600; }

      /* TOC */
      .toc { margin: 10mm 0 6mm; }
      .toc h2 { font-size:20px; margin:0 0 4mm; font-variant-caps: small-caps; letter-spacing:.04em; }
      .toc ol { list-style:none; padding-left:0; margin:0; }
      .toc li { margin: 2.2mm 0; }
      .toc a { text-decoration:none; color:var(--fg); }

      /* Headings das seções */
      h2{ font-size:22px; margin:12mm 0 4mm; font-variant-caps: small-caps; letter-spacing:.04em; position:relative; }
      h2::after{ content:""; position:absolute; left:0; bottom:-6px; width:85px; height:2px; background:#d1d5db; }

      /* Parágrafos */
      .section p{ font-size:12.5pt; text-align:justify; text-justify:inter-word; hyphens:auto; }
      .section p + p{ margin-top:3.5mm; }

      /* Drop cap */
      .section p:first-of-type::first-letter{
        float:left; font-size:44px; line-height:.9; padding-right:6px; padding-top:2px;
        font-weight:600; color:var(--accent);
      }

      /* Figuras & Tabelas */
      body { counter-reset: fig tbl; }
      figure { margin:6mm auto; counter-increment: fig; }
      figure img { max-width:100%; height:auto; display:block; margin:0 auto; }
      figure figcaption { margin-top:2mm; font-size:11.5pt; text-align:center; color:#334155; }
      figure figcaption::before { content: "Figura " counter(fig) ": "; font-weight:600; color:#111827; }

      table { border-collapse:collapse; width:100%; margin:6mm 0; font-size:12pt; counter-increment: tbl; }
      caption { caption-side: bottom; padding-top: 2mm; font-size:11.5pt; color:#334155; }
      caption::before { content: "Tabela " counter(tbl) ": "; font-weight:600; color:#111827; }
      th,td{ border:1px solid #e5e7eb; padding:6px 8px; text-align:left }
      th{ background:#f8fafc }

      .section{ break-inside: avoid; }
      .page-break{ break-before: page; }

      /* CSS extra (tema/arquivo/URL) — sobrepõe o base */
      {{ theme_css | safe }}
    </style>
  </head>
  <body>
    <div class="container">
      <!-- CAPA -->
      <header class="cover">
        <div class="brand">
          {% if logo_src %}
          <img class="logo" src="{{ logo_src }}" alt="Marca institucional">
          {% endif %}

          {% if ent_lines %}
          <div class="ents">
            {% for line in ent_lines %}
            <div class="ent">{{ line }}</div>
            {% endfor %}
          </div>
          {% endif %}

          <div class="dept">
            <span class="dept-line">{{ dept_text }}</span>
          </div>
          <div class="person">{{ person_name }}</div>
        </div>

        <h1>{{ title }}</h1>
        <div class="meta">
          <div>Sistema: <strong>{{ system_name }}</strong></div>
          <div>Gerado em: {{ generated_at }}</div>
        </div>
      </header>

      <!-- TOC opcional -->
      {% if toc and toc_items %}
      <section class="toc {% if toc_pagebreak %}page-break{% endif %}">
        <h2>{{ toc_title }}</h2>
        <ol>
          {% for it in toc_items %}
          <li><a href="#{{ it.id }}">{{ it.text }}</a></li>
          {% endfor %}
        </ol>
      </section>
      {% endif %}

      <!-- SEÇÕES -->
      {% for sec in sections %}
      <section class="section" id="{{ sec.id }}">
        <h2>{{ sec.heading }}</h2>
        {{ sec.html | safe }}
      </section>
      {% endfor %}
    </div>
  </body>
</html>
"""

# ===================== helpers =====================
def slugify(s: str) -> str:
    s2 = re.sub(r'[^0-9A-Za-zÀ-ÿ\s-]', '', s, flags=re.UNICODE)
    s2 = re.sub(r'\s+', '-', s2.strip())
    return "sec-" + s2.lower()

def to_logo_src(path_or_url: str) -> str:
    s = (path_or_url or "").strip()
    if not s: return ""
    low = s.lower()
    if low.startswith(("http://","https://","file://","data:")):
        return s
    p = Path(s)
    return "file://" + str(p.resolve()).replace("\\", "/") if p.exists() else ""

def load_ent_lines_from_file(path: str) -> List[str]:
    p = Path(path or "")
    if not path or not p.exists():
        return []
    try:
        lines = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines()]
        return [ln for ln in lines if ln]
    except Exception:
        return []

def load_ent_lines_from_env() -> List[str]:
    env_val = os.getenv("ENT_LINES", "").strip()
    if not env_val:
        return []
    parts = re.split(r'[|;]\s*', env_val)
    return [p.strip() for p in parts if p.strip()]

def render_html_document(payload: Dict, css_logo_h: str, css_dept_fs: str, css_brand_gap: str,
                         toc: bool, toc_title: str, toc_pagebreak: bool, css_logo_mb: str,
                         theme_css: str, css_ent_fs: str, css_ent_gap: str, css_ent_lh: str) -> str:
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape())
    tpl = env.from_string(HTML_TEMPLATE)
    # montar seções com IDs e TOC
    sections_out, toc_items = [], []
    for sec in payload.get("sections", []):
        sec_id = slugify(sec["heading"])
        sections_out.append({"id": sec_id, "heading": sec["heading"], "html": text_to_html(sec["body"])})
        toc_items.append({"id": sec_id, "text": sec["heading"]})
    return tpl.render(
        title=payload.get("title", "Relatório"),
        system_name=payload.get("system_name", "Projeto"),
        generated_at=payload.get("generated_at", ""),
        sections=sections_out,
        logo_src=payload.get("logo_src", ""),
        ent_lines=payload.get("ent_lines", []),
        dept_text=payload.get("dept_text", ""),
        person_name=payload.get("person_name", ""),
        css_logo_h=css_logo_h, css_dept_fs=css_dept_fs, css_brand_gap=css_brand_gap, css_logo_mb=css_logo_mb,
        css_ent_fs=css_ent_fs, css_ent_gap=css_ent_gap, css_ent_lh=css_ent_lh,
        toc=toc, toc_items=toc_items, toc_title=toc_title, toc_pagebreak=toc_pagebreak,
        theme_css=theme_css
    )

# ===================== HTML -> PDF =====================
def html_to_pdf_playwright(html: str, pdf_path: str, page_format: str = "A4") -> None:
    from playwright.sync_api import sync_playwright
    out_dir = Path(pdf_path).resolve().parent
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / (Path(pdf_path).stem + ".html")
    html_path.write_text(html, encoding="utf-8")

    header_html = "<div></div>"
    footer_html = """
    <div style="width:100%; font-size:10px; font-family:system-ui, -apple-system, 'Segoe UI', Roboto;
                color:#475569; padding:6px 10px; text-align:right;">
      Página <span class="pageNumber"></span>/<span class="totalPages"></span>
    </div>
    """

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(html_path.as_uri(), wait_until="networkidle")
        page.pdf(
            path=pdf_path,
            format=page_format,
            print_background=True,
            margin={"top": "16mm", "bottom": "16mm", "left": "14mm", "right": "14mm"},
            display_header_footer=True,
            header_template=header_html,
            footer_template=footer_html,
        )
        browser.close()

# ===================== Main (CLI) =====================
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Gerar relatório HTML+CSS→PDF (IA por seção), com capa, TOC, temas, CSS externo e múltiplas entidades.")
    # Capa / Identidade
    ap.add_argument("--logo-path", dest="logo_path", default=os.getenv("LOGO_PATH", "misc/logolula.png"),
                    help="Caminho local do logo ou URL (http/https/file/data).")
    ap.add_argument("--dept-text", dest="dept_text", default=os.getenv("DEPT_TEXT", "Coordenação-Geral de Assuntos Corporativos e Disseminação de Conhecimento"),
                    help="Texto institucional da linha sublinhada (Coordenação).")
    ap.add_argument("--person", dest="person_name", default=os.getenv("PERSON_NAME", "Gustavo Magalhães Mendes de Tarso"),
                    help="Nome abaixo da linha institucional.")

    # Entidades (acima da Coordenação) — NOVO
    ap.add_argument("--ent", dest="ent_lines", action="append", default=None,
                    help="Linha de órgão/entidade (pode repetir). Ex.: --ent 'República Federativa do Brasil'")
    ap.add_argument("--ent-file", dest="ent_file", default=os.getenv("ENT_FILE", ""),
                    help="Arquivo .txt com uma linha por entidade.")
    ap.add_argument("--ent-fs", dest="ent_fs", default=os.getenv("CSS_ENT_FS", "11pt"),
                    help="Tamanho da fonte das entidades. Ex.: 11pt")
    ap.add_argument("--ent-gap", dest="ent_gap", default=os.getenv("CSS_ENT_GAP", "1.5mm"),
                    help="Espaço vertical entre as linhas das entidades (gap). Ex.: 1mm, 0.5mm, 0mm")
    ap.add_argument("--ent-lh", dest="ent_lh", default=os.getenv("CSS_ENT_LH", "1.25"),
                    help="Line-height das entidades (número). Ex.: 1.15, 1.1, 1.0")

    # Ajustes finos
    ap.add_argument("--logo-h", dest="logo_h", default=os.getenv("CSS_LOGO_H", "64px"),
                    help="Altura da logo (CSS size). Ex.: 96px, 192px. Padrão: 64px")
    ap.add_argument("--dept-fs", dest="dept_fs", default=os.getenv("CSS_DEPT_FS", "12.5pt"),
                    help="Tamanho da fonte da Coordenação (CSS size). Ex.: 11pt. Padrão: 12.5pt")
    ap.add_argument("--brand-gap", dest="brand_gap", default=os.getenv("CSS_BRAND_GAP", "6mm"),
                    help="Espaço vertical entre logo e blocos (gap principal). Ex.: 0mm, 2mm. Padrão: 6mm (não aceita negativo no CSS)")
    ap.add_argument("--logo-mb", dest="logo_mb", default=os.getenv("CSS_LOGO_MB", "0mm"),
                    help="Margem inferior da logo (CSS; aceita negativo). Ex.: -4mm. Padrão: 0mm")

    # TOC
    ap.add_argument("--toc", dest="toc", action="store_true", help="Habilita sumário (TOC) clicável.")
    ap.add_argument("--toc-title", dest="toc_title", default=os.getenv("TOC_TITLE", "Sumário"),
                    help="Título do sumário. Padrão: 'Sumário'.")
    ap.add_argument("--toc-pagebreak", dest="toc_pagebreak", action="store_true",
                    help="Insere quebra de página antes do sumário.")

    # Tema / CSS extra
    ap.add_argument("--theme", dest="theme", default=os.getenv("DOC_THEME", "default"),
                    choices=list(THEMES.keys()),
                    help="Tema embutido de CSS. Padrão: default.")
    ap.add_argument("--css-file", dest="css_file", default=os.getenv("DOC_CSS_FILE", ""),
                    help="Caminho de um arquivo CSS customizado. Sobrepõe o tema.")
    ap.add_argument("--css-url", dest="css_url", default=os.getenv("DOC_CSS_URL", ""),
                    help="URL de um CSS customizado. (Usado se --css-file não for informado.)")
    return ap.parse_args()

def main():
    args = parse_args()

    # Seleção do CSS extra (precedência: arquivo > url > tema > default)
    theme_css = ""
    if args.css_file:
        theme_css = load_css_file(args.css_file)
    elif args.css_url:
        theme_css = load_css_url(args.css_url)
    elif args.theme and args.theme in THEMES:
        theme_css = THEMES[args.theme]

    # Entidades (linhas acima da Coordenação)
    ent_lines: List[str] = []
    if args.ent_lines:
        ent_lines.extend([s for s in args.ent_lines if s and s.strip()])
    if args.ent_file:
        ent_lines.extend(load_ent_lines_from_file(args.ent_file))
    if not ent_lines:
        ent_lines = load_ent_lines_from_env()

    css_logo_h, css_dept_fs, css_brand_gap, css_logo_mb = args.logo_h, args.dept_fs, args.brand_gap, args.logo_mb
    css_ent_fs, css_ent_gap, css_ent_lh = args.ent_fs, args.ent_gap, args.ent_lh

    # Carrega corpus do projeto
    print(f"==> Lendo código do projeto '{PROJETO_NOME}' ...")
    corpus_projeto = coletar_codigo_do_projeto(".")

    print("==> Descobrindo scripts usados pelos make-scripts (reports/make_*.py) ...")
    arquivos_usados = extrair_scripts_usados_pelos_makes()
    if arquivos_usados:
        for a in arquivos_usados: print("   -", a)
    else:
        print("⚠️  Nenhum make-script detectado; seguirei com as seções gerais.")

    print("==> Preparando CONTEXTO COMPACTO …")
    client = _client_openai_v1()
    contexto_base = resumir_corpus(client, corpus_projeto, alvo_tokens=2000)
    contexto_alvos = "Arquivos alvo:\n" + "\n".join(f"- {a}" for a in arquivos_usados)
    contexto_compacto = f"{contexto_base}\n\n{contexto_alvos}"

    print("==> Gerando SEÇÕES principais por IA …")
    secoes = []
    for t in SECOES_FIXAS:
        print(f"   [+] {t}")
        corpo = gerar_secao(client, t, contexto_compacto, PROJETO_NOME)
        if POST_POLISH:
            corpo = polir_texto(client, t, corpo, PROJETO_NOME)
        secoes.append({"heading": t, "body": corpo})

    print("==> Gerando seção 'Arquivos e Saídas Utilizados pelo Relatório' (IA, em lotes) …")
    if arquivos_usados:
        bloco_arquivos = gerar_paragrafos_arquivos_em_lotes(client, arquivos_usados, DOC_FILE_EXCERPT_CHARS)
        if POST_POLISH:
            bloco_arquivos = polir_texto(client, "Arquivos e Saídas Utilizados pelo Relatório", bloco_arquivos, PROJETO_NOME)
        secoes.append({"heading": "Arquivos e Saídas Utilizados pelo Relatório", "body": bloco_arquivos})

    print("==> Renderizando HTML …")
    logo_src = to_logo_src(args.logo_path)
    payload = {
        "title": f"RELATÓRIO INSTITUCIONAL – SISTEMA {PROJETO_NOME}",
        "system_name": PROJETO_NOME,
        "generated_at": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "sections": secoes,
        "logo_src": logo_src,
        "ent_lines": ent_lines,
        "dept_text": args.dept_text,
        "person_name": args.person_name,
    }
    html = render_html_document(payload,
                                css_logo_h=css_logo_h,
                                css_dept_fs=css_dept_fs,
                                css_brand_gap=css_brand_gap,
                                toc=args.toc, toc_title=args.toc_title, toc_pagebreak=args.toc_pagebreak,
                                css_logo_mb=css_logo_mb,
                                theme_css=theme_css,
                                css_ent_fs=css_ent_fs, css_ent_gap=css_ent_gap, css_ent_lh=css_ent_lh)

    out_dir = Path("docs"); out_dir.mkdir(exist_ok=True, parents=True)
    html_file = out_dir / f"relatorio_{PROJETO_NOME}_IA.html"
    pdf_file  = out_dir / f"relatorio_{PROJETO_NOME}_IA.pdf"
    html_file.write_text(html, encoding="utf-8")
    print(f"💾 HTML salvo em: {html_file}")

    if not NO_PDF:
        print("==> Gerando PDF (Chromium headless) …")
        html_to_pdf_playwright(html, str(pdf_file))
        print(f"📄 PDF salvo em: {pdf_file}")
    else:
        print("ℹ️ DOC_NO_PDF=1: geração de PDF desativada.")

if __name__ == "__main__":
    main()

