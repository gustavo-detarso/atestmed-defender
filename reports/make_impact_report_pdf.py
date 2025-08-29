#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Relatório "Impacto na Fila" (PDF e PNGs) — 100% em Python (ReportLab + Matplotlib)

✔ Insere HEADER e FRONT (em .org) no início do PDF, convertendo para títulos e parágrafos.
✔ Gera PNGs (Top, Curva do Cotovelo, Tornado, Estratos, Permutação, PSA) em alta resolução.
✔ Mantém geração do PDF (sem LaTeX).
✔ Remove toda referência e coluna 'uo' das tabelas.
✔ Ordena SEMPRE "do pior → melhor" conforme o critério da tabela:
   - Tabela principal (selecionados): IV desc, depois Excesso desc, NC desc, N asc
   - Binomial: q asc, p asc, Excesso desc
   - Beta-binomial: q_bb asc, p_bb asc, Excesso desc
   - Estratos: IV_total desc, N_tot desc, NC_tot desc
✔ Todas as métricas exibidas formatadas com DUAS casas decimais (helpers _fmt2s/_fmtp).
✔ Nomes de perito em Title Case (exc.: da, de, di, do, du, e).
"""

import os, sys, re, json, math, sqlite3, argparse, shutil, unicodedata
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List
from pathlib import Path

import numpy as np
import pandas as pd

from pypdf import PdfReader, PdfWriter

from argparse import BooleanOptionalAction

from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER

import subprocess, tempfile
try:
    from pypdf import PdfMerger
except Exception:
    try:
        from PyPDF2 import PdfMerger  # fallback
    except Exception:
        PdfMerger = None

# — Integração com make_kpi_report (Top-10 KPI)
try:
    # quando executado como módulo: python -m reports.make_impact_report_pdf
    from reports.make_kpi_report import pegar_10_piores_peritos as _kpi_pegar_top10
except Exception:
    try:
        # fallback: se rodar o .py de dentro da pasta reports
        from make_kpi_report import pegar_10_piores_peritos as _kpi_pegar_top10
    except Exception as e:
        print(f"[warn] Falha ao importar make_kpi_report: {e}")
        _kpi_pegar_top10 = None

# =======================
# Matplotlib (PNG)
# =======================
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

FIG_DPI_DEFAULT = 300
GLOBAL_FIG_SCALE = 1.0
GLOBAL_FIG_DPI   = FIG_DPI_DEFAULT
PDF_IMG_FRAC = 1.0

PDF_MARGIN_LEFT_CM   = 3
PDF_MARGIN_RIGHT_CM  = 2
PDF_MARGIN_TOP_CM    = 3
PDF_MARGIN_BOTTOM_CM = 2

TABLE_FONT_SIZE = 8.0
TABLE_HEADER_FONT_SIZE = None

def _mkfig(w: float, h: float, dpi: int = None):
    """Cria uma figura aplicando o scale global (w,h em polegadas)."""
    dpi = dpi or GLOBAL_FIG_DPI
    return plt.subplots(figsize=(w * GLOBAL_FIG_SCALE, h * GLOBAL_FIG_SCALE), dpi=dpi)

# ─────────────────────────────────────────────────────────────────────
# Raiz do projeto + .env + import de comentarios.py
# ─────────────────────────────────────────────────────────────────────
_THIS_FILE = Path(__file__).resolve()
# Raiz do projeto = pai do diretório "reports"
BASE_DIR = str(_THIS_FILE.parents[1])

# Garante que a raiz entre no sys.path (para importar comentarios.py na raiz)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
UTILS_DIR = os.path.join(BASE_DIR, "utils")
if UTILS_DIR not in sys.path:
    sys.path.insert(0, UTILS_DIR)

# Tenta carregar o .env na raiz (opcional)
try:
    from dotenv import load_dotenv  # pip install python-dotenv (opcional)
    load_dotenv(Path(BASE_DIR) / ".env")
except Exception:
    pass

# Comentários GPT (usa .env na raiz do projeto via comentarios.py)
try:
    from comentarios import comentar_impacto_fila, chamar_gpt, SYSTEM_PROMPT
except Exception as e:
    # Fallbacks “no-op” caso o módulo não esteja disponível
    print(f"[warn] comentarios.py não carregado: {e}. Comentários automáticos desativados.", file=sys.stderr)
    def comentar_impacto_fila(df_all, df_sel, meta, **kw):
        return ""
    def chamar_gpt(system_prompt, user_prompt, **kw):
        return {"prompt": user_prompt, "comment": ""}
    SYSTEM_PROMPT = (
        "Você é um analista de dados especializado em gestão pública e auditoria do ATESTMED. "
        "Escreva comentários claros, objetivos e tecnicamente corretos, em um parágrafo."
    )

# =======================
# Configuração geral
# =======================
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# Para organização dos arquivos finais
OUTPUTS_ROOT = os.path.join(BASE_DIR, 'reports', 'outputs')

def _ext_dir_map() -> Dict[str, str]:
    """Mapeamento extensão → subpasta. Fácil de estender."""
    return {
        '.png': 'imgs',
        '.pdf': 'pdf',
        # acrescente aqui novas extensões, ex.: '.csv': 'tabelas'
    }

def _safe_move(src: str, dst_dir: str) -> str:
    """Move com sobrescrição segura (anexa sufixo numérico se já existir)."""
    os.makedirs(dst_dir, exist_ok=True)
    base = os.path.basename(src)
    stem, ext = os.path.splitext(base)
    dst = os.path.join(dst_dir, base)
    i = 1
    while os.path.exists(dst):
        dst = os.path.join(dst_dir, f"{stem}_{i}{ext}")
        i += 1
    shutil.move(src, dst)
    return dst

def _save_comments_files(comments: Dict[str, str], comments_dir: str) -> None:
    """Salva cada comentário individualmente e um JSON agregado."""
    if not comments:
        return
    os.makedirs(comments_dir, exist_ok=True)
    # individual (um arquivo por chave)
    for key, txt in comments.items():
        if not txt:
            continue
        safe_key = re.sub(r'[^a-zA-Z0-9_\-]+', '_', key.strip().lower())
        out_md = os.path.join(comments_dir, f"{safe_key}.md")
        with open(out_md, "w", encoding="utf-8") as f:
            f.write(txt.strip() + "\n")
    # agregado (útil para edição em lote)
    agg_json = os.path.join(comments_dir, "all_comments.json")
    try:
        with open(agg_json, "w", encoding="utf-8") as f:
            json.dump(comments, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _organize_outputs(start: str,
                      end: str,
                      generated_paths: List[str],
                      comments: Optional[Dict[str, str]] = None) -> str:
    """
    Move arquivos do EXPORT_DIR para:
      reports/outputs/YYYY-MM-DD_a_YYYY-MM-DD/impacto_fila/{pdf,imgs,...}
    e salva comentários em .../comments/.
    """
    # Base final
    period_dir = f"{start}_a_{end}"
    dest_base = os.path.join(OUTPUTS_ROOT, period_dir, "impacto_fila")
    os.makedirs(dest_base, exist_ok=True)

    # Garante subpastas (com as que sabemos de antemão)
    ext_map = _ext_dir_map()
    for sub in set(ext_map.values()) | {"comments"}:
        os.makedirs(os.path.join(dest_base, sub), exist_ok=True)

    # Move arquivos
    moved = []
    for p in sorted(set(filter(None, generated_paths))):
        if not os.path.exists(p):
            continue
        ext = os.path.splitext(p)[1].lower()
        sub = ext_map.get(ext, "misc")
        dst_dir = os.path.join(dest_base, sub)
        moved.append(_safe_move(p, dst_dir))

    # Salva comentários (se houver)
    if comments:
        _save_comments_files(comments, os.path.join(dest_base, "comments"))

    # Log opcional
    try:
        meta_txt = os.path.join(dest_base, "RUN_INFO.txt")
        with open(meta_txt, "w", encoding="utf-8") as f:
            f.write(f"Período: {start} a {end}\n")
            f.write(f"Arquivos movidos: {len(moved)}\n")
            for m in moved:
                f.write(f"- {os.path.relpath(m, dest_base)}\n")
    except Exception:
        pass

    return dest_base

# =======================
# ReportLab (PDF)
# =======================
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak, ListFlowable, ListItem
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from xml.sax.saxutils import escape as xml_escape

# Tenta usar DejaVu (melhor cobertura PT-BR); cai para Helvetica se indisponível
def _register_fonts():
    try:
        dejavu = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        dejavu_b = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        if os.path.exists(dejavu) and os.path.exists(dejavu_b):
            pdfmetrics.registerFont(TTFont("DejaVu", dejavu))
            pdfmetrics.registerFont(TTFont("DejaVu-Bold", dejavu_b))
            return "DejaVu", "DejaVu-Bold"
    except Exception:
        pass
    return "Helvetica", "Helvetica-Bold"

FONT_REG, FONT_BOLD = _register_fonts()

def _styles():
    ss = getSampleStyleSheet()
    styles = {
        "Title": ParagraphStyle(
            "Title", parent=ss["Title"], fontName=FONT_BOLD, fontSize=16, leading=19, spaceAfter=8
        ),
        "Heading1": ParagraphStyle(
            "Heading1", parent=ss["Heading1"], fontName=FONT_BOLD, fontSize=14, leading=17, spaceBefore=10, spaceAfter=6
        ),
        "Heading2": ParagraphStyle(
            "Heading2", parent=ss["Heading2"], fontName=FONT_BOLD, fontSize=12.5, leading=15, spaceBefore=8, spaceAfter=5
        ),
        "Heading3": ParagraphStyle(
            "Heading3", parent=ss["Heading3"], fontName=FONT_BOLD, fontSize=11.5, leading=14, spaceBefore=6, spaceAfter=4
        ),
        "Body": ParagraphStyle(
            "Body", parent=ss["BodyText"], fontName=FONT_REG, fontSize=9.7, leading=13, spaceAfter=4
        ),
        "Small": ParagraphStyle(
            "Small", parent=ss["BodyText"], fontName=FONT_REG, fontSize=8.6, leading=11.5, spaceAfter=3
        ),
        "Caption": ParagraphStyle(
            "Caption", parent=ss["BodyText"], fontName=FONT_REG, fontSize=8.5, leading=11, textColor=colors.black
        ),
        "Toc": ParagraphStyle(
            "Toc", parent=ss["BodyText"], fontName=FONT_REG, fontSize=9.7, leading=12
        ),
        
        "CoverTitle": ParagraphStyle(
            "CoverTitle", parent=ss["Title"], fontName=FONT_BOLD,
            fontSize=20, leading=24, alignment=1, spaceAfter=12
        ),

        "CoverSub": ParagraphStyle(
            "CoverSub", parent=ss["BodyText"], fontName=FONT_REG,
            fontSize=12, leading=15, alignment=1, spaceAfter=4
        ),

        "CoverSmall": ParagraphStyle(
            "CoverSmall", parent=ss["BodyText"], fontName=FONT_REG,
            fontSize=10.5, leading=13, alignment=1, spaceAfter=2
        ),
        
    }
    return styles

STYLES = _styles()

# =======================
# Helpers de formatação / nomes
# =======================
EXC = {"da","de","di","do","du","e"}
def titlecase_pt(s: str) -> str:
    if not isinstance(s, str): return str(s)
    tokens = re.split(r'(\s+)', s.strip())
    out=[]
    for t in tokens:
        if t.isspace():
            out.append(t)
            continue
        w = t.lower()
        if w in EXC:
            out.append(w)
        elif "-" in w:
            parts = [p if p in EXC else (p[:1].upper()+p[1:]) for p in w.split("-")]
            out.append("-".join(parts))
        else:
            out.append(w[:1].upper()+w[1:])
    return "".join(out)

def abbreviate_middle_names_pt(name: str) -> str:
    """Abrevia nomes do meio (>=2) mantendo 1º e último; preserva 'da,de,di,do,du,e'."""
    if not isinstance(name, str):
        return str(name)
    s = titlecase_pt(name.strip())
    if not s:
        return s
    parts = s.split()
    # índices de palavras "substanciais" (não são preposições/conjunções)
    idx_sub = [i for i, p in enumerate(parts) if p.lower() not in EXC]
    if len(idx_sub) < 2:
        return s
    first_i, last_i = idx_sub[0], idx_sub[-1]
    middle_sub = [i for i in idx_sub[1:-1]]
    # só abrevia se houver 2+ nomes do meio
    if len(middle_sub) < 2:
        return s

    out = []
    for i, p in enumerate(parts):
        low = p.lower()
        if i in middle_sub and low not in EXC:
            # Abrevia também componentes hifenizados: "Maria-Clara" -> "M.-C."
            segs = p.split("-")
            abbr = "-".join([(seg[0].upper() + ".") if seg else "" for seg in segs])
            out.append(abbr)
        elif low in EXC:
            out.append(low)  # mantém minúsculo
        else:
            out.append(p)
    return " ".join(out)

def _fmt2s(x: Any) -> str:
    try:
        v = float(x)
        return f"{v:.2f}"
    except Exception:
        return "—"

def _fmtp(x: Any) -> str:
    """p-valor com duas casas (ou notação científica se <0.01)."""
    try:
        v = float(x)
        return f"{v:.2f}" if v >= 0.01 else f"{v:.2e}"
    except Exception:
        return "—"

def _escape(s: str) -> str:
    return xml_escape(s or "")

def _merge_pdfs(paths: List[str], out_path: str) -> None:
    writer = PdfWriter()
    for p in paths:
        if not p or not os.path.exists(p):
            continue
        reader = PdfReader(p)
        for page in reader.pages:
            writer.add_page(page)
    with open(out_path, "wb") as f:
        writer.write(f)

def md_to_rl(texto: str) -> str:
    """
    Converte marcações Markdown simples para tags aceitas pelo Paragraph do ReportLab.
    Suporta: **negrito**, *itálico* ou _itálico_, `código` e quebras de linha.
    """
    if not texto:
        return ""
    s = xml_escape(str(texto))          # escapa <, >, & com segurança
    # **negrito**
    s = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", s)
    # *itálico*  (depois do negrito para não conflitar)
    s = re.sub(r"(?<!\*)\*(.+?)\*(?!\*)", r"<i>\1</i>", s)
    # _itálico_
    s = re.sub(r"(?<!_)_(.+?)_(?!_)", r"<i>\1</i>", s)
    # `codigo`
    s = re.sub(r"`([^`]+)`", r"<font face=\"Courier\">\1</font>", s)
    # quebras de linha
    s = s.replace("\n", "<br/>")
    return s

def _render_org_to_pdf(org_path: str, out_pdf: Optional[str] = None,
                       no_toc: bool = True, no_nums: bool = True) -> str:
    """
    Renderiza um .org em PDF via Emacs (ox-latex).
    Se no_toc/no_nums forem True, desativa TOC e numeração apenas nesta exportação.
    """
    org_path = str(Path(org_path).resolve())
    org_dir  = str(Path(org_path).parent)
    expected = str(Path(org_path).with_suffix(".pdf"))

    # remove pdf anterior para evitar confusão
    try:
        if os.path.exists(expected):
            os.remove(expected)
    except Exception:
        pass

    # Monta um formulário Elisp que ajusta as opções só para esta exportação
    toc_sym  = "nil" if no_toc  else "t"
    nums_sym = "nil" if no_nums else "t"
    elisp = f"(progn (require 'ox-latex) (let ((org-export-with-toc {toc_sym}) (org-export-with-section-numbers {nums_sym})) (org-latex-export-to-pdf)))"

    cmd = ["emacs", "--batch", org_path, "--eval", elisp]

    r = subprocess.run(cmd, cwd=org_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if r.returncode != 0 or not os.path.exists(expected):
        raise RuntimeError(f"Falha ao renderizar ORG: {org_path}\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}")

    if out_pdf:
        shutil.copyfile(expected, out_pdf)
        return out_pdf
    return expected

def _concat_pdfs(paths: List[str], out_path: str) -> None:
    if not PdfMerger:
        raise RuntimeError("Instale 'pypdf' (ou 'PyPDF2') para concatenar PDFs: pip install pypdf")
    merger = PdfMerger()
    try:
        for p in paths:
            if not p or not os.path.exists(p):
                continue
            merger.append(str(p))
        os.makedirs(str(Path(out_path).parent), exist_ok=True)
        with open(out_path, "wb") as f:
            merger.write(f)
    finally:
        merger.close()

from reportlab.pdfbase.pdfmetrics import stringWidth

def _measure_col_width(strings, header_text,
                       font_body=FONT_REG, size_body=8.0,
                       font_header=FONT_BOLD, size_header=None,
                       padding=3) -> float:
    """Retorna a largura em pontos suficiente p/ a coluna (maior entre header e linhas) + padding."""
    size_header = float(size_header if size_header is not None else size_body)
    max_pts = stringWidth(str(header_text), font_header, size_header)
    for s in strings:
        max_pts = max(max_pts, stringWidth(str(s), font_body, size_body))
    return max_pts + 2*padding  # padding dos dois lados

def measure_col_pts(strings, header_text,
                    font_body=FONT_REG, size_body=None,
                    font_header=FONT_BOLD, size_header=None,
                    padding=2.0) -> float:
    """
    Wrapper global para calcular largura de coluna em pontos,
    herdando TABLE_FONT_SIZE / TABLE_HEADER_FONT_SIZE por padrão.
    """
    sb = float(size_body if size_body is not None else TABLE_FONT_SIZE)
    if size_header is None:
        sh = float(TABLE_HEADER_FONT_SIZE if TABLE_HEADER_FONT_SIZE is not None else sb)
    else:
        sh = float(size_header)
    return _measure_col_width(strings, header_text,
                              font_body=font_body, size_body=sb,
                              font_header=font_header, size_header=sh,
                              padding=padding)

def _render_math_to_png(tex_src: str, font_size: float = 10.0, dpi: Optional[int] = None) -> str:
    """
    Renderiza uma expressão LaTeX (subset do mathtext do Matplotlib) em PNG transparente
    e retorna o caminho do arquivo.
    """
    dpi = int(dpi or GLOBAL_FIG_DPI or 300)
    # garante delimitadores para mathtext
    tex = tex_src.strip()
    if not (tex.startswith("$") and tex.endswith("$")):
        tex = f"${tex}$"

    fig = plt.figure(figsize=(0.01, 0.01), dpi=dpi)
    fig.patch.set_alpha(0.0)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")

    txt = ax.text(0.0, 0.0, tex, fontsize=font_size, color="black")
    fig.canvas.draw()
    bbox = txt.get_window_extent(renderer=fig.canvas.get_renderer())
    # converte de pixels para inches
    w_in = max(bbox.width / dpi, 0.001)
    h_in = max(bbox.height / dpi, 0.001)
    fig.set_size_inches(w_in, h_in)

    # salva
    os.makedirs(EXPORT_DIR, exist_ok=True)
    fd, out_path = tempfile.mkstemp(prefix="math_", suffix=".png", dir=EXPORT_DIR)
    os.close(fd)
    fig.savefig(out_path, dpi=dpi, transparent=True, bbox_inches="tight", pad_inches=0.0)
    plt.close(fig); plt.close('all')
    return out_path


from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER
# ...

def _xml_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

_LATEXISH_MAP = [
    (r"\displaystyle", ""), (r"\textstyle", ""),
    (r"\left", ""), (r"\right", ""),
    (r"\,", " "), (r"\;", " "), (r"\:", " "), (r"\!", ""),
    (r"\cdot", "*"), (r"\times", "x"), (r"\mid", "|"),
    (r"\geq", ">="), (r"\ge", ">="), (r"\leq", "<="), (r"\le", "<="),
    (r"\Pr", "Pr"), (r"\mathrm{Pr}", "Pr"),
    (r"\alpha", "alpha"), (r"\beta", "beta"), (r"\rho", "rho"),
    (r"\sum", "SUM"), (r"\text{-}", "-"),
]

def _latexish_to_ascii(s: str) -> str:
    """Converte notação LaTeX-ish para ASCII (sem imagens)."""
    if not s:
        return s
    out = s

    # \frac / \tfrac / \dfrac -> (A)/(B)
    out = re.sub(r"\\[td]?frac\s*\{([^{}]+)\}\s*\{([^{}]+)\}", r"(\1)/(\2)", out)

    # acentos/ornamentos simples
    out = re.sub(r"\\hat\s*\{([^{}]+)\}",   r"\1_hat",   out)
    out = re.sub(r"\\bar\s*\{([^{}]+)\}",   r"\1_bar",   out)
    out = re.sub(r"\\tilde\s*\{([^{}]+)\}", r"\1_tilde", out)

    # subs/supers simples: a_{BR} -> a_BR ; a^{2} -> a^2
    out = re.sub(r"([A-Za-z0-9])\s*_\{([^{}]+)\}", r"\1_\2", out)
    out = re.sub(r"([A-Za-z0-9])\s*\^\{([^{}]+)\}", r"\1^\2", out)

    # \text{...} / \operatorname{...} -> ...
    out = re.sub(r"\\text\s*\{([^{}]+)\}",         r"\1", out)
    out = re.sub(r"\\operatorname\s*\{([^{}]+)\}", r"\1", out)

    for a, b in _LATEXISH_MAP:
        out = out.replace(a, b)

    out = out.replace("{", "").replace("}", "")
    out = re.sub(r"\s+", " ", out).strip()
    return out

def _comment_to_flowables_with_math(text: str,
                                    style: ParagraphStyle,
                                    doc_width: float,
                                    temp_paths: List[str],
                                    display_scale: float = 0.85,
                                    inline_scale: float = 0.45) -> List:
    r"""
    Converte texto com marcadores $$...$$ / \[...\] (display) e $...$ / \(...\) (inline)
    para Flowables. Inline vira *texto na mesma linha* com fonte Courier; display vira
    parágrafo centralizado em Courier. Sem LaTeX nem imagens.
    """
    if not text:
        return []

    pat = re.compile(
        r"(\$\$(?P<dollar_disp>.+?)\$\$|\\\[(?P<brack_disp>.+?)\\\]|"
        r"\$(?P<dollar_inline>.+?)\$|\\\((?P<brack_inline>.+?)\\\))",
        flags=re.DOTALL
    )

    flows: List = []
    # preserva parágrafos separados por linha em branco
    paragraphs = re.split(r"\n\s*\n", text.strip())

    for para in paragraphs:
        pos = 0
        buff_rl_parts: List[str] = []  # RL markup com <font name="Courier"> para inline

        for m in pat.finditer(para):
            start, end = m.span()

            # texto antes do marcador
            if start > pos:
                chunk = para[pos:start]
                if chunk:
                    buff_rl_parts.append(_xml_escape(chunk))

            disp = m.group("dollar_disp") or m.group("brack_disp")
            inln = m.group("dollar_inline") or m.group("brack_inline")
            formula_raw = (disp or inln or "").strip()
            if formula_raw:
                ascii_formula = _latexish_to_ascii(formula_raw)

                if disp:
                    # fecha o parágrafo acumulado antes do display
                    buff_text = "".join(buff_rl_parts).strip()
                    if buff_text:
                        flows.append(Paragraph(buff_text, style))
                        flows.append(Spacer(1, 2))
                    buff_rl_parts = []

                    code_style_disp = ParagraphStyle(
                        name=f"{style.name}_code_display",
                        parent=style,
                        fontName="Courier",
                        alignment=TA_CENTER,
                    )
                    flows.append(Paragraph(_xml_escape(ascii_formula), code_style_disp))
                    flows.append(Spacer(1, 4))
                else:
                    # inline: insere na mesma linha com Courier
                    # garante espaços de respiro
                    if buff_rl_parts and not buff_rl_parts[-1].endswith((" ", "(", "[")):
                        buff_rl_parts.append(" ")
                    buff_rl_parts.append(
                        f'<font name="Courier">{_xml_escape(ascii_formula)}</font>'
                    )
                    buff_rl_parts.append(" ")

            pos = end

        # cauda do parágrafo
        if pos < len(para):
            tail = para[pos:]
            if tail:
                buff_rl_parts.append(_xml_escape(tail))

        # flush do parágrafo (se sobrou algo)
        buff_text = "".join(buff_rl_parts).strip()
        if buff_text:
            flows.append(Paragraph(buff_text, style))
            flows.append(Spacer(1, 2))

    # remove Spacer final supérfluo
    if flows and isinstance(flows[-1], Spacer):
        flows.pop()

    return flows

# =======================
# ORG → Flowables simples
# =======================
ORG_META_PREFIXES = ("#+TITLE:", "#+EXPORT_FILE_NAME:", "#+AUTHOR:", "#+DATE:",
                     "#+LATEX_HEADER:", "#+OPTIONS:")

def _parse_org_meta(path: Optional[str]) -> Dict[str, str]:
    meta = {}
    if not path or not os.path.exists(path): 
        return meta
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                ln = raw.strip()
                if ln.upper().startswith("#+TITLE:"):
                    meta["title"] = ln.split(":",1)[1].strip()
                elif ln.upper().startswith("#+AUTHOR:"):
                    meta["author"] = ln.split(":",1)[1].strip()
                elif ln.upper().startswith("#+DATE:"):
                    meta["date"] = ln.split(":",1)[1].strip()
    except Exception:
        pass
    return meta

def _org_to_flowables(path: Optional[str], styles: Dict[str, ParagraphStyle]) -> List:
    """Conversão bem simples de .org: headings por nível de '*', listas '-' e parágrafos."""
    flows = []
    if not path or not os.path.exists(path):
        return flows
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.read().splitlines()
    except Exception:
        return flows

    buf_par = []
    buf_list = []

    def flush_par():
        nonlocal buf_par
        if buf_par:
            text = " ".join(buf_par).strip()
            if text:
                flows.append(Paragraph(_escape(text), styles["Body"]))
            buf_par = []

    def flush_list():
        nonlocal buf_list
        if buf_list:
            items = [ListItem(Paragraph(_escape(t), styles["Body"]), leftIndent=12) for t in buf_list]
            flows.append(ListFlowable(items, bulletType="bullet", start="•", leftIndent=12))
            buf_list = []

    for raw in lines:
        ln = raw.rstrip("\n")
        if not ln.strip():
            flush_par(); flush_list()
            flows.append(Spacer(1, 6))
            continue
        low = ln.strip()
        if any(low.startswith(p) for p in ORG_META_PREFIXES):
            continue
        m = re.match(r'^(\*+)\s+(.*)$', ln)
        if m:
            flush_par(); flush_list()
            stars, title = m.group(1), m.group(2).strip()
            lvl = len(stars)
            if lvl == 1:
                flows.append(Paragraph(_escape(title), styles["Heading1"]))
            elif lvl == 2:
                flows.append(Paragraph(_escape(title), styles["Heading2"]))
            else:
                flows.append(Paragraph(_escape(title), styles["Heading3"]))
            continue
        if ln.strip().startswith("- "):
            buf_list.append(ln.strip()[2:].strip())
            continue
        buf_par.append(ln.strip())
    flush_par(); flush_list()
    flows.append(Spacer(1, 10))
    return flows

# =======================
# Estatística básica
# =======================
def _log_binom_pmf(k: int, n: int, p: float) -> float:
    if p <= 0.0: return 0.0 if k == 0 else -np.inf
    if p >= 1.0: return 0.0 if k == n else -np.inf
    from math import lgamma, log
    return (lgamma(n+1)-lgamma(k+1)-lgamma(n-k+1) + k*log(p) + (n-k)*log(1.0-p))

def _binom_sf_one_sided(k_obs: int, n: int, p0: float) -> float:
    ks = np.arange(k_obs, n+1, dtype=int)
    lps = np.array([_log_binom_pmf(int(k), n, p0) for k in ks], dtype=float)
    m = np.max(lps)
    return float(np.exp(lps - m).sum() * np.exp(m))

def _wilson_ci(k: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n <= 0: return (0.0, 1.0)
    p = k/n
    denom = 1 + z*z/n
    center = (p + z*z/(2*n))/denom
    half = (z * math.sqrt((p*(1-p) + z*z/(4*n))/n))/denom
    lo = max(0.0, center-half)
    hi = min(1.0, center+half)
    return (lo, hi)

def _p_adjust_bh(pvals: np.ndarray) -> np.ndarray:
    n = len(pvals)
    order = np.argsort(pvals)
    ranked = pvals[order]
    q = np.empty(n, dtype=float)
    prev=1.0
    for i in range(n-1, -1, -1):
        val = ranked[i] * n / (i+1)
        prev = min(prev, val)
        q[i] = prev
    out = np.empty(n, dtype=float)
    out[order] = np.minimum(q, 1.0)
    return out

def _log_beta(a: float, b: float) -> float:
    from math import lgamma
    return lgamma(a)+lgamma(b)-lgamma(a+b)

def _log_betabinom_pmf(k: int, n: int, a: float, b: float) -> float:
    from math import lgamma
    return (lgamma(n+1)-lgamma(k+1)-lgamma(n-k+1)
            + _log_beta(k+a, n-k+b) - _log_beta(a,b))

def _betabin_sf_one_sided(k_obs: int, n: int, a: float, b: float) -> float:
    ks = np.arange(k_obs, n+1, dtype=int)
    lps = np.array([_log_betabinom_pmf(int(k), n, a, b) for k in ks], dtype=float)
    m = np.max(lps)
    return float(np.exp(lps - m).sum() * np.exp(m))

def _estimate_rho_mom(N: np.ndarray, NC: np.ndarray, p: float) -> float:
    Y = NC.astype(float)
    num = float(((Y - N*p)**2 - N*p*(1-p)).sum())
    den = float((N*p*(1-p)*(N-1)).sum())
    if den <= 0: return 0.0
    rho = num/den
    return float(min(max(rho, 0.0), 0.9999))

# =======================
# SQL helpers
# =======================
def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    r = conn.execute("SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?",(name,)).fetchone()
    return r is not None

def _cols(conn: sqlite3.Connection, table: str) -> set:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}

def _detect_schema(conn: sqlite3.Connection) -> Dict[str, Any]:
    table=None
    for t in ('analises','analises_atestmed'):
        if _table_exists(conn,t):
            table=t; break
    if not table: raise RuntimeError("Não encontrei as tabelas 'analises' nem 'analises_atestmed'.")
    cset = _cols(conn, table)
    if not _table_exists(conn,'peritos') or 'nomePerito' not in _cols(conn,'peritos'):
        raise RuntimeError("Tabela 'peritos' ausente ou sem 'nomePerito'.")
    if not _table_exists(conn,'indicadores'):
        raise RuntimeError("Tabela 'indicadores' ausente (precisa de scoreFinal).")
    motivo_col     = 'motivoNaoConformado' if 'motivoNaoConformado' in cset else None
    has_conformado = 'conformado' in cset
    date_col       = 'dataHoraIniPericia' if 'dataHoraIniPericia' in cset else None
    if not date_col:
        raise RuntimeError(f"Tabela '{table}' sem 'dataHoraIniPericia'.")
    has_protocolo  = 'protocolo' in cset
    has_protocolos = _table_exists(conn,'protocolos')
    return {
        'table':table,'motivo_col':motivo_col,'has_conformado':has_conformado,
        'date_col':date_col,'has_protocolo':has_protocolo,'has_protocolos_table':has_protocolos
    }

def _cond_nc_total(has_conf: bool, motivo_col: Optional[str]) -> str:
    if has_conf and motivo_col:
        return (" (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
                " OR (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
                "     AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) ")
    elif has_conf and not motivo_col:
        return " (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
    elif (not has_conf) and motivo_col:
        return (" (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
                "  AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) ")
    return " 0 "

def _fetch_perito_n_nc(conn: sqlite3.Connection, start: str, end: str, schema: Dict[str, Any]) -> pd.DataFrame:
    t=schema['table']; date_col=schema['date_col']; cond_nc=_cond_nc_total(schema['has_conformado'], schema['motivo_col'])
    use_pr=bool(schema.get('has_protocolo') and schema.get('has_protocolos_table'))
    join_prot="LEFT JOIN protocolos pr ON pr.protocolo = a.protocolo" if use_pr else ""
    sel_cr = "MAX(pr.cr) AS cr" if use_pr else "MAX(p.cr) AS cr"
    sel_dr = "MAX(pr.dr) AS dr" if use_pr else "MAX(p.dr) AS dr"
    sql=f"""
        SELECT p.nomePerito,
               COUNT(*) AS N,
               SUM(CASE WHEN {cond_nc} THEN 1 ELSE 0 END) AS NC,
               {sel_cr}, {sel_dr}
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE substr(a.{date_col},1,10) BETWEEN ? AND ?
         GROUP BY p.nomePerito
    """
    df=pd.read_sql_query(sql, conn, params=(start,end))
    for col in ("N","NC"): 
        if col in df.columns: df[col]=df[col].astype(int)
    return df

def _fetch_scores(conn: sqlite3.Connection) -> pd.DataFrame:
    sql = """
        SELECT i.perito AS siape, i.scoreFinal AS score_final, p.nomePerito
          FROM indicadores i
          JOIN peritos p ON p.siapePerito = i.perito
    """
    df=pd.read_sql_query(sql, conn)
    return df[["nomePerito","score_final"]].drop_duplicates()

def _compute_p_br_and_totals(conn: sqlite3.Connection, start: str, end: str, schema: Dict[str, Any]) -> Tuple[float,int,int]:
    t=schema['table']; date_col=schema['date_col']; cond_nc=_cond_nc_total(schema['has_conformado'], schema['motivo_col'])
    row=conn.execute(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN {cond_nc} THEN 1 ELSE 0 END) AS nc
          FROM {t} a
         WHERE substr(a.{date_col},1,10) BETWEEN ? AND ?
    """,(start,end)).fetchone()
    total=int(row[0] or 0); nc=int(row[1] or 0)
    p_br = (nc/total) if total>0 else 0.0
    return p_br,total,nc

def _prep_base(df_n: pd.DataFrame, df_scores: pd.DataFrame, p_br: float, alpha: float, min_analises: int) -> pd.DataFrame:
    m=df_n.merge(df_scores, on="nomePerito", how="left")
    if min_analises and "N" in m.columns:
        m=m.loc[m["N"]>=int(min_analises)].copy()
    m["E_raw"]=m["NC"] - m["N"]*float(p_br)
    m["E"]=np.maximum(0, m["E_raw"])
    m["E"]=np.ceil(m["E"]).astype(int)
    m["IV_vagas"]=np.ceil(float(alpha)*m["E"]).astype(int)
    return m

def _elbow_cutoff_score(df: pd.DataFrame) -> Optional[float]:
    tmp=df.dropna(subset=["score_final"]).copy()
    if tmp.empty: return None
    ss=np.sort(tmp["score_final"].unique())[::-1]
    if len(ss)==1: return float(ss[0])
    yy=[tmp.loc[tmp["score_final"]>=s,"IV_vagas"].sum() for s in ss]
    y=np.array(yy,dtype=float)
    y=(y - y.min())/(y.max()-y.min()+1e-9)
    x=np.linspace(0,1,num=len(ss))
    x0,y0=x[0],y[0]; x1,y1=x[-1],y[-1]
    denom=math.hypot(x1-x0,y1-y0)+1e-9
    dist=np.abs((y1-y0)*x - (x1-x0)*y + x1*y0 - y1*x0)/denom
    return float(ss[int(dist.argmax())])

# =======================
# PNGs
# =======================
def _save_fig(fig, name: str) -> str:
    path=os.path.join(EXPORT_DIR, name)
    fig.savefig(path, bbox_inches='tight', dpi=GLOBAL_FIG_DPI)
    plt.close(fig); plt.close('all')
    return path

def exportar_png_top(df_sel: pd.DataFrame, meta: Dict[str, Any], label_maxlen: int=18, label_fontsize: int=8) -> Optional[str]:
    if df_sel.empty: return None
    topn=int(meta.get("topn",10))
    # pior→melhor pelo critério IV, then E, then NC, then N asc
    g=df_sel.sort_values(["IV_vagas","E","NC","N"], ascending=[False,False,False,True]).head(topn)
    labels=[str(x) for x in g["nomePerito"].tolist()]
    labels=[x if len(x)<=label_maxlen else (x[:max(1,label_maxlen-1)]+"…") for x in labels]
    labels=[titlecase_pt(x) for x in labels]
    vals=g["IV_vagas"].astype(int).tolist()
    fig,ax=_mkfig(max(7.8, len(labels)*0.60), 5.2)
    ax.bar(labels, vals, edgecolor='black')
    ax.set_ylabel("Vagas presenciais (IV)")
    ax.set_title(f"Impacto na Fila — Top {topn} Peritos (impacto negativo) | {meta['start']} a {meta['end']}")
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    y_max = max(vals) if vals else 0
    for i,v in enumerate(vals):
        ax.text(i, v + (y_max*0.01 if y_max else 0.3), f"{int(v)}", ha='center', va='bottom', fontsize=max(7,label_fontsize-1))
    plt.xticks(rotation=45, ha='right')
    parts=[f"IV sel: {int(meta.get('iv_total_sel',0))}"]
    if meta.get("iv_total_period") is not None: parts.append(f"IV período: {int(meta['iv_total_period'])}")
    if meta.get("peso_sel") is not None and meta.get("iv_total_period"): parts.append(f"peso: {float(meta['peso_sel'])*100:.1f}%")
    if meta.get("delta_tmea_sel") is not None: parts.append(f"ΔTMEA sel≈ {int(meta['delta_tmea_sel'])} d")
    if meta.get("delta_tmea_period") is not None: parts.append(f"ΔTMEA período≈ {int(meta['delta_tmea_period'])} d")
    if meta.get("tmea_br") is not None: parts.append(f"TMEA base: {float(meta['tmea_br']):.0f} d")
    ax.text(0.98, 0.98, " | ".join(parts), transform=ax.transAxes, ha='right', va='top', fontsize=9,
            bbox=dict(facecolor='white', alpha=0.8, edgecolor='black'))
    return _save_fig(fig, f"impacto_top_peritos_{meta['start']}_a_{meta['end']}.png")

def plot_curva_cotovelo(df: pd.DataFrame, s_star: Optional[float], start: str, end: str,
                        iv_selected: Optional[int], iv_periodo: Optional[int],
                        peso_selected: Optional[float], delta_tmea_sel: Optional[int],
                        delta_tmea_periodo: Optional[int], tmea_br: Optional[float]) -> Optional[str]:
    tmp = df.dropna(subset=["score_final"]).copy()
    if tmp.empty: return None
    ss = np.sort(tmp["score_final"].astype(float).unique())[::-1]
    y = np.array([tmp.loc[tmp["score_final"] >= s, "IV_vagas"].sum() for s in ss], dtype=float)

    fig, ax = _mkfig(7.8, 5.0)
    ax.plot(ss, y, marker="o")
    if s_star is not None and not (math.isinf(float(s_star)) or math.isnan(float(s_star))):
        ax.axvline(float(s_star), linestyle="--")
        ax.text(float(ss.min()), (y.max()*0.05 if y.size else 0.05), f"S*={float(s_star):.2f}",
                rotation=90, va="bottom", ha="right", fontsize=9)
    ax.set_xlabel("Score Final (corte)")
    ax.set_ylabel("Impacto acumulado (vagas)")
    ax.set_title(f"Curva de Impacto negativo x Score — {start} a {end}")
    ax.grid(True, axis="both", linestyle="--", alpha=0.4)

    parts=[]
    if iv_selected is not None: parts.append(f"IV sel: {int(iv_selected)}")
    if iv_periodo is not None: parts.append(f"IV período: {int(iv_periodo)}")
    if (peso_selected is not None) and (iv_periodo is not None) and iv_periodo!=0:
        parts.append(f"peso: {float(peso_selected)*100:.1f}%")
    if delta_tmea_sel is not None: parts.append(f"ΔTMEA sel≈ {int(delta_tmea_sel)} d")
    if delta_tmea_periodo is not None: parts.append(f"ΔTMEA período≈ {int(delta_tmea_periodo)} d")
    if tmea_br is not None: parts.append(f"TMEA base: {float(tmea_br):.0f} d")
    if parts:
        ax.text(0.98, 0.98, " | ".join(parts), transform=ax.transAxes, ha="right", va="top", fontsize=9,
                bbox=dict(facecolor="white", alpha=0.8, edgecolor="black"))
    return _save_fig(fig, f"impacto_curva_cotovelo_{start}_a_{end}.png")

def exportar_png_tornado(meta: Dict[str, Any], delta: Dict[str, float], alpha_frac: float, pbr_pp: float) -> Optional[str]:
    labels=[f"α -{alpha_frac*100:.0f}%", f"α +{alpha_frac*100:.0f}%",
            f"p_BR -{pbr_pp*100:.0f} p.p.", f"p_BR +{pbr_pp*100:.0f} p.p."]
    base=meta["peso_sel"]*100.0
    vals=[ (delta.get("alpha_minus",base)-base), (delta.get("alpha_plus",base)-base),
           (delta.get("pbr_minus",base)-base), (delta.get("pbr_plus",base)-base) ]
    y=np.arange(len(labels))
    fig,ax=_mkfig(7.2, 4.8)
    ax.barh(y, vals, edgecolor='black'); ax.set_yticks(y); ax.set_yticklabels(labels)
    ax.axvline(0, linestyle='--', linewidth=1)
    ax.set_xlabel("Variação no peso (p.p.)"); ax.set_title(f"Sensibilidade do peso — {meta['start']} a {meta['end']}")
    for i,v in enumerate(vals):
        ax.text(v + (0.2 if v>=0 else -0.2), i, f"{v:+.1f}", va='center', ha='left' if v>=0 else 'right', fontsize=9)
    return _save_fig(fig, f"impacto_tornado_{meta['start']}_a_{meta['end']}.png")

def exportar_png_strat(df_strat_tot: pd.DataFrame, df_strat_sel: pd.DataFrame, by: str, meta: Dict[str, Any]) -> Optional[str]:
    if df_strat_tot.empty: return None
    idx=f"{by}_val"; m=df_strat_tot.merge(df_strat_sel, on=idx, how="left").fillna(0)
    # pior→melhor por IV_total
    m = m.sort_values(["IV_tot","N_tot","NC_tot"], ascending=[False,False,False])
    labels=m[idx].astype(str).tolist()
    iv_tot=m["IV_tot"].astype(int).tolist(); iv_sel=m["IV_sel"].astype(int).tolist()
    x=np.arange(len(labels)); width=0.4
    fig,ax=_mkfig(max(7.8, len(labels)*0.6), 5.0)
    ax.bar(x - width/2, iv_tot, width, label="IV_total", edgecolor="black")
    ax.bar(x + width/2, iv_sel, width, label="IV_sel", edgecolor="black")
    ax.set_xticks(x); ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Vagas (IV)"); ax.set_title(f"Impacto negativo por {by.upper()} — {meta['start']} a {meta['end']}")
    ax.legend(); ax.grid(axis="y", linestyle="--", alpha=0.5)
    hmax = max(iv_tot+iv_sel+[1])
    for i,(a,b) in enumerate(zip(iv_tot,iv_sel)):
        ax.text(i - width/2, a + hmax*0.01, str(int(a)), ha="center", va="bottom", fontsize=8)
        ax.text(i + width/2, b + hmax*0.01, str(int(b)), ha="center", va="bottom", fontsize=8)
    return _save_fig(fig, f"impacto_estratos_{by}_{meta['start']}_a_{meta['end']}.png")

# =======================
# Testes
# =======================
def run_test_binomial(df_all: pd.DataFrame, p_br: float) -> pd.DataFrame:
    out=df_all[["nomePerito","N","NC","E"]].copy()
    out["p_hat"]=out["NC"]/out["N"].replace(0,np.nan)
    pvals=[]; low=[]; high=[]
    for _,r in out.iterrows():
        n=int(r["N"]); k=int(r["NC"])
        pval=_binom_sf_one_sided(k, n, p_br) if n>0 else 1.0
        lo,hi=_wilson_ci(k,n)
        pvals.append(pval); low.append(lo); high.append(hi)
    out["p"]=np.array(pvals,dtype=float)
    out["wilson_low"]=np.array(low,dtype=float); out["wilson_high"]=np.array(high,dtype=float)
    out["q"]=_p_adjust_bh(out["p"].values)
    return out

def run_test_betabin(df_all: pd.DataFrame, p_br: float) -> Tuple[pd.DataFrame, float]:
    N=df_all["N"].astype(int).values
    NC=df_all["NC"].astype(int).values
    rho=_estimate_rho_mom(N, NC, p_br)
    rho = max(min(float(rho), 0.9999), 1e-9)
    ab = (1.0/rho) - 1.0
    a = float(p_br*ab); b=float((1.0-p_br)*ab)
    pvals=[]
    for n,k in zip(N.tolist(), NC.tolist()):
        pval=_betabin_sf_one_sided(int(k), int(n), a, b) if n>0 else 1.0
        pvals.append(pval)
    out=df_all[["nomePerito","N","NC","E"]].copy()  # garante coluna E (Excesso)
    out["p_hat"]=out["NC"]/out["N"].replace(0,np.nan)
    out["p_bb"]=np.array(pvals,dtype=float)
    out["q_bb"]=_p_adjust_bh(out["p_bb"].values)
    return out, float(rho)

def _parse_grid(s: str) -> List[float]:
    try:
        vals = [float(x) for x in re.split(r'[;, ]+', str(s).strip()) if x]
        vals = [v for v in vals if 0.0 < v < 1.0]
        return sorted(set(vals))
    except Exception:
        return [0.60, 0.70, 0.80, 0.85, 0.90, 0.95]

def _nc_outliers(df_all: pd.DataFrame,
                 p_br: float,
                 min_n: int = 50,
                 mode: str = "adaptive-fdr",
                 fixed_thresh: float = 0.90,
                 fdr_target: float = 0.05,
                 grid: Optional[List[float]] = None,
                 use_betabin: bool = True) -> Tuple[pd.DataFrame, Optional[float], float]:
    """
    Devolve (df_outliers, threshold_usado, fdr_estimada) com base em %NC e FDR.
    FDR estimada ≈ (soma de p-valores) / k no subconjunto selecionado.
    """
    if df_all.empty:
        return (df_all.copy(), None, float('nan'))

    m = df_all.copy()
    m = m.loc[m["N"].astype(int) >= int(min_n)].copy()
    if m.empty:
        return (m, None, float('nan'))

    m["p_hat"] = m["NC"].astype(float) / m["N"].replace(0, np.nan)

    # p-values (beta-binomial com MoM, ou binomial)
    pcol = "p_bb" if use_betabin else "p"
    if use_betabin:
        bb_df, _rho = run_test_betabin(m[["nomePerito","N","NC","E"]], p_br)
        m = m.merge(bb_df[["nomePerito","p_bb"]], on="nomePerito", how="left")
    else:
        b_df = run_test_binomial(m[["nomePerito","N","NC","E"]], p_br)
        m = m.merge(b_df[["nomePerito","p"]], on="nomePerito", how="left")

    # Seleção por modo
    if mode == "fixed":
        thr = float(fixed_thresh)
        sel = m.loc[m["p_hat"] >= thr].copy()
        k = max(1, sel.shape[0])
        fdr_est = float(sel[pcol].sum() / k) if not sel.empty else float('nan')
        return (sel, thr, fdr_est)

    # adaptive-fdr: pega o MENOR threshold da grade que mantém FDR ≤ alvo
    grid = grid or [0.60, 0.70, 0.80, 0.85, 0.90, 0.95]
    best_thr = None
    best_sel = pd.DataFrame()
    best_fdr = float('inf')
    for t in sorted(grid):
        s = m.loc[m["p_hat"] >= float(t)].copy()
        if s.empty:
            continue
        fdr_est = float(s[pcol].sum() / max(1, s.shape[0]))
        if fdr_est <= float(fdr_target):
            best_thr = float(t)
            best_sel = s
            best_fdr = fdr_est
            break  # menor t que satisfaz o alvo
    if best_thr is None:
        # fallback: se nada bater a meta, use o maior t da grade com menor FDR
        for t in sorted(grid):
            s = m.loc[m["p_hat"] >= float(t)].copy()
            if s.empty: 
                continue
            fdr_est = float(s[pcol].sum() / max(1, s.shape[0]))
            if fdr_est < best_fdr:
                best_thr = float(t); best_sel = s; best_fdr = fdr_est
        if best_thr is None:
            return (pd.DataFrame(columns=m.columns), None, float('nan'))
    return (best_sel, best_thr, best_fdr)

def run_permutation_weight(df_all: pd.DataFrame, df_sel: pd.DataFrame, alpha: float, p_br: float,
                           R: int, stratify_by: Optional[str]=None) -> Tuple[float, str]:
    if df_sel.empty or df_all.empty or R<=0:
        return (float('nan'), "")
    n_sel = df_sel.shape[0]
    iv_period = int(math.ceil(float(alpha)*float(df_all["NC"].sum())))
    rng=np.random.default_rng()
    weights=[]
    if stratify_by and stratify_by in df_all.columns:
        counts=df_sel[stratify_by].fillna("—").value_counts().to_dict()
        strata_groups={}
        for g,sub in df_all.groupby(df_all[stratify_by].fillna("—")):
            strata_groups[g]=sub.index.values
        for _ in range(int(R)):
            idx=[]
            for g,c in counts.items():
                pool=strata_groups.get(g, np.array([],dtype=int))
                if pool.size==0: continue
                pick=rng.choice(pool, size=min(c, pool.size), replace=False)
                idx.extend(list(pick))
            if len(idx)<n_sel:
                pool_extra=np.setdiff1d(df_all.index.values, np.array(idx,dtype=int), assume_unique=False)
                if pool_extra.size>0:
                    pick=rng.choice(pool_extra, size=(n_sel - len(idx)), replace=False)
                    idx.extend(list(pick))
            iv_s = int(df_all.loc[idx, "IV_vagas"].sum())
            w = (iv_s / iv_period) if iv_period>0 else 0.0
            weights.append(w)
    else:
        idx_all=df_all.index.values
        for _ in range(int(R)):
            idx=np.random.choice(idx_all, size=n_sel, replace=False)
            iv_s=int(df_all.loc[idx,"IV_vagas"].sum())
            w=(iv_s/iv_period) if iv_period>0 else 0.0
            weights.append(w)
    weights=np.array(weights, dtype=float)
    w_obs = (df_sel["IV_vagas"].sum()/iv_period) if iv_period>0 else 0.0
    pval = float((1.0 + (weights >= w_obs).sum()) / (len(weights)+1.0))
    fig,ax=_mkfig(7.2, 4.6)
    ax.hist(weights*100, bins=40, edgecolor='black')
    ax.axvline(w_obs*100, color='red', linestyle='--', label=f"w obs = {w_obs*100:.2f}%")
    ax.set_xlabel("Peso permutado (%)"); ax.set_ylabel("Frequência"); ax.set_title("Permutação do peso (w)")
    ax.legend(); plt.tight_layout()
    perm_png=os.path.join(EXPORT_DIR, f"perm_weight_hist_{n_sel}_{R}.png")
    fig.savefig(perm_png, bbox_inches='tight', dpi=GLOBAL_FIG_DPI); plt.close(fig); plt.close('all')
    return (pval, perm_png)

def run_psa(df_all_base: pd.DataFrame, df_sel_base: pd.DataFrame, alpha: float, p_br: float,
            total: int, nc: int, R: int, s_star: Optional[float], alpha_strength: float=50.0) -> Tuple[Tuple[float,float,float], str]:
    if R<=0 or df_all_base.empty:
        return ((float('nan'),float('nan'),float('nan')), "")
    rng=np.random.default_rng()
    a_al = max(alpha*alpha_strength, 1e-3); b_al=max((1.0-alpha)*alpha_strength, 1e-3)
    a_p  = nc + 1.0; b_p = (total - nc) + 1.0
    ws=[]
    for _ in range(int(R)):
        a_s = float(rng.beta(a_al, b_al))
        p_s = float(rng.beta(a_p,  b_p))
        m = df_all_base.copy()
        m["E_raw"]=m["NC"] - m["N"]*float(p_s)
        m["E"]=np.maximum(0, m["E_raw"])
        m["E"]=np.ceil(m["E"]).astype(int)
        m["IV_vagas"]=np.ceil(float(a_s)*m["E"]).astype(int)
        sel = m if s_star is None else m.loc[m["score_final"]>=s_star].copy()
        iv_period = int(math.ceil(a_s * float(m["NC"].sum())))
        iv_sel    = int(sel["IV_vagas"].sum()) if not sel.empty else 0
        w = (iv_sel/iv_period) if iv_period>0 else 0.0
        ws.append(w)
    ws=np.array(ws,dtype=float)
    p50=float(np.percentile(ws,50)); p2=float(np.percentile(ws,2.5)); p97=float(np.percentile(ws,97.5))
    fig,ax=_mkfig(7.2, 4.6)
    ax.hist(ws*100, bins=40, edgecolor='black')
    ax.axvline(p50*100, color='red', linestyle='--', label=f"mediana = {p50*100:.2f}%")
    ax.set_xlabel("Peso (w) em %"); ax.set_ylabel("Frequência"); ax.set_title("PSA — distribuição do peso (w)")
    ax.legend(); plt.tight_layout()
    psa_png=os.path.join(EXPORT_DIR, f"psa_weight_hist_{R}.png")
    fig.savefig(psa_png, bbox_inches='tight', dpi=GLOBAL_FIG_DPI); plt.close(fig); plt.close('all')
    return ((p50,p2,p97), psa_png)

# =======================
# Tabelas (ReportLab)
# =======================
def _df_to_table(
    df: pd.DataFrame,
    columns: List[str],
    col_rename: Dict[str, str],
    col_widths: List[float],
    font_size: float = 8,
    header_font_size: float = None,
    repeat_header: bool = True
) -> Table:
    # renomeia colunas se necessário
    if col_rename:
        df = df.rename(columns=col_rename)

    # normaliza tamanhos de fonte
    try:
        font_size = float(font_size)
    except Exception:
        font_size = 8.0

    if header_font_size is None:
        # usa a global se existir; senão, o mesmo do corpo
        header_font_size = float(globals().get("TABLE_HEADER_FONT_SIZE", font_size))
    else:
        header_font_size = float(header_font_size)

    # formata linhas
    rows = []
    for _, row in df.iterrows():
        out = []
        for col in columns:
            val = row[col] if col in row.index else ""
            if isinstance(val, (int, np.integer)):
                out.append(f"{int(val)}")
            elif isinstance(val, (float, np.floating)):
                out.append(_fmt2s(val))
            else:
                out.append(_escape(str(val)))
        rows.append(out)

    # cabeçalho + dados
    header = [_escape(c) for c in columns]
    data = [header] + rows

    # garante que há uma largura para cada coluna
    w = list(col_widths[:len(columns)])
    if len(w) < len(columns):
        fallback_w = (col_widths[-1] if col_widths else 1.5 * cm)
        w += [fallback_w] * (len(columns) - len(w))

    tbl = Table(
        data,
        colWidths=w,
        repeatRows=1 if repeat_header else 0,
        hAlign="LEFT"
    )

    # paddings menores ajudam quando font_size ~7–8
    pad = 2 if font_size <= 8 else 3

    style = TableStyle([
        # header
        ("FONTNAME", (0,0), (-1,0), FONT_BOLD),
        ("FONTSIZE", (0,0), (-1,0), header_font_size),
        ("BACKGROUND", (0,0), (-1,0), colors.whitesmoke),
        ("TEXTCOLOR", (0,0), (-1,0), colors.black),
        ("ALIGN", (0,0), (-1,0), "CENTER"),

        # corpo
        ("FONTNAME", (0,1), (-1,-1), FONT_REG),
        ("FONTSIZE", (0,1), (-1,-1), font_size),
        ("ALIGN", (1,1), (-1,-1), "RIGHT"),   # números à direita
        ("ALIGN", (0,1), (0,-1), "LEFT"),     # 1ª coluna (Perito/Estrato) à esquerda

        # grade, paddings e vertical alignment
        ("GRID", (0,0), (-1,-1), 0.25, colors.lightgrey),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("LEFTPADDING", (0,0), (-1,-1), pad),
        ("RIGHTPADDING", (0,0), (-1,-1), pad),
        ("TOPPADDING", (0,0), (-1,-1), pad),
        ("BOTTOMPADDING", (0,0), (-1,-1), pad),
    ])
    tbl.setStyle(style)
    return tbl

def _image_flowable(path: str, max_w: float) -> Image:
    im = Image(path)
    iw, ih = im.wrap(0,0)
    if iw > max_w:
        scale = max_w/iw
        im._restrictSize(max_w, ih*scale)
    return im

# =======================
# Build PDF
# =======================
def _header_footer(canvas, doc, meta: Dict[str, Any]):
    canvas.saveState()
    canvas.setFont(FONT_REG, 8.5)
    x_left  = doc.leftMargin
    x_right = doc.pagesize[0] - doc.rightMargin
    # posiciona o rodapé dentro da área da margem inferior
    y = max(0.5*cm, doc.bottomMargin - 0.5*cm)
    text_left  = f"Impacto na Fila — {meta['start']} a {meta['end']}"
    text_right = datetime.now().strftime("%d/%m/%Y %H:%M")
    canvas.drawString(x_left,  y, text_left)
    canvas.drawRightString(x_right, y, text_right)
    canvas.restoreState()

def _build_cover(meta: Dict[str, Any], styles: Dict[str, ParagraphStyle],
                 header_org: Optional[str]=None) -> List:
    m = _parse_org_meta(header_org)
    titulo = m.get("title") or f"Impacto na Fila — {meta['start']} a {meta['end']}"
    autor_curto = m.get("author") or "Gustavo M. Mendes de Tarso"
    date_str = m.get("date") or datetime.now().strftime("%d/%m/%Y")

    instituicoes = [
        "Ministério da Previdência Social",
        "Secretaria do Regime Geral da Previdência Social",
        "Departamento de Perícia Médica Federal",
        "Coordenação-Geral de Assuntos Corporativos e Disseminação de Conhecimento",
    ]
    autor_completo = "Gustavo Magalhães Mendes de Tarso"

    cover: List = []
    cover.append(Spacer(1, 2.2*cm))
    cover.append(Paragraph(_escape(titulo), styles["CoverTitle"]))
    cover.append(Spacer(1, 0.6*cm))
    cover.append(Paragraph(_escape(autor_curto), styles["CoverSub"]))
    cover.append(Paragraph(_escape(date_str), styles["CoverSmall"]))
    cover.append(Spacer(1, 0.6*cm))
    for linha in instituicoes:
        cover.append(Paragraph(_escape(linha), styles["CoverSmall"]))
    cover.append(Spacer(1, 0.4*cm))
    cover.append(Paragraph(_escape(autor_completo), styles["CoverSmall"]))
    cover.append(PageBreak())
    return cover

def build_pdf(meta: Dict[str, Any], 
              df_sel: pd.DataFrame, 
              tests: Dict[str, Any],
              pngs: Dict[str, Optional[str]],
              df_strat_tot: pd.DataFrame, 
              df_strat_sel: pd.DataFrame,
              pdf_path: str, 
              header_org: Optional[str]=None, 
              front_org: Optional[str]=None,
              comments: Optional[Dict[str, str]] = None) -> None:
    """
    Gera o PDF do Impacto na Fila com blocos e comentários GPT.
    Agora renderiza fórmulas LaTeX em imagens embutidas.
    """
    doc = SimpleDocTemplate(
        pdf_path, pagesize=A4,
        leftMargin=PDF_MARGIN_LEFT_CM*cm,
        rightMargin=PDF_MARGIN_RIGHT_CM*cm,
        topMargin=PDF_MARGIN_TOP_CM*cm,
        bottomMargin=PDF_MARGIN_BOTTOM_CM*cm,
    )

    # helper local para medir a largura da 1ª coluna
    def _measure_col(strings, header_text,
                     font_body=FONT_REG, size_body: float = None,
                     font_header=FONT_BOLD, size_header: float = None,
                     padding: float = 2.0) -> float:
        sb = float(size_body if size_body is not None else TABLE_FONT_SIZE)
        sh = float(size_header if size_header is not None else TABLE_HEADER_FONT_SIZE or sb)
        max_pts = pdfmetrics.stringWidth(str(header_text), font_header, sh)
        for s in strings:
            max_pts = max(max_pts, pdfmetrics.stringWidth(str(s), font_body, sb))
        return max_pts + 2.0 * padding

    story: List = []
    tmp_imgs: List[str] = []  # para limpar depois

    # ====== CAPA ======
    if header_org:
        story += _build_cover(meta, STYLES, header_org=header_org)

    # ====== FRONT ======
    if front_org:
        story += _org_to_flowables(front_org, STYLES)
        story.append(PageBreak())

    # ====== TÍTULO ======
    story.append(Paragraph(f"Impacto na Fila — {meta['start']} a {meta['end']}", STYLES["Title"]))
    story.append(Spacer(1, 4))

    # ====== RESUMO ======
    resumo_bits = [
        f"<b>α:</b> {_fmt2s(meta['alpha'])}",
        f"<b>p_BR:</b> {_fmt2s(meta['p_br']*100)}%",
        f"<b>Seleção:</b> {meta.get('select_src', 'impact')}",
        f"<b>Score-cut:</b> {(_fmt2s(meta['score_cut']) if meta.get('score_cut') is not None else 'auto/nd')}",
        f"<b>IV sel (vagas):</b> {int(meta.get('iv_total_sel',0))}",
        f"<b>IV período (vagas):</b> {int(meta.get('iv_total_period',0))}",
        f"<b>peso sel:</b> {_fmt2s(float(meta.get('peso_sel',0))*100)}%",
    ]
    if meta.get('peso_ci'):
        lo, hi = meta['peso_ci']
        resumo_bits.append(f"<b>IC95% (peso):</b> [{_fmt2s(lo*100)}%; {_fmt2s(hi*100)}%]")
    if meta.get('delta_tmea_sel') is not None:
        resumo_bits.append(f"<b>ΔTMEA sel≈</b> {int(meta['delta_tmea_sel'])} d")
    if meta.get('delta_tmea_period') is not None:
        resumo_bits.append(f"<b>ΔTMEA período≈</b> {int(meta['delta_tmea_period'])} d")

    story.append(Paragraph(" &nbsp;&nbsp; ".join(resumo_bits), STYLES["Body"]))
    if comments and comments.get("resumo"):
        story.append(Spacer(1, 4))
        story += _comment_to_flowables_with_math(comments["resumo"], STYLES["Body"], doc.width, tmp_imgs)
    story.append(Spacer(1, 6))

    # ====== TOP ======
    if pngs.get("top"):
        story.append(Paragraph("Top peritos por impacto negativo (vagas)", STYLES["Heading2"]))
        story.append(_image_flowable(pngs["top"], max_w=doc.width * PDF_IMG_FRAC))
        if comments and comments.get("top"):
            story.append(Spacer(1, 4))
            story += _comment_to_flowables_with_math(comments["top"], STYLES["Small"], doc.width, tmp_imgs)
        story.append(Spacer(1, 10))

    # ====== COTOVELO ======
    if pngs.get("cotovelo"):
        story.append(Paragraph("Curva de impacto negativo x corte de score (S*)", STYLES["Heading2"]))
        story.append(_image_flowable(pngs["cotovelo"], max_w=doc.width * PDF_IMG_FRAC))
        if comments and comments.get("cotovelo"):
            story.append(Spacer(1, 4))
            story += _comment_to_flowables_with_math(comments["cotovelo"], STYLES["Small"], doc.width, tmp_imgs)
        story.append(Spacer(1, 10))

    # ====== TORNADO ======
    if pngs.get("tornado"):
        story.append(Paragraph("Sensibilidade determinística do peso (tornado)", STYLES["Heading2"]))
        story.append(_image_flowable(pngs["tornado"], max_w=doc.width * PDF_IMG_FRAC))
        if comments and comments.get("tornado"):
            story.append(Spacer(1, 4))
            story += _comment_to_flowables_with_math(comments["tornado"], STYLES["Small"], doc.width, tmp_imgs)
        story.append(Spacer(1, 10))

    # ====== ESTRATOS ======
    if (not df_strat_tot.empty):
        title = f"Sumário estratificado por {meta.get('by','').upper()}" if meta.get('by') else "Sumário por estratos"
        story.append(Paragraph(title, STYLES["Heading2"]))

        if meta.get('by'):
            idx = f"{meta['by']}_val"
            merged = df_strat_tot.merge(df_strat_sel, on=idx, how="left").fillna(0)
            merged = merged.sort_values(["IV_tot","N_tot","NC_tot"], ascending=[False,False,False])
            show = pd.DataFrame({
                "Estrato": merged[idx].astype(str),
                "N_total": merged["N_tot"].astype(int),
                "NC_total": merged["NC_tot"].astype(int),
                "E_total": merged["E_tot"].astype(int),
                "IV_total": merged["IV_tot"].astype(int),
                "IV_sel": merged.get("IV_sel", 0).astype(int),
            })
        else:
            show = df_strat_tot.copy()
            if "Estrato" not in show.columns:
                show.insert(0, "Estrato", range(1, len(show)+1))

        keep = [c for c in ["Estrato","N_total","NC_total","E_total","IV_total","IV_sel"] if c in show.columns]

        other_fixed_all = [1.5*cm, 1.7*cm, 1.6*cm, 1.8*cm, 1.5*cm]
        other = other_fixed_all[:len(keep)-1]
        estr_strings = show["Estrato"].astype(str).tolist()
        estr_w = _measure_col(estr_strings, "Estrato",
                              size_body=TABLE_FONT_SIZE,
                              size_header=TABLE_HEADER_FONT_SIZE,
                              padding=2.0)
        max_estr_w = max(2.0*cm, doc.width - sum(other) - 0.2*cm)
        estr_w = min(estr_w, max_estr_w)
        widths = [estr_w] + other

        tbl = _df_to_table(
            show[keep], keep, {}, widths,
            font_size=TABLE_FONT_SIZE,
            header_font_size=TABLE_HEADER_FONT_SIZE,
            repeat_header=True
        )
        story.append(tbl)

        if pngs.get("estratos"):
            story.append(Spacer(1, 6))
            story.append(_image_flowable(pngs["estratos"], max_w=doc.width * PDF_IMG_FRAC))

        if comments and comments.get("estratos"):
            story.append(Spacer(1, 4))
            story += _comment_to_flowables_with_math(comments["estratos"], STYLES["Small"], doc.width, tmp_imgs)
        story.append(Spacer(1, 10))

    # ====== SELECIONADOS ======
    story.append(Paragraph("Peritos selecionados (alto risco por impacto)", STYLES["Heading2"]))
    if df_sel.empty:
        story.append(Paragraph("— Sem dados elegíveis —", STYLES["Body"]))
    else:
        base = df_sel.sort_values(["IV_vagas","E","NC","N"], ascending=[False,False,False,True]).copy()
        base["Perito"] = base["nomePerito"].astype(str).map(abbreviate_middle_names_pt)
        base["Score"]  = pd.to_numeric(base.get("score_final", np.nan), errors="coerce").round(2)
        show = base[["Perito","cr","dr","N","NC","E","IV_vagas","Score"]].rename(columns={
            "cr":"CR", "dr":"DR", "IV_vagas":"IV (vagas)"
        })

        other = [1.6*cm, 1.6*cm, 1.2*cm, 1.2*cm, 1.6*cm, 1.8*cm, 1.6*cm]
        perito_strings = show["Perito"].astype(str).tolist()
        perito_w = _measure_col(perito_strings, "Perito",
                                size_body=TABLE_FONT_SIZE,
                                size_header=TABLE_HEADER_FONT_SIZE,
                                padding=2.0)
        max_perito_w = max(2.0*cm, doc.width - sum(other) - 0.2*cm)
        perito_w = min(perito_w, max_perito_w)
        widths = [perito_w] + other

        tbl = _df_to_table(
            show, list(show.columns), {}, widths,
            font_size=TABLE_FONT_SIZE,
            header_font_size=TABLE_HEADER_FONT_SIZE,
            repeat_header=True
        )
        story.append(tbl)
    story.append(Spacer(1, 10))

    # ====== TESTES ======
    story.append(Paragraph("Validação estatística", STYLES["Heading1"]))

    # — Binomial
    dfb = tests.get("binomial_df")
    if isinstance(dfb, pd.DataFrame) and not dfb.empty:
        story.append(Paragraph("Teste binomial (unilateral, p_i > p_BR) com FDR (BH)", STYLES["Heading2"]))
        story.append(Paragraph(f"p_BR: {_fmt2s(meta['p_br']*100)}%", STYLES["Body"]))
        mapa = {
            "nomePerito": "Perito", "N": "N", "NC": "NC",
            "p_hat": "p_obs", "wilson_low": "IC95%_low", "wilson_high": "IC95%_high",
            "E": "Excesso", "p": "p", "q": "q(BH)",
        }
        dfb_sorted = dfb.sort_values(by=["q","p","E"], ascending=[True,True,False], kind="mergesort")
        tmp = dfb_sorted.rename(columns=mapa).copy()
        tmp["IC95%"] = tmp.apply(lambda r: f"[{_fmt2s(r['IC95%_low'])}; {_fmt2s(r['IC95%_high'])}]", axis=1)
        show = tmp[["Perito","N","NC","p_obs","IC95%","Excesso","p","q(BH)"]].copy()
        show["Perito"] = show["Perito"].astype(str).map(abbreviate_middle_names_pt)
        show["p"] = show["p"].map(_fmtp); show["q(BH)"] = show["q(BH)"].map(_fmtp)

        other = [1.1*cm, 1.1*cm, 1.6*cm, 2.2*cm, 1.6*cm, 1.2*cm, 1.6*cm]
        perito_strings = show["Perito"].astype(str).tolist()
        perito_w = _measure_col(perito_strings, "Perito",
                                size_body=TABLE_FONT_SIZE,
                                size_header=TABLE_HEADER_FONT_SIZE,
                                padding=2.0)
        max_perito_w = max(2.0*cm, doc.width - sum(other) - 0.2*cm)
        perito_w = min(perito_w, max_perito_w)
        widths = [perito_w] + other

        tbl = _df_to_table(show, list(show.columns), {}, widths,
                           font_size=TABLE_FONT_SIZE,
                           header_font_size=TABLE_HEADER_FONT_SIZE,
                           repeat_header=True)
        story.append(tbl)
        if comments and comments.get("binomial"):
            story.append(Spacer(1, 4))
            story += _comment_to_flowables_with_math(comments["binomial"], STYLES["Small"], doc.width, tmp_imgs)
        story.append(Spacer(1, 8))

    # — Beta-binomial
    dfbb = tests.get("betabin_df")
    if isinstance(dfbb, pd.DataFrame) and not dfbb.empty:
        story.append(Paragraph("Teste beta-binomial (overdispersão) com FDR (BH)", STYLES["Heading2"]))
        rho = tests.get("rho_mom", float('nan'))
        story.append(Paragraph(f"rho (MoM): {_fmt2s(rho)}", STYLES["Body"]))
        mapa = {
            "nomePerito": "Perito", "N": "N", "NC": "NC",
            "E": "Excesso", "p_bb": "p_betaBin", "q_bb": "q(BH)",
        }
        dfbb_sorted = dfbb.sort_values(by=["q_bb","p_bb","E"], ascending=[True,True,False], kind="mergesort")
        show = dfbb_sorted.rename(columns=mapa)[["Perito","N","NC","Excesso","p_betaBin","q(BH)"]].copy()
        show["Perito"] = show["Perito"].astype(str).map(abbreviate_middle_names_pt)
        show["p_betaBin"] = show["p_betaBin"].map(_fmtp); show["q(BH)"] = show["q(BH)"].map(_fmtp)

        other = [1.1*cm, 1.1*cm, 1.6*cm, 1.9*cm, 1.6*cm]
        perito_strings = show["Perito"].astype(str).tolist()
        perito_w = _measure_col(perito_strings, "Perito",
                                size_body=TABLE_FONT_SIZE,
                                size_header=TABLE_HEADER_FONT_SIZE,
                                padding=2.0)
        max_perito_w = max(2.0*cm, doc.width - sum(other) - 0.2*cm)
        perito_w = min(perito_w, max_perito_w)
        widths = [perito_w] + other

        tbl = _df_to_table(show, list(show.columns), {}, widths,
                           font_size=TABLE_FONT_SIZE,
                           header_font_size=TABLE_HEADER_FONT_SIZE,
                           repeat_header=True)
        story.append(tbl)
        if comments and comments.get("betabin"):
            story.append(Spacer(1, 4))
            story += _comment_to_flowables_with_math(comments["betabin"], STYLES["Small"], doc.width, tmp_imgs)
        story.append(Spacer(1, 8))

    # — Permutação
    if pngs.get("perm"):
        story.append(Paragraph("Teste de permutação do peso (w)", STYLES["Heading2"]))
        pperm = tests.get("perm_p", float('nan')); Rperm = tests.get("perm_R", 0)
        story.append(Paragraph(f"Observado: w = {_fmt2s(float(meta.get('peso_sel',0))*100)}% &nbsp;&nbsp; "
                               f"Réplicas: {int(Rperm)} &nbsp;&nbsp; p-valor: {_fmtp(pperm)}", STYLES["Body"]))
        story.append(_image_flowable(pngs["perm"], max_w=doc.width * PDF_IMG_FRAC))
        if comments and comments.get("perm"):
            story.append(Spacer(1, 4))
            story += _comment_to_flowables_with_math(comments["perm"], STYLES["Small"], doc.width, tmp_imgs)
        story.append(Spacer(1, 8))

    # — CMH
    if tests.get("cmh") is not None:
        ormh, x2_mh, p_mh, by_key = tests["cmh"]
        story.append(Paragraph(f"CMH 2×2×K (estratificado por {str(by_key).upper()})", STYLES["Heading2"]))
        story.append(Paragraph(f"OR_MH ≈ {_fmt2s(ormh)} &nbsp;&nbsp; p ≈ {_fmtp(p_mh)}", STYLES["Body"]))
        if comments and comments.get("cmh"):
            story.append(Spacer(1, 4))
            story += _comment_to_flowables_with_math(comments["cmh"], STYLES["Small"], doc.width, tmp_imgs)
        story.append(Spacer(1, 8))

    # — PSA
    if pngs.get("psa"):
        story.append(Paragraph("PSA — distribuição do peso (w)", STYLES["Heading2"]))
        ci = tests.get("psa_ci", (None,None,None))
        if ci and all(c is not None and not (isinstance(c,float) and math.isnan(c)) for c in ci):
            med, lo, hi = ci
            story.append(Paragraph(f"Réplicas: {int(tests.get('psa_R',0))} &nbsp;&nbsp; "
                                   f"Mediana: {_fmt2s(med*100)}% &nbsp;&nbsp; "
                                   f"IC95%: [{_fmt2s(lo*100)}%; {_fmt2s(hi*100)}%]", STYLES["Body"]))
        story.append(_image_flowable(pngs["psa"], max_w=doc.width * PDF_IMG_FRAC))
        if comments and comments.get("psa"):
            story.append(Spacer(1, 4))
            story += _comment_to_flowables_with_math(comments["psa"], STYLES["Small"], doc.width, tmp_imgs)

    # ====== build & cleanup ======
    doc.build(story,
              onFirstPage=lambda c,d: _header_footer(c,d,meta),
              onLaterPages=lambda c,d: _header_footer(c,d,meta))

    # remove imagens temporárias de fórmulas
    for p in tmp_imgs:
        try: os.remove(p)
        except Exception: pass

# =======================
# Estratos
# =======================
def _compute_strata(df_all: pd.DataFrame, df_sel: pd.DataFrame, by: str, alpha: float, p_br: float) -> Tuple[pd.DataFrame,pd.DataFrame]:
    col_map={"cr":"cr","dr":"dr"}; col=col_map.get(by)
    if not col or col not in df_all.columns:
        return (pd.DataFrame(columns=[f"{by}_val","N_tot","NC_tot","E_tot","IV_tot"]),
                pd.DataFrame(columns=[f"{by}_val","IV_sel"]))
    def agg_block(df):
        g=df.groupby(col, dropna=False).agg(N_tot=("N","sum"), NC_tot=("NC","sum")).reset_index()
        g[f"{by}_val"]=g[col].fillna("—").astype(str)
        g["E_tot"]=np.ceil(np.maximum(0, g["NC_tot"] - g["N_tot"]*float(p_br))).astype(int)
        g["IV_tot"]=np.ceil(float(alpha)*g["E_tot"]).astype(int)
        return g[[f"{by}_val","N_tot","NC_tot","E_tot","IV_tot"]]
    tot=agg_block(df_all.copy())
    sel=df_sel.groupby(col, dropna=False)["IV_vagas"].sum().reset_index().rename(columns={"IV_vagas":"IV_sel"})
    sel[f"{by}_val"]=sel[col].fillna("—").astype(str)
    sel=sel[[f"{by}_val","IV_sel"]].copy(); sel["IV_sel"]=sel["IV_sel"].astype(int)
    return tot, sel

def _kpi_top10_names(start: str, end: str, min_analises: int = 50) -> List[str]:
    """
    Devolve a lista (até 10) de peritos pelo algoritmo de Top-10 KPI.
    Usa a função 'pegar_10_piores_peritos' do make_kpi_report.py, se disponível.
    """
    if _kpi_pegar_top10 is None:
        print("[warn] make_kpi_report.pegar_10_piores_peritos não disponível.")
        return []
    try:
        df_kpi = _kpi_pegar_top10(start, end, min_analises=min_analises)
        if df_kpi is None or df_kpi.empty:
            return []
        col_nome = "nomePerito" if "nomePerito" in df_kpi.columns else (
            "nome" if "nome" in df_kpi.columns else df_kpi.columns[0]
        )
        nomes = [str(x) for x in df_kpi[col_nome].dropna().tolist()]
        return nomes[:10]
    except Exception as e:
        print(f"[warn] Falha ao obter Top-10 KPI: {e}")
        return []

# =======================
# CLI / MAIN
# =======================
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Impacto na Fila — PDF e PNGs (sem LaTeX)."
    )

    # ── Janela temporal ─────────────────────────────────────────────
    grp_time = ap.add_argument_group("Janela temporal")
    grp_time.add_argument('--start', required=True, help="Data inicial (YYYY-MM-DD)")
    grp_time.add_argument('--end',   required=True, help="Data final (YYYY-MM-DD)")

    # ── Modo de execução (excludentes) ──────────────────────────────
    grp_mode = ap.add_mutually_exclusive_group(required=True)
    grp_mode.add_argument('--perito', help='Executa para um único perito (nome exato).')
    grp_mode.add_argument('--top10', action='store_true', help='Executa para TopN com corte de score.')

    # ── Parâmetros principais ───────────────────────────────────────
    grp_params = ap.add_argument_group("Parâmetros")
    grp_params.add_argument('--min-analises', type=int, default=50)
    grp_params.add_argument('--topn', type=int, default=10)
    grp_params.add_argument('--alpha', type=float, default=0.8)
    grp_params.add_argument('--pbr', type=float, default=None)

    # ── Parâmetros de tempo médio / capacidade ──────────────────────
    grp_tmea = ap.add_argument_group("TMEA / Capacidade")
    grp_tmea.add_argument('--tmea-br', type=float, default=60.0)
    grp_tmea.add_argument('--cap-br', type=float, default=None)
    grp_tmea.add_argument('--att-br', type=int,   default=None)

    # ── Sensibilidade e estratos ────────────────────────────────────
    grp_sens = ap.add_argument_group("Sensibilidade e Estratos")
    grp_sens.add_argument('--sens-plot', action='store_true')
    grp_sens.add_argument('--sens-alpha-frac', type=float, default=0.10)
    grp_sens.add_argument('--sens-pbr-pp', type=float, default=0.02)
    grp_sens.add_argument('--by', choices=['cr','dr'])

    # ── Testes estatísticos ─────────────────────────────────────────
    grp_tests = ap.add_argument_group("Testes estatísticos")
    grp_tests.add_argument('--test-binomial', action='store_true')
    grp_tests.add_argument('--betabin', action='store_true')
    grp_tests.add_argument('--permute-weight', type=int, default=0, metavar="N")
    grp_tests.add_argument('--permute-stratify', action='store_true')
    grp_tests.add_argument('--cmh', type=str, default=None, help="Ex.: cr ou by=cr")
    grp_tests.add_argument('--psa', type=int, default=0, metavar="N")
    grp_tests.add_argument('--psa-alpha-strength', type=float, default=50.0)
    grp_tests.add_argument('--bootstrap-peso', type=int, default=0)
    grp_tests.add_argument('--bootstrap-recalc-sstar', action='store_true')
    
    # ── Outliers por %NC + Apêndice ───────────────────────────────────
    grp_nc = ap.add_argument_group("Outliers por % NC")
    grp_nc.add_argument('--nc-outlier-mode', choices=['off','fixed','adaptive-fdr'],
                        default='adaptive-fdr',
                        help="Regra p/ %NC alto: 'fixed' usa --nc-outlier-thresh; 'adaptive-fdr' escolhe o menor t que mantém FDR≤alvo.")
    grp_nc.add_argument('--nc-outlier-thresh', type=float, default=0.90,
                        help="Limite fixo de %NC (ex.: 0.90). Usado só em --nc-outlier-mode=fixed.")
    grp_nc.add_argument('--nc-outlier-min-n', type=int, default=50,
                        help="N mínimo para testar %NC alto.")
    grp_nc.add_argument('--nc-outlier-fdr', type=float, default=0.05,
                        help="Alvo de FDR para o modo adaptive-fdr.")
    grp_nc.add_argument('--nc-outlier-grid', default="0.60,0.70,0.80,0.85,0.90,0.95",
                        help="Grade de thresholds de %NC para varrer (vírgula separada).")
    grp_nc.add_argument('--nc-outlier-add-to', choices=['kpi','impact','both'], default='both',
                        help="Para onde adicionar os outliers de %NC (além da seleção por score) quando o apêndice estiver desligado.")

    # >>> Apêndice (booleans com default=ON e opção --no-...)
    grp_nc.add_argument('--appendix-nc-outliers', dest='appendix_nc_outliers',
                        action=BooleanOptionalAction, default=True,
                        help="Gera um apêndice separado com peritos outliers por %NC (N ≥ --nc-outlier-min-n) após o bloco KPI.")
    grp_nc.add_argument('--appendix-nc-explain', dest='appendix_nc_explain',
                        action=BooleanOptionalAction, default=True,
                        help="Insere comentário explicativo via GPT (cálculos, fórmulas, testes e gráficos) no apêndice.")

    # ── Aparência dos gráficos ─────────────────────────────────────────
    grp_fig = ap.add_argument_group("Gráficos")
    grp_fig.add_argument('--fig-scale', type=float, default=1.0,
                         help="Multiplicador do tamanho dos gráficos (ex.: 0.8 = 80%).")
    grp_fig.add_argument('--fig-dpi', type=int, default=300,
                         help="DPI para os PNGs (afeta nitidez/tamanho do arquivo).")
    grp_fig.add_argument('--pdf-img-frac', type=float, default=1.0,
                         help="Fator da largura das imagens dentro do PDF (ex.: 0.8 = 80% de doc.width).")
                         
    # ── Layout / Margens de página ─────────────────────────────────────
    grp_page = ap.add_argument_group("Layout/Página")
    grp_page.add_argument('--page-margin', type=float, default=None,
                          help="Margem única (cm) para todos os lados.")
    grp_page.add_argument('--page-margin-left', type=float, default=None)
    grp_page.add_argument('--page-margin-right', type=float, default=None)
    grp_page.add_argument('--page-margin-top', type=float, default=None)
    grp_page.add_argument('--page-margin-bottom', type=float, default=None)
    
    # ── Tamanho da fonte ─────────────────────────────────────
    grp_tbl = ap.add_argument_group("Tabelas (PDF)")
    grp_tbl.add_argument('--table-font-size', type=float, default=8.0,
                         help="Tamanho da fonte do corpo das tabelas.")
    grp_tbl.add_argument('--table-header-font-size', type=float, default=None,
                         help="Tamanho da fonte do cabeçalho das tabelas (padrão: igual ao corpo).")
    
    # ── Fonte da seleção Top-10 ───────────────────────────────────────── 
    grp_sel_src = ap.add_argument_group("Seleção Top-10 (fonte)")
    grp_sel_src.add_argument(
        "--select-src",
        choices=["impact", "kpi", "both"],
        default="impact",
        help="Origem da seleção Top-10: 'impact' (padrão), 'kpi' (algoritmo do KPI) ou 'both' (união)."
    )
    grp_sel_src.add_argument(
        "--kpi-min-analises",
        type=int, default=50,
        help="Mínimo de análises para o Top-10 KPI (padrão: 50)."
    )

    # ── Exportação ──────────────────────────────────────────────────
    grp_exp = ap.add_argument_group("Exportação")
    grp_exp.add_argument('--export-png', action='store_true')
    grp_exp.add_argument('--export-pdf', action='store_true')

    # ── Front matter como texto (ReportLab) ─────────────────────────
    grp_front_text = ap.add_argument_group("Front (texto .org convertido em parágrafos)")
    grp_front_text.add_argument('--header-org', default=None,
                                help="Arquivo .org com cabeçalho/capa para converter como texto.")
    grp_front_text.add_argument('--front-org',  default=None,
                                help="Arquivo .org com texto de abertura para converter como texto.")

    # ── Front matter como PDF (injetado antes do corpo) ────────────
    grp_front_pdf = ap.add_argument_group("Front (PDFs e .org renderizados)")
    grp_front_pdf.add_argument('--front-pdf', action='append', default=[],
                               help='Um ou mais PDFs prontos para inserir no início (pode repetir).')
    grp_front_pdf.add_argument('--front-org-render', action='append', default=[],
                               help='Um ou mais .org a renderizar via Emacs/ox-latex e inserir (pode repetir).')

    # ── .org único combinando header+texto ────────────────────
    grp_ht = ap.add_argument_group(".org único (capa + texto)")
    grp_ht.add_argument('--header-and-text', action='store_true',
                        help='Usa um único .org (capa + texto) renderizado e inserido no início.')
    grp_ht.add_argument('--header-and-text-file', default=None,
                        help='Caminho para o .org único (ex.: header_and_text.org).')

    # ── Comentários GPT ─────────────────────────────────────────────
    grp_gpt = ap.add_mutually_exclusive_group()
    grp_gpt.add_argument("--gpt-comments", dest="gpt_comments",
                         action="store_true", default=True,
                         help="Ativa comentários automáticos do GPT no PDF (padrão).")
    grp_gpt.add_argument("--no-gpt-comments", dest="gpt_comments",
                         action="store_false",
                         help="Desativa comentários automáticos do GPT.")

    # ── Atalho de testes ────────────────────────────────────────────
    ap.add_argument('--all-tests', action='store_true',
                    help='Atalho: --test-binomial --betabin --permute-weight 5000 --cmh by=cr --psa 10000')

    return ap.parse_args()

def _calc_delta_tmea(iv_total: float, tmea_br: Optional[float]=None, cap_br: Optional[float]=None, att_br: Optional[float]=None) -> Optional[int]:
    try:
        iv=float(iv_total)
        if cap_br is not None and cap_br>0:
            return int(math.ceil(iv/float(cap_br)))
        if (att_br is not None and att_br>0) and (tmea_br is not None and tmea_br>0):
            return int(math.ceil(float(tmea_br)*(iv/float(att_br))))
    except Exception:
        pass
    return None

def main():
    args = parse_args()
    
    # aplicar escala e dpi globais dos gráficos + fator de largura das imagens no PDF
    global GLOBAL_FIG_SCALE, GLOBAL_FIG_DPI, PDF_IMG_FRAC
    GLOBAL_FIG_SCALE = float(getattr(args, "fig_scale", 1.0) or 1.0)
    GLOBAL_FIG_DPI   = int(getattr(args, "fig_dpi", FIG_DPI_DEFAULT) or FIG_DPI_DEFAULT)
    PDF_IMG_FRAC     = max(0.1, min(float(getattr(args, "pdf_img_frac", 1.0) or 1.0), 1.0))

    # === margens de página vindas das flags ===
    global PDF_MARGIN_LEFT_CM, PDF_MARGIN_RIGHT_CM, PDF_MARGIN_TOP_CM, PDF_MARGIN_BOTTOM_CM
    m_all = getattr(args, "page_margin", None)
    if m_all is not None:
        val = float(m_all)
        PDF_MARGIN_LEFT_CM = PDF_MARGIN_RIGHT_CM = PDF_MARGIN_TOP_CM = PDF_MARGIN_BOTTOM_CM = val

    m_left   = getattr(args, "page_margin_left",   None)
    m_right  = getattr(args, "page_margin_right",  None)
    m_top    = getattr(args, "page_margin_top",    None)
    m_bottom = getattr(args, "page_margin_bottom", None)
    if m_left   is not None: PDF_MARGIN_LEFT_CM   = float(m_left)
    if m_right  is not None: PDF_MARGIN_RIGHT_CM  = float(m_right)
    if m_top    is not None: PDF_MARGIN_TOP_CM    = float(m_top)
    if m_bottom is not None: PDF_MARGIN_BOTTOM_CM = float(m_bottom)

    # === tamanhos de fonte das tabelas via flags ===
    global TABLE_FONT_SIZE, TABLE_HEADER_FONT_SIZE
    # corpo
    try:
        TABLE_FONT_SIZE = float(getattr(args, "table_font_size", TABLE_FONT_SIZE))
    except NameError:
        TABLE_FONT_SIZE = float(getattr(args, "table_font_size", 8.0) or 8.0)
    # cabeçalho
    thf = getattr(args, "table_header_font_size", None)
    try:
        TABLE_HEADER_FONT_SIZE = (float(thf) if thf is not None else None)
    except NameError:
        TABLE_HEADER_FONT_SIZE = (float(thf) if thf is not None else None)

    # Atalho --all-tests
    if args.all_tests:
        args.test_binomial   = True
        args.betabin         = True
        args.permute_weight  = args.permute_weight or 5000
        args.cmh             = args.cmh or "by=cr"
        args.psa             = args.psa or 10000

    # ------------------------
    # helpers locais p/ bloco
    # ------------------------
    def _metrics_for_block(df_all, df_sel, meta_base):
        NC_period = int(df_all["NC"].sum()) if not df_all.empty else 0
        iv_total_period = int(math.ceil(float(args.alpha) * float(NC_period)))
        iv_total_sel    = int(df_sel["IV_vagas"].sum()) if not df_sel.empty else 0
        peso_sel        = (iv_total_sel / iv_total_period) if iv_total_period > 0 else 0.0
        delta_tmea_sel     = _calc_delta_tmea(iv_total_sel,     tmea_br=args.tmea_br, cap_br=args.cap_br, att_br=args.att_br)
        delta_tmea_period  = _calc_delta_tmea(iv_total_period,  tmea_br=args.tmea_br, cap_br=args.cap_br, att_br=args.att_br)
        m = dict(meta_base)
        m.update({
            'iv_total_sel': iv_total_sel,
            'iv_total_period': iv_total_period,
            'peso_sel': peso_sel,
            'delta_tmea_sel': delta_tmea_sel,
            'delta_tmea_period': delta_tmea_period,
            'tmea_br': args.tmea_br,
        })
        return m

    def _export_pngs_for_block(df_all, df_sel, meta):
        png_top = exportar_png_top(df_sel, meta) if args.export_png and not df_sel.empty else None
        png_cotovelo = plot_curva_cotovelo(
            df_all, meta.get('score_cut'), args.start, args.end,
            meta.get('iv_total_sel'), meta.get('iv_total_period'), meta.get('peso_sel'),
            meta.get('delta_tmea_sel'), meta.get('delta_tmea_period'), args.tmea_br
        ) if args.export_png and not df_all.empty else None

        # Tornado (sensibilidade determinística)
        png_tornado = None
        sens_delta: Dict[str, float] = {}
        if args.sens_plot and not df_all.empty:
            # variação em α
            for sign, key in [(-1, "alpha_minus"), (1, "alpha_plus")]:
                a2 = max(min(args.alpha * (1.0 + sign * args.sens_alpha_frac), 0.9999), 1e-4)
                m2 = df_all.copy()
                m2["E_raw"]   = m2["NC"] - m2["N"]*float(meta['p_br'])
                m2["E"]       = np.maximum(0, m2["E_raw"])
                m2["E"]       = np.ceil(m2["E"]).astype(int)
                m2["IV_vagas"]= np.ceil(float(a2)*m2["E"]).astype(int)
                sel2 = m2 if meta['score_cut'] is None else m2.loc[m2["score_final"]>=meta['score_cut']].copy()
                ivp2 = int(math.ceil(a2 * float(m2["NC"].sum())))
                ivs2 = int(sel2["IV_vagas"].sum()) if not sel2.empty else 0
                sens_delta[key] = (ivs2 / ivp2) * 100.0 if ivp2 > 0 else 0.0
            # variação em p_BR (± p.p.)
            for sign, key in [(-1, "pbr_minus"), (1, "pbr_plus")]:
                p2 = max(min(meta['p_br'] + sign * args.sens_pbr_pp, 0.9999), 1e-6)
                m2 = df_all.copy()
                m2["E_raw"]   = m2["NC"] - m2["N"]*float(p2)
                m2["E"]       = np.maximum(0, m2["E_raw"])
                m2["E"]       = np.ceil(m2["E"]).astype(int)
                m2["IV_vagas"]= np.ceil(float(args.alpha)*m2["E"]).astype(int)
                sel2 = m2 if meta['score_cut'] is None else m2.loc[m2["score_final"]>=meta['score_cut']].copy()
                ivp2 = int(math.ceil(args.alpha * float(m2["NC"].sum())))
                ivs2 = int(sel2["IV_vagas"].sum()) if not sel2.empty else 0
                sens_delta[key] = (ivs2 / ivp2) * 100.0 if ivp2 > 0 else 0.0
            png_tornado = exportar_png_tornado(meta, sens_delta, args.sens_alpha_frac, args.sens_pbr_pp)
        meta['sens_delta'] = sens_delta

        # Estratos
        df_strat_tot = pd.DataFrame(); df_strat_sel = pd.DataFrame(); png_strat = None
        if args.by:
            df_strat_tot, df_strat_sel = _compute_strata(df_all, df_sel, by=args.by, alpha=args.alpha, p_br=meta['p_br'])
            if args.export_png:
                png_strat = exportar_png_strat(df_strat_tot, df_strat_sel, args.by, meta)

        return {
            "pngs": {
                "top": png_top,
                "cotovelo": png_cotovelo,
                "tornado": png_tornado,
                "estratos": png_strat,
                "perm": None,
                "psa": None,
            },
            "df_strat_tot": df_strat_tot,
            "df_strat_sel": df_strat_sel,
        }

    def _run_tests_for_block(df_all, df_sel, meta):
        tests: Dict[str, Any] = {}
        # binomial / beta-binomial (sobre todos — como no padrão)
        if args.test_binomial and not df_all.empty:
            tests['binomial_df'] = run_test_binomial(df_all, meta['p_br'])
        if args.betabin and not df_all.empty:
            bb_df, rho_mom = run_test_betabin(df_all, meta['p_br'])
            tests['betabin_df'] = bb_df
            tests['rho_mom'] = rho_mom
        # permutação (só faz sentido p/ blocos com seleção)
        if args.permute_weight and not df_all.empty and not df_sel.empty:
            stratify = args.by if (args.permute_stratify and args.by in ('cr','dr')) else None
            perm_p, perm_png = run_permutation_weight(df_all, df_sel, args.alpha, meta['p_br'], args.permute_weight, stratify_by=stratify)
            tests['perm_p'] = perm_p; tests['perm_png'] = perm_png; tests['perm_R'] = int(args.permute_weight)
        # CMH (informativo)
        if args.cmh and not df_all.empty:
            cmh_arg = args.cmh.strip()
            cmh_by = cmh_arg.split("=",1)[1].strip().lower() if cmh_arg.lower().startswith("by=") else cmh_arg.strip().lower()
            if cmh_by in ("cr","dr"):
                tabs=[]
                for g, sub_all in df_all.groupby(cmh_by):
                    sub_sel = df_sel.loc[df_sel[cmh_by]==g]
                    N_sel = int(sub_sel["N"].sum()); NC_sel=int(sub_sel["NC"].sum())
                    N_all = int(sub_all["N"].sum()); NC_all=int(sub_all["NC"].sum())
                    N_nsel = N_all - N_sel; NC_nsel = NC_all - NC_sel
                    conf_sel = max(N_sel - NC_sel, 0); conf_nsel = max(N_nsel - NC_nsel, 0)
                    a=NC_sel; b=conf_sel; c=NC_nsel; d=conf_nsel
                    n=a+b+c+d if (a+b+c+d)>0 else 1
                    tabs.append((a*d/max(n,1e-9), b*c/max(n,1e-9)))
                num=sum(x for x,_ in tabs); den=sum(y for _,y in tabs)
                ormh = (num/den) if den>0 else float('inf')
                x2 = 0.0; pval = 0.5
                tests['cmh'] = (ormh, x2, pval, cmh_by)
        # PSA (probabilística)
        if args.psa and not df_all.empty:
            total_calc = int(df_all["N"].sum())
            nc_calc    = int(df_all["NC"].sum())
            try:
                with sqlite3.connect(DB_PATH) as conn:
                    schema = _detect_schema(conn)
                    p_tmp, total_calc, nc_calc = _compute_p_br_and_totals(conn, args.start, args.end, schema)
                    _ = p_tmp
            except Exception:
                pass
            ci, psa_png = run_psa(df_all, df_sel, args.alpha, meta['p_br'],
                                  int(total_calc or df_all["N"].sum()),
                                  int(nc_calc or df_all["NC"].sum()),
                                  R=int(args.psa), s_star=meta.get('score_cut'), alpha_strength=float(args.psa_alpha_strength))
            tests['psa_ci'] = ci; tests['psa_R'] = int(args.psa)
            return tests, psa_png
        return tests, None

    def _comments_for_block(df_all, df_sel, meta, pngs, tests, df_strat_tot, df_strat_sel):
        comments: Dict[str, str] = {}
        if not args.gpt_comments:
            return comments
        try:
            import json as _json
            # resumo
            comments["resumo"] = comentar_impacto_fila(df_all.copy(), df_sel.copy(), dict(meta), call_api=True)
            # top
            if pngs.get("top") and not df_sel.empty:
                topn = int(meta.get("topn", 10))
                g = df_sel.sort_values(["IV_vagas","E","NC","N"], ascending=[False,False,False,True]).head(topn)
                payload = {
                    "periodo": [meta["start"], meta["end"]],
                    "top": [
                        {"perito": str(r["nomePerito"]), "cr": str(r.get("cr","—")), "dr": str(r.get("dr","—")),
                         "IV": int(r["IV_vagas"]), "E": int(r["E"]), "NC": int(r["NC"]), "N": int(r["N"])}
                        for _, r in g.iterrows()
                    ],
                }
                user = (
                    "Escreva um parágrafo curto (≤130 palavras, pt-BR) para o gráfico 'Top peritos por impacto negativo'. "
                    "Explique a concentração do IV (vagas) entre os listados, a ordem pior→melhor, cite 2–3 nomes do topo "
                    "e ressalve que o IV deriva de excesso de NC ponderado por α. Dados:\n\n"
                    + _json.dumps(payload, ensure_ascii=False)
                )
                out = chamar_gpt(SYSTEM_PROMPT, user, call_api=True, model="gpt-4o-mini", temperature=0.2)
                comments["top"] = (out.get("comment") or "").strip()
            # cotovelo
            if pngs.get("cotovelo"):
                payload = {
                    "periodo": [meta["start"], meta["end"]],
                    "s_star": (None if meta.get("score_cut") is None else float(meta["score_cut"])),
                    "iv_periodo": int(meta.get("iv_total_period",0) or 0),
                    "iv_sel": int(meta.get("iv_total_sel",0) or 0),
                    "peso": float(meta.get("peso_sel",0.0) or 0.0),
                    "delta_tmea_sel": meta.get("delta_tmea_sel"),
                    "delta_tmea_period": meta.get("delta_tmea_period"),
                }
                user = ("Comente a curva 'Impacto x corte de score (S*)' em até 110 palavras. "
                        "Explique o papel de S* como joelho, o ganho acumulado de IV acima do corte e o peso (%). "
                        "Evite causalidade. Dados:\n\n" + _json.dumps(payload, ensure_ascii=False))
                out = chamar_gpt(SYSTEM_PROMPT, user, call_api=True, model="gpt-4o-mini", temperature=0.2)
                comments["cotovelo"] = (out.get("comment") or "").strip()
            # tornado
            if pngs.get("tornado"):
                delta = dict(meta.get("sens_delta") or {})
                user = ("Interprete o gráfico de sensibilidade 'tornado' em ≤90 palavras, explicando como variações de α e p_BR "
                        "alteram o peso (%) dos selecionados, destacando a direção e a magnitude mais sensível. Dados:\n\n"
                        + _json.dumps(delta, ensure_ascii=False))
                out = chamar_gpt(SYSTEM_PROMPT, user, call_api=True, model="gpt-4o-mini", temperature=0.2)
                comments["tornado"] = (out.get("comment") or "").strip()
            # estratos
            if args.by and not df_strat_tot.empty:
                rows = []
                key = f"{meta['by']}_val"
                merged = df_strat_tot.merge(df_strat_sel, on=key, how="left").fillna(0)
                merged = merged.sort_values(["IV_tot","N_tot","NC_tot"], ascending=[False,False,False])
                for _, r in merged.head(6).iterrows():
                    rows.append({
                        "estrato": str(r.get(key, "—")),
                        "IV_total": int(r.get("IV_tot", 0)),
                        "IV_sel": int(r.get("IV_sel", 0)),
                        "N_total": int(r.get("N_tot", 0)),
                        "NC_total": int(r.get("NC_tot", 0)),
                    })
                payload = {"periodo": [meta["start"], meta["end"]], "by": meta.get("by"), "rows": rows}
                user = (
                    "Resuma a visão por estratos em ≤120 palavras, destacando os estratos com maior IV_total e "
                    "quanto do IV_sel se concentra neles, sem inferência causal. Dados:\n\n"
                    + _json.dumps(payload, ensure_ascii=False)
                )
                out = chamar_gpt(SYSTEM_PROMPT, user, call_api=True, model="gpt-4o-mini", temperature=0.2)
                comments["estratos"] = (out.get("comment") or "").strip()
            # binomial
            dfb = tests.get("binomial_df")
            if isinstance(dfb, pd.DataFrame) and not dfb.empty:
                sig = int((dfb["q"] <= 0.05).sum())
                payload = {"periodo": [meta["start"], meta["end"]], "p_br": float(meta["p_br"]),
                           "significativos_q05": sig, "n": int(dfb.shape[0])}
                user = ("Explique, em ≤90 palavras, o teste binomial (unilateral) com FDR: quantos peritos têm q≤0,05 "
                        "e a relação com o excesso (E). Evite causalidade. Dados:\n\n" + _json.dumps(payload, ensure_ascii=False))
                out = chamar_gpt(SYSTEM_PROMPT, user, call_api=True, model="gpt-4o-mini", temperature=0.2)
                comments["binomial"] = (out.get("comment") or "").strip()
            # betabin
            dfbb = tests.get("betabin_df")
            if isinstance(dfbb, pd.DataFrame) and not dfbb.empty:
                sig = int((dfbb["q_bb"] <= 0.05).sum())
                payload = {"periodo": [meta["start"], meta["end"]], "rho": float(tests.get("rho_mom") or 0.0),
                           "significativos_q05": sig, "n": int(dfbb.shape[0])}
                user = ("Em ≤90 palavras, resuma o teste beta-binomial: overdispersão (ρ), número com q≤0,05 e "
                        "consistência com o binomial. Evite causalidade. Dados:\n\n" + _json.dumps(payload, ensure_ascii=False))
                out = chamar_gpt(SYSTEM_PROMPT, user, call_api=True, model="gpt-4o-mini", temperature=0.2)
                comments["betabin"] = (out.get("comment") or "").strip()
            # perm
            if pngs.get("perm") and tests.get("perm_p") is not None:
                payload = {"periodo": [meta["start"], meta["end"]],
                           "w_obs_pct": float(meta.get("peso_sel", 0.0) * 100.0),
                           "R": int(tests.get("perm_R", 0)),
                           "p": float(tests.get("perm_p", float("nan")))}
                user = ("Em ≤90 palavras, interprete o histograma da permutação do peso (w): w observado, número de "
                        "réplicas e p-valor; p pequeno sugere concentração acima do acaso. Dados:\n\n"
                        + _json.dumps(payload, ensure_ascii=False))
                out = chamar_gpt(SYSTEM_PROMPT, user, call_api=True, model="gpt-4o-mini", temperature=0.2)
                comments["perm"] = (out.get("comment") or "").strip()
            # cmh
            if tests.get("cmh") is not None:
                ormh, x2, pval, by_key = tests["cmh"]
                payload = {"periodo": [meta["start"], meta["end"]], "by": by_key,
                           "OR_MH": float(ormh), "p": float(pval)}
                user = ("Em ≤80 palavras, descreva o CMH por estratos (valor informativo): cite OR_MH e p, "
                        "sem inferir causalidade. Dados:\n\n" + _json.dumps(payload, ensure_ascii=False))
                out = chamar_gpt(SYSTEM_PROMPT, user, call_api=True, model="gpt-4o-mini", temperature=0.2)
                comments["cmh"] = (out.get("comment") or "").strip()
            # psa
            if pngs.get("psa") and tests.get("psa_ci") is not None:
                ci = tests.get("psa_ci", (None, None, None))
                payload = {"periodo": [meta["start"], meta["end"]], "R": int(tests.get("psa_R", 0)),
                           "mediana_pct": float(((ci[0] or 0.0)) * 100.0),
                           "ic95_pct": [float(((ci[1] or 0.0)) * 100.0), float(((ci[2] or 0.0)) * 100.0)]}
                user = ("Em ≤90 palavras, comente a PSA do peso (w): mediana e IC95%; o intervalo reflete incerteza dos "
                        "parâmetros. Dados:\n\n" + _json.dumps(payload, ensure_ascii=False))
                out = chamar_gpt(SYSTEM_PROMPT, user, call_api=True, model="gpt-4o-mini", temperature=0.2)
                comments["psa"] = (out.get("comment") or "").strip()
        except Exception:
            comments = {}
        return comments

    def _rename_pngs(pngs: Dict[str, Optional[str]], suffix: str) -> Dict[str, Optional[str]]:
        out = {}
        for k, p in (pngs or {}).items():
            if p and os.path.exists(p):
                root, ext = os.path.splitext(p)
                newp = f"{root}_{suffix}{ext}"
                try:
                    shutil.move(p, newp)
                    out[k] = newp
                except Exception:
                    out[k] = p
            else:
                out[k] = p
        return out

    # ======= Outliers por %NC — helpers locais =======
    def _parse_thresh_grid(s: str) -> List[float]:
        vals=[]
        for x in (s or "").split(","):
            x=x.strip()
            if not x: continue
            try:
                v=float(x)
                if 0.0 <= v <= 1.0:
                    vals.append(v)
            except Exception:
                pass
        if not vals:
            vals=[0.6,0.7,0.8,0.85,0.9,0.95]
        vals=sorted(set(vals))
        return vals

    def _binom_pvals_subset(df_sub: pd.DataFrame, p_br: float) -> np.ndarray:
        pvals=[]
        for _, r in df_sub.iterrows():
            n=int(r["N"]); k=int(r["NC"])
            pvals.append(_binom_sf_one_sided(k, n, p_br) if n>0 else 1.0)
        return np.array(pvals, dtype=float)

    def _detect_nc_outliers(df_all_base: pd.DataFrame, p_br: float) -> Tuple[pd.DataFrame, Dict[str,Any]]:
        mode = getattr(args, "nc_outlier_mode", "off")
        if mode == "off" or df_all_base.empty:
            return pd.DataFrame(columns=list(df_all_base.columns)+["p_hat","p_bin","q_BH"]), {"active": False}

        df_cand = df_all_base.copy()
        df_cand["p_hat"] = df_cand["NC"] / df_cand["N"].replace(0, np.nan)
        df_cand = df_cand.loc[df_cand["N"] >= int(getattr(args, "nc_outlier_min_n", 50))].copy()
        if df_cand.empty:
            return pd.DataFrame(columns=list(df_all_base.columns)+["p_hat","p_bin","q_BH"]), {"active": True, "n_cand": 0}

        fdr_target = float(getattr(args, "nc_outlier_fdr", 0.05))

        if mode == "fixed":
            t = float(getattr(args, "nc_outlier_thresh", 0.90))
            sub = df_cand.loc[df_cand["p_hat"] >= t].copy()
            if sub.empty:
                return pd.DataFrame(columns=list(df_all_base.columns)+["p_hat","p_bin","q_BH"]), {
                    "active": True, "mode":"fixed", "thresh": t, "n_cand": int(df_cand.shape[0]), "n_sel": 0
                }
            pvals = _binom_pvals_subset(sub, p_br)
            q = _p_adjust_bh(pvals)
            sub["p_bin"] = pvals; sub["q_BH"]=q
            meta_nc = {"active": True, "mode":"fixed", "thresh": t, "n_cand": int(df_cand.shape[0]), "n_sel": int(sub.shape[0])}
            return sub.sort_values(["p_hat","N"], ascending=[False,False]).copy(), meta_nc

        # adaptive-fdr: varre a grade e escolhe t* que maximiza #descobertas (q<=alvo); empate resolve pelo menor t
        grid = _parse_thresh_grid(getattr(args, "nc_outlier_grid", "0.60,0.70,0.80,0.85,0.90,0.95"))
        best = {"t": None, "k": 0, "sub": None, "q": None}
        tried = []
        for t in grid:
            S = df_cand.loc[df_cand["p_hat"] >= t].copy()
            if S.empty:
                tried.append((t,0,0))
                continue
            pvals = _binom_pvals_subset(S, p_br)
            q = _p_adjust_bh(pvals)
            S["p_bin"] = pvals; S["q_BH"]=q
            D = S.loc[S["q_BH"] <= fdr_target].copy()
            tried.append((t, int(S.shape[0]), int(D.shape[0])))
            if D.shape[0] > best["k"] or (D.shape[0] == best["k"] and best["t"] is not None and t < best["t"]):
                best.update({"t": t, "k": int(D.shape[0]), "sub": D, "q": q})
        if best["sub"] is None or best["k"] == 0:
            return pd.DataFrame(columns=list(df_all_base.columns)+["p_hat","p_bin","q_BH"]), {
                "active": True, "mode":"adaptive-fdr", "grid": grid, "n_cand": int(df_cand.shape[0]), "t_star": None, "n_sel": 0
            }
        out = best["sub"].sort_values(["p_hat","N"], ascending=[False,False]).copy()
        meta_nc = {
            "active": True, "mode":"adaptive-fdr", "grid": grid, "n_cand": int(df_cand.shape[0]),
            "t_star": best["t"], "n_sel": int(out.shape[0]), "fdr_target": fdr_target
        }
        return out, meta_nc

    def _export_nc_bar_png(df_nc: pd.DataFrame, meta: Dict[str,Any]) -> Optional[str]:
        if df_nc.empty or not args.export_png:
            return None
        g = df_nc.sort_values(["p_hat","N"], ascending=[False,False]).copy()
        labels = [abbreviate_middle_names_pt(str(x)) for x in g["nomePerito"].tolist()]
        vals   = (g["p_hat"]*100.0).tolist()
        fig, ax = _mkfig(max(7.8, len(labels)*0.55), 5.0)
        ax.bar(labels, vals, edgecolor="black")
        ax.set_ylabel("% NC"); ax.set_title(f"Outliers por %NC alto — {meta['start']} a {meta['end']}")
        ax.grid(axis="y", linestyle="--", alpha=0.5)
        y_max = max(vals) if vals else 0
        for i, v in enumerate(vals):
            ax.text(i, v + (y_max*0.01 if y_max else 0.3), f"{v:.1f}%", ha="center", va="bottom", fontsize=8)
        plt.xticks(rotation=45, ha="right")
        return _save_fig(fig, f"impacto_nc_outliers_{meta['start']}_a_{meta['end']}.png")

    def _build_appendix_nc_pdf(meta_base: Dict[str,Any], df_nc: pd.DataFrame,
                               pdf_path: str, png_nc: Optional[str], meta_nc: Dict[str,Any],
                               gpt_text: Optional[str]) -> None:
        # Apêndice com tabela + (opcional) gráfico e comentário GPT com fórmulas renderizadas
        doc = SimpleDocTemplate(
            pdf_path, pagesize=A4,
            leftMargin=PDF_MARGIN_LEFT_CM*cm,
            rightMargin=PDF_MARGIN_RIGHT_CM*cm,
            topMargin=PDF_MARGIN_TOP_CM*cm,
            bottomMargin=PDF_MARGIN_BOTTOM_CM*cm,
        )
        story=[]; tmp_imgs: List[str] = []

        story.append(Paragraph("Apêndice — Outliers por %NC alto", STYLES["Heading1"]))
        bits=[]
        if meta_nc.get("mode")=="fixed":
            bits.append(f"Regra: %NC ≥ {float(meta_nc.get('thresh',0))*100:.0f}%")
        elif meta_nc.get("mode")=="adaptive-fdr":
            t_star = meta_nc.get("t_star")
            if t_star is not None:
                bits.append(f"Regra: grade adaptativa (FDR≤{float(meta_nc.get('fdr_target',0.05)):.2f}); t*={t_star*100:.0f}%")
            else:
                bits.append(f"Regra: grade adaptativa (FDR≤{float(meta_nc.get('fdr_target',0.05)):.2f}); t* não encontrado")
        bits.append(f"N mínimo: {int(getattr(args,'nc_outlier_min_n',50))}")
        story.append(Paragraph(" &nbsp;&nbsp; ".join(bits), STYLES["Body"]))
        story.append(Spacer(1,6))

        if not df_nc.empty:
            show = df_nc.copy()
            show["Perito"] = show["nomePerito"].astype(str).map(abbreviate_middle_names_pt)
            show["%NC"]    = (show["p_hat"]*100.0)
            show = show[["Perito","cr","dr","N","NC","%NC","E","p_bin","q_BH"]].rename(columns={
                "cr":"CR","dr":"DR","E":"Excesso","p_bin":"p(bin)","q_BH":"q(BH)"
            })
            # 1ª coluna dinâmica com helper global
            other = [1.6*cm, 1.6*cm, 1.2*cm, 1.2*cm, 1.2*cm, 1.6*cm, 1.6*cm, 1.6*cm]
            perito_strings = show["Perito"].astype(str).tolist()
            perito_w = measure_col_pts(
                perito_strings, "Perito",
                font_body=FONT_REG, size_body=TABLE_FONT_SIZE,
                font_header=FONT_BOLD, size_header=TABLE_HEADER_FONT_SIZE,
                padding=2.0
            )
            max_perito_w = max(2.0*cm, (doc.width - sum(other) - 0.2*cm))
            perito_w = min(perito_w, max_perito_w)
            widths = [perito_w] + other

            tbl = _df_to_table(
                show, list(show.columns), {}, widths,
                font_size=TABLE_FONT_SIZE,
                header_font_size=TABLE_HEADER_FONT_SIZE,
                repeat_header=True
            )
            story.append(tbl)
            story.append(Spacer(1,8))

        if png_nc:
            story.append(_image_flowable(png_nc, max_w=doc.width * PDF_IMG_FRAC))
            story.append(Spacer(1,8))

        if gpt_text:
            # Comentário com fórmulas LaTeX renderizadas
            story += _comment_to_flowables_with_math(gpt_text, STYLES["Small"], doc.width, tmp_imgs)

        doc.build(story,
                  onFirstPage=lambda c,d: _header_footer(c,d,meta_base),
                  onLaterPages=lambda c,d: _header_footer(c,d,meta_base))

        # limpar temporários de fórmulas
        for p in tmp_imgs:
            try: os.remove(p)
            except Exception: pass

    def _gpt_comment_nc(meta_nc: Dict[str, Any], df_nc: pd.DataFrame, meta_base: Dict[str, Any]) -> str:
        """
        Gera comentário para o apêndice de outliers %NC usando texto simples (sem LaTeX),
        reforçando que q_BH baixos (≤ alvo FDR) indicam significância.
        """
        if not args.gpt_comments or not getattr(args, "appendix_nc_explain", True):
            return ""
        try:
            import json as _json

            # alvo FDR usado na detecção
            fdr_target = float(meta_nc.get("fdr_target", getattr(args, "nc_outlier_fdr", 0.05)))

            # amostra resumida (top 10)
            rows = []
            for _, r in df_nc.head(10).iterrows():
                rows.append({
                    "perito": str(r.get("nomePerito", "")),
                    "N": int(r.get("N", 0)),
                    "NC": int(r.get("NC", 0)),
                    "p_hat": float(r.get("p_hat", 0.0)),
                    "q_BH": float(r.get("q_BH", float("nan")))
                })

            payload = {
                "periodo": [meta_base.get("start"), meta_base.get("end")],
                "regra":   meta_nc,
                "amostra": rows
            }

            user = (
                "Escreva um único parágrafo (≤160 palavras, pt-BR) para o apêndice de outliers por alta %NC. "
                "Use fórmulas em texto simples (sem LaTeX): p_hat = NC/N; teste binomial unilateral: "
                "H0: p = p_BR; H1: p > p_BR; p-valor = Pr(X >= NC | n = N, p = p_BR). "
                "Explique que aplicamos correção de FDR (Benjamini–Hochberg) e que significância ocorre "
                f"quando q_BH ≤ {fdr_target:.2f}. Descreva a regra de seleção (fixed ou grade adaptativa) "
                "e o limiar t* se houver. Evite linguagem causal e dados sensíveis. "
                "Retorne apenas o parágrafo, sem listas e sem quebras de linha extras. Dados:\n\n"
                + _json.dumps(payload, ensure_ascii=False)
            )

            out = chamar_gpt(
                SYSTEM_PROMPT, user, call_api=True,
                model="gpt-4o-mini", temperature=0.2
            )
            return (out.get("comment") or "").strip()
        except Exception:
            return ""

    generated_paths: List[str] = []

    # ==========================
    # Bases (df_all / seleções)
    # ==========================
    if args.perito:
        with sqlite3.connect(DB_PATH) as conn:
            schema = _detect_schema(conn)
            df_n = _fetch_perito_n_nc(conn, args.start, args.end, schema)
            if df_n.empty:
                df_all = pd.DataFrame(columns=['nomePerito','N','NC','cr','dr','E','IV_vagas','score_final'])
                df_sel = df_all.copy()
                p_br, total_calc, nc_calc = 0.0, 0, 0
            else:
                if args.pbr is None:
                    p_br, total_calc, nc_calc = _compute_p_br_and_totals(conn, args.start, args.end, schema)
                else:
                    p_br, total_calc, nc_calc = float(args.pbr), None, None
                df_scores = _fetch_scores(conn)
                df_all = _prep_base(df_n, df_scores, p_br, args.alpha, min_analises=0)
                df_sel = df_all.loc[
                    df_all["nomePerito"].str.strip().str.upper() == args.perito.strip().upper()
                ].copy()
        s_star = None
        meta = {
            'mode': 'single',
            'perito': args.perito,
            'start': args.start,
            'end': args.end,
            'alpha': args.alpha,
            'p_br': p_br,
            'score_cut': None,
            'n_all': int(df_all.shape[0]),
            'n_sel': int(df_sel.shape[0]),
            'topn': int(args.topn),
            'by': args.by
        }

        # ----- bloco único (perito) -----
        meta = _metrics_for_block(df_all, df_sel, meta)
        block = _export_pngs_for_block(df_all, df_sel, meta)
        pngs = block["pngs"]; df_strat_tot = block["df_strat_tot"]; df_strat_sel = block["df_strat_sel"]
        tests, psa_png = _run_tests_for_block(df_all, df_sel, meta)
        if psa_png: pngs['psa'] = psa_png
        comments = _comments_for_block(df_all, df_sel, meta, pngs, tests, df_strat_tot, df_strat_sel)

        # PDF (com header/front se fornecidos)
        final_path = None
        if args.export_pdf:
            pdf_out = os.path.join(EXPORT_DIR, f"impacto_fila_{args.start}_a_{args.end}.pdf")
            front_pdfs: List[str] = []
            use_header_and_text = bool(getattr(args, "header_and_text", False))
            if use_header_and_text:
                path_ht = getattr(args, "header_and_text_file", None)
                if not path_ht:
                    base_hint = args.front_org or args.header_org or os.getcwd()
                    base_dir  = str(Path(base_hint).resolve().parent if os.path.exists(str(base_hint)) else Path(base_hint))
                    path_ht = os.path.join(base_dir, "header_and_text.org")
                try:
                    ht_pdf = _render_org_to_pdf(path_ht)
                    if os.path.exists(ht_pdf): front_pdfs.append(ht_pdf)
                except Exception as e:
                    print(f"[warn] Falha ao renderizar header_and_text: {e}", file=sys.stderr)

            if getattr(args, "front_pdf", None):
                if isinstance(args.front_pdf, (list, tuple)):
                    for p in args.front_pdf:
                        if p and os.path.exists(p): front_pdfs.append(str(p))
                elif isinstance(args.front_pdf, str) and os.path.exists(args.front_pdf):
                    front_pdfs.append(args.front_pdf)

            if getattr(args, "front_org_render", None):
                org_list = args.front_org_render
                if isinstance(org_list, str): org_list = [org_list]
                for opath in (org_list or []):
                    try:
                        pdf_r = _render_org_to_pdf(opath)
                        if os.path.exists(pdf_r): front_pdfs.append(pdf_r)
                    except Exception as e:
                        print(f"[warn] Falha ao renderizar ORG '{opath}': {e}", file=sys.stderr)

            inject_front = len(front_pdfs) > 0
            header_for_build = None if inject_front or use_header_and_text else args.header_org
            front_for_build  = None if inject_front or use_header_and_text else args.front_org

            build_pdf(meta, df_sel, tests, pngs, df_strat_tot, df_strat_sel,
                      pdf_out, header_org=header_for_build, front_org=front_for_build,
                      comments=comments if args.gpt_comments else None)
            if inject_front:
                final_path = os.path.join(EXPORT_DIR, f"impacto_fila_{args.start}_a_{args.end}_FINAL.pdf")
                try:
                    _concat_pdfs(front_pdfs + [pdf_out], final_path)
                except Exception as e:
                    print(f"[warn] Falha ao concatenar PDFs: {e}", file=sys.stderr)
                    final_path = pdf_out
            else:
                final_path = pdf_out
            print(f"PDF: {final_path}")

        # LOG
        print(f"p_BR = {_fmt2s(meta['p_br'])}")
        print(f"IV período (vagas): {meta['iv_total_period']} | IV selecionado: {meta['iv_total_sel']} | peso: {_fmt2s(meta['peso_sel']*100)}%")
        for k, v in (pngs or {}).items():
            if v: print(f"PNG {k}: {v}")

        # organização
        if args.export_pdf:
            if 'pdf_out' in locals() and os.path.exists(pdf_out): generated_paths.append(pdf_out)
            if 'final_path' in locals() and final_path and os.path.exists(final_path): generated_paths.append(final_path)
        for k, v in (pngs or {}).items():
            if v and os.path.exists(v): generated_paths.append(v)
        dest_base = _organize_outputs(args.start, args.end, generated_paths=generated_paths,
                                      comments=(comments if args.gpt_comments else None))
        print(f"Saída organizada em: {dest_base}")
        return

    # ----------------------------
    # Modo TOPN (impact/kpi/both)
    # ----------------------------
    with sqlite3.connect(DB_PATH) as conn:
        schema = _detect_schema(conn)
        df_n = _fetch_perito_n_nc(conn, args.start, args.end, schema)
        if df_n.empty:
            df_all = pd.DataFrame(columns=['nomePerito','N','NC','cr','dr','E','IV_vagas','score_final'])
            p_br, total_calc, nc_calc = 0.0, 0, 0
        else:
            if args.pbr is None:
                p_br, total_calc, nc_calc = _compute_p_br_and_totals(conn, args.start, args.end, schema)
            else:
                p_br, total_calc, nc_calc = float(args.pbr), None, None
            df_scores = _fetch_scores(conn)
            df_all = _prep_base(df_n, df_scores, p_br, args.alpha, min_analises=args.min_analises)
    s_star = _elbow_cutoff_score(df_all)

    # Outliers por %NC (sobre a base completa deste período)
    df_nc_out, meta_nc = _detect_nc_outliers(df_all, p_br)

    # base do meta
    base_meta = {
        'start': args.start, 'end': args.end, 'alpha': args.alpha, 'p_br': p_br,
        'score_cut': s_star, 'n_all': int(df_all.shape[0]), 'topn': int(args.topn), 'by': args.by
    }

    # ---------- seleção Impact ----------
    df_cut_impact = df_all if s_star is None else df_all.loc[df_all["score_final"] >= s_star].copy()
    if args.topn and args.topn > 0 and not df_cut_impact.empty:
        df_sel_impact = df_cut_impact.sort_values(
            ["IV_vagas","E","NC","N"],
            ascending=[False, False, False, True]
        ).head(int(args.topn)).copy()
    else:
        df_sel_impact = df_cut_impact.copy()

    # ---------- seleção KPI ----------
    names_kpi_upper = set()
    if getattr(args, "select_src", "impact") in ("kpi", "both"):
        _kpi_names = _kpi_top10_names(args.start, args.end, min_analises=getattr(args, "kpi_min_analises", 50))
        names_kpi_upper = set([n.strip().upper() for n in _kpi_names])
    df_sel_kpi = df_all.loc[
        df_all["nomePerito"].str.strip().str.upper().isin(names_kpi_upper)
    ].copy()
    if args.topn and args.topn > 0 and not df_sel_kpi.empty:
        df_sel_kpi = df_sel_kpi.sort_values(
            ["IV_vagas","E","NC","N"],
            ascending=[False, False, False, True]
        ).head(int(args.topn)).copy()

    # ------ Integração dos outliers %NC ------
    # Se houver apêndice ligado, NÃO mistura no corpo principal; caso contrário, respeita --nc-outlier-add-to
    df_nc_appendix = pd.DataFrame()
    if meta_nc.get("active", False) and not df_nc_out.empty:
        if getattr(args, "appendix_nc_outliers", False):
            df_nc_appendix = df_nc_out.copy()
        else:
            add_to = getattr(args, "nc_outlier_add_to", "both")
            if add_to in ("impact", "both") and not df_sel_impact.empty:
                base_names = set(df_sel_impact["nomePerito"].str.upper())
                to_add = df_nc_out.loc[~df_nc_out["nomePerito"].str.upper().isin(base_names), df_sel_impact.columns]
                df_sel_impact = pd.concat([df_sel_impact, to_add], ignore_index=True)
            if add_to in ("kpi", "both"):
                base_names = set(df_sel_kpi["nomePerito"].str.upper())
                to_add = df_nc_out.loc[~df_nc_out["nomePerito"].str.upper().isin(base_names), df_sel_kpi.columns]
                df_sel_kpi = pd.concat([df_sel_kpi, to_add], ignore_index=True)

    sel_src = getattr(args, "select_src", "impact").lower()

    # ============= CASOS: impact / kpi =============
    if sel_src in ("impact", "kpi"):
        df_sel = df_sel_impact.copy() if sel_src=="impact" else df_sel_kpi.copy()
        meta = dict(base_meta); meta.update({
            'mode': f"top10-{sel_src}",
            'select_src': sel_src,
            'n_sel': int(df_sel.shape[0]),
        })
        # métricas + pngs + testes + comentários
        meta = _metrics_for_block(df_all, df_sel, meta)
        block = _export_pngs_for_block(df_all, df_sel, meta)
        pngs = block["pngs"]; df_strat_tot = block["df_strat_tot"]; df_strat_sel = block["df_strat_sel"]
        tests, psa_png = _run_tests_for_block(df_all, df_sel, meta)
        if psa_png: pngs['psa'] = psa_png
        comments = _comments_for_block(df_all, df_sel, meta, pngs, tests, df_strat_tot, df_strat_sel)

        # PDF principal
        final_path = None
        if args.export_pdf:
            pdf_out = os.path.join(EXPORT_DIR, f"impacto_fila_{args.start}_a_{args.end}.pdf")
            # FRONT em PDF? (header único OU listas front_pdf/front_org_render)
            front_pdfs: List[str] = []
            use_header_and_text = bool(getattr(args, "header_and_text", False))
            if use_header_and_text:
                path_ht = getattr(args, "header_and_text_file", None)
                if not path_ht:
                    base_hint = args.front_org or args.header_org or os.getcwd()
                    base_dir  = str(Path(base_hint).resolve().parent if os.path.exists(str(base_hint)) else Path(base_hint))
                    path_ht = os.path.join(base_dir, "header_and_text.org")
                try:
                    ht_pdf = _render_org_to_pdf(path_ht)
                    if os.path.exists(ht_pdf): front_pdfs.append(ht_pdf)
                except Exception as e:
                    print(f"[warn] Falha ao renderizar header_and_text: {e}", file=sys.stderr)

            if getattr(args, "front_pdf", None):
                if isinstance(args.front_pdf, (list, tuple)):
                    for p in args.front_pdf:
                        if p and os.path.exists(p): front_pdfs.append(str(p))
                elif isinstance(args.front_pdf, str) and os.path.exists(args.front_pdf):
                    front_pdfs.append(args.front_pdf)

            if getattr(args, "front_org_render", None):
                org_list = args.front_org_render
                if isinstance(org_list, str): org_list = [org_list]
                for opath in (org_list or []):
                    try:
                        pdf_r = _render_org_to_pdf(opath)
                        if os.path.exists(pdf_r): front_pdfs.append(pdf_r)
                    except Exception as e:
                        print(f"[warn] Falha ao renderizar ORG '{opath}': {e}", file=sys.stderr)

            inject_front = len(front_pdfs) > 0
            header_for_build = None if inject_front or use_header_and_text else args.header_org
            front_for_build  = None if inject_front or use_header_and_text else args.front_org

            build_pdf(meta, df_sel, tests, pngs, df_strat_tot, df_strat_sel,
                      pdf_out, header_org=header_for_build, front_org=front_for_build,
                      comments=comments if args.gpt_comments else None)

            # Montagem final (podendo anexar apêndice de %NC, se houver e estiver ligado)
            pieces = []
            if inject_front:  # concatena FRONT+corpo
                pdf_with_front = os.path.join(EXPORT_DIR, f"impacto_fila_{args.start}_a_{args.end}_WITH_FRONT.pdf")
                try:
                    _concat_pdfs(front_pdfs + [pdf_out], pdf_with_front)
                except Exception as e:
                    print(f"[warn] Falha ao concatenar FRONT: {e}", file=sys.stderr)
                    pdf_with_front = pdf_out
                pieces.append(pdf_with_front)
            else:
                pieces.append(pdf_out)

            # Apêndice (se ligado)
            if getattr(args, "appendix_nc_outliers", False) and not df_nc_appendix.empty:
                png_nc = _export_nc_bar_png(df_nc_appendix, base_meta)
                if png_nc: generated_paths.append(png_nc)
                gpt_txt = _gpt_comment_nc(meta_nc, df_nc_appendix, base_meta)
                pdf_app = os.path.join(EXPORT_DIR, f"impacto_fila_{args.start}_a_{args.end}_APPENDIX_NC.pdf")
                _build_appendix_nc_pdf(base_meta, df_nc_appendix, pdf_app, png_nc, meta_nc, gpt_txt)
                pieces.append(pdf_app)

            # arquivo final
            if len(pieces) == 1:
                final_path = pieces[0]
            else:
                final_path = os.path.join(EXPORT_DIR, f"impacto_fila_{args.start}_a_{args.end}_FINAL.pdf")
                try:
                    _concat_pdfs(pieces, final_path)
                except Exception as e:
                    print(f"[warn] Falha ao concatenar FINAL: {e}", file=sys.stderr)
                    final_path = pieces[0]
            print(f"PDF: {final_path}")

        # LOG básico
        print(f"p_BR = {_fmt2s(meta['p_br'])}")
        print(f"IV período (vagas): {meta['iv_total_period']} | IV selecionado: {meta['iv_total_sel']} | peso: {_fmt2s(meta['peso_sel']*100)}%")
        if meta_nc.get("active", False):
            print(f"[NC outliers] modo={meta_nc.get('mode')} "
                  f"t*={meta_nc.get('t_star')} n_sel={meta_nc.get('n_sel',0)} n_cand={meta_nc.get('n_cand',0)}")
        for k, v in (pngs or {}).items():
            if v: print(f"PNG {k}: {v}")

        # organizar saída
        if args.export_pdf:
            if 'pdf_out' in locals() and os.path.exists(pdf_out): generated_paths.append(pdf_out)
            if 'final_path' in locals() and final_path and os.path.exists(final_path): generated_paths.append(final_path)
            if 'pdf_with_front' in locals() and os.path.exists(pdf_with_front): generated_paths.append(pdf_with_front)
            if 'pdf_app' in locals() and os.path.exists(pdf_app): generated_paths.append(pdf_app)
        for k, v in (pngs or {}).items():
            if v and os.path.exists(v): generated_paths.append(v)
        dest_base = _organize_outputs(args.start, args.end, generated_paths=generated_paths,
                                      comments=(comments if args.gpt_comments else None))
        print(f"Saída organizada em: {dest_base}")
        return

    # ----------------------------
    # Modo BOTH: dois blocos (+ apêndice)
    # ----------------------------
    # IMPACT meta/bloco
    meta_imp = dict(base_meta); meta_imp.update({'mode': 'top10-impact', 'select_src': 'impact', 'n_sel': int(df_sel_impact.shape[0])})
    meta_imp = _metrics_for_block(df_all, df_sel_impact, meta_imp)
    blk_imp = _export_pngs_for_block(df_all, df_sel_impact, meta_imp)
    pngs_imp = blk_imp["pngs"]; df_strat_tot_imp = blk_imp["df_strat_tot"]; df_strat_sel_imp = blk_imp["df_strat_sel"]
    tests_imp, psa_png_imp = _run_tests_for_block(df_all, df_sel_impact, meta_imp)
    if psa_png_imp: pngs_imp['psa'] = psa_png_imp
    pngs_imp = _rename_pngs(pngs_imp, "impact")
    comments_imp = _comments_for_block(df_all, df_sel_impact, meta_imp, pngs_imp, tests_imp, df_strat_tot_imp, df_strat_sel_imp)

    # KPI meta/bloco
    meta_kpi = dict(base_meta); meta_kpi.update({'mode': 'top10-kpi', 'select_src': 'kpi', 'n_sel': int(df_sel_kpi.shape[0])})
    meta_kpi = _metrics_for_block(df_all, df_sel_kpi, meta_kpi)
    blk_kpi = _export_pngs_for_block(df_all, df_sel_kpi, meta_kpi)
    pngs_kpi = blk_kpi["pngs"]; df_strat_tot_kpi = blk_kpi["df_strat_tot"]; df_strat_sel_kpi = blk_kpi["df_strat_sel"]
    tests_kpi, psa_png_kpi = _run_tests_for_block(df_all, df_sel_kpi, meta_kpi)
    if psa_png_kpi: pngs_kpi['psa'] = psa_png_kpi
    pngs_kpi = _rename_pngs(pngs_kpi, "kpi")
    comments_kpi = _comments_for_block(df_all, df_sel_kpi, meta_kpi, pngs_kpi, tests_kpi, df_strat_tot_kpi, df_strat_sel_kpi)

    # ===== Apêndice %NC (se ligado) =====
    pdf_app_nc = None
    if getattr(args, "appendix_nc_outliers", False) and not df_nc_appendix.empty:
        png_nc = _export_nc_bar_png(df_nc_appendix, base_meta)
        if png_nc: generated_paths.append(png_nc)
        gpt_txt = _gpt_comment_nc(meta_nc, df_nc_appendix, base_meta)
        pdf_app_nc = os.path.join(EXPORT_DIR, f"impacto_fila_{args.start}_a_{args.end}_APPENDIX_NC.pdf")
        _build_appendix_nc_pdf(base_meta, df_nc_appendix, pdf_app_nc, png_nc, meta_nc, gpt_txt)

    # ===== PDFs =====
    final_path = None
    if args.export_pdf:
        # 1) Corpo IMPACT (com header/front/FRONT-PDFs se houver)
        pdf_out_imp = os.path.join(EXPORT_DIR, f"impacto_fila_{args.start}_a_{args.end}_IMPACT.pdf")

        # FRONT em PDF? (header único OU listas front_pdf/front_org_render) — só para o IMPACT!
        front_pdfs: List[str] = []
        use_header_and_text = bool(getattr(args, "header_and_text", False))
        if use_header_and_text:
            path_ht = getattr(args, "header_and_text_file", None)
            if not path_ht:
                base_hint = args.front_org or args.header_org or os.getcwd()
                base_dir  = str(Path(base_hint).resolve().parent if os.path.exists(str(base_hint)) else Path(base_hint))
                path_ht = os.path.join(base_dir, "header_and_text.org")
            try:
                ht_pdf = _render_org_to_pdf(path_ht)
                if os.path.exists(ht_pdf): front_pdfs.append(ht_pdf)
            except Exception as e:
                print(f"[warn] Falha ao renderizar header_and_text: {e}", file=sys.stderr)

        if getattr(args, "front_pdf", None):
            if isinstance(args.front_pdf, (list, tuple)):
                for p in args.front_pdf:
                    if p and os.path.exists(p): front_pdfs.append(str(p))
            elif isinstance(args.front_pdf, str) and os.path.exists(args.front_pdf):
                front_pdfs.append(args.front_pdf)

        if getattr(args, "front_org_render", None):
            org_list = args.front_org_render
            if isinstance(org_list, str): org_list = [org_list]
            for opath in (org_list or []):
                try:
                    pdf_r = _render_org_to_pdf(opath)
                    if os.path.exists(pdf_r): front_pdfs.append(pdf_r)
                except Exception as e:
                    print(f"[warn] Falha ao renderizar ORG '{opath}': {e}", file=sys.stderr)

        inject_front = len(front_pdfs) > 0
        header_for_build = None if inject_front or use_header_and_text else args.header_org
        front_for_build  = None if inject_front or use_header_and_text else args.front_org

        build_pdf(meta_imp, df_sel_impact, tests_imp, pngs_imp, df_strat_tot_imp, df_strat_sel_imp,
                  pdf_out_imp, header_org=header_for_build, front_org=front_for_build,
                  comments=(comments_imp if args.gpt_comments else None))
        if inject_front:
            pdf_with_front = os.path.join(EXPORT_DIR, f"impacto_fila_{args.start}_a_{args.end}_IMPACT_FRONT.pdf")
            try:
                _concat_pdfs(front_pdfs + [pdf_out_imp], pdf_with_front)
            except Exception as e:
                print(f"[warn] Falha ao concatenar FRONT (IMPACT): {e}", file=sys.stderr)
                pdf_with_front = pdf_out_imp
        else:
            pdf_with_front = pdf_out_imp

        # 2) Corpo KPI (sem header/front)
        pdf_out_kpi = os.path.join(EXPORT_DIR, f"impacto_fila_{args.start}_a_{args.end}_KPI.pdf")
        build_pdf(meta_kpi, df_sel_kpi, tests_kpi, pngs_kpi, df_strat_tot_kpi, df_strat_sel_kpi,
                  pdf_out_kpi, header_org=None, front_org=None,
                  comments=(comments_kpi if args.gpt_comments else None))

        # 3) Final = (FRONT+IMPACT) + KPI (+ APPENDIX NC opcional)
        pieces = [pdf_with_front, pdf_out_kpi]
        if pdf_app_nc and os.path.exists(pdf_app_nc):
            pieces.append(pdf_app_nc)
        final_path = os.path.join(EXPORT_DIR, f"impacto_fila_{args.start}_a_{args.end}_FINAL.pdf")
        try:
            _concat_pdfs(pieces, final_path)
        except Exception as e:
            print(f"[warn] Falha ao concatenar IMPACT+KPI(+APP): {e}", file=sys.stderr)
            final_path = pdf_with_front
        print(f"PDF: {final_path}")

    # -------- LOG ----------
    print("==== BLOCO IMPACT ====")
    print(f"p_BR = {_fmt2s(meta_imp['p_br'])}")
    print(f"IV período (vagas): {meta_imp['iv_total_period']} | IV sel: {meta_imp['iv_total_sel']} | peso: {_fmt2s(meta_imp['peso_sel']*100)}%")
    for k, v in (pngs_imp or {}).items():
        if v: print(f"PNG impact {k}: {v}")
    print("==== BLOCO KPI ====")
    print(f"p_BR = {_fmt2s(meta_kpi['p_br'])}")
    print(f"IV período (vagas): {meta_kpi['iv_total_period']} | IV sel: {meta_kpi['iv_total_sel']} | peso: {_fmt2s(meta_kpi['peso_sel']*100)}%")
    for k, v in (pngs_kpi or {}).items():
        if v: print(f"PNG kpi {k}: {v}")
    if meta_nc.get("active", False):
        print("==== OUTLIERS %NC ====")
        print(f"modo={meta_nc.get('mode')} t*={meta_nc.get('t_star')} n_sel={meta_nc.get('n_sel',0)} n_cand={meta_nc.get('n_cand',0)}")

    # -------- ORGANIZAR SAÍDA ----------
    # PNGs
    for d in (pngs_imp, pngs_kpi):
        for k, v in (d or {}).items():
            if v and os.path.exists(v):
                generated_paths.append(v)
    # PDFs
    if args.export_pdf:
        for p in (locals().get("pdf_out_imp"), locals().get("pdf_out_kpi"), locals().get("pdf_with_front"),
                  locals().get("final_path"), locals().get("pdf_app_nc")):
            if p and os.path.exists(p):
                generated_paths.append(p)

    # comentários: junta IMPACT + KPI (apêndice tem comentário separado e já vai salvo no PDF)
    comments_merge = {}
    if args.gpt_comments:
        for d in (comments_imp, comments_kpi):
            if isinstance(d, dict): comments_merge.update({f"kpi_{k}" if d is comments_kpi else k: v for k, v in d.items()})

    dest_base = _organize_outputs(
        args.start, args.end,
        generated_paths=generated_paths,
        comments=(comments_merge if args.gpt_comments else None)
    )
    print(f"Saída organizada em: {dest_base}")

if __name__ == "__main__":
    # Corrige um erro de digitação comum em --sens_alpha_frac
    sys.argv = [a.replace("sens_alpha_fr ac", "sens_alpha_frac") for a in sys.argv]
    main()


