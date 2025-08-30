#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Relatório ATESTMED — KPI (NG, PDF direto, sem ORG)
- Mantém a lógica do make_kpi_report.py (seleção Top10 via import dinâmico).
- Gera PDF direto (ReportLab) com gráficos e comentários .md.
- Detecção de flags suportadas pelos compare_*.
- Limpeza automática de quaisquer .org gerados por scripts legados.
- Tolerância a PNGs truncados/corrompidos (PIL + re-encode; skip com aviso).
"""

import os, sys, re, csv, json, math, shutil, zipfile, sqlite3, argparse, subprocess, unicodedata
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List
from pathlib import Path

import pandas as pd

# Evitar backends GUI
import matplotlib
matplotlib.use("Agg")

# PIL robusto para imagens truncadas
from PIL import Image, ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True

# PDF utils
try:
    from pypdf import PdfMerger
except Exception:
    try:
        from PyPDF2 import PdfMerger
    except Exception:
        PdfMerger = None

# ReportLab
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image as RLImage, Table, TableStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from xml.sax.saxutils import escape as xml_escape

# ────────────────────────────────────────────────────────────────────────────────
# Paths e Constantes
# ────────────────────────────────────────────────────────────────────────────────
_THIS_FILE = Path(__file__).resolve()
BASE_DIR    = str(_THIS_FILE.parent.parent if _THIS_FILE.parent.name == "reports" else _THIS_FILE.parent)
DB_PATH     = os.path.join(BASE_DIR, "db", "atestmed.db")
GRAPHS_DIR  = os.path.join(BASE_DIR, "graphs_and_tables")
EXPORT_DIR  = os.path.join(GRAPHS_DIR, "exports")
OUTPUTS_DIR = os.path.join(BASE_DIR, "reports", "outputs")
RCHECK_DIR  = os.path.join(BASE_DIR, "r_checks")

for d in (EXPORT_DIR, OUTPUTS_DIR):
    os.makedirs(d, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Limpeza de .org (scripts legados)
# ────────────────────────────────────────────────────────────────────────────────
def _purge_org_files(dirpath: str, verbose: bool = True) -> int:
    removed = 0
    try:
        base = Path(dirpath)
        for pat in ("*.org", "*_comment.org"):
            for p in base.glob(pat):
                try:
                    p.unlink()
                    removed += 1
                    if verbose:
                        print(f"[CLEAN] Removido: {p}")
                except Exception as e:
                    print(f"[warn] Não foi possível remover {p}: {e}")
    except Exception as e:
        print(f"[warn] Falha ao varrer {dirpath} para limpar .org: {e}")
    return removed

# ────────────────────────────────────────────────────────────────────────────────
# Seleção Top10 (import original + fallback)
# ────────────────────────────────────────────────────────────────────────────────
pegar_10_piores_peritos_original = None

def _try_import_original_selector():
    global pegar_10_piores_peritos_original
    for spath, modname, attr in [
        (str(_THIS_FILE.parent), "make_kpi_report", "pegar_10_piores_peritos"),
        (str(_THIS_FILE.parent), "reports.make_kpi_report", "pegar_10_piores_peritos"),
        (str(Path(BASE_DIR)), "make_kpi_report", "pegar_10_piores_peritos"),
        (str(Path(BASE_DIR) / "reports"), "make_kpi_report", "pegar_10_piores_peritos"),
        ("", "make_kpi_report", "pegar_10_piores_peritos"),
    ]:
        try:
            if spath and spath not in sys.path:
                sys.path.insert(0, spath)
            mod = __import__(modname, fromlist=[attr])
            func = getattr(mod, attr, None)
            if callable(func):
                pegar_10_piores_peritos_original = func
                print(f"[INFO] Seleção Top10: usando {modname}.{attr}")
                return
        except Exception:
            continue
    print("[AVISO] Não consegui importar a seleção Top10 do script original. Usarei o fallback interno.")

_try_import_original_selector()

def _fetch_scores(conn) -> pd.DataFrame:
    try:
        return pd.read_sql_query("SELECT UPPER(nomePerito) AS nomePerito, score_final FROM ranking_final", conn)
    except Exception:
        return pd.DataFrame(columns=["nomePerito","score_final"])

def _fetch_perito_n_nc(conn, start: str, end: str) -> pd.DataFrame:
    q = """
        SELECT UPPER(nomePerito) AS nomePerito, COUNT(*) AS N,
               SUM(CASE WHEN conformado=0 THEN 1 ELSE 0 END) AS NC
        FROM analises_atestmed
        WHERE date(dataHoraIniPericia) >= date(?) AND date(dataHoraIniPericia) <= date(?)
        GROUP BY UPPER(nomePerito)
    """
    try:
        return pd.read_sql_query(q, conn, params=[start, end])
    except Exception:
        return pd.DataFrame(columns=["nomePerito","N","NC"])

def pegar_10_piores_peritos_fallback(start: str, end: str, min_analises: int=50) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        df_n = _fetch_perito_n_nc(conn, start, end)
        if df_n.empty:
            return pd.DataFrame(columns=["nomePerito"])
        df_n = df_n.loc[df_n["N"] >= int(min_analises)].copy()
        if df_n.empty:
            return pd.DataFrame(columns=["nomePerito"])
        df_scores = _fetch_scores(conn)
        base = df_n.merge(df_scores, on="nomePerito", how="left")
        base["score_final"] = base["score_final"].fillna(0.0)
        base = base.sort_values(["score_final","NC","N"], ascending=[False,False,True])
        return base.head(10)[["nomePerito"]].copy()

def _call_original_top10(start: str, end: str, min_analises: int=50):
    f = pegar_10_piores_peritos_original
    if not callable(f):
        return None
    try:
        return f(start, end, min_analises=min_analises)
    except TypeError:
        try:
            return f(start, end, min_analises)
        except Exception:
            return None
    except Exception:
        return None

def get_top10_dataframe(start: str, end: str, min_analises: int=50) -> pd.DataFrame:
    df = _call_original_top10(start, end, min_analises)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        print("[INFO] Usando fallback interno para seleção Top10.")
        df = pegar_10_piores_peritos_fallback(start, end, min_analises=min_analises)
    if isinstance(df, list):
        df = pd.DataFrame({"nomePerito": df})
    if not isinstance(df, pd.DataFrame):
        try:
            df = pd.DataFrame(df)
        except Exception:
            df = pd.DataFrame(columns=["nomePerito"])
    if "nomePerito" not in df.columns:
        for c in df.columns:
            if str(c).lower() in ("perito","nomeperito","nome_perito"):
                df = df.rename(columns={c:"nomePerito"})
                break
    if "nomePerito" not in df.columns:
        df["nomePerito"] = []
    return df[["nomePerito"]].dropna().drop_duplicates()

# ────────────────────────────────────────────────────────────────────────────────
# Estilos / helpers PDF
# ────────────────────────────────────────────────────────────────────────────────
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
    return {
        "Title":    ParagraphStyle("Title", parent=ss["Title"], fontName=FONT_BOLD, fontSize=16, leading=19, spaceAfter=8),
        "Heading2": ParagraphStyle("Heading2", parent=ss["Heading2"], fontName=FONT_BOLD, fontSize=12.5, leading=15, spaceBefore=8, spaceAfter=5),
        "Small":    ParagraphStyle("Small", parent=ss["BodyText"], fontName=FONT_REG, fontSize=9, leading=12, spaceAfter=3),
    }

STYLES = _styles()
PDF_MARGIN_LEFT_CM=3; PDF_MARGIN_RIGHT_CM=2; PDF_MARGIN_TOP_CM=3; PDF_MARGIN_BOTTOM_CM=2; PDF_IMG_FRAC=1.0
TABLE_FONT_SIZE=8.5; TABLE_HEADER_FONT_SIZE=None

def _escape(s: str) -> str:
    return xml_escape(s or "")

def _header_footer(canvas, doc, meta: Dict[str, Any]):
    canvas.saveState()
    canvas.setFont(FONT_REG, 8.5)
    x_left  = doc.leftMargin
    x_right = doc.pagesize[0] - doc.rightMargin
    y = max(0.5*cm, doc.bottomMargin - 0.5*cm)
    title = meta.get("title", "Relatório KPI")
    periodo = f"{meta.get('start','')} a {meta.get('end','')}"
    canvas.drawString(x_left,  y, f"{title} — {periodo}".strip(" —"))
    canvas.drawRightString(x_right, y, datetime.now().strftime("%d/%m/%Y %H:%M"))
    canvas.restoreState()

def _df_table(df: pd.DataFrame, headers: List[str], widths: List[float]):
    data = [headers] + df.values.tolist()
    tbl = Table(data, colWidths=widths, repeatRows=1)
    pad=3.0
    tbl.setStyle(TableStyle([
        ("FONTNAME",(0,0),(-1,0), FONT_BOLD), ("FONTSIZE",(0,0),(-1,0), 8.5),
        ("FONTNAME",(0,1),(-1,-1), FONT_REG), ("FONTSIZE",(0,1),(-1,-1), 8.5),
        ("GRID",(0,0),(-1,-1), 0.25, colors.lightgrey),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("LEFTPADDING",(0,0),(-1,-1),pad), ("RIGHTPADDING",(0,0),(-1,-1),pad),
        ("TOPPADDING",(0,0),(-1,-1),pad), ("BOTTOMPADDING",(0,0),(-1,-1),pad),
    ]))
    return tbl

# ────────────────────────────────────────────────────────────────────────────────
# Helpers gerais
# ────────────────────────────────────────────────────────────────────────────────
def _safe(name: str) -> str:
    s = unicodedata.normalize("NFKD", name or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", s.strip())
    return re.sub(r"_+", "_", s).strip("_").lower() or "perito"

def _run(cmd: List[str], cwd: Optional[str]=None) -> int:
    print("[CMD]", " ".join(cmd))
    try:
        out = subprocess.run(cmd, cwd=cwd, check=False, capture_output=True, text=True)
        if out.stdout: print(out.stdout.strip())
        if out.stderr: print(out.stderr.strip())
        return out.returncode
    except Exception as e:
        print(f"[ERRO] Falha ao executar: {e}")
        return 1

def _read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""

# ────────────────────────────────────────────────────────────────────────────────
# Execução dos compare_* / R com detecção de flags
# ────────────────────────────────────────────────────────────────────────────────
GLOBAL_SCRIPTS = ["g_weekday_to_weekend_table.py"]

GRAPH_SCRIPTS  = [
    "compare_nc_rate.py",
    "compare_fifteen_seconds.py",
    "compare_productivity.py",
    "compare_overlap.py",
    "compare_motivos_perito_vs_brasil.py",
    "compare_indicadores_composto.py",
]

FIFTEEN_THRESHOLD = 15
PRODUCTIVITY_THRESHOLD = 50

RCHECK_SCRIPTS = [
    ("01_nc_rate_check.R",          {"need_perito": True}),
    ("02_le15s_check.R",            {"need_perito": True, "defaults": {"--threshold": FIFTEEN_THRESHOLD}}),
    ("03_productivity_check.R",     {"need_perito": True, "defaults": {"--threshold": PRODUCTIVITY_THRESHOLD}}),
    ("04_overlap_check.R",          {"need_perito": True}),
    ("05_motivos_chisq.R",          {"need_perito": True}),
    ("06_composite_robustness.R",   {"need_perito": True}),
    ("07_kpi_icra_iatd_score.R",    {"need_perito": True}),
    ("08_weighted_props.R",         {"need_perito": True, "defaults": {"--measure": "nc"}}),
    ("08_weighted_props.R",         {"need_perito": True, "defaults": {"--measure": "le", "--threshold": FIFTEEN_THRESHOLD}}),
]

RCHECK_GROUP_SCRIPTS = [
    ("g01_top10_nc_rate_check.R",        {"defaults": {}}),
    ("g02_top10_le15s_check.R",          {"defaults": {"--threshold": FIFTEEN_THRESHOLD}}),
    ("g03_top10_productivity_check.R",   {"defaults": {"--threshold": PRODUCTIVITY_THRESHOLD}}),
    ("g04_top10_overlap_check.R",        {"defaults": {}}),
    ("g05_top10_motivos_chisq.R",        {"defaults": {}}),
    ("g06_top10_composite_robustness.R", {"defaults": {}}),
    ("g07_top10_kpi_icra_iatd_score.R",  {"defaults": {}}),
    ("08_weighted_props.R",              {"pass_top10": True, "defaults": {"--measure": "nc"}}),
    ("08_weighted_props.R",              {"pass_top10": True, "defaults": {"--measure": "le", "--threshold": FIFTEEN_THRESHOLD}}),
]

def _script_path(dirpath: str, script_name: str) -> str:
    p = os.path.join(dirpath, script_name)
    if not os.path.exists(p): raise FileNotFoundError(f"Script ausente: {p}")
    return p

def _get_help_text(py_script_path: str) -> str:
    try:
        out = subprocess.run([sys.executable, py_script_path, "--help"],
                             check=False, capture_output=True, text=True, cwd=GRAPHS_DIR)
        return (out.stdout or "") + "\n" + (out.stderr or "")
    except Exception:
        return ""

def _supports_flag(help_text: str, flag: str) -> bool:
    token = flag.strip()
    if not token.startswith("--"): token = "--" + token
    return token in help_text

def _build_global_cmds(start: str, end: str) -> List[List[str]]:
    cmds = []
    for s in GLOBAL_SCRIPTS:
        try:
            p = _script_path(GRAPHS_DIR, s)
        except FileNotFoundError as e:
            print(f"[AVISO] {e}")
            continue
        cmds.append([sys.executable, p, "--db", DB_PATH, "--start", start, "--end", end, "--export-png"])
    return cmds

def _build_graph_cmds(top10: bool, perito: Optional[str],
                      start: str, end: str, min_analises: int,
                      add_comments: bool, topn: int) -> List[List[str]]:
    cmds = []
    for s in GRAPH_SCRIPTS:
        try:
            p = _script_path(GRAPHS_DIR, s)
        except FileNotFoundError as e:
            print(f"[AVISO] {e}")
            continue
        help_txt = _get_help_text(p)
        c = [sys.executable, p, "--start", start, "--end", end]
        if _supports_flag(help_txt, "--min-analises"):
            c += ["--min-analises", str(min_analises)]
        if top10 and _supports_flag(help_txt, "--top10"):
            c += ["--top10"]
            if _supports_flag(help_txt, "--topn"):
                c += ["--topn", str(topn)]
        elif perito and _supports_flag(help_txt, "--perito"):
            c += ["--perito", perito]
        if _supports_flag(help_txt, "--export-png"):
            c += ["--export-png"]
        if add_comments:
            if _supports_flag(help_txt, "--export-comment"):
                c += ["--export-comment"]
            elif _supports_flag(help_txt, "--export-md"):
                c += ["--export-md"]
            if _supports_flag(help_txt, "--add-comments"):
                c += ["--add-comments"]
            if _supports_flag(help_txt, "--call-api"):
                c += ["--call-api"]
        cmds.append(c)
    return cmds

def _detect_r_out_flag(script_path: str) -> Optional[str]:
    try:
        out = subprocess.run(["Rscript", script_path, "--help"], check=False, capture_output=True, text=True)
        text = (out.stdout or "") + "\n" + (out.stderr or "")
        if "--out" in text:
            return "--out"
    except Exception:
        pass
    return None

def _build_r_cmds(top10: bool, perito: Optional[str], start: str, end: str, min_analises: int) -> List[List[str]]:
    cmds = []
    scripts = RCHECK_GROUP_SCRIPTS if top10 else RCHECK_SCRIPTS
    for fname, meta in scripts:
        fpath = os.path.join(RCHECK_DIR, fname)
        if not os.path.exists(fpath):
            print(f"[AVISO] R check ausente: {fpath}")
            continue
        c = ["Rscript", fpath, "--db", DB_PATH, "--start", start, "--end", end, "--min-analises", str(min_analises)]
        if meta.get("need_perito") and perito: c += ["--perito", perito]
        if meta.get("pass_top10", False) and top10: c += ["--top10"]
        out_flag = _detect_r_out_flag(fpath)
        if out_flag: c += [out_flag, EXPORT_DIR]
        for k, v in (meta.get("defaults") or {}).items(): c += [k, str(v)]
        cmds.append(c)
    return cmds

# ────────────────────────────────────────────────────────────────────────────────
# Coleta de assets (PNG/MD) com validação de imagem
# ────────────────────────────────────────────────────────────────────────────────
def _is_valid_png(path: str) -> bool:
    try:
        if Path(path).stat().st_size < 100:
            return False
        from io import BytesIO
        bio = BytesIO(Path(path).read_bytes())
        img = Image.open(bio)
        img.load()
        return True
    except Exception:
        return False

def _collect_assets(start: str, end: str, perito: Optional[str], top10: bool) -> Dict[str, Any]:
    mapping = [
        ("Taxa de Não Conformidade (NC)",            ("nc_rate","nc-rate","comparativo_nc")),
        ("≤ 15 segundos",                            ("fifteen","15s","le15")),
        ("Produtividade (≥ 50/h)",                   ("productivity","produtiv","share_50h")),
        ("Sobreposição de análises",                 ("overlap",)),
        ("Motivos de NC — Top10 vs Brasil",          ("motivo","vs_brasil","motivos")),
        ("Indicadores (ICRA/IATD/Score)",            ("composto","icra","iatd","score")),
        ("Weekday → Weekend (panorama)",             ("weekday","weekend")),
        ("R: Proporções ponderadas (NC/LE)",         ("weighted",)),
        ("R: Testes / Robustez (Top10/Individual)",  ("robust","robustness","chisq","cmh","binom","betabin")),
    ]
    order = {t:i for i,(t,_) in enumerate(mapping)}
    def _classify(fn:str)->Optional[str]:
        low=fn.lower()
        for title, keys in mapping:
            if any(k in low for k in keys): return title
        return None
    sections: Dict[str, Dict[str, Any]] = {}
    for png in sorted(Path(EXPORT_DIR).glob("*.png")):
        key = _classify(png.name)
        if not key:
            continue
        full = str(png)
        if not _is_valid_png(full):
            print(f"[WARN] Ignorando PNG corrompido/truncado: {full}")
            continue
        s = sections.setdefault(key, {"title": key, "images": [], "comment_md": "", "kind":"result"})
        s["images"].append(full)
    for md in sorted(Path(EXPORT_DIR).glob("*.md")):
        key = _classify(md.name.replace(".md",".png"))
        if not key: continue
        sections.setdefault(key, {"title": key, "images": [], "comment_md": "", "kind":"result"})
        sections[key]["comment_md"] = _read_text(str(md))
    ordered = sorted(sections.values(), key=lambda d: order.get(d["title"], 999))
    return {"sections": ordered}

# ────────────────────────────────────────────────────────────────────────────────
# PDF build (com proteção per-image)
# ────────────────────────────────────────────────────────────────────────────────
def _image_flowable(path: str, max_w: float):
    """Abre com PIL, re-encoda para PNG RGB e retorna RLImage dimensionada."""
    from io import BytesIO
    with open(path, "rb") as fh:
        raw = fh.read()
    bio_in = BytesIO(raw)
    pil = Image.open(bio_in)
    pil.load()
    if pil.mode not in ("RGB","L"):
        pil = pil.convert("RGB")
    bio_out = BytesIO()
    pil.save(bio_out, format="PNG")
    bio_out.seek(0)
    img = RLImage(bio_out)
    iw, ih = img.wrap(0,0)
    if iw > max_w:
        scale = max_w/iw
        img._restrictSize(max_w, ih*scale)
    return img

def _build_pdf(meta: Dict[str, Any], assets: Dict[str, Any], pdf_path: str, front_pdfs: Optional[List[str]]=None) -> str:
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
    doc = SimpleDocTemplate(pdf_path, pagesize=A4,
        leftMargin=PDF_MARGIN_LEFT_CM*cm, rightMargin=PDF_MARGIN_RIGHT_CM*cm,
        topMargin=PDF_MARGIN_TOP_CM*cm, bottomMargin=PDF_MARGIN_BOTTOM_CM*cm)
    story: List = []

    title = meta.get("title") or ("Relatório ATESTMED — KPI (Top10)" if meta.get("top10") else f"Relatório ATESTMED — KPI: {meta.get('perito')}")
    story.append(Paragraph(_escape(f"{title} — {meta['start']} a {meta['end']}"), STYLES["Title"]))
    story.append(Spacer(1,6))

    if meta.get("top10") and meta.get("lista_top10"):
        df_list = pd.DataFrame({"Perito":[str(x) for x in meta["lista_top10"]]})
        story.append(Paragraph("Top 10 peritos selecionados", STYLES["Heading2"]))
        story.append(_df_table(df_list, ["Perito"], [doc.width]))
        story.append(Spacer(1,10))

    for sec in assets.get("sections", []):
        story.append(Paragraph(_escape(sec["title"]), STYLES["Heading2"]))
        for path_png in sec["images"]:
            try:
                story.append(_image_flowable(path_png, max_w=doc.width*PDF_IMG_FRAC))
                story.append(Spacer(1,4))
            except Exception as e:
                story.append(Paragraph(_escape(f"[AVISO] Não foi possível inserir a imagem: {path_png} ({e})"), STYLES["Small"]))
        if sec.get("comment_md"):
            # Markdowns simples (sem renderização): quebra por parágrafos
            paras = [p.strip() for p in sec["comment_md"].strip().split("\n\n") if p.strip()]
            for ptext in paras:
                story.append(Paragraph(_escape(ptext).replace("\n","<br/>"), STYLES["Small"]))
        story.append(Spacer(1,10))

    doc.build(story, onFirstPage=lambda c,d:_header_footer(c,d,meta),
                     onLaterPages=lambda c,d:_header_footer(c,d,meta))

    final_path = pdf_path
    if front_pdfs and len(front_pdfs) and PdfMerger:
        merged = pdf_path.replace(".pdf","_FINAL.pdf")
        try:
            merger = PdfMerger()
            for p in front_pdfs:
                if p and os.path.exists(p):
                    merger.append(p)
            merger.append(pdf_path)
            merger.write(merged)
            merger.close()
            final_path = merged
        except Exception as e:
            print(f"[warn] Falha ao concatenar FRONT PDFs: {e}")
    return final_path

# ────────────────────────────────────────────────────────────────────────────────
# Shipping
# ────────────────────────────────────────────────────────────────────────────────
def _ship_outputs(start: str, end: str, subdir: str, files: List[str], copy_png_md: bool=True) -> str:
    dest = os.path.join(OUTPUTS_DIR, f"{start}_a_{end}", subdir)
    os.makedirs(dest, exist_ok=True)
    for p in files:
        if p and os.path.exists(p):
            shutil.copy2(p, os.path.join(dest, os.path.basename(p)))
    if copy_png_md:
        for f in Path(EXPORT_DIR).glob("*.png"):
            shutil.copy2(str(f), os.path.join(dest, f.name))
        for f in Path(EXPORT_DIR).glob("*.md"):
            shutil.copy2(str(f), os.path.join(dest, f.name))
    print(f"[OK] Saída organizada em: {dest}")
    return dest

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
def _build_argparser():
    ap = argparse.ArgumentParser(description="Relatório ATESTMED — KPI (NG, PDF direto)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--top10", action="store_true", help="Relatório Top-10 (KPI)")
    g.add_argument("--perito", type=str, help="Relatório individual para o perito")
    ap.add_argument("--start", required=True); ap.add_argument("--end", required=True)
    ap.add_argument("--min-analises", type=int, default=50)
    ap.add_argument("--topn", type=int, default=10)
    ap.add_argument("--export-png", action="store_true")
    ap.add_argument("--all-tests", action="store_true")
    ap.add_argument("--add-comments", action="store_true")
    ap.add_argument("--export-pdf", action="store_true")
    ap.add_argument("--out-root", default=OUTPUTS_DIR); ap.add_argument("--out-subdir", default=None)
    ap.add_argument("--ship-outputs", action="store_true")
    ap.add_argument("--front-pdf", nargs="*", default=None)
    # compat inócuas (ex-ORG)
    ap.add_argument("--export-org", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--final-org", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--final-org-name", type=str, help=argparse.SUPPRESS)
    ap.add_argument("--no-front", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--no-head", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--header-org", type=str, help=argparse.SUPPRESS)
    ap.add_argument("--front-org", type=str, help=argparse.SUPPRESS)
    ap.add_argument("--selection-mode", type=str, default="kpi", help=argparse.SUPPRESS)
    return ap

# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────
def main():
    args = _build_argparser().parse_args()

    # Limpa quaisquer .org antes de iniciar
    _purge_org_files(EXPORT_DIR)

    # Seleção Top10 / lista de peritos
    lista_top10: List[str] = []
    if args.top10:
        df_top = get_top10_dataframe(args.start, args.end, min_analises=args.min_analises)
        if df_top is None or df_top.empty:
            print("[AVISO] Nenhum perito encontrado para Top10 com os critérios atuais.")
        else:
            lista_top10 = df_top["nomePerito"].tolist()
            print("[INFO] Top10:", lista_top10)

    # Scripts globais
    for c in _build_global_cmds(args.start, args.end):
        _run(c, cwd=GRAPHS_DIR)
    _purge_org_files(EXPORT_DIR)

    # Gráficos Python
    if args.export_png:
        gcmds = _build_graph_cmds(args.top10, args.perito, args.start, args.end, args.min_analises, args.add_comments, args.topn)
        for c in gcmds:
            _run(c, cwd=GRAPHS_DIR)
            _purge_org_files(EXPORT_DIR)

    # R checks
    if args.all_tests:
        rcmds = _build_r_cmds(args.top10, args.perito, args.start, args.end, args.min_analises)
        for c in rcmds:
            _run(c, cwd=RCHECK_DIR)
        _purge_org_files(EXPORT_DIR)

    # Coleta assets + PDF
    assets = _collect_assets(args.start, args.end, args.perito, args.top10)
    meta = {"title":"Relatório ATESTMED — KPI (NG)","top10":bool(args.top10),
            "perito": args.perito, "lista_top10": lista_top10, "start": args.start, "end": args.end}

    safe_perito = _safe(args.perito) if args.perito else None
    pdf_basename = f"kpi_{args.start}_a_{args.end}.pdf" if not args.perito else f"kpi_{safe_perito}_{args.start}_a_{args.end}.pdf"
    pdf_out = os.path.join(EXPORT_DIR, pdf_basename)

    final_path = _build_pdf(meta, assets, pdf_out, front_pdfs=args.front_pdf)

    # Shipping
    if args.ship_outputs or args.export_pdf:
        subdir = args.out_subdir or ("top10/kpi" if args.top10 else f"perito/{safe_perito}/kpi")
        outdir = os.path.join(args.out_root, f"{args.start}_a_{args.end}", subdir)
        os.makedirs(outdir, exist_ok=True)
        shipped_dir = _ship_outputs(args.start, args.end, subdir, [final_path], copy_png_md=True)
        try:
            zip_path = os.path.join(outdir, os.path.basename(pdf_out).replace(".pdf", ".zip"))
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
                for root, _, files in os.walk(shipped_dir):
                    for f in files:
                        full = os.path.join(root, f); rel = os.path.relpath(full, shipped_dir)
                        z.write(full, rel)
            print(f"[OK] ZIP gerado: {zip_path}")
        except Exception as e:
            print(f"[warn] Falha ao zipar: {e}")
        print(f"PDF final: {final_path}\nSaída em: {shipped_dir}")
    else:
        print(f"PDF (local): {final_path}")

if __name__ == "__main__":
    main()
