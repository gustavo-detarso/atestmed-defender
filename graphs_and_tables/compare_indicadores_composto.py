#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Indicadores (composto) — Perito OU Top 10 piores vs Brasil (excl.)
Compatível com o pipeline do make_kpi_report:
- Flags extras: --peritos-csv, --scope-csv, --save-manifests, --fluxo {A,B}, --db, --out-dir
- Fluxo B (padrão): gate %NC ≥ 2× Brasil e ranking por scoreFinal (se disponível).

Barras (4 indicadores) + linhas (média, mediana, média+DP do BR-excl.)
e quadro de “cortes atingidos” (Top10: X/Y; Individual: Sim/Não).

Indicadores (%):
1) % NC                         → (motivoNaoConformado=1 ou conformado=0) / total * 100
2) Produtividade (% do alvo)   → (total / horas_efetivas) / alvo * 100
3) ≤ 15s (%)                    → (dur <= 15s) / total * 100
4) Sobreposição (%)            → % de análises que participam de overlap

Regras:
- Exclui análises com duração > 1h
- Duração: (fim - ini). Se fim faltar, tenta `duracaoPericia` 'HH:MM:SS'
- Sobreposição: análise participa se seu intervalo cruza outro (mesmo perito)

Cortes (opcionais):
--cut-nc-pct, --cut-prod-pct (em % do alvo), --cut-le15s-pct, --cut-overlap-pct
Alvo de produtividade: --alvo-prod (padrão 50 análises/h)

Saídas:
--export-png, --export-org, --export-comment, --export-comment-org (opcional: --chart ASCII, --call-api)

Compat flags (make_kpi_report):
--peritos-csv, --scope-csv, --save-manifests, --fluxo {A,B}, --db, --out-dir
"""

import os
import sys
import argparse
import sqlite3
from typing import List, Dict, Any, Tuple, Optional, Set

# permitir imports de utils/*
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
try:
    import plotext as p
except Exception:
    p = None

from pathlib import Path

# ────────────────────────────────────────────────────────────────────────────────
# Integração com utils/comentarios (opcional)
# ────────────────────────────────────────────────────────────────────────────────
try:
    # ideal: recebe payload e retorna {'prompt','comment'} OU string
    from utils.comentarios import comentar_composto as _comentar_composto_api  # type: ignore
except Exception:
    _comentar_composto_api = None

# ────────────────────────────────────────────────────────────────────────────────
# Paths padrão (podem ser sobrescritos por --db / --out-dir)
# ────────────────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# OpenAI helper (SDK 1.x e legado) + .env
# ────────────────────────────────────────────────────────────────────────────────
def _load_openai_key_from_dotenv() -> Optional[str]:
    """Carrega OPENAI_API_KEY do .env na raiz do projeto (se existir)."""
    env_path = os.path.join(BASE_DIR, ".env")
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(env_path, override=False)
    except Exception:
        if os.path.exists(env_path) and not os.getenv("OPENAI_API_KEY"):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        k, v = line.split("=", 1)
                        if k.strip() == "OPENAI_API_KEY":
                            os.environ.setdefault("OPENAI_API_KEY", v.strip().strip('"').strip("'"))
                            break
            except Exception:
                pass
    return os.getenv("OPENAI_API_KEY")

def _call_openai(messages: List[Dict[str, str]], model: str = "gpt-4o-mini", temperature: float = 0.2) -> Optional[str]:
    """Tenta SDK novo e legado; retorna texto limpo ou None."""
    api_key = _load_openai_key_from_dotenv()
    if not api_key:
        return None
    # SDK novo
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
        )
        txt = (resp.choices[0].message.content or "").strip()
        if txt:
            return txt
    except Exception:
        pass
    # SDK legado
    try:
        import openai  # type: ignore
        openai.api_key = api_key
        resp = openai.ChatCompletion.create(
            model=model,
            messages=messages,
            temperature=temperature,
        )
        txt = resp["choices"][0]["message"]["content"]
        if txt:
            return txt.strip()
    except Exception:
        pass
    return None

def _strip_markers(text: str) -> str:
    """Remove cercas de código, blocos [..], tabelas md/org e diretivas org."""
    if not text:
        return ""
    import re
    text = re.sub(r"^```.*?$", "", text, flags=re.M)
    text = re.sub(r"^~~~.*?$", "", text, flags=re.M)
    kept = []
    for ln in text.splitlines():
        t = ln.strip()
        if not t:
            continue
        if t.startswith("[") and t.endswith("]"):
            continue
        if t.startswith("|"):
            continue
        if t.startswith("#+"):
            continue
        kept.append(ln)
    return "\n".join(kept).strip()

def _to_one_paragraph(text: str) -> str:
    import re
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", " ", text)
    return re.sub(r"\s{2,}", " ", text).strip()

def _cap_words(text: str, max_words: int) -> str:
    ws = text.split()
    return " ".join(ws[:max_words]).rstrip() + ("…" if len(ws) > max_words else "")

# -----------------------
# Utilidades de schema e CSV (compat flags)
# -----------------------

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?",
        (name,)
    ).fetchone() is not None

def detect_analises_table(conn: sqlite3.Connection) -> str:
    for t in ('analises', 'analises_atestmed'):
        if table_exists(conn, t):
            return t
    raise RuntimeError("Não encontrei 'analises' nem 'analises_atestmed'.")

def _load_names_from_csv(path: Optional[str]) -> Optional[Set[str]]:
    """Lê uma coluna 'nomePerito' de um CSV e devolve um conjunto em UPPER()."""
    if not path:
        return None
    try:
        x = pd.read_csv(path)
        if "nomePerito" not in x.columns:
            return None
        return set(x["nomePerito"].astype(str).str.strip().str.upper())
    except Exception:
        return None

def _apply_scope(df_all: pd.DataFrame, scope_csv: Optional[str]) -> pd.DataFrame:
    """Se scope_csv for dado, limita df_all aos peritos listados nele."""
    if df_all.empty or not scope_csv:
        return df_all
    names = _load_names_from_csv(scope_csv)
    if not names:
        return df_all
    return df_all.loc[df_all["nomePerito"].str.strip().str.upper().isin(names)].copy()

# -----------------------
# Carga de dados
# -----------------------

def load_period(conn: sqlite3.Connection, start: str, end: str) -> pd.DataFrame:
    t = detect_analises_table(conn)
    sql = f"""
        SELECT a.protocolo, a.siapePerito,
               a.dataHoraIniPericia AS ini,
               a.dataHoraFimPericia AS fim,
               a.duracaoPericia     AS dur_txt,
               a.motivoNaoConformado AS nc_txt,
               a.conformado          AS conf,
               p.nomePerito
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
         WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
    """
    df = pd.read_sql(sql, conn, params=(start, end))
    df["nomePerito"] = df["nomePerito"].astype(str).str.strip()
    return df

def parse_durations(df: pd.DataFrame) -> pd.DataFrame:
    # parse ini/fim
    df = df.copy()
    df['ini_dt'] = pd.to_datetime(df['ini'], errors='coerce')
    df['fim_dt'] = pd.to_datetime(df['fim'], errors='coerce')

    # duração por coluna dur_txt se necessário
    def hhmmss_to_sec(s: str) -> Optional[float]:
        if not isinstance(s, str) or not s:
            return None
        parts = s.strip().split(':')
        if len(parts) != 3:
            return None
        try:
            h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
            return h*3600 + m*60 + sec
        except Exception:
            return None

    dur = (df['fim_dt'] - df['ini_dt']).dt.total_seconds()
    need_fallback = dur.isna()
    if need_fallback.any():
        dur_fb = df.loc[need_fallback, 'dur_txt'].apply(hhmmss_to_sec)
        dur.loc[need_fallback] = dur_fb.values

    df['dur_s'] = pd.to_numeric(dur, errors='coerce')

    # filtros: remover <=0, e >1h
    df = df[df['dur_s'].notna()]
    df = df[df['dur_s'] > 0]
    df = df[df['dur_s'] <= 3600]  # regra do projeto

    # NC robusto
    conf_int = pd.to_numeric(df.get('conf'), errors='coerce').fillna(1).astype(int)
    nc_txt = df.get('nc_txt').astype(str).str.strip().fillna("")
    nc_cast = pd.to_numeric(nc_txt, errors='coerce').fillna(0).astype(int)
    df['nc_flag'] = np.where(
        (conf_int == 0) | ((nc_txt != "") & (nc_cast != 0)),
        1, 0
    ).astype(int)
    return df

# -----------------------
# Métricas por perito / grupos
# -----------------------

def overlap_percent_for_perito(rows: pd.DataFrame) -> float:
    x = rows[['ini_dt', 'fim_dt']].sort_values('ini_dt').reset_index(drop=True)
    if x.empty:
        return 0.0
    overlapped = np.zeros(len(x), dtype=bool)
    current_end = pd.Timestamp.min
    last_idx = -1
    for i, (ini, fim) in enumerate(zip(x['ini_dt'], x['fim_dt'])):
        if pd.isna(ini) or pd.isna(fim):
            continue
        if ini < current_end:  # overlap com anterior
            overlapped[i] = True
            if last_idx >= 0:
                overlapped[last_idx] = True
            current_end = max(current_end, fim)
        else:
            current_end = fim
            last_idx = i
    pct = overlapped.sum() / len(x) * 100.0
    return float(pct)

def perito_metrics(rows: pd.DataFrame, alvo_prod: float) -> Dict[str, float]:
    total = len(rows)
    if total == 0:
        return dict(nc_pct=0.0, prod_pct=0.0, le15s_pct=0.0, overlap_pct=0.0,
                    prod_abs=0.0)
    horas = rows['dur_s'].sum() / 3600.0
    prod_abs = (total / horas) if horas > 0 else 0.0
    prod_pct = (prod_abs / alvo_prod * 100.0) if alvo_prod > 0 else 0.0
    nc_pct = rows['nc_flag'].sum() / total * 100.0
    le15s_pct = (rows['dur_s'] <= 15).sum() / total * 100.0
    overlap_pct = overlap_percent_for_perito(rows)
    return dict(nc_pct=float(nc_pct),
                prod_pct=float(prod_pct),
                le15s_pct=float(le15s_pct),
                overlap_pct=float(overlap_pct),
                prod_abs=float(prod_abs))

def build_panels(df: pd.DataFrame,
                 grupo: List[str],
                 alvo_prod: float) -> Tuple[Dict[str, float], Dict[str, float],
                                            pd.DataFrame]:
    df_g = df[df['nomePerito'].str.upper().isin([g.upper() for g in grupo])]
    df_b = df[~df['nomePerito'].str.upper().isin([g.upper() for g in grupo])]

    def panel_from(df_any: pd.DataFrame) -> Dict[str, float]:
        if df_any.empty:
            return dict(nc_pct=0.0, prod_pct=0.0, le15s_pct=0.0, overlap_pct=0.0)
        total = len(df_any)
        nc_pct = df_any['nc_flag'].sum() / total * 100.0
        le15s_pct = (df_any['dur_s'] <= 15).sum() / total * 100.0
        horas = df_any['dur_s'].sum() / 3600.0
        prod_abs = (total / horas) if horas > 0 else 0.0
        prod_pct = (prod_abs / alvo_prod * 100.0) if alvo_prod > 0 else 0.0
        # overlap: média ponderada pelo nº de análises de cada perito
        weights = df_any.groupby('nomePerito').size()
        ov = 0.0
        for (nome, sub), w in zip(df_any.groupby('nomePerito'), weights):
            ov += overlap_percent_for_perito(sub) * (w / total)
        return dict(nc_pct=float(nc_pct),
                    prod_pct=float(prod_pct),
                    le15s_pct=float(le15s_pct),
                    overlap_pct=float(ov))

    grp_panel = panel_from(df_g)
    br_panel  = panel_from(df_b)

    metrics_b = []
    for nome, sub in df_b.groupby('nomePerito'):
        m = perito_metrics(sub, alvo_prod)
        m['nomePerito'] = nome
        metrics_b.append(m)
    mdf_b = pd.DataFrame(metrics_b)
    return grp_panel, br_panel, mdf_b

# -----------------------
# Estatísticas (BR-excl.)
# -----------------------

def compute_br_stats(mdf_b: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    def stats(col: str) -> Dict[str, float]:
        v = mdf_b[col].dropna().values if col in mdf_b.columns else np.array([])
        if v.size == 0:
            return dict(mean=0.0, median=0.0, mean_plus_sd=0.0)
        mu = float(np.mean(v)); sd = float(np.std(v)); med = float(np.median(v))
        return dict(mean=mu, median=med, mean_plus_sd=mu + sd)

    return {
        "nc_pct":       stats("nc_pct"),
        "prod_pct":     stats("prod_pct"),
        "le15s_pct":    stats("le15s_pct"),
        "overlap_pct":  stats("overlap_pct"),
    }

# -----------------------
# Contagem de “cortes”
# -----------------------

def count_cut_hits(df: pd.DataFrame,
                   grupo: List[str],
                   alvo_prod: float,
                   cut_nc_pct: Optional[float],
                   cut_prod_pct: Optional[float],
                   cut_le15s_pct: Optional[float],
                   cut_overlap_pct: Optional[float]) -> Dict[str, Any]:
    peritos = sorted(set([n for n in df['nomePerito'].unique() if n]))
    sel = [p for p in peritos if p.upper() in {g.upper() for g in grupo}]

    def hit_row(sub: pd.DataFrame) -> Dict[str, bool]:
        m = perito_metrics(sub, alvo_prod)
        return dict(
            nc = (cut_nc_pct is not None and m['nc_pct'] >= cut_nc_pct),
            prod = (cut_prod_pct is not None and m['prod_pct'] >= cut_prod_pct),
            le15s = (cut_le15s_pct is not None and m['le15s_pct'] >= cut_le15s_pct),
            overlap = (cut_overlap_pct is not None and m['overlap_pct'] >= cut_overlap_pct),
        )

    if len(sel) == 1:
        sub = df[df['nomePerito'].str.upper()==sel[0].upper()]
        return hit_row(sub)

    hits = dict(nc=0, prod=0, le15s=0, overlap=0)
    for nome in sel:
        sub = df[df['nomePerito'].str.upper()==nome.upper()]
        h = hit_row(sub)
        for k in hits.keys():
            hits[k] += int(bool(h[k]))
    return dict(
        nc=f"{hits['nc']}/{len(sel)}" if cut_nc_pct is not None else "—",
        prod=f"{hits['prod']}/{len(sel)}" if cut_prod_pct is not None else "—",
        le15s=f"{hits['le15s']}/{len(sel)}" if cut_le15s_pct is not None else "—",
        overlap=f"{hits['overlap']}/{len(sel)}" if cut_overlap_pct is not None else "—",
        _n=len(sel)
    )

# -----------------------
# Seleção Top10 — Fluxo A vs Fluxo B (compat make_kpi_report)
# -----------------------

def _scores_df(conn: sqlite3.Connection) -> pd.DataFrame:
    """Lê (nomePerito, scoreFinal) da tabela indicadores se existir."""
    if not table_exists(conn, "indicadores"):
        return pd.DataFrame(columns=["nomePerito","scoreFinal"])
    sql = """
        SELECT p.nomePerito, i.scoreFinal
          FROM indicadores i
          JOIN peritos p ON p.siapePerito = i.perito
    """
    df = pd.read_sql(sql, conn)
    df["nomePerito"] = df["nomePerito"].astype(str).str.strip()
    return df

def _top10_fluxo_a(conn: sqlite3.Connection, start: str, end: str, min_analises: int) -> List[str]:
    """Top10 piores somente por scoreFinal (compat fluxo A)."""
    t = detect_analises_table(conn)
    if not table_exists(conn, "indicadores"):
        return []
    sql = f"""
        SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
          FROM indicadores i
          JOIN peritos p  ON i.perito = p.siapePerito
          JOIN {t} a      ON a.siapePerito = i.perito
         WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
         GROUP BY p.nomePerito, i.scoreFinal
        HAVING total_analises >= ?
         ORDER BY i.scoreFinal DESC, total_analises DESC
         LIMIT 10
    """
    rows = conn.execute(sql, (start, end, min_analises)).fetchall()
    return [r[0] for r in rows]

def _top10_fluxo_b(conn: sqlite3.Connection, df_periodo_parsed: pd.DataFrame,
                   start: str, end: str, min_analises: int) -> List[str]:
    """
    Gate: %NC_perito ≥ 2× p_BR e N ≥ min_analises.
    Ranking: scoreFinal (se disponível). Caso não haja 'indicadores', retorna só os gated (até 10).
    """
    # p_br no período
    tot = len(df_periodo_parsed)
    if tot == 0:
        return []
    p_br = df_periodo_parsed['nc_flag'].sum() / tot

    base = df_periodo_parsed.groupby("nomePerito").agg(
        N=("protocolo","count"),
        NC=("nc_flag","sum")
    ).reset_index()
    base["p_hat"] = base["NC"] / base["N"].replace(0, np.nan)
    gated = base.loc[(base["N"] >= int(min_analises)) & (base["p_hat"] >= 2.0 * float(p_br))].copy()
    if gated.empty:
        return []

    # ranking por scoreFinal se houver
    scores = _scores_df(conn)
    if scores.empty:
        out = gated.sort_values(["NC","N"], ascending=[False, False]).head(10)["nomePerito"].tolist()
        return out

    merged = gated.merge(scores, on="nomePerito", how="left")
    merged["scoreFinal"] = pd.to_numeric(merged["scoreFinal"], errors="coerce").fillna(0.0)
    out = merged.sort_values(["scoreFinal","NC","N"], ascending=[False, False, False]).head(10)["nomePerito"].tolist()
    return out

# -----------------------
# Gráfico
# -----------------------

def plot_png(start: str, end: str,
             grp_title: str,
             grp_panel: Dict[str, float],
             br_panel: Dict[str, float],
             br_stats: Dict[str, Dict[str, float]],
             alvo_prod: float,
             cut_prod_pct: Optional[float],
             cut_hits: Dict[str, Any],
             out_path: str) -> str:

    labels  = ['% NC', 'Produtividade (% alvo)', '≤ 15s (%)', 'Sobreposição (%)']
    grp_vals = [grp_panel['nc_pct'], grp_panel['prod_pct'], grp_panel['le15s_pct'], grp_panel['overlap_pct']]
    br_vals  = [br_panel['nc_pct'],  br_panel['prod_pct'],  br_panel['le15s_pct'],  br_panel['overlap_pct']]

    means  = [br_stats['nc_pct']['mean'],   br_stats['prod_pct']['mean'],   br_stats['le15s_pct']['mean'],   br_stats['overlap_pct']['mean']]
    meds   = [br_stats['nc_pct']['median'], br_stats['prod_pct']['median'], br_stats['le15s_pct']['median'], br_stats['overlap_pct']['median']]
    meanp1 = [br_stats['nc_pct']['mean_plus_sd'], br_stats['prod_pct']['mean_plus_sd'], br_stats['le15s_pct']['mean_plus_sd'], br_stats['overlap_pct']['mean_plus_sd']]

    fig, ax = plt.subplots(figsize=(11, 6.5), dpi=220)
    x = np.arange(len(labels)); width = 0.34

    ax.bar(x - width/2, grp_vals, width, label=grp_title, edgecolor='black')
    ax.bar(x + width/2, br_vals,  width, label='Brasil (excl.)', edgecolor='black')

    ax.plot(x, means,  'o--', label='Média (BR-excl.)')
    ax.plot(x, meds,   's:',  label='Mediana (BR-excl.)')
    ax.plot(x, meanp1, '^-.', label='Média+DP (BR-excl.)')

    if cut_prod_pct is not None:
        ax.plot([x[1]], [cut_prod_pct], marker='D', color='red', linestyle='None', label='Corte Prod. (% alvo)')

    ax.set_xticks(x, labels, rotation=20, ha='right')
    ax.set_ylabel("%")
    ax.set_title(f"Indicadores (composto) – {grp_title} vs Brasil (excl.)\n"
                 f"{start} a {end}  |  alvo prod: {int(alvo_prod)}/h")
    ax.grid(axis='y', linestyle='--', alpha=0.4)

    leg = ax.legend(loc='upper right', framealpha=0.95)

    ymax = max(grp_vals + br_vals + [1.0])
    for i in range(len(labels)):
        ax.text(x[i] - width/2, grp_vals[i] + ymax*0.02, f"{grp_vals[i]:.1f}%", ha='center', va='bottom', fontsize=9)
        ax.text(x[i] + width/2, br_vals[i]  + ymax*0.02, f"{br_vals[i]:.1f}%",  ha='center', va='bottom', fontsize=9)

    if '_n' in cut_hits:  # Top10
        n = cut_hits['_n']
        lines = [
            f"Cortes atingidos (grupo, {n} peritos):",
            f" • % NC ≥ corte .......... {cut_hits['nc']}",
            f" • Prod (% alvo) ≥ corte . {cut_hits['prod']}",
            f" • ≤ 15s ≥ corte .......... {cut_hits['le15s']}",
            f" • Sobreposição ≥ corte .. {cut_hits['overlap']}",
        ]
    else:
        def fmt(v): return "Sim" if v else "Não"
        lines = [
            "Perito atingiu os cortes?",
            f" • % NC ≥ corte .......... {fmt(cut_hits.get('nc'))}",
            f" • Prod (% alvo) ≥ corte . {fmt(cut_hits.get('prod'))}",
            f" • ≤ 15s ≥ corte .......... {fmt(cut_hits.get('le15s'))}",
            f" • Sobreposição ≥ corte .. {fmt(cut_hits.get('overlap'))}",
        ]
    box_text = "\n".join(lines)

    fig.canvas.draw()
    bbox_px = leg.get_window_extent(fig.canvas.get_renderer())
    bbox_ax = bbox_px.transformed(ax.transAxes.inverted())
    x_right = min(0.98, bbox_ax.x1)
    y_below = max(0.05, bbox_ax.y0 - 0.02)

    ax.text(x_right, y_below, box_text,
            transform=ax.transAxes, ha='right', va='top', fontsize=9,
            bbox=dict(facecolor='white', alpha=0.92, edgecolor='#999'))

    plt.tight_layout()
    out = os.path.abspath(out_path)
    fig.savefig(out, bbox_inches='tight')
    plt.close(fig)
    print(f"✅ PNG salvo em: {out}")
    return out

# -----------------------
# Export ORG (inclui estatísticas BR-excl.)
# -----------------------

def export_org(path_png: Optional[str],
               start: str, end: str,
               grp_title: str,
               grp_panel: Dict[str, float],
               br_panel: Dict[str, float],
               br_stats: Dict[str, Dict[str, float]],
               alvo_prod: float,
               cuts: Dict[str, Optional[float]],
               cut_hits: Dict[str, Any],
               out_name: str) -> str:

    out_path = os.path.join(EXPORT_DIR, out_name)
    lines = []
    lines.append(f"* Indicadores (composto) – {grp_title} vs Brasil (excl.)")
    lines.append(":PROPERTIES:")
    lines.append(f":PERIODO: {start} a {end}")
    lines.append(f":ALVO_PROD: {alvo_prod}/h")
    cuts_str = ", ".join([f"{k}={v}" for k, v in cuts.items() if v is not None]) or "nenhum"
    lines.append(f":CUTS: {cuts_str}")
    lines.append(":END:\n")

    # Tabela de valores (barras)
    lines.append("| Indicador | " + grp_title + " | Brasil (excl.) |")
    lines.append("|-")
    lines.append(f"| % NC | {grp_panel['nc_pct']:.2f}% | {br_panel['nc_pct']:.2f}% |")
    lines.append(f"| Prod (% alvo) | {grp_panel['prod_pct']:.2f}% | {br_panel['prod_pct']:.2f}% |")
    lines.append(f"| ≤ 15s (%) | {grp_panel['le15s_pct']:.2f}% | {br_panel['le15s_pct']:.2f}% |")
    lines.append(f"| Sobreposição (%) | {grp_panel['overlap_pct']:.2f}% | {br_panel['overlap_pct']:.2f}% |\n")

    # Estatísticas BR-excl. (linhas)
    lines.append("** Estatísticas BR-excl. (linhas)")
    lines.append("| Indicador | Média | Mediana | Média+DP |")
    lines.append("|-")
    lines.append(f"| % NC | {br_stats['nc_pct']['mean']:.2f}% | {br_stats['nc_pct']['median']:.2f}% | {br_stats['nc_pct']['mean_plus_sd']:.2f}% |")
    lines.append(f"| Prod (% alvo) | {br_stats['prod_pct']['mean']:.2f}% | {br_stats['prod_pct']['median']:.2f}% | {br_stats['prod_pct']['mean_plus_sd']:.2f}% |")
    lines.append(f"| ≤ 15s (%) | {br_stats['le15s_pct']['mean']:.2f}% | {br_stats['le15s_pct']['median']:.2f}% | {br_stats['le15s_pct']['mean_plus_sd']:.2f}% |")
    lines.append(f"| Sobreposição (%) | {br_stats['overlap_pct']['mean']:.2f}% | {br_stats['overlap_pct']['median']:.2f}% | {br_stats['overlap_pct']['mean_plus_sd']:.2f}% |\n")

    # Tabela cortes atingidos
    lines.append("** Cortes atingidos")
    if '_n' in cut_hits:
        lines.append(f"Grupo com {cut_hits['_n']} peritos.")
        lines.append("| Corte | Atingiram |")
        lines.append("|-")
        lines.append(f"| % NC | {cut_hits['nc']} |")
        lines.append(f"| Prod (% alvo) | {cut_hits['prod']} |")
        lines.append(f"| ≤ 15s | {cut_hits['le15s']} |")
        lines.append(f"| Sobreposição | {cut_hits['overlap']} |\n")
    else:
        def sym(b): return "Sim" if b else "Não"
        lines.append("| Corte | Atingiu? |")
        lines.append("|-")
        lines.append(f"| % NC | {sym(cut_hits.get('nc'))} |")
        lines.append(f"| Prod (% alvo) | {sym(cut_hits.get('prod'))} |")
        lines.append(f"| ≤ 15s | {sym(cut_hits.get('le15s'))} |")
        lines.append(f"| Sobreposição | {sym(cut_hits.get('overlap'))} |\n")

    if path_png and os.path.exists(path_png):
        lines.append("#+CAPTION: Indicadores compostos (barras) e estatísticas do BR-excl. (linhas).")
        lines.append(f"[[file:{os.path.basename(path_png)}]]\n")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"✅ Org salvo em: {out_path}")
    return out_path

# -----------------------
# Comentários (composto)
# -----------------------

def _fallback_prompt_composto(payload: Dict[str, Any]) -> str:
    start, end = payload["period"]
    grp_title  = payload["grp_title"]
    alvo_prod  = payload["alvo_prod"]
    G = payload["metrics"]        # grupo
    B = payload["br_metrics"]     # BR-excl.
    S = payload["br_stats"]       # linhas
    cuts = payload.get("cuts", {})
    cut_hits = payload.get("cut_hits", {})
    cuts_txt = ", ".join([f"{k}={v}" for k, v in cuts.items() if v is not None]) or "nenhum"

    return (
        f"[Contexto]\n"
        f"Período: {start} a {end}. Grupo: {grp_title}. Alvo de produtividade: {int(alvo_prod)}/h.\n"
        f"As barras mostram o grupo vs Brasil (excl.). As linhas referenciam o BR-excl.: média, mediana e média+DP.\n\n"
        f"[Barras — valores]\n"
        f"- % NC: {G['nc_pct']:.1f}% (grupo) vs {B['nc_pct']:.1f}% (BR-excl.)\n"
        f"- Prod (% alvo): {G['prod_pct']:.1f}% vs {B['prod_pct']:.1f}%\n"
        f"- ≤15s: {G['le15s_pct']:.1f}% vs {B['le15s_pct']:.1f}%\n"
        f"- Sobreposição: {G['overlap_pct']:.1f}% vs {B['overlap_pct']:.1f}%\n\n"
        f"[Linhas — BR-excl.]\n"
        f"- % NC: média {S['nc_pct']['mean']:.1f}%, mediana {S['nc_pct']['median']:.1f}%, média+DP {S['nc_pct']['mean_plus_sd']:.1f}%\n"
        f"- Prod: média {S['prod_pct']['mean']:.1f}%, mediana {S['prod_pct']['median']:.1f}%, média+DP {S['prod_pct']['mean_plus_sd']:.1f}%\n"
        f"- ≤15s: média {S['le15s_pct']['mean']:.1f}%, mediana {S['le15s_pct']['median']:.1f}%, média+DP {S['le15s_pct']['mean_plus_sd']:.1f}%\n"
        f"- Sobreposição: média {S['overlap_pct']['mean']:.1f}%, mediana {S['overlap_pct']['median']:.1f}%, média+DP {S['overlap_pct']['mean_plus_sd']:.1f}%\n\n"
        f"[Cortes]\n"
        f"Cortes configurados: {cuts_txt}. Resultado (grupo/perito): {cut_hits}.\n\n"
        f"[Instruções ao modelo]\n"
        f"1) Em 3–4 frases, descreva o posicionamento do grupo em cada indicador vs as linhas de referência do BR-excl.\n"
        f"2) Aponte quais indicadores mais contribuem para o composto (ex.: %NC alto + ≤15s alto).\n"
        f"3) Registre limitações de leitura (bases pequenas, dispersão alta, outliers) quando pertinente.\n"
        f"4) Feche com uma ação objetiva (ex.: revisar motivos de NC mais discrepantes; verificar janelas de sobreposição).\n"
    )

def _build_messages_composto(payload: Dict[str, Any], max_words: int = 180) -> List[Dict[str, str]]:
    """Prompt para GPT em texto corrido, um parágrafo, com resumo JSON dos dados."""
    start, end = payload["period"]
    grp_title  = payload["grp_title"]
    resumo = {
        "periodo": f"{start} a {end}",
        "grupo": grp_title,
        "alvo_prod": float(payload.get("alvo_prod", 50.0)),
        "barras": {
            "grupo": payload["metrics"],
            "br_excl": payload["br_metrics"],
        },
        "linhas_br_excl": payload["br_stats"],
        "cuts": payload.get("cuts", {}),
        "cut_hits": payload.get("cut_hits", {}),
    }
    system = "Você é um analista de dados do ATESTMED. Escreva comentários claros, objetivos e tecnicamente corretos."
    user = (
        "Escreva um comentário interpretativo, em português (Brasil), para um gráfico composto com 4 indicadores "
        "(barras) e linhas de referência do BR-excl. Use TEXTO CORRIDO (um único parágrafo, sem títulos/listas/tabelas), "
        f"com no máximo {max_words} palavras. Inclua: (1) leitura comparativa por indicador; "
        "(2) destaque de onde há maior desvio em pontos percentuais; "
        "(3) referência às linhas (média, mediana, média+DP); "
        "(4) ressalvas amostrais quando pertinente; e (5) uma ação objetiva de verificação. "
        "Dados resumidos (JSON):\n\n" + __import__("json").dumps(resumo, ensure_ascii=False)
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]

def _gerar_comentario_composto(payload: Dict[str, Any], call_api: bool, max_words: int = 180) -> str:
    """Gera comentário final. Tenta utils.comentarios; se não, tenta API local; por fim retorna prompt fallback."""
    # 1) Se houver função no utils, usar
    if _comentar_composto_api:
        try:
            res = _comentar_composto_api(payload, call_api=call_api)
            if isinstance(res, dict):
                txt = (res.get("comment") or res.get("prompt") or "").strip()
            else:
                txt = str(res or "").strip()
            if txt:
                txt = _cap_words(_to_one_paragraph(_strip_markers(txt)), max_words)
                return txt
        except Exception as e:
            print(f"⚠️ comentar_composto falhou: {e}. Tentando API direta/fallback.")
    # 2) API direta (se houver chave)
    if call_api or _load_openai_key_from_dotenv():
        msgs = _build_messages_composto(payload, max_words=max_words)
        txt = _call_openai(msgs, model="gpt-4o-mini", temperature=0.2)
        if txt:
            return _cap_words(_to_one_paragraph(_strip_markers(txt)), max_words)
    # 3) Fallback: prompt instrucional
    fb = _fallback_prompt_composto(payload)
    return _cap_words(_to_one_paragraph(_strip_markers(fb)), max_words)

# -----------------------
# CLI e pipeline (compat make_kpi_report)
# -----------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Indicadores (composto): Perito OU Top10 piores vs Brasil (excl.).")
    ap.add_argument('--start', required=True)
    ap.add_argument('--end',   required=True)

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--perito', help='Nome exato do perito')
    g.add_argument('--top10', action='store_true', help='Usa os 10 piores (ver --fluxo)')

    # compat: controle de DB/out-dir como no wrapper
    ap.add_argument('--db', default=DB_PATH, help='Caminho do SQLite (padrão: db/atestmed.db)')
    ap.add_argument('--out-dir', default=EXPORT_DIR, help='Diretório de saída (padrão: graphs_and_tables/exports)')

    ap.add_argument('--min-analises', type=int, default=50, help='Mínimo p/ elegibilidade ao Top10 [50]')
    ap.add_argument('--alvo-prod', type=float, default=50.0, help='Alvo de produtividade (análises/h) [50]')

    # cortes (opcionais)
    ap.add_argument('--cut-prod-pct', type=float, default=100.0, help='Corte Produtividade (%% do alvo) [100]')
    ap.add_argument('--cut-nc-pct', type=float, default=None, help='Corte %% NC (opcional)')
    ap.add_argument('--cut-le15s-pct', type=float, default=None, help='Corte %% ≤15s (opcional)')
    ap.add_argument('--cut-overlap-pct', type=float, default=None, help='Corte %% sobreposição (opcional)')

    # exportações/saídas
    ap.add_argument('--export-png', action='store_true')
    ap.add_argument('--export-org', action='store_true')
    ap.add_argument('--export-comment', action='store_true', help='Gera *_comment.md (IA) ao lado do .org; se --call-api ausente, salva só o prompt')
    ap.add_argument('--export-comment-org', action='store_true',
                    help='Gera comentário (IA ou prompt) e incorpora no arquivo .org')
    ap.add_argument('--call-api', action='store_true', help='Chama a API (utils.comentarios) ou OpenAI local para obter o texto final do comentário')
    ap.add_argument('--chart', action='store_true', help='Gráfico ASCII no terminal')

    # ╭───────────────────────────────────── compat wrapper ─────────────────────────────────────╮
    # │ ver make_kpi_report: --peritos-csv / --scope-csv / --save-manifests / --fluxo (A/B)     │
    # ╰─────────────────────────────────────────────────────────────────────────────────────────╯
    ap.add_argument('--peritos-csv', default=None, help="(Opcional) CSV com lista de peritos (coluna nomePerito). Se informado, sobrescreve a seleção Top10/--perito.")
    ap.add_argument('--scope-csv', default=None, help="(Opcional) CSV com peritos que definem o ESCOPO da base para gráficos (ex.: coorte do gate do fluxo B).")
    ap.add_argument('--save-manifests', action='store_true', help="Salvar CSVs com Top10 e escopo (gate) usados neste run.")
    ap.add_argument('--fluxo', choices=['A','B'], default='B',
                    help="B (padrão): gate %%NC ≥ 2× Brasil e ranking por scoreFinal; A: Top 10 direto por scoreFinal.")
    # ───────────────────────────────────────────────────────────────────────────────────────────

    # flag inócua para compatibilidade (ignoramos se passada pelo wrapper global)
    ap.add_argument('--export-protocols', action='store_true', help=argparse.SUPPRESS)

    return ap.parse_args()

# -----------------------
# Main
# -----------------------

def main():
    args = parse_args()

    # sobrescreve caminhos globais se vierem por CLI (compat)
    global DB_PATH, EXPORT_DIR
    DB_PATH = args.db
    EXPORT_DIR = args.out_dir
    os.makedirs(EXPORT_DIR, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        df = load_period(conn, args.start, args.end)

    df = parse_durations(df)
    if df.empty:
        print("⚠️ Sem dados no período.")
        return

    # aplica escopo, se fornecido (compat make_kpi_report)
    df_scoped = _apply_scope(df, args.scope_csv)

    # Seleção do grupo (perito ou top10)
    if args.peritos_csv:
        names_set = _load_names_from_csv(args.peritos_csv) or set()
        if not names_set:
            print("⚠️ --peritos-csv não pôde ser lido ou está sem coluna 'nomePerito'.")
            return
        grupo = sorted({n for n in df_scoped['nomePerito'].unique()
                        if n.strip().upper() in names_set})
        if not grupo:
            print("⚠️ Nenhum perito do CSV encontrado no período/escopo.")
            return
        grp_title = "Top 10 piores" if args.top10 else "Seleção CSV"
    elif args.top10:
        with sqlite3.connect(DB_PATH) as conn:
            if args.fluxo.upper() == "B":
                grupo = _top10_fluxo_b(conn, df, args.start, args.end, args.min_analises)
            else:
                grupo = _top10_fluxo_a(conn, args.start, args.end, args.min_analises)
        if not grupo:
            print("⚠️ Nenhum perito elegível ao Top10 no período.")
            return
        grp_title = "Top 10 piores"
    else:
        if not args.perito:
            print("ERRO: informe --perito, --top10 ou --peritos-csv.")
            return
        grupo = [args.perito]
        grp_title = args.perito

    # painéis e estatísticas BR-excl. (sempre sobre df_scoped — respeita --scope-csv)
    grp_panel, br_panel, mdf_b = build_panels(df_scoped, grupo, args.alvo_prod)
    br_stats = compute_br_stats(mdf_b)

    # contagem de cortes
    cut_hits = count_cut_hits(
        df_scoped, grupo, args.alvo_prod,
        cut_nc_pct=args.cut_nc_pct,
        cut_prod_pct=args.cut_prod_pct,
        cut_le15s_pct=args.cut_le15s_pct,
        cut_overlap_pct=args.cut_overlap_pct
    )

    # nomes de saída
    safe = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in grp_title).strip("_")
    png_name = "indicadores_composto_top10.png" if args.top10 else f"indicadores_composto_{safe}.png"
    org_name = "indicadores_composto_top10.org" if args.top10 else f"indicadores_composto_{safe}.org"

    # opcionalmente salva manifests (compat wrapper)
    if args.save_manifests and args.top10:
        try:
            peritos_csv_path = os.path.join(EXPORT_DIR, "top10_peritos.csv")
            pd.DataFrame({"nomePerito": grupo, "rank": range(1, len(grupo)+1),
                          "fluxo": args.fluxo, "start": args.start, "end": args.end}
                        ).to_csv(peritos_csv_path, index=False, encoding="utf-8")
            print(f"🗂️  top10_peritos.csv salvo em {peritos_csv_path}")
            if args.fluxo.upper() == "B" and args.scope_csv:
                # se o escopo veio externamente, replique como manifest
                scope_csv_path = os.path.join(EXPORT_DIR, "scope_gate_b.csv")
                pd.DataFrame({"nomePerito": sorted({n for n in df['nomePerito'].unique()
                                                   if n.strip().upper() in (_load_names_from_csv(args.scope_csv) or set())})}
                            ).to_csv(scope_csv_path, index=False, encoding="utf-8")
                print(f"🗂️  scope_gate_b.csv salvo em {scope_csv_path}")
        except Exception as e:
            print(f"[WARN] Falha salvando manifests: {e}")

    # gerar PNG
    png_path = None
    if args.export_png:
        png_path = plot_png(
            start=args.start, end=args.end,
            grp_title=grp_title,
            grp_panel=grp_panel, br_panel=br_panel, br_stats=br_stats,
            alvo_prod=args.alvo_prod,
            cut_prod_pct=args.cut_prod_pct,
            cut_hits=cut_hits,
            out_path=os.path.join(EXPORT_DIR, png_name)
        )

    # gerar ORG (e comentário, se pedido)
    if args.export_org or args.export_comment or args.export_comment_org:
        if not png_path:
            png_path = plot_png(
                start=args.start, end=args.end,
                grp_title=grp_title,
                grp_panel=grp_panel, br_panel=br_panel, br_stats=br_stats,
                alvo_prod=args.alvo_prod,
                cut_prod_pct=args.cut_prod_pct,
                cut_hits=cut_hits,
                out_path=os.path.join(EXPORT_DIR, png_name)
            )

        cuts_dict = dict(cut_prod_pct=args.cut_prod_pct,
                         cut_nc_pct=args.cut_nc_pct,
                         cut_le15s_pct=args.cut_le15s_pct,
                         cut_overlap_pct=args.cut_overlap_pct)

        org_path = export_org(
            path_png=png_path,
            start=args.start, end=args.end,
            grp_title=grp_title,
            grp_panel=grp_panel, br_panel=br_panel, br_stats=br_stats,
            alvo_prod=args.alvo_prod,
            cuts=cuts_dict,
            cut_hits=cut_hits,
            out_name=org_name
        )

        # comentário
        if args.export_comment or args.export_comment_org:
            payload = {
                "period": (args.start, args.end),
                "grp_title": grp_title,
                "grupo": "top10" if (args.top10 or args.peritos_csv) else "perito",
                "alvo_prod": args.alvo_prod,
                "metrics": grp_panel,
                "br_metrics": br_panel,
                "br_stats": br_stats,
                "cuts": cuts_dict,
                "cut_hits": cut_hits,
            }
            call_api = bool(args.call_api or _load_openai_key_from_dotenv())
            comment_text = _gerar_comentario_composto(payload, call_api=call_api)

            if args.export_comment:
                cpath = org_path.replace(".org", "_comment.md")
                Path(cpath).write_text(comment_text, encoding="utf-8")
                print(f"✅ Comentário salvo em: {cpath}")

            if args.export_comment_org:
                with open(org_path, "a", encoding="utf-8") as f:
                    f.write("\n** Comentário\n")
                    f.write(comment_text.strip() + "\n")
                print(f"✅ Comentário incorporado ao ORG: {org_path}")

    if args.chart:
        if p is None:
            print("plotext não instalado; pulei o gráfico ASCII.")
        else:
            labels = ['%NC', 'Prod(%alvo)', '≤15s', 'Sobrep.']
            p.clear_data()
            try:
                func = getattr(p, "multiple_bar", None) or getattr(p, "multi_bar", None)
                if func:
                    func(labels,
                         [[grp_panel['nc_pct'], grp_panel['prod_pct'], grp_panel['le15s_pct'], grp_panel['overlap_pct']],
                          [br_panel['nc_pct'],  br_panel['prod_pct'],  br_panel['le15s_pct'],  br_panel['overlap_pct']]],
                         label=[grp_title, 'Brasil (excl.)'])
                else:
                    p.bar(labels, [grp_panel['nc_pct'], grp_panel['prod_pct'], grp_panel['le15s_pct'], grp_panel['overlap_pct']], label=grp_title)
                    p.bar(labels, [br_panel['nc_pct'],  br_panel['prod_pct'],  br_panel['le15s_pct'],  br_panel['overlap_pct']], label='Brasil (excl.)')
                p.title(f"Indicadores (composto) — {grp_title} vs BR-excl.")
                p.plotsize(90, 20)
                p.show()
            except Exception:
                p.clear_data()
                p.bar([grp_title, 'Brasil (excl.)'], [1, 1])  # placeholder
                p.title("plotext incompatível; exibição simplificada")
                p.show()

if __name__ == "__main__":
    main()

