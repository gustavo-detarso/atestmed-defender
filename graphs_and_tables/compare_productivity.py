#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sqlite3
import argparse
from typing import Optional, Callable, Tuple, List, Set

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt

try:
    import plotext as p  # opcional para --chart
except Exception:
    p = None

from utils.comentarios import comentar_produtividade  # Integração GPT

# Caminhos
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Args
# ────────────────────────────────────────────────────────────────────────────────
def parse_args():
    ap = argparse.ArgumentParser(description="Produtividade ≥ threshold/h (Perito ou Top 10) vs Brasil (excl.)")
    ap.add_argument('--start',     required=True, help='Data inicial YYYY-MM-DD')
    ap.add_argument('--end',       required=True, help='Data final   YYYY-MM-DD')

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--perito', help='Nome do perito a destacar (exato)')
    g.add_argument('--nome',   help='Nome do perito a destacar (alias)')
    g.add_argument('--top10',  action='store_true', help='Comparar os 10 piores por scoreFinal no período')

    ap.add_argument('--min-analises', type=int, default=50,
                    help='Elegibilidade p/ Top 10 (mínimo de análises no período)')
    ap.add_argument('--threshold', '-t', type=int, default=50,
                    help='Limite de produtividade (análises por hora)')

    # Métrica: perito-share (padrão), task-share (ponderada por tarefas), time-share (ponderada por tempo)
    ap.add_argument('--mode', choices=['perito-share', 'task-share', 'time-share'],
                    default='perito-share',
                    help=("Métrica: 'perito-share' = % de peritos ≥ limiar; "
                          "'task-share' = % de tarefas produzidas por peritos ≥ limiar; "
                          "'time-share' = % do tempo trabalhado por peritos ≥ limiar."))

    # Exportações
    ap.add_argument('--export-md',      action='store_true', help='Exporta tabela em Markdown')
    ap.add_argument('--export-png',     action='store_true', help='Exporta gráfico em PNG')
    ap.add_argument('--export-org',     action='store_true', help='Exporta resumo em Org-mode (.org) com a imagem')
    ap.add_argument('--chart',          action='store_true', help='Gráfico ASCII no terminal')
    ap.add_argument('--export-comment', action='store_true', help='Exporta comentário GPT')
    ap.add_argument('--add-comments',   action='store_true', help='Gera comentário automaticamente (modo PDF)')

    return ap.parse_args()

# ────────────────────────────────────────────────────────────────────────────────
# Import parse_durations (compatível com os outros scripts)
# ────────────────────────────────────────────────────────────────────────────────
import importlib, importlib.util

def _import_composto_module():
    candidates = [
        "graphs_and_tables.compare_indicadores_composto",
        "compare_indicadores_composto",
        "/mnt/data/compare_indicadores_composto.py",
    ]
    for path in candidates:
        try:
            if path.endswith(".py") and os.path.exists(path):
                spec = importlib.util.spec_from_file_location("compare_indicadores_composto", path)
                if spec and spec.loader:
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)  # type: ignore
                    return mod
            else:
                return importlib.import_module(path)
        except Exception:
            pass
    return None

COMPOSTO = _import_composto_module()
PARSE_DURATIONS: Optional[Callable] = getattr(COMPOSTO, "parse_durations", None) if COMPOSTO else None

def _parse_durations_fallback(df: pd.DataFrame) -> pd.DataFrame:
    """Cria dur_s por fim−início; fallback HH:MM:SS/MM:SS; remove inválidos/≤0 e >3600s."""
    df = df.copy()
    ini = pd.to_datetime(df["ini"], errors="coerce")
    fim = pd.to_datetime(df["fim"], errors="coerce")
    dur = (fim - ini).dt.total_seconds()

    need_fb = dur.isna() | (dur <= 0)
    if "dur_txt" in df.columns and need_fb.any():
        raw = df.loc[need_fb, "dur_txt"].astype(str).str.strip()

        def parse_hms(s: str) -> float:
            if not s or s in ("0", "00:00", "00:00:00"):
                return float("nan")
            if ":" in s:
                parts = s.split(":")
                try:
                    if len(parts) == 3:
                        h, m, s2 = [int(x) for x in parts]
                        return float(h*3600 + m*60 + s2)
                    if len(parts) == 2:
                        m, s2 = [int(x) for x in parts]
                        return float(m*60 + s2)
                except Exception:
                    return float("nan")
                return float("nan")
            try:
                val = float(s)
                return val if val > 0 else float("nan")
            except Exception:
                return float("nan")

        dur_fb = raw.map(parse_hms)
        dur.loc[need_fb] = dur_fb

    df["dur_s"] = pd.to_numeric(dur, errors="coerce")
    df = df[(df["dur_s"].notna()) & (df["dur_s"] > 0) & (df["dur_s"] <= 3600)]
    return df

def _parse_durations(df: pd.DataFrame) -> pd.DataFrame:
    if PARSE_DURATIONS is not None:
        try:
            return PARSE_DURATIONS(df)
        except Exception:
            pass
    return _parse_durations_fallback(df)

# ────────────────────────────────────────────────────────────────────────────────
# DB helpers
# ────────────────────────────────────────────────────────────────────────────────
def _detect_tables(conn: sqlite3.Connection) -> Tuple[str, bool]:
    def has_table(name: str) -> bool:
        row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)).fetchone()
        return row is not None

    analises_tbl = None
    for t in ("analises", "analises_atestmed"):
        if has_table(t):
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({t})").fetchall()}
            if {"siapePerito", "dataHoraIniPericia"}.issubset(cols):
                analises_tbl = t
                break
    if not analises_tbl:
        raise RuntimeError("Não encontrei 'analises' nem 'analises_atestmed' com colunas mínimas.")
    indicadores_ok = has_table("indicadores")
    return analises_tbl, indicadores_ok

def _load_period_df(conn: sqlite3.Connection, tbl: str, start: str, end: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            p.nomePerito,
            a.protocolo,
            a.dataHoraIniPericia AS ini,
            a.dataHoraFimPericia AS fim,
            a.duracaoPericia     AS dur_txt
        FROM {tbl} a
        JOIN peritos p ON p.siapePerito = a.siapePerito
        WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
    """
    df = pd.read_sql_query(sql, conn, params=(start, end))
    df["nomePerito"] = df["nomePerito"].astype(str).str.strip()
    return _parse_durations(df)

def _top10_names(conn: sqlite3.Connection, tbl: str, start: str, end: str, min_analises: int) -> List[str]:
    sql = f"""
        SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
          FROM indicadores i
          JOIN peritos p  ON i.perito = p.siapePerito
          JOIN {tbl} a    ON a.siapePerito = i.perito
         WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
         GROUP BY p.nomePerito, i.scoreFinal
        HAVING total_analises >= ?
         ORDER BY i.scoreFinal DESC, total_analises DESC
         LIMIT 10
    """
    rows = conn.execute(sql, (start, end, min_analises)).fetchall()
    return [r[0] for r in rows]

# ────────────────────────────────────────────────────────────────────────────────
# Produtividade por perito e marcação ≥ threshold
# ────────────────────────────────────────────────────────────────────────────────
def _perito_productivity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Entrada: df com [nomePerito, protocolo, dur_s]
    Saída: por perito:
      [nomePerito, tasks_total, time_h, prod_h, meets]
      onde prod_h = tasks_total / time_h (0 se time_h<=0)
    """
    agg = df.groupby("nomePerito").agg(
        tasks_total=("protocolo", "count"),
        time_s=("dur_s", "sum"),
    ).reset_index()
    agg["time_h"] = agg["time_s"] / 3600.0
    agg["prod_h"] = agg.apply(lambda r: (r["tasks_total"] / r["time_h"]) if r["time_h"] > 0 else 0.0, axis=1)
    return agg

def _mark_meets(agg: pd.DataFrame, threshold: float) -> pd.DataFrame:
    agg = agg.copy()
    agg["meets"] = (agg["prod_h"] >= float(threshold)).astype(int)
    return agg

# ────────────────────────────────────────────────────────────────────────────────
# Agregação por grupo conforme modo
# ────────────────────────────────────────────────────────────────────────────────
def _aggregate_group(agg: pd.DataFrame, names: Optional[Set[str]], mode: str) -> Tuple[float, float, float]:
    """
    Retorna (num, den, pct) do grupo.
    - perito-share: num = #peritos meets, den = #peritos
    - task-share:   num = Σ tasks_total dos peritos meets, den = Σ tasks_total
    - time-share:   num = Σ time_s     dos peritos meets, den = Σ time_s
    """
    if names is not None:
        sub = agg[agg["nomePerito"].isin(names)]
    else:
        sub = agg

    if sub.empty:
        return 0.0, 0.0, 0.0

    if mode == "perito-share":
        num = float(sub["meets"].sum())
        den = float(sub.shape[0])
    elif mode == "task-share":
        num = float(sub.loc[sub["meets"] == 1, "tasks_total"].sum())
        den = float(sub["tasks_total"].sum())
    else:  # time-share
        num = float(sub.loc[sub["meets"] == 1, "time_s"].sum())
        den = float(sub["time_s"].sum())

    pct = (100.0 * num / den) if den > 0 else 0.0
    return num, den, pct

# ────────────────────────────────────────────────────────────────────────────────
# Helpers de UI/Export
# ────────────────────────────────────────────────────────────────────────────────
def _safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(name)).strip("_") or "output"

def _labels_for_mode(mode: str) -> Tuple[str, str, str]:
    if mode == "perito-share":
        return ("% de peritos ≥ limiar", "Peritos ≥ limiar (n)", "Peritos (total)")
    if mode == "task-share":
        return ("% de tarefas de peritos ≥ limiar", "Tarefas (peritos ≥ limiar)", "Tarefas (total)")
    return ("% do tempo de peritos ≥ limiar", "Tempo (s) peritos ≥ limiar", "Tempo (s) total")

def _title_for_mode(mode: str, threshold: float, scope: str) -> str:
    prefix = {
        "perito-share": "% de peritos ≥",
        "task-share":   "% de tarefas de peritos ≥",
        "time-share":   "% do tempo de peritos ≥",
    }[mode]
    return f"Produtividade — {prefix} {threshold}/h ({scope})"

def _render_png(title: str, y_label: str,
                left_label: str, right_label: str,
                left_pct: float, right_pct: float,
                left_num: float, left_den: float, right_num: float, right_den: float,
                mode: str, outfile: str) -> str:
    colors = ["#1f77b4", "#ff7f0e"]
    fig, ax = plt.subplots(figsize=(10, 6), dpi=400)
    x = [left_label, right_label]
    y = [left_pct, right_pct]
    bars = ax.bar(x, y, color=colors, edgecolor='black')

    ax.set_title(title, pad=15)
    ax.set_ylabel(y_label)
    ax.grid(axis='y', linestyle='--', alpha=0.6)

    ymax = max(10.0, min(100.0, max(y) * 1.15 if any(y) else 10.0))
    ax.set_ylim(0, ymax)

    for bar, pct, num, den in zip(bars, y, [left_num, right_num], [left_den, right_den]):
        if mode == "time-share":
            line2 = f"(n={num:.0f}s/{den:.0f}s)"
        else:
            line2 = f"(n={int(num)}/{int(den)})"
        txt = f"{pct:.1f}%\n{line2}"

        x0 = bar.get_x() + bar.get_width()/2
        off = ymax * 0.02
        if pct + off * 3 <= ymax:
            y0, va, color = pct + off, "bottom", "black"
        else:
            y0, va, color = max(pct - off * 1.5, off * 1.2), "top", "white"
        ax.text(x0, y0, txt, ha='center', va=va, fontsize=9, color=color)

    plt.tight_layout()
    fig.savefig(outfile, bbox_inches='tight')
    plt.close(fig)
    print("✅ PNG salvo em", outfile)
    return outfile

def _export_md(title: str, start: str, end: str,
               left_label: str, right_label: str, left_num: float, left_den: float, left_pct: float,
               right_num: float, right_den: float, right_pct: float,
               a_name: str, b_name: str, stem: str, mode: str) -> str:
    path = os.path.join(EXPORT_DIR, f"{stem}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# {title}\n\n")
        f.write(f"- Período: {start} a {end}\n")
        f.write(f"- Métrica: {mode}\n\n")
        f.write(f"| Categoria | {a_name} | {b_name} | % |\n")
        f.write(f"|-----------|------------------:|------------------:|---:|\n")
        if mode == "time-share":
            f.write(f"| {left_label}  | {left_num:.0f} | {left_den:.0f} | {left_pct:.1f}% |\n")
            f.write(f"| {right_label} | {right_num:.0f} | {right_den:.0f} | {right_pct:.1f}% |\n")
        else:
            f.write(f"| {left_label}  | {int(left_num)} | {int(left_den)} | {left_pct:.1f}% |\n")
            f.write(f"| {right_label} | {int(right_num)} | {int(right_den)} | {right_pct:.1f}% |\n")
    print("✅ Markdown salvo em", path)
    return path

def _export_org(title: str, start: str, end: str,
                left_label: str, right_label: str, left_num: float, left_den: float, left_pct: float,
                right_num: float, right_den: float, right_pct: float,
                a_name: str, b_name: str, png_path: str, out_name: str,
                top_names: Optional[List[str]] = None, mode: str = "") -> str:
    out = os.path.join(EXPORT_DIR, out_name)
    lines = []
    lines.append(f"* {title}")
    lines.append(":PROPERTIES:")
    lines.append(f":PERIODO: {start} a {end}")
    lines.append(f":METRICA: {mode}")
    if top_names:
        lines.append(f":TOP10: {', '.join(top_names)}")
    lines.append(":END:\n")

    lines.append(f"| Categoria | {a_name} | {b_name} | % |")
    lines.append("|-")
    if mode == "time-share":
        lines.append(f"| {left_label}  | {left_num:.0f} | {left_den:.0f} | {left_pct:.2f}% |")
        lines.append(f"| {right_label} | {right_num:.0f} | {right_den:.0f} | {right_pct:.2f}% |\n")
    else:
        lines.append(f"| {left_label}  | {int(left_num)} | {int(left_den)} | {left_pct:.2f}% |")
        lines.append(f"| {right_label} | {int(right_num)} | {int(right_den)} | {right_pct:.2f}% |\n")

    if png_path and os.path.exists(png_path):
        lines.append(f"#+CAPTION: {title}")
        lines.append(f"[[file:{os.path.basename(png_path)}]]\n")

    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("✅ Org salvo em", out)
    return out

def _render_ascii(title: str, y_label: str, left_label: str, right_label: str, left_pct: float, right_pct: float):
    if p is None:
        print("plotext não instalado; pulei o gráfico ASCII.")
        return
    p.clear_data()
    p.bar([left_label, right_label], [left_pct, right_pct])
    p.title(title)
    p.xlabel("")
    p.ylabel(y_label)
    p.plotsize(80, 18)
    p.show()

def _export_comment(md_table: str, ascii_chart: str, start: str, end: str, threshold: float, stem: str) -> str:
    comentario = comentar_produtividade(md_table, ascii_chart, start, end, threshold)
    path = os.path.join(EXPORT_DIR, f"{stem}_comment.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(comentario)
    print("🗒️ Comentário ChatGPT salvo em", path)
    return path

# ────────────────────────────────────────────────────────────────────────────────
# Execução por modo
# ────────────────────────────────────────────────────────────────────────────────
def run_perito(start: str, end: str, perito: str, threshold: float, mode: str,
               export_md: bool, export_png: bool, export_org: bool,
               chart: bool, export_comment: bool, add_comments: bool) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        tbl, _ = _detect_tables(conn)
        df = _load_period_df(conn, tbl, start, end)

    agg = _perito_productivity(df)
    agg = _mark_meets(agg, threshold)

    if perito not in set(agg["nomePerito"]):
        similares = agg[agg["nomePerito"].str.contains(perito, case=False, na=False)]["nomePerito"].unique().tolist()
        sugest = f" Peritos semelhantes: {', '.join(similares)}." if similares else ""
        raise ValueError(f"Perito '{perito}' não encontrado no período.{sugest}")

    left_set  = {perito}
    right_set = set(agg["nomePerito"]) - left_set

    left_num, left_den, left_pct   = _aggregate_group(agg, left_set,  mode)
    right_num, right_den, right_pct = _aggregate_group(agg, right_set, mode)

    y_label, a_name, b_name = _labels_for_mode(mode)
    title = _title_for_mode(mode, threshold, "Perito vs Demais")
    safe  = _safe(perito)
    stem  = f"produtividade_{mode}_{int(threshold)}h_{safe}"
    png   = os.path.join(EXPORT_DIR, f"{stem}.png")
    org   = f"{stem}.org"

    # tabela MD inline (também para comentário)
    if mode == "time-share":
        md_tbl = (
            f"| Categoria | {a_name} | {b_name} | % |\n"
            f"|-----------|------------------:|------------------:|---:|\n"
            f"| {perito}  | {left_num:.0f} | {left_den:.0f} | {left_pct:.1f}% |\n"
            f"| Demais    | {right_num:.0f} | {right_den:.0f} | {right_pct:.1f}% |\n"
        )
    else:
        md_tbl = (
            f"| Categoria | {a_name} | {b_name} | % |\n"
            f"|-----------|------------------:|------------------:|---:|\n"
            f"| {perito}  | {int(left_num)} | {int(left_den)} | {left_pct:.1f}% |\n"
            f"| Demais    | {int(right_num)} | {int(right_den)} | {right_pct:.1f}% |\n"
        )

    if export_md or export_comment or add_comments:
        _export_md(title, start, end, perito, "Demais",
                   left_num, left_den, left_pct,
                   right_num, right_den, right_pct,
                   a_name, b_name, stem, mode)

    if export_png:
        _render_png(title, y_label, perito, "Demais",
                    left_pct, right_pct, left_num, left_den, right_num, right_den,
                    mode, png)

    if export_org:
        if not os.path.exists(png):
            _render_png(title, y_label, perito, "Demais",
                        left_pct, right_pct, left_num, left_den, right_num, right_den,
                        mode, png)
        _export_org(title, start, end, perito, "Demais",
                    left_num, left_den, left_pct, right_num, right_den, right_pct,
                    a_name, b_name, png, org, mode=mode)

    if chart:
        _render_ascii(title, y_label, perito, "Demais", left_pct, right_pct)

    if export_comment or add_comments:
        chart_ascii = ""
        if p is not None:
            p.clear_data()
            p.bar([perito, "Demais"], [left_pct, right_pct])
            p.title(title)
            p.plotsize(80, 15)
            chart_ascii = p.build()
        _export_comment(md_tbl, chart_ascii, start, end, threshold, stem)

    # print log
    print(f"\n📊 {perito}: {left_pct:.1f}%  |  Demais: {right_pct:.1f}%  [{mode}, threshold={threshold}/h]")
    if mode == "time-share":
        print(f"   n={left_num:.0f}/{left_den:.0f} (esq.)  |  n={right_num:.0f}/{right_den:.0f} (dir.)\n")
    else:
        print(f"   n={int(left_num)}/{int(left_den)} (esq.)  |  n={int(right_num)}/{int(right_den)} (dir.)\n")

def run_top10(start: str, end: str, min_analises: int, threshold: float, mode: str,
              export_md: bool, export_png: bool, export_org: bool,
              chart: bool, export_comment: bool, add_comments: bool) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        tbl, has_ind = _detect_tables(conn)
        if not has_ind:
            raise RuntimeError("Tabela 'indicadores' não encontrada — calcule indicadores antes de usar --top10.")
        names = _top10_names(conn, tbl, start, end, min_analises)
        if not names:
            print("⚠️ Nenhum perito elegível para Top 10 nesse período.")
            return
        df = _load_period_df(conn, tbl, start, end)

    agg = _perito_productivity(df)
    agg = _mark_meets(agg, threshold)

    left_set  = set(names)
    right_set = set(agg["nomePerito"]) - left_set

    left_num, left_den, left_pct   = _aggregate_group(agg, left_set,  mode)
    right_num, right_den, right_pct = _aggregate_group(agg, right_set, mode)

    y_label, a_name, b_name = _labels_for_mode(mode)
    title = _title_for_mode(mode, threshold, "Top 10 vs Brasil (excl.)")
    stem  = f"produtividade_{mode}_{int(threshold)}h_top10"
    png   = os.path.join(EXPORT_DIR, f"{stem}.png")
    org   = f"{stem}.org"

    if mode == "time-share":
        md_tbl = (
            f"| Categoria | {a_name} | {b_name} | % |\n"
            f"|-----------|------------------:|------------------:|---:|\n"
            f"| Top 10 piores  | {left_num:.0f} | {left_den:.0f} | {left_pct:.1f}% |\n"
            f"| Brasil (excl.) | {right_num:.0f} | {right_den:.0f} | {right_pct:.1f}% |\n"
        )
    else:
        md_tbl = (
            f"| Categoria | {a_name} | {b_name} | % |\n"
            f"|-----------|------------------:|------------------:|---:|\n"
            f"| Top 10 piores  | {int(left_num)} | {int(left_den)} | {left_pct:.1f}% |\n"
            f"| Brasil (excl.) | {int(right_num)} | {int(right_den)} | {right_pct:.1f}% |\n"
        )

    if export_md or export_comment or add_comments:
        _export_md(title, start, end, "Top 10 piores", "Brasil (excl.)",
                   left_num, left_den, left_pct, right_num, right_den, right_pct,
                   a_name, b_name, stem, mode)

    if export_png:
        _render_png(title, y_label, "Top 10 piores", "Brasil (excl.)",
                    left_pct, right_pct, left_num, left_den, right_num, right_den,
                    mode, png)

    if export_org:
        if not os.path.exists(png):
            _render_png(title, y_label, "Top 10 piores", "Brasil (excl.)",
                        left_pct, right_pct, left_num, left_den, right_num, right_den,
                        mode, png)
        _export_org(title, start, end, "Top 10 piores", "Brasil (excl.)",
                    left_num, left_den, left_pct, right_num, right_den, right_pct,
                    a_name, b_name, png, org, top_names=names, mode=mode)

    if chart:
        _render_ascii(title, y_label, "Top 10 piores", "Brasil (excl.)", left_pct, right_pct)

    if export_comment or add_comments:
        chart_ascii = ""
        if p is not None:
            p.clear_data()
            p.bar(["Top 10 piores", "Brasil (excl.)"], [left_pct, right_pct])
            p.title(title)
            p.plotsize(80, 15)
            chart_ascii = p.build()
        _export_comment(md_tbl, chart_ascii, start, end, threshold, stem)

    # print log
    print(f"\n📊 Top 10: {left_pct:.1f}%  |  Brasil (excl.): {right_pct:.1f}%  [{mode}, threshold={threshold}/h]")
    if mode == "time-share":
        print(f"   n={left_num:.0f}/{left_den:.0f} (grupo)  |  n={right_num:.0f}/{right_den:.0f} (Brasil excl.)\n")
    else:
        print(f"   n={int(left_num)}/{int(left_den)} (grupo)  |  n={int(right_num)}/{int(right_den)} (Brasil excl.)\n")

# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    if args.top10:
        run_top10(args.start, args.end, args.min_analises, args.threshold, args.mode,
                  args.export_md, args.export_png, args.export_org,
                  args.chart, args.export_comment, args.add_comments)
    else:
        perito = args.perito or args.nome
        run_perito(args.start, args.end, perito, args.threshold, args.mode,
                   args.export_md, args.export_png, args.export_org,
                   args.chart, args.export_comment, args.add_comments)

if __name__ == '__main__':
    main()

