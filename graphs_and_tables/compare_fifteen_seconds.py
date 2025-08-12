#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Perícias ≤ THRESHOLD s — comparação em %
- Modo 1: --perito "NOME"  vs Brasil (excluindo esse perito)
- Modo 2: --top10 (10 piores por scoreFinal no período) vs Brasil (excluindo o grupo)

Pipeline alinhado ao compare_indicadores_composto:
1) Carrega dados do período e NORMALIZA durações via parse_durations do
   compare_indicadores_composto (se disponível). Fallback replica o mesmo:
   - dur_s por fim−início (preferência), fallback HH:MM:SS/MM:SS/numérico,
   - remove inválidos/≤0 e > 3600s (1h).

2) Cálculo pedido:
   - Denominador = total de protocolos do grupo no período (linhas após limpeza).
   - Numerador   = soma das tarefas ≤ threshold **apenas** dos peritos que,
     individualmente, tenham ≥ cut_n tarefas ≤ threshold no período.
   - Mesma regra para Brasil (excl.), excluindo o perito/grupo.

Exportações (iguais ao outro script):
    --export-png  (gráfico PNG)
    --export-org  (arquivo .org com :PROPERTIES:, tabela e imagem)
    --chart       (gráfico ASCII no terminal)
"""

from __future__ import annotations
import os
import sys
import sqlite3
import argparse
from typing import Tuple, List, Optional, Callable

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    import plotext as p  # opcional (--chart)
except Exception:
    p = None

import importlib
import importlib.util

try:
    import pandas as pd
except Exception as e:
    raise RuntimeError("Pandas é necessário para este script.") from e

# ────────────────────────────────────────────────────────────────────────────────
# Paths
# ────────────────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Import helpers (usa parse_durations do módulo que funcionou)
# ────────────────────────────────────────────────────────────────────────────────
def _import_composto_module():
    candidates = [
        "graphs_and_tables.compare_indicadores_composto",
        "compare_indicadores_composto",
    ]
    for modname in candidates:
        try:
            return importlib.import_module(modname)
        except Exception:
            pass
    fallback_path = "/mnt/data/compare_indicadores_composto.py"
    if os.path.exists(fallback_path):
        spec = importlib.util.spec_from_file_location("compare_indicadores_composto", fallback_path)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)  # type: ignore
            return mod
    return None

COMPOSTO = _import_composto_module()
PARSE_DURATIONS: Optional[Callable] = getattr(COMPOSTO, "parse_durations", None) if COMPOSTO else None

# ────────────────────────────────────────────────────────────────────────────────
# DB helpers
# ────────────────────────────────────────────────────────────────────────────────
def _detect_tables(conn: sqlite3.Connection) -> Tuple[str, bool]:
    def has_table(name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
        ).fetchone()
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
            a.protocolo,
            a.siapePerito,
            p.nomePerito,
            a.dataHoraIniPericia AS ini,
            a.dataHoraFimPericia AS fim,
            a.duracaoPericia     AS dur_txt
        FROM {tbl} a
        JOIN peritos p ON p.siapePerito = a.siapePerito
        WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
    """
    df = pd.read_sql_query(sql, conn, params=(start, end))
    df["nomePerito"] = df["nomePerito"].astype(str).str.strip()
    return df

# ────────────────────────────────────────────────────────────────────────────────
# Duração: fallback compatível
# ────────────────────────────────────────────────────────────────────────────────
def _parse_durations_fallback(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    ini = pd.to_datetime(df["ini"], errors="coerce")
    fim = pd.to_datetime(df["fim"], errors="coerce")
    dur = (fim - ini).dt.total_seconds()

    need_fb = dur.isna()
    if "dur_txt" in df.columns and need_fb.any():
        raw = df.loc[need_fb, "dur_txt"].astype(str).str.strip()

        def parse_hms(s: str) -> float:
            if not s or s in ("0", "00:00", "00:00:00"):
                return float("nan")
            if ":" in s:
                parts = s.split(":")
                if len(parts) == 3:
                    try:
                        h, m, s2 = [int(x) for x in parts]
                        return float(h*3600 + m*60 + s2)
                    except Exception:
                        return float("nan")
                if len(parts) == 2:
                    try:
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
# Top 10
# ────────────────────────────────────────────────────────────────────────────────
def _top10_names(conn: sqlite3.Connection, tbl: str,
                 start: str, end: str, min_analises: int) -> List[str]:
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
# Métrica com corte no numerador
# ────────────────────────────────────────────────────────────────────────────────
def _sum_tot_and_leq_with_perito_cut_df(
    df: pd.DataFrame,
    names: List[str],
    include: bool,
    threshold: int,
    cut_n: int,
) -> Tuple[int, int]:
    if names:
        names_up = {n.strip().upper() for n in names}
        mask = df["nomePerito"].str.upper().isin(names_up)
        mask = mask if include else ~mask
        sub = df.loc[mask]
    else:
        sub = df

    total = int(len(sub))
    if total == 0:
        return 0, 0

    leq_mask = sub["dur_s"] <= float(threshold)
    leq_by_perito = sub.loc[leq_mask].groupby("nomePerito", dropna=False)["protocolo"].size()
    elegiveis = set(leq_by_perito[leq_by_perito >= int(cut_n)].index)

    if not elegiveis:
        return total, 0

    leq_final = int(sub.loc[leq_mask & sub["nomePerito"].isin(elegiveis)].shape[0])
    return total, leq_final

# ────────────────────────────────────────────────────────────────────────────────
# Gráficos / Export (iguais em opções ao outro script)
# ────────────────────────────────────────────────────────────────────────────────
def _pct(n: int, d: int) -> float:
    return (n / d * 100.0) if d > 0 else 0.0

def _safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name).strip("_") or "output"

def render_png(title: str, left_label: str, right_label: str,
               left_pct: float, right_pct: float,
               left_leq: int, right_leq: int,
               left_tot: int, right_tot: int,
               threshold: int, cut_n: int,
               outfile: str) -> str:
    fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
    x = [left_label, right_label]
    y = [left_pct, right_pct]
    colors = ["#1f77b4", "#ff7f0e"]
    bars = ax.bar(x, y, color=colors, edgecolor='black')

    ax.set_title(title, pad=14)
    ax.set_ylabel("% de perícias ≤ {}s".format(threshold))
    ymax = max(100.0, max(y) * 1.15 if any(y) else 10.0)
    ax.set_ylim(0, ymax)
    ax.grid(axis='y', linestyle='--', alpha=0.5)

    for bar, pct, leq, tot in zip(bars, y, [left_leq, right_leq], [left_tot, right_tot]):
        ax.text(bar.get_x() + bar.get_width()/2,
                pct + ymax*0.01,
                f"{pct:.1f}% ({leq}/{tot})",
                ha='center', va='bottom', fontsize=10)

    ax.text(0.98, 0.98,
            f"Threshold: ≤ {threshold}s\nCorte (por perito): ≥ {cut_n} tarefas",
            transform=ax.transAxes, ha='right', va='top',
            fontsize=10, bbox=dict(facecolor='white', alpha=0.92, edgecolor='#999'))

    plt.tight_layout()
    out = os.path.abspath(outfile)
    fig.savefig(out, bbox_inches='tight')
    plt.close(fig)
    print(f"✅ PNG salvo em: {out}")
    return out

def render_ascii(left_label: str, right_label: str,
                 left_pct: float, right_pct: float,
                 threshold: int, cut_n: int, title: str) -> None:
    if p is None:
        print("plotext não instalado; pulei o gráfico ASCII.")
        return
    p.clear_data()
    p.bar([left_label, right_label], [left_pct, right_pct])
    p.title(title)
    p.xlabel("")
    p.ylabel(f"% ≤ {threshold}s (corte por perito ≥ {cut_n})")
    p.plotsize(90, 20)
    p.show()

def export_org(path_png: Optional[str],
               start: str, end: str,
               grp_title: str,
               left_tot: int, left_leq: int, left_pct: float,
               right_tot: int, right_leq: int, right_pct: float,
               threshold: int, cut_n: int,
               out_name: str) -> str:
    """
    Estilo replicado do compare_indicadores_composto:
    - bloco :PROPERTIES:
    - tabela com valores
    - imagem com #+CAPTION
    """
    out_path = os.path.join(EXPORT_DIR, out_name)
    lines = []
    lines.append(f"* Perícias ≤ {threshold}s – {grp_title} vs Brasil (excl.)")
    lines.append(":PROPERTIES:")
    lines.append(f":PERIODO: {start} a {end}")
    lines.append(f":THRESHOLD: {threshold}s")
    lines.append(f":CUT_N: {cut_n}")
    lines.append(":END:\n")

    lines.append("| Grupo | ≤{0}s | Total | % |".format(threshold))
    lines.append("|-")
    lines.append(f"| {grp_title} | {left_leq} | {left_tot} | {left_pct:.2f}% |")
    lines.append(f"| Brasil (excl.) | {right_leq} | {right_tot} | {right_pct:.2f}% |\n")

    if path_png and os.path.exists(path_png):
        lines.append("#+CAPTION: Comparação do % de perícias ≤ {0}s (com corte por perito).".format(threshold))
        lines.append(f"[[file:{os.path.basename(path_png)}]]\n")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"✅ Org salvo em: {out_path}")
    return out_path

# ────────────────────────────────────────────────────────────────────────────────
# Execução
# ────────────────────────────────────────────────────────────────────────────────
def run_perito(start: str, end: str, perito: str,
               threshold: int, cut_n: int,
               export_png_flag: bool, export_org_flag: bool, chart: bool) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        tbl, _ = _detect_tables(conn)
        df_raw = _load_period_df(conn, tbl, start, end)
        df = _parse_durations(df_raw)

        left_tot, left_leq   = _sum_tot_and_leq_with_perito_cut_df(df, [perito], True, threshold, cut_n)
        right_tot, right_leq = _sum_tot_and_leq_with_perito_cut_df(df, [perito], False, threshold, cut_n)

    left_pct  = _pct(left_leq, left_tot)
    right_pct = _pct(right_leq, right_tot)

    left_label  = perito
    right_label = "Brasil (excl.)"
    title = f"Perícias ≤ {threshold}s – {perito} vs Brasil (excl.)"
    safe = _safe(perito)

    print(f"\n📊 {title}")
    print(f"  {left_label}:  {left_leq}/{left_tot}  ({left_pct:.1f}%)")
    print(f"  {right_label}: {right_leq}/{right_tot}  ({right_pct:.1f}%)")

    png_path = os.path.join(EXPORT_DIR, f"compare_{threshold}s_{safe}.png")
    org_name = f"compare_{threshold}s_{safe}.org"

    if export_png_flag:
        png_path = render_png(title, left_label, right_label,
                              left_pct, right_pct, left_leq, right_leq, left_tot, right_tot,
                              threshold, cut_n, png_path)

    if export_org_flag:
        if not (png_path and os.path.exists(png_path)):
            png_path = render_png(title, left_label, right_label,
                                  left_pct, right_pct, left_leq, right_leq, left_tot, right_tot,
                                  threshold, cut_n, png_path)
        export_org(png_path, start, end, left_label,
                   left_tot, left_leq, left_pct,
                   right_tot, right_leq, right_pct,
                   threshold, cut_n, org_name)

    if chart:
        render_ascii(left_label, right_label, left_pct, right_pct, threshold, cut_n, title)

def run_top10(start: str, end: str, min_analises: int,
              threshold: int, cut_n: int,
              export_png_flag: bool, export_org_flag: bool, chart: bool) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        tbl, has_ind = _detect_tables(conn)
        if not has_ind:
            raise RuntimeError("Tabela 'indicadores' não encontrada — calcule indicadores antes de usar --top10.")
        names = _top10_names(conn, tbl, start, end, min_analises)
        if not names:
            print("⚠️ Nenhum perito elegível para Top 10 nesse período.")
            return

        df_raw = _load_period_df(conn, tbl, start, end)
        df = _parse_durations(df_raw)

        left_tot, left_leq   = _sum_tot_and_leq_with_perito_cut_df(df, names, True, threshold, cut_n)
        right_tot, right_leq = _sum_tot_and_leq_with_perito_cut_df(df, names, False, threshold, cut_n)

    left_pct  = _pct(left_leq, left_tot)
    right_pct = _pct(right_leq, right_tot)

    left_label  = "Top 10 piores"
    right_label = "Brasil (excl.)"
    title = f"Perícias ≤ {threshold}s – Top 10 piores vs Brasil (excl.)"

    print(f"\n📊 {title}")
    print(f"  Grupo: {left_leq}/{left_tot}  ({left_pct:.1f}%)  | peritos: {', '.join(names)}")
    print(f"  {right_label}: {right_leq}/{right_tot}  ({right_pct:.1f}%)")

    png_path = os.path.join(EXPORT_DIR, f"compare_{threshold}s_top10.png")
    org_name = f"compare_{threshold}s_top10.org"

    if export_png_flag:
        png_path = render_png(title, left_label, right_label,
                              left_pct, right_pct, left_leq, right_leq, left_tot, right_tot,
                              threshold, cut_n, png_path)

    if export_org_flag:
        if not (png_path and os.path.exists(png_path)):
            png_path = render_png(title, left_label, right_label,
                                  left_pct, right_pct, left_leq, right_leq, left_tot, right_tot,
                                  threshold, cut_n, png_path)
        export_org(png_path, start, end, left_label,
                   left_tot, left_leq, left_pct,
                   right_tot, right_leq, right_pct,
                   threshold, cut_n, org_name)

    if chart:
        render_ascii(left_label, right_label, left_pct, right_pct, threshold, cut_n, title)

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Perícias ≤ THRESHOLD s — comparação em % (perito ou Top 10 piores) vs Brasil (excl.)"
    )
    ap.add_argument('--start',     required=True)
    ap.add_argument('--end',       required=True)

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--perito', help='Nome do perito (exato)')
    g.add_argument('--top10',  action='store_true', help='Comparar o grupo dos 10 piores por scoreFinal')

    ap.add_argument('--min-analises', type=int, default=50, help='Elegibilidade p/ Top 10 (mínimo no período)')
    ap.add_argument('--threshold', '-t', type=int, default=15, help='Limite em segundos para considerar “≤ threshold” (padrão: 15)')
    ap.add_argument('--cut-n',      type=int, default=10, help='Corte: mínimo de tarefas ≤ threshold por perito para entrar no numerador (padrão: 10)')

    # >>> Exportações (iguais ao outro script) <<<
    ap.add_argument('--export-png', action='store_true')
    ap.add_argument('--export-org', action='store_true')
    ap.add_argument('--chart',      action='store_true', help='Gráfico ASCII no terminal')

    return ap.parse_args()

def main() -> None:
    args = parse_args()
    if args.top10:
        run_top10(args.start, args.end, args.min_analises,
                  args.threshold, args.cut_n,
                  args.export_png, args.export_org, args.chart)
    else:
        run_perito(args.start, args.end, args.perito,
                   args.threshold, args.cut_n,
                   args.export_png, args.export_org, args.chart)

if __name__ == "__main__":
    main()

