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

Exportações:
    --export-png           (gráfico PNG)
    --export-org           (arquivo .org com :PROPERTIES:, tabela e imagem)
    --export-comment       (arquivo *_comment.org com comentário automático)
    --export-comment-org   (incorpora o comentário no .org principal)
    --call-api             (liga a chamada da API p/ gerar o comentário via utils/comentarios/OpenAI)
    --chart                (gráfico ASCII no terminal)
"""

from __future__ import annotations
import os
import sys
import re
import sqlite3
import argparse
from typing import Tuple, List, Optional, Callable, Dict, Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    import plotext as p  # opcional (--chart) e p/ ASCII no comentário
except Exception:
    p = None

def _px_build() -> str:
    """Compat: retorna o buffer ASCII do plotext se disponível."""
    if p is None:
        return ""
    b = getattr(p, "build", None)
    try:
        return b() if callable(b) else ""
    except Exception:
        return ""

import importlib
import importlib.util

try:
    import pandas as pd
except Exception as e:
    raise RuntimeError("Pandas é necessário para este script.") from e

from pathlib import Path

# Preferência: usar utils/comentarios.comentar_le15s se existir
_COMENT_FUNCS: List[Callable[..., Any]] = []
try:
    # Assinatura moderna preferida:
    # comentar_le15s(md_table, ascii_chart, start, end, threshold, cut_n, *, call_api=True, model=..., ...)
    from utils.comentarios import comentar_le15s as _cf_le15s  # type: ignore
    _COMENT_FUNCS.append(_cf_le15s)
except Exception:
    pass
# (Sem problemas se não existir; o fallback local cobre.)

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
# Comentário (.org) — integra utils/comentarios (se existir) + fallback local
# ────────────────────────────────────────────────────────────────────────────────
def _md_table_leq(lhs_label: str, rhs_label: str,
                  lhs_leq: int, lhs_tot: int, lhs_pct: float,
                  rhs_leq: int, rhs_tot: int, rhs_pct: float,
                  threshold: int) -> str:
    return (
        f"| Grupo | ≤{threshold}s | Total | % |\n"
        f"|-------|-----------:|------:|---:|\n"
        f"| {lhs_label}  | {lhs_leq} | {lhs_tot} | {lhs_pct:.1f}% |\n"
        f"| {rhs_label} | {rhs_leq} | {rhs_tot} | {rhs_pct:.1f}% |\n"
    )

def _build_ascii_for_comment(lhs_label: str, rhs_label: str, lhs_pct: float, rhs_pct: float,
                             threshold: int, cut_n: int, title: str) -> str:
    if p is None:
        return ""
    try:
        p.clear_data()
        p.bar([lhs_label, rhs_label], [lhs_pct, rhs_pct])
        p.title(title)
        p.xlabel("")
        p.ylabel(f"% ≤ {threshold}s (corte ≥ {cut_n})")
        p.plotsize(80, 15)
        return _px_build()
    except Exception:
        return ""

def _strip_markers(text: str) -> str:
    if not text:
        return ""
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
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", " ", text)
    return re.sub(r"\s{2,}", " ", text).strip()

def _cap_words(text: str, max_words: int = 180) -> str:
    ws = text.split()
    return " ".join(ws[:max_words]).rstrip() + ("…" if len(ws) > max_words else "")

def _fallback_prompt_le15s(start: str, end: str,
                           lhs_label: str, lhs_leq: int, lhs_tot: int, lhs_pct: float,
                           rhs_label: str, rhs_leq: int, rhs_tot: int, rhs_pct: float,
                           threshold: int, cut_n: int) -> str:
    dpp = lhs_pct - rhs_pct
    alert_l = (lhs_tot < 50)
    alert_r = (rhs_tot < 50)
    alert_txt = []
    if alert_l: alert_txt.append(f"{lhs_label.lower()} com amostra reduzida")
    if alert_r: alert_txt.append(f"{rhs_label.lower()} com amostra reduzida")
    alerta = f" Atenção: {', '.join(alert_txt)}." if alert_txt else ""
    return (
        f"No período {start} a {end}, considerando o limiar de ≤{threshold}s e o corte por perito "
        f"(apenas profissionais com ≥{cut_n} tarefas ≤{threshold}s entram no numerador), "
        f"{lhs_label} registrou {lhs_pct:.1f}% ({lhs_leq}/{lhs_tot}), enquanto {rhs_label} apresentou "
        f"{rhs_pct:.1f}% ({rhs_leq}/{rhs_tot}), diferença de {abs(dpp):.1f} p.p. "
        f"Os percentuais refletem a participação relativa de tarefas muito curtas entre os elegíveis e podem "
        f"variar com o mix de casos e horários de pico; recomenda-se revisar a distribuição de durações e a "
        f"consistência de registros.{alerta}"
    )

def _generate_comment_text(md_table: str, ascii_chart: str, start: str, end: str,
                           threshold: int, cut_n: int, call_api: bool) -> str:
    """
    Tenta usar utils.comentarios.comentar_le15s em diferentes assinaturas; se não der, gera texto local.
    """
    for fn in _COMENT_FUNCS:
        # Assinatura preferida
        try:
            out = fn(md_table, ascii_chart, start, end, threshold, cut_n, call_api=call_api)  # type: ignore
            if isinstance(out, dict):
                text = (out.get("comment") or out.get("prompt") or "").strip()
            else:
                text = str(out or "").strip()
            if text:
                return _cap_words(_to_one_paragraph(_strip_markers(text)))
        except TypeError:
            # Assinatura antiga (por .org inteiro ou sem threshold/cut_n)
            try:
                out = fn(md_table, ascii_chart, start, end, call_api=call_api)  # type: ignore
                text = (out.get("comment") if isinstance(out, dict) else str(out)).strip()
                if text:
                    return _cap_words(_to_one_paragraph(_strip_markers(text)))
            except Exception:
                try:
                    out = fn(md_table, call_api=call_api)  # type: ignore
                    text = (out.get("comment") if isinstance(out, dict) else str(out)).strip()
                    if text:
                        return _cap_words(_to_one_paragraph(_strip_markers(text)))
                except Exception:
                    pass
        except Exception:
            continue

    # Fallback local (texto final, não apenas prompt)
    try:
        lines = [ln for ln in md_table.splitlines() if ln.strip().startswith("|")]
        def parse_row(s: str):
            c = [x.strip() for x in s.strip("|").split("|")]
            return c[0], int(float(c[1])), int(float(c[2])), float(c[3].replace("%","").strip())
        left_label, left_leq, left_tot, left_pct = parse_row(lines[2])
        right_label, right_leq, right_tot, right_pct = parse_row(lines[3])
        return _cap_words(_to_one_paragraph(_fallback_prompt_le15s(
            start, end, left_label, left_leq, left_tot, left_pct,
            right_label, right_leq, right_tot, right_pct,
            threshold, cut_n
        )))
    except Exception:
        # fallback mínimo
        return _cap_words(_to_one_paragraph(
            f"No período {start} a {end}, compara-se o % de perícias ≤{threshold}s com corte por perito (≥{cut_n})."
        ))

def _export_comment_org(title: str, start: str, end: str,
                        md_table: str, ascii_chart: str,
                        threshold: int, cut_n: int,
                        stem: str, call_api: bool) -> str:
    comment_text = _generate_comment_text(md_table, ascii_chart, start, end, threshold, cut_n, call_api)
    out = os.path.join(EXPORT_DIR, f"{stem}_comment.org")
    lines = []
    lines.append(f"* Comentário – {title}")
    lines.append(":PROPERTIES:")
    lines.append(f":PERIODO: {start} a {end}")
    lines.append(f":THRESHOLD: {threshold}s")
    lines.append(f":CUT_N: {cut_n}")
    lines.append(":END:\n")

    lines.append("** Tabela base")
    lines.append("#+BEGIN_EXAMPLE")
    lines.append(md_table.strip())
    lines.append("#+END_EXAMPLE\n")

    if ascii_chart and ascii_chart.strip():
        lines.append("** Gráfico ASCII (opcional)")
        lines.append("#+BEGIN_EXAMPLE")
        lines.append(ascii_chart.strip())
        lines.append("#+END_EXAMPLE\n")

    lines.append("** Texto")
    lines.append(comment_text.strip() or "(sem comentário)")

    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("🗒️ Comentário .org salvo em", out)
    return out

# ────────────────────────────────────────────────────────────────────────────────
# Execução
# ────────────────────────────────────────────────────────────────────────────────
def run_perito(start: str, end: str, perito: str,
               threshold: int, cut_n: int,
               export_png_flag: bool, export_org_flag: bool,
               export_comment_flag: bool, export_comment_org_flag: bool,
               call_api: bool, chart: bool) -> None:
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
    org_path = os.path.join(EXPORT_DIR, org_name)

    if export_png_flag:
        png_path = render_png(title, left_label, right_label,
                              left_pct, right_pct, left_leq, right_leq, left_tot, right_tot,
                              threshold, cut_n, png_path)

    # Se vamos comentar e/ou exportar .org, gere .org (precisamos da imagem linkada)
    if export_org_flag or export_comment_flag or export_comment_org_flag:
        if not (png_path and os.path.exists(png_path)):
            png_path = render_png(title, left_label, right_label,
                                  left_pct, right_pct, left_leq, right_leq, left_tot, right_tot,
                                  threshold, cut_n, png_path)
        org_path = export_org(png_path, start, end, left_label,
                              left_tot, left_leq, left_pct,
                              right_tot, right_leq, right_pct,
                              threshold, cut_n, org_name)

    # Preparar insumos de comentário (uma vez só)
    if export_comment_flag or export_comment_org_flag:
        md_tbl = _md_table_leq(left_label, right_label,
                               left_leq, left_tot, left_pct,
                               right_leq, right_tot, right_pct,
                               threshold)
        ascii_chart = _build_ascii_for_comment(left_label, right_label, left_pct, right_pct,
                                               threshold, cut_n, title)
        stem = f"compare_{threshold}s_{safe}"
        comment_text = _generate_comment_text(md_tbl, ascii_chart, start, end, threshold, cut_n, call_api)

        if export_comment_flag:
            _export_comment_org(title, start, end, md_tbl, ascii_chart, threshold, cut_n, stem,
                                call_api=call_api)

        if export_comment_org_flag:
            with open(org_path, "a", encoding="utf-8") as f:
                f.write("\n** Comentário\n")
                f.write(comment_text.strip() + "\n")
            print(f"✅ Comentário incorporado ao ORG: {org_path}")

    if chart:
        render_ascii(left_label, right_label, left_pct, right_pct, threshold, cut_n, title)

def run_top10(start: str, end: str, min_analises: int,
              threshold: int, cut_n: int,
              export_png_flag: bool, export_org_flag: bool,
              export_comment_flag: bool, export_comment_org_flag: bool,
              call_api: bool, chart: bool) -> None:
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
    org_path = os.path.join(EXPORT_DIR, org_name)

    if export_png_flag:
        png_path = render_png(title, left_label, right_label,
                              left_pct, right_pct, left_leq, right_leq, left_tot, right_tot,
                              threshold, cut_n, png_path)

    if export_org_flag or export_comment_flag or export_comment_org_flag:
        if not (png_path and os.path.exists(png_path)):
            png_path = render_png(title, left_label, right_label,
                                  left_pct, right_pct, left_leq, right_leq, left_tot, right_tot,
                                  threshold, cut_n, png_path)
        org_path = export_org(png_path, start, end, left_label,
                              left_tot, left_leq, left_pct,
                              right_tot, right_leq, right_pct,
                              threshold, cut_n, org_name)

    if export_comment_flag or export_comment_org_flag:
        md_tbl = _md_table_leq(left_label, right_label,
                               left_leq, left_tot, left_pct,
                               right_leq, right_tot, right_pct,
                               threshold)
        ascii_chart = _build_ascii_for_comment(left_label, right_label, left_pct, right_pct,
                                               threshold, cut_n, title)
        stem = f"compare_{threshold}s_top10"
        comment_text = _generate_comment_text(md_tbl, ascii_chart, start, end, threshold, cut_n, call_api)

        if export_comment_flag:
            _export_comment_org(title, start, end, md_tbl, ascii_chart, threshold, cut_n, stem,
                                call_api=call_api)

        if export_comment_org_flag:
            with open(org_path, "a", encoding="utf-8") as f:
                f.write("\n** Comentário\n")
                f.write(comment_text.strip() + "\n")
            print(f"✅ Comentário incorporado ao ORG: {org_path}")

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

    # Exportações/saídas
    ap.add_argument('--export-png',           action='store_true')
    ap.add_argument('--export-org',           action='store_true')
    ap.add_argument('--export-comment',       action='store_true', help='Gera *_comment.org com texto corrido (IA ou fallback local)')
    ap.add_argument('--export-comment-org',   action='store_true', help='Incorpora o comentário automaticamente no arquivo .org principal')
    ap.add_argument('--call-api',             action='store_true', help='Chama a API (utils.comentarios) para obter o texto final do comentário (requer OPENAI_API_KEY)')
    ap.add_argument('--chart',                action='store_true', help='Gráfico ASCII no terminal')

    return ap.parse_args()

def main() -> None:
    args = parse_args()

    # liga API automaticamente se existir OPENAI_API_KEY no ambiente
    call_api = bool(args.call_api or os.getenv("OPENAI_API_KEY"))

    if args.top10:
        run_top10(args.start, args.end, args.min_analises,
                  args.threshold, args.cut_n,
                  args.export_png, args.export_org,
                  args.export_comment, args.export_comment_org,
                  call_api, args.chart)
    else:
        run_perito(args.start, args.end, args.perito,
                   args.threshold, args.cut_n,
                   args.export_png, args.export_org,
                   args.export_comment, args.export_comment_org,
                   call_api, args.chart)

if __name__ == "__main__":
    main()

