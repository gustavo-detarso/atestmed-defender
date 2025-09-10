#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Perícias ≤ THRESHOLD s — comparação em %
- Modo 1: --perito "NOME"  vs Brasil (excluindo esse perito)
- Modo 2: --top10 (10 piores por scoreFinal no período) vs Brasil (excluindo o grupo)
- Modo 3: --peritos-csv <arquivo.csv>  (usa EXATAMENTE esses peritos como grupo)
         [opcional] --scope-csv <arquivo.csv>  (restringe o "Brasil (excl.)" ao escopo)
         [opcional] --fluxo A|B  (apenas para log/telemetria)

Regras de limpeza e métrica:
1) Durações normalizadas:
   - Preferência: fim−início (segundos).
   - Fallback: HH:MM:SS / MM:SS / numérico.
   - Exclui inválidos/≤0 e > 3600s (1h).

2) Métrica (com corte por perito no numerador):
   - Denominador = total de protocolos do grupo no período (linhas após limpeza).
   - Numerador   = soma das tarefas ≤ threshold **apenas** dos peritos que,
     individualmente, tenham ≥ cut_n tarefas ≤ threshold no período.
   - Mesma regra para Brasil (excl.), excluindo o perito/grupo.

Exportações:
    --export-png           (gráfico PNG)
    --export-org           (.org com :PROPERTIES:, tabela e imagem)
    --export-comment       (.org separado com comentário automático)
    --export-comment-org   (incorpora comentário no .org principal)
    --call-api             (tenta gerar comentário via OpenAI; senão heurístico)
    --chart                (gráfico ASCII no terminal)
"""

from __future__ import annotations

import os
import re
import sys
import json
import argparse
import sqlite3
from typing import Tuple, List, Optional, Callable, Dict, Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    import plotext as p  # opcional (--chart)
except Exception:
    p = None

try:
    import pandas as pd
except Exception as e:
    raise RuntimeError("Pandas é necessário para este script.") from e

import importlib
import importlib.util
from pathlib import Path

# ────────────────────────────────────────────────────────────────────────────────
# Paths
# ────────────────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Comentários (API externa ou heurística local)
# ────────────────────────────────────────────────────────────────────────────────
_COMENT_FUNCS: List[Callable[..., Any]] = []
try:
    # Assinatura moderna preferida:
    # comentar_le15s(md_table, ascii_chart, start, end, threshold, cut_n, *, call_api=True, model=..., ...)
    from utils.comentarios import comentar_le15s as _cf_le15s  # type: ignore
    _COMENT_FUNCS.append(_cf_le15s)
except Exception:
    pass

def _load_openai_key_from_dotenv(env_path: str) -> Optional[str]:
    """Carrega OPENAI_API_KEY do .env (python-dotenv se disponível; senão parse manual)."""
    if not os.path.exists(env_path):
        return os.getenv("OPENAI_API_KEY")
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(env_path, override=False)
        if os.getenv("OPENAI_API_KEY"):
            return os.getenv("OPENAI_API_KEY")
    except Exception:
        pass
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                if k.strip() == "OPENAI_API_KEY":
                    v = v.strip().strip('"').strip("'")
                    if v:
                        os.environ.setdefault("OPENAI_API_KEY", v)
                        return v
    except Exception:
        pass
    return os.getenv("OPENAI_API_KEY")

def _call_openai_chat(messages: List[Dict[str, str]], model: str, temperature: float) -> Optional[str]:
    """Compat com SDK novo (openai>=1.x) e legado."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(model=model, messages=messages, temperature=temperature)
        txt = (resp.choices[0].message.content or "").strip()
        if txt:
            return txt
    except Exception:
        pass
    try:
        import openai  # type: ignore
        openai.api_key = api_key
        resp = openai.ChatCompletion.create(model=model, messages=messages, temperature=temperature)
        txt = resp["choices"][0]["message"]["content"]
        if txt:
            return txt.strip()
    except Exception:
        pass
    return None

def _sanitize_paragraph(text: str, max_words: int = 180) -> str:
    """Remove cercas/headers/tabelas e limita nº de palavras para 1 parágrafo."""
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
    text = " ".join(" ".join(kept).split())
    words = text.split()
    if len(words) > max_words:
        text = " ".join(words[:max_words]).rstrip() + "…"
    return text

def _build_leq_messages(start: str, end: str,
                        lhs_label: str, rhs_label: str,
                        lhs_leq: int, lhs_tot: int, lhs_pct: float,
                        rhs_leq: int, rhs_tot: int, rhs_pct: float,
                        threshold: int, cut_n: int,
                        max_words: int) -> List[Dict[str, str]]:
    payload = {
        "periodo": {"inicio": start, "fim": end},
        "metric": "share_pericias_leq_threshold",
        "threshold_s": threshold,
        "cut_n_per_perito": cut_n,
        "lhs": {"label": lhs_label, "leq": lhs_leq, "tot": lhs_tot, "pct": round(float(lhs_pct), 2)},
        "rhs": {"label": rhs_label, "leq": rhs_leq, "tot": rhs_tot, "pct": round(float(rhs_pct), 2)},
        "diff_pp": round(float(lhs_pct - rhs_pct), 2)
    }
    system = "Você é um analista de dados do ATESTMED. Escreva comentários claros, objetivos e tecnicamente corretos."
    user = (
        "Escreva um único parágrafo (máx. {mw} palavras) interpretando a comparação do % de perícias ≤ threshold "
        "entre dois grupos, considerando que o numerador inclui apenas peritos com pelo menos 'cut_n' tarefas ≤ threshold. "
        "Inclua: (1) leitura direta; (2) diferença em pontos percentuais; (3) os denominadores e a regra do corte; "
        "(4) ressalva de análise descritiva sem causalidade. Dados em JSON:\n\n{json}"
    ).format(mw=max_words, json=json.dumps(payload, ensure_ascii=False))
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]

def _heuristic_comment_leq(start: str, end: str,
                           lhs_label: str, lhs_leq: int, lhs_tot: int, lhs_pct: float,
                           rhs_label: str, rhs_leq: int, rhs_tot: int, rhs_pct: float,
                           threshold: int, cut_n: int, max_words: int = 180) -> str:
    diff = lhs_pct - rhs_pct
    small_l = lhs_tot < 50
    small_r = rhs_tot < 50
    alerta = []
    if small_l: alerta.append(f"{lhs_label.lower()} com amostra reduzida")
    if small_r: alerta.append(f"{rhs_label.lower()} com amostra reduzida")
    alerta_txt = f" Atenção: {', '.join(alerta)}." if alerta else ""
    txt = (
        f"No período {start} a {end}, considerando o limiar de ≤{threshold}s e o corte por perito "
        f"(apenas profissionais com ≥{cut_n} tarefas ≤{threshold}s entram no numerador), "
        f"{lhs_label} registrou {lhs_pct:.1f}% ({lhs_leq}/{lhs_tot}), enquanto {rhs_label} apresentou "
        f"{rhs_pct:.1f}% ({rhs_leq}/{rhs_tot}), diferença de {abs(diff):.1f} p.p. "
        f"Os percentuais refletem a participação relativa de tarefas muito curtas entre os elegíveis e podem "
        f"variar com o mix de casos e horários de pico; resultados são descritivos e não implicam causalidade."
        f"{alerta_txt}"
    )
    return _sanitize_paragraph(txt, max_words=max_words)

def _comment_from_values_leq(start: str, end: str,
                             lhs_label: str, lhs_leq: int, lhs_tot: int, lhs_pct: float,
                             rhs_label: str, rhs_leq: int, rhs_tot: int, rhs_pct: float,
                             threshold: int, cut_n: int,
                             *, call_api: bool, model: str, max_words: int, temperature: float) -> str:
    """Gera comentário exclusivamente a partir dos VALORES (API direta → heurístico)."""
    if call_api:
        try:
            _load_openai_key_from_dotenv(os.path.join(BASE_DIR, ".env"))
            messages = _build_leq_messages(
                start, end, lhs_label, rhs_label,
                lhs_leq, lhs_tot, lhs_pct,
                rhs_leq, rhs_tot, rhs_pct,
                threshold, cut_n, max_words
            )
            api_txt = _call_openai_chat(messages, model=model, temperature=temperature)
            if api_txt:
                return _sanitize_paragraph(api_txt, max_words=max_words)
        except Exception:
            pass
    return _heuristic_comment_leq(
        start, end,
        lhs_label, lhs_leq, lhs_tot, lhs_pct,
        rhs_label, rhs_leq, rhs_tot, rhs_pct,
        threshold, cut_n, max_words=max_words
    )

# ────────────────────────────────────────────────────────────────────────────────
# Import de módulo para reaproveitar parse_durations (se existir)
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
    fallback_path = os.path.join(BASE_DIR, "graphs_and_tables", "compare_indicadores_composto.py")
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
# Duração
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
# Top 10 por score (Fluxo A)
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
# Render / Export
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
    Bloco Org:
      - :PROPERTIES:
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
    lines.append(f"| Brasil (excl.) | {right_leq} | {right_tot} | {right_pct:.2f}% |")
    lines.append("")

    if path_png and os.path.exists(path_png):
        base = os.path.basename(path_png)
        lines.append("#+ATTR_LATEX: :placement [H] :width \\linewidth")
        lines.append(f"#+CAPTION: Perícias ≤ {threshold}s — {grp_title} vs Brasil (excl.)")
        lines.append(f"[[file:{base}]]")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")

    print(f"✅ ORG salvo em: {out_path}")
    return out_path

def _write_comment_org(org_main_path: str, comment_text: str) -> str:
    stem = os.path.splitext(os.path.basename(org_main_path))[0]
    out = os.path.join(EXPORT_DIR, f"{stem}_comment.org")
    with open(out, "w", encoding="utf-8") as f:
        f.write("#+TITLE: Comentário — " + stem + "\n\n")
        f.write(_sanitize_paragraph(comment_text) + "\n")
    print(f"📝 Comentário salvo em: {out}")
    return out

def _append_comment_into_org(org_main_path: str, comment_text: str) -> None:
    try:
        with open(org_main_path, "a", encoding="utf-8") as f:
            f.write("\n#+BEGIN_QUOTE\n")
            f.write(_sanitize_paragraph(comment_text) + "\n")
            f.write("#+END_QUOTE\n")
        print("🧩 Comentário incorporado ao .org principal.")
    except Exception as e:
        print(f"[AVISO] Falha ao incorporar comentário no .org: {e}")

# ────────────────────────────────────────────────────────────────────────────────
# CSV helpers (Fluxo B / wrappers)
# ────────────────────────────────────────────────────────────────────────────────
def _load_names_from_csv(path: str) -> List[str]:
    import pandas as pd
    df = pd.read_csv(path)
    col = next((c for c in df.columns if c.lower() == "nomeperito"), None)
    if not col:
        raise RuntimeError("CSV sem coluna 'nomePerito'.")
    return df[col].astype(str).str.strip().tolist()

def _apply_scope(df: pd.DataFrame, scope_names: List[str] | None) -> pd.DataFrame:
    """Se scope_names vier, limita df aos peritos listados no escopo."""
    if not scope_names:
        return df
    scope_upper = {n.strip().upper() for n in scope_names}
    return df[df["nomePerito"].str.upper().isin(scope_upper)].copy()

# ────────────────────────────────────────────────────────────────────────────────
# Execuções
# ────────────────────────────────────────────────────────────────────────────────
def run_group_from_csv(start: str, end: str,
                       peritos_csv: str, scope_csv: Optional[str],
                       threshold: int, cut_n: int,
                       export_png_flag: bool, export_org_flag: bool,
                       export_comment_flag: bool, export_comment_org_flag: bool,
                       call_api: bool, chart: bool,
                       model: str, max_words: int, temperature: float,
                       fluxo: Optional[str] = None) -> None:
    names = _load_names_from_csv(peritos_csv)
    scope_names = _load_names_from_csv(scope_csv) if scope_csv and os.path.exists(scope_csv) else None

    with sqlite3.connect(DB_PATH) as conn:
        tbl, _ = _detect_tables(conn)
        df_raw = _load_period_df(conn, tbl, start, end)
    df = _parse_durations(df_raw)

    # Grupo (à esquerda)
    left_tot, left_leq = _sum_tot_and_leq_with_perito_cut_df(df, names, True, threshold, cut_n)

    # Brasil (excl.) — opcionalmente restringe ao ESCOPO
    df_right = _apply_scope(df, scope_names) if scope_names else df
    right_tot, right_leq = _sum_tot_and_leq_with_perito_cut_df(df_right, names, False, threshold, cut_n)

    left_pct  = _pct(left_leq, left_tot)
    right_pct = _pct(right_leq, right_tot)
    left_label, right_label = "Top 10 piores", "Brasil (excl.)"
    title = f"Perícias ≤ {threshold}s – Top 10 piores vs Brasil (excl.)"

    print(f"\n📊 {title}")
    print(f"  Grupo: {left_leq}/{left_tot}  ({left_pct:.1f}%)  | peritos: {', '.join(names)}")
    print(f"  {right_label}: {right_leq}/{right_tot}  ({right_pct:.1f}%)")
    if fluxo:
        print(f"  [fluxo={fluxo}] peritos_csv={os.path.basename(peritos_csv)} scope_csv={os.path.basename(scope_csv) if scope_csv else '-'}")

    png_path = None
    if chart:
        render_ascii(left_label, right_label, left_pct, right_pct, threshold, cut_n, title)
    if export_png_flag:
        png_path = os.path.join(EXPORT_DIR, f"compare_{threshold}s_top10.png")
        png_path = render_png(title, left_label, right_label,
                              left_pct, right_pct, left_leq, right_leq, left_tot, right_tot,
                              threshold, cut_n, png_path)
    org_path = None
    if export_org_flag:
        org_path = export_org(png_path, start, end, left_label,
                              left_tot, left_leq, left_pct,
                              right_tot, right_leq, right_pct,
                              threshold, cut_n, f"compare_{threshold}s_top10.org")

    # Comentário
    if export_comment_flag or export_comment_org_flag:
        comment = ""
        # 1) tenta função utilitária se existir
        for fn in _COMENT_FUNCS:
            try:
                ascii_buf = ""  # deixamos vazio; algumas impls aceitam
                md_tbl = f"| Grupo | ≤{threshold}s | Total | % |\n" \
                         f"|---|---:|---:|---:|\n" \
                         f"| {left_label} | {left_leq} | {left_tot} | {left_pct:.1f}% |\n" \
                         f"| {right_label} | {right_leq} | {right_tot} | {right_pct:.1f}% |\n"
                comment = fn(md_tbl, ascii_buf, start, end, threshold, cut_n, call_api=call_api)
                if comment:
                    comment = _sanitize_paragraph(comment)
                    break
            except Exception:
                pass
        # 2) fallback por valores
        if not comment:
            comment = _comment_from_values_leq(start, end,
                                               left_label, left_leq, left_tot, left_pct,
                                               right_label, right_leq, right_tot, right_pct,
                                               threshold, cut_n,
                                               call_api=call_api, model=model, max_words=max_words, temperature=temperature)
        if comment:
            if export_comment_flag and org_path:
                _write_comment_org(org_path, comment)
            if export_comment_org_flag and org_path:
                _append_comment_into_org(org_path, comment)

def run_perito(start: str, end: str, perito: str,
               threshold: int, cut_n: int,
               export_png_flag: bool, export_org_flag: bool,
               export_comment_flag: bool, export_comment_org_flag: bool,
               call_api: bool, chart: bool,
               model: str, max_words: int, temperature: float) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        tbl, _ = _detect_tables(conn)
        df_raw = _load_period_df(conn, tbl, start, end)
    df = _parse_durations(df_raw)

    names = [perito]
    left_tot, left_leq   = _sum_tot_and_leq_with_perito_cut_df(df, names, True,  threshold, cut_n)
    right_tot, right_leq = _sum_tot_and_leq_with_perito_cut_df(df, names, False, threshold, cut_n)

    left_pct  = _pct(left_leq, left_tot)
    right_pct = _pct(right_leq, right_tot)
    left_label, right_label = perito, "Brasil (excl.)"
    title = f"Perícias ≤ {threshold}s – {perito} vs Brasil (excl.)"

    print(f"\n📊 {title}")
    print(f"  {perito}: {left_leq}/{left_tot}  ({left_pct:.1f}%)")
    print(f"  {right_label}: {right_leq}/{right_tot}  ({right_pct:.1f}%)")

    png_path = None
    if chart:
        render_ascii(left_label, right_label, left_pct, right_pct, threshold, cut_n, title)
    if export_png_flag:
        safe = _safe(perito)
        png_path = os.path.join(EXPORT_DIR, f"compare_{threshold}s_{safe}.png")
        png_path = render_png(title, left_label, right_label,
                              left_pct, right_pct, left_leq, right_leq, left_tot, right_tot,
                              threshold, cut_n, png_path)
    org_path = None
    if export_org_flag:
        safe = _safe(perito)
        org_path = export_org(png_path, start, end, perito,
                              left_tot, left_leq, left_pct,
                              right_tot, right_leq, right_pct,
                              threshold, cut_n, f"compare_{threshold}s_{safe}.org")

    # Comentário
    if export_comment_flag or export_comment_org_flag:
        comment = _comment_from_values_leq(start, end,
                                           left_label, left_leq, left_tot, left_pct,
                                           right_label, right_leq, right_tot, right_pct,
                                           threshold, cut_n,
                                           call_api=call_api, model=model, max_words=max_words, temperature=temperature)
        if comment and org_path:
            if export_comment_flag:
                _write_comment_org(org_path, comment)
            if export_comment_org_flag:
                _append_comment_into_org(org_path, comment)

def run_top10(start: str, end: str, min_analises: int,
              threshold: int, cut_n: int,
              export_png_flag: bool, export_org_flag: bool,
              export_comment_flag: bool, export_comment_org_flag: bool,
              call_api: bool, chart: bool,
              model: str, max_words: int, temperature: float) -> None:
    """Top 10 por scoreFinal no período (Fluxo A, legado)."""
    with sqlite3.connect(DB_PATH) as conn:
        tbl, indicadores_ok = _detect_tables(conn)
        if not indicadores_ok:
            raise RuntimeError("Tabela 'indicadores' não encontrada para calcular Top 10 por score.")
        names = _top10_names(conn, tbl, start, end, min_analises)
        df_raw = _load_period_df(conn, tbl, start, end)
    df = _parse_durations(df_raw)

    left_tot, left_leq   = _sum_tot_and_leq_with_perito_cut_df(df, names, True,  threshold, cut_n)
    right_tot, right_leq = _sum_tot_and_leq_with_perito_cut_df(df, names, False, threshold, cut_n)

    left_pct  = _pct(left_leq, left_tot)
    right_pct = _pct(right_leq, right_tot)
    left_label, right_label = "Top 10 piores", "Brasil (excl.)"
    title = f"Perícias ≤ {threshold}s – Top 10 piores vs Brasil (excl.)"

    print(f"\n📊 {title}")
    print(f"  Grupo: {left_leq}/{left_tot}  ({left_pct:.1f}%)  | peritos: {', '.join(names)}")
    print(f"  {right_label}: {right_leq}/{right_tot}  ({right_pct:.1f}%)")

    png_path = None
    if chart:
        render_ascii(left_label, right_label, left_pct, right_pct, threshold, cut_n, title)
    if export_png_flag:
        png_path = os.path.join(EXPORT_DIR, f"compare_{threshold}s_top10.png")
        png_path = render_png(title, left_label, right_label,
                              left_pct, right_pct, left_leq, right_leq, left_tot, right_tot,
                              threshold, cut_n, png_path)
    org_path = None
    if export_org_flag:
        org_path = export_org(png_path, start, end, left_label,
                              left_tot, left_leq, left_pct,
                              right_tot, right_leq, right_pct,
                              threshold, cut_n, f"compare_{threshold}s_top10.org")

    if export_comment_flag or export_comment_org_flag:
        comment = _comment_from_values_leq(start, end,
                                           left_label, left_leq, left_tot, left_pct,
                                           right_label, right_leq, right_tot, right_pct,
                                           threshold, cut_n,
                                           call_api=call_api, model=model, max_words=max_words, temperature=temperature)
        if comment and org_path:
            if export_comment_flag:
                _write_comment_org(org_path, comment)
            if export_comment_org_flag:
                _append_comment_into_org(org_path, comment)

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
def parse_args():
    ap = argparse.ArgumentParser(description="Comparação do % de perícias ≤ limiar (com corte por perito no numerador).")
    ap.add_argument('--db', default=DB_PATH)
    ap.add_argument('--start', required=True)
    ap.add_argument('--end',   required=True)

    who = ap.add_mutually_exclusive_group(required=True)
    who.add_argument('--perito', help='Nome do perito para comparação individual.')
    who.add_argument('--top10', action='store_true', help='Top 10 piores por scoreFinal (Fluxo A, legado).')

    ap.add_argument('--min-analises', type=int, default=50, help='Mínimo de tarefas no período para elegibilidade do Top 10 por score.')
    ap.add_argument('--threshold', type=int, default=15, help='Limiar (segundos).')
    ap.add_argument('--cut-n', type=int, default=10, help='Corte por perito para entrar no numerador (tarefas ≤threshold).')

    # Novas flags (Fluxo B)
    ap.add_argument('--peritos-csv', default=None, help='CSV com coluna nomePerito; se informado, ignora --top10 interno.')
    ap.add_argument('--scope-csv', default=None, help='CSV com coluna nomePerito para definir o ESCOPO do Brasil (excl.).')
    ap.add_argument('--fluxo', choices=['A','B'], default=None, help='Apenas log/telemetria (não altera o cálculo).')

    # Export/comentários/visual
    ap.add_argument('--export-png', action='store_true')
    ap.add_argument('--export-org', action='store_true')
    ap.add_argument('--export-comment', action='store_true')
    ap.add_argument('--export-comment-org', action='store_true')
    ap.add_argument('--call-api', action='store_true', help='Se setado, tenta usar OPENAI_API_KEY para comentar.')
    ap.add_argument('--model', default='gpt-4o-mini', help='Modelo de chat para comentário (se --call-api).')
    ap.add_argument('--temperature', type=float, default=0.15)
    ap.add_argument('--max-words', type=int, default=180)
    ap.add_argument('--chart', action='store_true', help='Imprime gráfico ASCII (plotext).')

    return ap.parse_args()

# ────────────────────────────────────────────────────────────────────────────────
# main
# ────────────────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    # Permite override do DB por flag --db
    global DB_PATH
    if args.db and os.path.abspath(args.db) != os.path.abspath(DB_PATH):
        DB_PATH = args.db

    # Prioridade Fluxo B: se vier --peritos-csv, usa a lista EXATA
    if args.peritos_csv:
        return run_group_from_csv(
            args.start, args.end,
            args.peritos_csv, args.scope_csv,
            args.threshold, args.cut_n,
            args.export_png, args.export_org,
            args.export_comment, args.export_comment_org,
            args.call_api, args.chart,
            args.model, args.max_words, args.temperature,
            fluxo=args.fluxo
        )

    # Caso não tenha CSV: segue os modos clássicos
    if args.top10:
        return run_top10(
            args.start, args.end, args.min_analises,
            args.threshold, args.cut_n,
            args.export_png, args.export_org,
            args.export_comment, args.export_comment_org,
            args.call_api, args.chart,
            args.model, args.max_words, args.temperature
        )

    # Senão, é --perito
    if args.perito:
        return run_perito(
            args.start, args.end, args.perito,
            args.threshold, args.cut_n,
            args.export_png, args.export_org,
            args.export_comment, args.export_comment_org,
            args.call_api, args.chart,
            args.model, args.max_words, args.temperature
        )

    raise SystemExit("Nenhum modo selecionado. Use --perito, --top10 ou --peritos-csv.")

if __name__ == "__main__":
    main()

