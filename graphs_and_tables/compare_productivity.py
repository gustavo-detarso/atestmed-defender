#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sqlite3
import argparse
import re
import json
from typing import Optional, Callable, Tuple, List, Set, Dict, Any

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt

try:
    import plotext as p  # opcional para --chart / ASCII no comentário
except Exception:
    p = None

def _px_build():
    if p is None:
        return ""
    b = getattr(p, "build", None)
    return b() if callable(b) else ""

# Caminhos
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Args
# ────────────────────────────────────────────────────────────────────────────────
def parse_args():
    ap = argparse.ArgumentParser(description="Produtividade ≥ limiar/h (Perito ou Top 10) versus Brasil (excl.) — com suporte ao fluxo B (escopo)")
    ap.add_argument('--start',     required=True, help='Data inicial YYYY-MM-DD')
    ap.add_argument('--end',       required=True, help='Data final   YYYY-MM-DD')

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--perito', help='Nome do perito a destacar (exato)')
    g.add_argument('--nome',   help='Nome do perito a destacar (alias)')
    g.add_argument('--top10',  action='store_true', help='Comparar o Top 10 vs Brasil (excl.)')

    ap.add_argument('--min-analises', type=int, default=50,
                    help='Elegibilidade para Top 10 (mínimo de análises no período)')
    ap.add_argument('--threshold', '-t', type=int, default=50,
                    help='Limiar de produtividade (análises por hora)')

    # Métrica (ajuda sem sinais de porcentagem)
    ap.add_argument('--mode', choices=['perito-share', 'task-share', 'time-share'],
                    default='perito-share',
                    help=("Métrica: 'perito-share' = proporção de peritos acima do limiar; "
                          "'task-share' = proporção de tarefas produzidas por peritos acima do limiar; "
                          "'time-share' = proporção do tempo trabalhado por peritos acima do limiar."))

    # Fluxo A/B + integração com manifestos externos
    ap.add_argument('--fluxo', choices=['A', 'B'], default='B',
                    help="Seleção Top 10: A = direto por score; B = usa escopo (gate) informado via --scope-csv e, opcionalmente, --peritos-csv.")
    ap.add_argument('--peritos-csv', default=None,
                    help="(Opcional) CSV com coluna 'nomePerito' listando o grupo esquerdo (ex.: Top 10) — útil no fluxo B.")
    ap.add_argument('--scope-csv', default=None,
                    help="(Opcional) CSV com coluna 'nomePerito' definindo o ESCOPO da base (ex.: coorte do gate do fluxo B).")
    ap.add_argument('--save-manifests', action='store_true',
                    help="Salvar, em exports/, os CSVs com peritos usados e escopo aplicado neste run.")

    # Exportações
    ap.add_argument('--export-md',      action='store_true', help='Exporta tabela em Markdown')
    ap.add_argument('--export-png',     action='store_true', help='Exporta gráfico em PNG')
    ap.add_argument('--export-org',     action='store_true', help='Exporta resumo em Org-mode (.org) com a imagem e, se solicitado, o comentário')
    ap.add_argument('--chart',          action='store_true', help='Gráfico ASCII no terminal')

    # Comentários (agora inseridos no .org principal)
    ap.add_argument('--export-comment',     action='store_true', help='Gera comentário (alias para inserir comentário no .org)')
    ap.add_argument('--add-comments',       action='store_true', help='Sinônimo de --export-comment (compatibilidade)')
    ap.add_argument('--export-comment-org', action='store_true', help='Insere comentário interpretativo diretamente no .org')
    ap.add_argument('--call-api',           action='store_true', help='Usa OPENAI_API_KEY (.env) para gerar comentário via API')
    ap.add_argument('--debug-comments',     action='store_true', help='Exibe qual caminho foi usado (API vs fallback)')

    # Parâmetros de geração (API)
    ap.add_argument('--model',       default='gpt-4o-mini', help='Modelo OpenAI (padrão: gpt-4o-mini)')
    ap.add_argument('--max-words',   type=int, default=180, help='Máximo de palavras no comentário')
    ap.add_argument('--temperature', type=float, default=0.2, help='Temperatura da geração')

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
# Fluxo B – escopo/top10 via CSV
# ────────────────────────────────────────────────────────────────────────────────
def _load_names_from_csv(path: Optional[str]) -> Optional[Set[str]]:
    if not path:
        return None
    try:
        df = pd.read_csv(path)
        if "nomePerito" not in df.columns:
            return None
        return set(df["nomePerito"].astype(str).str.strip())
    except Exception:
        return None

def _apply_scope_df(df: pd.DataFrame, scope_csv: Optional[str]) -> pd.DataFrame:
    """Limita o DF às linhas do escopo, se fornecido (fluxo B)."""
    if not scope_csv:
        return df
    names = _load_names_from_csv(scope_csv)
    if not names:
        return df
    return df[df["nomePerito"].isin(names)].copy()

def _save_manifest_csv(names: List[str], path: str):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pd.DataFrame({"nomePerito": names}).to_csv(path, index=False, encoding="utf-8")
        print(f"🗂️  Manifest salvo: {path}")
    except Exception as e:
        print(f"[WARN] Falha salvando manifest {path}: {e}")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers de UI/Export
# ────────────────────────────────────────────────────────────────────────────────
def _safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(name)).strip("_") or "output"

def _labels_for_mode(mode: str) -> Tuple[str, str, str]:
    if mode == "perito-share":
        return ("Percentual de peritos acima do limiar", "Peritos acima do limiar (n)", "Peritos (total)")
    if mode == "task-share":
        return ("Percentual de tarefas de peritos acima do limiar", "Tarefas (peritos acima do limiar)", "Tarefas (total)")
    return ("Percentual do tempo de peritos acima do limiar", "Tempo (s) peritos acima do limiar", "Tempo (s) total")

def _title_for_mode(mode: str, threshold: float, scope: str) -> str:
    prefix = {
        "perito-share": "Percentual de peritos ≥",
        "task-share":   "Percentual de tarefas de peritos ≥",
        "time-share":   "Percentual do tempo de peritos ≥",
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
                top_names: Optional[List[str]] = None, mode: str = "",
                comment_text: Optional[str] = None) -> str:
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

    # Comentário inserido diretamente no .org
    if comment_text:
        lines.append("** Comentário")
        lines.append(comment_text.strip())
        lines.append("")

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

def _export_comment_sidecar(stem: str, comment_text: Optional[str]) -> Optional[str]:
    """Grava graphs_and_tables/exports/<stem>_comment.org com o parágrafo interpretativo."""
    if not comment_text:
        return None
    path = os.path.join(EXPORT_DIR, f"{stem}_comment.org")
    with open(path, "w", encoding="utf-8") as f:
        f.write(comment_text.strip() + "\n")
    print(f"📝 Comment(org) salvo em {path}")
    return path

def _build_parsed_from_values(mode: str,
                              a_name: str, b_name: str,
                              left_label: str, right_label: str,
                              left_num: float, left_den: float, left_pct: float,
                              right_num: float, right_den: float, right_pct: float) -> Dict[str, Any]:
    return {
        "a_label": a_name,
        "b_label": b_name,
        "mode": mode,
        "left":  {"label": left_label,  "num": float(left_num),  "den": float(left_den),  "pct": float(left_pct)},
        "right": {"label": right_label, "num": float(right_num), "den": float(right_den), "pct": float(right_pct)},
    }

# ────────────────────────────────────────────────────────────────────────────────
# Comentários (integra utils/comentarios + API direta + fallback) → texto p/ .org
# ────────────────────────────────────────────────────────────────────────────────

# utils.comentarios
_COMENT_FUNCS: List[Callable[..., Any]] = []
try:
    from utils.comentarios import comentar_produtividade as _cf1  # se existir, será utilizado
    _COMENT_FUNCS.append(_cf1)
except Exception:
    pass
try:
    from utils.comentarios import comentar_overlap as _cf2
    _COMENT_FUNCS.append(_cf2)
except Exception:
    pass

# .env + OpenAI
def _load_openai_key_from_dotenv(env_path: str) -> Optional[str]:
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
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    # SDK novo
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(model=model, messages=messages, temperature=temperature)
        txt = (resp.choices[0].message.content or "").strip()
        if txt:
            return txt
    except Exception:
        pass
    # SDK legado
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

# Sanitização
def _sanitize_org_text(text: str, max_words: int) -> str:
    if not text:
        return ""
    text = re.sub(r"^```.*?$", "", text, flags=re.M)
    text = re.sub(r"^~~~.*?$", "", text, flags=re.M)
    kept = []
    for ln in text.splitlines():
        t = ln.strip()
        if not t:
            continue
        if t.startswith("[") and t.endswith("]"):  # cabeçalhos
            continue
        if t.startswith("|"):   # tabelas
            continue
        if t.startswith("#+"):  # diretivas org
            continue
        kept.append(ln)
    text = " ".join(" ".join(kept).split())
    words = text.split()
    if len(words) > max_words:
        text = " ".join(words[:max_words]).rstrip() + "…"
    return text

# Prompts e geradores
def _build_messages_produtividade(start: str, end: str, threshold: float, mode: str,
                                  parsed: Dict[str, Any], ascii_chart: str, max_words: int) -> List[Dict[str, str]]:
    human_metric = {
        "perito-share": "proporção de peritos acima do limiar",
        "task-share":   "proporção de tarefas de peritos acima do limiar",
        "time-share":   "proporção do tempo de peritos acima do limiar"
    }.get(mode, "métrica")

    resumo = {
        "periodo": f"{start} a {end}",
        "limiar_h": float(threshold),
        "metrica": mode,
        "descricao_metrica": human_metric,
        "lhs": parsed["left"],
        "rhs": parsed["right"],
        "grafico_ascii": ascii_chart or ""
    }

    system = "Você é um analista de dados do ATESTMED. Escreva comentários claros, objetivos e tecnicamente corretos."
    user = (
        "Escreva um comentário interpretativo em português (Brasil) para acompanhar um gráfico de duas barras "
        f"sobre produtividade maior ou igual a {threshold}/h. Use TEXTO CORRIDO (um único parágrafo, sem títulos/listas/tabelas), "
        f"com no máximo {max_words} palavras. Inclua: (1) leitura direta da comparação; "
        "(2) diferença em pontos percentuais; (3) referência aos denominadores (n); "
        "evite jargões e conclusões causais. Dados resumidos (JSON):\n\n" + json.dumps(resumo, ensure_ascii=False)
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]

def _comment_from_values(start: str, end: str, threshold: float, mode: str,
                         a_name: str, b_name: str,
                         left_label: str, right_label: str,
                         left_num: float, left_den: float, left_pct: float,
                         right_num: float, right_den: float, right_pct: float,
                         ascii_chart: str,
                         *, call_api: bool, debug: bool,
                         model: str, max_words: int, temperature: float) -> str:
    """Gera o comentário usando diretamente os números já calculados (sem parse de Markdown)."""
    parsed = _build_parsed_from_values(mode, a_name, b_name,
                                       left_label, right_label,
                                       left_num, left_den, left_pct,
                                       right_num, right_den, right_pct)

    # 1) Tenta API (se habilitada)
    if call_api:
        try:
            msgs = _build_messages_produtividade(start, end, threshold, mode, parsed, ascii_chart, max_words)
            api_txt = _call_openai_chat(msgs, model=model, temperature=temperature)
            if api_txt:
                if debug:
                    print("ℹ️ comentários: usando API com valores diretos (sem parse).")
                return _sanitize_org_text(api_txt, max_words)
        except Exception:
            if debug:
                print("⚠️ comentários: falha API; usando heurística.")

    # 2) Fallback heurístico com os valores diretos
    if mode == "time-share":
        esq = f"{left_pct:.1f}% (n={left_num:.0f}/{left_den:.0f} s)"
        dir = f"{right_pct:.1f}% (n={right_num:.0f}/{right_den:.0f} s)"
    else:
        esq = f"{left_pct:.1f}% (n={int(left_num)}/{int(left_den)})"
        dir = f"{right_pct:.1f}% (n={int(right_num)}/{int(right_den)})"

    diff = left_pct - right_pct
    txt = (
        f"No período {start} a {end}, considerando o limiar de {threshold}/h, "
        f"{left_label} registrou {esq}, enquanto {right_label} apresentou {dir}. "
        f"A diferença é de {abs(diff):.1f} p.p., "
        f"{'acima' if diff > 0 else 'abaixo' if diff < 0 else 'em linha'} do comparativo. "
        "Os percentuais refletem a participação relativa dos profissionais que atingem o limiar e podem variar conforme o mix de casos e janelas de pico."
    )
    return _sanitize_org_text(txt, max_words)

# ────────────────────────────────────────────────────────────────────────────────
# Execução por modo
# ────────────────────────────────────────────────────────────────────────────────
def run_perito(start: str, end: str, perito: str, threshold: float, mode: str,
               export_md: bool, export_png: bool, export_org: bool,
               chart: bool, want_comment: bool,
               call_api: bool, debug_comments: bool,
               model: str, max_words: int, temperature: float,
               fluxo: str, scope_csv: Optional[str], save_manifests: bool) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        tbl, _ = _detect_tables(conn)
        df_raw = _load_period_df(conn, tbl, start, end)

    # Fluxo B (opcional): restringe universo ao escopo informado
    df = _apply_scope_df(df_raw, scope_csv)
    if scope_csv and len(df) != len(df_raw):
        print(f"ℹ️ Fluxo {fluxo}: escopo aplicado via --scope-csv ({len(df)} / {len(df_raw)} linhas).")

    agg = _perito_productivity(df)
    agg = _mark_meets(agg, threshold)

    if perito not in set(agg["nomePerito"]):
        similares = agg[agg["nomePerito"].str.contains(perito, case=False, na=False)]["nomePerito"].unique().tolist()
        sugest = f" Peritos semelhantes: {', '.join(similares)}." if similares else ""
        raise ValueError(f"Perito '{perito}' não encontrado no período (após escopo, se aplicado).{sugest}")

    left_set  = {perito}
    right_set = set(agg["nomePerito"]) - left_set

    left_num, left_den, left_pct    = _aggregate_group(agg, left_set,  mode)
    right_num, right_den, right_pct = _aggregate_group(agg, right_set, mode)

    y_label, a_name, b_name = _labels_for_mode(mode)
    scope_txt = "Perito vs Demais" + (" — escopo" if scope_csv else "")
    title = _title_for_mode(mode, threshold, scope_txt)
    safe  = _safe(perito)
    stem  = f"produtividade_{mode}_{int(threshold)}h_{safe}"
    if scope_csv:
        stem += "_escopo"
    png   = os.path.join(EXPORT_DIR, f"{stem}.png")
    org   = f"{stem}.org"

    # tabela MD (para .md e para o comentário)
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

    if export_md or want_comment:
        _export_md(title, start, end, perito, "Demais",
                   left_num, left_den, left_pct,
                   right_num, right_den, right_pct,
                   a_name, b_name, stem, mode)

    if export_png or export_org or want_comment:
        if not os.path.exists(png):
            _render_png(title, y_label, perito, "Demais",
                        left_pct, right_pct, left_num, left_den, right_num, right_den,
                        mode, png)

    if chart:
        _render_ascii(title, y_label, perito, "Demais", left_pct, right_pct)

    # Org principal (gera se pediu org OU se pediu comentário)
    if export_org or want_comment:
        ascii_chart = ""
        if p is not None:
            try:
                p.clear_data()
                p.bar([perito, "Demais"], [left_pct, right_pct])
                p.title(title)
                p.plotsize(80, 15)
                ascii_chart = _px_build()
            except Exception:
                ascii_chart = ""
        # gera texto do comentário com os VALORES (sem parse de tabela)
        comment_text = None
        if want_comment:
            _load_openai_key_from_dotenv(os.path.join(BASE_DIR, ".env"))
            comment_text = _comment_from_values(
                start, end, threshold, mode,
                a_name, b_name,
                left_label=perito, right_label="Demais",
                left_num=left_num, left_den=left_den, left_pct=left_pct,
                right_num=right_num, right_den=right_den, right_pct=right_pct,
                ascii_chart=ascii_chart,
                call_api=call_api and bool(os.getenv("OPENAI_API_KEY")),
                debug=debug_comments, model=model, max_words=max_words, temperature=temperature
            )

        _export_org(title, start, end, perito, "Demais",
                    left_num, left_den, left_pct, right_num, right_den, right_pct,
                    a_name, b_name, png, org, mode=mode, comment_text=comment_text)

    # Manifest (individual não salva peritos; mas salva escopo se pedido)
    if save_manifests and scope_csv:
        try:
            escopo = sorted(set(pd.read_csv(scope_csv)["nomePerito"].astype(str)))
            _save_manifest_csv(escopo, os.path.join(EXPORT_DIR, f"{stem}_scope.csv"))
        except Exception:
            pass

    # log
    print(f"\n📊 {perito}: {left_pct:.1f}%  |  Demais: {right_pct:.1f}%  [{mode}, threshold={threshold}/h, fluxo={fluxo}{', escopo' if scope_csv else ''}]")
    if mode == "time-share":
        print(f"   n={left_num:.0f}/{left_den:.0f} (esq.)  |  n={right_num:.0f}/{right_den:.0f} (dir.)\n")
    else:
        print(f"   n={int(left_num)}/{int(left_den)} (esq.)  |  n={int(right_num)}/{int(right_den)} (dir.)\n")


def run_top10(start: str, end: str, min_analises: int, threshold: float, mode: str,
              export_md: bool, export_png: bool, export_org: bool,
              chart: bool, want_comment: bool,
              call_api: bool, debug_comments: bool,
              model: str, max_words: int, temperature: float,
              fluxo: str, peritos_csv: Optional[str], scope_csv: Optional[str], save_manifests: bool) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        tbl, has_ind = _detect_tables(conn)
        if not has_ind and not peritos_csv:
            raise RuntimeError("Tabela 'indicadores' não encontrada — para --top10 sem --peritos-csv, é necessário 'indicadores'.")
        df_raw = _load_period_df(conn, tbl, start, end)

        # Seleção do lado esquerdo (Top 10)
        csv_names = _load_names_from_csv(peritos_csv)
        if csv_names:
            names = sorted(csv_names)
            print(f"ℹ️ Top10/Grupo fornecido por --peritos-csv ({len(names)} nomes).")
        else:
            # Fluxo A: Top10 direto por scoreFinal; Fluxo B sem CSV → cai no mesmo Top10 padrão
            names = _top10_names(conn, tbl, start, end, min_analises)
            print(f"ℹ️ Top10 via DB (fluxo {fluxo}): {len(names)} nomes.")

    # Fluxo B (opcional): restringe universo ao escopo informado
    df = _apply_scope_df(df_raw, scope_csv)
    if scope_csv and len(df) != len(df_raw):
        print(f"ℹ️ Fluxo {fluxo}: escopo aplicado via --scope-csv ({len(df)} / {len(df_raw)} linhas).")

    if not names:
        print("⚠️ Nenhum perito elegível para Top 10 nesse período.")
        return

    agg = _perito_productivity(df)
    agg = _mark_meets(agg, threshold)

    left_set  = set([n for n in names if n in set(agg['nomePerito'])])
    right_set = set(agg["nomePerito"]) - left_set

    left_num, left_den, left_pct    = _aggregate_group(agg, left_set,  mode)
    right_num, right_den, right_pct = _aggregate_group(agg, right_set, mode)

    y_label, a_name, b_name = _labels_for_mode(mode)
    scope_txt = "Top 10 vs Brasil (excl.)" + (" — escopo" if scope_csv else "")
    title = _title_for_mode(mode, threshold, scope_txt)
    stem  = f"produtividade_{mode}_{int(threshold)}h_top10"
    if scope_csv:
        stem += "_escopo"
    png   = os.path.join(EXPORT_DIR, f"{stem}.png")
    org   = f"{stem}.org"

    # tabela MD (para .md e documentação)
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

    if export_md or want_comment:
        _export_md(title, start, end, "Top 10 piores", "Brasil (excl.)",
                   left_num, left_den, left_pct, right_num, right_den, right_pct,
                   a_name, b_name, stem, mode)

    if export_png or export_org or want_comment:
        if not os.path.exists(png):
            _render_png(title, y_label, "Top 10 piores", "Brasil (excl.)",
                        left_pct, right_pct, left_num, left_den, right_num, right_den,
                        mode, png)

    if chart:
        _render_ascii(title, y_label, "Top 10 piores", "Brasil (excl.)", left_pct, right_pct)

    if export_org or want_comment:
        ascii_chart = ""
        if p is not None:
            try:
                p.clear_data()
                p.bar(["Top 10 piores", "Brasil (excl.)"], [left_pct, right_pct])
                p.title(title)
                p.plotsize(80, 15)
                ascii_chart = _px_build()
            except Exception:
                ascii_chart = ""

        # Comentário a partir dos VALORES (sem parse de tabela)
        comment_text = None
        if want_comment:
            _load_openai_key_from_dotenv(os.path.join(BASE_DIR, ".env"))
            comment_text = _comment_from_values(
                start, end, threshold, mode,
                a_name, b_name,
                left_label="Top 10 piores", right_label="Brasil (excl.)",
                left_num=left_num, left_den=left_den, left_pct=left_pct,
                right_num=right_num, right_den=right_den, right_pct=right_pct,
                ascii_chart=ascii_chart,
                call_api=call_api and bool(os.getenv("OPENAI_API_KEY")),
                debug=debug_comments, model=model, max_words=max_words, temperature=temperature
            )
            _export_comment_sidecar(stem, comment_text)

        _export_org(title, start, end, "Top 10 piores", "Brasil (excl.)",
                    left_num, left_den, left_pct, right_num, right_den, right_pct,
                    a_name, b_name, png, org, top_names=sorted(list(left_set)), mode=mode,
                    comment_text=comment_text)

    # Manifests (útil p/ pipeline do relatório)
    if save_manifests:
        if names:
            _save_manifest_csv(sorted(names), os.path.join(EXPORT_DIR, f"{stem}_peritos.csv"))
        if scope_csv:
            try:
                escopo = sorted(set(pd.read_csv(scope_csv)["nomePerito"].astype(str)))
                _save_manifest_csv(escopo, os.path.join(EXPORT_DIR, f"{stem}_scope.csv"))
            except Exception:
                pass

    print(f"\n📊 Top 10: {left_pct:.1f}%  |  Brasil (excl.): {right_pct:.1f}%  [{mode}, threshold={threshold}/h, fluxo={fluxo}{', escopo' if scope_csv else ''}]")
    if mode == "time-share":
        print(f"   n={left_num:.0f}/{left_den:.0f} (grupo)  |  n={right_num:.0f}/{right_den:.0f} (Brasil excl.)\n")
    else:
        print(f"   n={int(left_num)}/{int(left_den)} (grupo)  |  n={int(right_num)}/{int(right_den)} (Brasil excl.)\n")


# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    # normaliza aliases
    want_comment = args.export_comment or args.add_comments or args.export_comment_org

    if args.top10:
        run_top10(args.start, args.end, args.min_analises, args.threshold, args.mode,
                  args.export_md, args.export_png, args.export_org,
                  args.chart, want_comment,
                  call_api=args.call_api, debug_comments=args.debug_comments,
                  model=args.model, max_words=args.max_words, temperature=args.temperature,
                  fluxo=args.fluxo, peritos_csv=args.peritos_csv, scope_csv=args.scope_csv, save_manifests=args.save_manifests)
    else:
        perito = args.perito or args.nome
        run_perito(args.start, args.end, perito, args.threshold, args.mode,
                   args.export_md, args.export_png, args.export_org,
                   args.chart, want_comment,
                   call_api=args.call_api, debug_comments=args.debug_comments,
                   model=args.model, max_words=args.max_words, temperature=args.temperature,
                   fluxo=args.fluxo, scope_csv=args.scope_csv, save_manifests=args.save_manifests)

if __name__ == '__main__':
    main()

