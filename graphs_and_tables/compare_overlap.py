#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sqlite3
import argparse
import re
import json
from typing import Optional, List, Tuple, Dict
import pandas as pd

import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt

# ────────────────────────────────────────────────────────────────────────────────
# plotext (compat build/multi versões)
# ────────────────────────────────────────────────────────────────────────────────
try:
    import plotext as p  # opcional para --chart
except Exception:
    p = None

def _px_build():
    if p is None:
        return ""
    b = getattr(p, "build", None)
    return b() if callable(b) else ""

# Integração GPT (robusta: funciona mesmo que o módulo não exista)
try:
    # utils.comentarios.comentar_overlap(md_table, chart_ascii, start, end, *, call_api=False, model=None) -> str
    from utils.comentarios import comentar_overlap as _GPT_COMMENTER  # type: ignore
except Exception:
    _GPT_COMMENTER = None

# Caminhos
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Helpers OpenAI (.env + chamada)
# ────────────────────────────────────────────────────────────────────────────────
def _load_openai_key_from_dotenv(env_path: str) -> Optional[str]:
    """Carrega OPENAI_API_KEY do .env (python-dotenv se disponível; senão parse manual)."""
    if not os.path.exists(env_path):
        return os.getenv("OPENAI_API_KEY")
    # tenta dotenv
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(env_path, override=False)
        if os.getenv("OPENAI_API_KEY"):
            return os.getenv("OPENAI_API_KEY")
    except Exception:
        pass
    # parse manual simples
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
    """Compatível com SDK novo (openai>=1.x) e legado. Retorna texto ou None."""
    api_key = os.getenv("OPENAI_API_KEY")
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

def _sanitize_org_text(text: str, max_words: int = 180) -> str:
    """Remove cercas de código, cabeçalhos [..], tabelas e diretivas org; compacta para um parágrafo; limita palavras."""
    if not text:
        return ""
    # remove cercas
    text = re.sub(r"^```.*?$", "", text, flags=re.M)
    text = re.sub(r"^~~~.*?$", "", text, flags=re.M)
    # filtra linhas
    kept = []
    for ln in text.splitlines():
        t = ln.strip()
        if not t:
            continue
        if t.startswith("[") and t.endswith("]"):
            continue
        if t.startswith("|"):   # tabelas
            continue
        if t.startswith("#+"):  # diretivas org
            continue
        kept.append(ln)
    text = " ".join(" ".join(kept).split())
    # cap palavras
    words = text.split()
    if len(words) > max_words:
        text = " ".join(words[:max_words]).rstrip() + "…"
    return text

def _build_overlap_messages(start: str, end: str, mode: str,
                            left_label: str, right_label: str,
                            left_pct: float, right_pct: float,
                            left_num, left_den, right_num, right_den,
                            md_table: str, ascii_chart: str,
                            max_words: int) -> List[Dict[str, str]]:
    """Monta mensagens (system/user) para chat.completions."""
    human_metric = {
        "perito-share": "proporção de peritos com sobreposição",
        "task-share":   "proporção de tarefas sobrepostas",
        "time-share":   "proporção do tempo em sobreposição"
    }[mode]

    resumo = {
        "periodo": f"{start} a {end}",
        "metrica": mode,
        "descricao_metrica": human_metric,
        "lhs": {
            "label": left_label, "pct": round(float(left_pct), 2),
            "num": float(left_num) if mode == "time-share" else int(left_num),
            "den": float(left_den) if mode == "time-share" else int(left_den)
        },
        "rhs": {
            "label": right_label, "pct": round(float(right_pct), 2),
            "num": float(right_num) if mode == "time-share" else int(right_num),
            "den": float(right_den) if mode == "time-share" else int(right_den)
        },
        "tabela_markdown": md_table,
        "grafico_ascii": ascii_chart or ""
    }

    system = "Você é um analista de dados do ATESTMED. Escreva comentários claros, objetivos e tecnicamente corretos."
    user = (
        "Escreva um comentário interpretativo em português (Brasil) para acompanhar um gráfico de duas barras. "
        f"Use TEXTO CORRIDO (um único parágrafo, sem títulos, listas ou tabelas), com no máximo {max_words} palavras. "
        "Inclua: (1) leitura direta da comparação entre os dois grupos; "
        "(2) a diferença em pontos percentuais; "
        "(3) referência aos denominadores (n) quando fizer sentido; "
        "evite jargões e conclusões causais. "
        "Dados do gráfico (JSON):\n\n" + json.dumps(resumo, ensure_ascii=False)
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]

def _heuristic_overlap_comment(start: str, end: str, mode: str,
                               left_label: str, right_label: str,
                               left_pct: float, right_pct: float,
                               left_num, left_den, right_num, right_den) -> str:
    """Fallback em um parágrafo."""
    diff = float(left_pct) - float(right_pct)
    if mode == "time-share":
        esq = f"{left_pct:.1f}% (n={left_num:.0f}/{left_den:.0f} s)"
        dir = f"{right_pct:.1f}% (n={right_num:.0f}/{right_den:.0f} s)"
    else:
        esq = f"{left_pct:.1f}% (n={int(left_num)}/{int(left_den)})"
        dir = f"{right_pct:.1f}% (n={int(right_num)}/{int(right_den)})"
    return (
        f"No período {start} a {end}, {left_label} apresentou {esq}, enquanto {right_label} registrou {dir}. "
        f"A diferença é de {abs(diff):.1f} p.p., {'acima' if diff > 0 else 'abaixo' if diff < 0 else 'em linha'} do comparativo. "
        "Os percentuais representam a composição relativa dentro de cada grupo; não indicam, por si só, causa ou desempenho clínico."
    )

def _generate_comment_text_overlap(start: str, end: str, mode: str,
                                   left_label: str, right_label: str,
                                   left_pct: float, right_pct: float,
                                   left_num, left_den, right_num, right_den,
                                   md_table: str, ascii_chart: str,
                                   *, call_api: bool, model: str, max_words: int, temperature: float) -> str:
    """
    Tenta: (1) utils.comentarios; (2) API direta; (3) fallback heurístico.
    Retorna texto limpo para inserir no .org.
    """
    # (1) utils.comentarios
    if callable(_GPT_COMMENTER):
        try:
            bruto = _GPT_COMMENTER(md_table, ascii_chart, start, end, call_api=call_api, model=model)  # type: ignore
            if isinstance(bruto, dict):
                bruto = bruto.get("comment") or bruto.get("prompt") or ""
            if isinstance(bruto, str) and bruto.strip():
                return _sanitize_org_text(bruto, max_words=max_words)
        except TypeError:
            try:
                bruto = _GPT_COMMENTER(md_table=md_table, chart_ascii=ascii_chart, start=start, end=end, call_api=call_api, model=model)  # type: ignore
                if isinstance(bruto, dict):
                    bruto = bruto.get("comment") or bruto.get("prompt") or ""
                if isinstance(bruto, str) and bruto.strip():
                    return _sanitize_org_text(bruto, max_words=max_words)
            except Exception:
                pass
        except Exception:
            pass

    # (2) API direta
    if call_api:
        try:
            _load_openai_key_from_dotenv(os.path.join(BASE_DIR, ".env"))
            messages = _build_overlap_messages(
                start, end, mode,
                left_label, right_label,
                left_pct, right_pct,
                left_num, left_den, right_num, right_den,
                md_table, ascii_chart, max_words=max_words
            )
            api_txt = _call_openai_chat(messages, model=model, temperature=temperature)
            if api_txt:
                return _sanitize_org_text(api_txt, max_words=max_words)
        except Exception:
            pass

    # (3) Heurístico
    return _sanitize_org_text(
        _heuristic_overlap_comment(start, end, mode, left_label, right_label,
                                   left_pct, right_pct, left_num, left_den, right_num, right_den),
        max_words=max_words
    )

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
def parse_args():
    ap = argparse.ArgumentParser(description="Comparação de sobreposição (perito ou Top 10) com diferentes métricas")
    ap.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    ap.add_argument('--end',   required=True, help='Data final   YYYY-MM-DD')

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--perito', help='Nome do perito (exato)')
    g.add_argument('--nome',   help='Nome do perito (alias)')
    g.add_argument('--top10',  action='store_true', help='Comparar Top 10 piores (scoreFinal) vs Brasil (excl.)')

    ap.add_argument('--min-analises', type=int, default=50,
                    help='Elegibilidade para Top 10 (mínimo de análises no período)')

    ap.add_argument('--mode', choices=['perito-share', 'task-share', 'time-share'],
                    default='task-share',
                    help=("Métrica de comparação: "
                          "'perito-share' = proporção de peritos com overlap; "
                          "'task-share' = proporção de tarefas sobrepostas; "
                          "'time-share' = proporção do tempo total em sobreposição."))

    # Exportações
    ap.add_argument('--chart',              action='store_true', help='Exibe gráfico ASCII no terminal')
    ap.add_argument('--export-md',          action='store_true', help='Exporta tabela em Markdown')
    ap.add_argument('--export-png',         action='store_true', help='Exporta gráfico em PNG')
    ap.add_argument('--export-org',         action='store_true', help='Exporta resumo em Org-mode (.org) com a imagem e, se solicitado, o comentário')
    ap.add_argument('--export-comment',     action='store_true', help='Exporta comentário (Markdown, compatibilidade)')
    ap.add_argument('--export-comment-org', action='store_true', help='Insere comentário interpretativo diretamente no .org')
    ap.add_argument('--add-comments',       action='store_true', help='Sinônimo de --export-comment-org')

    # GPT
    ap.add_argument('--call-api',   action='store_true', help='Usa OPENAI_API_KEY para gerar comentário via utils.comentarios/ChatGPT')
    ap.add_argument('--model',      default='gpt-4o-mini', help='Modelo ChatGPT (padrão: gpt-4o-mini)')
    ap.add_argument('--max-words',  type=int, default=180, help='Tamanho máximo do comentário (palavras)')
    ap.add_argument('--temperature',type=float, default=0.2, help='Temperatura da geração')

    return ap.parse_args()

# ────────────────────────────────────────────────────────────────────────────────
# DB helpers
# ────────────────────────────────────────────────────────────────────────────────
def _detect_tables(conn: sqlite3.Connection) -> tuple[str, bool]:
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

def _load_period_intervals(conn: sqlite3.Connection, tbl: str, start: str, end: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            p.nomePerito,
            a.protocolo,
            a.dataHoraIniPericia AS ini,
            a.dataHoraFimPericia AS fim
        FROM {tbl} a
        JOIN peritos p ON p.siapePerito = a.siapePerito
        WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
    """
    df = pd.read_sql_query(sql, conn, params=(start, end))
    df["nomePerito"] = df["nomePerito"].astype(str).str.strip()
    # parse datas; remove linhas sem ini/fim válidos ou fim <= ini
    df["ini"] = pd.to_datetime(df["ini"], errors="coerce")
    df["fim"] = pd.to_datetime(df["fim"], errors="coerce")
    df = df[(df["ini"].notna()) & (df["fim"].notna()) & (df["fim"] > df["ini"])]
    # duração em segundos (útil para time-share)
    df["dur_s"] = (df["fim"] - df["ini"]).dt.total_seconds().astype(float)
    return df

def _top10_names(conn: sqlite3.Connection, tbl: str, start: str, end: str, min_analises: int) -> list[str]:
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
# Overlap por perito (tarefas e tempo)
# ────────────────────────────────────────────────────────────────────────────────
def _perito_overlap_stats(g: pd.DataFrame) -> dict:
    """
    Recebe um DataFrame de um perito (colunas: protocolo, ini, fim, dur_s)
    Retorna:
      {
        'has_overlap': bool,
        'tasks_total': int,
        'tasks_overlap': int,
        'time_total': float,        # soma das durações
        'time_overlap': float       # segundos com cobertura >= 2 (sweep-line)
      }
    """
    g = g.sort_values("ini")
    tasks_total = len(g)
    if tasks_total == 0:
        return dict(has_overlap=False, tasks_total=0, tasks_overlap=0, time_total=0.0, time_overlap=0.0)

    # Detectar tarefas sobrepostas (marca tarefa atual e todas ativas)
    overlapped_idxs = set()
    active = []  # lista de (end_time, index)
    for idx, row in g.reset_index(drop=True).iterrows():
        cur_start = row["ini"]
        cur_end   = row["fim"]
        # remove ativos que já terminaram
        active = [(e, i) for (e, i) in active if e > cur_start]
        if active:  # há sobreposição com alguém ativo
            overlapped_idxs.add(idx)
            for _, i in active:
                overlapped_idxs.add(i)
        # adiciona atual
        active.append((cur_end, idx))

    tasks_overlap = len(overlapped_idxs)
    has_overlap   = tasks_overlap > 0

    # Tempo total (soma simples das durações)
    time_total = float(g["dur_s"].sum())

    # Tempo em sobreposição (sweep-line: eventos +1/-1, soma trechos com k>=2)
    events = []
    for _, row in g.iterrows():
        events.append((row["ini"].to_datetime64(), +1))
        events.append((row["fim"].to_datetime64(), -1))
    events.sort(key=lambda x: x[0])

    time_overlap = 0.0
    k = 0
    prev_t = None
    for t, delta in events:
        t = pd.Timestamp(t)
        if prev_t is not None:
            dt = (t - prev_t).total_seconds()
            if k >= 2 and dt > 0:
                time_overlap += dt
        k += delta
        prev_t = t

    return dict(
        has_overlap=has_overlap,
        tasks_total=tasks_total,
        tasks_overlap=tasks_overlap,
        time_total=time_total,
        time_overlap=time_overlap
    )

def _compute_all_peritos_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa por perito e calcula estatísticas de overlap.
    Saída: DataFrame com uma linha por perito e colunas:
      [nomePerito, has_overlap, tasks_total, tasks_overlap, time_total, time_overlap]
    """
    stats = []
    for nome, g in df.groupby("nomePerito", sort=False):
        st = _perito_overlap_stats(g)
        st["nomePerito"] = nome
        stats.append(st)
    res = pd.DataFrame(stats, columns=["nomePerito","has_overlap","tasks_total","tasks_overlap","time_total","time_overlap"])
    return res

# ────────────────────────────────────────────────────────────────────────────────
# Agregações por grupo conforme modo
# ────────────────────────────────────────────────────────────────────────────────
def _aggregate_group(stats: pd.DataFrame, names_set: set[str] | None, mode: str) -> tuple[int|float, int|float, float, dict]:
    """
    stats: DF por perito com colunas de overlap.
    names_set: conjunto de peritos no grupo (ou None para 'todos').
    mode: 'perito-share' | 'task-share' | 'time-share'

    Retorna: (num, den, pct, detail_dict)
    """
    if names_set is not None:
        sub = stats[stats["nomePerito"].isin(names_set)]
    else:
        sub = stats

    if sub.empty:
        return 0, 0, 0.0, {"n_peritos": 0}

    if mode == "perito-share":
        num = int(sub["has_overlap"].sum())
        den = int(sub.shape[0])
        pct = (100.0 * num / den) if den > 0 else 0.0
        detail = {"n_peritos": den}
        return num, den, pct, detail

    if mode == "task-share":
        num = int(sub["tasks_overlap"].sum())
        den = int(sub["tasks_total"].sum())
        pct = (100.0 * num / den) if den > 0 else 0.0
        detail = {"n_peritos": int(sub.shape[0])}
        return num, den, pct, detail

    # time-share
    num = float(sub["time_overlap"].sum())
    den = float(sub["time_total"].sum())
    pct = (100.0 * num / den) if den > 0 else 0.0
    detail = {"n_peritos": int(sub.shape[0])}
    return num, den, pct, detail

# ────────────────────────────────────────────────────────────────────────────────
# Exportações (gráfico/markdown/org)
# ────────────────────────────────────────────────────────────────────────────────
def _unit_labels(mode: str) -> tuple[str, str]:
    if mode == "perito-share":
        return ("Com sobreposição (n peritos)", "Total de peritos")
    if mode == "task-share":
        return ("Tarefas sobrepostas (n)", "Total de tarefas")
    # time-share
    return ("Tempo sobreposto (s)", "Tempo total (s)")

def _yaxis_label(mode: str) -> str:
    if mode == "perito-share":
        return "Percentual de peritos com sobreposição"
    if mode == "task-share":
        return "Percentual de tarefas sobrepostas"
    return "Percentual do tempo em sobreposição"

def _render_png(title: str, left_label: str, right_label: str,
                left_pct: float, right_pct: float,
                left_num, left_den, right_num, right_den,
                mode: str, outfile: str) -> str:
    # cores padrão
    colors = ["#1f77b4", "#ff7f0e"]

    fig, ax = plt.subplots(figsize=(10, 6), dpi=400)
    cats = [left_label, right_label]
    vals = [left_pct, right_pct]
    bars = ax.bar(cats, vals, color=colors, edgecolor='black')
    ax.set_title(title, pad=15)
    ax.set_ylabel(_yaxis_label(mode))
    ax.grid(axis='y', linestyle='--', alpha=0.6)

    # evitar rótulo “vazar” do gráfico
    ymax = max(10.0, min(100.0, max(vals) * 1.15)) if any(vals) else 10.0
    ax.set_ylim(0, ymax)

    pairs = [(left_num, left_den), (right_num, right_den)]
    for bar, pct, (n, tot) in zip(bars, vals, pairs):
        if mode == "time-share":
            line2 = f"(n={n:.0f}s/{tot:.0f}s)"
        else:
            line2 = f"(n={int(n)}/{int(tot)})"
        txt = f"{pct:.1f}%\n{line2}"
        x = bar.get_x() + bar.get_width()/2
        off = ymax * 0.02
        if pct + off * 3 <= ymax:  # cabe em cima
            y, va, color = pct + off, "bottom", "black"
        else:                      # escreve dentro
            y, va, color = max(pct - off * 1.5, off * 1.2), "top", "white"
        ax.text(x, y, txt, ha='center', va=va, fontsize=9, color=color)

    plt.tight_layout()
    fig.savefig(outfile, bbox_inches='tight')
    plt.close(fig)
    print("✅ PNG salvo em", outfile)
    return outfile

def _export_md(title: str, start: str, end: str,
               left_label: str, right_label: str,
               left_num, left_den, left_pct: float,
               right_num, right_den, right_pct: float,
               mode: str, stem: str) -> str:
    a_label, b_label = _unit_labels(mode)
    path = os.path.join(EXPORT_DIR, f"{stem}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# {title}\n\n")
        f.write(f"- Período: {start} a {end}\n")
        f.write(f"- Métrica: {mode}\n\n")
        f.write("| Categoria | {0} | {1} | % |\n".format(a_label, b_label))
        f.write("|-----------|------------------:|------------------:|---:|\n")
        if mode == "time-share":
            f.write(f"| {left_label}  | {left_num:.0f} | {left_den:.0f} | {left_pct:.1f}% |\n")
            f.write(f"| {right_label} | {right_num:.0f} | {right_den:.0f} | {right_pct:.1f}% |\n")
        else:
            f.write(f"| {left_label}  | {int(left_num)} | {int(left_den)} | {left_pct:.1f}% |\n")
            f.write(f"| {right_label} | {int(right_num)} | {int(right_den)} | {right_pct:.1f}% |\n")
    print("✅ Markdown salvo em", path)
    return path

def _export_org(title: str, start: str, end: str,
                left_label: str, right_label: str,
                left_num, left_den, left_pct: float,
                right_num, right_den, right_pct: float,
                mode: str, png_path: str, out_name: str,
                top_names: list[str] | None = None,
                comment_text: Optional[str] = None) -> str:
    a_label, b_label = _unit_labels(mode)
    out = os.path.join(EXPORT_DIR, out_name)
    lines = []
    lines.append(f"* {title}")
    lines.append(":PROPERTIES:")
    lines.append(f":PERIODO: {start} a {end}")
    lines.append(f":METRICA: {mode}")
    if top_names:
        lines.append(f":TOP10: {', '.join(top_names)}")
    lines.append(":END:\n")

    lines.append(f"| Categoria | {a_label} | {b_label} | % |")
    lines.append("|-")
    if mode == "time-share":
        lines.append(f"| {left_label}  | {left_num:.0f} | {left_den:.0f} | {left_pct:.2f}% |")
        lines.append(f"| {right_label} | {right_num:.0f} | {right_den:.0f} | {right_pct:.2f}% |\n")
    else:
        lines.append(f"| {left_label}  | {int(left_num)} | {int(left_den)} | {left_pct:.2f}% |")
        lines.append(f"| {right_label} | {int(right_num)} | {int(right_den)} | {right_pct:.2f}% |\n")

    if png_path and os.path.exists(png_path):
        cap = {
            "perito-share": "Comparação do percentual de peritos com sobreposição.",
            "task-share":   "Comparação do percentual de tarefas sobrepostas.",
            "time-share":   "Comparação do percentual do tempo em sobreposição.",
        }[mode]
        lines.append(f"#+CAPTION: {cap}")
        lines.append(f"[[file:{os.path.basename(png_path)}]]\n")

    if comment_text:
        lines.append("** Comentário")
        lines.append(comment_text.strip())
        lines.append("")

    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("✅ Org salvo em", out)
    return out

def _render_ascii(title: str, left_label: str, right_label: str, left_pct: float, right_pct: float, mode: str) -> None:
    if p is None:
        print("plotext não instalado; pulei o gráfico ASCII.")
        return
    p.clear_data()
    p.bar([left_label, right_label], [left_pct, right_pct])
    p.title(title)
    p.xlabel("")
    p.ylabel(_yaxis_label(mode))
    p.plotsize(80, 18)
    p.show()

def _export_comment(md_table: str, ascii_chart: str, start: str, end: str, stem: str, *, call_api: bool) -> str:
    """
    Gera o comentário usando utils.comentarios.comentar_overlap, se disponível.
    Aceita retorno str (preferido). No fallback, salva a tabela + gráfico ASCII.
    """
    out_text = ""
    if callable(_GPT_COMMENTER):
        try:
            # assinatura moderna
            out_text = _GPT_COMMENTER(md_table, ascii_chart, start, end, call_api=call_api)
            if isinstance(out_text, dict):  # por segurança (versões antigas)
                out_text = out_text.get("comment") or out_text.get("prompt") or ""
        except TypeError:
            try:
                # compat: assinatura nomeada
                out_text = _GPT_COMMENTER(md_table=md_table, chart_ascii=ascii_chart, start=start, end=end, call_api=call_api)
                if isinstance(out_text, dict):
                    out_text = out_text.get("comment") or out_text.get("prompt") or ""
            except Exception as e:
                out_text = f"(falha ao gerar comentário automático: {e})\n\n{md_table}\n\n{ascii_chart}"
        except Exception as e:
            out_text = f"(falha ao gerar comentário automático: {e})\n\n{md_table}\n\n{ascii_chart}"
    else:
        out_text = (
            f"**Período:** {start} a {end}\n\n"
            f"_Observação:_ módulo de comentários não encontrado; abaixo segue a tabela-base.\n\n"
            f"{md_table}\n\n{ascii_chart}"
        )

    path = os.path.join(EXPORT_DIR, f"{stem}_comment.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write((out_text or "").strip() + "\n")
    print("🗒️ Comentário salvo em", path)
    return path

# ────────────────────────────────────────────────────────────────────────────────
# Execução
# ────────────────────────────────────────────────────────────────────────────────
def _safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(name)).strip("_") or "output"

def run_perito(start: str, end: str, perito: str, mode: str,
               export_md: bool, export_png: bool, export_org: bool,
               chart: bool, want_comment: bool, call_api: bool,
               model: str, max_words: int, temperature: float) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        tbl, _ = _detect_tables(conn)
        df = _load_period_intervals(conn, tbl, start, end)

    if df.empty:
        print("⚠️ Nenhuma análise válida no período.")
        return

    # Calcula stats por perito
    stats = _compute_all_peritos_stats(df)

    if perito not in set(stats["nomePerito"]):
        similares = stats[stats["nomePerito"].str.contains(perito, case=False, na=False)]["nomePerito"].unique().tolist()
        sugest = f" Peritos semelhantes: {', '.join(similares)}." if similares else ""
        raise ValueError(f"Perito '{perito}' não encontrado no período.{sugest}")

    # Esquerda: somente o perito; Direita: todos exceto o perito
    left_set  = {perito}
    right_set = set(stats["nomePerito"]) - left_set

    left_num, left_den, left_pct, _    = _aggregate_group(stats, left_set, mode)
    right_num, right_den, right_pct, _ = _aggregate_group(stats, right_set, mode)

    left_label, right_label = perito, "Brasil (excl.)"
    title = {
        "perito-share": f"Sobreposição — percentual de peritos com overlap — {perito} vs Brasil (excl.)",
        "task-share":   f"Sobreposição — percentual de tarefas sobrepostas — {perito} vs Brasil (excl.)",
        "time-share":   f"Sobreposição — percentual do tempo em overlap — {perito} vs Brasil (excl.)",
    }[mode]

    safe  = _safe(perito)
    stem  = f"sobreposicao_{mode}_{safe}"
    png   = os.path.join(EXPORT_DIR, f"{stem}.png")
    org   = f"{stem}.org"

    # MD (também para comentário .md)
    a_label, b_label = _unit_labels(mode)
    if mode == "time-share":
        md_tbl = (
            f"| Categoria | {a_label} | {b_label} | % |\n"
            f"|-----------|------------------:|------------------:|---:|\n"
            f"| {left_label}  | {left_num:.0f} | {left_den:.0f} | {left_pct:.1f}% |\n"
            f"| {right_label} | {right_num:.0f} | {right_den:.0f} | {right_pct:.1f}% |\n"
        )
    else:
        md_tbl = (
            f"| Categoria | {a_label} | {b_label} | % |\n"
            f"|-----------|------------------:|------------------:|---:|\n"
            f"| {left_label}  | {int(left_num)} | {int(left_den)} | {left_pct:.1f}% |\n"
            f"| {right_label} | {int(right_num)} | {int(right_den)} | {right_pct:.1f}% |\n"
        )
    if export_md or want_comment:
        _export_md(title, start, end, left_label, right_label,
                   left_num, left_den, left_pct, right_num, right_den, right_pct, mode, stem)

    if export_png or export_org or want_comment:
        if not os.path.exists(png):
            _render_png(title, left_label, right_label,
                        left_pct, right_pct, left_num, left_den, right_num, right_den, mode, png)

    if chart:
        _render_ascii(title, left_label, right_label, left_pct, right_pct, mode)

    # Gera comentário (se solicitado) e escreve .org principal
    comment_text: Optional[str] = None
    if want_comment:
        ascii_chart = ""
        if p is not None:
            try:
                p.clear_data()
                p.bar([left_label, right_label], [left_pct, right_pct])
                p.title(title)
                p.plotsize(80, 15)
                ascii_chart = _px_build()
            except Exception:
                ascii_chart = ""
        _load_openai_key_from_dotenv(os.path.join(BASE_DIR, ".env"))
        comment_text = _generate_comment_text_overlap(
            start, end, mode,
            left_label, right_label,
            left_pct, right_pct,
            left_num, left_den, right_num, right_den,
            md_tbl, ascii_chart,
            call_api=call_api and bool(os.getenv("OPENAI_API_KEY")),
            model=model, max_words=max_words, temperature=temperature
        )

    if export_org or want_comment:
        _export_org(title, start, end, left_label, right_label,
                    left_num, left_den, left_pct, right_num, right_den, right_pct,
                    mode, png, org, comment_text=comment_text)

    # Log
    print(f"\n📊 {left_label}: {left_pct:.1f}%  |  {right_label}: {right_pct:.1f}%")
    if mode == "time-share":
        print(f"   n={left_num:.0f}/{left_den:.0f} (esq.)  |  n={right_num:.0f}/{right_den:.0f} (dir.)\n")
    else:
        print(f"   n={int(left_num)}/{int(left_den)} (esq.)  |  n={int(right_num)}/{int(right_den)} (dir.)\n")

def run_top10(start: str, end: str, min_analises: int, mode: str,
              export_md: bool, export_png: bool, export_org: bool,
              chart: bool, want_comment: bool, call_api: bool,
              model: str, max_words: int, temperature: float) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        tbl, has_ind = _detect_tables(conn)
        if not has_ind:
            raise RuntimeError("Tabela 'indicadores' não encontrada — calcule indicadores antes de usar --top10.")
        names = _top10_names(conn, tbl, start, end, min_analises)
        if not names:
            print("⚠️ Nenhum perito elegível para Top 10 nesse período.")
            return
        df = _load_period_intervals(conn, tbl, start, end)

    if df.empty:
        print("⚠️ Nenhuma análise válida no período.")
        return

    stats = _compute_all_peritos_stats(df)

    left_set  = set(names)
    right_set = set(stats["nomePerito"]) - left_set

    left_num, left_den, left_pct, _    = _aggregate_group(stats, left_set, mode)
    right_num, right_den, right_pct, _ = _aggregate_group(stats, right_set, mode)

    left_label, right_label = "Top 10 piores", "Brasil (excl.)"
    title = {
        "perito-share": "Sobreposição — percentual de peritos com overlap (Top 10 vs Brasil (excl.))",
        "task-share":   "Sobreposição — percentual de tarefas sobrepostas (Top 10 vs Brasil (excl.))",
        "time-share":   "Sobreposição — percentual do tempo em overlap (Top 10 vs Brasil (excl.))",
    }[mode]

    stem  = f"sobreposicao_{mode}_top10"
    png   = os.path.join(EXPORT_DIR, f"{stem}.png")
    org   = f"{stem}.org"

    a_label, b_label = _unit_labels(mode)
    if mode == "time-share":
        md_tbl = (
            f"| Categoria | {a_label} | {b_label} | % |\n"
            f"|-----------|------------------:|------------------:|---:|\n"
            f"| {left_label}  | {left_num:.0f} | {left_den:.0f} | {left_pct:.1f}% |\n"
            f"| {right_label} | {right_num:.0f} | {right_den:.0f} | {right_pct:.1f}% |\n"
        )
    else:
        md_tbl = (
            f"| Categoria | {a_label} | {b_label} | % |\n"
            f"|-----------|------------------:|------------------:|---:|\n"
            f"| {left_label}  | {int(left_num)} | {int(left_den)} | {left_pct:.1f}% |\n"
            f"| {right_label} | {int(right_num)} | {int(right_den)} | {right_pct:.1f}% |\n"
        )

    if export_md or want_comment:
        _export_md(title, start, end, left_label, right_label,
                   left_num, left_den, left_pct, right_num, right_den, right_pct, mode, stem)

    if export_png or export_org or want_comment:
        if not os.path.exists(png):
            _render_png(title, left_label, right_label,
                        left_pct, right_pct, left_num, left_den, right_num, right_den, mode, png)

    if chart:
        _render_ascii(title, left_label, right_label, left_pct, right_pct, mode)

    comment_text: Optional[str] = None
    if want_comment:
        ascii_chart = ""
        if p is not None:
            try:
                p.clear_data()
                p.bar([left_label, right_label], [left_pct, right_pct])
                p.title(title)
                p.plotsize(80, 15)
                ascii_chart = _px_build()
            except Exception:
                ascii_chart = ""
        _load_openai_key_from_dotenv(os.path.join(BASE_DIR, ".env"))
        comment_text = _generate_comment_text_overlap(
            start, end, mode,
            left_label, right_label,
            left_pct, right_pct,
            left_num, left_den, right_num, right_den,
            md_tbl, ascii_chart,
            call_api=call_api and bool(os.getenv("OPENAI_API_KEY")),
            model=model, max_words=max_words, temperature=temperature
        )

    if export_org or want_comment:
        _export_org(title, start, end, left_label, right_label,
                    left_num, left_den, left_pct, right_num, right_den, right_pct,
                    mode, png, org, top_names=names, comment_text=comment_text)

    print(f"\n📊 {left_label}: {left_pct:.1f}%  |  {right_label}: {right_pct:.1f}%")
    if mode == "time-share":
        print(f"   n={left_num:.0f}/{left_den:.0f} (grupo)  |  n={right_num:.0f}/{right_den:.0f} (Brasil excl.)\n")
    else:
        print(f"   n={int(left_num)}/{int(left_den)} (grupo)  |  n={int(right_num)}/{int(right_den)} (Brasil excl.)\n")

# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    want_comment = args.export_comment or args.add_comments or args.export_comment_org
    if args.top10:
        run_top10(args.start, args.end, args.min_analises, args.mode,
                  args.export_md, args.export_png, args.export_org,
                  args.chart, want_comment, args.call_api,
                  args.model, args.max_words, args.temperature)
    else:
        perito = args.perito or args.nome
        run_perito(args.start, args.end, perito, args.mode,
                   args.export_md, args.export_png, args.export_org,
                   args.chart, want_comment, args.call_api,
                   args.model, args.max_words, args.temperature)

if __name__ == "__main__":
    main()

