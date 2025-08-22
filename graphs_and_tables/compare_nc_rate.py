#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Compara a porcentagem dos motivos de não conformidade (NC) de:
  a) um PERITO específico (vs Brasil excluindo esse perito), ou
  b) o GRUPO dos 10 piores peritos por scoreFinal (vs Brasil excluindo o grupo),

usando a DESCRIÇÃO no eixo X (texto em protocolos.motivo, com fallback para o código).

Regras IMPORTANTES:
- A definição de NC é robusta e considera:
    NC = (conformado = 0)  OU  (motivoNaoConformado, mesmo como TEXTO, não-vazio e CAST(...) <> 0)
  OBS: O campo protocolos.motivo é usado APENAS como DESCRIÇÃO para o gráfico/tabelas, e não
       influencia na contagem de NC.
- Compatibilidade de schema:
    * Detecta a tabela de análises: analises OU analises_atestmed
    * Usa substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
- Backend Matplotlib = Agg (gera PNG em ambiente headless)

Saídas:
--export-org, --export-md (opcional), --export-png, --export-comment (MD legado),
--export-comment-org (insere comentário no .org), --chart (ASCII)

Parâmetros visuais:
--label-maxlen (abrevia rótulos do eixo X com “…”)
--label-fontsize (tamanho da fonte dos rótulos do eixo X)
"""

import os
import sys
import re
import json
import sqlite3
import argparse
from typing import Dict, Any, Tuple, Optional, List

# permitir imports de utils/*
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    import plotext as p
except Exception:
    p = None

BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# Integração com comentários (GPT)
_COMENT_FUNC = None
try:
    # novo dispatcher do utils/comentarios.py
    from utils.comentarios import comentar_motivos as _COMENT_FUNC  # type: ignore
except Exception:
    _COMENT_FUNC = None


# ──────────────────────────────────────────────────────────────────────
# OpenAI helpers (.env + clientes novo/legado)
# ──────────────────────────────────────────────────────────────────────

def _load_openai_key_from_dotenv(env_path: str) -> Optional[str]:
    """Carrega OPENAI_API_KEY do .env se existir (python-dotenv ou parse manual)."""
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


def _call_openai_chat(messages: List[Dict[str, str]], model: str = "gpt-4o-mini", temperature: float = 0.2) -> Optional[str]:
    """Compat com SDK novo e legado. Retorna texto ou None."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    # Novo
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
    # Legado
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


def gerar_comentario_gpt(df: pd.DataFrame,
                         meta: Dict[str, Any],
                         cuts: Optional[Dict[str, Any]],
                         model: str = "gpt-4o-mini",
                         max_words: int = 180,
                         temperature: float = 0.2) -> Optional[str]:
    """Pede um comentário em texto corrido (PT-BR) para inserir no .org."""
    linhas = []
    for _, r in df.iterrows():
        linhas.append({
            "descricao": str(r.get("descricao", "")),
            "pct_lhs": round(float(r.get("pct_perito", 0.0)), 2),
            "pct_rhs": round(float(r.get("pct_brasil", 0.0)), 2),
            "n_lhs": int(r.get("n_perito", 0) or 0),
            "n_rhs": int(r.get("n_brasil", 0) or 0),
        })

    lhs = meta.get("label_lhs", "Grupo")
    rhs = meta.get("label_rhs", "Brasil (excl.)")
    periodo = f"{meta.get('start','')} a {meta.get('end','')}"
    taxa_lhs = meta.get("nc_rate_p", None)
    taxa_rhs = meta.get("nc_rate_b", None)
    taxa_lhs_str = f"{float(taxa_lhs):.1f}%" if isinstance(taxa_lhs, (int, float)) else "n/d"
    taxa_rhs_str = f"{float(taxa_rhs):.1f}%" if isinstance(taxa_rhs, (int, float)) else "n/d"

    resumo_json = {
        "periodo": periodo,
        "labels": {"lhs": lhs, "rhs": rhs},
        "taxa_nc": {"lhs": taxa_lhs_str, "rhs": taxa_rhs_str},
        "rows": linhas,
        "cuts": {k: v for k, v in (cuts or {}).items() if v is not None},
        "mode": meta.get("mode", "single"),
    }

    system_msg = (
        "Você é um analista de dados especializado em gestão pública e auditoria do ATESTMED. "
        "Escreva comentários claros, objetivos e tecnicamente corretos."
    )
    user_msg = (
        f"Produza um comentário interpretativo em português (Brasil), em texto corrido (sem títulos, sem listas, sem tabelas), "
        f"com no máximo {max_words} palavras, para acompanhar o gráfico 'Motivos de NC – {lhs} vs {rhs}' no período {periodo}. "
        f"Inclua: (1) leitura geral das diferenças; (2) destaque 2–4 motivos com maiores diferenças em pontos percentuais "
        f"no formato 'X% vs Y%'; (3) mencione as taxas gerais de NC ({lhs}: {taxa_lhs_str}; {rhs}: {taxa_rhs_str}); "
        f"(4) evite jargões e conclusões causais. Dados (JSON):\n\n{json.dumps(resumo_json, ensure_ascii=False)}"
    )

    _load_openai_key_from_dotenv(os.path.join(BASE_DIR, ".env"))

    messages = [{"role": "system", "content": system_msg},
                {"role": "user",   "content": user_msg}]
    return _call_openai_chat(messages, model=model, temperature=temperature)


# ──────────────────────────────────────────────────────────────────────
# Utilidades gerais / limpeza de texto
# ──────────────────────────────────────────────────────────────────────

def _protect_comment_text(text: str, word_cap: int = 220) -> str:
    """Remove cercas de código, cabeçalhos [..], tabelas md/org e reduz para um parágrafo."""
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
    out = " ".join(" ".join(kept).split())
    words = out.split()
    if len(words) > word_cap:
        out = " ".join(words[:word_cap]).rstrip() + "…"
    return out


# ──────────────────────────────────────────────────────────────────────
# Schema helpers
# ──────────────────────────────────────────────────────────────────────

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
        (name,)
    ).fetchone()
    return row is not None

def _cols(conn: sqlite3.Connection, table: str) -> set:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}

def _detect_schema(conn: sqlite3.Connection) -> Dict[str, Any]:
    table = None
    for t in ('analises', 'analises_atestmed'):
        if _table_exists(conn, t):
            table = t
            break
    if not table:
        raise RuntimeError("Não encontrei as tabelas 'analises' nem 'analises_atestmed'.")

    cset = _cols(conn, table)
    required = {'siapePerito', 'dataHoraIniPericia'}
    missing = required - cset
    if missing:
        raise RuntimeError(f"Tabela '{table}' sem colunas obrigatórias: {missing}")

    motivo_col      = 'motivoNaoConformado' if 'motivoNaoConformado' in cset else None
    has_conformado  = 'conformado' in cset
    has_protocolo   = 'protocolo' in cset
    has_protocolos  = _table_exists(conn, 'protocolos')

    if not _table_exists(conn, 'peritos') or 'nomePerito' not in _cols(conn, 'peritos'):
        raise RuntimeError("Tabela 'peritos' ausente ou sem coluna 'nomePerito'.")

    has_indicadores = _table_exists(conn, 'indicadores')

    return {
        'table': table,
        'motivo_col': motivo_col,
        'has_conformado': has_conformado,
        'date_col': 'dataHoraIniPericia',
        'has_protocolo': has_protocolo,
        'has_protocolos_table': has_protocolos,
        'has_indicadores': has_indicadores,
    }


# ──────────────────────────────────────────────────────────────────────
# Núcleo (queries)
# ──────────────────────────────────────────────────────────────────────

def _fetch_df(conn: sqlite3.Connection, sql: str, params: Tuple) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)

def _get_counts_single(conn: sqlite3.Connection, start: str, end: str, perito: str, schema: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Retorna (df_perito, df_brasil_excl) com ['descricao','n'] usando regra robusta de NC."""
    t            = schema['table']
    motivo_col   = schema['motivo_col']
    has_conf     = schema['has_conformado']
    date_col     = schema['date_col']
    has_protcol  = schema['has_protocolo']
    has_prot     = schema['has_protocolos_table']

    join_prot = "LEFT JOIN protocolos pr ON pr.protocolo = a.protocolo" if (has_protcol and has_prot) else ""
    cast_target = f"a.{motivo_col}" if motivo_col else "NULL"
    desc_expr = f"COALESCE(NULLIF(TRIM(pr.motivo), ''), CAST({cast_target} AS TEXT)) AS descricao"

    if has_conf and motivo_col:
        cond_nc_total = (
            " (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
            " OR (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
            "     AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) "
        )
    elif has_conf and not motivo_col:
        cond_nc_total = " (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
    elif (not has_conf) and motivo_col:
        cond_nc_total = (
            " (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
            "  AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) "
        )
    else:
        cond_nc_total = " 0 "

    base_select = f"""
        SELECT {desc_expr}, COUNT(*) AS n
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE TRIM(UPPER(p.nomePerito)) {{cmp}}
           AND substr(a.{date_col},1,10) BETWEEN ? AND ?
           AND ( {cond_nc_total} )
         GROUP BY descricao
    """

    q_perito = base_select.format(cmp="= TRIM(UPPER(?))")
    q_outros = base_select.format(cmp="<> TRIM(UPPER(?))")

    df_p = _fetch_df(conn, q_perito, (perito, start, end))
    df_b = _fetch_df(conn, q_outros, (perito, start, end))

    if not df_p.empty:
        df_p['descricao'] = df_p['descricao'].astype(str).str.strip()
    if not df_b.empty:
        df_b['descricao'] = df_b['descricao'].astype(str).str.strip()

    return df_p, df_b

def _get_counts_group(conn: sqlite3.Connection, start: str, end: str, peritos: List[str], schema: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Agrega motivos do grupo (Top10) vs Brasil (excl.)."""
    if not peritos:
        return pd.DataFrame(columns=['descricao','n']), pd.DataFrame(columns=['descricao','n'])

    t            = schema['table']
    motivo_col   = schema['motivo_col']
    has_conf     = schema['has_conformado']
    date_col     = schema['date_col']
    has_protcol  = schema['has_protocolo']
    has_prot     = schema['has_protocolos_table']

    join_prot = "LEFT JOIN protocolos pr ON pr.protocolo = a.protocolo" if (has_protcol and has_prot) else ""
    cast_target = f"a.{motivo_col}" if motivo_col else "NULL"
    desc_expr = f"COALESCE(NULLIF(TRIM(pr.motivo), ''), CAST({cast_target} AS TEXT)) AS descricao"

    if has_conf and motivo_col:
        cond_nc_total = (
            " (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
            " OR (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
            "     AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) "
        )
    elif has_conf and not motivo_col:
        cond_nc_total = " (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
    elif (not has_conf) and motivo_col:
        cond_nc_total = (
            " (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
            "  AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) "
        )
    else:
        cond_nc_total = " 0 "

    placeholders = ",".join(["?"] * len(peritos))
    where_in  = f"IN ({placeholders})"
    where_out = f"NOT IN ({placeholders})"

    base_select = f"""
        SELECT {desc_expr}, COUNT(*) AS n
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE TRIM(UPPER(p.nomePerito)) {{cmp}}
           AND substr(a.{date_col},1,10) BETWEEN ? AND ?
           AND ( {cond_nc_total} )
         GROUP BY descricao
    """

    q_grp = base_select.format(cmp=where_in)
    q_out = base_select.format(cmp=where_out)

    peritos_upper = [p.strip().upper() for p in peritos]
    df_g = _fetch_df(conn, q_grp, tuple(peritos_upper) + (start, end))
    df_b = _fetch_df(conn, q_out, tuple(peritos_upper) + (start, end))

    if not df_g.empty: df_g['descricao'] = df_g['descricao'].astype(str).str.strip()
    if not df_b.empty: df_b['descricao'] = df_b['descricao'].astype(str).str.strip()
    return df_g, df_b


def _get_nc_rates_single(conn: sqlite3.Connection, start: str, end: str, perito: str, schema: Dict[str, Any]) -> Tuple[float, float]:
    """Retorna (taxa NC perito %, taxa NC Brasil-excl %)."""
    t            = schema['table']
    date_col     = schema['date_col']
    motivo_col   = schema['motivo_col']
    has_conf     = schema['has_conformado']
    has_protcol  = schema['has_protocolo']
    has_prot     = schema['has_protocolos_table']

    join_prot = "LEFT JOIN protocolos pr ON pr.protocolo = a.protocolo" if (has_protcol and has_prot) else ""

    if has_conf and motivo_col:
        cond_nc_total = (
            " (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
            " OR (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
            "     AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) "
        )
    elif has_conf and not motivo_col:
        cond_nc_total = " (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
    elif (not has_conf) and motivo_col:
        cond_nc_total = (
            " (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
            "  AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) "
        )
    else:
        cond_nc_total = " 0 "

    q_base = f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN {cond_nc_total} THEN 1 ELSE 0 END) AS nc
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE TRIM(UPPER(p.nomePerito)) {{cmp}} TRIM(UPPER(?))
           AND substr(a.{date_col},1,10) BETWEEN ? AND ?
    """
    row_p = conn.execute(q_base.format(cmp="="), (perito, start, end)).fetchone()
    total_p = int(row_p[0] or 0); nc_p = int(row_p[1] or 0)
    rate_p  = (nc_p / total_p * 100.0) if total_p > 0 else 0.0

    row_b = conn.execute(q_base.format(cmp="<>"), (perito, start, end)).fetchone()
    total_b = int(row_b[0] or 0); nc_b = int(row_b[1] or 0)
    rate_b  = (nc_b / total_b * 100.0) if total_b > 0 else 0.0
    return rate_p, rate_b


def _get_nc_rates_group(conn: sqlite3.Connection, start: str, end: str, peritos: List[str], schema: Dict[str, Any]) -> Tuple[float, float]:
    """Retorna (taxa NC grupo %, taxa NC Brasil-excl-grupo %)."""
    if not peritos:
        return 0.0, 0.0

    t            = schema['table']
    date_col     = schema['date_col']
    motivo_col   = schema['motivo_col']
    has_conf     = schema['has_conformado']
    has_protcol  = schema['has_protocolo']
    has_prot     = schema['has_protocolos_table']

    join_prot = "LEFT JOIN protocolos pr ON pr.protocolo = a.protocolo" if (has_protcol and has_prot) else ""

    if has_conf and motivo_col:
        cond_nc_total = (
            " (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
            " OR (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
            "     AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) "
        )
    elif has_conf and not motivo_col:
        cond_nc_total = " (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
    elif (not has_conf) and motivo_col:
        cond_nc_total = (
            " (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
            "  AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) "
        )
    else:
        cond_nc_total = " 0 "

    placeholders = ",".join(["?"] * len(peritos))
    where_in  = f"IN ({placeholders})"
    where_out = f"NOT IN ({placeholders})"

    q_grp = f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN {cond_nc_total} THEN 1 ELSE 0 END) AS nc
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE TRIM(UPPER(p.nomePerito)) {where_in}
           AND substr(a.{date_col},1,10) BETWEEN ? AND ?
    """
    q_out = f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN {cond_nc_total} THEN 1 ELSE 0 END) AS nc
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE TRIM(UPPER(p.nomePerito)) {where_out}
           AND substr(a.{date_col},1,10) BETWEEN ? AND ?
    """
    peritos_upper = [p.strip().upper() for p in peritos]
    row_g = conn.execute(q_grp, tuple(peritos_upper) + (start, end)).fetchone()
    row_b = conn.execute(q_out, tuple(peritos_upper) + (start, end)).fetchone()

    total_g = int(row_g[0] or 0); nc_g = int(row_g[1] or 0)
    total_b = int(row_b[0] or 0); nc_b = int(row_b[1] or 0)

    rate_g  = (nc_g / total_g * 100.0) if total_g > 0 else 0.0
    rate_b  = (nc_b / total_b * 100.0) if total_b > 0 else 0.0
    return rate_g, rate_b


# ──────────────────────────────────────────────────────────────────────
# Montagem dos comparativos
# ──────────────────────────────────────────────────────────────────────

def _build_comparativo_single(start: str, end: str, perito: str, topn: int = 10) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        schema = _detect_schema(conn)
        df_p, df_b = _get_counts_single(conn, start, end, perito, schema)

        total_p = int(df_p['n'].sum()) if not df_p.empty else 0
        total_b = int(df_b['n'].sum()) if not df_b.empty else 0

        if df_p.empty: df_p = pd.DataFrame(columns=['descricao', 'n'])
        if df_b.empty: df_b = pd.DataFrame(columns=['descricao', 'n'])

        df = pd.merge(
            df_b.rename(columns={'n': 'n_brasil'}),
            df_p.rename(columns={'n': 'n_perito'}),
            on='descricao', how='outer'
        ).fillna(0)

        df['n_brasil'] = df['n_brasil'].astype(int)
        df['n_perito'] = df['n_perito'].astype(int)
        df['pct_brasil'] = (df['n_brasil'] / total_b * 100.0) if total_b > 0 else 0.0
        df['pct_perito'] = (df['n_perito'] / total_p * 100.0) if total_p > 0 else 0.0

        nc_rate_p, nc_rate_b = _get_nc_rates_single(conn, start, end, perito, schema)

    df = df.sort_values(['pct_brasil', 'n_brasil'], ascending=[False, False]).head(topn).reset_index(drop=True)
    meta = {
        'mode': 'single',
        'perito': perito,
        'start': start,
        'end': end,
        'total_p': total_p,
        'total_b': total_b,
        'nc_rate_p': nc_rate_p,
        'nc_rate_b': nc_rate_b,
        'label_lhs': perito,
        'label_rhs': 'Brasil (excl.)',
        'safe_stub': perito,
    }
    return df, meta


def _get_top10_peritos(conn: sqlite3.Connection, start: str, end: str, min_analises: int, schema: Dict[str, Any]) -> List[str]:
    """Retorna lista dos 10 piores por scoreFinal (elegíveis por mínimo de análises)."""
    if not schema.get('has_indicadores', False):
        raise RuntimeError("Tabela 'indicadores' não encontrada — calcule indicadores antes de usar --top10.")
    t        = schema['table']
    date_col = schema['date_col']

    sql = f"""
        SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
          FROM indicadores i
          JOIN peritos p  ON i.perito = p.siapePerito
          JOIN {t} a      ON a.siapePerito = i.perito
         WHERE substr(a.{date_col},1,10) BETWEEN ? AND ?
         GROUP BY p.nomePerito, i.scoreFinal
        HAVING total_analises >= ?
         ORDER BY i.scoreFinal DESC, total_analises DESC
         LIMIT 10
    """
    rows = conn.execute(sql, (start, end, min_analises)).fetchall()
    return [r[0] for r in rows]


def _build_comparativo_top10(start: str, end: str, topn: int = 10, min_analises: int = 50) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        schema = _detect_schema(conn)
        peritos = _get_top10_peritos(conn, start, end, min_analises, schema)
        if not peritos:
            return pd.DataFrame(columns=['descricao','n_brasil','n_perito','pct_brasil','pct_perito']), {
                'mode': 'top10', 'peritos_lista': [], 'start': start, 'end': end,
                'total_p': 0, 'total_b': 0, 'nc_rate_p': 0.0, 'nc_rate_b': 0.0,
                'label_lhs': 'Top 10 piores', 'label_rhs': 'Brasil (excl.)', 'safe_stub': 'Top 10 piores'
            }

        df_g, df_b = _get_counts_group(conn, start, end, peritos, schema)

        total_g = int(df_g['n'].sum()) if not df_g.empty else 0
        total_b = int(df_b['n'].sum()) if not df_b.empty else 0

        if df_g.empty: df_g = pd.DataFrame(columns=['descricao', 'n'])
        if df_b.empty: df_b = pd.DataFrame(columns=['descricao', 'n'])

        df = pd.merge(
            df_b.rename(columns={'n': 'n_brasil'}),
            df_g.rename(columns={'n': 'n_perito'}),
            on='descricao', how='outer'
        ).fillna(0)

        df['n_brasil'] = df['n_brasil'].astype(int)
        df['n_perito'] = df['n_perito'].astype(int)
        df['pct_brasil'] = (df['n_brasil'] / total_b * 100.0) if total_b > 0 else 0.0
        df['pct_perito'] = (df['n_perito'] / total_g * 100.0) if total_g > 0 else 0.0

        nc_rate_g, nc_rate_b = _get_nc_rates_group(conn, start, end, peritos, schema)

    df = df.sort_values(['pct_brasil', 'n_brasil'], ascending=[False, False]).head(topn).reset_index(drop=True)
    meta = {
        'mode': 'top10',
        'peritos_lista': peritos,
        'start': start,
        'end': end,
        'total_p': total_g,
        'total_b': total_b,
        'nc_rate_p': nc_rate_g,
        'nc_rate_b': nc_rate_b,
        'label_lhs': 'Top 10 piores',
        'label_rhs': 'Brasil (excl.)',
        'safe_stub': 'Top 10 piores',
    }
    return df, meta


# ──────────────────────────────────────────────────────────────────────
# Filtros (cuts) e Top-N
# ──────────────────────────────────────────────────────────────────────

def aplicar_cuts_e_topn(df: pd.DataFrame,
                        topn: int,
                        min_pct_perito: Optional[float],
                        min_pct_brasil: Optional[float],
                        min_n_perito: Optional[int],
                        min_n_brasil: Optional[int]) -> pd.DataFrame:
    if df.empty:
        return df

    m = df.copy()
    if min_pct_perito is not None:
        m = m[m['pct_perito'] >= float(min_pct_perito)]
    if min_pct_brasil is not None:
        m = m[m['pct_brasil'] >= float(min_pct_brasil)]
    if min_n_perito is not None:
        m = m[m['n_perito'] >= int(min_n_perito)]
    if min_n_brasil is not None:
        m = m[m['n_brasil'] >= int(min_n_brasil)]

    m = m.sort_values(['pct_brasil', 'n_brasil'], ascending=[False, False]).head(topn).reset_index(drop=True)
    return m


# ──────────────────────────────────────────────────────────────────────
# Exportações e gráficos
# ──────────────────────────────────────────────────────────────────────

def _safe_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name).strip("_")

def _abbrev(s: str) -> str:
    return s

def _abbrev(s: str, maxlen: int) -> str:
    s = (str(s) or "").strip()
    return s if len(s) <= maxlen else s[:max(1, maxlen - 1)] + "…"

def _ascii_label(s: str, maxlen: int) -> str:
    return _abbrev(s, maxlen=maxlen)

def exportar_md(df: pd.DataFrame, meta: Dict[str, Any], cuts: Dict[str, Any]) -> str:
    safe = _safe_name(meta['safe_stub'])
    fname = "motivos_top10_vs_brasil.md" if meta['mode'] == 'top10' else f"motivos_perito_vs_brasil_{safe}.md"
    path = os.path.join(EXPORT_DIR, fname)

    if df.empty:
        if meta['mode'] == 'top10':
            md = (f"# Motivos de NC – Top 10 piores vs Brasil (excl.)\n\n"
                  f"**Período:** {meta['start']} a {meta['end']}\n\n"
                  f"Sem dados de não conformidade no período para o Top 10 e/ou Brasil (excl.).\n")
        else:
            md = (f"# Motivos de NC – {meta['label_lhs']} vs {meta['label_rhs']}\n\n"
                  f"**Período:** {meta['start']} a {meta['end']}\n\n"
                  f"Sem dados de não conformidade no período.\n")
        with open(path, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"⚠️ Sem dados. Markdown salvo em: {path}")
        return path

    if meta['mode'] == 'top10':
        header = (
            f"# Motivos de NC – Top 10 piores vs Brasil (excl.)\n\n"
            f"**Período:** {meta['start']} a {meta['end']}\n\n"
            f"**Top 10 piores (scoreFinal):** {', '.join(meta.get('peritos_lista', []))}\n\n"
            f"**Total NC Top 10:** {meta['total_p']}  "
            f"**Total NC Brasil (excl.):** {meta['total_b']}\n\n"
            f"**Taxa NC Top 10:** {meta['nc_rate_p']:.1f}%  "
            f"**Taxa NC Brasil (excl.):** {meta['nc_rate_b']:.1f}%\n\n"
            f"**Cortes aplicados:** {cuts}\n\n"
        )
    else:
        header = (
            f"# Motivos de NC – {meta['label_lhs']} vs {meta['label_rhs']}\n\n"
            f"**Período:** {meta['start']} a {meta['end']}\n\n"
            f"**Total NC {meta['label_lhs']}:** {meta['total_p']}  "
            f"**Total NC {meta['label_rhs']}:** {meta['total_b']}\n\n"
            f"**Taxa NC {meta['label_lhs']}:** {meta['nc_rate_p']:.1f}%  "
            f"**Taxa NC {meta['label_rhs']}:** {meta['nc_rate_b']:.1f}%\n\n"
            f"**Cortes aplicados:** {cuts}\n\n"
        )

    tbl = ["| Motivo (descrição) | % " + meta['label_lhs'] + " | % " + meta['label_rhs'] + " | n " + meta['label_lhs'] + " | n " + meta['label_rhs'] + " |",
           "|--------------------|---------:|---------:|---------:|---------:|"]
    for _, r in df.iterrows():
        tbl.append(
            f"| {r['descricao']} | {r['pct_perito']:.2f}% | {r['pct_brasil']:.2f}% | "
            f"{int(r['n_perito'])} | {int(r['n_brasil'])} |"
        )
    md = header + "\n".join(tbl) + "\n"

    with open(path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"✅ Markdown salvo em: {path}")
    return path


def exportar_org(df: pd.DataFrame, meta: Dict[str, Any], cuts: Dict[str, Any],
                 png_path: Optional[str], comment_text: Optional[str] = None) -> str:
    safe = _safe_name(meta['safe_stub'])
    fname = "motivos_top10_vs_brasil.org" if meta['mode'] == 'top10' else f"motivos_perito_vs_brasil_{safe}.org"
    path = os.path.join(EXPORT_DIR, fname)

    lines = []
    title = "Motivos de NC – Top 10 piores vs Brasil (excl.)" if meta['mode']=='top10' \
            else f"Motivos de NC – {meta['label_lhs']} vs {meta['label_rhs']}"
    lines.append(f"* {title}")
    lines.append(":PROPERTIES:")
    lines.append(f":PERIODO: {meta['start']} a {meta['end']}")
    if meta['mode']=='top10' and meta.get('peritos_lista'):
        lines.append(f":TOP10: {', '.join(meta['peritos_lista'])}")
    lines.append(f":NC_{meta['label_lhs']}: {meta['nc_rate_p']:.1f}%")
    lines.append(f":NC_{meta['label_rhs']}: {meta['nc_rate_b']:.1f}%")
    cuts_str = ", ".join([f"{k}={v}" for k, v in cuts.items() if v is not None]) or "nenhum"
    lines.append(f":CUTS: {cuts_str}")
    lines.append(":END:\n")

    # Tabela
    lines.append("| Motivo (descrição) | % " + meta['label_lhs'] + " | % " + meta['label_rhs'] +
                 " | n " + meta['label_lhs'] + " | n " + meta['label_rhs'] + " |")
    lines.append("|-")
    if not df.empty:
        for _, r in df.iterrows():
            lines.append(f"| {r['descricao']} | {r['pct_perito']:.2f}% | {r['pct_brasil']:.2f}% | {int(r['n_perito'])} | {int(r['n_brasil'])} |")
    else:
        lines.append("| — | — | — | — | — |")

    # Imagem (se existir)
    if png_path and os.path.exists(png_path):
        rel_png = os.path.basename(png_path)
        lines.append("\n#+CAPTION: Distribuição percentual dos motivos (barras lado a lado).")
        lines.append(f"[[file:{rel_png}]]")

    # Comentário (se solicitado)
    if comment_text:
        lines.append("\n** Comentário")
        lines.append(comment_text.strip())

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"✅ Org salvo em: {path}")
    return path


def exportar_png(df: pd.DataFrame, meta: Dict[str, Any],
                 label_maxlen: int = 18, label_fontsize: int = 8) -> Optional[str]:
    if df.empty:
        print("⚠️ Sem dados para gerar PNG.")
        return None

    safe = _safe_name(meta['safe_stub'])
    fname = "motivos_top10_vs_brasil.png" if meta['mode'] == 'top10' else f"motivos_perito_vs_brasil_{safe}.png"
    path = os.path.join(EXPORT_DIR, fname)

    motivos_full = df['descricao'].astype(str).tolist()
    motivos = [_abbrev(m, label_maxlen) for m in motivos_full]
    pct_lhs = df['pct_perito'].tolist()
    pct_rhs = df['pct_brasil'].tolist()

    fig_width = max(7.5, len(motivos) * 0.7)
    fig, ax = plt.subplots(figsize=(fig_width, 5.2), dpi=300)

    x = range(len(motivos))
    width = 0.4

    label_lhs = f"{meta['label_lhs']} (NC {meta.get('nc_rate_p', 0.0):.1f}%)"
    label_rhs = f"{meta['label_rhs']} (NC {meta.get('nc_rate_b', 0.0):.1f}%)"

    ax.bar([i - width/2 for i in x], pct_lhs, width, label=label_lhs, edgecolor='black')
    ax.bar([i + width/2 for i in x], pct_rhs, width, label=label_rhs, edgecolor='black')

    ax.set_xticks(list(x))
    ax.set_xticklabels(motivos, rotation=45, ha='right')
    ax.tick_params(axis='x', labelsize=label_fontsize)

    ax.set_ylabel("% dentro dos NC")
    title_main = "Motivos de NC – Top 10 piores vs Brasil (excl.)" if meta['mode']=='top10' \
                 else f"Motivos de NC – {meta['label_lhs']} vs {meta['label_rhs']}"
    ax.set_title(f"{title_main}\n{meta['start']} a {meta['end']}", pad=10)
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    ax.legend()

    ymax = max(pct_lhs + pct_rhs + [1.0])
    for i, (pl, pr) in enumerate(zip(pct_lhs, pct_rhs)):
        ax.text(i - width/2, pl + ymax*0.01, f"{pl:.1f}%", ha='center', va='bottom', fontsize=max(7, label_fontsize-1))
        ax.text(i + width/2, pr + ymax*0.01, f"{pr:.1f}%", ha='center', va='bottom', fontsize=max(7, label_fontsize-1))

    plt.tight_layout()
    fig.subplots_adjust(bottom=0.20)
    fig.savefig(path, bbox_inches='tight')
    plt.close(fig); plt.close('all')
    print(f"✅ PNG salvo em: {path}")
    return path

def _stem_from_meta(meta: Dict[str, Any]) -> str:
    """Base do nome de arquivo (igual à imagem) para gerar o sidecar de comentário."""
    safe = _safe_name(meta['safe_stub'])
    return "motivos_top10_vs_brasil" if meta['mode'] == 'top10' else f"motivos_perito_vs_brasil_{safe}"

def _export_comment_sidecar_org(meta: Dict[str, Any], text: Optional[str]) -> Optional[str]:
    """Grava graphs_and_tables/exports/<stem>_comment.org com o parágrafo interpretativo."""
    if not text:
        return None
    stem = _stem_from_meta(meta)
    path = os.path.join(EXPORT_DIR, f"{stem}_comment.org")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text.strip() + "\n")
    print(f"📝 Comentário(ORG) salvo em: {path}")
    return path

def _plotext_multi_bar(x_labels, series_list, labels):
    """Compat: usa multiple_bar (novo) ou multi_bar (antigo); fallback em p.bar."""
    if p is None:
        print("plotext não instalado; pulei o gráfico ASCII.")
        return
    func = getattr(p, "multiple_bar", None) or getattr(p, "multi_bar", None)
    if func:
        try:
            func(x_labels, series_list, labels=labels)  # novas versões
        except TypeError:
            func(x_labels, series_list, label=labels)   # versões antigas
        return
    for s, lab in zip(series_list, labels):
        p.bar(x_labels, s, label=lab)

def exibir_chart_ascii(df: pd.DataFrame, meta: Dict[str, Any], label_maxlen: int = 18) -> None:
    if p is None:
        print("plotext não instalado; pulei o gráfico ASCII.")
        return
    if df.empty:
        print("⚠️ Sem dados para exibir gráfico ASCII.")
        return
    p.clear_data()
    motivos = [_ascii_label(m, maxlen=label_maxlen) for m in df['descricao'].astype(str).tolist()]
    label_lhs = f"{meta['label_lhs']} (NC {meta.get('nc_rate_p', 0.0):.1f}%)"
    label_rhs = f"{meta['label_rhs']} (NC {meta.get('nc_rate_b', 0.0):.1f}%)"
    _plotext_multi_bar(
        motivos,
        [df['pct_perito'].tolist(), df['pct_brasil'].tolist()],
        labels=[label_lhs, label_rhs]
    )
    p.title("Motivos de NC – Top 10 piores vs Brasil (excl.)" if meta['mode']=='top10'
            else f"Motivos de NC – {meta['label_lhs']} vs {meta['label_rhs']}")
    p.xlabel("Motivo"); p.ylabel("% nos NC")
    p.plotsize(100, 20)
    p.show()


# ──────────────────────────────────────────────────────────────────────
# Comentários (MD retrocompat + geração para ORG)
#   → PRIORIDADE: API direta por valores → utils.comentarios → heurístico
# ──────────────────────────────────────────────────────────────────────

def _build_payload_motivos_for_gpt(df: pd.DataFrame, meta: Dict[str, Any], cuts: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Payload no formato usado por utils.comentarios.comentar_motivos."""
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "descricao": str(r.get("descricao", "")),
            "pct_perito": float(r.get("pct_perito", 0.0)),
            "pct_brasil": float(r.get("pct_brasil", 0.0)),
            "n_perito": int(r.get("n_perito", 0) or 0),
            "n_brasil": int(r.get("n_brasil", 0) or 0),
        })
    nc_p = meta.get("nc_rate_p", None)
    nc_b = meta.get("nc_rate_b", None)
    nc_lhs = float(nc_p) if isinstance(nc_p, (int, float)) else None
    nc_rhs = float(nc_b) if isinstance(nc_b, (int, float)) else None

    payload = {
        "period": (meta.get("start",""), meta.get("end","")),
        "nc_rate": {"lhs": nc_lhs, "rhs": nc_rhs},
        "rows": rows,
        "mode": meta.get("mode", "single"),
        "meta": {
            "lhs_label": meta.get("label_lhs", "Grupo"),
            "rhs_label": meta.get("label_rhs", "Brasil (excl.)"),
            "peritos_lista": meta.get("peritos_lista", []),
            "cuts": cuts or {},
        },
    }
    return payload


def exportar_comment(df: pd.DataFrame, meta: Dict[str, Any], cuts: Optional[Dict[str, Any]] = None,
                     model: str = "gpt-4o-mini", max_words: int = 180, temperature: float = 0.2) -> str:
    """Retrocompat: gera comentário separado em Markdown, priorizando VALORES (API) e com fallbacks."""
    safe = _safe_name(meta['safe_stub'])
    fname = "motivos_top10_vs_brasil_comment.md" if meta['mode']=='top10' else f"motivos_perito_vs_brasil_{safe}_comment.md"
    path = os.path.join(EXPORT_DIR, fname)

    if df.empty:
        texto = (
            f"**Período:** {meta['start']} a {meta['end']}\n\n"
            f"Sem dados de NC para {meta['label_lhs']} e/ou {meta['label_rhs']} no período."
        )
        with open(path, "w", encoding="utf-8") as f:
            f.write(texto)
        print(f"⚠️ Comentário salvo (sem dados) em: {path}")
        return path

    # 1) API direta (VALORES)
    texto = None
    try:
        _load_openai_key_from_dotenv(os.path.join(BASE_DIR, ".env"))
        texto = gerar_comentario_gpt(df, meta, cuts, model=model, max_words=max_words, temperature=temperature)
    except Exception:
        texto = None

    # 2) utils.comentarios (fallback)
    if (not texto) and (_COMENT_FUNC is not None):
        payload = _build_payload_motivos_for_gpt(df, meta, cuts)
        try:
            bruto = _COMENT_FUNC(payload, call_api=True)  # string
            texto = bruto if isinstance(bruto, str) else str(bruto)
        except Exception as e:
            print(f"⚠️ Falha ao gerar comentário via utils.comentarios: {e}")
            texto = None

    # 3) heurístico local
    if not texto:
        diffs = [(float(r['pct_perito']) - float(r['pct_brasil']), r) for _, r in df.iterrows()]
        diffs_sorted = sorted(diffs, key=lambda x: abs(x[0]), reverse=True)[:3]
        bullets = []
        for dpp, r in diffs_sorted:
            direcao = "acima" if dpp > 0 else "abaixo"
            bullets.append(f"- **{r['descricao']}**: {abs(dpp):.2f} p.p. {direcao} de {meta['label_rhs']}.")
        texto = (
            f"**Período:** {meta['start']} a {meta['end']}**\n\n"
            f"**Taxa NC {meta['label_lhs']}:** {meta['nc_rate_p']:.1f}%  "
            f"**Taxa NC {meta['label_rhs']}:** {meta['nc_rate_b']:.1f}%\n\n"
            + ("\n".join(bullets) if bullets else "Sem diferenças relevantes.")
            + "\n\nObservação: percentuais calculados dentro do conjunto de NC de cada grupo."
        )

    texto = _protect_comment_text(texto)

    with open(path, "w", encoding="utf-8") as f:
        f.write(texto.strip() + "\n")
    print(f"✅ Comentário salvo em: {path}")
    return path


def _gerar_texto_comentario_para_org(
    df: pd.DataFrame,
    meta: Dict[str, Any],
    cuts: Optional[Dict[str, Any]],
    model: str,
    max_words: int,
    temperature: float
) -> str:
    """Gera texto corrido para inserir no .org
       PRIORIDADE: API por valores → utils.comentarios → heurístico."""
    texto = None

    # 1) API direta (valores)
    if not df.empty:
        try:
            _load_openai_key_from_dotenv(os.path.join(BASE_DIR, ".env"))
            texto = gerar_comentario_gpt(df, meta, cuts, model=model, max_words=max_words, temperature=temperature)
        except Exception:
            texto = None

    # 2) utils.comentarios (fallback)
    if (not texto) and (_COMENT_FUNC is not None) and (not df.empty):
        try:
            payload = _build_payload_motivos_for_gpt(df, meta, cuts)
            bruto = _COMENT_FUNC(payload, call_api=True)
            texto = bruto if isinstance(bruto, str) else str(bruto)
        except Exception:
            texto = None

    # 3) heurístico local
    if not texto:
        if df.empty:
            texto = (f"Sem dados de não conformidade suficientes no período "
                     f"para {meta.get('label_lhs','Grupo')} e/ou {meta.get('label_rhs','Brasil (excl.)')}.")
        else:
            diffs = [(float(r['pct_perito']) - float(r['pct_brasil']), r) for _, r in df.iterrows()]
            diffs_sorted = sorted(diffs, key=lambda x: abs(x[0]), reverse=True)[:3]
            partes = []
            partes.append(
                f"No período {meta['start']} a {meta['end']}, as taxas gerais de NC foram "
                f"{meta['label_lhs']}: {meta['nc_rate_p']:.1f}% e {meta['label_rhs']}: {meta['nc_rate_b']:.1f}%."
            )
            if diffs_sorted:
                destaques = [f"{r['descricao']} ({float(r['pct_perito']):.2f}% vs {float(r['pct_brasil']):.2f}%)" for _, r in diffs_sorted]
                partes.append("Maiores diferenças em: " + "; ".join(destaques) + ".")
            partes.append("Percentuais referem-se à distribuição interna dos NC em cada grupo.")
            texto = " ".join(partes)

    return _protect_comment_text(texto or "", word_cap=max_words + 40)


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Compara a porcentagem dos motivos de NC: perito (ou Top 10 piores por scoreFinal) "
            "versus Brasil (excluindo o(s) perito(s)) no período (X = descrição)."
        )
    )
    ap.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    ap.add_argument('--end',   required=True, help='Data final   YYYY-MM-DD')

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--perito', help='Nome do perito (exato)')
    g.add_argument('--top10', action='store_true', help='Usar o grupo dos 10 piores por scoreFinal no período')

    ap.add_argument('--min-analises', type=int, default=50,
                    help='Mínimo de análises no período para elegibilidade ao Top 10 (padrão 50)')
    ap.add_argument('--topn', type=int, default=10,
                    help='Quantidade de motivos exibidos (Top-N, padrão 10)')

    # cuts (sem '%' nos textos para evitar crash do argparse)
    ap.add_argument('--min-pct-perito', type=float, default=None,
                    help='Filtro: porcentagem do perito maior ou igual a X')
    ap.add_argument('--min-pct-brasil', type=float, default=None,
                    help='Filtro: porcentagem do Brasil (excl.) maior ou igual a X')
    ap.add_argument('--min-n-perito', type=int, default=None,
                    help='Filtro: contagem do perito maior ou igual a N')
    ap.add_argument('--min-n-brasil', type=int, default=None,
                    help='Filtro: contagem do Brasil (excl.) maior ou igual a N')

    # layout
    ap.add_argument('--label-maxlen', type=int, default=18,
                    help='Comprimento máximo dos rótulos do eixo X (abrevia com …)')
    ap.add_argument('--label-fontsize', type=int, default=8,
                    help='Tamanho da fonte dos rótulos do eixo X (px)')

    # outputs
    ap.add_argument('--chart', action='store_true',
                    help='Exibe gráfico ASCII no terminal (plotext)')
    ap.add_argument('--export-md', action='store_true',
                    help='(Opcional) Exporta tabela em Markdown')
    ap.add_argument('--export-org', action='store_true',
                    help='Exporta tabela (e imagem, se existir) em Org-mode')
    ap.add_argument('--export-png', action='store_true',
                    help='Exporta gráfico em PNG')
    ap.add_argument('--export-comment', action='store_true',
                    help='Exporta comentário em Markdown (retrocompat)')
    ap.add_argument('--export-comment-org', action='store_true',
                    help='Insere o comentário diretamente no arquivo Org gerado')
    ap.add_argument('--add-comments', action='store_true',
                    help='Atalho: também gera comentário interpretativo')

    # OpenAI params
    ap.add_argument('--model', default='gpt-4o-mini', help='Modelo (padrão: gpt-4o-mini)')
    ap.add_argument('--max-words', type=int, default=180, help='Limite de palavras do comentário (padrão: 180)')
    ap.add_argument('--temperature', type=float, default=0.2, help='Temperatura da geração (padrão: 0.2)')
    return ap.parse_args()


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    # monta DF base (sem filtros)
    if args.top10:
        df, meta = _build_comparativo_top10(args.start, args.end, args.topn, args.min_analises)
    else:
        df, meta = _build_comparativo_single(args.start, args.end, args.perito, args.topn)

    # aplica cuts + reaplica topn
    df = aplicar_cuts_e_topn(
        df,
        topn=args.topn,
        min_pct_perito=args.min_pct_perito,
        min_pct_brasil=args.min_pct_brasil,
        min_n_perito=args.min_n_perito,
        min_n_brasil=args.min_n_brasil
    )

    # Saída no console
    if df.empty:
        print("\n⚠️ Não há dados suficientes para comparação no período informado (após cortes/topN).\n")
    else:
        ttl = "Top 10 piores" if meta['mode']=='top10' else meta['label_lhs']
        print(f"\n📊 Motivos de NC – {ttl} vs {meta['label_rhs']} | {meta['start']} a {meta['end']}")
        print(f"   Taxa NC {meta['label_lhs']}: {meta['nc_rate_p']:.1f}% | {meta['label_rhs']}: {meta['nc_rate_b']:.1f}%\n")
        print(df[['descricao', 'pct_perito', 'pct_brasil', 'n_perito', 'n_brasil']])

    # Cuts para cabeçalho
    cuts_info = {
        'min_pct_perito': args.min_pct_perito,
        'min_pct_brasil': args.min_pct_brasil,
        'min_n_perito': args.min_n_perito,
        'min_n_brasil': args.min_n_brasil,
        'topn': args.topn
    }

    # Exports (garanta PNG se for gerar .org ou comentário)
    png_path = None
    if args.export_png or args.export_org or args.export_comment_org or args.add_comments:
        png_path = exportar_png(df, meta, label_maxlen=args.label_maxlen, label_fontsize=args.label_fontsize)

    # Comentário a ser inserido no .org (valores → utils → heurístico)
    comment_for_org = None
    if args.export_comment_org or args.add_comments:
        comment_for_org = _gerar_texto_comentario_para_org(
            df, meta, cuts_info, model=args.model, max_words=args.max_words, temperature=args.temperature
        )

    if args.export_md:
        exportar_md(df, meta, cuts_info)

    if args.export_comment:
        exportar_comment(df, meta, cuts_info, model=args.model, max_words=args.max_words, temperature=args.temperature)

    if args.export_org or args.export_comment_org or args.add_comments:
        exportar_org(df, meta, cuts_info, png_path, comment_text=comment_for_org)
        # sidecar .org para o coletor
        _export_comment_sidecar_org(meta, comment_for_org)

    if args.chart:
        exibir_chart_ascii(df, meta, label_maxlen=args.label_maxlen)


if __name__ == "__main__":
    main()

