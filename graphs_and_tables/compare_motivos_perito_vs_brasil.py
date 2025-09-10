#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Compara o percentual dos motivos de não conformidade (NC) de:
  a) um PERITO específico (vs Brasil excluindo esse perito), ou
  b) o GRUPO dos 10 piores peritos por scoreFinal (vs Brasil excluindo o grupo),

usando a DESCRIÇÃO no eixo X (texto em protocolos.motivo, com fallback para o código).

Regras IMPORTANTES:
- A definição de NC é **robusta** e considera:
    NC = (conformado = 0)  OU  (motivoNaoConformado, mesmo como TEXTO, não-vazio e CAST(...) <> 0)
  OBS: O campo protocolos.motivo é usado APENAS como DESCRIÇÃO para o gráfico/tabelas, e não
       influencia na contagem de NC.
- Compatibilidade de schema:
    * Detecta a tabela de análises: analises OU analises_atestmed
    * Usa substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
- Backend Matplotlib = Agg (gera PNG em ambiente headless)

Saídas:
--export-org, --export-md, --export-png,
--export-comment            (comentário em .md)
--export-comment-org        (comentário inserido no .org)
--chart (ASCII), --call-api

Parâmetros visuais:
--label-maxlen (abrevia rótulos do eixo X com “…”)
--label-fontsize (tamanho da fonte dos rótulos do eixo X)

Modo Top 10:
--top10            → agrupa os 10 piores (por scoreFinal) no período
--min-analises N   → mínimo de análises no período para elegibilidade ao Top 10 (padrão: 50)

Cortes (cuts) para filtrar motivos antes do Top-N:
--min-pct-perito X     → descarta motivos com % do perito < X
--min-pct-brasil X     → descarta motivos com % do Brasil (excl.) < X
--min-n-perito N       → descarta motivos com n do perito < N
--min-n-brasil N       → descarta motivos com n do Brasil (excl.) < N

Integrações Fluxo B (make_kpi_report):
--fluxo {A,B}         → B (padrão) não altera lógica aqui, apenas para compatibilidade de CLI
--peritos-csv PATH    → substitui a seleção Top 10 por uma lista explícita (coluna nomePerito)
--scope-csv PATH      → limita o universo (Brasil e LHS) aos peritos do CSV (coluna nomePerito)

Exemplo:
python3 graphs_and_tables/compare_nc_rate.py \
  --start 2025-07-01 --end 2025-07-31 \
  --perito "CASSIO DE AZEVEDO MARQUES FILHO" \
  --export-png --export-org --export-comment-org
"""

import os
import sys
import re
import json
from typing import Dict, Any, Tuple, Optional, List

# permitir imports de utils/*
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import sqlite3
import argparse
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ────────────────────────────────────────────────────────────────────────────────
# plotext (compat: multi_bar vs multiple_bar; label vs labels)
# ────────────────────────────────────────────────────────────────────────────────
try:
    import plotext as p
except Exception:
    p = None
    def px_multi_bar(*args, **kwargs):
        raise RuntimeError("plotext indisponível (p=None)")
    def px_build():
        return ""
else:
    _mb  = getattr(p, "multi_bar", None)
    _mb2 = getattr(p, "multiple_bar", None)

    def px_multi_bar(x, ys, labels=None, **kw):
        f = _mb or _mb2
        if f is None:
            raise AttributeError("plotext sem multi_bar/multiple_bar")
        try:
            return f(x, ys, labels=labels, **kw)   # versões novas (labels=)
        except TypeError:
            return f(x, ys, label=labels, **kw)    # versões antigas (label=)

    def px_build():
        b = getattr(p, "build", None)
        return b() if callable(b) else ""

BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# Integração com comentários (GPT) — compat legado
_COMENT_FUNC = None
try:
    from utils.comentarios import comentar_motivos as _COMENT_FUNC  # preferido
except Exception:
    try:
        from utils.comentarios import comentar_motivos_perito_vs_brasil as _COMENT_FUNC  # compat antigo
    except Exception:
        _COMENT_FUNC = None

# ────────────────────────────────────────────────────────────────────────────────
# Helpers OpenAI (API direta, sem parse de tabela)
# ────────────────────────────────────────────────────────────────────────────────
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

def _sanitize_paragraph(text: str, max_words: int = 180) -> str:
    if not text:
        return ""
    text = re.sub(r"^```.*?$", "", text, flags=re.M)
    text = re.sub(r"^~~~.*?$", "", text, flags=re.M)
    kept = []
    for ln in text.splitlines():
        t = ln.strip()
        if not t: continue
        if t.startswith("[") and t.endswith("]"): continue
        if t.startswith("|"): continue
        if t.startswith("#+"): continue
        kept.append(ln)
    text = " ".join(" ".join(kept).split())
    words = text.split()
    if len(words) > max_words:
        text = " ".join(words[:max_words]).rstrip() + "…"
    return text

def _build_motivos_messages(df: pd.DataFrame, meta: Dict[str, Any], cuts: Dict[str, Any],
                            max_words: int) -> List[Dict[str, str]]:
    rows = [
        {
            "descricao": str(r["descricao"]),
            "pct_lhs": float(r["pct_perito"]),
            "pct_rhs": float(r["pct_brasil"]),
            "n_lhs": int(r["n_perito"]),
            "n_rhs": int(r["n_brasil"]),
            "diff_pp": float(r["pct_perito"] - r["pct_brasil"]),
        }
        for _, r in df.iterrows()
    ]
    payload = {
        "periodo": {"inicio": meta["start"], "fim": meta["end"]},
        "lhs_label": meta["label_lhs"],
        "rhs_label": meta["label_rhs"],
        "totais_nc": {"lhs": int(meta["total_p"]), "rhs": int(meta["total_b"])},
        "taxas_nc": {"lhs": float(meta["nc_rate_p"]), "rhs": float(meta["nc_rate_b"])},
        "cuts": {k: v for k, v in (cuts or {}).items() if v is not None},
        "rows": rows,
        "mode": meta.get("mode", "single"),
    }
    system = "Você é um analista de dados do ATESTMED. Escreva comentários claros, objetivos e tecnicamente corretos."
    user = (
        "Escreva um único parágrafo (máx. {mw} palavras) interpretando a comparação de motivos de NC "
        "entre dois grupos. Inclua: (1) leitura direta; (2) 2–3 maiores diferenças em pontos percentuais; "
        "(3) referência aos denominadores (n) e às taxas de NC gerais; (4) ressalva de que a análise é descritiva. "
        "Evite jargões e causalidade. Dados em JSON:\n\n{json}"
    ).format(mw=max_words, json=json.dumps(payload, ensure_ascii=False))
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]

def _heuristic_comment_motivos(df: pd.DataFrame, meta: Dict[str, Any], cuts: Dict[str, Any], max_words: int = 180) -> str:
    if df.empty:
        return "Sem dados suficientes para um comentário interpretativo."
    lhs = meta["label_lhs"]; rhs = meta["label_rhs"]
    ini, fim = meta["start"], meta["end"]
    # top diferenças absolutas
    m = df.copy()
    m["diff"] = (m["pct_perito"] - m["pct_brasil"]).astype(float)
    m = m.sort_values("diff", key=lambda s: s.abs(), ascending=False)
    top_pos = m[m["diff"] > 0].head(2)
    top_neg = m[m["diff"] < 0].head(2)
    pos_txt = ", ".join(f"{r['descricao']} (+{r['diff']:.1f} p.p.)" for _, r in top_pos.iterrows()) or "—"
    neg_txt = ", ".join(f"{r['descricao']} ({r['diff']:.1f} p.p.)" for _, r in top_neg.iterrows()) or "—"
    txt = (
        f"No período {ini} a {fim}, entre os registros não conformes, {lhs} apresentou distribuição por motivos "
        f"com destaque (vs {rhs}) para: {pos_txt}; por outro lado, motivos com menor participação foram: {neg_txt}. "
        f"No total de NC, {lhs} = {int(meta['total_p'])} (taxa {meta['nc_rate_p']:.1f}%), "
        f"{rhs} = {int(meta['total_b'])} (taxa {meta['nc_rate_b']:.1f}%). "
        "Os resultados são descritivos e podem refletir o mix de casos e volume amostral; diferenças não implicam causalidade."
    )
    return _sanitize_paragraph(txt, max_words=max_words)

def _comment_from_values_motivos(df: pd.DataFrame, meta: Dict[str, Any], cuts: Dict[str, Any],
                                 *, call_api: bool, model: str = "gpt-4o-mini",
                                 max_words: int = 180, temperature: float = 0.2) -> str:
    """Gera comentário exclusivamente a partir dos VALORES (API direta → heurístico)."""
    if df.empty:
        return "Sem dados suficientes para um comentário interpretativo."
    if call_api:
        try:
            _load_openai_key_from_dotenv(os.path.join(BASE_DIR, ".env"))
            messages = _build_motivos_messages(df, meta, cuts or {}, max_words)
            api_txt = _call_openai_chat(messages, model=model, temperature=temperature)
            if api_txt:
                return _sanitize_paragraph(api_txt, max_words=max_words)
        except Exception:
            pass
    # fallback heurístico
    return _heuristic_comment_motivos(df, meta, cuts or {}, max_words=max_words)

# ============================
# Helpers de schema
# ============================

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
    """
    Detecta tabela de análises e colunas necessárias.
    Exige: siapePerito, dataHoraIniPericia
    Usa: motivoNaoConformado (se existir) e conformado (se existir) para a regra robusta de NC.
    Protocolo é opcional (usado para pegar protocolos.motivo como descrição).
    """
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
        'motivo_col': motivo_col,                # pode ser None
        'has_conformado': has_conformado,        # True/False
        'date_col': 'dataHoraIniPericia',
        'has_protocolo': has_protocolo,
        'has_protocolos_table': has_protocolos,
        'has_indicadores': has_indicadores,
    }

# ============================
# Núcleo (queries e cálculos)
# ============================

def _fetch_df(conn: sqlite3.Connection, sql: str, params: Tuple) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)

def _get_counts_single(conn: sqlite3.Connection, start: str, end: str, perito: str, schema: Dict[str, Any],
                       scope_upper: Optional[List[str]] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retorna (df_perito, df_brasil_excl) com colunas: ['descricao', 'n'].
    Descrição: COALESCE(NULLIF(TRIM(pr.motivo), ''), CAST(código AS TEXT))
    Critério de NC (regra robusta):
      - conformado = 0  OR
      - motivoNaoConformado (texto) != '' E CAST(...) <> 0
    """
    t            = schema['table']
    motivo_col   = schema['motivo_col']    # pode ser None
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

    # cláusula opcional de ESCOPO
    scope_clause = ""
    if scope_upper:
        placeholders_scope = ",".join(["?"] * len(scope_upper))
        scope_clause = f" AND TRIM(UPPER(p.nomePerito)) IN ({placeholders_scope}) "

    base_select = f"""
        SELECT {desc_expr},
               COUNT(*) AS n
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE TRIM(UPPER(p.nomePerito)) {{cmp}}
           {scope_clause}
           AND substr(a.{date_col},1,10) BETWEEN ? AND ?
           AND ( {cond_nc_total} )
         GROUP BY descricao
    """

    q_perito = base_select.format(cmp="= TRIM(UPPER(?))")
    q_outros = base_select.format(cmp="<> TRIM(UPPER(?))")

    params_common: List[Any] = []
    if scope_upper:
        params_common.extend(scope_upper)
    params_tail = [start, end]

    df_p = _fetch_df(conn, q_perito, tuple([perito] + params_common + params_tail))
    df_b = _fetch_df(conn, q_outros, tuple([perito] + params_common + params_tail))

    if not df_p.empty:
        df_p['descricao'] = df_p['descricao'].astype(str).str.strip()
    if not df_b.empty:
        df_b['descricao'] = df_b['descricao'].astype(str).str.strip()

    return df_p, df_b

def _get_counts_group(conn: sqlite3.Connection, start: str, end: str, peritos: List[str], schema: Dict[str, Any],
                      scope_upper: Optional[List[str]] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Agrega motivos do grupo de peritos (Top10 ou lista CSV) vs Brasil (excluindo o grupo).
    Retorna (df_grupo, df_brasil_excl) com ['descricao', 'n'].
    """
    if not peritos:
        return pd.DataFrame(columns=['descricao', 'n']), pd.DataFrame(columns=['descricao', 'n'])

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

    scope_clause = ""
    if scope_upper:
        placeholders_scope = ",".join(["?"] * len(scope_upper))
        scope_clause = f" AND TRIM(UPPER(p.nomePerito)) IN ({placeholders_scope}) "

    base_select = f"""
        SELECT {desc_expr},
               COUNT(*) AS n
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE TRIM(UPPER(p.nomePerito)) {{cmp}}
           {scope_clause}
           AND substr(a.{date_col},1,10) BETWEEN ? AND ?
           AND ( {cond_nc_total} )
         GROUP BY descricao
    """

    q_grp = base_select.format(cmp=where_in)
    q_out = base_select.format(cmp=where_out)

    peritos_upper = [pname.strip().upper() for pname in peritos]
    params_grp = tuple(peritos_upper) + tuple(scope_upper or []) + (start, end)
    params_out = tuple(peritos_upper) + tuple(scope_upper or []) + (start, end)

    df_g = _fetch_df(conn, q_grp, params_grp)
    df_b = _fetch_df(conn, q_out, params_out)

    if not df_g.empty: df_g['descricao'] = df_g['descricao'].astype(str).str.strip()
    if not df_b.empty: df_b['descricao'] = df_b['descricao'].astype(str).str.strip()
    return df_g, df_b

def _get_nc_rates_single(conn: sqlite3.Connection, start: str, end: str, perito: str, schema: Dict[str, Any],
                         scope_upper: Optional[List[str]] = None) -> Tuple[float, float]:
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

    scope_clause = ""
    if scope_upper:
        placeholders_scope = ",".join(["?"] * len(scope_upper))
        scope_clause = f" AND TRIM(UPPER(p.nomePerito)) IN ({placeholders_scope}) "

    q_base = f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN {cond_nc_total} THEN 1 ELSE 0 END) AS nc
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE TRIM(UPPER(p.nomePerito)) {{cmp}} TRIM(UPPER(?))
           {scope_clause}
           AND substr(a.{date_col},1,10) BETWEEN ? AND ?
    """
    params_common: List[Any] = []
    if scope_upper:
        params_common.extend(scope_upper)

    row_p = conn.execute(q_base.format(cmp="="), tuple([perito] + params_common + [start, end])).fetchone()
    total_p = int(row_p[0] or 0); nc_p = int(row_p[1] or 0)
    rate_p  = (nc_p / total_p * 100.0) if total_p > 0 else 0.0

    row_b = conn.execute(q_base.format(cmp="<>"), tuple([perito] + params_common + [start, end])).fetchone()
    total_b = int(row_b[0] or 0); nc_b = int(row_b[1] or 0)
    rate_b  = (nc_b / total_b * 100.0) if total_b > 0 else 0.0
    return rate_p, rate_b

def _get_nc_rates_group(conn: sqlite3.Connection, start: str, end: str, peritos: List[str], schema: Dict[str, Any],
                        scope_upper: Optional[List[str]] = None) -> Tuple[float, float]:
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

    scope_clause = ""
    if scope_upper:
        placeholders_scope = ",".join(["?"] * len(scope_upper))
        scope_clause = f" AND TRIM(UPPER(p.nomePerito)) IN ({placeholders_scope}) "

    q_grp = f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN {cond_nc_total} THEN 1 ELSE 0 END) AS nc
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE TRIM(UPPER(p.nomePerito)) {where_in}
           {scope_clause}
           AND substr(a.{date_col},1,10) BETWEEN ? AND ?
    """
    q_out = f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN {cond_nc_total} THEN 1 ELSE 0 END) AS nc
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE TRIM(UPPER(p.nomePerito)) {where_out}
           {scope_clause}
           AND substr(a.{date_col},1,10) BETWEEN ? AND ?
    """
    peritos_upper = [p.strip().upper() for p in peritos]
    row_g = conn.execute(q_grp, tuple(peritos_upper) + tuple(scope_upper or []) + (start, end)).fetchone()
    row_b = conn.execute(q_out, tuple(peritos_upper) + tuple(scope_upper or []) + (start, end)).fetchone()

    total_g = int(row_g[0] or 0); nc_g = int(row_g[1] or 0)
    total_b = int(row_b[0] or 0); nc_b = int(row_b[1] or 0)

    rate_g  = (nc_g / total_g * 100.0) if total_g > 0 else 0.0
    rate_b  = (nc_b / total_b * 100.0) if total_b > 0 else 0.0
    return rate_g, rate_b

def _get_top10_peritos(conn: sqlite3.Connection, start: str, end: str, min_analises: int, schema: Dict[str, Any]) -> List[str]:
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

def _build_comparativo_single(start: str, end: str, perito: str, topn: int = 10,
                              scope_upper: Optional[List[str]] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        schema = _detect_schema(conn)
        df_p, df_b = _get_counts_single(conn, start, end, perito, schema, scope_upper=scope_upper)

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

        nc_rate_p, nc_rate_b = _get_nc_rates_single(conn, start, end, perito, schema, scope_upper=scope_upper)

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
        'label_rhs': 'Brasil (excl.)' if not scope_upper else 'Brasil (excl., escopo)',
        'safe_stub': perito,
    }
    return df, meta

def _build_comparativo_top10(start: str, end: str, topn: int = 10, min_analises: int = 50,
                             peritos_csv_upper: Optional[List[str]] = None,
                             scope_upper: Optional[List[str]] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        schema = _detect_schema(conn)
        if peritos_csv_upper:
            peritos = peritos_csv_upper
        else:
            peritos = _get_top10_peritos(conn, start, end, min_analises, schema)
        if not peritos:
            return pd.DataFrame(columns=['descricao','n_brasil','n_perito','pct_brasil','pct_perito']), {
                'mode': 'top10', 'peritos_lista': [], 'start': start, 'end': end,
                'total_p': 0, 'total_b': 0, 'nc_rate_p': 0.0, 'nc_rate_b': 0.0,
                'label_lhs': 'Top 10 piores', 'label_rhs': ('Brasil (excl.)' if not scope_upper else 'Brasil (excl., escopo)'), 'safe_stub': 'Top 10 piores'
            }

        df_g, df_b = _get_counts_group(conn, start, end, peritos, schema, scope_upper=scope_upper)

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

        nc_rate_g, nc_rate_b = _get_nc_rates_group(conn, start, end, peritos, schema, scope_upper=scope_upper)

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
        'label_rhs': 'Brasil (excl.)' if not scope_upper else 'Brasil (excl., escopo)',
        'safe_stub': 'Top 10 piores',
    }
    return df, meta

# ============================
# Filtros (cuts) e Top-N
# ============================

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

# ============================
# Exportações e gráficos
# ============================

def _safe_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name).strip("_")

def _abbrev(s: str, maxlen: int) -> str:
    s = (str(s) or "").strip()
    return s if len(s) <= maxlen else s[:max(1, maxlen - 1)] + "…"

def _ascii_label(s: str, maxlen: int = 18) -> str:
    return _abbrev(s, maxlen=maxlen)

def _build_motivos_payload(df: pd.DataFrame, meta: Dict[str, Any], cuts: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "period": (meta['start'], meta['end']),
        "nc_rate": {"lhs": meta.get("nc_rate_p"), "rhs": meta.get("nc_rate_b")},
        "rows": [
            {
                "descricao": str(r['descricao']),
                "pct_perito": float(r['pct_perito']),
                "pct_brasil": float(r['pct_brasil']),
                "n_perito": int(r['n_perito']),
                "n_brasil": int(r['n_brasil']),
            }
            for _, r in df.iterrows()
        ],
        "mode": meta.get("mode", "single"),
        "meta": {
            "lhs_label": meta.get("label_lhs", "Grupo"),
            "rhs_label": meta.get("label_rhs", "Brasil (excl.)"),
            "peritos_lista": meta.get("peritos_lista", []),
            "cuts": cuts or {},
        },
    }

def gerar_comentario(df: pd.DataFrame, meta: Dict[str, Any], cuts: Optional[Dict[str, Any]], call_api: bool) -> str:
    """
    Novo fluxo preferencial: gera comentário a partir dos VALORES (API direta → heurístico).
    Mantém compatibilidade com utils.comentarios como fallback.
    """
    if df.empty:
        return "Sem dados suficientes para um comentário interpretativo."
    # 1) Caminho novo (valores)
    txt = _comment_from_values_motivos(
        df, meta, cuts or {},
        call_api=call_api,
        model="gpt-4o-mini",
        max_words=180,
        temperature=0.2
    )
    if txt:
        return txt
    # 2) Fallback compat
    if _COMENT_FUNC is None:
        return "Comentário não disponível no momento."
    payload = _build_motivos_payload(df, meta, cuts or {})
    try:
        out = _COMENT_FUNC(payload, call_api=call_api)
        if isinstance(out, dict):
            return (out.get("comment") or out.get("prompt") or "").strip()
        if isinstance(out, str):
            return out.strip()
        return str(out).strip()
    except Exception:
        return "Comentário não disponível no momento."

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

def exportar_org(df: pd.DataFrame, meta: Dict[str, Any], cuts: Dict[str, Any], png_path: Optional[str], comment_text: Optional[str] = None) -> str:
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

    lines.append("| Motivo (descrição) | % " + meta['label_lhs'] + " | % " + meta['label_rhs'] +
                 " | n " + meta['label_lhs'] + " | n " + meta['label_rhs'] + " |")
    lines.append("|-")
    if not df.empty:
        for _, r in df.iterrows():
            lines.append(f"| {r['descricao']} | {r['pct_perito']:.2f}% | {r['pct_brasil']:.2f}% | {int(r['n_perito'])} | {int(r['n_brasil'])} |")
    else:
        lines.append("| — | — | — | — | — |")

    if png_path and os.path.exists(png_path):
        rel_png = os.path.basename(png_path)
        lines.append("\n#+CAPTION: Distribuição percentual dos motivos (barras lado a lado).")
        lines.append(f"[[file:{rel_png}]]")

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
    px_multi_bar(
        motivos,
        [df['pct_perito'].tolist(), df['pct_brasil'].tolist()],
        labels=[label_lhs, label_rhs]
    )
    p.title("Motivos de NC – Top 10 piores vs Brasil (excl.)" if meta['mode']=='top10'
            else f"Motivos de NC – {meta['label_lhs']} vs {meta['label_rhs']}")
    p.xlabel("Motivo"); p.ylabel("% nos NC")
    p.plotsize(100, 20)
    p.show()

def exportar_comment(df: pd.DataFrame, meta: Dict[str, Any], cuts: Optional[Dict[str, Any]] = None, call_api: bool = False) -> str:
    """
    Gera e salva comentário (ou prompt) sobre os motivos **em .md**.
    Agora usa preferencialmente a rota por VALORES (API direta → heurístico).
    """
    safe = _safe_name(meta['safe_stub'])
    fname = "motivos_top10_vs_brasil_comment.md" if meta['mode']=='top10' else f"motivos_perito_vs_brasil_{safe}_comment.md"
    path = os.path.join(EXPORT_DIR, fname)

    comment_text = gerar_comentario(df, meta, cuts, call_api=call_api)

    header_tbl = [
        "| Motivo (descrição) | % " + meta['label_lhs'] + " | % " + meta['label_rhs'] + " | n " + meta['label_lhs'] + " | n " + meta['label_rhs'] + " |",
        "|--------------------|---------:|---------:|---------:|---------:|"
    ]
    for _, r in df.iterrows():
        header_tbl.append(
            f"| {r['descricao']} | {r['pct_perito']:.2f}% | {r['pct_brasil']:.2f}% | {int(r['n_perito'])} | {int(r['n_brasil'])} |"
        )
    tabela_md = "\n".join(header_tbl)

    md_out = []
    md_out.append(f"**Período:** {meta['start']} a {meta['end']}")
    md_out.append(f"**Comparação:** {meta['label_lhs']} vs {meta['label_rhs']}")
    md_out.append("\n### Tabela\n")
    md_out.append(tabela_md)
    md_out.append("\n### Comentário\n")
    md_out.append(comment_text.strip() if comment_text else "(sem comentário)")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_out))
    print(f"✅ Comentário salvo em: {path}")
    return path

# ============================
# CLI
# ============================

def _load_names_csv(path: Optional[str]) -> Optional[List[str]]:
    """Lê um CSV com coluna 'nomePerito' e retorna lista de nomes (ou None)."""
    if not path:
        return None
    try:
        df = pd.read_csv(path)
        col = next((c for c in df.columns if c.strip().lower() == "nomeperito"), None)
        if not col:
            return None
        names = (
            df[col]
            .astype(str)
            .map(lambda s: s.strip())
            .replace({"": None})
            .dropna()
            .tolist()
        )
        return names or None
    except Exception:
        return None

def _to_upper_list(xs: Optional[List[str]]) -> Optional[List[str]]:
    return [x.strip().upper() for x in xs] if xs else None

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Compara a porcentagem dos motivos de NC: perito (ou Top 10 piores por scoreFinal) "
            "vs Brasil (excluindo o(s) perito(s)) no período (X = descrição)."
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

    # cuts (sem % no texto do help)
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
                    help='Exporta comentário em .md (legado)')
    ap.add_argument('--export-comment-org', action='store_true',
                    help='Insere o comentário diretamente no .org gerado')

    # chamar API de comentários (usa OPENAI_API_KEY se disponível)
    ap.add_argument('--call-api', action='store_true',
                    help='Usa a API (se disponível) para gerar o comentário')

    # Integração com make_kpi_report (Fluxo B)
    ap.add_argument('--fluxo', choices=['A', 'B'], default='B',
                    help="Fluxo do relatório (A ou B). Padrão: B.")
    ap.add_argument('--peritos-csv', default=None,
                    help="CSV com nomes de peritos (coluna 'nomePerito') para definir o grupo explicitamente.")
    ap.add_argument('--scope-csv', default=None,
                    help="CSV com peritos (coluna 'nomePerito') que definem o ESCOPO (coorte) da base no período.")

    return ap.parse_args()

def main() -> None:
    args = parse_args()
    call_api = bool(args.call_api or os.getenv("OPENAI_API_KEY"))

    # CSVs opcionais
    peritos_csv = _load_names_csv(args.peritos_csv)
    scope_csv   = _load_names_csv(args.scope_csv)
    peritos_csv_upper = _to_upper_list(peritos_csv)
    scope_upper       = _to_upper_list(scope_csv)

    # monta DF base (sem filtros)
    if args.top10 or peritos_csv_upper:
        # Se peritos-csv vier, usa esse grupo (independe de --top10)
        df, meta = _build_comparativo_top10(
            args.start, args.end,
            args.topn, args.min_analises,
            peritos_csv_upper=peritos_csv_upper,
            scope_upper=scope_upper
        )
    else:
        df, meta = _build_comparativo_single(
            args.start, args.end, args.perito, args.topn,
            scope_upper=scope_upper
        )

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

    # Exports
    png_path = None
    if args.export_png or args.export_org or args.export_comment_org:
        png_path = exportar_png(df, meta, label_maxlen=args.label_maxlen, label_fontsize=args.label_fontsize)

    if args.export_md:
        exportar_md(df, meta, cuts_info)

    # Comentário (string) — usado tanto no .md quanto no .org
    comment_for_org = None
    if args.export_comment:
        exportar_comment(df, meta, cuts_info, call_api=call_api)
    if args.export_comment_org:
        comment_for_org = gerar_comentario(df, meta, cuts_info, call_api=call_api)

    if args.export_org or args.export_comment_org:
        exportar_org(df, meta, cuts_info, png_path, comment_text=comment_for_org)

    if args.chart:
        exibir_chart_ascii(df, meta, label_maxlen=args.label_maxlen)

if __name__ == "__main__":
    main()

