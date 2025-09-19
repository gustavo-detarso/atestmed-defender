#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Relatório ATESTMED — Individual ou Top 10
Executa a geração de gráficos (Python), análises (R), junta comentários e
monta relatórios em Org/PDF. Inclui panorama weekday→weekend e apêndices.
"""

# ────────────────────────────────────────────────────────────────────────────────
# Imports (todos consolidados no início)
# ────────────────────────────────────────────────────────────────────────────────
import os
import sys
import re
import csv
import shutil
import sqlite3
import subprocess
import tempfile
import time
import calendar
import shlex
import numpy as np
from glob import glob
from datetime import datetime, date
from collections import defaultdict
from typing import Optional, Set, Dict, List, Tuple, Any
from argparse import BooleanOptionalAction

import pandas as pd
from PyPDF2 import PdfMerger

# ────────────────────────────────────────────────────────────────────────────────
# Paths e diretórios
# ────────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────
# Layout compatível com make_kpi_report.py (reports/outputs)
# ─────────────────────────────────────────────────────────
BASE_DIR     = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
OUTPUTS_DIR  = os.path.join(BASE_DIR, 'reports', 'outputs')
os.makedirs(OUTPUTS_DIR, exist_ok=True)

# pasta do período: reports/outputs/START_a_END/
PERIODO_DIR  = os.path.join(OUTPUTS_DIR, f"{args.start}_a_{args.end}")
os.makedirs(PERIODO_DIR, exist_ok=True)

# para o fluxo B, criamos um sub-rótulo fixo
RELATORIO_DIR = os.path.join(PERIODO_DIR, "fluxo_b")

# subpastas espelhando o make_kpi_report.py
IMGS_DIR      = os.path.join(RELATORIO_DIR, "imgs")
COMMENTS_DIR  = os.path.join(RELATORIO_DIR, "comments")
ORGS_DIR      = os.path.join(RELATORIO_DIR, "orgs")
MARKDOWN_DIR  = os.path.join(RELATORIO_DIR, "markdown")
PDF_DIR       = os.path.join(RELATORIO_DIR, "pdf")

for d in (RELATORIO_DIR, IMGS_DIR, COMMENTS_DIR, ORGS_DIR, MARKDOWN_DIR, PDF_DIR):
    os.makedirs(d, exist_ok=True)

# Redireciona as saídas do fluxo B:
#  - CSVs/PNGs (que o script grava em args.out_dir) → imgs/
#  - .org → orgs/
#  - .pdf → pdf/
# Mantém flags do usuário se ele passou caminhos explícitos diferentes (heurística: apenas
# sobrescrever quando os caminhos atuais ainda são os defaults da própria ferramenta).
def _is_default(p: str, defaults: set[str]) -> bool:
    try:
        ap = os.path.abspath(p)
        return any(os.path.abspath(x) == ap for x in defaults)
    except Exception:
        return False

defaults_out = {
    os.path.join(BASE_DIR, 'graphs_and_tables', 'exports'),
}
defaults_org = {
    os.path.join(BASE_DIR, 'graphs_and_tables', 'exports'),
}
defaults_pdf = {
    os.path.join(BASE_DIR, 'graphs_and_tables', 'exports', 'pdf'),
}

if _is_default(getattr(args, "out_dir", ""), defaults_out):
    args.out_dir = IMGS_DIR
if _is_default(getattr(args, "org_dir", ""), defaults_org):
    args.org_dir = ORGS_DIR
if _is_default(getattr(args, "pdf_dir", ""), defaults_pdf):
    args.pdf_dir = PDF_DIR

# opcional: expor para outras partes do script se quiser reutilizar
args.relatorio_dir = RELATORIO_DIR
args.periodo_dir   = PERIODO_DIR
args.imgs_dir      = IMGS_DIR
args.comments_dir  = COMMENTS_DIR
args.markdown_dir  = MARKDOWN_DIR

# ────────────────────────────────────────────────────────────────────────────────
# Carregamento de .env na raiz (se existir)
# ────────────────────────────────────────────────────────────────────────────────
def _load_env_from_root():
    """Carrega variáveis do arquivo .env na raiz do projeto (sem sobrescrever o ambiente)."""
    env_path = os.path.join(BASE_DIR, ".env")
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path, override=False)
        return
    except Exception:
        pass
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                v = v.strip().strip('"').strip("'")
                if v:
                    os.environ.setdefault(k.strip(), v)

_load_env_from_root()
if not os.getenv("OPENAI_API_KEY"):
    print("⚠️  OPENAI_API_KEY não encontrado no ambiente (.env na raiz não carregou ou não tem a chave).")

# ────────────────────────────────────────────────────────────────────────────────
# Ordem padrão dos scripts Python e configurações
# ────────────────────────────────────────────────────────────────────────────────
SCRIPT_ORDER = [
    "compare_nc_rate.py",
    "compare_motivos_perito_vs_brasil.py",
    "compare_productivity.py",
    "compare_fifteen_seconds.py",
    "compare_overlap.py",
    "compare_indicadores_composto.py",
]

DEFAULT_MODES = {
    "compare_productivity.py": ["perito-share", "task-share", "time-share"],
    "compare_overlap.py":      ["perito-share", "task-share", "time-share"],
}

EXTRA_ARGS = {
    "compare_motivos_perito_vs_brasil.py": ["--label-maxlen", "14", "--label-fontsize", "7"],
}

PRODUCTIVITY_THRESHOLD = "50"  # análises/h
FIFTEEN_THRESHOLD      = "15"  # segundos
FIFTEEN_CUT_N          = "10"  # mínimo de análises ≤ threshold

# ────────────────────────────────────────────────────────────────────────────────
# Scripts globais (por período; independem de perito/top10)
# ────────────────────────────────────────────────────────────────────────────────
GLOBAL_SCRIPTS = [
    "g_weekday_to_weekend_table.py",
]

def build_commands_for_global(script_file: str, start: str, end: str) -> list:
    """Monta comando para scripts globais que só precisam de --start/--end."""
    return [[
        sys.executable, script_file,
        "--db", DB_PATH,
        "--start", start,
        "--end", end,
        "--out-dir", EXPORT_DIR,
        "--export-org",
        "--export-png",
        "--export-protocols",
    ]]

# ────────────────────────────────────────────────────────────────────────────────
# R checks — individuais e top10
# ────────────────────────────────────────────────────────────────────────────────
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

# Comentário ChatGPT para apêndice R (opcional)
try:
    from utils.comentarios import comentar_r_apendice
except Exception:
    comentar_r_apendice = None

# ────────────────────────────────────────────────────────────────────────────────
# Conhecimento explícito das flags dos scripts Python
# ────────────────────────────────────────────────────────────────────────────────
ASSUME_FLAGS = {
    "compare_nc_rate.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--export-md", "--export-png", "--export-org",
        "--export-comment", "--export-comment-org", "--call-api",
        "--peritos-csv", "--scope-csv", "--fluxo",
    },
    "compare_fifteen_seconds.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--threshold", "--cut-n",
        "--export-png", "--export-org",
        "--export-comment", "--export-comment-org", "--call-api",
        "--peritos-csv", "--scope-csv", "--fluxo",
    },
    "compare_overlap.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--mode", "--chart",
        "--export-md", "--export-png", "--export-org",
        "--export-comment", "--export-comment-org", "--call-api",
        "--peritos-csv", "--scope-csv", "--fluxo",
    },
    "compare_productivity.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--threshold", "--mode", "--chart",
        "--export-md", "--export-png", "--export-org",
        "--export-comment", "--export-comment-org", "--call-api",
        "--peritos-csv", "--scope-csv", "--fluxo",
    },
    "compare_indicadores_composto.py": {
        "--perito", "--top10", "--min-analises",
        "--alvo-prod", "--cut-prod-pct", "--cut-nc-pct", "--cut-le15s-pct", "--cut-overlap-pct",
        "--export-png", "--export-org", "--chart",
        "--export-comment", "--export-comment-org", "--call-api",
        "--peritos-csv", "--scope-csv", "--fluxo",
    },
    "compare_motivos_perito_vs_brasil.py": {
        "--perito", "--top10", "--min-analises",
        "--topn", "--min-pct-perito", "--min-pct-brasil", "--min-n-perito", "--min-n-brasil",
        "--label-maxlen", "--label-fontsize",
        "--chart", "--export-md", "--export-org", "--export-png",
        "--export-comment", "--export-comment-org", "--call-api",
        "--peritos-csv", "--scope-csv", "--fluxo",
    },
}

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Relatório KPI + Impacto na Fila (wrapper).")
    p.add_argument('--start', required=True)
    p.add_argument('--end',   required=True)

    who = p.add_mutually_exclusive_group(required=True)
    who.add_argument('--perito')
    who.add_argument('--top10', action='store_true')
    # NOVO: Top-K variável
    who.add_argument('--topk', type=int, help="Seleciona Top-K (ex.: --topk 15, 20, 30). Se ausente e --top10 não for passado, usa fluxo normal.")
    # NOVO: todos os que se enquadram no fluxo
    who.add_argument('--all-matching', action='store_true',
                     help="Seleciona todos os peritos que se enquadram no algoritmo do fluxo (ex.: Fluxo B = gate %NC ≥ 2× Brasil e N ≥ min).")

    p.add_argument('--min-analises', type=int, default=50)

    # flags do KPI base
    p.add_argument('--export-org', action='store_true')
    p.add_argument('--export-pdf', action='store_true')
    p.add_argument('--add-comments', action='store_true')
    p.add_argument('--r-appendix', dest='r_appendix', action='store_true', default=True)
    p.add_argument('--no-r-appendix', dest='r_appendix', action='store_false')
    p.add_argument('--include-high-nc', dest='include_high_nc', action='store_true', default=True)
    p.add_argument('--no-high-nc', dest='include_high_nc', action='store_false')
    p.add_argument('--high-nc-threshold', type=float, default=90.0)
    p.add_argument('--high-nc-min-tasks', type=int, default=50)
    p.add_argument('--r-bin', default='Rscript')
    p.add_argument('--plan-only', action='store_true', help='Apenas listar o plano de execução (dry-run) e sair.')
    p.add_argument('--fluxo', choices=['A','B'], default='B',
                   help="B (padrão): gate %NC ≥ 2× Brasil (válidas) e ranking por scoreFinal; A: ranking direto por scoreFinal.")
    # corrigindo as aspas quebradas do arquivo original
    p.add_argument('--rank-by', dest='rank_by', choices=["scoreFinal", "harm"], default=None,
                   help=("Critério de ranking p/ seleção quando não houver --peritos-csv. "
                         "Se omitido: Fluxo A => scoreFinal; Fluxo B => harm."))

    p.add_argument('--kpi-base', choices=['full', 'nc-only'], default='full',
                   help="Base para os KPI: 'full' (comportamento atual) ou 'nc-only' (recalcula score_final a partir de NC).")
    
    p.add_argument('--peritos-csv', default=None,
                   help="(Opcional) CSV com a lista de peritos a usar (nomePerito,...). Se informado, ignora seleção interna.")
    p.add_argument('--scope-csv', default=None,
                   help="(Opcional) CSV com peritos que definem o ESCOPO da base para gráficos/curvas (ex.: coorte do gate do fluxo B).")
    p.add_argument('--save-manifests', action='store_true',
                   help="Salvar CSVs com TopK e escopo (gate) usados neste run.")
    
    # impacto
    p.add_argument('--with-impact', action='store_true')
    p.add_argument('--impact-all-tests', action='store_true')

    # Reutilizar saídas já geradas pelo make_kpi_report.py
    p.add_argument('--reuse-kpi', action='store_true',
                   help='Não reexecuta o KPI base; usa o .org/imgs já existentes para montar e exportar.')
    p.add_argument('--assemble-only', dest='reuse_kpi', action='store_true',
                   help='Atalho para --reuse-kpi.')
    return p.parse_args()

# ────────────────────────────────────────────────────────────────────────────────
# Helpers de esquema/DB (mínimos para seleção TopK/All-matching)
# ────────────────────────────────────────────────────────────────────────────────
def _detect_schema(conn) -> dict:
    """
    Inspeciona as tabelas e retorna um dict com os nomes de colunas a usar.
    Espera encontrar tabelas: peritos, analises, indicadores (opcional).
    """
    def cols(tbl):
        try:
            return {r[1] for r in conn.execute(f"PRAGMA table_info({tbl})").fetchall()}
        except Exception:
            return set()

    cols_analises  = cols("analises")
    cols_peritos   = cols("peritos")
    cols_scores    = cols("indicadores")  # opcional

    # candidatos comuns nas bases ATESTMED
    ini_cands = ["dataHoraIniPericia", "dataHoraIniAnalise", "dataHoraIni"]
    fim_cands = ["dataHoraFimPericia", "dataHoraFimAnalise", "dataHoraFim"]
    ini_col = next((c for c in ini_cands if c in cols_analises), None)
    fim_col = next((c for c in fim_cands if c in cols_analises), None)

    # chaves/joins
    perito_fk = "siapePerito" if "siapePerito" in cols_analises else (
                "perito" if "perito" in cols_analises else None)

    schema = {
        "analises": {
            "table": "analises",
            "ini_col": ini_col,
            "fim_col": fim_col,
            "perito_fk": perito_fk,
            "conformado_col": "conformado" if "conformado" in cols_analises else None,
            "motivo_nc_col":  "motivoNaoConformado" if "motivoNaoConformado" in cols_analises else None,
            "protocolo_col":  "protocolo" if "protocolo" in cols_analises else None,
        },
        "peritos": {
            "table": "peritos",
            "id_col": "siapePerito" if "siapePerito" in cols_peritos else None,
            "nome_col": "nomePerito" if "nomePerito" in cols_peritos else None,
            "cr_col": "cr" if "cr" in cols_peritos else None,
            "dr_col": "dr" if "dr" in cols_peritos else None,
        },
        "indicadores": {
            "table": "indicadores" if cols_scores else None,
            "perito_fk": "perito" if "perito" in cols_scores else (
                         "siapePerito" if "siapePerito" in cols_scores else None),
            "score_col": "scoreFinal" if "scoreFinal" in cols_scores else None,
            "harm_col":  "harm" if "harm" in cols_scores else None,
        }
    }
    return schema

def _fetch_perito_n_nc(conn, start: str, end: str, schema: dict) -> pd.DataFrame:
    a = schema["analises"]; p = schema["peritos"]
    ini, per_fk = a["ini_col"], a["perito_fk"]
    nome = p["nome_col"]; pid = p["id_col"]
    if not ini or not per_fk or not nome or not pid:
        return pd.DataFrame(columns=["nomePerito","N","NC","cr","dr"])

    # regra de NC robusta: (conformado=0) OU (motivoNaoConformado != '' e != 0)
    conf = a["conformado_col"]; motivo = a["motivo_nc_col"]
    nc_expr = "0"
    if conf:
        nc_expr = f"CASE WHEN CAST(IFNULL(a.{conf},1) AS INTEGER)=0 THEN 1 ELSE 0 END"
    if motivo:
        # soma às condições
        nc_expr = f"""( {nc_expr} 
                        OR (TRIM(IFNULL(a.{motivo},'')) <> '' 
                            AND CAST(IFNULL(a.{motivo},'0') AS INTEGER) <> 0) )"""

    sql = f"""
        SELECT 
            p.{nome}  AS nomePerito,
            p.{p.get('cr_col','cr') or 'cr'} AS cr,
            p.{p.get('dr_col','dr') or 'dr'} AS dr,
            COUNT(*)  AS N,
            SUM(CASE WHEN {nc_expr} THEN 1 ELSE 0 END) AS NC
        FROM analises a
        JOIN peritos  p ON a.{per_fk} = p.{pid}
        WHERE date(a.{ini}) BETWEEN ? AND ?
        GROUP BY p.{nome}, p.{p.get('cr_col','cr') or 'cr'}, p.{p.get('dr_col','dr') or 'dr'}
    """
    try:
        df = pd.read_sql(sql, conn, params=(start, end))
    except Exception:
        return pd.DataFrame(columns=["nomePerito","N","NC","cr","dr"])

    # normaliza colunas
    for c in ["cr","dr"]:
        if c not in df.columns: df[c] = "-"
    return df[["nomePerito","N","NC","cr","dr"]]

def _compute_p_br_and_totals(conn, start: str, end: str, schema: dict) -> tuple[float,int,int]:
    a = schema["analises"]; ini = a["ini_col"]
    if not ini:
        return 0.0, 0, 0
    conf = a["conformado_col"]; motivo = a["motivo_nc_col"]
    nc_expr = "0"
    if conf:
        nc_expr = f"CASE WHEN CAST(IFNULL({conf},1) AS INTEGER)=0 THEN 1 ELSE 0 END"
    if motivo:
        nc_expr = f"( {nc_expr} OR (TRIM(IFNULL({motivo},'')) <> '' AND CAST(IFNULL({motivo},'0') AS INTEGER) <> 0) )"
    sql = f"""
        SELECT COUNT(*) AS N, SUM(CASE WHEN {nc_expr} THEN 1 ELSE 0 END) AS NC
        FROM analises WHERE date({ini}) BETWEEN ? AND ?
    """
    N, NC = 0, 0
    try:
        row = conn.execute(sql, (start, end)).fetchone()
        if row:
            N = int(row[0] or 0); NC = int(row[1] or 0)
    except Exception:
        pass
    p_br = (NC / N) if N else 0.0
    return float(p_br), N, NC

def _fetch_scores(conn, start: str, end: str, schema: dict) -> pd.DataFrame:
    t = schema["indicadores"]["table"]
    per_fk = schema["indicadores"]["perito_fk"]
    score_col = schema["indicadores"]["score_col"]
    harm_col  = schema["indicadores"]["harm_col"]
    p = schema["peritos"]; pid = p["id_col"]; nome = p["nome_col"]
    if not t or not per_fk or not score_col or not pid or not nome:
        return pd.DataFrame(columns=["nomePerito","scoreFinal","harm"])

    # Indicadores costumam não ter data; agregamos pelo período via analises.
    a = schema["analises"]; ini = a["ini_col"]
    if not ini:
        return pd.DataFrame(columns=["nomePerito","scoreFinal","harm"])
    sql = f"""
        SELECT p.{nome} AS nomePerito,
               MAX(i.{score_col}) AS scoreFinal,
               MAX({('i.'+harm_col) if harm_col else 'NULL'}) AS harm
        FROM analises a
        JOIN peritos p     ON a.{a['perito_fk']} = p.{pid}
        JOIN {t} i         ON i.{per_fk} = p.{pid}
        WHERE date(a.{ini}) BETWEEN ? AND ?
        GROUP BY p.{nome}
    """
    try:
        df = pd.read_sql(sql, conn, params=(start, end))
        if "harm" not in df.columns: df["harm"] = np.nan
        return df
    except Exception:
        return pd.DataFrame(columns=["nomePerito","scoreFinal","harm"])

def perito_tem_dados(perito: str, start: str, end: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        schema = _detect_schema(conn)
        a, p = schema["analises"], schema["peritos"]
        ini = a["ini_col"]; per_fk = a["perito_fk"]; pid = p["id_col"]; nome = p["nome_col"]
        if not all([ini, per_fk, pid, nome]):
            return False
        sql = f"""
            SELECT 1
            FROM analises a
            JOIN peritos p ON a.{per_fk}=p.{pid}
            WHERE p.{nome} = ? AND date(a.{ini}) BETWEEN ? AND ?
            LIMIT 1
        """
        row = conn.execute(sql, (perito, start, end)).fetchone()
        return bool(row)


def gerar_scope_gate_b(start: str, end: str, min_analises: int = 50, factor_nc: float = 2.0) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        schema = _detect_schema(conn)
        df_n = _fetch_perito_n_nc(conn, start, end, schema)
        if df_n.empty: return df_n
        p_br, _, _ = _compute_p_br_and_totals(conn, start, end, schema)
    df = df_n.copy()
    df["p_hat"] = df["NC"] / df["N"].replace(0, np.nan)
    gate = df.loc[(df["N"] >= int(min_analises)) & (df["p_hat"] >= float(factor_nc) * float(p_br))].copy()
    return gate[["nomePerito"]]

def _mover_markdowns_de_exports(markdown_dir: str):
    os.makedirs(markdown_dir, exist_ok=True)
    for src in glob(os.path.join(EXPORT_DIR, "*.md")):
        try:
            shutil.copy2(src, os.path.join(markdown_dir, os.path.basename(src)))
            os.remove(src)
        except Exception:
            pass

# ────────────────────────────────────────────────────────────────────────────────
# Helpers gerais
# ────────────────────────────────────────────────────────────────────────────────
def _safe(name: str) -> str:
    """Sanitiza nome para uso em arquivos/caminhos."""
    return "".join(c if c.isalnum() or c in ("-","_") else "_" for c in str(name)).strip("_") or "output"

def _env_with_project_path():
    """Garante BASE_DIR no PYTHONPATH ao chamar scripts Python filhos."""
    env = os.environ.copy()
    py = env.get("PYTHONPATH", "")
    parts = [BASE_DIR] + ([py] if py else [])
    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env

def script_path(name: str) -> str:
    """Resolve caminho absoluto de um script Python do pacote."""
    path = os.path.join(SCRIPTS_DIR, name)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Script não encontrado: {path}")
    return path

def rscript_path(name: str) -> str:
    """Resolve caminho absoluto de um script R do pacote."""
    path = os.path.join(RCHECK_DIR, name)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"R script não encontrado: {path}")
    return path

def introspect_script(script_file: str, timeout_sec: int = 6) -> dict:
    """Descobre flags e modos de um script via --help, com timeout e fallback seguro."""
    info = {"flags": set(), "modes": []}
    name = os.path.basename(script_file)

    # Fast path opcional: pule introspecção se quiser (export ATESTMED_FAST_INTROSPECT=1)
    if os.getenv("ATESTMED_FAST_INTROSPECT", "").strip() == "1":
        info["flags"] = set(ASSUME_FLAGS.get(name, []))
        info["modes"] = DEFAULT_MODES.get(name, [])
        return info

    try:
        out = subprocess.run(
            [sys.executable, script_file, "--help"],
            capture_output=True, text=True,
            env=_env_with_project_path(), cwd=SCRIPTS_DIR,
            timeout=timeout_sec
        )
        text = (out.stdout or "") + "\n" + (out.stderr or "")
        for m in re.finditer(r"(--[a-zA-Z0-9][a-zA-Z0-9\-]*)", text):
            info["flags"].add(m.group(1))
        mm = re.search(r"--mode[^\n]*\{([^}]+)\}", text)
        if mm:
            info["modes"] = [x.strip() for x in mm.group(1).split(",") if x.strip()]

        # Fallback complementar caso o --help não liste nada útil
        if not info["flags"]:
            info["flags"] = set(ASSUME_FLAGS.get(name, []))
        if not info["modes"]:
            info["modes"] = DEFAULT_MODES.get(name, [])
        return info

    except subprocess.TimeoutExpired:
        print(f"[WARN] introspect_script: timeout ({timeout_sec}s) em {name}; usando ASSUME_FLAGS/DEFAULT_MODES.")
    except Exception as e:
        print(f"[WARN] introspect_script: falha em {name}: {e}; usando ASSUME_FLAGS/DEFAULT_MODES.")

    # Fallback total
    info["flags"] = set(ASSUME_FLAGS.get(name, []))
    info["modes"] = DEFAULT_MODES.get(name, [])
    return info


def detect_modes(script_file: str, help_info: dict) -> list:
    """Retorna a lista de modos suportados pelo script (via --help ou DEFAULT_MODES)."""
    name = os.path.basename(script_file)
    modes = help_info.get("modes") or []
    return modes if modes else DEFAULT_MODES.get(name, [])
    
def _inject_comment_for_stem(stem, comments_dir, output_dir, imgs_dir=None):
    """
    Ordem de busca:
      (A) comments_dir → *.org (usa conteúdo inteiro) → *.md (converte p/ org)
      (B) EXPORT_DIR   → *.org (extrai 1º parágrafo via _extract_comment_from_org)
      (C) output_dir/orgs → *.org (extrai 1º parágrafo)
      (D) imgs_dir     → *.org (extrai 1º parágrafo)

    Retorna ['#+BEGIN_QUOTE', ..., '#+END_QUOTE'] ou [].
    """
    def _quote_block(txt):
        return ["", "#+BEGIN_QUOTE", _protect_tables_in_quote(txt.strip()), "#+END_QUOTE"]

    # (A) comments_dir — .org preferencial; depois .md
    if comments_dir and os.path.isdir(comments_dir):
        for orgc in (os.path.join(comments_dir, f"{stem}_comment.org"),
                     os.path.join(comments_dir, f"{stem}.org")):
            if os.path.exists(orgc):
                with open(orgc, encoding="utf-8") as f:
                    comment_org = f.read().strip()
                print(f"[comments] .org (comments_dir) → {orgc}")
                return _quote_block(comment_org)

        for md in (os.path.join(comments_dir, f"{stem}_comment.md"),
                   os.path.join(comments_dir, f"{stem}.md")):
            if os.path.exists(md):
                with open(md, encoding="utf-8") as f:
                    comment_md = f.read().strip()
                print(f"[comments] .md (comments_dir) → {md}")
                org_txt = markdown_para_org(comment_md)
                org_txt = "\n".join(
                    ln for ln in org_txt.splitlines()
                    if not ln.strip().lower().startswith('#+title')
                ).strip()
                return _quote_block(org_txt)

    # (B) EXPORT_DIR — .org auxiliar (ex.: produtividade_perito-share_50h_<NOME>.org)
    export_dir = globals().get("EXPORT_DIR", None)
    if export_dir and os.path.isdir(export_dir):
        for orgc in (os.path.join(export_dir, f"{stem}.org"),
                     os.path.join(export_dir, f"{stem}_comment.org")):
            if os.path.exists(orgc):
                with open(orgc, encoding="utf-8") as f:
                    aux_org = f.read()
                extra = _extract_comment_from_org(aux_org)
                if extra:
                    print(f"[comments] extraído do EXPORT_DIR → {orgc}")
                    return _quote_block(extra)

    # (C) output_dir/orgs — .org auxiliar salvo junto ao relatório
    aux_org_path = os.path.join(output_dir, "orgs", f"{stem}.org")
    if os.path.exists(aux_org_path):
        with open(aux_org_path, encoding="utf-8") as f:
            aux_org = f.read()
        extra = _extract_comment_from_org(aux_org)
        if extra:
            print(f"[comments] extraído de output_dir/orgs → {aux_org_path}")
            return _quote_block(extra)
        else:
            print(f"[comments] org auxiliar sem parágrafo extraível → {aux_org_path}")

    # (D) imgs_dir — .org ao lado do PNG (caso raro)
    if imgs_dir and os.path.isdir(imgs_dir):
        local_org = os.path.join(imgs_dir, f"{stem}.org")
        if os.path.exists(local_org):
            with open(local_org, encoding="utf-8") as f:
                aux_org = f.read()
            extra = _extract_comment_from_org(aux_org)
            if extra:
                print(f"[comments] extraído de imgs_dir → {local_org}")
                return _quote_block(extra)

    print(f"[comments] comentário não encontrado para stem='{stem}'")
    return []

def _find_impacto_dir_for_perito(periodo_dir: str, perito: str) -> str | None:
    """
    Procura .../outputs/<periodo>/impacto_fila/<PERITO>/ e retorna esse caminho.
    Usa o mesmo sanitizador _safe() para casar o nome da pasta.
    """
    root = os.path.join(periodo_dir, "impacto_fila")
    if not os.path.isdir(root):
        return None
    safe = _safe(perito)
    # tentativa direta
    cand = os.path.join(root, safe)
    if os.path.isdir(cand):
        return cand
    # varre subpastas e compara pelo _safe
    for d in glob(os.path.join(root, "*")):
        if os.path.isdir(d) and _safe(os.path.basename(d)).lower() == safe.lower():
            return d
    return None

def _copy_impacto_imgs_to_relatorio(impacto_dir: str, imgs_dir_dest: str) -> int:
    """
    Copia PNGs de .../impacto_fila/<PERITO>/exports para imgs/ do relatório.
    """
    os.makedirs(imgs_dir_dest, exist_ok=True)
    src_dir = os.path.join(impacto_dir, "exports")
    moved = 0
    if os.path.isdir(src_dir):
        for src in glob(os.path.join(src_dir, "*.png")):
            shutil.copy2(src, os.path.join(imgs_dir_dest, os.path.basename(src)))
            moved += 1
    return moved

def _append_impacto_fila_perito(lines, periodo_dir, perito, start, end, heading_level="**"):
    safe = _safe(perito)
    org_dir = os.path.join(periodo_dir, "impacto_fila", safe, "org")
    candidates = [
        os.path.join(org_dir, f"impacto_fila_{safe}_{start}_a_{end}.org"),
        os.path.join(org_dir, f"impacto_fila_{start}_a_{end}.org"),
    ]
    # fallback: qualquer arquivo de impacto desse perito no período
    if not any(os.path.exists(p) for p in candidates):
        candidates = sorted(glob(os.path.join(org_dir, f"impacto_fila*{start}_a_{end}.org")))
    for path in candidates:
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    content = f.read().strip()
                if content:
                    lines.append(f"{heading_level} Impacto na fila — {perito}")
                    content = shift_org_headings(content, delta=1)
                    content = _protect_org_text_for_pandoc(content)
                    lines.append(content)
                    lines.append("\n#+LATEX: \\newpage\n")
                    return True
            except Exception as e:
                print(f"[AVISO] Falha lendo impacto_fila para {perito}: {e}")
            break
    return False

def _append_impacto_fila_grupo(lines, periodo_dir, start, end, heading_level="**"):
    # procura um único arquivo “impacto_fila_<start>_a_<end>.org” (sem nome de perito)
    pattern = os.path.join(periodo_dir, "impacto_fila", "**", "org", f"impacto_fila_{start}_a_{end}.org")
    matches = sorted(glob(pattern, recursive=True))
    if not matches:
        return False
    path = matches[0]  # inclui só uma vez
    try:
        with open(path, encoding="utf-8") as f:
            content = f.read().strip()
        if content:
            lines.append(f"{heading_level} Impacto na fila — Grupo")
            content = shift_org_headings(content, delta=1)
            content = _protect_org_text_for_pandoc(content)
            lines.append(content)
            lines.append("\n#+LATEX: \\newpage\n")
            return True
    except Exception as e:
        print(f"[AVISO] Falha lendo impacto_fila (grupo): {e}")
    return False

def coletar_orgs_impacto(out_root: str, start: str, end: str,
                         perito: str | None = None,
                         group: str = "impacto_fila") -> list[str]:
    """
    Retorna os .org de 'Impacto na Fila' para o grupo (Top10) OU para um perito específico.

    - Se perito is None: procura apenas o impacto do GRUPO:
        {out_root}/top10/{group}/org/{group}_{start}_a_{end}.org  (exato)
        fallback: {out_root}/top10/{group}/org/{group}_*_{start}_a_{end}.org

    - Se perito for dado: procura o impacto DO PERITO:
        {out_root}/individual/{_safe(perito)}/{group}/org/{group}_{start}_a_{end}.org  (exato)
        fallback: {out_root}/individual/{_safe(perito)}/{group}/org/{group}_*_{start}_a_{end}.org

    Usa import local 'from glob import glob as _glob' para evitar conflitos
    com qualquer 'import glob' em outro ponto do arquivo.
    """
    from glob import glob as _glob
    import os

    hits: list[str] = []

    def _collect(patterns: list[str]) -> None:
        for pat in patterns:
            for p in _glob(pat):
                if os.path.isfile(p):
                    hits.append(p)

    if perito:
        safe = _safe(perito)
        base_dir = os.path.join(out_root, "individual", safe, group, "org")
        patterns = [
            os.path.join(base_dir, f"{group}_{start}_a_{end}.org"),
            os.path.join(base_dir, f"{group}_*_{start}_a_{end}.org"),
        ]
        _collect(patterns)
    else:
        base_dir = os.path.join(out_root, "top10", group, "org")
        patterns = [
            os.path.join(base_dir, f"{group}_{start}_a_{end}.org"),
            os.path.join(base_dir, f"{group}_*_{start}_a_{end}.org"),
        ]
        _collect(patterns)

    return sorted(set(hits))

def _apply_kpi_base(df_scores: pd.DataFrame, kpi_base: str = "full") -> pd.DataFrame:
    """
    Adapta o DataFrame de scores para a base de KPI escolhida.
    - full: retorna (após normalização de nomes) como veio.
    - nc-only: se houver coluna 'score_final_nc', copia para 'score_final'.
    Também normaliza possíveis colunas vindas do DB: scoreFinal → score_final.
    """
    if not isinstance(df_scores, pd.DataFrame) or df_scores.empty:
        return df_scores

    out = df_scores.copy()

    # Normalização de nomes vindos do DB/consultas
    rename_map = {}
    if "scoreFinal" in out.columns and "score_final" not in out.columns:
        rename_map["scoreFinal"] = "score_final"
    if "scoreFinal_nc" in out.columns and "score_final_nc" not in out.columns:
        rename_map["scoreFinal_nc"] = "score_final_nc"
    if rename_map:
        out = out.rename(columns=rename_map)

    if (kpi_base or "full") == "full":
        # Caso especial: só existir 'score_final_nc' → espelha em 'score_final'
        if "score_final" not in out.columns and "score_final_nc" in out.columns:
            out["score_final"] = out["score_final_nc"]
        return out

    # nc-only
    if "score_final_nc" in out.columns:
        out["score_final"] = out["score_final_nc"]
    else:
        # Fallback: mantém/gera 'score_final' e avisa
        if "score_final" not in out.columns:
            out["score_final"] = 0.0
        try:
            print("[warn] --kpi-base=nc-only sem 'score_final_nc'; usando 'score_final' existente.")
        except Exception:
            pass
    return out

def _prep_base(df_n: pd.DataFrame,
               df_scores: pd.DataFrame,
               p_br: float,
               alpha: float,
               min_analises: int = 50,
               kpi_base: str = "full") -> pd.DataFrame:
    """
    Prepara base consolidada por perito com N, NC, E, IV_vagas e score_final.
    - Filtra por N >= min_analises
    - Calcula E = max(0, NC - N*p_br), IV_vagas = ceil(alpha * E)
    - Junta score_final vindo de df_scores (ou score_final_nc se --kpi-base=nc-only)
    Observação: se df_n trouxer 'cr'/'dr', elas são preservadas.
    """
    cols_needed = {"nomePerito", "N", "NC"}
    if df_n is None or df_n.empty or not cols_needed.issubset(set(df_n.columns)):
        return pd.DataFrame(columns=["nomePerito","N","NC","cr","dr","E","IV_vagas","score_final"])

    # filtro por N mínimo
    base = df_n.copy()
    base = base.loc[base["N"].fillna(0).astype(int) >= int(min_analises)].copy()
    if base.empty:
        return pd.DataFrame(columns=["nomePerito","N","NC","cr","dr","E","IV_vagas","score_final"])

    # escolhe score conforme --kpi-base (com normalização de nomes)
    if isinstance(df_scores, pd.DataFrame) and not df_scores.empty:
        df_scores_adj = _apply_kpi_base(df_scores, kpi_base=kpi_base).copy()
        if "score_final" not in df_scores_adj.columns:
            # última proteção
            if "score_final_nc" in df_scores_adj.columns:
                df_scores_adj = df_scores_adj.rename(columns={"score_final_nc": "score_final"})
            else:
                df_scores_adj["score_final"] = 0.0
        df_scores_adj = df_scores_adj[["nomePerito", "score_final"]].copy()
        df_scores_adj["nomePerito"] = df_scores_adj["nomePerito"].astype(str)
    else:
        df_scores_adj = pd.DataFrame({"nomePerito": [], "score_final": []})

    base["nomePerito"] = base["nomePerito"].astype(str)
    out = base.merge(df_scores_adj, on="nomePerito", how="left")

    # score_final → 0.0 se ausente
    out["score_final"] = out["score_final"].astype(float).fillna(0.0)

    # Excedente E e IV_vagas
    out["E_raw"] = out["NC"].astype(float) - out["N"].astype(float) * float(p_br)
    out["E"] = np.maximum(0.0, out["E_raw"])
    out["E"] = np.ceil(out["E"]).astype(int)

    out["IV_vagas"] = np.ceil(float(alpha) * out["E"].astype(float)).astype(int)
    out.drop(columns=["E_raw"], inplace=True, errors="ignore")

    # Ordenação auxiliar (impacto maior primeiro)
    out = out.sort_values(["IV_vagas","E","NC","N"], ascending=[False, False, False, True]).reset_index(drop=True)
    return out

def _write_manifest_csv(df: pd.DataFrame, path: str, cols: list):
    """Salva CSV simples com as colunas pedidas; cria pasta pai se preciso."""
    if not path: return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    out = df.loc[:, [c for c in cols if c in df.columns]].copy()
    out.to_csv(path, index=False, encoding="utf-8")

def _load_names_from_csv(path: Optional[str]) -> Optional[Set[str]]:
    if not path: return None
    try:
        x = pd.read_csv(path)
        if "nomePerito" not in x.columns: return None
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

def _build_gate_fluxo_b(start: str, end: str, min_analises: int = 50) -> pd.DataFrame:
    """
    Coorte do 'gate' do fluxo B: peritos com %NC ≥ 2× p_BR e N ≥ min_analises.
    Depende das mesmas bases usadas na seleção (N, NC).
    """
    with sqlite3.connect(DB_PATH) as conn:
        schema = _detect_schema(conn)
        df_n = _fetch_perito_n_nc(conn, start, end, schema)
        if df_n.empty:
            return df_n
        p_br, _, _ = _compute_p_br_and_totals(conn, start, end, schema)
    df = df_n.copy()
    df["p_hat"] = df["NC"] / df["N"].replace(0, np.nan)
    gate = df.loc[(df["N"] >= int(min_analises)) & (df["p_hat"] >= 2.0 * float(p_br))].copy()
    # mantenha colunas úteis
    keep = [c for c in ["nomePerito", "cr", "dr", "N", "NC", "p_hat"] if c in gate.columns]
    return gate.loc[:, keep]

def _select_candidates(args) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Retorna (df_sel, df_scope) com a seleção de peritos para o run.
      - df_sel tem pelo menos 'nomePerito'; df_scope é usado como ESCOPO (Fluxo B).
    Regras:
      1) Se --peritos-csv vier, lê diretamente e retorna (peritos_csv, scope_csv|None).
      2) Se --all-matching:
         - Fluxo B: todos do gate (%NC ≥ 2× p_BR) com N ≥ min_analises.
         - Fluxo A: todos com N ≥ min_analises (ordenados por scoreFinal, mas sem cortar).
      3) Se --topk:
         - Fluxo B: ordena pela métrica de impacto (harm/E/IV_vagas se disponível; fallback scoreFinal) e corta K.
         - Fluxo A: ordena por scoreFinal e corta K.
      4) Se --top10:
         - mantém comportamento legacy (pode retornar None; usaremos --top10 nos scripts que suportam).
      5) Caso nenhum: retorna DataFrame vazio para que a pipeline trate como “individual” ou “legacy top10”.
    """
    # 1) CSV explícito
    if args.peritos_csv:
        try:
            df = pd.read_csv(args.peritos_csv)
            if "nomePerito" not in df.columns:
                print(f"❌ {args.peritos_csv} sem coluna 'nomePerito'.")
                return pd.DataFrame(columns=["nomePerito"]), None
            scope = None
            if args.scope_csv:
                try:
                    scope = pd.read_csv(args.scope_csv)
                except Exception as e:
                    print(f"[WARN] Falha lendo scope_csv: {e}")
            return df[["nomePerito"]].drop_duplicates(), (scope if scope is not None else None)
        except Exception as e:
            print(f"[WARN] Falha lendo peritos_csv: {e}")
            return pd.DataFrame(columns=["nomePerito"]), None

    # 2/3/4) Precisamos calcular base N/NC e scores
    with sqlite3.connect(DB_PATH) as conn:
        schema = _detect_schema(conn)
        df_n = _fetch_perito_n_nc(conn, args.start, args.end, schema)
        if df_n.empty:
            return pd.DataFrame(columns=["nomePerito"]), None
        p_br, _, _ = _compute_p_br_and_totals(conn, args.start, args.end, schema)
        df_scores = _fetch_scores(conn, args.start, args.end, schema) if '_fetch_scores' in globals() else pd.DataFrame()

    rank_key = (args.rank_by or ("scoreFinal" if args.fluxo == "A" else "harm"))
    base = _prep_base(df_n, df_scores, p_br, alpha=1.0, min_analises=args.min_analises, kpi_base=args.kpi_base).copy()
    # normalizações de coluna
    for col_pair in [("scoreFinal", "score_final"), ("harm", "harm")]:
        a, b = col_pair
        if a in base.columns and b not in base.columns:
            base = base.rename(columns={a: b})
    # harm/impacto: preferir IV_vagas (derivado) > E > harm > score_final
    if "IV_vagas" in base.columns:
        base["__rank_metric__"] = base["IV_vagas"].astype(float)
    elif "E" in base.columns:
        base["__rank_metric__"] = base["E"].astype(float)
    elif "harm" in base.columns:
        base["__rank_metric__"] = base["harm"].astype(float)
    else:
        base["__rank_metric__"] = base.get("score_final", pd.Series(0, index=base.index)).astype(float)

    # Gate do fluxo B para construir escopo (para --all-matching e --topk B)
    scope_b = _build_gate_fluxo_b(args.start, args.end, args.min_analises) if str(args.fluxo).upper() == "B" else None

    # 2) all-matching
    if args.all_matching:
        if args.fluxo.upper() == "B":
            if scope_b is None or scope_b.empty:
                return pd.DataFrame(columns=["nomePerito"]), None
            # todos do gate, sem cortar
            return scope_b[["nomePerito"]].drop_duplicates().reset_index(drop=True), scope_b
        else:
            # Fluxo A: todos com N ≥ min_analises
            full = base.loc[base["N"] >= int(args.min_analises)].copy()
            if full.empty:
                return pd.DataFrame(columns=["nomePerito"]), None
            full = full.sort_values(["__rank_metric__", "score_final", "NC", "N"], ascending=[False, False, False, True])
            return full[["nomePerito"]].drop_duplicates().reset_index(drop=True), None

    # 3) topk
    if args.topk and args.topk > 0:
        if args.fluxo.upper() == "B":
            # cortar dentro do gate
            if scope_b is None or scope_b.empty:
                return pd.DataFrame(columns=["nomePerito"]), None
            # junta métrica de rank
            b2 = base.merge(scope_b[["nomePerito"]], on="nomePerito", how="inner")
            b2 = b2.sort_values(["__rank_metric__", "score_final", "NC", "N"], ascending=[False, False, False, True])
            b2 = b2.head(int(args.topk))
            return b2[["nomePerito"]].reset_index(drop=True), scope_b
        else:
            # Fluxo A: ranking direto por score/impacto
            b2 = base.loc[base["N"] >= int(args.min_analises)].copy()
            if b2.empty:
                return pd.DataFrame(columns=["nomePerito"]), None
            b2 = b2.sort_values(["__rank_metric__", "score_final", "NC", "N"], ascending=[False, False, False, True])
            b2 = b2.head(int(args.topk))
            return b2[["nomePerito"]].reset_index(drop=True), None

    # 4) top10 legacy: retorna vazio para que a pipeline use --top10 nos scripts que suportam
    if args.top10:
        return pd.DataFrame(columns=["nomePerito"]), (scope_b if args.fluxo.upper()=="B" else None)

    # 5) nenhum caso — volta vazio
    return pd.DataFrame(columns=["nomePerito"]), None


def _materialize_selection_to_csvs(df_sel: pd.DataFrame,
                                   scope_df: Optional[pd.DataFrame],
                                   relatorio_dir: str,
                                   fluxo: str,
                                   start: str, end: str,
                                   save_manifests: bool) -> Tuple[Optional[str], Optional[str]]:
    """
    Salva os CSVs com a seleção e o escopo (se houver), retornando (peritos_csv, scope_csv).
    Em caso de Top10 legacy (df_sel vazio), retorna (None, scope.csv|None).
    """
    os.makedirs(relatorio_dir, exist_ok=True)

    peritos_csv = None
    scope_csv = None

    if df_sel is not None and not df_sel.empty:
        peritos_csv = os.path.join(relatorio_dir, "topk_peritos.csv")
        df_sel[["nomePerito"]].drop_duplicates().to_csv(peritos_csv, index=False, encoding="utf-8")

    if scope_df is not None and not scope_df.empty:
        scope_csv = os.path.join(relatorio_dir, "scope_gate_b.csv")
        scope_df[["nomePerito"]].drop_duplicates().to_csv(scope_csv, index=False, encoding="utf-8")

    if save_manifests:
        # mantém compat com rotina antiga de salvar top10 e scope
        try:
            if df_sel is not None and not df_sel.empty:
                tmp = df_sel.copy().reset_index(drop=True)
                tmp["rank"] = tmp.index + 1
                tmp["fluxo"] = fluxo
                tmp["start"] = start
                tmp["end"] = end
                man_path = os.path.join(relatorio_dir, "topk_manifest.csv")
                tmp.to_csv(man_path, index=False, encoding="utf-8")
        except Exception as e:
            print(f"[WARN] Falha salvando manifest topk: {e}")

    return peritos_csv, scope_csv

# --- helpers manifest/scope (drop-in) ---
def _save_top10_manifests(peritos_df: "pd.DataFrame",
                          relatorio_dir: str,
                          fluxo: str,
                          start: str, end: str,
                          scope_df: "Optional[pd.DataFrame]" = None) -> "Tuple[Optional[str], Optional[str]]":
    """
    Salva:
      - top10_peritos.csv  (colunas: nomePerito, rank, fluxo, start, end)
      - scope_gate_b.csv   (apenas nomePerito) se fluxo==B e scope_df vier.
    Retorna (peritos_csv_path, scope_csv_path)
    """
    peritos_csv_path = None
    scope_csv_path = None
    try:
        out = peritos_df.copy()
        if "nomePerito" not in out.columns:
            raise RuntimeError("peritos_df sem coluna 'nomePerito'")
        out = out.reset_index(drop=True)
        out["rank"] = out.index + 1
        out["fluxo"] = str(fluxo)
        out["start"] = start
        out["end"] = end
        peritos_csv_path = os.path.join(relatorio_dir, "top10_peritos.csv")
        out[["nomePerito", "rank", "fluxo", "start", "end"]].to_csv(peritos_csv_path, index=False, encoding="utf-8")
    except Exception as e:
        print(f"[WARN] Falha salvando top10_peritos.csv: {e}")

    if str(fluxo).upper() == "B" and scope_df is not None:
        try:
            scope_csv_path = os.path.join(relatorio_dir, "scope_gate_b.csv")
            cols = [c for c in scope_df.columns if c.lower() == "nomeperito"]
            if not cols:
                raise RuntimeError("scope_df sem coluna 'nomePerito'")
            scope_df[["nomePerito"]].drop_duplicates().to_csv(scope_csv_path, index=False, encoding="utf-8")
        except Exception as e:
            print(f"[WARN] Falha salvando scope_gate_b.csv: {e}")

    return peritos_csv_path, scope_csv_path

# ────────────────────────────────────────────────────────────────────────────────
# Weekday→Weekend: consultas e inserções em Org
# ────────────────────────────────────────────────────────────────────────────────
def _detect_end_datetime_column(conn) -> str | None:
    """Detecta o nome da coluna de término na tabela 'analises'."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(analises)").fetchall()]
    for cand in ("dataHoraFimPericia", "dataHoraFimAnalise", "dataHoraFim"):
        if cand in cols:
            return cand
    return None

def _weekday2weekend_protocols_by_perito(start: str, end: str):
    """
    Retorna (lista_ordenada, total) de protocolos: início em dia útil e conclusão no fim de semana.
    lista_ordenada = [(perito, [protocolos...]), ...]
    """
    conn = sqlite3.connect(DB_PATH)
    end_col = _detect_end_datetime_column(conn)
    if not end_col:
        conn.close()
        print("[AVISO] Coluna de término não encontrada na tabela 'analises'.")
        return [], 0

    sql = f"""
        SELECT p.nomePerito AS perito, a.protocolo AS protocolo
          FROM analises a
          JOIN peritos p ON a.siapePerito = p.siapePerito
         WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
           AND CAST(strftime('%w', date(a.dataHoraIniPericia)) AS INTEGER) BETWEEN 1 AND 5
           AND a.{end_col} IS NOT NULL
           AND CAST(strftime('%w', date(a.{end_col})) AS INTEGER) IN (0,6)
         ORDER BY p.nomePerito, a.protocolo
    """
    rows = conn.execute(sql, (start, end)).fetchall()
    conn.close()

    por_perito = {}
    for perito, protocolo in rows:
        por_perito.setdefault(perito, []).append(str(protocolo))

    ordenado = sorted(por_perito.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    total = len(rows)
    return ordenado, total

def _append_weekday2weekend_panorama_block(
    lines: list,
    imgs_dir: str,
    comments_dir: str,
    start: str = None,
    end: str = None,
    heading_level: str = "**",
    imgs_prefix: str = "imgs/",
) -> bool:
    orgs_dir   = os.path.join(os.path.dirname(imgs_dir), "orgs")
    png_path   = os.path.join(imgs_dir, "rcheck_weekday_to_weekend_by_cr.png")
    table_org  = os.path.join(orgs_dir, "rcheck_weekday_to_weekend_table.org")
    protos_org = os.path.join(orgs_dir, "rcheck_weekday_to_weekend_protocols.org")
    comment_org= os.path.join(comments_dir, "rcheck_weekday_to_weekend_table_comment.org")

    found_any = any(os.path.exists(p) for p in (png_path, table_org, protos_org, comment_org))
    if not found_any:
        return False

    lines.append(f"{heading_level} Panorama global — Início em dia útil → conclusão no fim de semana (por CR)")

    if os.path.exists(comment_org):
        try:
            with open(comment_org, encoding="utf-8") as f:
                ctext = f.read().strip()
            ctext = _protect_tables_in_quote(ctext)
            lines.extend(["", "#+BEGIN_QUOTE", ctext, "#+END_QUOTE"])
        except Exception as e:
            print(f"[AVISO] Falha ao anexar comentário do panorama: {e}")

    table_content = ""
    has_png_inside_table = False
    if os.path.exists(table_org):
        try:
            with open(table_org, encoding="utf-8") as f:
                table_content = f.read().strip()
            table_content = "\n".join(
                ln for ln in table_content.splitlines()
                if not ln.strip().lower().startswith("#+title")
            ).strip()
            table_content = table_content.replace("[[file:imgs/", f"[[file:{imgs_prefix}")
            has_png_inside_table = "rcheck_weekday_to_weekend_by_cr.png" in table_content
            table_content = _ensure_blank_lines_around_tables(table_content)
        except Exception as e:
            print(f"[AVISO] Falha ao ler tabela do panorama: {e}")
            table_content = ""

    if os.path.exists(png_path) and not has_png_inside_table:
        base = os.path.basename(png_path)
        lines.extend([
            "",
            "#+ATTR_LATEX: :placement [H] :width \\linewidth",
            "#+CAPTION: Tarefas iniciadas em dia útil e concluídas no fim de semana — por CR (ordenado)",
            f"[[file:{imgs_prefix}{base}]]",
        ])

    if table_content:
        lines.extend(["", table_content])

    appended_protocols = False
    if os.path.exists(protos_org):
        try:
            with open(protos_org, encoding="utf-8") as f:
                protos_content = f.read().strip()
            protos_content = "\n".join(
                ln for ln in protos_content.splitlines()
                if not ln.strip().lower().startswith("#+title")
            ).strip()
            protos_content = protos_content.replace("[[file:imgs/", f"[[file:{imgs_prefix}")
            protos_content = re.sub(
                r'^\s*\*+\s+Protocolos envolvidos\s*\(por perito\)\s*\n',
                '',
                protos_content,
                count=1,
                flags=re.IGNORECASE
            ).lstrip()
            lines.append("")
            lines.append(f"{heading_level} Protocolos envolvidos (por perito)")
            try:
                protos_content = shift_org_headings(protos_content, delta=1)
            except NameError:
                pass
            lines.append(protos_content)
            appended_protocols = True
        except Exception as e:
            print(f"[AVISO] Falha ao anexar protocolos (.org): {e}")

    if not appended_protocols and start and end:
        try:
            lista, total = _weekday2weekend_protocols_by_perito(start, end)
            if lista:
                lines.append("")
                lines.append(f"{heading_level} Protocolos envolvidos (por perito)")
                for perito, protos in lista:
                    lines.append(f"- *{perito}* ({len(protos)}): {', '.join(protos)}")
                lines.append(f"\n- **Total de protocolos:** {total}")
        except Exception as e:
            print(f"[AVISO] Falha no fallback dinâmico de protocolos: {e}")

    lines.append("\n#+LATEX: \\newpage\n")
    return True


def _append_weekday2weekend_perito_block_if_any(
    lines: list,
    perito: str,
    imgs_dir: str,
    comments_dir: str,
    start: str,
    end: str,
    heading_level: str = "**",
) -> bool:
    """
    Adiciona, AO FINAL do relatório individual, um bloco com os casos weekday→weekend
    somente se o perito tiver pelo menos 1 protocolo.
    """
    orgs_dir   = os.path.join(os.path.dirname(imgs_dir), "orgs")
    table_org  = os.path.join(orgs_dir, "rcheck_weekday_to_weekend_table.org")
    protos_org = os.path.join(orgs_dir, "rcheck_weekday_to_weekend_protocols.org")

    protos = _read_protocols_for_perito_from_org(protos_org, perito) if os.path.exists(protos_org) else []
    if not protos:
        try:
            lst, _total = _weekday2weekend_protocols_by_perito(start, end)
            perito_lower = perito.strip().lower()
            for p, pnums in lst:
                if p.strip().lower() == perito_lower:
                    protos = list(pnums)
                    break
        except Exception as e:
            print(f"[AVISO] Fallback dinâmico de protocolos falhou: {e}")

    if not protos:
        return False

    lines.append(f"{heading_level} Início em dia útil → conclusão no fim de semana (este perito)")
    lines.append(f"Janela: {start} a {end}. Somente tarefas deste perito iniciadas em dia útil e concluídas no fim de semana.\n")

    mini_table = _extract_single_row_from_org_table(table_org, perito) if os.path.exists(table_org) else ""
    if mini_table:
        lines.append("#+CAPTION: Resumo (apenas este perito)")
        lines.append(mini_table)
        lines.append("")

    lines.append("#+CAPTION: Protocolos deste perito")
    protos_sorted = sorted(protos)
    chunk, max_len = [], 25
    while protos_sorted:
        chunk, protos_sorted = protos_sorted[:max_len], protos_sorted[max_len:]
        lines.append(f"- {', '.join(chunk)}")
    lines.append("\n#+LATEX: \\newpage\n")
    return True

def _read_protocols_for_perito_from_org(path: str, perito: str) -> list:
    """Extrai lista de protocolos do perito a partir do .org de protocolos."""
    if not os.path.exists(path):
        return []
    perito_low = perito.strip().lower()
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if ":" not in line:
                    continue
                l = line.lstrip("-• ").strip()
                l_norm = l.replace("*", "")
                if perito_low in l_norm.lower():
                    after = line.split(":", 1)[1].strip()
                    if after:
                        parts = [p.strip() for p in after.split(",") if p.strip()]
                        out.extend(parts)
    except Exception as e:
        print(f"[AVISO] Falha lendo protocolos em {path}: {e}")
    return out

def _extract_single_row_from_org_table(path: str, perito: str) -> str:
    """Extrai somente a linha do perito de uma tabela Org que tenha a coluna 'Perito'."""
    try:
        with open(path, encoding="utf-8") as f:
            lines = [ln.rstrip("\n") for ln in f]
        i = 0
        while i < len(lines):
            if lines[i].lstrip().startswith("|"):
                tbl = []
                while i < len(lines) and lines[i].lstrip().startswith("|"):
                    tbl.append(lines[i].strip())
                    i += 1
                if not tbl:
                    continue
                header = [h.strip() for h in tbl[0].strip("| ").split("|")]
                if "Perito" not in header:
                    continue
                per_idx = header.index("Perito")
                for row in tbl[2:]:
                    cols = [c.strip() for c in row.strip("| ").split("|")]
                    if per_idx < len(cols) and cols[per_idx].strip().lower() == perito.strip().lower():
                        return "\n".join([
                            "| " + " | ".join(header) + " |",
                            "|" + "|".join("---" for _ in header) + "|",
                            "| " + " | ".join(cols) + " |",
                        ])
            else:
                i += 1
    except Exception as e:
        print(f"[AVISO] Falha extraindo linha da tabela {path}: {e}")
    return ""

# ——— Reincidentes: helpers ————————————————————————————————————————————————
REINC_SCRIPT = os.path.join(SCRIPTS_DIR, "scan_top10_names_with_months.py")

def _run_reincidentes_scan(out_csv: str, min_months: int, root: str,
                           start: str = None, end: str = None,
                           min_analises: int = 50) -> None:
    """
    (NOVA) Gera reincidentes consultando o BANCO, sem depender de .org antigos.
    Jan->end do ano de 'end', mês a mês. Em cada mês, pega o Top 10 (scoreFinal)
    exigindo 'min_analises' tarefas no próprio mês.

    Parâmetros
    ----------
    out_csv : str
        Caminho do CSV de saída (será criado/reescrito).
    min_months : int
        Número mínimo de meses distintos para o perito ser considerado reincidente.
    root : str
        Ignorado nesta versão (mantido por compatibilidade da assinatura).
    start : str | None
        Data inicial YYYY-MM-DD (usada apenas se 'end' vier vazio; caso contrário ignorada).
    end : str | None
        Data final YYYY-MM-DD (define o ano e o último mês da janela Jan->end).
    min_analises : int
        Mínimo de análises no mês para que o perito seja elegível naquele mês.

    Saída
    -----
    CSV com colunas: nome, matricula, CR, DR, meses (lista 'YYYY-MM' separados por vírgula).
    """
    import csv

    # Determina o fim da janela
    if not end:
        if not start:
            print("[AVISO] _run_reincidentes_scan: sem 'end' (nem 'start'). Abortando.")
            return
        end_dt = datetime.strptime(start, "%Y-%m-%d").date()
    else:
        end_dt = datetime.strptime(end, "%Y-%m-%d").date()

    # Jan/AAAA → end
    scan_start = date(end_dt.year, 1, 1)
    scan_end   = end_dt

    # Iterador de meses
    def _iter_months(dini: date, dfim: date):
        y, m = dini.year, dini.month
        while (y < dfim.year) or (y == dfim.year and m <= dfim.month):
            yield y, m
            if m == 12:
                y, m = y + 1, 1
            else:
                m += 1

    conn = sqlite3.connect(DB_PATH)

    # siape -> dados agregados
    seen = {}

    try:
        for y, m in _iter_months(scan_start, scan_end):
            m_ini = date(y, m, 1)
            m_fim = date(y, m, calendar.monthrange(y, m)[1])

            # Top 10 do mês por score, exigindo min_analises no mês
            query = """
                SELECT
                    p.siapePerito AS siape,
                    p.nomePerito  AS nome,
                    p.cr          AS CR,
                    p.dr          AS DR,
                    MAX(i.scoreFinal) AS score,
                    COUNT(a.protocolo) AS total_analises
                FROM indicadores i
                JOIN analises    a ON a.siapePerito = i.perito
                JOIN peritos     p ON p.siapePerito = i.perito
                WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
                GROUP BY p.siapePerito, p.nomePerito, p.cr, p.dr
                HAVING total_analises >= ?
                ORDER BY score DESC
                LIMIT 10
            """
            dfm = pd.read_sql(
                query, conn,
                params=(m_ini.isoformat(), m_fim.isoformat(), int(min_analises))
            )
            if dfm.empty:
                continue

            month_key = f"{y:04d}-{m:02d}"
            for _, row in dfm.iterrows():
                siape = str(row["siape"])
                entry = seen.setdefault(siape, {
                    "nome": row["nome"],
                    "CR":   row.get("CR", "") or "",
                    "DR":   row.get("DR", "") or "",
                    "meses": set(),
                })
                entry["meses"].add(month_key)

    finally:
        conn.close()

    # Filtra reincidentes (>= min_months)
    rows = []
    for siape, data in seen.items():
        meses_sorted = sorted(data["meses"])
        if len(meses_sorted) >= (min_months or 1):
            rows.append({
                "nome":      data["nome"],
                "matricula": siape,
                "CR":        data["CR"],
                "DR":        data["DR"],
                "meses":     ", ".join(meses_sorted),
            })

    # Dedup (por matrícula; se matrícula vazia, cai para nome)
    dedup = {}
    for r in rows:
        key = r["matricula"] or r["nome"]
        dedup.setdefault(key, r)

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["nome", "matricula", "CR", "DR", "meses"])
        w.writeheader()
        for r in sorted(dedup.values(), key=lambda x: (x["nome"], x["matricula"])):
            w.writerow(r)

    print(f"✅ Reincidentes (DB, Jan→{scan_end:%Y-%m}, min_analises={min_analises}) salvos em: {out_csv}")

def _csv_to_org_table(csv_path: str, headers=("nome","cr","dr","meses")) -> str:
    """Converte um CSV simples em uma org-table (acesso case-insensitive)."""
    if not os.path.exists(csv_path):
        return ""

    rows = []
    with open(csv_path, newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            rows.append(row)
    if not rows:
        return ""

    # Cabeçalhos bonitinhos para a tabela
    hdr_print = []
    for h in headers:
        hl = h.lower()
        if hl == "meses":      hdr_print.append("Meses")
        elif hl == "nome":     hdr_print.append("Perito")
        elif hl == "cr":       hdr_print.append("CR")
        elif hl == "dr":       hdr_print.append("DR")
        elif hl == "matricula":hdr_print.append("Matrícula")
        else:                  hdr_print.append(h)

    lines = []
    lines.append("| " + " | ".join(hdr_print) + " |")
    lines.append("|" + "|".join("---" for _ in hdr_print) + "|")

    # Acesso case-insensitive aos campos
    for row in rows:
        row_ci = { (k or "").strip().lower(): (v or "") for k, v in row.items() }
        vals = [ row_ci.get(h.lower(), "") for h in headers ]
        lines.append("| " + " | ".join(vals) + " |")

    return "\n".join(lines)


def _append_reincidentes_to_org(org_path: str, csv_path: str, min_months: int = 2, heading_level: str = "**"):
    """
    Acrescenta seção de 'Peritos reincidentes' ao final do .org, em fonte pequena
    e com quebras automáticas na coluna 'Meses' (p{largura} + longtable).
    """
    table = _csv_to_org_table(csv_path)
    if not table:
        print("[INFO] Sem reincidentes para anexar (tabela vazia).")
        return False

    section = []
    section.append("")
    section.append(f"{heading_level} Peritos reincidentes no Top 10 (≥ {min_months} meses)")
    section.append("")
    # Atributos LaTeX para a tabela: usa longtable e fixa largura da última coluna
    # Ajuste p{8cm} conforme necessidade (7.5cm, 7cm, etc.)
    section.append("#+ATTR_LATEX: :environment longtable :align l l l p{8cm}")
    # Grupo local para reduzir fonte e padding, sem afetar o resto do documento
    section.append("#+LATEX: \\begingroup\\setlength{\\tabcolsep}{3pt}\\renewcommand{\\arraystretch}{1.0}\\scriptsize")
    section.append(table)
    section.append("#+LATEX: \\endgroup")
    section.append("\n#+LATEX: \\newpage\n")

    with open(org_path, "a", encoding="utf-8") as f:
        f.write("\n".join(section))

    print(f"✅ Reincidentes anexado ao final de: {org_path}")
    return True


# ────────────────────────────────────────────────────────────────────────────────
# Ajustes de Org e utilitários de texto
# ────────────────────────────────────────────────────────────────────────────────
_ANSI_RE     = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_BOX_DRAW_RE = re.compile(r"[┌┬┐└┴┘├┼┤│─━┃╭╮╯╰█▓▒░]")

def shift_org_headings(text: str, delta: int = 1) -> str:
    """Rebaixa/eleva níveis de heading em um texto Org."""
    lines = []
    for ln in text.splitlines():
        if ln.startswith('*'):
            i = 0
            while i < len(ln) and ln[i] == '*':
                i += 1
            if i > 0 and i + delta < 7 and (i < len(ln) and ln[i] == ' '):
                ln = ('*' * (i + delta)) + ln[i:]
        lines.append(ln)
    return "\n".join(lines)

def _rewrite_org_image_links(org_text: str, imgs_rel_prefix: str = "../imgs/") -> str:
    """Normaliza links [[file:...]] para apontarem para a pasta imgs/ relativa."""
    def repl(m):
        path = m.group(1).strip()
        if path.startswith(("http://", "https://", "/")):
            return m.group(0)
        base = os.path.basename(path)
        return f"[[file:{imgs_rel_prefix}{base}]]"
    return re.sub(r"\[\[file:([^\]]+)\]\]", repl, org_text)

def _nice_caption(fname: str) -> str:
    """Gera uma legenda amigável a partir do nome do arquivo."""
    base = os.path.splitext(os.path.basename(fname))[0]
    return base.replace("_", " ").replace("-", " ")

def markdown_para_org(texto_md: str) -> str:
    """Converte texto Markdown para Org usando Pandoc, retornando o texto Org."""
    with tempfile.NamedTemporaryFile("w+", suffix=".md", delete=False) as fmd:
        fmd.write(texto_md)
        fmd.flush()
        org_path = fmd.name.replace(".md", ".org")
        subprocess.run(["pandoc", fmd.name, "-t", "org", "-o", org_path])
        with open(org_path, encoding="utf-8") as forg:
            org_text = forg.read()
    return org_text

def _strip_ansi(s: str) -> str:
    return _ANSI_RE.sub("", s)

def _wrap_ascii_blocks(text: str) -> str:
    """Envelopa trechos com pseudografismo/ANSI em #+begin_example/# +end_example."""
    lines = text.splitlines()
    out = []
    in_block = False
    for ln in lines:
        has_ansi = bool(_ANSI_RE.search(ln))
        has_box  = bool(_BOX_DRAW_RE.search(ln))
        if (has_ansi or has_box) and ln.strip():
            if not in_block:
                out.append("#+begin_example")
                in_block = True
            out.append(_strip_ansi(ln))
            continue
        else:
            if in_block:
                out.append("#+end_example")
                in_block = False
            out.append(ln)
    if in_block:
        out.append("#+end_example")
    return "\n".join(out)

def _ensure_blank_lines_around_tables(text: str) -> str:
    """Garante linhas em branco ao redor de tabelas Org para não confundir o Pandoc."""
    text = re.sub(r"(\[Tabela[^\]]*\])\s*(\|)", r"\1\n\2", text, flags=re.I)
    lines = text.splitlines()
    out = []
    i = 0
    while i < len(lines):
        ln = lines[i]
        if ln.lstrip().startswith("|"):
            if out and out[-1].strip() != "":
                out.append("")
            while i < len(lines) and lines[i].lstrip().startswith("|"):
                out.append(lines[i])
                i += 1
            if i < len(lines) and lines[i].strip() != "":
                out.append("")
            continue
        out.append(ln)
        i += 1
    return "\n".join(out)

def _normalize_org_table_block(block_lines):
    """Normaliza um bloco de tabela Org (pipes, espaços e bordas)."""
    fixed = []
    for ln in block_lines:
        raw = ln.strip()
        if raw == "|" or raw == "":
            continue
        raw = re.sub(r"\s*\|\s*", " | ", raw)
        raw = raw.strip()
        if not raw.startswith("|"):
            raw = "| " + raw
        if not raw.endswith("|"):
            raw = raw + " |"
        fixed.append(raw)
    return fixed

def _normalize_all_tables(text: str) -> str:
    """Varre o texto Org e normaliza todas as tabelas."""
    lines = text.splitlines()
    out = []
    i = 0
    while i < len(lines):
        if lines[i].lstrip().startswith("|"):
            block = []
            while i < len(lines) and lines[i].lstrip().startswith("|"):
                block.append(lines[i])
                i += 1
            fixed = _normalize_org_table_block(block)
            if fixed:
                out.extend(fixed)
            if i < len(lines) and lines[i].strip() != "":
                out.append("")
        else:
            out.append(lines[i])
            i += 1
    return "\n".join(out)

def _protect_org_text_for_pandoc(text: str) -> str:
    """Protege e normaliza um texto Org para reduzir erros do Pandoc."""
    text = _ensure_blank_lines_around_tables(text)
    text = _normalize_all_tables(text)
    text = _wrap_ascii_blocks(text)
    return text

def _protect_tables_in_quote(txt: str) -> str:
    """Se encontra tabelas dentro de quotes, envolve com example para não quebrar render."""
    s = re.sub(r'(\S)\n\|', r'\1\n\n|', txt)
    out_lines = []
    in_tbl = False
    for ln in s.splitlines():
        if ln.lstrip().startswith('|'):
            if not in_tbl:
                out_lines.append('#+begin_example')
                in_tbl = True
            out_lines.append(ln)
        else:
            if in_tbl:
                out_lines.append('#+end_example')
                in_tbl = False
            out_lines.append(ln)
    if in_tbl:
        out_lines.append('#+end_example')
    return "\n".join(out_lines)

def _extract_comment_from_org(org_text: str) -> str:
    """
    Extrai comentário de um .org de gráfico:
    1) bloco QUOTE; 2) seção 'Comentário'; 3) 1º parágrafo após a imagem.
    """
    def _strip_drawers(s: str) -> str:
        return re.sub(r'(?ms)^\s*:PROPERTIES:\s*.*?^\s*:END:\s*$', '', s, flags=re.MULTILINE)

    def _strip_noise_lines(s: str) -> str:
        out = []
        for ln in s.splitlines():
            sl = ln.strip()
            if not sl:
                out.append(ln); continue
            if sl.startswith("#+"):   continue
            if sl.startswith("|"):    continue
            if sl.startswith("[[file:"): continue
            out.append(ln)
        return "\n".join(out)

    txt = org_text
    m = re.search(r'(?mis)^\s*#\+BEGIN_QUOTE\s*(.*?)^\s*#\+END_QUOTE', txt)
    if m:
        body = _strip_noise_lines(_strip_drawers(m.group(1))).strip()
        if body:
            return body
    m = re.search(r'(?mis)^\*+\s+coment[aá]ri?o[^\n]*\n(.*?)(?=^\*+\s|\Z)', txt)
    if m:
        body = _strip_noise_lines(_strip_drawers(m.group(1))).strip()
        if body:
            return body
    after_img = re.split(r'(?mi)^\s*\[\[file:[^\]]+\]\]\s*$', txt, maxsplit=1)
    if len(after_img) == 2:
        tail = _strip_noise_lines(_strip_drawers(after_img[1]))
        para = []
        started = False
        for ln in tail.splitlines():
            s = ln.strip()
            if not s:
                if started: break
                else: continue
            if s.startswith("*") or s.startswith("#+") or s.startswith("|"):
                if started: break
                else: continue
            para.append(ln)
            started = True
        res = " ".join(" ".join(para).split())
        if res:
            return res
    return ""

# ────────────────────────────────────────────────────────────────────────────────
# Ordenação/Ranking de imagens para montagem do relatório
# ────────────────────────────────────────────────────────────────────────────────
def _mode_rank(name: str) -> int:
    name = name.lower()
    if "perito-share" in name: return 0
    if "task-share" in name:   return 1
    if "time-share" in name:   return 2
    return 9

def _script_rank(name: str) -> int:
    n = name.lower()
    order = [
        ("nc",           ["nc_rate", "taxa_nc", "nc-rate", "comparenc", "nc_", "rate_nc", "compare_nc_rate", "nao_conformidade"]),
        ("motivos",      ["motivos_perito_vs_brasil", "motivos_top10_vs_brasil", "motivos_", "motivo_"]),
        ("produtiv",     ["produtividade_", "prod_", "productivity_"]),
        ("le15s",        ["le15", "le15s", "compare_15s", "15s"]),
        ("overlap",      ["sobreposicao_", "overlap", "sobrel"]),
        ("composto",     ["indicadores_composto", "composto", "composite"]),
    ]
    for idx, (_, keys) in enumerate(order):
        if any(k in n for k in keys):
            return idx
    return 99

def _png_rank_main(fname: str) -> tuple:
    base = os.path.basename(fname).lower()
    return (_script_rank(base), _mode_rank(base), base)

def _rcheck_perito_rank(fname: str) -> int:
    n = os.path.basename(fname).lower()
    order = [
        "rcheck_nc_rate_", "rcheck_le15", "rcheck_productivity",
        "rcheck_overlap", "rcheck_motivos_chisq", "rcheck_composite",
        "rcheck_weighted_props_nc", "rcheck_weighted_props_le", "rcheck_weighted_props_",
    ]
    for i, key in enumerate(order):
        if key in n:
            return i
    return 99

def _rcheck_group_rank(fname: str) -> int:
    n = os.path.basename(fname).lower()
    order = [
        "rcheck_top10_nc_rate", "rcheck_top10_le15", "rcheck_top10_productivity",
        "rcheck_top10_overlap", "rcheck_top10_motivos_chisq", "rcheck_top10_composite"
    ]
    for i, key in enumerate(order):
        if key in n:
            return i
    if "rcheck_top10_composite_robustness" in n:
        return 5
    return 99


# ────────────────────────────────────────────────────────────────────────────────
# Pré-flight e planners de execução (R e Python)
# ────────────────────────────────────────────────────────────────────────────────
def _preflight_r(r_bin: str):
    """Valida binário do R, mostra versão e encerra se não encontrado."""
    r_path = shutil.which(r_bin)
    if not r_path:
        print(f"❌ Não encontrei o binário '{r_bin}' no PATH. Instale o R e garanta que Rscript esteja disponível.")
        print("   Ex.: sudo apt install r-base-core  (ou ajuste --r-bin com o caminho completo)")
        sys.exit(2)
    try:
        out = subprocess.run([r_bin, "--version"], capture_output=True, text=True)
        ver = (out.stdout or out.stderr or "").splitlines()[0].strip()
        print(f"[R] Ok: {ver} ({r_path})")
    except Exception as e:
        print(f"❌ Falha ao executar '{r_bin} --version': {e}")
        sys.exit(2)

def _is_r_cmd(cmd: list) -> bool:
    """Retorna True se o comando parece ser R/Rscript."""
    if not cmd:
        return False
    exe = os.path.basename(str(cmd[0])).lower()
    return ("rscript" in exe) or exe == "r"

def _detect_r_out_flag(r_file_path: str):
    """Tenta inferir a flag de saída do script R (--out-dir ou --out)."""
    try:
        with open(r_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            txt = f.read().lower()
        if "--out-dir" in txt or "out-dir" in txt or "out_dir" in txt:
            return "--out-dir"
        if "--out" in txt or "make_option(c(\"--out\"" in txt:
            return "--out"
    except Exception:
        pass
    return None

def _run_or_dry(cmd: list, plan_only: bool = False) -> int:
    """
    Executa `cmd` ou apenas imprime (dry-run) mantendo o mesmo prefixo [R]/[PY].
    Usa RCHECK_DIR para scripts R e SCRIPTS_DIR para Python, e injeta PYTHONPATH
    quando for Python.
    """
    try:
        tag = "[R]" if _is_r_cmd(cmd) else "[PY]"
    except Exception:
        tag = "[?]"

    # Imprime o comando “bonito” no modo plano
    if plan_only:
        try:
            line = " ".join(shlex.quote(str(x)) for x in cmd)
        except Exception:
            line = " ".join(map(str, cmd))
        print(f"{tag} {line}")
        return 0

    # Execução
    try:
        if _is_r_cmd(cmd):
            print(f"{tag} {' '.join(map(str, cmd))}")
            r_env = os.environ.copy()  # (se precisar, injete variáveis extras antes de chamar aqui)
            subprocess.run(cmd, check=False, cwd=RCHECK_DIR, env=r_env)
        else:
            print(f"{tag} {' '.join(map(str, cmd))}")
            subprocess.run(cmd, check=False, cwd=SCRIPTS_DIR, env=_env_with_project_path())
        return 0
    except Exception as e:
        print(f"[ERRO] Falha executando: {' '.join(map(str, cmd))}\n  -> {e}")
        return 1

def _r_deps_bootstrap_cmd(r_bin: str):
    """Gera script R para checar/instalar dependências básicas e retorna o comando para rodá-lo."""
    os.makedirs(RCHECK_DIR, exist_ok=True)
    deps_path = os.path.join(RCHECK_DIR, "_ensure_deps.R")
    if not os.path.exists(deps_path):
        r_code = r'''# auto-gerado pelo make_report.py ─ não editar manualmente
options(warn = 1)
repos <- getOption("repos"); repos["CRAN"] <- "https://cloud.r-project.org"; options(repos = repos)
user_lib <- Sys.getenv("R_LIBS_USER")
if (nzchar(user_lib)) { dir.create(user_lib, showWarnings = FALSE, recursive = TRUE); .libPaths(unique(c(user_lib, .libPaths()))) }
message("[deps] .libPaths(): ", paste(.libPaths(), collapse = " | "))
need <- c("dplyr","tidyr","readr","stringr","purrr","forcats","lubridate","ggplot2","scales","broom","DBI","RSQLite","ggtext","gridtext","ragg","textshaping","cli","glue","curl","httr")
have <- rownames(installed.packages()); to_install <- setdiff(need, have)
ncpus <- 1L; try({ ncpus <- max(1L, parallel::detectCores(logical = TRUE) - 1L) }, silent = TRUE)
if (length(to_install)) { message("[deps] Instalando: ", paste(to_install, collapse = ", ")); 
  tryCatch({ install.packages(to_install, dependencies = TRUE, Ncpus = ncpus, lib = if (nzchar(user_lib)) user_lib else .libPaths()[1]) },
           error = function(e) { message("[deps][ERRO] ", conditionMessage(e)); quit(status = 1L) }) 
} else { message("[deps] Todos os pacotes já presentes.") }
ok <- TRUE; for (pkg in c("dplyr","ggplot2","DBI","RSQLite")) ok <- ok && requireNamespace(pkg, quietly = TRUE)
ok <- ok && requireNamespace("ggtext", quietly = TRUE) && requireNamespace("ragg", quietly = TRUE)
if (!ok) message("[deps][AVISO] Verifique dependências de sistema (ex.: libcurl, harfbuzz, fribidi, freetype).") else message("[deps] OK")
message(capture.output(sessionInfo()), sep = "\n")
'''
        with open(deps_path, "w", encoding="utf-8") as f:
            f.write(r_code)
    return [r_bin, deps_path]

def build_commands_for_script(script_file: str, context: dict) -> list:
    """
    Monta comandos p/ um script Python de gráfico, dado o contexto.
    Ordem de preferência da seleção:
      (1) --peritos-csv / --scope-csv
      (2) --perito / --nome   (execução individual)
      (3) --top10             (apenas quando kind==group/top10 e top_k==10)
    Caso nenhum dos três seja possível, o script é pulado.
    """
    cmds = []
    name = os.path.basename(script_file)
    help_info = introspect_script(script_file)
    flags = set(help_info.get("flags", set())) or set(ASSUME_FLAGS.get(name, []))
    modes = detect_modes(script_file, help_info)

    base = ["--start", context["start"], "--end", context["end"]]

    # Fluxo
    fluxo = context.get("fluxo", "B")
    if "--fluxo" in flags:
        base += ["--fluxo", str(fluxo)]

    # ===== Seleção (grupo/individual) =====
    sel: list[str] = []

    peritos_csv = context.get("peritos_csv")
    scope_csv   = context.get("scope_csv")
    top_k       = int(context.get("top_k") or 0)
    kind        = (context.get("kind") or "").lower()

    # (1) Manifesto (grupo TopK/All-matching)
    if peritos_csv and "--peritos-csv" in flags:
        sel += ["--peritos-csv", peritos_csv]
        if scope_csv and "--scope-csv" in flags:
            sel += ["--scope-csv", scope_csv]
        # NOVO: vários scripts exigem seletor exclusivo (--perito | --top10).
        # Quando estamos em execução de grupo com manifesto, passamos --top10
        # para satisfazer o parser e indicar “modo grupo”.
        if "--top10" in flags and kind in ("group", "top10"):
            sel += ["--top10"]

    # (2) Execução individual
    elif context.get("perito"):
        perito = str(context["perito"])
        if "--perito" in flags:
            sel += ["--perito", perito]
        elif "--nome" in flags:
            sel += ["--nome", perito]
        else:
            print(f"[INFO] {name} não expõe --perito/--nome; exige --peritos-csv. Pulando este script para execução individual.")
            return []

    # (3) Top10 legacy (só se K==10 e for grupo)
    elif kind in ("group", "top10") and top_k == 10 and "--top10" in flags:
        sel += ["--top10"]
        if "--min-analises" in flags and "min_analises" in context:
            sel += ["--min-analises", str(context["min_analises"])]

    else:
        print(f"[INFO] {name} exige --peritos-csv para K≠10/all-matching; pulando este script.")
        return []

    # ===== Exportações / comentários =====
    if "--export-org" in flags and context.get("export_org"):
        base += ["--export-org"]
    if "--export-pdf" in flags and context.get("export_pdf"):
        base += ["--export-pdf"]
    if context.get("add_comments"):
        if "--export-comment-org" in flags:
            base += ["--export-comment-org"]
        elif "--export-comment" in flags:
            base += ["--export-comment"]
        if "--call-api" in flags:
            base += ["--call-api"]
    if "--export-png" in flags:
        base += ["--export-png"]
    if "--export-md" in flags:
        base += ["--export-md"]

    # ===== Modos (quando aplicável) =====
    if modes:
        for m in modes:
            cmd = [sys.executable, script_file] + base + sel + ["--mode", m]
            ex = EXTRA_ARGS.get(name, [])
            if ex:
                cmd += ex
            cmds.append(cmd)
    else:
        cmd = [sys.executable, script_file] + base + sel
        ex = EXTRA_ARGS.get(name, [])
        if ex:
            cmd += ex
        cmds.append(cmd)

    return cmds

def build_r_commands_for_perito(perito: str, start: str, end: str, r_bin: str) -> list:
    """Planeja a fila de R checks individuais para o perito."""
    cmds = []
    for fname, meta in RCHECK_SCRIPTS:
        try:
            fpath = rscript_path(fname)
        except FileNotFoundError:
            print(f"[AVISO] R check ausente: {fname} (crie em {RCHECK_DIR})")
            continue
        out_flag = _detect_r_out_flag(fpath)
        cmd = [r_bin, fpath, "--db", DB_PATH, "--start", start, "--end", end]
        if out_flag: cmd += [out_flag, EXPORT_DIR]
        if meta.get("need_perito", False): cmd += ["--perito", perito]
        for k, v in (meta.get("defaults") or {}).items():
            cmd += [k, str(v)]
        cmds.append(cmd)
    if cmds:
        print(f"[INFO] R checks individuais enfileirados para '{perito}': {len(cmds)}")
    else:
        print("[INFO] Nenhum R check individual enfileirado.")
    return cmds

def build_r_commands_for_top10(
    start: str,
    end: str,
    r_bin: str,
    min_analises: int,
    fluxo: str = "B",
    peritos_csv: Optional[str] = None,
    rank_by: Optional[str] = None,
) -> list[list[str]]:
    """
    Agenda os R checks de GRUPO (g01..g07) para o Top 10.
    - Se `peritos_csv` for dado, usa manifesto revalidado (n>=min_analises).
    - Senão, usa seleção interna: fluxo A=scoreFinal, B=harm (default).
    """
    cmds: list[list[str]] = []

    # --- paths/consts (ajuste estes helpers se no seu projeto tiver nomes diferentes) ---
    rscript = r_bin or "Rscript"
    db_path = DB_PATH  # deve existir no módulo (o mesmo usado pelos checks individuais)
    try:
        # igual ao fallback dos R scripts: ../graphs_and_tables/exports a partir do DB
        exports_dir = os.path.normpath(os.path.join(os.path.dirname(db_path), "..", "graphs_and_tables", "exports"))
    except Exception:
        exports_dir = os.path.join(PROJECT_ROOT, "graphs_and_tables", "exports")  # fallback

    common = ["--db", db_path, "--start", start, "--end", end, "--out-dir", exports_dir]

    fluxo = (fluxo or "B").upper()
    if not rank_by:
        rank_by = "harm" if fluxo == "B" else "scoreFinal"
    rank_by = rank_by.lower()

    if peritos_csv:
        sel = ["--peritos-csv", peritos_csv, "--min-analises", str(min_analises)]
    else:
        sel = ["--flow", fluxo, "--rank-by", rank_by, "--min-analises", str(min_analises)]

    # lista dos g-scripts + flags específicas
    g_scripts: list[tuple[str, list[str]]] = [
        ("g01_top10_nc_rate_check.R",            []),
        ("g02_top10_le15s_check.R",              ["--threshold", "15"]),
        ("g03_top10_productivity_check.R",       ["--threshold", "50"]),
        ("g04_top10_overlap_check.R",            []),
        ("g05_top10_motivos_chisq.R",            ["--min-count", "10", "--topn", "15"]),
        ("g06_top10_composite_robustness.R",     ["--le-threshold", "15"]),
        ("g07_top10_kpi_icra_iatd_score.R",      []),
    ]

    for script, extra in g_scripts:
        script_path = os.path.join(RCHECK_DIR, script)  # RCHECK_DIR deve existir no módulo
        if os.path.exists(script_path):
            cmds.append([rscript, script_path, *common, *sel, *extra])
        else:
            print(f"[AVISO] Script de grupo ausente: {script_path}")

    return cmds
    
def pretty_print_plan(
    planned_cmds: List[list],
    args,
    peritos_csv_path: Optional[str] = None,
    scope_csv_path: Optional[str] = None,
) -> None:
    """
    Imprime um 'dry run' do plano de execução:
      - Resumo do período, seleção (Top10/Individual), fluxo e rank-by efetivo
      - Flags de exportação / apêndice R
      - Variáveis de ambiente R que seriam injetadas (FLUXO, PERITOS_CSV, SCOPE_CSV)
      - Lista numerada dos comandos (marcados como [R] ou [PY])

    Compatível com chamadas: pretty_print_plan(planned_cmds, args, peritos_csv_path=..., scope_csv_path=...)
    """

    # Helpers locais (fallback se _is_r_cmd não existir no módulo)
    def _detect_is_r(cmd):
        try:
            return _is_r_cmd(cmd)  # se já existir no arquivo
        except NameError:
            try:
                return os.path.basename(str(cmd[0])).lower().startswith("rscript")
            except Exception:
                return False

    # Extrai e normaliza opções relevantes
    fluxo   = str(getattr(args, "fluxo", "B") or "B").upper()
    top10   = bool(getattr(args, "top10", False))
    rank_by = getattr(args, "rank_by", None)
    eff_rank = (rank_by or ("harm" if fluxo == "B" else "scorefinal")).lower()

    print("=" * 78)
    print("DRY RUN — PLANO DE EXECUÇÃO (make_kpi_report.py)")
    print("=" * 78)
    print(f"Período: {args.start} a {args.end}")
    print(f"Min. análises: {getattr(args, 'min_analises', 'N/A')}")
    print(f"Fluxo: {fluxo}")

    if top10:
        if peritos_csv_path:
            print(f"Seleção Top10: manifesto (ordem preservada; n≥{args.min_analises})")
            print(f"  • peritos_csv: {peritos_csv_path}")
        else:
            print(f"Seleção Top10: ranking interno (n≥{args.min_analises})")
            print(f"  • rank-by efetivo: {eff_rank}")
            print(f"  • (derivado de --rank-by ou --fluxo)")
        if scope_csv_path:
            print(f"  • scope_csv: {scope_csv_path}")
    else:
        print(f"Relatório individual para: {getattr(args, 'perito', '(n/d)')}")

    print(f"Apêndice R: {'ON' if getattr(args, 'r_appendix', False) else 'OFF'}")
    print(f"Exportações: org={bool(getattr(args, 'export_org', False))} "
          f"pdf={bool(getattr(args, 'export_pdf', False))} "
          f"add_comments={bool(getattr(args, 'add_comments', False))}")

    total = len(planned_cmds)
    n_r   = sum(1 for c in planned_cmds if _detect_is_r(c))
    n_py  = total - n_r
    print(f"Total de comandos: {total} (R={n_r}, Python={n_py})")

    # Ambiente que seria injetado nos R scripts (quando top10)
    if top10 and getattr(args, "r_appendix", False):
        env = {"FLUXO": fluxo}
        if peritos_csv_path:
            env["PERITOS_CSV"] = peritos_csv_path
        if scope_csv_path:
            env["SCOPE_CSV"] = scope_csv_path
        print("\nAmbiente R sugerido (env):")
        for k, v in env.items():
            print(f"  {k}={v}")

    print("\n-- Lista de comandos --")
    for i, cmd in enumerate(planned_cmds, 1):
        tag = "[R]" if _detect_is_r(cmd) else "[PY]"
        try:
            line = " ".join(shlex.quote(str(x)) for x in cmd)
        except Exception:
            line = " ".join(map(str, cmd))
        print(f"{i:02d}. {tag} {line}")
    print("-- fim do plano --\n")

def _run_rchecks_for_selection(args, peritos_csv: Optional[str], plan_only: bool = False):
    """
    Executa os R checks conforme a seleção:
      - Top10 legacy (sem peritos_csv): roda SOMENTE os R checks de GRUPO (RCHECK_GROUP_SCRIPTS).
      - Caso geral (peritos_csv presente OU perito único via CLI): roda os R checks INDIVIDUAIS (RCHECK_SCRIPTS) 1x por perito.
    Evita duplicação: nunca dispara grupo e individuais no mesmo ciclo.
    """
    if not getattr(args, "r_appendix", False):
        return

    _preflight_r(args.r_bin)

    # Se não recebemos peritos_csv por parâmetro, tenta usar o do CLI (quando existir)
    if not peritos_csv and getattr(args, "peritos_csv", None):
        peritos_csv = args.peritos_csv

    # ─────────────────────────── Modo Top10 legacy (sem peritos_csv) ───────────────────────────
    # Mantém apenas os checks de GRUPO. Não roda os individuais aqui.
    if getattr(args, "top10", False) and not peritos_csv:
        for rname, opts in RCHECK_GROUP_SCRIPTS:
            rfile = rscript_path(rname)
            outflag = _detect_r_out_flag(rfile) or "--out-dir"
            cmd = [
                args.r_bin, rfile,
                "--db", DB_PATH,
                "--start", args.start, "--end", args.end,
                outflag, EXPORT_DIR
            ]
            # defaults opcionais do script (ex.: thresholds)
            for k, v in (opts.get("defaults") or {}).items():
                cmd += [k, str(v)]
            _run_or_dry(cmd, plan_only)
        return  # encerra aqui para não duplicar com individuais

    # ─────────────────────────── Caso geral: por perito ───────────────────────────
    peritos: list[str] = []

    # Lista a partir do CSV materializado (TopK, all-matching, etc.)
    if peritos_csv and os.path.exists(peritos_csv):
        try:
            dfp = pd.read_csv(peritos_csv)
            if "nomePerito" in dfp.columns:
                peritos = sorted(dfp["nomePerito"].dropna().astype(str).unique().tolist())
            else:
                print(f"[WARN] {peritos_csv} não possui coluna 'nomePerito'; nenhum perito carregado.")
        except Exception as e:
            print(f"[WARN] Falha lendo {peritos_csv}: {e}")

    # Fallback: perito único via CLI
    if not peritos and getattr(args, "perito", None):
        peritos = [args.perito]

    if not peritos:
        print("[INFO] Sem peritos para R-checks em seleção.")
        return

    # Executa os checks INDIVIDUAIS (01..08) para cada perito selecionado
    for nome in peritos:
        for rname, opts in RCHECK_SCRIPTS:
            rfile = rscript_path(rname)
            outflag = _detect_r_out_flag(rfile) or "--out-dir"
            cmd = [
                args.r_bin, rfile,
                "--db", DB_PATH,
                "--start", args.start, "--end", args.end,
                outflag, EXPORT_DIR,
                "--perito", nome
            ]
            for k, v in (opts.get("defaults") or {}).items():
                cmd += [k, str(v)]
            _run_or_dry(cmd, plan_only)


# ────────────────────────────────────────────────────────────────────────────────
# Coleta de saídas R (fallback)
# ────────────────────────────────────────────────────────────────────────────────
def collect_r_outputs_to_export():
    """Copia rcheck*.{png,md,org} de r_checks/ para exports/ (caso gerados lá)."""
    os.makedirs(EXPORT_DIR, exist_ok=True)
    patterns = ["rcheck*.png", "rcheck*.md", "rcheck*.org", "*top10*.png", "*top10*.md", "*top10*.org"]
    moved = 0
    for pat in patterns:
        for src in glob(os.path.join(RCHECK_DIR, pat)):
            dst = os.path.join(EXPORT_DIR, os.path.basename(src))
            try:
                shutil.copy2(src, dst)
                moved += 1
            except Exception:
                pass
    if moved:
        print(f"[INFO] R-outputs coletados de r_checks/ → exports/: {moved} arquivo(s).")

# ────────────────────────────────────────────────────────────────────────────────
# Limpeza, cópia e organização de artefatos (.png, .org, .md)
# ────────────────────────────────────────────────────────────────────────────────
def _cleanup_exports_for_perito(safe_perito: str):
    """Remove do exports/ os artefatos pendentes do perito (para evitar mistura entre execuções)."""
    for f in glob(os.path.join(EXPORT_DIR, f"*_{safe_perito}.*")):
        try: os.remove(f)
        except Exception: pass
    for f in glob(os.path.join(EXPORT_DIR, f"*{safe_perito}.*")):
        if "top10" in os.path.basename(f).lower():
            continue
        try: os.remove(f)
        except Exception: pass

def _cleanup_exports_top10():
    """Remove do exports/ os artefatos pendentes de top10."""
    for f in glob(os.path.join(EXPORT_DIR, "*_top10*.*")):
        try: os.remove(f)
        except Exception: pass
    for f in glob(os.path.join(EXPORT_DIR, "*top10*.*")):
        try: os.remove(f)
        except Exception: pass

def _move_md_generic_to(markdown_dir: str, pattern: str = "*.md"):
    """Move quaisquer .md remanescentes de exports/ para a pasta markdown/."""
    os.makedirs(markdown_dir, exist_ok=True)
    for src in glob(os.path.join(EXPORT_DIR, pattern)):
        base = os.path.basename(src)
        dst  = os.path.join(markdown_dir, base)
        try:
            shutil.copy2(src, dst)
            os.remove(src)
        except Exception:
            pass

def copiar_artefatos_perito(perito: str, imgs_dir: str, comments_dir: str, orgs_dir: str = None, markdown_dir: str = None):
    """
    Copia artefatos do perito de exports/ para as pastas do relatório:
    - imgs/      (PNGs)
    - comments/  (comentários .org e conversões de .md→.org)
    - orgs/      (.org auxiliares dos scripts, com links normalizados)
    - markdown/  (NOVO: todos os .md são preservados aqui)
    """
    if orgs_dir is None:
        orgs_dir = os.path.join(os.path.dirname(imgs_dir), "orgs")
    if markdown_dir is None:
        markdown_dir = os.path.join(os.path.dirname(imgs_dir), "markdown")

    os.makedirs(imgs_dir, exist_ok=True)
    os.makedirs(comments_dir, exist_ok=True)
    os.makedirs(orgs_dir, exist_ok=True)
    os.makedirs(markdown_dir, exist_ok=True)

    safe = _safe(perito)

    # PNGs
    for pat in (f"*_{safe}.png", f"*{safe}.png"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            if "top10" in base.lower():
                continue
            shutil.copy2(src, os.path.join(imgs_dir, base))
            try: os.remove(src)
            except Exception: pass

    # Comentários já em .org
    for pat in (f"*_{safe}_comment.org", f"*{safe}_comment.org"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            if "top10" in base.lower():
                continue
            with open(src, encoding="utf-8") as f:
                content = f.read()
            dst = os.path.join(comments_dir, base)
            with open(dst, "w", encoding="utf-8") as g:
                g.write(content)
            try: os.remove(src)
            except Exception: pass

    # Comentários em .md → converte para .org (para o relatório) e também MOVE o .md para markdown/
    for pat in (f"*_{safe}_comment.md", f"*{safe}_comment.md", f"*_{safe}.md", f"*{safe}.md"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            if "top10" in base.lower():
                continue
            # 1) converter .md -> .org para uso no relatório
            try:
                with open(src, encoding="utf-8") as f:
                    md_text = f.read()
                org_text = markdown_para_org(md_text)
                org_text = "\n".join(ln for ln in org_text.splitlines() if not ln.strip().lower().startswith('#+title')).strip()
                dst_org = os.path.join(comments_dir, os.path.splitext(base)[0] + ".org")
                with open(dst_org, "w", encoding="utf-8") as g:
                    g.write(org_text + "\n")
            except Exception as e:
                print(f"[AVISO] Falha na conversão .md→.org de {base}: {e}")

            # 2) preservar o .md em markdown/
            try:
                shutil.copy2(src, os.path.join(markdown_dir, base))
                os.remove(src)
            except Exception:
                pass

    # ORGs auxiliares dos scripts (links normalizados para ../imgs/)
    for pat in (f"*_{safe}.org", f"*{safe}.org"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            if "top10" in base.lower():
                continue
            with open(src, encoding="utf-8") as f:
                content = f.read()
            content = _rewrite_org_image_links(content, imgs_rel_prefix="../imgs/")
            dst = os.path.join(orgs_dir, base)
            with open(dst, "w", encoding="utf-8") as g:
                g.write(content)
            try: os.remove(src)
            except Exception: pass

    # Por fim, move qualquer outro .md remanescente
    _move_md_generic_to(markdown_dir)

def copiar_artefatos_top10(imgs_dir: str, comments_dir: str, orgs_dir: str = None, markdown_dir: str = None):
    """
    Copia artefatos do grupo top10:
    - imgs/      (PNGs)
    - comments/  (comentários .org e conversões de .md→.org)
    - orgs/      (.org auxiliares dos scripts, com links normalizados)
    - markdown/  (NOVO: todos os .md são preservados aqui)
    """
    if orgs_dir is None:
        orgs_dir = os.path.join(os.path.dirname(imgs_dir), "orgs")
    if markdown_dir is None:
        markdown_dir = os.path.join(os.path.dirname(imgs_dir), "markdown")

    os.makedirs(imgs_dir, exist_ok=True)
    os.makedirs(comments_dir, exist_ok=True)
    os.makedirs(orgs_dir, exist_ok=True)
    os.makedirs(markdown_dir, exist_ok=True)

    # PNGs
    for pat in ("*_top10*.png", "*top10*.png"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            shutil.copy2(src, os.path.join(imgs_dir, base))
            try: os.remove(src)
            except Exception: pass

    # Comentários .org (grupo)
    for pat in ("*_top10*_comment.org", "*top10*_comment.org"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            shutil.copy2(src, os.path.join(comments_dir, base))
            try: os.remove(src)
            except Exception: pass

    # Comentários .md → converte p/ .org e preserva .md em markdown/
    for pat in ("*_top10*_comment.md", "*top10*_comment.md", "*_top10*.md", "*top10*.md"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            try:
                with open(src, encoding="utf-8") as f:
                    md_text = f.read()
                org_text = markdown_para_org(md_text)
                org_text = "\n".join(ln for ln in org_text.splitlines() if not ln.strip().lower().startswith('#+title')).strip()
                dst_org = os.path.join(comments_dir, os.path.splitext(base)[0] + ".org")
                with open(dst_org, "w", encoding="utf-8") as g:
                    g.write(org_text + "\n")
            except Exception as e:
                print(f"[AVISO] Falha na conversão .md→.org de {base}: {e}")
            try:
                shutil.copy2(src, os.path.join(markdown_dir, base))
                os.remove(src)
            except Exception:
                pass

    # ORGs auxiliares (links normalizados)
    for pat in ("*_top10*.org", "*top10*.org"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            with open(src, encoding="utf-8") as f:
                content = f.read()
            content = _rewrite_org_image_links(content, imgs_rel_prefix="../imgs/")
            dst = os.path.join(orgs_dir, base)
            with open(dst, "w", encoding="utf-8") as g:
                g.write(content)
            try: os.remove(src)
            except Exception: pass

    # Move quaisquer .md restantes
    _move_md_generic_to(markdown_dir)

def copiar_artefatos_weekday2weekend(imgs_dir: str, comments_dir: str, orgs_dir: str = None):
    """
    Move o panorama weekday→weekend:
      - PNG: rcheck_weekday_to_weekend_by_cr.png → imgs/
      - .org: rcheck_weekday_to_weekend_table.org, rcheck_weekday_to_weekend_protocols.org → orgs/
      - .org de comentário → comments/
    """
    if orgs_dir is None:
        orgs_dir = os.path.join(os.path.dirname(imgs_dir), "orgs")
    os.makedirs(imgs_dir, exist_ok=True)
    os.makedirs(comments_dir, exist_ok=True)
    os.makedirs(orgs_dir, exist_ok=True)

    for fname in ("rcheck_weekday_to_weekend_by_cr.png",):
        src = os.path.join(EXPORT_DIR, fname)
        dst = os.path.join(imgs_dir, fname)
        if os.path.exists(src):
            shutil.copy2(src, dst)
            try: os.remove(src)
            except Exception: pass

    for fname in ("rcheck_weekday_to_weekend_table.org", "rcheck_weekday_to_weekend_protocols.org"):
        src = os.path.join(EXPORT_DIR, fname)
        dst = os.path.join(orgs_dir, fname)
        if os.path.exists(src):
            try:
                with open(src, encoding="utf-8") as f:
                    content = f.read()
                content = _rewrite_org_image_links(content, imgs_rel_prefix="../imgs/")
                with open(dst, "w", encoding="utf-8") as g:
                    g.write(content)
            except Exception as e:
                print(f"[AVISO] Falha ao normalizar {fname}: {e}. Copiando bruto.")
                shutil.copy2(src, dst)
            finally:
                try: os.remove(src)
                except Exception: pass

    for fname in ("rcheck_weekday_to_weekend_table_comment.org",):
        src = os.path.join(EXPORT_DIR, fname)
        dst = os.path.join(comments_dir, fname)
        if os.path.exists(src):
            shutil.copy2(src, dst)
            try: os.remove(src)
            except Exception: pass

# ────────────────────────────────────────────────────────────────────────────────
# Estatísticas rápidas (cabeçalho)
# ────────────────────────────────────────────────────────────────────────────────
def get_summary_stats(perito, start, end):
    """Retorna (total_tarefas, pct_nc, CR, DR) do perito no período."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT cr, dr FROM peritos WHERE nomePerito = ?""", (perito,))
    row = cur.fetchone()
    cr, dr = (row if row else ("-", "-"))
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(
                CASE
                    WHEN CAST(IFNULL(a.conformado, 1) AS INTEGER) = 0 THEN 1
                    WHEN TRIM(IFNULL(a.motivoNaoConformado, '')) <> ''
                         AND CAST(IFNULL(a.motivoNaoConformado, '0') AS INTEGER) <> 0 THEN 1
                    ELSE 0
                END
            ) AS nc_count
        FROM analises a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        WHERE p.nomePerito = ?
          AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
    """, (perito, start, end))
    total, nc_count = cur.fetchone() or (0, 0)
    conn.close()
    pct_nc = (nc_count or 0) / (total or 1) * 100.0
    return total or 0, pct_nc, cr, dr

# ────────────────────────────────────────────────────────────────────────────────
# Apêndices por perito
# ────────────────────────────────────────────────────────────────────────────────
def gerar_apendice_nc(perito, start, end):
    """Retorna DataFrame com protocolos NC por motivo para o perito no período."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT a.protocolo, pr.motivo AS motivo_text
        FROM analises a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        JOIN protocolos pr ON a.protocolo = pr.protocolo
        WHERE p.nomePerito = ?
          AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
          AND (
                CAST(IFNULL(a.conformado, 1) AS INTEGER) = 0
                OR (
                    TRIM(IFNULL(a.motivoNaoConformado, '')) <> ''
                    AND CAST(IFNULL(a.motivoNaoConformado, '0') AS INTEGER) <> 0
                )
              )
        ORDER BY a.protocolo
    """, conn, params=(perito, start, end))
    conn.close()
    return df

def _infer_datetime_cols_for_durations(conn):
    """
    Descobre colunas de início/fim nas tabelas para cálculo de duração em segundos.
    Retorna (ini_col, fim_col). Usa os mesmos candidatos do restante do app.
    """
    cols = [r[1] for r in conn.execute("PRAGMA table_info(analises)").fetchall()]
    ini_col = None
    for cand in ("dataHoraIniPericia", "dataHoraIniAnalise", "dataHoraIni"):
        if cand in cols:
            ini_col = cand
            break
    fim_col = _detect_end_datetime_column(conn)
    return ini_col, fim_col

def gerar_apendice_le15s(perito: str, start: str, end: str, max_seconds: int = 3600):
    """
    Retorna DataFrame com protocolos do perito no período com duração válida (0<dur<=max_seconds)
    e <= FIFTEEN_THRESHOLD (padrão 15s).
    """
    thr = int(FIFTEEN_THRESHOLD)
    conn = sqlite3.connect(DB_PATH)
    try:
        ini_col, fim_col = _infer_datetime_cols_for_durations(conn)
        if not ini_col or not fim_col:
            return pd.DataFrame(columns=["protocolo","dur_s"])
        sql = f"""
            SELECT
                a.protocolo AS protocolo,
                CAST(ROUND((julianday({'a.'+fim_col}) - julianday({ 'a.'+ini_col })) * 86400.0, 3) AS REAL) AS dur_s
            FROM analises a
            JOIN peritos p ON a.siapePerito = p.siapePerito
            WHERE p.nomePerito = ?
              AND date(a.{ini_col}) BETWEEN ? AND ?
              AND { 'a.'+fim_col } IS NOT NULL
              AND ( (julianday({ 'a.'+fim_col }) - julianday({ 'a.'+ini_col })) * 86400.0 ) > 0
              AND ( (julianday({ 'a.'+fim_col }) - julianday({ 'a.'+ini_col })) * 86400.0 ) <= ?
              AND ( (julianday({ 'a.'+fim_col }) - julianday({ 'a.'+ini_col })) * 86400.0 ) <= ?
            ORDER BY dur_s ASC, protocolo
        """
        df = pd.read_sql(sql, conn, params=(perito, start, end, max_seconds, thr))
        return df
    finally:
        conn.close()

def gerar_apendice_prod50(perito: str, start: str, end: str, max_seconds: int = 3600):
    """
    Se a produtividade do perito no período for >= PRODUCTIVITY_THRESHOLD (análises/h),
    retorna DataFrame com todos os protocolos válidos (0<dur<=max_seconds) que compõem o numerador.
    Caso contrário, retorna DF vazio.
    """
    thr_prod = float(PRODUCTIVITY_THRESHOLD)
    conn = sqlite3.connect(DB_PATH)
    try:
        ini_col, fim_col = _infer_datetime_cols_for_durations(conn)
        if not ini_col or not fim_col:
            return pd.DataFrame(columns=["protocolo","dur_s"])

        # 1) Coleta todas as durações válidas do período (já aplicando filtro de 0<dur<=max_seconds)
        base_sql = f"""
            SELECT
                a.protocolo AS protocolo,
                CAST(ROUND((julianday({ 'a.'+fim_col }) - julianday({ 'a.'+ini_col })) * 86400.0, 3) AS REAL) AS dur_s
            FROM analises a
            JOIN peritos p ON a.siapePerito = p.siapePerito
            WHERE p.nomePerito = ?
              AND date(a.{ini_col}) BETWEEN ? AND ?
              AND { 'a.'+fim_col } IS NOT NULL
              AND ( (julianday({ 'a.'+fim_col }) - julianday({ 'a.'+ini_col })) * 86400.0 ) > 0
              AND ( (julianday({ 'a.'+fim_col }) - julianday({ 'a.'+ini_col })) * 86400.0 ) <= ?
        """
        df = pd.read_sql(base_sql, conn, params=(perito, start, end, max_seconds))
        if df.empty:
            return df

        n = len(df)
        hours = df["dur_s"].sum() / 3600.0
        prod = (n / hours) if hours > 0 else 0.0

        # 2) Se atingiu o limiar, devolve a lista completa dos protocolos válidos (numerador)
        if prod >= thr_prod:
            return df.sort_values(["dur_s","protocolo"]).reset_index(drop=True)
        return pd.DataFrame(columns=["protocolo","dur_s"])
    finally:
        conn.close()

def gerar_apendice_sobreposicao(perito: str, start: str, end: str, max_seconds: int = 3600):
    """
    Identifica protocolos do perito cuja janela [ini,fim] se sobrepõe a pelo menos outro protocolo
    do MESMO perito no período. Aplica filtro de duração válida (0<dur<=max_seconds).
    Retorna DF com colunas: protocolo, ini, fim, overlapped_with (lista CSV de protocolos que cruzam).
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        ini_col, fim_col = _infer_datetime_cols_for_durations(conn)
        if not ini_col or not fim_col:
            return pd.DataFrame(columns=["protocolo","ini","fim","overlapped_with"])

        # Janela base do perito com durações válidas
        per_sql = f"""
            SELECT
                a.protocolo AS protocolo,
                a.{ini_col} AS ini,
                a.{fim_col} AS fim
            FROM analises a
            JOIN peritos p ON a.siapePerito = p.siapePerito
            WHERE p.nomePerito = ?
              AND date(a.{ini_col}) BETWEEN ? AND ?
              AND a.{fim_col} IS NOT NULL
              AND ( (julianday(a.{fim_col}) - julianday(a.{ini_col})) * 86400.0 ) > 0
              AND ( (julianday(a.{fim_col}) - julianday(a.{ini_col})) * 86400.0 ) <= ?
        """
        per = pd.read_sql(per_sql, conn, params=(perito, start, end, max_seconds))
        if per.empty:
            return pd.DataFrame(columns=["protocolo","ini","fim","overlapped_with"])

        # Junta consigo mesmo para achar interseções: NOT (a.fim <= b.ini OR b.fim <= a.ini)
        # Evita pares duplicados impondo a ordem por protocolo (a.protocolo < b.protocolo)
        per["ini"] = pd.to_datetime(per["ini"], errors="coerce")
        per["fim"] = pd.to_datetime(per["fim"], errors="coerce")
        per = per.dropna(subset=["ini","fim"]).sort_values(["ini","fim","protocolo"]).reset_index(drop=True)

        if per.empty:
            return pd.DataFrame(columns=["protocolo","ini","fim","overlapped_with"])

        # Índice para acelerar busca (varre com janela deslizante)
        out_map = { str(row.protocolo): set() for _, row in per.iterrows() }
        starts = per["ini"].tolist()
        ends   = per["fim"].tolist()
        protos = per["protocolo"].astype(str).tolist()

        j0 = 0
        for i in range(len(per)):
            # avança j0 enquanto o fim de j0 terminar antes (ou igual) ao início de i
            while j0 < len(per) and ends[j0] <= starts[i]:
                j0 += 1
            # compara i com [j0..] enquanto o início do candidato < fim de i
            j = j0
            while j < len(per) and starts[j] < ends[i]:
                if i != j:
                    pi, pj = protos[i], protos[j]
                    # registra sobreposição para ambos
                    out_map[pi].add(pj)
                    out_map[pj].add(pi)
                j += 1

        rows = []
        for _, row in per.iterrows():
            pi = str(row.protocolo)
            peers = sorted(out_map.get(pi) or [])
            if peers:
                rows.append({
                    "protocolo": pi,
                    "ini": row.ini.strftime("%Y-%m-%d %H:%M"),
                    "fim": row.fim.strftime("%Y-%m-%d %H:%M"),
                    "overlapped_with": ", ".join(peers),
                })

        return pd.DataFrame(rows, columns=["protocolo","ini","fim","overlapped_with"])
    finally:
        conn.close()

def gerar_r_apendice_comments_if_possible(perito: str, imgs_dir: str, comments_dir: str, start: str, end: str):
    """Gera comentários GPT para R checks individuais (se utils.comentarios estiver disponível)."""
    if comentar_r_apendice is None:
        return
    safe = _safe(perito)
    r_pngs = glob(os.path.join(imgs_dir, f"rcheck_*_{safe}.png"))
    r_pngs.sort(key=lambda p: _rcheck_perito_rank(p))
    for png in r_pngs:
        base = os.path.basename(png)
        stem = os.path.splitext(base)[0]
        md_out = os.path.join(comments_dir, f"{stem}_comment.md")
        try:
            texto = comentar_r_apendice(
                titulo="Apêndice estatístico (R) — Perito",
                imagem_rel=f"imgs/{base}",
                perito=perito,
                start=start,
                end=end
            )
            with open(md_out, "w", encoding="utf-8") as f:
                f.write(texto)
        except Exception as e:
            print(f"[AVISO] Falha ao gerar comentário GPT do R check {base}: {e}")

def gerar_r_apendice_group_comments_if_possible(imgs_dir: str, comments_dir: str, start: str, end: str):
    """Gera comentários GPT para R checks do grupo Top10 (se disponível)."""
    if comentar_r_apendice is None:
        return
    r_pngs = glob(os.path.join(imgs_dir, "rcheck_top10_*.png"))
    r_pngs.sort(key=lambda p: _rcheck_group_rank(p))
    for png in r_pngs:
        base = os.path.basename(png)
        stem = os.path.splitext(base)[0]
        md_out = os.path.join(comments_dir, f"{stem}_comment.md")
        try:
            texto = comentar_r_apendice(
                titulo="Apêndice estatístico (R) — Top 10 (grupo)",
                imagem_rel=f"imgs/{base}",
                perito=None,
                start=start,
                end=end
            )
            with open(md_out, "w", encoding="utf-8") as f:
                f.write(texto)
        except Exception as e:
            print(f"[AVISO] Falha ao gerar comentário GPT do R check de grupo {base}: {e}")

# ────────────────────────────────────────────────────────────────────────────────
# Montagem dos .org (perito e grupo)
# ────────────────────────────────────────────────────────────────────────────────
def gerar_org_perito(
    perito: str,
    start: str,
    end: str,
    add_comments: bool,
    imgs_dir: str,
    comments_dir: str,
    output_dir: str,
    orgs_dir: str | None = None,
):
    """
    Gera o .org individual do perito com:
      - Header ressumido (tarefas, %NC, CR/DR)
      - Gráficos principais + comentários
      - BLOCO NOVO: Detalhamento dos KPIs (protocolos por critério acionado)
         • ≤15s
         • Produtividade ≥ 50/h (por janela-hora)
         • Sobreposição
         • %NC do perito ≥ 2× média nacional  → lista protocolos NC
      - Apêndice: Protocolos NC por motivo
      - Apêndice estatístico (R) + comentários (se disponíveis)
      - Protocolos transferidos (indicador complementar)
    """
    import sqlite3
    import pandas as pd
    import numpy as np
    import os
    from glob import glob

    safe = _safe(perito)
    out_dir = orgs_dir or output_dir
    os.makedirs(out_dir, exist_ok=True)
    org_path = os.path.join(out_dir, f"{safe}.org")

    # prefixo correto de imagens conforme destino
    imgs_prefix = "../imgs/" if orgs_dir else "imgs/"

    # -------------------------------------------------------------------------
    # Cabeçalho: estatísticas rápidas
    # -------------------------------------------------------------------------
    lines = [f"** {perito}"]
    total, pct_nc, cr, dr = get_summary_stats(perito, start, end)
    lines += [f"- Tarefas: {total}", f"- % NC: {pct_nc:.1f}", f"- CR: {cr} | DR: {dr}", ""]

    # -------------------------------------------------------------------------
    # Gráficos principais + comentários
    # -------------------------------------------------------------------------
    all_pngs = glob(os.path.join(imgs_dir, f"*{safe}.png"))
    main_pngs = [p for p in all_pngs if not os.path.basename(p).lower().startswith("rcheck_")]
    main_pngs.sort(key=_png_rank_main)

    for png in main_pngs:
        base = os.path.basename(png)
        lines += [
            "#+ATTR_LATEX: :placement [H] :width \\linewidth",
            f"#+CAPTION: {_nice_caption(base)}",
            f"[[file:{imgs_prefix}{base}]]",
        ]
        if add_comments:
            stem = os.path.splitext(base)[0]
            quote_lines = _inject_comment_for_stem(stem, comments_dir, output_dir, imgs_dir=imgs_dir)
            lines += quote_lines
        lines.append("\n#+LATEX: \\newpage\n")

    # -------------------------------------------------------------------------
    # NOVO: Detalhamento dos KPIs (protocolos por critério)
    # -------------------------------------------------------------------------
    # Convenções:
    #  - duração válida: 0 < dur ≤ 3600s
    #  - ≤15s: dur ≤ FIFTEEN_THRESHOLD
    #  - produtividade ≥50/h: janelas por strftime('%Y-%m-%d %H'), contando análises válidas na hora; se count>=50,
    #    todos os protocolos pertencentes à(s) hora(s) elegíveis compõem a lista.
    #  - sobreposição: self-join por (ini,fim) com intersecção de intervalos para o mesmo perito.
    #  - NC nacional: média robusta como (conformado=0) OU (motivoNaoConformado != '' E CAST(...)!=0).
    #    Critério ativa se pct_perito >= 2× pct_nacional. Protocolos listados = os NC do perito.
    conn = sqlite3.connect(DB_PATH)

    # Detecta coluna de fim (pode variar por base)
    end_col = _detect_end_datetime_column(conn) or "dataHoraFimPericia"

    # Tabela-base do perito com durações
    df = pd.read_sql(
        f"""
        SELECT
            a.protocolo                       AS protocolo,
            a.dataHoraIniPericia              AS ini,
            {end_col}                         AS fim,
            a.conformado                      AS conformado,
            a.motivoNaoConformado             AS motivoNC
        FROM analises a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        WHERE p.nomePerito = ?
          AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
        """,
        conn, params=(perito, start, end)
    )

    # Nacional (%NC média no período)
    nat = pd.read_sql(
        f"""
        SELECT
            COUNT(*) AS total,
            SUM(
              CASE
                WHEN CAST(IFNULL(a.conformado, 1) AS INTEGER) = 0 THEN 1
                WHEN TRIM(IFNULL(a.motivoNaoConformado,'')) <> '' AND
                     CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0 THEN 1
                ELSE 0
              END
            ) AS nc_count
        FROM analises a
        WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
        """,
        conn, params=(start, end)
    )
    conn.close()

    # Conversões de tempo
    df["ini"] = pd.to_datetime(df["ini"], errors="coerce")
    df["fim"] = pd.to_datetime(df["fim"], errors="coerce")
    df["dur"] = (df["fim"] - df["ini"]).dt.total_seconds()
    # duração válida: (0, 3600]
    df_valid = df[(df["dur"] > 0) & (df["dur"] <= 3600)].copy()

    # --------- KPI 1: ≤15 segundos -------------
    thr_15 = float(FIFTEEN_THRESHOLD)
    prot_le15 = sorted(df_valid.loc[df_valid["dur"] <= thr_15, "protocolo"].astype(str).unique().tolist())
    n_le15 = len(prot_le15)

    # --------- KPI 2: Produtividade ≥ 50/h (por janela-hora) -------------
    thr_prod = float(PRODUCTIVITY_THRESHOLD)
    prots_prod = []
    if not df_valid.empty:
        # bucket hora local do início
        df_valid["hora_bucket"] = df_valid["ini"].dt.strftime("%Y-%m-%d %H")
        # conta análises por bucket
        hits = df_valid.groupby("hora_bucket")["protocolo"].count().reset_index(name="n")
        horas_elegiveis = set(hits.loc[hits["n"] >= thr_prod, "hora_bucket"].tolist())
        if horas_elegiveis:
            prots_prod = (
                df_valid.loc[df_valid["hora_bucket"].isin(horas_elegiveis), "protocolo"]
                .astype(str).unique().tolist()
            )
    prot_prod = sorted(prots_prod)
    n_prod = len(prot_prod)

    # --------- KPI 3: Sobreposição -------------
    # protocolos do perito que tiveram interseção temporal com outro protocolo do mesmo perito
    prot_overlap = []
    if len(df_valid) > 1:
        x = df_valid[["protocolo", "ini", "fim"]].dropna().copy()
        # junta consigo mesmo, evita mesma linha
        x["k"] = 1
        m = x.merge(x, on="k", suffixes=("_a", "_b"))
        m = m[m["protocolo_a"] != m["protocolo_b"]]
        # interseção: ini_a < fim_b AND ini_b < fim_a
        ov = m[(m["ini_a"] < m["fim_b"]) & (m["ini_b"] < m["fim_a"])]
        if not ov.empty:
            prot_overlap = sorted(
                pd.unique(ov[["protocolo_a", "protocolo_b"]].astype(str).values.ravel()).tolist()
            )
    n_overlap = len(prot_overlap)

    # --------- KPI 4: %NC ≥ 2× média nacional -------------
    nat_total = float(nat.iloc[0]["total"] or 0.0)
    nat_nc = float(nat.iloc[0]["nc_count"] or 0.0)
    nat_pct = (nat_nc / nat_total * 100.0) if nat_total > 0 else 0.0

    # perito NC robusto
    df["nc_flag"] = (
        (df["conformado"].fillna(1).astype("Int64") == 0)
        | (
            df["motivoNC"].fillna("").astype(str).str.strip().ne("")
            & df["motivoNC"].fillna("0").astype(str).str.strip().astype(str).ne("0")
        )
    )
    per_nc_total = int(df["nc_flag"].sum())
    per_total = int(len(df))
    per_pct = (per_nc_total / per_total * 100.0) if per_total > 0 else 0.0

    crit_nc_2x = per_pct >= (2.0 * nat_pct if nat_pct > 0 else 0.0)
    prot_nc = sorted(df.loc[df["nc_flag"], "protocolo"].astype(str).unique().tolist())
    n_nc = len(prot_nc)

    # Bloco textual no .org
    lines.append("*** Detalhamento dos KPIs (protocolos por critério)")
    lines.append("")
    lines.append("_Critérios e pesos do ICRA; listagem de protocolos somente informativa (não soma pontos adicional)._")
    lines.append("")

    # ≤15s
    lines.append(f"- *≤ {int(thr_15)}s* — protocolos ({n_le15}): " + (", ".join(prot_le15) if n_le15 else "_nenhum_"))

    # Produtividade
    if n_prod:
        lines.append(f"- *Produtividade ≥ {int(thr_prod)}/h* (por janela-hora) — protocolos ({n_prod}): " + ", ".join(prot_prod))
    else:
        lines.append(f"- *Produtividade ≥ {int(thr_prod)}/h* (por janela-hora) — protocolos (0): _nenhum_")

    # Sobreposição
    if n_overlap:
        lines.append(f"- *Sobreposição* — protocolos envolvidos ({n_overlap}): " + ", ".join(prot_overlap))
    else:
        lines.append("- *Sobreposição* — protocolos (0): _nenhum_")

    # NC ≥ 2× média nacional
    if crit_nc_2x:
        lines.append(f"- *%NC do perito ≥ 2× média nacional* — média BR={nat_pct:.1f}%, perito={per_pct:.1f}% → protocolos NC ({n_nc}): " + (", ".join(prot_nc) if n_nc else "_nenhum_"))
    else:
        lines.append(f"- *%NC do perito ≥ 2× média nacional* — **não acionado** (BR={nat_pct:.1f}% | perito={per_pct:.1f}%).")
    lines.append("\n#+LATEX: \\newpage\n")

    # -------------------------------------------------------------------------
    # Protocolos Transferidos (complementar – já existia)
    # -------------------------------------------------------------------------
    _append_protocol_transfers_perito_block_if_any(
        lines, perito, start, end, relatorio_dir=output_dir, heading_level="***",
        link_prefix="../" if orgs_dir else ""
    )

    # -------------------------------------------------------------------------
    # Apêndice: Protocolos NC por motivo
    # -------------------------------------------------------------------------
    apdf = gerar_apendice_nc(perito, start, end)
    if not apdf.empty:
        lines.append(f"*** Apêndice: Protocolos Não-Conformados por Motivo")
        grouped = apdf.groupby('motivo_text')['protocolo'].apply(lambda seq: ', '.join(map(str, seq))).reset_index()
        for _, grp in grouped.iterrows():
            lines.append(f"- *{grp['motivo_text']}*: {grp['protocolo']}")
        lines.append("")

    # -------------------------------------------------------------------------
    # Apêndice estatístico (R) — imagens + comentários IA se houver
    # -------------------------------------------------------------------------
    r_pngs = glob(os.path.join(imgs_dir, f"rcheck_*_{safe}.png"))
    r_pngs.sort(key=lambda p: _rcheck_perito_rank(p))
    if r_pngs:
        lines.append(f"*** Apêndice estatístico (R) — {perito}\n")
        for png in r_pngs:
            base = os.path.basename(png)
            lines += [
                "#+ATTR_LATEX: :placement [H] :width \\linewidth",
                f"#+CAPTION: {_nice_caption(base)}",
                f"[[file:{imgs_prefix}{base}]]",
            ]
            if add_comments:
                stem = os.path.splitext(base)[0]
                quote_lines = _inject_comment_for_stem(stem, comments_dir, output_dir, imgs_dir=imgs_dir)
                lines += quote_lines
            lines.append("\n#+LATEX: \\newpage\n")

    # -------------------------------------------------------------------------
    # Grava o .org
    # -------------------------------------------------------------------------
    with open(org_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"✅ Org individual salvo em: {org_path}")
    return org_path


def gerar_org_top10_grupo(start, end, output_dir, imgs_dir, comments_dir, orgs_dir=None):
    """
    Monta o org do grupo Top10 (gráficos + comentários, incluindo R checks do grupo).
    Agora salva em orgs_dir (se informado), corrige prefixo de imagens e evita duplicatas.
    """
    save_dir = orgs_dir or output_dir
    os.makedirs(save_dir, exist_ok=True)
    org_path = os.path.join(save_dir, f"top10_grupo.org")
    imgs_prefix = "../imgs/" if orgs_dir else "imgs/"

    lines = [f"** Top 10 — Gráficos do Grupo ({start} a {end})\n"]

    # ✅ dedup: use set() na união dos padrões
    all_pngs = sorted({
        *glob(os.path.join(imgs_dir, "*_top10*.png")),
        *glob(os.path.join(imgs_dir, "*top10*.png")),
    })

    # ✅ particiona já sem duplicatas e com ordenação estável
    main_pngs = sorted(
        (p for p in all_pngs if "rcheck_" not in os.path.basename(p).lower()),
        key=_png_rank_main
    )
    r_pngs = sorted(
        (p for p in all_pngs if "rcheck_" in os.path.basename(p).lower()),
        key=lambda p: _rcheck_group_rank(p)
    )

    seen_imgs = set()  # ✅ guarda bases já emitidas
    for png in main_pngs + r_pngs:
        base = os.path.basename(png)
        if base in seen_imgs:
            continue
        seen_imgs.add(base)

        lines += [
            "#+ATTR_LATEX: :placement [H] :width \\linewidth",
            f"#+CAPTION: {_nice_caption(base)}",
            f"[[file:{imgs_prefix}{base}]]",
        ]
        stem = os.path.splitext(base)[0]
        quote_lines = _inject_comment_for_stem(stem, comments_dir, output_dir)
        lines += quote_lines
        lines.append("\n#+LATEX: \\newpage\n")

    with open(org_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"✅ Org do grupo Top 10 salvo em: {org_path}")
    return org_path


def gerar_org_individual_consolidado(perito: str, start: str, end: str, relatorio_dir: str):
    import shutil
    safe = _safe(perito)
    orgs_dir = os.path.join(relatorio_dir, "orgs")
    os.makedirs(orgs_dir, exist_ok=True)

    cand_new = os.path.join(orgs_dir, f"{safe}.org")
    cand_old = os.path.join(relatorio_dir, f"{safe}.org")

    if os.path.exists(cand_new):
        perito_org = cand_new
    elif os.path.exists(cand_old):
        try:
            shutil.move(cand_old, cand_new)
            print(f"[MOVE] {cand_old} → {cand_new}")
            perito_org = cand_new
        except Exception as e:
            print(f"[AVISO] Falha ao mover {cand_old} → {cand_new}: {e}. Usando origem antiga.")
            perito_org = cand_old
    else:
        raise FileNotFoundError(f"Org individual não encontrado (new/old): {cand_new} | {cand_old}")

    final_org  = os.path.join(orgs_dir, f"relatorio_{safe}_{start}_a_{end}.org")
    with open(perito_org, "r", encoding="utf-8") as f:
        content = f.read().strip()
    content = _protect_org_text_for_pandoc(content)

    lines = [f"* Relatório individual — {perito} ({start} a {end})", "", content, ""]
    with open(final_org, "w", encoding="utf-8") as g:
        g.write("\n".join(lines))
    print(f"✅ Org consolidado (individual) salvo em: {final_org}")
    return final_org


def gerar_org_individual_consolidado_com_panorama(perito: str, start: str, end: str, relatorio_dir: str):
    """
    Insere panorama global UMA VEZ antes da seção do perito.

    Ajustes:
      - Mesmo comportamento de mover {perito}.org para orgs/ se estiver no topo.
    """
    import shutil

    safe = _safe(perito)
    orgs_dir = os.path.join(relatorio_dir, "orgs")
    os.makedirs(orgs_dir, exist_ok=True)

    cand_new = os.path.join(orgs_dir, f"{safe}.org")
    cand_old = os.path.join(relatorio_dir, f"{safe}.org")

    if os.path.exists(cand_new):
        perito_org = cand_new
    elif os.path.exists(cand_old):
        try:
            shutil.move(cand_old, cand_new)
            print(f"[MOVE] {cand_old} → {cand_new}")
            perito_org = cand_new
        except Exception as e:
            print(f"[AVISO] Falha ao mover {cand_old} → {cand_new}: {e}. Usando origem antiga.")
            perito_org = cand_old
    else:
        raise FileNotFoundError(f"Org individual não encontrado (new/old): {cand_new} | {cand_old}")

    final_org  = os.path.join(relatorio_dir, f"relatorio_{safe}_{start}_a_{end}.org")

    lines = []
    _append_weekday2weekend_panorama_block(
        lines,
        os.path.join(relatorio_dir, "imgs"),
        os.path.join(relatorio_dir, "comments"),
        heading_level="**"
    )

    with open(perito_org, "r", encoding="utf-8") as f:
        content = f.read().strip()
    content = _protect_org_text_for_pandoc(content)
    lines.extend([content, ""])

    with open(final_org, "w", encoding="utf-8") as g:
        g.write("\n".join(lines))
    print(f"✅ Org consolidado (individual) salvo em: {final_org}")
    return final_org


def gerar_org_individual_consolidado_com_panorama_depois(perito: str, start: str, end: str, relatorio_dir: str, perito_org_path: str) -> str:
    imgs_dir     = os.path.join(relatorio_dir, "imgs")
    comments_dir = os.path.join(relatorio_dir, "comments")
    safe_perito  = _safe(perito)
    orgs_dir     = os.path.join(relatorio_dir, "orgs")
    os.makedirs(orgs_dir, exist_ok=True)
    org_final    = os.path.join(orgs_dir, f"relatorio_{safe_perito}_{start}_a_{end}.org")

    lines = [f"* Relatório individual — {perito} ({start} a {end})", ""]
    if perito_org_path and os.path.exists(perito_org_path):
        with open(perito_org_path, encoding="utf-8") as f:
            content = f.read().strip()
        if content:
            lines.append(content)
            lines.append("#+LATEX: \\newpage\n")
    else:
        print(f"[AVISO] Org do perito não encontrado: {perito_org_path}")

    _append_weekday2weekend_panorama_block(lines, imgs_dir, comments_dir, start=start, end=end, heading_level="**", imgs_prefix="../imgs/")

    with open(org_final, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")
    print(f"✅ Org consolidado (individual) salvo em: {org_final}")
    return org_final


# ────────────────────────────────────────────────────────────────────────────────
# Seleção Top 10 & Coorte Extra (funções auxiliares usadas no main)
# ────────────────────────────────────────────────────────────────────────────────
def pegar_10_piores_peritos(start: str, end: str, min_analises: int = 50) -> pd.DataFrame:
    """
    Fluxo A: ordena por scoreFinal (desc). Considera apenas quem tem N >= min_analises.
    Retorna: nomePerito (Top 10).
    """
    with sqlite3.connect(DB_PATH) as conn:
        schema = _detect_schema(conn)
        df_n = _fetch_perito_n_nc(conn, start, end, schema)   # colunas: nomePerito, N, NC
        df_s = _fetch_scores(conn, start, end, schema)        # colunas: nomePerito, scoreFinal, harm

    base = df_n.merge(df_s, on="nomePerito", how="left")
    base = base.loc[base["N"].fillna(0).astype(int) >= int(min_analises)].copy()
    base["scoreFinal"] = base["scoreFinal"].astype(float).fillna(0.0)

    out = base.sort_values(["scoreFinal", "NC", "N"], ascending=[False, False, True]).head(10)
    return out[["nomePerito"]].reset_index(drop=True)


def pegar_top10_harm_first(start: str, end: str, min_analises: int = 50, factor_nc: float = 2.0) -> pd.DataFrame:
    """
    Fluxo B (harm-first):
      1) Gate: %NC perito >= factor_nc × p_BR e N >= min_analises
      2) Ordena por 'harm' (se houver) com fallback em scoreFinal, depois NC desc, N asc.
    Retorna: nomePerito (Top 10).
    """
    with sqlite3.connect(DB_PATH) as conn:
        schema = _detect_schema(conn)
        df_n = _fetch_perito_n_nc(conn, start, end, schema)             # nomePerito, N, NC
        p_br, _, _ = _compute_p_br_and_totals(conn, start, end, schema) # média nacional %NC
        df_s = _fetch_scores(conn, start, end, schema)                  # nomePerito, scoreFinal, harm

    df = df_n.copy()
    df["p_hat"] = df["NC"] / df["N"].replace(0, np.nan)

    gate = df.loc[
        (df["N"].astype(float) >= float(min_analises)) &
        (df["p_hat"].astype(float) >= float(factor_nc) * float(p_br or 0.0))
    ].copy()

    sel = gate.merge(df_s, on="nomePerito", how="left")
    sel["__rank__"] = sel.get("harm", np.nan).astype(float).fillna(
        sel.get("scoreFinal", 0.0).astype(float)
    )

    sel = sel.sort_values(["__rank__", "NC", "N"], ascending=[False, False, True]).head(10)
    return sel[["nomePerito"]].reset_index(drop=True)


def pegar_peritos_nc_altissima(start: str, end: str,
                               nc_threshold: float = 90.0,
                               min_tasks: int = 50) -> pd.DataFrame:
    """
    Retorna peritos com % de não conformidade >= nc_threshold e total de tarefas >= min_tasks
    no período (start..end). A %NC é calculada por perito sobre suas próprias tarefas.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        query = """
            SELECT
                p.nomePerito,
                COUNT(*) AS total,
                SUM(
                    CASE
                        WHEN CAST(IFNULL(a.conformado, 1) AS INTEGER) = 0 THEN 1
                        WHEN TRIM(IFNULL(a.motivoNaoConformado, '')) <> ''
                             AND CAST(IFNULL(a.motivoNaoConformado, '0') AS INTEGER) <> 0 THEN 1
                        ELSE 0
                    END
                ) AS nc_count,
                (SUM(
                    CASE
                        WHEN CAST(IFNULL(a.conformado, 1) AS INTEGER) = 0 THEN 1
                        WHEN TRIM(IFNULL(a.motivoNaoConformado, '')) <> ''
                             AND CAST(IFNULL(a.motivoNaoConformado, '0') AS INTEGER) <> 0 THEN 1
                    END
                ) * 100.0) / COUNT(*) AS pct_nc
            FROM analises a
            JOIN peritos p ON a.siapePerito = p.siapePerito
            WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
            GROUP BY p.nomePerito
            HAVING total >= ? AND pct_nc >= ?
            ORDER BY pct_nc DESC, total DESC
        """
        df = pd.read_sql(query, conn, params=(start, end, min_tasks, nc_threshold))
        return df
    finally:
        conn.close()


def perito_tem_dados(perito: str, start: str, end: str) -> bool:
    """
    True se o perito possui pelo menos 1 tarefa no período.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        count = conn.execute(
            """
            SELECT COUNT(*)
            FROM analises a
            JOIN peritos p ON a.siapePerito = p.siapePerito
            WHERE p.nomePerito = ?
              AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
            """,
            (perito, start, end)
        ).fetchone()[0]
        return count > 0
    finally:
        conn.close()

def _nat_nc_pct_valid(start: str, end: str) -> float:
    """
    %NC nacional no período, mas **apenas** sobre análises com duração válida:
      - coluna de início/fim detectada
      - fim não nulo
      - 0 < duração (s) ≤ 3600
    Definição de NC robusta: conformado==0 OU (motivoNaoConformado != '' e != 0).
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        # Detecta colunas de tempo
        cols = [r[1] for r in conn.execute("PRAGMA table_info(analises)").fetchall()]
        ini_col = next((c for c in ("dataHoraIniPericia","dataHoraIniAnalise","dataHoraIni") if c in cols), None)
        fim_col = _detect_end_datetime_column(conn)
        if not ini_col or not fim_col:
            # Sem colunas → volta 0.0 para não travar o fluxo
            return 0.0

        sql = f"""
            WITH base AS (
              SELECT
                a.{ini_col} AS ini,
                a.{fim_col} AS fim,
                CASE
                  WHEN CAST(IFNULL(a.conformado, 1) AS INTEGER) = 0 THEN 1
                  WHEN TRIM(IFNULL(a.motivoNaoConformado,'')) <> ''
                       AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0 THEN 1
                  ELSE 0
                END AS nc_flag,
                CAST((julianday(a.{fim_col}) - julianday(a.{ini_col})) * 86400.0 AS REAL) AS dur_s
              FROM analises a
              WHERE date(a.{ini_col}) BETWEEN ? AND ?
            ),
            valid AS (
              SELECT * FROM base
              WHERE fim IS NOT NULL AND dur_s IS NOT NULL AND dur_s > 0 AND dur_s <= 3600
            )
            SELECT COUNT(*) AS total, SUM(nc_flag) AS nc_count FROM valid
        """
        row = conn.execute(sql, (start, end)).fetchone()
        total = float(row[0] or 0.0)
        nc = float(row[1] or 0.0)
        return (nc / total * 100.0) if total > 0 else 0.0
    finally:
        conn.close()
        
def _perito_nc_pct_valid_df(start: str, end: str, min_tasks: int = 50) -> pd.DataFrame:
    """
    %NC por perito no período, considerando apenas análises com duração válida:
      - colunas de início/fim detectadas;
      - fim não nulo;
      - 0 < duração (s) ≤ 3600.
    Definição de NC robusta: conformado==0 OU (motivoNaoConformado!='' e !=0).
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        # Detecta colunas
        cols = [r[1] for r in conn.execute("PRAGMA table_info(analises)").fetchall()]
        ini_col = next((c for c in ("dataHoraIniPericia","dataHoraIniAnalise","dataHoraIni") if c in cols), None)
        fim_col = _detect_end_datetime_column(conn)
        if not ini_col or not fim_col:
            return pd.DataFrame(columns=["nomePerito","total","nc_count","pct_nc"])

        sql = f"""
            WITH base AS (
              SELECT
                p.nomePerito AS nomePerito,
                a.{ini_col}  AS ini,
                a.{fim_col}  AS fim,
                CASE
                  WHEN CAST(IFNULL(a.conformado,1) AS INTEGER)=0 THEN 1
                  WHEN TRIM(IFNULL(a.motivoNaoConformado,''))<>'' AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER)<>0 THEN 1
                  ELSE 0
                END AS nc_flag,
                CAST((julianday(a.{fim_col}) - julianday(a.{ini_col})) * 86400.0 AS REAL) AS dur_s
              FROM analises a
              JOIN peritos p ON p.siapePerito=a.siapePerito
              WHERE date(a.{ini_col}) BETWEEN ? AND ?
                AND a.{fim_col} IS NOT NULL
            ),
            valid AS (
              SELECT nomePerito, nc_flag
              FROM base
              WHERE dur_s > 0 AND dur_s <= 3600
            )
            SELECT
              nomePerito,
              COUNT(*)                                  AS total,
              SUM(nc_flag)                              AS nc_count,
              (SUM(nc_flag) * 100.0) / COUNT(*)         AS pct_nc
            FROM valid
            GROUP BY nomePerito
            HAVING total >= ?
        """
        return pd.read_sql(sql, conn, params=(start, end, int(min_tasks)))
    finally:
        conn.close()

def selecionar_top10_fluxo_b(start: str, end: str, min_analises: int = 50, fator_nc: float = 2.0) -> pd.DataFrame:
    """
    Fluxo B: seleciona peritos com %NC >= fator_nc × média nacional (válidas) no período,
    exigindo 'min_analises' tarefas. Ordena por scoreFinal (desc) e limita a 10.

    Regras de NC (iguais às usadas no resto do relatório):
      NC = 1 se (conformado == 0) OU (motivoNaoConformado não-vazio e != '0'); senão 0.
      Valores ausentes de 'conformado' são tratados como 1 (conforme padrão do projeto).
    """
    with sqlite3.connect(DB_PATH) as conn:
        # --- média nacional p_BR no período ---
        sql_pbr = """
            SELECT
              (SUM(
                 CASE
                   WHEN CAST(IFNULL(a.conformado, 1) AS INTEGER) = 0 THEN 1
                   WHEN TRIM(IFNULL(a.motivoNaoConformado, '')) <> ''
                        AND CAST(IFNULL(a.motivoNaoConformado, '0') AS INTEGER) <> 0 THEN 1
                   ELSE 0
                 END
              ) * 1.0) / COUNT(*) AS p_br
            FROM analises a
            WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
        """
        p_br = pd.read_sql(sql_pbr, conn, params=(start, end)).iloc[0]['p_br'] or 0.0
        limiar = float(fator_nc) * float(p_br or 0.0)

        # --- agregação por perito + score ---
        sql_per = """
            SELECT
              p.nomePerito                  AS nomePerito,
              COUNT(*)                      AS total_analises,
              SUM(
                 CASE
                   WHEN CAST(IFNULL(a.conformado, 1) AS INTEGER) = 0 THEN 1
                   WHEN TRIM(IFNULL(a.motivoNaoConformado, '')) <> ''
                        AND CAST(IFNULL(a.motivoNaoConformado, '0') AS INTEGER) <> 0 THEN 1
                   ELSE 0
                 END
              )                             AS nc_count,
              COALESCE(MAX(i.scoreFinal), 0) AS scoreFinal
            FROM analises a
            JOIN peritos      p ON p.siapePerito = a.siapePerito
            LEFT JOIN indicadores i ON i.perito = a.siapePerito
            WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
            GROUP BY p.nomePerito
            HAVING total_analises >= ?
               AND ( (nc_count * 1.0) / NULLIF(total_analises,0) ) >= ?
            ORDER BY scoreFinal DESC
            LIMIT 10
        """
        df = pd.read_sql(sql_per, conn, params=(start, end, int(min_analises), float(limiar)))
    return df


# ────────────────────────────────────────────────────────────────────────────────
# Exportação para PDF e main()
# ────────────────────────────────────────────────────────────────────────────────

LATEX_HEADER_CONTENT = r"""
%% Inserido automaticamente pelo make_report.py
\usepackage{float}
\usepackage{placeins}
\floatplacement{figure}{H}

% Pacotes úteis para tabelas compridas e colunas com largura fixa
\usepackage{longtable}
\usepackage{array}
"""


def exportar_org_para_pdf(org_path: str, font: str = "DejaVu Sans", pdf_dir: str | None = None) -> str | None:
    """
    Converte .org -> PDF via Pandoc + xelatex.
    Agora salva o PDF em uma pasta irmã 'pdfs/' da pasta onde está o .org (normalmente 'orgs/').
    Ex.: .../top10/orgs/relatorio.org  -->  .../top10/pdfs/relatorio.pdf
    """
    import shutil as sh

    # Diretórios básicos
    output_dir = os.path.dirname(org_path)            # geralmente .../orgs
    base_root  = os.path.dirname(output_dir)          # pai de orgs/  (ex.: .../top10)
    pdf_dir    = pdf_dir or os.path.join(base_root, "pdfs")
    os.makedirs(pdf_dir, exist_ok=True)

    org_name   = os.path.basename(org_path)
    pdf_name   = org_name.replace('.org', '.pdf')
    pdf_path   = os.path.join(pdf_dir, pdf_name)

    log_path    = org_path + ".log"
    header_path = os.path.join(output_dir, "_header_figs.tex")

    # Cabeçalho LaTeX (figuras estáveis) fica ao lado do .org
    LATEX_HEADER_CONTENT = r"""
%% Inserido automaticamente pelo make_report.py
\usepackage{float}
\usepackage{placeins}
\floatplacement{figure}{H}
\usepackage{longtable}
\usepackage{array}
"""
    with open(header_path, "w", encoding="utf-8") as fh:
        fh.write(LATEX_HEADER_CONTENT)

    # Protege o .org para consumo pelo Pandoc
    with open(org_path, "r", encoding="utf-8") as f:
        raw = f.read()
    protected = _protect_org_text_for_pandoc(raw)
    prot_name = org_name.replace(".org", "._pandoc.org")
    prot_path = os.path.join(output_dir, prot_name)
    with open(prot_path, "w", encoding="utf-8") as fprot:
        fprot.write(protected)

    # Checa pandoc
    pandoc = sh.which("pandoc")
    if not pandoc:
        print("❌ Pandoc não encontrado no PATH. Instale com: sudo apt install pandoc texlive-xetex")
        return None

    # Saída relativa a partir de output_dir (onde vamos rodar o pandoc)
    pdf_rel = os.path.relpath(pdf_path, start=output_dir)

    cmd = [
        "pandoc", prot_name, "-o", pdf_rel,
        "--pdf-engine=xelatex",
        "--include-in-header", os.path.basename(header_path),
        "--variable", f"mainfont={font}",
        "--variable", "geometry:margin=2cm",
        "--highlight-style=zenburn",
    ]

    print(f"[Pandoc] Gerando PDF: {' '.join(cmd)} (cwd={output_dir})")
    prev_cwd = os.getcwd()
    try:
        os.chdir(output_dir)
        with open(log_path, "w", encoding="utf-8") as flog:
            result = subprocess.run(cmd, stdout=flog, stderr=flog, text=True)
    finally:
        os.chdir(prev_cwd)

    if result.returncode == 0 and os.path.exists(pdf_path):
        print(f"✅ PDF gerado: {pdf_path}")
        return pdf_path
    else:
        print(f"❌ Erro ao gerar PDF. Veja o log: {log_path}")
        return None


def adicionar_capa_pdf(pdf_final_path: str) -> None:
    """
    Prependa a capa oficial (misc/capa.pdf) ao PDF final.
    Gera um novo arquivo com sufixo '_com_capa.pdf' no mesmo diretório.

    Parâmetros
    ----------
    pdf_final_path : str
        Caminho do PDF base (sem capa).
    """
    capa_path = os.path.join(MISC_DIR, "capa.pdf")
    if not os.path.exists(capa_path):
        print(f"[AVISO] Capa não encontrada: {capa_path}. Pulando.")
        return
    if not os.path.exists(pdf_final_path):
        print(f"[ERRO] PDF base não encontrado: {pdf_final_path}.")
        return

    output_path = pdf_final_path.replace(".pdf", "_com_capa.pdf")
    merger = PdfMerger()
    try:
        merger.append(capa_path)
        merger.append(pdf_final_path)
        merger.write(output_path)
        merger.close()
        print(f"✅ Relatório final com capa: {output_path}")
    except Exception as e:
        print(f"[ERRO] Falha ao adicionar capa: {e}")

def _mover_markdowns_de_exports(markdown_dir: str) -> int:
    """
    Move todos os arquivos '.md' que restarem em EXPORT_DIR para a pasta 'markdown/'
    do relatório corrente. Isso ocorre após:
      - conversão de comentários .md → .org (que já remove os .md correspondentes); e
      - cópia dos artefatos principais (.png/.org) para suas pastas.

    Parâmetros
    ----------
    markdown_dir : str
        Caminho para a pasta 'markdown' do relatório (será criada se necessário).

    Retorna
    -------
    int
        Quantidade de arquivos .md movidos.
    """
    os.makedirs(markdown_dir, exist_ok=True)
    moved = 0
    for src in glob(os.path.join(EXPORT_DIR, "*.md")):
        dst = os.path.join(markdown_dir, os.path.basename(src))
        try:
            shutil.copy2(src, dst)
            os.remove(src)
            moved += 1
        except Exception as e:
            print(f"[AVISO] Falha movendo {src} → {dst}: {e}")
    if moved:
        print(f"[INFO] Markdowns movidos para '{markdown_dir}': {moved} arquivo(s).")
    return moved



# ────────────────────────────────────────────────────────────────────────────────
# Protocolos transferidos (mesmo protocolo analisado por >1 perito)
# ────────────────────────────────────────────────────────────────────────────────

_PROTO_TRANSFER_CACHE = {}

def _find_protocol_transfers(start: str, end: str) -> "pd.DataFrame":
    """
    Retorna um DataFrame com protocolos analisados por >1 perito no período.
    Colunas: protocolo, perito, ini, fim
    """
    key = (start, end)
    if key in _PROTO_TRANSFER_CACHE:
        # retorna uma cópia defensiva
        return _PROTO_TRANSFER_CACHE[key].copy()

    import pandas as pd  # já importado globalmente, mas garante no escopo

    conn = sqlite3.connect(DB_PATH)
    try:
        end_col = _detect_end_datetime_column(conn)
        fim_expr = f"a.{end_col}" if end_col else "NULL"
        sql = f"""
            WITH per_protocol AS (
               SELECT a.protocolo      AS protocolo,
                      p.nomePerito     AS perito,
                      a.dataHoraIniPericia AS ini,
                      {fim_expr}       AS fim
                 FROM analises a
                 JOIN peritos p ON p.siapePerito = a.siapePerito
                WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
            ),
            prot_multi AS (
               SELECT protocolo
                 FROM per_protocol
             GROUP BY protocolo
               HAVING COUNT(DISTINCT perito) > 1
            )
            SELECT pp.protocolo, pp.perito, pp.ini, pp.fim
              FROM per_protocol pp
              JOIN prot_multi m USING (protocolo)
             ORDER BY pp.protocolo, pp.ini
        """
        df = pd.read_sql(sql, conn, params=(start, end))
    finally:
        conn.close()

    # normalização de tipos (para ordenação e resumo)
    if not df.empty:
        df["ini"] = pd.to_datetime(df["ini"], errors="coerce")
        if "fim" in df.columns:
            df["fim"] = pd.to_datetime(df["fim"], errors="coerce")

    _PROTO_TRANSFER_CACHE[key] = df.copy()
    return df

def _summarize_protocol_transfers(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Resume por protocolo: cadeia de peritos (ordem cronológica), nº de trocas,
    primeiro início e último fim.
    """
    import pandas as pd
    if df.empty:
        return pd.DataFrame(columns=["protocolo","peritos_env","trocas","primeiro_ini","ultimo_fim"])

    df = df.sort_values(["protocolo","ini"])
    def chain(series):
        seen = []
        for x in series:
            if len(seen)==0 or seen[-1] != x:
                seen.append(x)
        return " → ".join(seen)

    def swaps(series):
        uniq = []
        for x in series:
            if len(uniq)==0 or uniq[-1] != x:
                uniq.append(x)
        return max(len(uniq)-1, 1)

    g = df.groupby("protocolo", as_index=False).agg(
        peritos_env = ("perito", chain),
        trocas      = ("perito", swaps),
        primeiro_ini= ("ini", "min"),
        ultimo_fim  = ("fim", "max"),
    )
    # formata datas para string curta
    g["primeiro_ini"] = g["primeiro_ini"].dt.strftime("%Y-%m-%d %H:%M").fillna("")
    g["ultimo_fim"]   = g["ultimo_fim"].dt.strftime("%Y-%m-%d %H:%M").fillna("")
    return g

def _csv_write(df: "pd.DataFrame", path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")

def _df_to_org_table(df: "pd.DataFrame", headers: list[str]) -> str:
    """Converte DataFrame para tabela Org simples com cabeçalho informado."""
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join("---" for _ in headers) + "|")
    for _, row in df.iterrows():
        vals = [str(row.get(h, "")) for h in headers]
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)

def _append_protocol_transfers_group_block(
    lines: list[str],
    relatorio_dir: str,
    start: str,
    end: str,
    heading_level: str = "**",
    link_prefix: str = "",
) -> bool:
    import pandas as pd
    df = _find_protocol_transfers(start, end)
    lines.append("")
    lines.append(f"{heading_level} Protocolos transferidos (mesmo protocolo analisado por >1 perito)")
    lines.append("Critério: mesmo identificador de protocolo com participação de dois ou mais peritos distintos. Indicador complementar (não pontuado).")

    if df.empty:
        lines.append(f"\nNão foram identificados protocolos com troca de perito no período {start} a {end}.\n")
        return False

    summary = _summarize_protocol_transfers(df)
    csv_path = os.path.join(relatorio_dir, f"protocolos_transferidos_{start}_a_{end}.csv")
    _csv_write(summary, csv_path)

    headers = ["protocolo","peritos_env","trocas","primeiro_ini","ultimo_fim"]
    table = _df_to_org_table(summary, headers)
    lines.append("")
    lines.append("#+ATTR_LATEX: :environment longtable :align l l c l l")
    lines.append(table)
    lines.append("")
    lines.append(f"Arquivo completo: [[file:{link_prefix}{os.path.basename(csv_path)}]]")
    lines.append("")
    lines.append("_Nota:_ Transferência pode ocorrer por reanálise, redistribuição de carga ou outros motivos operacionais; não implica, por si só, irregularidade.")
    lines.append("")
    return True


def _append_protocol_transfers_perito_block_if_any(
    lines: list[str],
    perito: str,
    start: str,
    end: str,
    relatorio_dir: str,
    heading_level: str = "***",
    link_prefix: str = "",
) -> bool:
    df = _find_protocol_transfers(start, end)
    if df.empty:
        lines.append(f"{heading_level} Protocolos transferidos (este perito)")
        lines.append(f"Não foram identificados protocolos transferidos envolvendo este perito no período {start} a {end}.\n")
        return False

    mask = df["perito"].astype(str).str.strip().str.casefold() == str(perito).strip().casefold()
    protos = sorted(df.loc[mask, "protocolo"].astype(str).unique().tolist())
    lines.append(f"{heading_level} Protocolos transferidos (este perito)")
    if not protos:
        lines.append(f"**Protocolos transferidos:** este perito não participou de protocolos transferidos no período.")
        lines.append("")
        return False

    csv_name = f"protocolos_transferidos_{start}_a_{end}.csv"
    csv_path = os.path.join(relatorio_dir, csv_name)
    hint = f"  (ver [[file:{link_prefix}{csv_name}]] para detalhes)" if os.path.exists(csv_path) else ""
    lines.append(f"**Protocolos transferidos que envolvem este perito ({len(protos)}):** {', '.join(protos)}{hint}")
    lines.append("")
    return True

def main():
    """
    Orquestração principal:
      1) Parse de argumentos e preflight (env, R).
      2) Seleção de peritos (Top10 legacy | TopK | all-matching | perito único).
      3) Planejamento e execução dos gráficos (Python e R).
      4) Coleta e cópia dos artefatos para pastas do relatório.
      5) Geração do(s) arquivo(s) .org consolidado(s) e, opcionalmente, PDF com capa.
      6) Organização dos arquivos '.md' remanescentes em 'markdown/'.
    """
    t0 = time.time()
    args = parse_args()

    # Aviso apenas se pretendemos gerar comentários IA sem OPENAI_API_KEY
    if (args.add_comments or getattr(args, "export_comment_org", False)) and not os.getenv("OPENAI_API_KEY"):
        print("⚠️  Sem OPENAI_API_KEY. Os scripts com comentário podem cair no fallback (sem IA).")

    # Preflight do R, se apêndice habilitado
    if args.r_appendix:
        _preflight_r(args.r_bin)

    # --- Deriva o "tipo de execução" (grupo vs individual) e rótulo da pasta ---
    is_group_run = bool(args.top10 or getattr(args, "topk", None) or getattr(args, "all_matching", False) or getattr(args, "peritos_csv", None))
    fluxo = str(getattr(args, "fluxo", "B")).upper()

    # Diretórios base do período
    PERIODO_DIR = os.path.join(OUTPUTS_DIR, f"{args.start}_a_{args.end}")
    os.makedirs(PERIODO_DIR, exist_ok=True)

    # Vars p/ manifests e caminhos
    peritos_csv_path: Optional[str] = None
    scope_csv_path: Optional[str]   = None
    lista_selecionados: list[str]   = []   # lista final de nomes para rodar individuais
    set_top10: set[str]             = set()

    # =========================
    # SELEÇÃO
    # =========================
    # Top10 legacy: deixamos df_sel vazio e usamos --top10 nos scripts que suportam.
    legacy_top10 = bool(args.top10)

    # Se houver qualquer uma das novas formas (topk / all-matching / peritos-csv), usamos _select_candidates
    df_sel, scope_df = _select_candidates(args)  # pode retornar vazio no caso Top10 legacy

    # Define rótulo da pasta de relatório de GRUPO (quando aplicável)
    if is_group_run:
        if legacy_top10 and (df_sel is None or df_sel.empty):
            group_label = "top10"
        elif getattr(args, "topk", None):
            group_label = f"top{int(args.topk)}"
        elif getattr(args, "all_matching", False):
            group_label = "all_matching"
        else:
            group_label = "selecionados"
        RELATORIO_DIR = os.path.join(PERIODO_DIR, group_label)
    else:
        safe_perito = _safe(args.perito.strip())
        RELATORIO_DIR = os.path.join(PERIODO_DIR, "individual", safe_perito)

    # Pastas de saída
    IMGS_DIR      = os.path.join(RELATORIO_DIR, "imgs")
    COMMENTS_DIR  = os.path.join(RELATORIO_DIR, "comments")
    ORGS_DIR      = os.path.join(RELATORIO_DIR, "orgs")
    MARKDOWN_DIR  = os.path.join(RELATORIO_DIR, "markdown")
    for d in (RELATORIO_DIR, IMGS_DIR, COMMENTS_DIR, ORGS_DIR, MARKDOWN_DIR):
        os.makedirs(d, exist_ok=True)

    planned_cmds: list[list[str]] = []

    # =========================
    # PLANEJAMENTO — GRUPO
    # =========================
    if is_group_run:
        # Limpa exports antigos de grupo
        _cleanup_exports_top10()

        # Materializa seleção/escopo quando houver (TopK/all-matching/peritos-csv)
        # No Top10 legacy (df_sel vazio), não gera peritos_csv para manter fallback --top10
        if df_sel is not None and not df_sel.empty:
            peritos_csv_path, scope_csv_path = _materialize_selection_to_csvs(
                df_sel, scope_df, RELATORIO_DIR, fluxo, args.start, args.end, save_manifests=bool(getattr(args, "save_manifests", False))
            )
            # lista para rodar apêndices/individuais
            lista_selecionados = sorted(df_sel["nomePerito"].astype(str).unique().tolist())
        else:
            peritos_csv_path, scope_csv_path = None, (scope_df if scope_df is not None else None)
            if legacy_top10:
                print("ℹ️  Top 10 legacy (sem peritos_csv): scripts de grupo que suportam --top10 serão chamados com --top10.")
                # Para gerar individuais no consolidado, calculamos a lista do Top 10 (como no seu fluxo atual)
                if fluxo == "B":
                    peritos_df = pegar_top10_harm_first(args.start, args.end, min_analises=args.min_analises, factor_nc=2.0)
                else:
                    peritos_df = pegar_10_piores_peritos(args.start, args.end, min_analises=args.min_analises)
                if not peritos_df.empty:
                    lista_selecionados = peritos_df['nomePerito'].astype(str).tolist()
                    set_top10 = set(lista_selecionados)
                else:
                    print("⚠️  Nenhum perito elegível para o Top 10 no período/fluxo informados.")
                    # Seguimos só com os gráficos de grupo/top10 (se houver) e saídas globais

        # Scripts globais (por período) — execute antes para garantir orgs auxiliares
        for script in GLOBAL_SCRIPTS:
            script_file = script_path(script)
            planned_cmds[:0] = build_commands_for_global(script_file, args.start, args.end)

        # Contexto de GRUPO para os scripts Python
        group_ctx = {
            "kind": "group",
            "top_k": (10 if legacy_top10 and not peritos_csv_path else int(getattr(args, "topk", 0) or 0)),
            "start": args.start,
            "end":   args.end,
            "min_analises": args.min_analises,
            "add_comments": args.add_comments,
            "export_org": bool(getattr(args, "export_org", False)),
            "export_pdf": bool(getattr(args, "export_pdf", False)),
            "fluxo": fluxo,
            "kpi_base": getattr(args, "kpi_base", None),
            "peritos_csv": peritos_csv_path,
            "scope_csv": scope_csv_path,
        }

        # Agenda scripts Python de GRUPO
        for script in SCRIPT_ORDER:
            script_file = script_path(script)
            planned_cmds.extend(build_commands_for_script(script_file, group_ctx))

        # Agenda R checks (grupo ou por perito conforme a seleção)
        _run_rchecks_for_selection(args, peritos_csv=peritos_csv_path, plan_only=bool(args.plan_only))

        # Agenda scripts INDIVIDUAIS para cada perito selecionado
        for perito in lista_selecionados:
            if not perito_tem_dados(perito, args.start, args.end):
                print(f"⚠️  Perito '{perito}' sem análises no período! Pulando.")
                continue
            safe = _safe(perito)
            _cleanup_exports_for_perito(safe)

            indiv_ctx = {
                "kind": "perito",
                "perito": perito,
                "start": args.start,
                "end":   args.end,
                "add_comments": args.add_comments,
                "export_org": bool(getattr(args, "export_org", False)),
                "export_pdf": bool(getattr(args, "export_pdf", False)),
                "fluxo": fluxo,
                "kpi_base": getattr(args, "kpi_base", None),
            }
            for script in SCRIPT_ORDER:
                script_file = script_path(script)
                planned_cmds.extend(build_commands_for_script(script_file, indiv_ctx))

            if args.r_appendix:
                planned_cmds.extend(build_r_commands_for_perito(perito, args.start, args.end, args.r_bin))

        # Coorte extra %NC altíssima (opcional) — mantém sua lógica
        extras_list = []
        if args.include_high_nc:
            df_high = pegar_peritos_nc_altissima(args.start, args.end, nc_threshold=args.high_nc_threshold, min_tasks=args.high_nc_min_tasks)
            if not df_high.empty:
                base_set = set(lista_selecionados) if lista_selecionados else set_top10
                extras_list = [n for n in df_high['nomePerito'].astype(str).tolist() if n not in base_set]
                if extras_list:
                    print(f"Incluindo coorte extra (%NC ≥ {args.high_nc_threshold} e ≥ {args.high_nc_min_tasks} tarefas): {extras_list}")
        else:
            print("Coorte extra de %NC alta desativada (--no-high-nc).")

        for perito in extras_list:
            if not perito_tem_dados(perito, args.start, args.end):
                continue
            safe = _safe(perito)
            _cleanup_exports_for_perito(safe)

            indiv_ctx = {
                "kind": "perito",
                "perito": perito,
                "start": args.start,
                "end":   args.end,
                "add_comments": args.add_comments,
                "export_org": bool(getattr(args, "export_org", False)),
                "export_pdf": bool(getattr(args, "export_pdf", False)),
                "fluxo": fluxo,
                "kpi_base": getattr(args, "kpi_base", None),
            }
            for script in SCRIPT_ORDER:
                script_file = script_path(script)
                planned_cmds.extend(build_commands_for_script(script_file, indiv_ctx))
            if args.r_appendix:
                planned_cmds.extend(build_r_commands_for_perito(perito, args.start, args.end, args.r_bin))

    # =========================
    # PLANEJAMENTO — INDIVIDUAL
    # =========================
    else:
        perito = args.perito.strip()
        if not perito_tem_dados(perito, args.start, args.end):
            print(f"⚠️  Perito '{perito}' sem análises no período.")
            return

        safe = _safe(perito)
        _cleanup_exports_for_perito(safe)

        # Scripts globais (por período) — ainda executa para obter orgs-base do W→WE
        for script in GLOBAL_SCRIPTS:
            script_file = script_path(script)
            planned_cmds[:0] = build_commands_for_global(script_file, args.start, args.end)

        indiv_ctx = {
            "kind": "perito",
            "perito": perito,
            "start": args.start,
            "end":   args.end,
            "add_comments": args.add_comments,
            "export_org": bool(getattr(args, "export_org", False)),
            "export_pdf": bool(getattr(args, "export_pdf", False)),
            "fluxo": fluxo,
            "kpi_base": getattr(args, "kpi_base", None),
        }
        for script in SCRIPT_ORDER:
            script_file = script_path(script)
            planned_cmds.extend(build_commands_for_script(script_file, indiv_ctx))

        if args.r_appendix:
            planned_cmds.extend(build_r_commands_for_perito(perito, args.start, args.end, args.r_bin))

    # Bootstrap de dependências R (entra antes de tudo)
    if args.r_appendix:
        cmd_boot = _r_deps_bootstrap_cmd(args.r_bin)
        if cmd_boot:
            planned_cmds.insert(0, cmd_boot)
        else:
            print("[AVISO] r_checks/_ensure_deps.R não encontrado; pulando bootstrap de pacotes.")

    # Apenas listar plano?
    if args.plan_only:
        pretty_print_plan(planned_cmds, args, peritos_csv_path=peritos_csv_path, scope_csv_path=scope_csv_path)
        return 0

    # ===== Injeção de ambiente para TODOS os R checks =====
    R_EXTRA_ENV: dict = {}
    if is_group_run:
        R_EXTRA_ENV["FLUXO"] = str(fluxo)
        if peritos_csv_path:
            R_EXTRA_ENV["PERITOS_CSV"] = peritos_csv_path
        if scope_csv_path:
            R_EXTRA_ENV["SCOPE_CSV"] = scope_csv_path

    # Execução (Python e R)
    for cmd in planned_cmds:
        try:
            if _is_r_cmd(cmd):
                print(f"[RUN] {' '.join(map(str, cmd))}")
                r_env = os.environ.copy()
                r_env.update(R_EXTRA_ENV)
                subprocess.run(cmd, check=False, cwd=RCHECK_DIR, env=r_env)
            else:
                print(f"[RUN] {' '.join(map(str, cmd))}")
                subprocess.run(cmd, check=False, env=_env_with_project_path(), cwd=SCRIPTS_DIR)
        except Exception as e:
            print(f"[ERRO] Falha executando: {' '.join(map(str, cmd))}\n  -> {e}")

    # Coleta saídas R (fallback) em EXPORT_DIR
    collect_r_outputs_to_export()

    # --------------------------------------------------------------------------
    # MONTAGEM DOS RELATÓRIOS E MOVIMENTAÇÃO DE ARTEFATOS
    # --------------------------------------------------------------------------
    org_paths = []
    extras_org_paths = []
    org_grupo_top = None
    org_to_export = None

    if is_group_run:
        # Copia artefatos de grupo e panorama global W→WE
        copiar_artefatos_top10(IMGS_DIR, COMMENTS_DIR, ORGS_DIR, MARKDOWN_DIR)
        try:
            copiar_artefatos_weekday2weekend(IMGS_DIR, COMMENTS_DIR, ORGS_DIR)
        except NameError:
            pass

        # Comentários GPT dos R checks (se habilitado)
        if args.r_appendix and args.add_comments:
            gerar_r_apendice_group_comments_if_possible(IMGS_DIR, COMMENTS_DIR, args.start, args.end)

        # Org do grupo (salva em orgs/)
        org_grupo_top = gerar_org_top10_grupo(args.start, args.end, RELATORIO_DIR, IMGS_DIR, COMMENTS_DIR, orgs_dir=ORGS_DIR)

        # Peritos selecionados (lista_selecionados) — monta org individual de cada um
        for perito in lista_selecionados:
            if not perito_tem_dados(perito, args.start, args.end):
                continue
            copiar_artefatos_perito(perito, IMGS_DIR, COMMENTS_DIR, ORGS_DIR)
            if args.r_appendix and args.add_comments:
                gerar_r_apendice_comments_if_possible(perito, IMGS_DIR, COMMENTS_DIR, args.start, args.end)
            org_path = gerar_org_perito(perito, args.start, args.end, args.add_comments, IMGS_DIR, COMMENTS_DIR, RELATORIO_DIR, orgs_dir=ORGS_DIR)
            org_paths.append(org_path)

        # Coorte extra (se existiu)
        if args.include_high_nc:
            df_high = pegar_peritos_nc_altissima(args.start, args.end, nc_threshold=args.high_nc_threshold, min_tasks=args.high_nc_min_tasks)
            base_set = set(lista_selecionados) if lista_selecionados else set_top10
            extras_list = [n for n in df_high['nomePerito'].astype(str).tolist() if n not in base_set]
            for perito in extras_list:
                if not perito_tem_dados(perito, args.start, args.end):
                    continue
                copiar_artefatos_perito(perito, IMGS_DIR, COMMENTS_DIR, ORGS_DIR)
                if args.r_appendix and args.add_comments:
                    gerar_r_apendice_comments_if_possible(perito, IMGS_DIR, COMMENTS_DIR, args.start, args.end)
                org_path = gerar_org_perito(perito, args.start, args.end, args.add_comments, IMGS_DIR, COMMENTS_DIR, RELATORIO_DIR, orgs_dir=ORGS_DIR)
                extras_org_paths.append(org_path)

        # Mover quaisquer .md remanescentes para 'markdown/'
        _mover_markdowns_de_exports(MARKDOWN_DIR)

        # Org consolidado do grupo — nomeia conforme label
        if (args.export_org or args.export_pdf) and (org_paths or org_grupo_top or extras_org_paths):
            org_final = os.path.join(ORGS_DIR, f"relatorio_{group_label}_{args.start}_a_{args.end}.org")
            lines = [f"* Relatório — {group_label.replace('_', ' ').upper()} ({args.start} a {args.end}) — Fluxo {fluxo}", ""]

            # Grupo (TopX)
            if org_grupo_top and os.path.exists(org_grupo_top):
                with open(org_grupo_top, encoding="utf-8") as f:
                    lines.append(f.read().strip())
                    lines.append("#+LATEX: \\newpage\n")

            # Cada perito + Impacto na Fila (perito)
            for org_path in org_paths:
                with open(org_path, encoding="utf-8") as f:
                    content = f.read().strip()
                if content:
                    lines.append(content)
                    lines.append("#+LATEX: \\newpage\n")

                # Impacto na Fila do PERITO (se existir)
                perito_safe = os.path.splitext(os.path.basename(org_path))[0]
                per_impacts = coletar_orgs_impacto(PERIODO_DIR, args.start, args.end, perito=perito_safe)
                for imp_path in per_impacts:
                    try:
                        with open(imp_path, encoding="utf-8") as fi:
                            imp_txt = fi.read().strip()
                        if imp_txt:
                            imp_txt = shift_org_headings(_protect_org_text_for_pandoc(imp_txt), delta=1)
                            lines.append(imp_txt)
                            lines.append("#+LATEX: \\newpage\n")
                    except Exception as e:
                        print(f"[AVISO] Falha ao anexar impacto do perito ({imp_path}): {e}")

            # Extras (coorte %NC altíssima)
            if extras_org_paths:
                lines.append(f"** Peritos com %NC ≥ {args.high_nc_threshold:.0f}% e ≥ {args.high_nc_min_tasks} tarefas\n")
                for org_path in extras_org_paths:
                    with open(org_path, encoding="utf-8") as f:
                        content = f.read().strip()
                        if content:
                            content = shift_org_headings(content, delta=1)
                            lines.append(content)
                            lines.append("#+LATEX: \\newpage\n")

            # Impacto na Fila do GRUPO (se houver)
            grp_impacts = coletar_orgs_impacto(PERIODO_DIR, args.start, args.end, perito=None)
            for path in grp_impacts:
                try:
                    with open(path, encoding="utf-8") as f:
                        content = f.read().strip()
                    if content:
                        content = shift_org_headings(_protect_org_text_for_pandoc(content), delta=1)
                        lines.append(content)
                        lines.append("#+LATEX: \\newpage\n")
                except Exception as e:
                    print(f"[AVISO] Falha ao anexar impacto do grupo ({path}): {e}")

            # Protocolos transferidos (grupo)
            _append_protocol_transfers_group_block(lines, RELATORIO_DIR, args.start, args.end, heading_level="**", link_prefix="../")

            # Panorama global (W→WE) AO FINAL — imagens com ../imgs/
            _append_weekday2weekend_panorama_block(
                lines, IMGS_DIR, COMMENTS_DIR,
                start=args.start, end=args.end,
                heading_level="**", imgs_prefix="../imgs/"
            )

            with open(org_final, "w", encoding="utf-8") as f:
                f.write("\n".join(lines).strip() + "\n")
            print(f"✅ Org consolidado salvo em: {org_final}")
            org_to_export = org_final
        else:
            # Caso não queira consolidar, tenta expor o primeiro org individual
            org_to_export = org_paths[0] if org_paths else None

    # --------------------------------------------------------------------------
    # INDIVIDUAL — consolidado + bloco W→WE condicional por perito
    # --------------------------------------------------------------------------
    if not is_group_run:
        perito = args.perito.strip()
        imgs_dir_i     = os.path.join(RELATORIO_DIR, "imgs")
        comments_dir_i = os.path.join(RELATORIO_DIR, "comments")
        orgs_dir_i     = os.path.join(RELATORIO_DIR, "orgs")

        copiar_artefatos_perito(perito, imgs_dir_i, comments_dir_i, orgs_dir_i)
        try:
            copiar_artefatos_weekday2weekend(imgs_dir_i, comments_dir_i, orgs_dir_i)
        except NameError:
            pass

        _mover_markdowns_de_exports(MARKDOWN_DIR)

        perito_org_path = gerar_org_perito(perito, args.start, args.end, args.add_comments, imgs_dir_i, comments_dir_i, RELATORIO_DIR, orgs_dir=orgs_dir_i)

        org_final = os.path.join(orgs_dir_i, f"relatorio_{_safe(perito)}_{args.start}_a_{args.end}.org")
        lines = [f"* Relatório individual — {perito} ({args.start} a {args.end})", ""]

        with open(perito_org_path, encoding="utf-8") as f:
            content = f.read().strip()
            if content:
                lines.append(content)
                lines.append("#+LATEX: \\newpage\n")

        _append_weekday2weekend_perito_block_if_any(lines, perito, imgs_dir_i, comments_dir_i, start=args.start, end=args.end, heading_level="**")

        with open(org_final, "w", encoding="utf-8") as f:
            f.write("\n".join(lines).strip() + "\n")

        org_to_export = org_final

    # Exportação para PDF (opcional) + capa
    if args.export_pdf and org_to_export:
        pdf_path = exportar_org_para_pdf(org_to_export, font="DejaVu Sans")
        if pdf_path and os.path.exists(pdf_path):
            adicionar_capa_pdf(pdf_path)

    # Tempo total
    dt = time.time() - t0
    mm, ss = divmod(int(dt + 0.5), 60)
    hh, mm = divmod(mm, 60)
    print(f"⏱️ Tempo total: {hh:02d}:{mm:02d}:{ss:02d}")


if __name__ == '__main__':
    main()

