#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import sqlite3
import shutil
from glob import glob
import pandas as pd
from PyPDF2 import PdfMerger
import re
import time

# ────────────────────────────────────────────────────────────────────────────────
# Paths
# ────────────────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH     = os.path.join(BASE_DIR, 'db', 'atestmed.db')
GRAPHS_DIR  = os.path.join(BASE_DIR, 'graphs_and_tables')
SCRIPTS_DIR = GRAPHS_DIR  # alias
EXPORT_DIR  = os.path.join(GRAPHS_DIR, 'exports')
OUTPUTS_DIR = os.path.join(BASE_DIR, 'reports', 'outputs')
MISC_DIR    = os.path.join(BASE_DIR, 'misc')
RCHECK_DIR  = os.path.join(BASE_DIR, 'r_checks')  # onde você colocará os .R

os.makedirs(EXPORT_DIR, exist_ok=True)
os.makedirs(OUTPUTS_DIR, exist_ok=True)

# depois de definir BASE_DIR:
def _load_env_from_root():
    env_path = os.path.join(BASE_DIR, ".env")
    # tenta python-dotenv
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path, override=False)
        return
    except Exception:
        pass
    # fallback manual
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

# aviso global (antes de qualquer uso do ambiente)
if not os.getenv("OPENAI_API_KEY"):
    print("⚠️  OPENAI_API_KEY não encontrado no ambiente (.env na raiz não carregou ou não tem a chave).")
    
# ────────────────────────────────────────────────────────────────────────────────
# Ordem e defaults dos SCRIPTS PYTHON
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
# R checks (apêndice estatístico) — por perito
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

# ────────────────────────────────────────────────────────────────────────────────
# R checks (apêndice estatístico) — Top 10 (grupo)
# ────────────────────────────────────────────────────────────────────────────────
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

# Comentário ChatGPT para apêndice R
try:
    from utils.comentarios import comentar_r_apendice
except Exception:
    comentar_r_apendice = None

# ────────────────────────────────────────────────────────────────────────────────
# Conhecimento explícito (fallback) das flags de cada script PYTHON
# ────────────────────────────────────────────────────────────────────────────────
ASSUME_FLAGS = {
    "compare_nc_rate.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--export-md", "--export-png", "--export-org",
        "--export-comment", "--export-comment-org", "--call-api",
    },
    "compare_fifteen_seconds.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--threshold", "--cut-n",
        "--export-png", "--export-org",
        "--export-comment", "--export-comment-org", "--call-api",
    },
    "compare_overlap.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--mode", "--chart",
        "--export-md", "--export-png", "--export-org",
        "--export-comment", "--export-comment-org", "--call-api",
    },
    "compare_productivity.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--threshold", "--mode", "--chart",
        "--export-md", "--export-png", "--export-org",
        "--export-comment", "--export-comment-org", "--call-api",
    },
    "compare_indicadores_composto.py": {
        "--perito", "--top10", "--min-analises",
        "--alvo-prod", "--cut-prod-pct", "--cut-nc-pct", "--cut-le15s-pct", "--cut-overlap-pct",
        "--export-png", "--export-org", "--chart",
        "--export-comment", "--export-comment-org", "--call-api",
    },
    "compare_motivos_perito_vs_brasil.py": {
        "--perito", "--top10", "--min-analises",
        "--topn", "--min-pct-perito", "--min-pct-brasil", "--min-n-perito", "--min-n-brasil",
        "--label-maxlen", "--label-fontsize",
        "--chart", "--export-md", "--export-org", "--export-png",
        "--export-comment", "--export-comment-org", "--call-api",
    },
}

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Relatório ATESTMED — Individual ou Top 10 (executa todos os gráficos/modos + apêndice estatístico em R, inclusive Top 10 grupo)")
    p.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    p.add_argument('--end',   required=True, help='Data final   YYYY-MM-DD')

    who = p.add_mutually_exclusive_group(required=True)
    who.add_argument('--perito', help='Nome do perito (relatório individual)')
    who.add_argument('--top10', action='store_true', help='Gera relatório para os 10 piores peritos do período')

    p.add_argument('--min-analises', type=int, default=50, help='Mínimo de análises para elegibilidade do Top 10')

    p.add_argument('--include-high-nc', dest='include_high_nc', action='store_true', default=True,
                   help='(default) Incluir peritos com %NC ≥ limiar e tarefas ≥ min no relatório Top 10')
    p.add_argument('--no-high-nc', dest='include_high_nc', action='store_false',
                   help='Não incluir o coorte extra de %NC muito alta')
    p.add_argument('--high-nc-threshold', type=float, default=90.0, help='Limiar de %NC para o coorte extra (padrão: 90)')
    p.add_argument('--high-nc-min-tasks', type=int, default=50, help='Mínimo de tarefas para o coorte extra (padrão: 50)')

    p.add_argument('--export-org', action='store_true', help='Exporta relatório consolidado em Org-mode')
    p.add_argument('--export-pdf', action='store_true', help='Exporta relatório consolidado em PDF (via Pandoc)')
    p.add_argument('--add-comments', action='store_true', help='Inclui comentários GPT (quando suportado pelos scripts)')

    p.add_argument('--plan-only', action='store_true', help='Somente imprime os comandos planejados (não executa nada)')

    p.add_argument('--r-appendix', action='store_true', default=True, help='(default) Executa os R checks e inclui no apêndice')
    p.add_argument('--no-r-appendix', dest='r_appendix', action='store_false', help='Não executar os R checks')
    p.add_argument('--r-bin', default='Rscript', help='Binário do Rscript (padrão: Rscript)')

    return p.parse_args()

# ────────────────────────────────────────────────────────────────────────────────
# Helpers gerais
# ────────────────────────────────────────────────────────────────────────────────
def _safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-","_") else "_" for c in str(name)).strip("_") or "output"

def _env_with_project_path():
    env = os.environ.copy()
    py = env.get("PYTHONPATH", "")
    parts = [BASE_DIR] + ([py] if py else [])
    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env

def script_path(name: str) -> str:
    path = os.path.join(SCRIPTS_DIR, name)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Script não encontrado: {path}")
    return path

def rscript_path(name: str) -> str:
    path = os.path.join(RCHECK_DIR, name)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"R script não encontrado: {path}")
    return path

def introspect_script(script_file: str) -> dict:
    info = {"flags": set(), "modes": []}
    try:
        out = subprocess.run(
            [sys.executable, script_file, "--help"],
            capture_output=True, text=True, env=_env_with_project_path(), cwd=SCRIPTS_DIR
        )
        text = (out.stdout or "") + "\n" + (out.stderr or "")
        for m in re.finditer(r"(--[a-zA-Z0-9][a-zA-Z0-9\-]*)", text):
            info["flags"].add(m.group(1))
        mm = re.search(r"--mode[^\n]*\{([^}]+)\}", text)
        if mm:
            info["modes"] = [x.strip() for x in mm.group(1).split(",") if x.strip()]
    except Exception:
        pass
    return info

def detect_modes(script_file: str, help_info: dict) -> list:
    name = os.path.basename(script_file)
    modes = help_info.get("modes") or []
    if modes:
        return modes
    return DEFAULT_MODES.get(name, [])

# ——— R preflight & detecção robusta ————————————————————————————————————————
import shutil as _shutil

def _preflight_r(r_bin: str):
    r_path = _shutil.which(r_bin)
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
    if not cmd:
        return False
    exe = os.path.basename(str(cmd[0])).lower()
    return ("rscript" in exe) or exe == "r"

def _detect_r_out_flag(r_file_path: str):
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

def _r_deps_bootstrap_cmd(r_bin: str):
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

# ────────────────────────────────────────────────────────────────────────────────
# Ordenação de figuras (Python e R)
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
        "rcheck_nc_rate_",          # 01
        "rcheck_le15",              # 02
        "rcheck_productivity",      # 03
        "rcheck_overlap",           # 04
        "rcheck_motivos_chisq",     # 05
        "rcheck_composite",         # 06
        "rcheck_weighted_props_nc",
        "rcheck_weighted_props_le",
        "rcheck_weighted_props_",
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
# Planejador (PYTHON)
# ────────────────────────────────────────────────────────────────────────────────
def build_commands_for_script(script_file: str, context: dict) -> list:
    cmds = []
    name = os.path.basename(script_file)
    help_info = introspect_script(script_file)
    flags = set(help_info.get("flags", set()))
    if not flags and name in ASSUME_FLAGS:
        flags = set(ASSUME_FLAGS[name])
    modes = detect_modes(script_file, help_info)
    base = ["--start", context["start"], "--end", context["end"]]

    if context["kind"] == "perito":
        perito = context["perito"]
        if "--perito" in flags:
            base += ["--perito", perito]
        elif "--nome" in flags:
            base += ["--nome", perito]
        else:
            base += ["--perito", perito]
    else:
        if ("--top10" in flags) or (name in ASSUME_FLAGS and "--top10" in ASSUME_FLAGS[name]):
            base += ["--top10"]
            if "--min-analises" in flags:
                base += ["--min-analises", str(context["min_analises"])]
        else:
            print(f"[INFO] {name} não suporta --top10; pulando no grupo.")
            return []

    # Sempre pedir o .org do gráfico
    if "--export-org" in flags:
        base += ["--export-org"]

    # Solicitar comentários quando add_comments
    if context.get("add_comments"):
        if "--export-comment-org" in flags:
            base += ["--export-comment-org"]
        elif "--export-comment" in flags:
            base += ["--export-comment"]
        if "--call-api" in flags:
            base += ["--call-api"]

    # PNG sempre útil
    if "--export-png" in flags:
        base += ["--export-png"]

    # Markdown opcional (alguns fluxos ainda gostam)
    if "--export-md" in flags:
        base += ["--export-md"]

    if name == "compare_productivity.py" and "--threshold" in flags:
        base += ["--threshold", PRODUCTIVITY_THRESHOLD]
    if name == "compare_fifteen_seconds.py":
        if "--threshold" in flags: base += ["--threshold", FIFTEEN_THRESHOLD]
        if "--cut-n" in flags:     base += ["--cut-n", FIFTEEN_CUT_N]

    def _apply_extra(cmd_list):
        extra = EXTRA_ARGS.get(name, [])
        i = 0
        while i < len(extra):
            tok = extra[i]
            if isinstance(tok, str) and tok.startswith("--") and tok in flags:
                cmd_list.append(tok)
                if i + 1 < len(extra) and (not isinstance(extra[i+1], str) or not extra[i+1].startswith("--")):
                    cmd_list.append(str(extra[i+1]))
                    i += 1
            i += 1
        return cmd_list

    if "--mode" in flags and modes:
        for m in modes:
            cmd = [sys.executable, script_file] + base + ["--mode", m]
            cmds.append(_apply_extra(cmd))
    else:
        cmd = [sys.executable, script_file] + base
        cmds.append(_apply_extra(cmd))

    return cmds

# ────────────────────────────────────────────────────────────────────────────────
# Planejador (R) — por perito
# ────────────────────────────────────────────────────────────────────────────────
def build_r_commands_for_perito(perito: str, start: str, end: str, r_bin: str) -> list:
    cmds = []
    for fname, meta in RCHECK_SCRIPTS:
        try:
            fpath = rscript_path(fname)
        except FileNotFoundError:
            print(f"[AVISO] R check ausente: {fname} (pule ou crie este arquivo em {RCHECK_DIR})")
            continue

        out_flag = _detect_r_out_flag(fpath)
        cmd = [r_bin, fpath, "--db", DB_PATH, "--start", start, "--end", end]
        if out_flag:
            cmd += [out_flag, EXPORT_DIR]
        if meta.get("need_perito", False):
            cmd += ["--perito", perito]
        for k, v in (meta.get("defaults") or {}).items():
            cmd += [k, str(v)]
        cmds.append(cmd)
    if not cmds:
        print("[INFO] Nenhum R check individual enfileirado (provável ausência de arquivos em r_checks/).")
    else:
        print(f"[INFO] R checks individuais enfileirados para '{perito}': {len(cmds)}")
    return cmds

# ────────────────────────────────────────────────────────────────────────────────
# Planejador (R) — Top 10 (grupo)
# ────────────────────────────────────────────────────────────────────────────────
def build_r_commands_for_top10(start: str, end: str, r_bin: str, min_analises: int) -> list:
    cmds = []
    for fname, meta in RCHECK_GROUP_SCRIPTS:
        try:
            fpath = rscript_path(fname)
        except FileNotFoundError:
            print(f"[AVISO] R check de grupo ausente: {fname} (pule ou crie este arquivo em {RCHECK_DIR})")
            continue

        out_flag = _detect_r_out_flag(fpath)
        cmd = [
            r_bin, fpath,
            "--db", DB_PATH,
            "--start", start,
            "--end", end,
            "--min-analises", str(min_analises),
        ]

        if meta.get("pass_top10"):
            cmd += ["--top10"]

        if out_flag:
            cmd += [out_flag, EXPORT_DIR]

        for k, v in (meta.get("defaults") or {}).items():
            if k == "--min-analises":
                v = str(min_analises)
            cmd += [k, str(v)]

        cmds.append(cmd)

    if not cmds:
        print("[INFO] Nenhum R check de grupo enfileirado (provável ausência de g*_top10_*.R em r_checks/).")
    else:
        print(f"[INFO] R checks de grupo (Top10) enfileirados: {len(cmds)}")
    return cmds

# ────────────────────────────────────────────────────────────────────────────────
# Top 10 e coorte extra
# ────────────────────────────────────────────────────────────────────────────────
def pegar_10_piores_peritos(start, end, min_analises=50):
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
      FROM indicadores i
      JOIN peritos p ON i.perito = p.siapePerito
      JOIN analises a ON a.siapePerito = i.perito
     WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
     GROUP BY p.nomePerito, i.scoreFinal
    HAVING total_analises >= ?
     ORDER BY i.scoreFinal DESC
     LIMIT 10
    """
    df = pd.read_sql(query, conn, params=(start, end, min_analises))
    conn.close()
    return df

def pegar_peritos_nc_altissima(start, end, nc_threshold=90.0, min_tasks=50):
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()
    return df

def perito_tem_dados(perito, start, end):
    conn = sqlite3.connect(DB_PATH)
    count = conn.execute("""
        SELECT COUNT(*) FROM analises a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        WHERE p.nomePerito = ? AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
    """, (perito, start, end)).fetchone()[0]
    conn.close()
    return count > 0

# ────────────────────────────────────────────────────────────────────────────────
# Limpeza e cópia (inclui tolerância a variações de nome)
# ────────────────────────────────────────────────────────────────────────────────
def _cleanup_exports_for_perito(safe_perito: str):
    for f in glob(os.path.join(EXPORT_DIR, f"*_{safe_perito}.*")):
        try: os.remove(f)
        except Exception: pass
    for f in glob(os.path.join(EXPORT_DIR, f"*{safe_perito}.*")):
        if "top10" in os.path.basename(f).lower():
            continue
        try: os.remove(f)
        except Exception: pass

def _cleanup_exports_top10():
    for f in glob(os.path.join(EXPORT_DIR, "*_top10*.*")):
        try: os.remove(f)
        except Exception: pass
    for f in glob(os.path.join(EXPORT_DIR, "*top10*.*")):
        try: os.remove(f)
        except Exception: pass

def copiar_artefatos_perito(perito: str, imgs_dir: str, comments_dir: str, orgs_dir: str = None):
    if orgs_dir is None:
        orgs_dir = os.path.join(os.path.dirname(imgs_dir), "orgs")
        os.makedirs(orgs_dir, exist_ok=True)

    safe = _safe(perito)

    # PNGs
    png_patterns = [f"*_{safe}.png", f"*{safe}.png"]
    for pat in png_patterns:
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            if "top10" in base.lower():
                continue
            shutil.copy2(src, os.path.join(imgs_dir, base))
            try: os.remove(src)
            except Exception: pass

    # Comentários já em .org (preferência)
    orgc_patterns = [f"*_{safe}_comment.org", f"*{safe}_comment.org"]
    for pat in orgc_patterns:
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

    # Comentários legados em .md -> converte para .org e salva em comments/
    for pat in (f"*_{safe}_comment.md", f"*{safe}_comment.md"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            if "top10" in base.lower():
                continue
            with open(src, encoding="utf-8") as f:
                md_text = f.read()
            org_text = markdown_para_org(md_text)
            org_text = "\n".join(
                ln for ln in org_text.splitlines()
                if not ln.strip().lower().startswith('#+title')
            ).strip()
            dst = os.path.join(comments_dir, os.path.splitext(base)[0] + ".org")
            with open(dst, "w", encoding="utf-8") as g:
                g.write(org_text + "\n")
            try: os.remove(src)
            except Exception: pass

    # ORGs auxiliares dos scripts (ajusta links para ../imgs/)
    org_patterns = [f"*_{safe}.org", f"*{safe}.org"]
    for pat in org_patterns:
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

def copiar_artefatos_top10(imgs_dir: str, comments_dir: str, orgs_dir: str = None):
    if orgs_dir is None:
        orgs_dir = os.path.join(os.path.dirname(imgs_dir), "orgs")
        os.makedirs(orgs_dir, exist_ok=True)

    # PNGs
    for pat in ("*_top10*.png", "*top10*.png"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            shutil.copy2(src, os.path.join(imgs_dir, base))
            try: os.remove(src)
            except Exception: pass

    # Comentários já em .org
    for pat in ("*_top10*_comment.org", "*top10*_comment.org"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            shutil.copy2(src, os.path.join(comments_dir, base))
            try: os.remove(src)
            except Exception: pass

    # Comentários legados em .md -> converte para .org
    for pat in ("*_top10*_comment.md", "*top10*_comment.md"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            with open(src, encoding="utf-8") as f:
                md_text = f.read()
            org_text = markdown_para_org(md_text)
            org_text = "\n".join(
                ln for ln in org_text.splitlines()
                if not ln.strip().lower().startswith('#+title')
            ).strip()
            dst = os.path.join(comments_dir, os.path.splitext(base)[0] + ".org")
            with open(dst, "w", encoding="utf-8") as g:
                g.write(org_text + "\n")
            try: os.remove(src)
            except Exception: pass

    # ORGs auxiliares (ajusta links)
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

# ────────────────────────────────────────────────────────────────────────────────
# Coletor de saídas R (fallback)
# ────────────────────────────────────────────────────────────────────────────────
def collect_r_outputs_to_export():
    os.makedirs(EXPORT_DIR, exist_ok=True)
    patterns = [
        "rcheck*.png", "rcheck*.md", "rcheck*.org",
        "*top10*.png", "*top10*.md", "*top10*.org",
    ]
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
# Ajustes de .org
# ────────────────────────────────────────────────────────────────────────────────
def shift_org_headings(text: str, delta: int = 1) -> str:
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
    def repl(m):
        path = m.group(1).strip()
        if path.startswith(("http://", "https://", "/")):
            return m.group(0)
        base = os.path.basename(path)
        return f"[[file:{imgs_rel_prefix}{base}]]"
    return re.sub(r"\[\[file:([^\]]+)\]\]", repl, org_text)

def _nice_caption(fname: str) -> str:
    base = os.path.splitext(os.path.basename(fname))[0]
    return base.replace("_", " ").replace("-", " ")

def markdown_para_org(texto_md):
    import tempfile, subprocess as sp
    with tempfile.NamedTemporaryFile("w+", suffix=".md", delete=False) as fmd:
        fmd.write(texto_md)
        fmd.flush()
        org_path = fmd.name.replace(".md", ".org")
        sp.run(["pandoc", fmd.name, "-t", "org", "-o", org_path])
        with open(org_path, encoding="utf-8") as forg:
            org_text = forg.read()
    return org_text

# ────────────────────────────────────────────────────────────────────────────────
# Sanitização do .org para o Pandoc (proteções)
# ────────────────────────────────────────────────────────────────────────────────
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")  # sequências ANSI (plotext etc.)
_BOX_DRAW_RE = re.compile(r"[┌┬┐└┴┘├┼┤│─━┃╭╮╯╰█▓▒░]")

def _strip_ansi(s: str) -> str:
    return _ANSI_RE.sub("", s)

def _wrap_ascii_blocks(text: str) -> str:
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
    text = _ensure_blank_lines_around_tables(text)
    text = _normalize_all_tables(text)
    text = _wrap_ascii_blocks(text)
    return text

def _protect_tables_in_quote(txt: str) -> str:
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

# ────────────────────────────────────────────────────────────────────────────────
# Extrator
# ────────────────────────────────────────────────────────────────────────────────

def _extract_comment_from_org(org_text: str) -> str:
    """
    Extrai o comentário embutido em um .org de gráfico.

    Ordem de busca:
      1) Primeiro bloco #+BEGIN_QUOTE ... #+END_QUOTE
      2) Seção cujo título contenha 'Comentário' (qualquer nível de '*'),
         coletando tudo até o próximo cabeçalho.
      3) Fallback: primeiro parágrafo “corrente” APÓS a imagem (CAPTION/[[file:...]]).

    Em todos os casos, removemos drawers de propriedades, linhas #+..., tabelas e a linha de imagem.
    """
    import re

    def _strip_drawers(s: str) -> str:
        # Remove drawers :PROPERTIES: ... :END:
        return re.sub(r'(?ms)^\s*:PROPERTIES:\s*.*?^\s*:END:\s*$', '', s, flags=re.MULTILINE)

    def _strip_noise_lines(s: str) -> str:
        out = []
        for ln in s.splitlines():
            sl = ln.strip()
            # remove diretivas e tabelas/imagens
            if not sl:
                out.append(ln)
                continue
            if sl.startswith("#+"):
                continue
            if sl.startswith("|"):
                continue
            if sl.startswith("[[file:"):
                continue
            out.append(ln)
        return "\n".join(out)

    txt = org_text

    # 1) Bloco QUOTE
    m = re.search(r'(?mis)^\s*#\+BEGIN_QUOTE\s*(.*?)^\s*#\+END_QUOTE', txt)
    if m:
        body = _strip_drawers(m.group(1))
        body = _strip_noise_lines(body)
        body = body.strip()
        if body:
            return body

    # 2) Seção ** Comentário
    m = re.search(r'(?mis)^\*+\s+coment[aá]ri?o[^\n]*\n(.*?)(?=^\*+\s|\Z)', txt)
    if m:
        body = _strip_drawers(m.group(1))
        body = _strip_noise_lines(body).strip()
        if body:
            return body

    # 3) Fallback: 1º parágrafo “corrente” APÓS a imagem
    #    – pega tudo depois da primeira linha [[file:...]]
    after_img = re.split(r'(?mi)^\s*\[\[file:[^\]]+\]\]\s*$', txt, maxsplit=1)
    if len(after_img) == 2:
        tail = _strip_drawers(after_img[1])
        tail = _strip_noise_lines(tail)
        # primeiro parágrafo “corrente”
        para = []
        started = False
        for ln in tail.splitlines():
            s = ln.strip()
            if not s:
                if started:
                    break
                else:
                    continue
            if s.startswith("*") or s.startswith("#+") or s.startswith("|"):
                if started:
                    break
                else:
                    continue
            para.append(ln)
            started = True
        res = " ".join(" ".join(para).split())
        if res:
            return res

    return ""

# ────────────────────────────────────────────────────────────────────────────────
# Estatísticas de cabeçalho e apêndices
# ────────────────────────────────────────────────────────────────────────────────
def get_summary_stats(perito, start, end):
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

def gerar_apendice_nc(perito, start, end):
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

def gerar_r_apendice_comments_if_possible(perito: str, imgs_dir: str, comments_dir: str, start: str, end: str):
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
# Org por perito e do grupo (inclui apêndice R)
# ────────────────────────────────────────────────────────────────────────────────
def gerar_org_perito(perito, start, end, add_comments, imgs_dir, comments_dir, output_dir):
    safe = _safe(perito)
    org_path = os.path.join(output_dir, f"{safe}.org")
    lines = []
    lines.append(f"** {perito}")
    total, pct_nc, cr, dr = get_summary_stats(perito, start, end)
    lines.append(f"- Tarefas: {total}")
    lines.append(f"- % NC: {pct_nc:.1f}")
    lines.append(f"- CR: {cr} | DR: {dr}")
    lines.append("")

    # Gráficos principais (Python) — ORDEM CONTROLADA
    all_pngs = glob(os.path.join(imgs_dir, f"*{safe}.png"))
    main_pngs = [p for p in all_pngs if not os.path.basename(p).lower().startswith("rcheck_")]
    main_pngs.sort(key=_png_rank_main)

    for png in main_pngs:
        base = os.path.basename(png)
        lines.append(f"#+ATTR_LATEX: :placement [H] :width \\linewidth")
        lines.append(f"#+CAPTION: {_nice_caption(base)}")
        lines.append(f"[[file:imgs/{base}]]")
        if add_comments:
            stem = os.path.splitext(base)[0]
            # 1) preferir comments/*.org
            inserted = False
            for orgc in (os.path.join(comments_dir, f"{stem}_comment.org"),
                         os.path.join(comments_dir, f"{stem}.org")):
                if os.path.exists(orgc):
                    with open(orgc, encoding="utf-8") as f:
                        comment_org = f.read().strip()
                    comment_org = _protect_tables_in_quote(comment_org)
                    lines.append("")
                    lines.append("#+BEGIN_QUOTE")
                    lines.append(comment_org)
                    lines.append("#+END_QUOTE")
                    inserted = True
                    break
            if not inserted:
                # 2) fallback: comments/*.md -> converte para org inline
                for md in (os.path.join(comments_dir, f"{stem}_comment.md"),
                           os.path.join(comments_dir, f"{stem}.md")):
                    if os.path.exists(md):
                        with open(md, encoding="utf-8") as f:
                            comment_md = f.read().strip()
                        comment_org = markdown_para_org(comment_md)
                        comment_org = "\n".join(
                            ln for ln in comment_org.splitlines()
                            if not ln.strip().lower().startswith('#+title')
                        ).strip()
                        comment_org = _protect_tables_in_quote(comment_org)
                        lines.append("")
                        lines.append("#+BEGIN_QUOTE")
                        lines.append(comment_org)
                        lines.append("#+END_QUOTE")
                        inserted = True
                        break
            if not inserted:
                # 3) fallback final: extrair 1º parágrafo do .org auxiliar
                aux_org_path = os.path.join(output_dir, "orgs", f"{stem}.org")
                if os.path.exists(aux_org_path):
                    with open(aux_org_path, encoding="utf-8") as f:
                        aux_org = f.read()
                    extra = _extract_comment_from_org(aux_org)
                    if extra:
                        extra = _protect_tables_in_quote(extra)
                        lines.append("")
                        lines.append("#+BEGIN_QUOTE")
                        lines.append(extra)
                        lines.append("#+END_QUOTE")
        lines.append("\n#+LATEX: \\newpage\n")

    # Apêndice: Protocolos NC por motivo
    apdf = gerar_apendice_nc(perito, start, end)
    if not apdf.empty:
        lines.append(f"*** Apêndice: Protocolos Não-Conformados por Motivo")
        grouped = apdf.groupby('motivo_text')['protocolo'].apply(lambda seq: ', '.join(map(str, seq))).reset_index()
        for _, grp in grouped.iterrows():
            lines.append(f"- *{grp['motivo_text']}*: {grp['protocolo']}")
        lines.append("")

    # Apêndice: R checks (perito) — ORDEM CONTROLADA 01→06
    r_pngs = glob(os.path.join(imgs_dir, f"rcheck_*_{safe}.png"))
    r_pngs.sort(key=lambda p: _rcheck_perito_rank(p))
    if r_pngs:
        lines.append(f"*** Apêndice estatístico (R) — {perito}\n")
        for png in r_pngs:
            base = os.path.basename(png)
            lines.append(f"#+ATTR_LATEX: :placement [H] :width \\linewidth")
            lines.append(f"#+CAPTION: {_nice_caption(base)}")
            lines.append(f"[[file:imgs/{base}]]")
            if add_comments:
                stem = os.path.splitext(base)[0]
                inserted = False
                for orgc in (os.path.join(comments_dir, f"{stem}_comment.org"),
                             os.path.join(comments_dir, f"{stem}.org")):
                    if os.path.exists(orgc):
                        with open(orgc, encoding="utf-8") as f:
                            comment_org = f.read().strip()
                        comment_org = _protect_tables_in_quote(comment_org)
                        lines.append("")
                        lines.append("#+BEGIN_QUOTE")
                        lines.append(comment_org)
                        lines.append("#+END_QUOTE")
                        inserted = True
                        break
                if not inserted:
                    for md in (os.path.join(comments_dir, f"{stem}_comment.md"),
                               os.path.join(comments_dir, f"{stem}.md")):
                        if os.path.exists(md):
                            with open(md, encoding="utf-8") as f:
                                comment_md = f.read().strip()
                            comment_org = markdown_para_org(comment_md)
                            comment_org = "\n".join(
                                ln for ln in comment_org.splitlines()
                                if not ln.strip().lower().startswith('#+title')
                            ).strip()
                            comment_org = _protect_tables_in_quote(comment_org)
                            lines.append("")
                            lines.append("#+BEGIN_QUOTE")
                            lines.append(comment_org)
                            lines.append("#+END_QUOTE")
                            break
            lines.append("\n#+LATEX: \\newpage\n")

    # ORGs auxiliares (dos scripts) — DEVE SER SEMPRE A ÚLTIMA SEÇÃO
    lines.append("#+LATEX: \\FloatBarrier")
    org_aux_dir = os.path.join(output_dir, "orgs")
    org_aux = sorted(glob(os.path.join(org_aux_dir, f"*{safe}.org")))
    if org_aux:
        lines.append(f"*** Arquivos .org auxiliares dos gráficos")
        for fpath in org_aux:
            fname = os.path.basename(fpath)
            lines.append(f"- [[file:orgs/{fname}][{fname}]]")
        lines.append("")

    with open(org_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"✅ Org individual salvo em: {org_path}")
    return org_path

def gerar_org_top10_grupo(start, end, output_dir, imgs_dir, comments_dir):
    org_path = os.path.join(output_dir, f"top10_grupo.org")
    lines = []
    lines.append(f"** Top 10 — Gráficos do Grupo ({start} a {end})\n")

    all_pngs = sorted(glob(os.path.join(imgs_dir, "*_top10*.png")) + glob(os.path.join(imgs_dir, "*top10*.png")))
    main_pngs = [p for p in all_pngs if "rcheck_" not in os.path.basename(p).lower()]
    r_pngs    = [p for p in all_pngs if "rcheck_" in os.path.basename(p).lower()]

    main_pngs.sort(key=_png_rank_main)
    r_pngs.sort(key=lambda p: _rcheck_group_rank(p))

    for png in main_pngs + r_pngs:
        base = os.path.basename(png)
        lines.append(f"#+ATTR_LATEX: :placement [H] :width \\linewidth")
        lines.append(f"#+CAPTION: {_nice_caption(base)}")
        lines.append(f"[[file:imgs/{base}]]")
        stem = os.path.splitext(base)[0]
        # Preferir comments/*.org; depois .md; por fim extrair do org auxiliar
        inserted = False
        for orgc in (os.path.join(comments_dir, f"{stem}_comment.org"),
                     os.path.join(comments_dir, f"{stem}.org")):
            if os.path.exists(orgc):
                with open(orgc, encoding="utf-8") as f:
                    comment_org = f.read().strip()
                comment_org = _protect_tables_in_quote(comment_org)
                lines.append("")
                lines.append("#+BEGIN_QUOTE")
                lines.append(comment_org)
                lines.append("#+END_QUOTE")
                inserted = True
                break
        if not inserted:
            for md in (os.path.join(comments_dir, f"{stem}_comment.md"),
                       os.path.join(comments_dir, f"{stem}.md")):
                if os.path.exists(md):
                    with open(md, encoding="utf-8") as f:
                        comment_md = f.read().strip()
                    comment_org = markdown_para_org(comment_md)
                    comment_org = "\n".join(
                        ln for ln in comment_org.splitlines()
                        if not ln.strip().lower().startswith('#+title')
                    ).strip()
                    comment_org = _protect_tables_in_quote(comment_org)
                    lines.append("")
                    lines.append("#+BEGIN_QUOTE")
                    lines.append(comment_org)
                    lines.append("#+END_QUOTE")
                    inserted = True
                    break
        if not inserted:
            aux_org_path = os.path.join(output_dir, "orgs", f"{stem}.org")
            if os.path.exists(aux_org_path):
                with open(aux_org_path, encoding="utf-8") as f:
                    aux_org = f.read()
                extra = _extract_comment_from_org(aux_org)
                if extra:
                    extra = _protect_tables_in_quote(extra)
                    lines.append("")
                    lines.append("#+BEGIN_QUOTE")
                    lines.append(extra)
                    lines.append("#+END_QUOTE")
        lines.append("\n#+LATEX: \\newpage\n")

    with open(org_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"✅ Org do grupo Top 10 salvo em: {org_path}")
    return org_path

def gerar_org_individual_consolidado(perito, start, end, relatorio_dir):
    safe = _safe(perito)
    perito_org = os.path.join(relatorio_dir, f"{safe}.org")
    final_org  = os.path.join(relatorio_dir, f"relatorio_{safe}_{start}_a_{end}.org")
    if not os.path.exists(perito_org):
        raise FileNotFoundError(f"Org individual não encontrado: {perito_org}")
    with open(perito_org, "r", encoding="utf-8") as f:
        content = f.read().strip()

    content = _protect_org_text_for_pandoc(content)

    lines = [
        f"* Relatório individual — {perito} ({start} a {end})",
        "",
        content,
        ""
    ]
    with open(final_org, "w", encoding="utf-8") as g:
        g.write("\n".join(lines))
    print(f"✅ Org consolidado (individual) salvo em: {final_org}")
    return final_org

# ────────────────────────────────────────────────────────────────────────────────
# Export PDF (com header LaTeX para segurar figuras no lugar)
# ────────────────────────────────────────────────────────────────────────────────
LATEX_HEADER_CONTENT = r"""
%% Inserido automaticamente pelo make_report.py
\usepackage{float}
\usepackage{placeins}
\floatplacement{figure}{H}
"""

def exportar_org_para_pdf(org_path, font="DejaVu Sans"):
    import shutil as sh
    output_dir = os.path.dirname(org_path)
    org_name = os.path.basename(org_path)
    pdf_name = org_name.replace('.org', '.pdf')
    log_path = org_path + ".log"
    header_path = os.path.join(output_dir, "_header_figs.tex")
    with open(header_path, "w", encoding="utf-8") as fh:
        fh.write(LATEX_HEADER_CONTENT)

    with open(org_path, "r", encoding="utf-8") as f:
        raw = f.read()
    protected = _protect_org_text_for_pandoc(raw)
    prot_name = org_name.replace(".org", "._pandoc.org")
    prot_path = os.path.join(output_dir, prot_name)
    with open(prot_path, "w", encoding="utf-8") as fprot:
        fprot.write(protected)

    pandoc = sh.which("pandoc")
    if not pandoc:
        print("❌ Pandoc não encontrado no PATH. Instale com: sudo apt install pandoc texlive-xetex")
        return None
    cmd = [
        "pandoc", prot_name, "-o", pdf_name,
        "--pdf-engine=xelatex",
        "--include-in-header", os.path.basename(header_path),
        "--variable", f"mainfont={font}",
        "--variable", "geometry:margin=2cm",
        "--highlight-style=zenburn"
    ]
    print(f"[Pandoc] Gerando PDF: {' '.join(cmd)} (cwd={output_dir})")
    prev_cwd = os.getcwd()
    try:
        os.chdir(output_dir)
        with open(log_path, "w", encoding="utf-8") as flog:
            result = subprocess.run(cmd, stdout=flog, stderr=flog, text=True)
    finally:
        os.chdir(prev_cwd)
    pdf_path = os.path.join(output_dir, pdf_name)
    if result.returncode == 0 and os.path.exists(pdf_path):
        print(f"✅ PDF gerado: {pdf_path}")
    else:
        print(f"❌ Erro ao gerar PDF. Veja o log: {log_path}")
    return pdf_path

def adicionar_capa_pdf(pdf_final_path):
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

# ────────────────────────────────────────────────────────────────────────────────
# Execução
# ────────────────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    args = parse_args()
    
    # avisa apenas se vamos tentar gerar comentários
    if (args.add_comments or getattr(args, "export_comment_org", False)) and not os.getenv("OPENAI_API_KEY"):
        print("⚠️  Sem OPENAI_API_KEY. Os scripts com comentário podem cair no fallback (sem IA).")

    if args.r_appendix:
        _preflight_r(args.r_bin)

    PERIODO_DIR = os.path.join(OUTPUTS_DIR, f"{args.start}_a_{args.end}")

    # Se for individual, a pasta leva o nome do perito
    if args.top10:
        RELATORIO_DIR = os.path.join(PERIODO_DIR, "top10")
    else:
        safe_perito = _safe(args.perito.strip())
        RELATORIO_DIR = os.path.join(PERIODO_DIR, "individual", safe_perito)

    IMGS_DIR      = os.path.join(RELATORIO_DIR, "imgs")
    COMMENTS_DIR  = os.path.join(RELATORIO_DIR, "comments")
    ORGS_DIR      = os.path.join(RELATORIO_DIR, "orgs")
    for d in (PERIODO_DIR, RELATORIO_DIR, IMGS_DIR, COMMENTS_DIR, ORGS_DIR):
        os.makedirs(d, exist_ok=True)

    planned_cmds = []

    if args.top10:
        peritos_df = pegar_10_piores_peritos(args.start, args.end, min_analises=args.min_analises)
        if peritos_df.empty:
            print("Nenhum perito encontrado com os critérios.")
            return
        lista_top10 = peritos_df['nomePerito'].tolist()
        set_top10 = set(lista_top10)
        print(f"Gerando para os 10 piores: {lista_top10}")

        _cleanup_exports_top10()
        group_ctx = {
            "kind": "top10",
            "start": args.start,
            "end": args.end,
            "min_analises": args.min_analises,
            "add_comments": args.add_comments,
        }
        for script in SCRIPT_ORDER:
            script_file = script_path(script)
            cmds = build_commands_for_script(script_file, group_ctx)
            planned_cmds.extend(cmds)

        if args.r_appendix:
            planned_cmds.extend(build_r_commands_for_top10(args.start, args.end, args.r_bin, args.min_analises))

        for perito in lista_top10:
            if not perito_tem_dados(perito, args.start, args.end):
                print(f"⚠️  Perito '{perito}' sem análises no período! Pulando.")
                continue
            safe = _safe(perito)
            _cleanup_exports_for_perito(safe)
            indiv_ctx = {
                "kind": "perito",
                "perito": perito,
                "start": args.start,
                "end": args.end,
                "add_comments": args.add_comments,
            }
            for script in SCRIPT_ORDER:
                script_file = script_path(script)
                cmds = build_commands_for_script(script_file, indiv_ctx)
                planned_cmds.extend(cmds)
            if args.r_appendix:
                planned_cmds.extend(build_r_commands_for_perito(perito, args.start, args.end, args.r_bin))

        extras_list = []
        if args.include_high_nc:
            df_high = pegar_peritos_nc_altissima(args.start, args.end,
                                                 nc_threshold=args.high_nc_threshold,
                                                 min_tasks=args.high_nc_min_tasks)
            if not df_high.empty:
                extras_list = [n for n in df_high['nomePerito'].tolist() if n not in set_top10]
                if extras_list:
                    print(f"Incluindo coorte extra (%NC ≥ {args.high_nc_threshold} e ≥ {args.high_nc_min_tasks} tarefas): {extras_list}")
                else:
                    print("Coorte extra presente, mas todos já estão no Top 10 — sem novos peritos.")
            else:
                print("Nenhum perito com %NC acima do limiar e mínimo de tarefas.")
        else:
            print("Coorte extra de %NC alta desativada (--no-high-nc).")

        for perito in extras_list:
            if not perito_tem_dados(perito, args.start, args.end):
                print(f"⚠️  Perito '{perito}' sem análises no período! Pulando.")
                continue
            safe = _safe(perito)
            _cleanup_exports_for_perito(safe)
            indiv_ctx = {
                "kind": "perito",
                "perito": perito,
                "start": args.start,
                "end": args.end,
                "add_comments": args.add_comments,
            }
            for script in SCRIPT_ORDER:
                script_file = script_path(script)
                cmds = build_commands_for_script(script_file, indiv_ctx)
                planned_cmds.extend(cmds)
            if args.r_appendix:
                planned_cmds.extend(build_r_commands_for_perito(perito, args.start, args.end, args.r_bin))

    else:
        perito = args.perito.strip()
        if not perito_tem_dados(perito, args.start, args.end):
            print(f"⚠️  Perito '{perito}' sem análises no período.")
            return
        safe = _safe(perito)
        _cleanup_exports_for_perito(safe)
        indiv_ctx = {
            "kind": "perito",
            "perito": perito,
            "start": args.start,
            "end": args.end,
            "add_comments": args.add_comments,
        }
        for script in SCRIPT_ORDER:
            script_file = script_path(script)
            cmds = build_commands_for_script(script_file, indiv_ctx)
            planned_cmds.extend(cmds)
        if args.r_appendix:
            planned_cmds.extend(build_r_commands_for_perito(perito, args.start, args.end, args.r_bin))

    # Bootstrap R deps primeiro
    if args.r_appendix:
        cmd_boot = _r_deps_bootstrap_cmd(args.r_bin)
        if cmd_boot:
            planned_cmds.insert(0, cmd_boot)
        else:
            print("[AVISO] r_checks/_ensure_deps.R não encontrado; pulando bootstrap de pacotes.")

    if args.plan_only:
        print("\n===== PLANO DE EXECUÇÃO (dry-run) =====")
        for c in planned_cmds:
            print(" ".join(map(str, c)))
        print("=======================================\n")
        return

    # Executa comandos
    for cmd in planned_cmds:
        print(f"[RUN] {' '.join(map(str, cmd))}")
        try:
            if _is_r_cmd(cmd):
                subprocess.run(cmd, check=False, cwd=RCHECK_DIR)
            else:
                subprocess.run(cmd, check=False, env=_env_with_project_path(), cwd=SCRIPTS_DIR)
        except Exception as e:
            print(f"[ERRO] Falha executando: {' '.join(map(str, cmd))}\n  -> {e}")

    # Coleta saídas de R
    collect_r_outputs_to_export()

    org_paths = []
    extras_org_paths = []
    org_grupo_top10 = None

    if args.top10:
        copiar_artefatos_top10(IMGS_DIR, COMMENTS_DIR, ORGS_DIR)

        if args.r_appendix and args.add_comments:
            gerar_r_apendice_group_comments_if_possible(IMGS_DIR, COMMENTS_DIR, args.start, args.end)

        org_grupo_top10 = gerar_org_top10_grupo(args.start, args.end, RELATORIO_DIR, IMGS_DIR, COMMENTS_DIR)

        peritos_df = pegar_10_piores_peritos(args.start, args.end, min_analises=args.min_analises)
        lista_top10 = peritos_df['nomePerito'].tolist()
        for perito in lista_top10:
            if not perito_tem_dados(perito, args.start, args.end):
                continue
            copiar_artefatos_perito(perito, IMGS_DIR, COMMENTS_DIR, ORGS_DIR)
            if args.r_appendix and args.add_comments:
                gerar_r_apendice_comments_if_possible(perito, IMGS_DIR, COMMENTS_DIR, args.start, args.end)
            org_path = gerar_org_perito(perito, args.start, args.end, args.add_comments, IMGS_DIR, COMMENTS_DIR, RELATORIO_DIR)
            org_paths.append(org_path)

        if args.include_high_nc:
            df_high = pegar_peritos_nc_altissima(args.start, args.end,
                                                 nc_threshold=args.high_nc_threshold,
                                                 min_tasks=args.high_nc_min_tasks)
            set_top10 = set(lista_top10)
            extras_list = [n for n in df_high['nomePerito'].tolist() if n not in set_top10]
            for perito in extras_list:
                if not perito_tem_dados(perito, args.start, args.end):
                    continue
                copiar_artefatos_perito(perito, IMGS_DIR, COMMENTS_DIR, ORGS_DIR)
                if args.r_appendix and args.add_comments:
                    gerar_r_apendice_comments_if_possible(perito, IMGS_DIR, COMMENTS_DIR, args.start, args.end)
                org_path = gerar_org_perito(perito, args.start, args.end, args.add_comments, IMGS_DIR, COMMENTS_DIR, RELATORIO_DIR)
                extras_org_paths.append(org_path)

        org_to_export = None
        if (args.export_org or args.export_pdf) and (org_paths or org_grupo_top10 or extras_org_paths):
            org_final = os.path.join(RELATORIO_DIR, f"relatorio_dez_piores_{args.start}_a_{args.end}.org")
            lines = [f"* Relatório dos 10 piores peritos ({args.start} a {args.end})", ""]
            if org_grupo_top10 and os.path.exists(org_grupo_top10):
                with open(org_grupo_top10, encoding="utf-8") as f:
                    lines.append(f.read().strip())
                    lines.append("#+LATEX: \\newpage\n")
            for org_path in org_paths:
                with open(org_path, encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        lines.append(content)
                        lines.append("#+LATEX: \\newpage\n")
            if extras_org_paths:
                lines.append(f"** Peritos com %NC ≥ {args.high_nc_threshold:.0f}% e ≥ {args.high_nc_min_tasks} tarefas\n")
                for org_path in extras_org_paths:
                    with open(org_path, encoding="utf-8") as f:
                        content = f.read().strip()
                        if content:
                            content = shift_org_headings(content, delta=1)
                            lines.append(content)
                            lines.append("#+LATEX: \\newpage\n")
            with open(org_final, "w", encoding="utf-8") as f:
                f.write("\n".join(lines).strip() + "\n")
            print(f"✅ Org consolidado salvo em: {org_final}")
            org_to_export = org_final
        else:
            org_to_export = org_paths[0] if org_paths else None

    else:
        perito = args.perito.strip()

        copiar_artefatos_perito(
            perito,
            os.path.join(RELATORIO_DIR, "imgs"),
            os.path.join(RELATORIO_DIR, "comments"),
            os.path.join(RELATORIO_DIR, "orgs")
        )

        if args.r_appendix and args.add_comments:
            gerar_r_apendice_comments_if_possible(
                perito,
                os.path.join(RELATORIO_DIR, "imgs"),
                os.path.join(RELATORIO_DIR, "comments"),
                args.start, args.end
            )

        _ = gerar_org_perito(
            perito, args.start, args.end, args.add_comments,
            os.path.join(RELATORIO_DIR, "imgs"),
            os.path.join(RELATORIO_DIR, "comments"),
            RELATORIO_DIR
        )

        if args.export_org or args.export_pdf:
            org_to_export = gerar_org_individual_consolidado(
                perito, args.start, args.end, RELATORIO_DIR
            )
        else:
            org_to_export = os.path.join(RELATORIO_DIR, f"{_safe(perito)}.org")

    if args.export_pdf and org_to_export:
        pdf_path = exportar_org_para_pdf(org_to_export, font="DejaVu Sans")
        if pdf_path and os.path.exists(pdf_path):
            adicionar_capa_pdf(pdf_path)

    dt = time.time() - t0
    mm, ss = divmod(int(dt + 0.5), 60)
    hh, mm = divmod(mm, 60)
    print(f"⏱️ Tempo total: {hh:02d}:{mm:02d}:{ss:02d}")

if __name__ == '__main__':
    main()

