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

# ────────────────────────────────────────────────────────────────────────────────
# Ordem e defaults dos SCRIPTS PYTHON
# ────────────────────────────────────────────────────────────────────────────────
SCRIPT_ORDER = [
    "compare_nc_rate.py",
    "compare_fifteen_seconds.py",
    "compare_overlap.py",
    "compare_productivity.py",
    "compare_indicadores_composto.py",
    "compare_motivos_perito_vs_brasil.py",
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
]

# ────────────────────────────────────────────────────────────────────────────────
# R checks (apêndice estatístico) — Top 10 (grupo)
# ────────────────────────────────────────────────────────────────────────────────
# Estes devem produzir arquivos no padrão: rcheck_top10_<slug>.png (ex.: rcheck_top10_nc_rate.png)
RCHECK_GROUP_SCRIPTS = [
    ("g01_top10_nc_rate_check.R",        {"defaults": {}}),
    ("g02_top10_le15s_check.R",          {"defaults": {"--threshold": FIFTEEN_THRESHOLD}}),
    ("g03_top10_productivity_check.R",   {"defaults": {"--threshold": PRODUCTIVITY_THRESHOLD}}),
    ("g04_top10_overlap_check.R",        {"defaults": {}}),
    ("g05_top10_motivos_chisq.R",        {"defaults": {}}),
    ("g06_top10_composite_robustness.R", {"defaults": {}}),
]

# Comentário ChatGPT para apêndice R: tenta importar; ignora se não houver
try:
    from utils.comentarios import comentar_r_apendice  # função opcional (usa tanto perito quanto grupo)
except Exception:
    comentar_r_apendice = None

# ────────────────────────────────────────────────────────────────────────────────
# Conhecimento explícito (fallback) das flags de cada script PYTHON
# ────────────────────────────────────────────────────────────────────────────────
ASSUME_FLAGS = {
    "compare_nc_rate.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--export-md", "--export-png", "--export-org", "--export-comment",
    },
    "compare_fifteen_seconds.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--threshold", "--cut-n",
        "--export-md", "--export-png", "--export-org", "--export-comment",
    },
    "compare_overlap.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--mode", "--chart",
        "--export-md", "--export-png", "--export-org", "--export-comment",
    },
    "compare_productivity.py": {
        "--perito", "--nome", "--top10", "--min-analises",
        "--threshold", "--mode", "--chart",
        "--export-md", "--export-png", "--export-org", "--export-comment",
    },
    "compare_indicadores_composto.py": {
        "--perito", "--top10", "--min-analises",
        "--alvo-prod", "--cut-prod-pct", "--cut-nc-pct", "--cut-le15s-pct", "--cut-overlap-pct",
        "--export-png", "--export-org", "--chart",
    },
    "compare_motivos_perito_vs_brasil.py": {
        "--perito", "--top10", "--min-analises",
        "--topn", "--min-pct-perito", "--min-pct-brasil", "--min-n-perito", "--min-n-brasil",
        "--label-maxlen", "--label-fontsize",
        "--chart", "--export-md", "--export-org", "--export-png", "--export-comment",
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

    # Coorte extra: %NC muito alta (além do Top 10)
    p.add_argument('--include-high-nc', dest='include_high_nc', action='store_true', default=True,
                   help='(default) Incluir peritos com %NC ≥ limiar e tarefas ≥ min no relatório Top 10')
    p.add_argument('--no-high-nc', dest='include_high_nc', action='store_false',
                   help='Não incluir o coorte extra de %NC muito alta')
    p.add_argument('--high-nc-threshold', type=float, default=90.0, help='Limiar de %NC para o coorte extra (padrão: 90)')
    p.add_argument('--high-nc-min-tasks', type=int, default=50, help='Mínimo de tarefas para o coorte extra (padrão: 50)')

    # Saídas finais
    p.add_argument('--export-org', action='store_true', help='Exporta relatório consolidado em Org-mode')
    p.add_argument('--export-pdf', action='store_true', help='Exporta relatório consolidado em PDF (via Pandoc)')
    p.add_argument('--add-comments', action='store_true', help='Inclui comentários GPT (quando suportado pelos scripts)')

    # Planejamento
    p.add_argument('--plan-only', action='store_true', help='Somente imprime os comandos planejados (não executa nada)')

    # Apêndice R
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
    """Tenta ler --help e extrair flags e choices de --mode. Tolerante a falhas."""
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
    """Inspeciona o código R para descobrir se ele aceita '--out-dir' ou '--out'."""
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
    deps = os.path.join(RCHECK_DIR, "_ensure_deps.R")
    if os.path.exists(deps):
        return [r_bin, deps]
    return None

# ────────────────────────────────────────────────────────────────────────────────
# Planejador (PYTHON)
# ────────────────────────────────────────────────────────────────────────────────
def build_commands_for_script(script_file: str, context: dict) -> list:
    """
    Retorna lista de comandos CLI prontos para os scripts Python.
    Garante obrigatórios (--perito/--top10), injeta exports, thresholds, e itera pelos modos.
    """
    cmds = []

    name = os.path.basename(script_file)
    help_info = introspect_script(script_file)
    flags = set(help_info.get("flags", set()))

    # Fallback: se nada encontrado, assume capacidades conhecidas
    if not flags and name in ASSUME_FLAGS:
        flags = set(ASSUME_FLAGS[name])

    modes = detect_modes(script_file, help_info)
    base = ["--start", context["start"], "--end", context["end"]]

    # Quem comparar
    if context["kind"] == "perito":
        perito = context["perito"]
        if "--perito" in flags:
            base += ["--perito", perito]
        elif "--nome" in flags:
            base += ["--nome", perito]
        else:
            base += ["--perito", perito]  # força (nossos scripts aceitam)
    else:
        if ("--top10" in flags) or (name in ASSUME_FLAGS and "--top10" in ASSUME_FLAGS[name]):
            base += ["--top10"]
            if "--min-analises" in flags:
                base += ["--min-analises", str(context["min_analises"])]
        else:
            print(f"[INFO] {name} não suporta --top10; pulando no grupo.")
            return []

    # Exports
    if "--export-md" in flags:
        base += ["--export-md"]
    if "--export-png" in flags:
        base += ["--export-png"]
    if "--export-org" in flags:
        base += ["--export-org"]
    if context.get("add_comments") and "--export-comment" in flags:
        base += ["--export-comment"]

    # Defaults por script
    if name == "compare_productivity.py":
        if "--threshold" in flags:
            base += ["--threshold", PRODUCTIVITY_THRESHOLD]
    if name == "compare_fifteen_seconds.py":
        if "--threshold" in flags:
            base += ["--threshold", FIFTEEN_THRESHOLD]
        if "--cut-n" in flags:
            base += ["--cut-n", FIFTEEN_CUT_N]

    # Modos (quando houver)
    if "--mode" in flags and modes:
        for m in modes:
            cmd = [sys.executable, script_file] + base + ["--mode", m]
            # extras específicos
            extra = EXTRA_ARGS.get(name, [])
            i = 0
            while i < len(extra):
                tok = extra[i]
                if isinstance(tok, str) and tok.startswith("--") and tok in flags:
                    cmd.append(tok)
                    if i + 1 < len(extra) and (not isinstance(extra[i+1], str) or not extra[i+1].startswith("--")):
                        cmd.append(str(extra[i+1]))
                        i += 1
                i += 1
            cmds.append(cmd)
    else:
        cmd = [sys.executable, script_file] + base
        extra = EXTRA_ARGS.get(name, [])
        i = 0
        while i < len(extra):
            tok = extra[i]
            if isinstance(tok, str) and tok.startswith("--") and tok in flags:
                cmd.append(tok)
                if i + 1 < len(extra) and (not isinstance(extra[i+1], str) or not extra[i+1].startswith("--")):
                    cmd.append(str(extra[i+1]))
                    i += 1
            i += 1
        cmds.append(cmd)

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
        cmd = [r_bin, fpath, "--db", DB_PATH, "--start", start, "--end", end, "--min-analises", str(min_analises)]
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
    SELECT p.nomePerito,
           COUNT(*) AS total,
           SUM(CASE WHEN a.motivoNaoConformado != 0 THEN 1 ELSE 0 END) AS nc_count,
           (SUM(CASE WHEN a.motivoNaoConformado != 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*) AS pct_nc
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
    # também limpa padrões sem underscore
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

    # PNGs (inclui rcheck_*; pega com e sem underscore; exclui top10)
    png_patterns = [f"*_{safe}.png", f"*{safe}.png"]
    for pat in png_patterns:
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            if "top10" in base.lower():
                continue
            shutil.copy2(src, os.path.join(imgs_dir, base))
            try: os.remove(src)
            except Exception: pass

    # Comentários (md e _comment.md) — idem filtro top10
    md_patterns = [f"*_{safe}.md", f"*{safe}.md", f"*_{safe}_comment.md", f"*{safe}_comment.md"]
    for pat in md_patterns:
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            if "top10" in base.lower():
                continue
            shutil.copy2(src, os.path.join(comments_dir, base))
            try: os.remove(src)
            except Exception: pass

    # ORG auxiliares gerados pelos scripts — idem filtro top10
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

    # PNGs de grupo (inclui motivos_top10_vs_brasil.png, rcheck_top10_*.png e variantes sem underscore)
    for pat in ("*_top10*.png", "*top10*.png"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            shutil.copy2(src, os.path.join(imgs_dir, base))
            try: os.remove(src)
            except Exception: pass

    # Comentários de grupo
    for pat in ("*_top10*.md", "*top10*.md", "*_top10*_comment.md", "*top10*_comment.md"):
        for src in glob(os.path.join(EXPORT_DIR, pat)):
            base = os.path.basename(src)
            shutil.copy2(src, os.path.join(comments_dir, base))
            try: os.remove(src)
            except Exception: pass

    # ORGs de grupo
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
    """
    Copia artefatos gerados em r_checks/ para exports/ caso os .R não aceitem flag de saída.
    """
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
# Estatísticas de cabeçalho e apêndices
# ────────────────────────────────────────────────────────────────────────────────
def get_summary_stats(perito, start, end):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT cr, dr FROM peritos WHERE nomePerito = ?""", (perito,))
    row = cur.fetchone()
    cr, dr = (row if row else ("-", "-"))
    cur.execute("""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN motivoNaoConformado != 0 THEN 1 ELSE 0 END) AS nc_count
          FROM analises a
          JOIN peritos p ON a.siapePerito = p.siapePerito
         WHERE p.nomePerito = ?
           AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
    """, (perito, start, end))
    total, nc_count = cur.fetchone()
    conn.close()
    pct_nc = (nc_count or 0) / (total or 1) * 100
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
           AND a.motivoNaoConformado != 0
         ORDER BY a.protocolo
    """, conn, params=(perito, start, end))
    conn.close()
    return df

def gerar_r_apendice_comments_if_possible(perito: str, imgs_dir: str, comments_dir: str, start: str, end: str):
    """Se houver comentar_r_apendice, cria .md para cada rcheck_* do perito."""
    if comentar_r_apendice is None:
        return
    safe = _safe(perito)
    r_pngs = sorted(glob(os.path.join(imgs_dir, f"rcheck_*_{safe}.png")))
    for png in r_pngs:
        base = os.path.basename(png)
        stem = os.path.splitext(base)[0]  # rcheck_xxx_<safe>
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
    """Se houver comentar_r_apendice, cria .md para cada rcheck_top10_* (grupo)."""
    if comentar_r_apendice is None:
        return
    r_pngs = sorted(glob(os.path.join(imgs_dir, "rcheck_top10_*.png")))
    for png in r_pngs:
        base = os.path.basename(png)
        stem = os.path.splitext(base)[0]  # rcheck_top10_xxx
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

    # Gráficos principais (Python)
    pngs = sorted(glob(os.path.join(imgs_dir, f"*{safe}.png")))
    for png in pngs:
        base = os.path.basename(png)
        # pula os rcheck_* aqui; eles irão para apêndice
        if base.startswith("rcheck_"):
            continue
        lines.append(f"#+CAPTION: {_nice_caption(base)}")
        lines.append(f"[[file:imgs/{base}]]")

        if add_comments:
            stem = os.path.splitext(base)[0]
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
                    lines.append("")
                    lines.append("#+BEGIN_QUOTE")
                    lines.append(comment_org)
                    lines.append("#+END_QUOTE")
                    break
        lines.append("\n#+LATEX: \\newpage\n")

    # Apêndice: Protocolos NC por motivo
    apdf = gerar_apendice_nc(perito, start, end)
    if not apdf.empty:
        lines.append(f"*** Apêndice: Protocolos Não-Conformados por Motivo")
        grouped = apdf.groupby('motivo_text')['protocolo'].apply(lambda seq: ', '.join(map(str, seq))).reset_index()
        for _, grp in grouped.iterrows():
            lines.append(f"- *{grp['motivo_text']}*: {grp['protocolo']}")
        lines.append("")

    # Apêndice: R checks (perito)
    r_pngs = sorted(glob(os.path.join(imgs_dir, f"rcheck_*_{safe}.png")))
    if r_pngs:
        lines.append(f"*** Apêndice estatístico (R) — {perito}\n")
        for png in r_pngs:
            base = os.path.basename(png)
            lines.append(f"#+CAPTION: {_nice_caption(base)}")
            lines.append(f"[[file:imgs/{base}]]")
            if add_comments:
                stem = os.path.splitext(base)[0]
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
                        lines.append("")
                        lines.append("#+BEGIN_QUOTE")
                        lines.append(comment_org)
                        lines.append("#+END_QUOTE")
                        break
            lines.append("\n#+LATEX: \\newpage\n")

    # ORGs auxiliares (dos scripts)
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

    # Gráficos de grupo (Python + R grupo): qualquer *_top10*.png ou *top10*.png
    pngs = sorted(glob(os.path.join(imgs_dir, "*_top10*.png")) + glob(os.path.join(imgs_dir, "*top10*.png")))
    for png in pngs:
        base = os.path.basename(png)
        lines.append(f"#+CAPTION: {_nice_caption(base)}")
        lines.append(f"[[file:imgs/{base}]]")
        stem = os.path.splitext(base)[0]
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
                lines.append("")
                lines.append("#+BEGIN_QUOTE")
                lines.append(comment_org)
                lines.append("#+END_QUOTE")
                break
        lines.append("\n#+LATEX: \\newpage\n")

    with open(org_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"✅ Org do grupo Top 10 salvo em: {org_path}")
    return org_path

# ────────────────────────────────────────────────────────────────────────────────
# Export PDF
# ────────────────────────────────────────────────────────────────────────────────
def exportar_org_para_pdf(org_path, font="DejaVu Sans"):
    import shutil as sh
    output_dir = os.path.dirname(org_path)
    org_name = os.path.basename(org_path)
    pdf_name = org_name.replace('.org', '.pdf')
    log_path = org_path + ".log"
    pandoc = sh.which("pandoc")
    if not pandoc:
        print("❌ Pandoc não encontrado no PATH. Instale com: sudo apt install pandoc texlive-xetex")
        return None
    cmd = [
        "pandoc", org_name, "-o", pdf_name,
        "--pdf-engine=xelatex",
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

    if args.r_appendix:
        _preflight_r(args.r_bin)

    PERIODO_DIR   = os.path.join(OUTPUTS_DIR, f"{args.start}_a_{args.end}")
    RELATORIO_DIR = os.path.join(PERIODO_DIR, "top10" if args.top10 else "individual")
    IMGS_DIR      = os.path.join(RELATORIO_DIR, "imgs")
    COMMENTS_DIR  = os.path.join(RELATORIO_DIR, "comments")
    ORGS_DIR      = os.path.join(RELATORIO_DIR, "orgs")
    for d in (PERIODO_DIR, RELATORIO_DIR, IMGS_DIR, COMMENTS_DIR, ORGS_DIR):
        os.makedirs(d, exist_ok=True)

    planned_cmds = []  # lista de listas (comandos)

    # ——— Top 10 ———
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
        # Python (grupo)
        for script in SCRIPT_ORDER:
            script_file = script_path(script)
            cmds = build_commands_for_script(script_file, group_ctx)
            planned_cmds.extend(cmds)

        # R (grupo) — Top 10
        if args.r_appendix:
            planned_cmds.extend(build_r_commands_for_top10(args.start, args.end, args.r_bin, args.min_analises))

        # individuais dos Top 10
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
            # Python (perito)
            for script in SCRIPT_ORDER:
                script_file = script_path(script)
                cmds = build_commands_for_script(script_file, indiv_ctx)
                planned_cmds.extend(cmds)
            # R (perito)
            if args.r_appendix:
                planned_cmds.extend(build_r_commands_for_perito(perito, args.start, args.end, args.r_bin))

        # Coorte extra (%NC alta)
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

    # ——— Individual ———
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

    # ——— Bootstrap de dependências R (se aplicável) ———
    if args.r_appendix:
        cmd_boot = _r_deps_bootstrap_cmd(args.r_bin)
        if cmd_boot:
            planned_cmds.insert(0, cmd_boot)
        else:
            print("[AVISO] r_checks/_ensure_deps.R não encontrado; pulando bootstrap de pacotes.")

    # ——— Dry-run (plan-only) ———
    if args.plan_only:
        print("\n===== PLANO DE EXECUÇÃO (dry-run) =====")
        for c in planned_cmds:
            print(" ".join(map(str, c)))
        print("=======================================\n")
        return

    # ——— Execução ———
    for cmd in planned_cmds:
        print(f"[RUN] {' '.join(map(str, cmd))}")
        try:
            if _is_r_cmd(cmd):
                subprocess.run(cmd, check=False, cwd=RCHECK_DIR)
            else:
                subprocess.run(cmd, check=False, env=_env_with_project_path(), cwd=SCRIPTS_DIR)
        except Exception as e:
            print(f"[ERRO] Falha executando: {' '.join(map(str, cmd))}\n  -> {e}")

    # Coleta outputs R caso os .R tenham escrito dentro de r_checks/
    collect_r_outputs_to_export()

    # ——— Pós: cópia de artefatos e geração de ORG ———
    org_paths = []
    extras_org_paths = []
    org_grupo_top10 = None

    if args.top10:
        # Copia qualquer *_top10*.* e *top10*.* (inclui motivos_top10_vs_brasil.* e rcheck_top10_*.png)
        copiar_artefatos_top10(IMGS_DIR, COMMENTS_DIR, ORGS_DIR)

        # Comentários GPT dos R checks do grupo (se disponível)
        if args.r_appendix and args.add_comments:
            gerar_r_apendice_group_comments_if_possible(IMGS_DIR, COMMENTS_DIR, args.start, args.end)

        # Org do grupo Top 10 (inclui Python e R grupo)
        org_grupo_top10 = gerar_org_top10_grupo(args.start, args.end, RELATORIO_DIR, IMGS_DIR, COMMENTS_DIR)

        # Peritos Top 10 — mover artefatos + org individual
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

        # Coorte extra
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

        # Consolidado final
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
        # Individual
        perito = args.perito.strip()
        copiar_artefatos_perito(perito, os.path.join(RELATORIO_DIR, "imgs"),
                                os.path.join(RELATORIO_DIR, "comments"),
                                os.path.join(RELATORIO_DIR, "orgs"))
        if args.r_appendix and args.add_comments:
            gerar_r_apendice_comments_if_possible(perito, os.path.join(RELATORIO_DIR, "imgs"),
                                                  os.path.join(RELATORIO_DIR, "comments"),
                                                  args.start, args.end)
        org_to_export = gerar_org_perito(perito, args.start, args.end, args.add_comments,
                                         os.path.join(RELATORIO_DIR, "imgs"),
                                         os.path.join(RELATORIO_DIR, "comments"),
                                         RELATORIO_DIR)

    # ——— PDF opcional ———
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
