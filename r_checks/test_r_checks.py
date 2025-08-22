#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_r_checks.py — roda scripts R (grupo/individual) e também scripts globais (Python),
gera logs por execução e um summary CSV.

Exemplos de uso ao final do arquivo.
"""
from __future__ import annotations
import argparse, csv, os, sys, subprocess, shlex, glob
from pathlib import Path
from datetime import datetime

# ────────────────────────────────────────────────────────────────────────────────
# Configuração da suíte R
# ────────────────────────────────────────────────────────────────────────────────

GROUP_SCRIPTS = [
    ("g01_top10_nc_rate_check.R",         lambda o: []),
    ("g02_top10_le15s_check.R",           lambda o: ["--threshold", str(o.le_threshold)]),
    ("g03_top10_productivity_check.R",    lambda o: ["--threshold", str(o.prod_threshold)]),
    ("g04_top10_overlap_check.R",         lambda o: []),
    ("g05_top10_motivos_chisq.R",         lambda o: []),
    ("g06_top10_composite_robustness.R",  lambda o: []),
    ("g07_top10_kpi_icra_iatd_score.R",   lambda o: []),
    # weighted props (duas execuções)
    ("08_weighted_props.R",               lambda o: ["--top10", "--measure", "nc"]),
    ("08_weighted_props.R",               lambda o: ["--top10", "--measure", "le", "--threshold", str(o.le_threshold)]),
]

INDIVIDUAL_SCRIPTS = [
    ("01_nc_rate_check.R",        lambda o: ["--perito", o.perito]),
    ("02_le15s_check.R",          lambda o: ["--perito", o.perito, "--threshold", str(o.le_threshold)]),
    ("03_productivity_check.R",   lambda o: ["--perito", o.perito, "--threshold", str(o.prod_threshold)]),
    ("04_overlap_check.R",        lambda o: ["--perito", o.perito]),
    ("05_motivos_chisq.R",        lambda o: ["--perito", o.perito]),
    ("06_composite_robustness.R", lambda o: ["--perito", o.perito]),
    ("07_kpi_icra_iatd_score.R",  lambda o: ["--perito", o.perito]),
    # weighted props (duas execuções: nc e le)
    ("08_weighted_props.R",       lambda o: ["--perito", o.perito, "--measure", "nc"]),
    ("08_weighted_props.R",       lambda o: ["--perito", o.perito, "--measure", "le", "--threshold", str(o.le_threshold)]),
]

# ────────────────────────────────────────────────────────────────────────────────
# Utilidades
# ────────────────────────────────────────────────────────────────────────────────
def which_rscript(r_bin: str | None) -> str:
    if r_bin:
        return r_bin
    from shutil import which
    path = which("Rscript") or which("RScript") or which("rscript")
    if not path:
        print("❌ Não encontrei 'Rscript' no PATH. Informe com --r-bin /caminho/para/Rscript", file=sys.stderr)
        sys.exit(2)
    return path

def which_python(py_bin: str | None) -> str:
    if py_bin:
        return py_bin
    return sys.executable or "python3"

def ensure_abs(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve())

def mkdir(p: str | Path) -> Path:
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p

def run_and_log(cmd: list[str], log_path: Path, cwd: Path | None = None) -> int:
    """Executa comando e grava stdout+stderr no log. Retorna returncode."""
    header = []
    header.append(f"[suite] CMD: {' '.join(shlex.quote(c) for c in cmd)}")
    header.append(f"[suite] CWD: {cwd or os.getcwd()}")
    header.append(f"[suite] When: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    header_txt = "\n".join(header) + "\n\n"
    log_path.write_text(header_txt, encoding="utf-8")

    with open(log_path, "a", encoding="utf-8") as fh:
        try:
            proc = subprocess.run(cmd, stdout=fh, stderr=fh, cwd=str(cwd) if cwd else None, text=True)
            rc = int(proc.returncode or 0)
        except Exception as e:
            fh.write(f"\n[suite][ERRO] Falha ao executar: {e}\n")
            rc = 99
        fh.write(f"\n[suite] Return code: {rc}\n")
    return rc

def write_summary_csv(rows: list[dict], path: Path) -> None:
    cols = ["scope", "script", "variant", "status", "return_code", "log_path", "cmd"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})

def pretty_status(rc: int) -> str:
    return "PASS" if rc == 0 else "FAIL"

# ────────────────────────────────────────────────────────────────────────────────
# Plano de execução — R
# ────────────────────────────────────────────────────────────────────────────────
def build_base_args(opts) -> list[str]:
    base = ["--db", opts.db, "--start", opts.start, "--end", opts.end, "--out-dir", opts.out_dir]
    if opts.min_analises is not None:
        base += ["--min-analises", str(opts.min_analises)]
    return base

def plan_group(opts) -> list[tuple[str, list[str], str]]:
    items = []
    base = build_base_args(opts)
    for name, extra_fn in GROUP_SCRIPTS:
        script_path = str((Path(opts.scripts_dir) / name).resolve())
        extra = extra_fn(opts)
        items.append((script_path, base + extra, name if name != "08_weighted_props.R" else f"{name} {' '.join(extra)}"))
    return items

def plan_individual(opts) -> list[tuple[str, list[str], str]]:
    if not opts.perito:
        print("⚠️ --scope individual exige --perito 'NOME COMPLETO'. Pulando testes individuais.")
        return []
    items = []
    base = build_base_args(opts)
    for name, extra_fn in INDIVIDUAL_SCRIPTS:
        script_path = str((Path(opts.scripts_dir) / name).resolve())
        extra = extra_fn(opts)
        items.append((script_path, base + extra, name if name != "08_weighted_props.R" else f"{name} {' '.join(extra)}"))
    return items

# ────────────────────────────────────────────────────────────────────────────────
# Plano de execução — Globais (Python)
# ────────────────────────────────────────────────────────────────────────────────
DEFAULT_GLOBAL_CANDIDATES = [
    "graphs_and_tables/weekday2weekend_panorama.py",
    "graphs_and_tables/weekday_to_weekend_panorama.py",
    "graphs_and_tables/g_weekday_to_weekend_table.py",
    "graphs_and_tables/global_weekday2weekend.py",
    "graphs_and_tables/global_nc_overview.py",
    "graphs_and_tables/global_panorama.py",
]
DEFAULT_GLOBAL_PATTERNS = [
    "graphs_and_tables/*weekday*weekend*.py",
    "graphs_and_tables/global_*.py",
    "graphs_and_tables/g_*weekday*weekend*.py",
]

def discover_global_scripts(opts) -> list[Path]:
    # 1) Se o usuário passou explicitamente, usa esses
    if opts.global_scripts:
        out = []
        for s in opts.global_scripts:
            p = Path(s).expanduser()
            if not p.is_absolute():
                p = Path(opts.project_root) / p
            if p.exists() and p.suffix == ".py":
                out.append(p.resolve())
            else:
                print(f"⚠️ Ignorando global inexistente: {p}")
        return out

    # 2) Candidatos comuns existentes
    found = []
    for c in DEFAULT_GLOBAL_CANDIDATES:
        p = Path(opts.project_root) / c
        if p.exists():
            found.append(p.resolve())

    # 3) Padrões
    for pat in DEFAULT_GLOBAL_PATTERNS:
        for g in glob.glob(str(Path(opts.project_root) / pat)):
            gp = Path(g)
            if gp.suffix == ".py" and gp.is_file():
                found.append(gp.resolve())

    # Dedup preservando ordem
    seen = set()
    uniq = []
    for f in found:
        if f not in seen:
            seen.add(f)
            uniq.append(f)
    return uniq

def build_globals_base_args(opts) -> list[str]:
    args = ["--start", opts.start, "--end", opts.end, "--out-dir", opts.out_dir]
    # Bandeiras de export
    if opts.globals_exports:
        for flag in opts.globals_exports:
            if flag in ("org", "png", "md", "pdf"):
                args.append(f"--export-{flag}")
    # Extras livres (string shlex-split)
    if opts.globals_extra:
        args += shlex.split(opts.globals_extra)
    return args

def plan_globals(opts) -> list[tuple[str, list[str], str]]:
    scripts = discover_global_scripts(opts)
    if not scripts:
        print("ℹ️ Nenhum script global encontrado. Use --global-scripts para definir explicitamente.")
        return []
    base = build_globals_base_args(opts)
    items = []
    for p in scripts:
        items.append((str(p), base[:], p.name))
    return items

# ────────────────────────────────────────────────────────────────────────────────
# CLI e main
# ────────────────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Testa a suíte de scripts R e globais (Python), gera logs e resumo.")
    # Período/DB
    p.add_argument("--db", required=True, help="Caminho para db/atestmed.db")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--out-dir", required=True, help="Pasta de exports (será criada se não existir)")
    p.add_argument("--min-analises", type=int, default=50, help="Mínimo de análises (onde aplicável)")
    # Direts
    p.add_argument("--project-root", default=".", help="Raiz do projeto (pai de r_checks/ e graphs_and_tables/)")
    p.add_argument("--scripts-dir", default="r_checks", help="Pasta onde estão os .R (relativo à raiz do projeto)")
    # Bins
    p.add_argument("--r-bin", default=None, help="Caminho do Rscript; se omitir, busca no PATH")
    p.add_argument("--py-bin", default=None, help="Caminho do Python; se omitir, usa o atual")
    # Escopo R
    p.add_argument("--scope", choices=["group", "individual", "all"], default="group", help="Quais testes R rodar")
    p.add_argument("--perito", default=None, help="Nome do perito (para testes individuais)")
    p.add_argument("--le-threshold", type=float, default=15.0, dest="le_threshold", help="Threshold p/ le15s")
    p.add_argument("--prod-threshold", type=float, default=50.0, dest="prod_threshold", help="Threshold p/ produtividade")
    # Globais
    p.add_argument("--include-globals", action="store_true", help="Rodar também scripts globais (Python)")
    p.add_argument("--global-scripts", nargs="*", help="Lista de caminhos (relativos ou absolutos) dos scripts globais a rodar")
    p.add_argument("--globals-exports", nargs="*", choices=["org","png","md","pdf"], default=["org","png"],
                   help="Quais flags --export-* passar aos globais (default: org png)")
    p.add_argument("--globals-extra", default="", help="Flags extras para TODOS os scripts globais (string única)")
    # Logs
    p.add_argument("--logs-dir", default=None, help="Onde salvar os logs (default: r_checks/__suite_logs/<ts>)")
    return p.parse_args()

def main():
    opts = parse_args()
    # Normaliza paths
    opts.project_root = ensure_abs(opts.project_root)
    opts.db      = ensure_abs(opts.db)
    opts.out_dir = ensure_abs(opts.out_dir)
    # scripts_dir relativo à raiz
    opts.scripts_dir = ensure_abs(Path(opts.project_root) / opts.scripts_dir)
    rscript = which_rscript(opts.r_bin)
    pybin   = which_python(opts.py_bin)

    # Diretório de logs
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_logs = Path(opts.scripts_dir) / "__suite_logs" / ts
    logs_root = Path(opts.logs_dir).expanduser().resolve() if opts.logs_dir else default_logs
    mkdir(logs_root)

    # Plano R
    r_plan = []
    if opts.scope in ("group", "all"):
        r_plan += [("group",) + it for it in plan_group(opts)]
    if opts.scope in ("individual", "all"):
        r_plan += [("individual",) + it for it in plan_individual(opts)]

    # Plano globais
    g_plan = []
    if opts.include_globals:
        g_plan = plan_globals(opts)

    if not r_plan and not g_plan:
        print("Nada para rodar (verifique --scope/--perito/--include-globals).")
        sys.exit(0)

    # Execução
    summary_rows = []
    total = len(r_plan) + len(g_plan)
    print(f"🧪 Rodando {total} execução(ões). Logs em: {logs_root}")

    # R
    for idx, (scope, script_path, argv, variant) in enumerate(r_plan, start=1):
        script_name = Path(script_path).name
        tag = f"{idx:02d}_{scope}_{Path(script_path).stem}"
        log_file = logs_root / f"{tag}.log"
        cmd = [rscript, "--vanilla", script_path] + argv
        print(f"[{idx}/{total}] {scope:<10} {script_name:<35} → {variant}")
        rc = run_and_log(cmd, log_file, cwd=Path(opts.project_root))
        summary_rows.append({
            "scope": scope,
            "script": script_name,
            "variant": variant,
            "status": pretty_status(rc),
            "return_code": rc,
            "log_path": str(log_file),
            "cmd": " ".join(shlex.quote(c) for c in cmd),
        })

    # Globais (Python)
    for jdx, (script_path, argv, variant) in enumerate(g_plan, start=len(r_plan)+1):
        script_name = Path(script_path).name
        tag = f"{jdx:02d}_global_{Path(script_path).stem}"
        log_file = logs_root / f"{tag}.log"
        cmd = [pybin, script_path] + argv
        print(f"[{jdx}/{total}] {'global':<10} {script_name:<35} → {variant}")
        rc = run_and_log(cmd, log_file, cwd=Path(opts.project_root))
        summary_rows.append({
            "scope": "global",
            "script": script_name,
            "variant": variant,
            "status": pretty_status(rc),
            "return_code": rc,
            "log_path": str(log_file),
            "cmd": " ".join(shlex.quote(c) for c in cmd),
        })

    # Resumo
    summary_csv = logs_root / "summary.csv"
    write_summary_csv(summary_rows, summary_csv)

    ok = sum(1 for r in summary_rows if r["status"] == "PASS")
    fail = len(summary_rows) - ok
    print("\n──── SUMMARY ───────────────────────────────────────────")
    for r in summary_rows:
        print(f"{r['status']:<4}  {r['scope']:<10}  {r['script']:<35}  {r['variant']}")
        print(f"      log: {r['log_path']}")
    print("────────────────────────────────────────────────────────")
    print(f"Total: {len(summary_rows)} | PASS: {ok} | FAIL: {fail}")
    print(f"CSV:   {summary_csv}")

if __name__ == "__main__":
    main()

