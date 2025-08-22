#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Vasculha relatórios consolidados:
  relatorio_dez_piores_YYYY-MM-DD_a_YYYY-MM-DD.org
e produz um CSV com:
  nome, cr, dr, meses
• 'meses' é a lista (ordenada) de meses YYYY-MM em que o perito apareceu.
• Inclui apenas quem tem >= N meses (default: 2).
• Ignora apêndices / panoramas / grupos estatísticos.
"""

import os
import re
import csv
import argparse
from datetime import datetime
from collections import defaultdict, Counter

# ----------------------------- CLI -----------------------------
def build_parser():
    p = argparse.ArgumentParser(
        description="Extrai nomes, CR, DR e meses (YYYY-MM) de peritos que apareceram no Top10 em >= N meses."
    )
    p.add_argument("--root", required=True, help="Pasta raiz onde estão os .org consolidados.")
    p.add_argument("--out",  required=True, help="Caminho do CSV de saída.")
    p.add_argument("--min-months", type=int, default=2, help="Mínimo de meses distintos (default: 2).")
    p.add_argument("--verbose", action="store_true", help="Logs de progresso.")
    return p

# ----------- Detecção de arquivo e mês do período ---------------
ORG_FILE_RE = re.compile(r"relatorio_dez_piores_(\d{4}-\d{2}-\d{2})_a_(\d{4}-\d{2}-\d{2})\.org$")

def period_month_from_filename(path):
    m = ORG_FILE_RE.search(os.path.basename(path))
    if not m:
        return None
    start_s, end_s = m.group(1), m.group(2)
    try:
        dstart = datetime.strptime(start_s, "%Y-%m-%d")
        _ = datetime.strptime(end_s, "%Y-%m-%d")
    except ValueError:
        return None
    return dstart.strftime("%Y-%m")

# ---------------- Extração dentro do .org -----------------------
HDR_RE = re.compile(r"^\*\*\s+(.+?)\s*$")  # cabeçalhos nível 2: "** Nome"
EXCLUDE_HDR_PATTERNS = (
    "Panorama global",
    "Peritos com %NC",
    "Protocolos envolvidos",
)

CR_RE = re.compile(r"\bCR\s*:\s*([A-Za-z0-9_-]+)")
DR_RE = re.compile(r"\bDR\s*:\s*([A-Za-z0-9_-]+)")

def is_excluded_heading(title: str) -> bool:
    t = title.strip()
    return any(key in t for key in EXCLUDE_HDR_PATTERNS)

def extract_blocks_with_meta(path, verbose=False):
    """
    Retorna lista de tuplas (nome, cr, dr) de seções de perito.
    Heurística: seção válida se houver "- Tarefas:" nas ~12 linhas seguintes.
    CR/DR são procurados no bloco até o próximo "** ".
    """
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            lines = f.read().splitlines()
    except Exception as e:
        if verbose:
            print(f"[AVISO] Falha lendo {path}: {e}")
        return out

    n = len(lines)
    i = 0
    while i < n:
        m = HDR_RE.match(lines[i])
        if not m:
            i += 1
            continue
        title = m.group(1).strip()
        if is_excluded_heading(title):
            i += 1
            continue

        # Limites do bloco
        start_idx = i + 1
        end_idx = start_idx
        while end_idx < n and not lines[end_idx].startswith("** "):
            end_idx += 1

        # Verifica "- Tarefas:" nas primeiras linhas do bloco
        has_tarefas = False
        for j in range(start_idx, min(start_idx + 12, end_idx)):
            lj = lines[j].lstrip()
            if lj.startswith("- Tarefas:"):
                has_tarefas = True
                break

        if not has_tarefas:
            i = end_idx
            continue

        # Procura CR/DR dentro do bloco
        cr_val = ""
        dr_val = ""
        for j in range(start_idx, end_idx):
            s = lines[j]
            if not cr_val:
                mcr = CR_RE.search(s)
                if mcr:
                    cr_val = mcr.group(1).strip()
            if not dr_val:
                mdr = DR_RE.search(s)
                if mdr:
                    dr_val = mdr.group(1).strip()
            if cr_val and dr_val:
                break

        out.append((title, cr_val, dr_val))
        i = end_idx

    if verbose:
        print(f"[DEBUG] {os.path.basename(path)} -> {len(out)} seções de perito.")
    return out

# ------------------------------ Main ----------------------------
def main():
    args = build_parser().parse_args()

    # Localiza arquivos .org candidatos
    org_files = []
    for root, _, files in os.walk(args.root):
        for f in files:
            if ORG_FILE_RE.match(f):
                org_files.append(os.path.join(root, f))
    if args.verbose:
        print(f"[INFO] Encontrados {len(org_files)} arquivos candidatos.")

    # Mapas acumuladores
    perito_months = defaultdict(set)            # nome -> {YYYY-MM}
    perito_crdr_counts = defaultdict(Counter)   # nome -> Counter{(cr,dr): freq}

    for org in sorted(org_files):
        month = period_month_from_filename(org)
        if not month:
            if args.verbose:
                print(f"[SKIP] Ignorando (nome fora do padrão): {org}")
            continue

        blocks = extract_blocks_with_meta(org, verbose=args.verbose)
        for nome, cr, dr in blocks:
            perito_months[nome].add(month)
            key = (cr or "", dr or "")
            perito_crdr_counts[nome][key] += 1

    # Filtra quem tem >= min_months e escolhe o par (CR,DR) mais frequente
    rows = []
    for nome, months in perito_months.items():
        if len(months) >= args.min_months:
            # mais frequente; desempate por ordem lexicográfica para estabilidade
            freq_counter = perito_crdr_counts[nome]
            if freq_counter:
                best_pair = max(freq_counter.items(), key=lambda kv: (kv[1], kv[0]))[0]
                cr, dr = best_pair
            else:
                cr, dr = "", ""
            ordered_months = ", ".join(sorted(months))  # YYYY-MM ordena por string
            rows.append((nome, cr, dr, ordered_months))

    # Ordena por nome (case-insensitive)
    rows.sort(key=lambda x: x[0].lower())

    # Grava CSV
    out_dir = os.path.dirname(os.path.abspath(args.out))
    if out_dir and not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["nome", "cr", "dr", "meses"])
        for nome, cr, dr, meses in rows:
            w.writerow([nome, cr, dr, meses])

    if args.verbose:
        print(f"[INFO] Salvo: {args.out} ({len(rows)} linhas).")

if __name__ == "__main__":
    main()

