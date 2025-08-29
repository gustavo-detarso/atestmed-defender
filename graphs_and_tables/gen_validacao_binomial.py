#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import sys
from pathlib import Path

# garante que o PROJ_ROOT esteja no sys.path
PROJ_ROOT = Path(__file__).resolve().parents[1]
if str(PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJ_ROOT))

from utils.utils_tex import tex_escape, fmt_ic, make_longtable, write_text

BIN_ALIGN_R = r"@{} L{5.1cm} S[table-format=4.0] S[table-format=4.0] S[table-format=1.2] L{3.2cm} S[table-format=3.0] r r @{}"
BIN_ALIGN_S = r"@{} L{5.1cm} S[table-format=4.0] S[table-format=4.0] S[table-format=1.2] L{3.2cm} S[table-format=3.0] S[table-format=1.2e2] S[table-format=1.2e2] @{}"

def parse_args():
    ap = argparse.ArgumentParser(description="Gera validacao_binomial.tex a partir de CSV.")
    ap.add_argument("--csv", required=True, help="Caminho do binomial.csv")
    ap.add_argument("--out-tex", required=True, help="Arquivo TEX de saída (validacao_binomial.tex)")
    ap.add_argument("--pbr", type=float, default=None, help="Valor de p_BR (0..1). Se omitido, não mostra no título.")
    ap.add_argument("--pq-align", choices=["r", "s"], default="r",
                    help="Alinhamento das colunas p e q: 'r' (robusto) ou 's' (siunitx S[1.2e2]). Padrão: r.")
    return ap.parse_args()

def main():
    args = parse_args()
    rows = []
    with open(args.csv, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for r in rd:
            perito = tex_escape(r["Perito"])
            N = int(float(r["N"]))
            NC = int(float(r["NC"]))
            p_hat = float(r["p_hat"])
            ic_low = float(r["IC95_low"])
            ic_high = float(r["IC95_high"])
            excesso = int(float(r["Excesso"]))
            pval = float(r["p"])
            qbh = float(r["q"])
            rows.append([
                perito,
                f"{N:d}",
                f"{NC:d}",
                f"{p_hat:.2f}",
                fmt_ic(ic_low, ic_high),
                f"{excesso:d}",
                f"{pval:.2e}",
                f"{qbh:.2e}",
            ])

    align = BIN_ALIGN_R if args.pq_align == "r" else BIN_ALIGN_S

    header = [
        "Perito", "{N}", "{NC}", r"$\hat{p}_i$", "IC95\\% (Wilson)", "{Excesso}", r"$p$", r"$q(\text{BH})$"
    ]
    table = make_longtable(align, header, rows)

    title = r"\noindent \textbf{Teste binomial (unilateral, $p_i > p_{BR}$) com FDR (BH)}"
    if args.pbr is not None:
        title += f" \\hfill $p_{{BR}}={args.pbr*100:.2f}\\%$"
    title += "\n\n"
    out = title + table
    write_text(args.out_tex, out)
    print(f"✅ Gerado: {args.out_tex}")

if __name__ == "__main__":
    main()

