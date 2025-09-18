#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera (a) figura dos cenários e (b) fragmento .org com a tabela-resumo.
Entrada padrão: CSV com colunas: Mes,IV_total,IV_sel (padrão: data/iv_data.csv).
Saídas:
  - graphs/impacto_fila_cenarios_jan_ago_2025.png
  - tables/resumo_cenarios.org  (tabela em sintaxe Org)
  - tables/resumo_cenarios.csv  (opcional, para referência)
"""

import os, csv, argparse
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DEFAULT_IN = "data/iv_data.csv"
OUT_FIG = "graphs/impacto_fila_cenarios_jan_ago_2025.png"
OUT_ORG = "tables/resumo_cenarios.org"
OUT_CSV = "tables/resumo_cenarios.csv"

def pct(n, d):
    return round((n*100.0)/d, 2) if d else 0.0

def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = []
        for row in r:
            rows.append({
                "Mes": row["Mes"],
                "IV_total": float(row["IV_total"]),
                "IV_sel": float(row["IV_sel"]),
            })
        return rows

def ensure_dirs():
    os.makedirs(os.path.dirname(OUT_FIG), exist_ok=True)
    os.makedirs(os.path.dirname(OUT_ORG), exist_ok=True)

def build_series(rows):
    mes   = [r["Mes"] for r in rows]
    ivtot = [r["IV_total"] for r in rows]
    ivsel = [r["IV_sel"] for r in rows]
    cen_a = [t - 0.5*s for t,s in zip(ivtot, ivsel)]
    cen_b = [t - 0.7*s for t,s in zip(ivtot, ivsel)]
    cen_c = [t - 1.0*s for t,s in zip(ivtot, ivsel)]
    return mes, ivtot, ivsel, cen_a, cen_b, cen_c

def save_figure(mes, ivtot, cen_a, cen_b, cen_c, out_path):
    plt.figure(figsize=(10,6))
    plt.plot(mes, ivtot, marker="o", linewidth=2, label="Real (IV_total)")
    plt.plot(mes, cen_a, marker="o", linestyle="--", label="Cenário A (–50% IV_sel)")
    plt.plot(mes, cen_b, marker="o", linestyle="--", label="Cenário B (–70% IV_sel)")
    plt.plot(mes, cen_c, marker="o", linestyle="--", label="Cenário C (–100% IV_sel)")
    plt.title("Impacto na Fila (jan–ago/2025): Real vs. Cenários de Intervenção (Top-10)")
    plt.xlabel("Mês/2025"); plt.ylabel("Vagas presenciais (proxy de pressão)")
    plt.legend(); plt.grid(True); plt.tight_layout()
    plt.savefig(out_path, dpi=160); plt.close()

def write_org_table(rows, path_org, path_csv=None):
    # monta tabela-resumo por mês + totais
    header = ["Mes","IV_total","IV_sel",
              "Red_A","Red_A_%","Red_B","Red_B_%","Red_C","Red_C_%"]
    body = []
    tot_ivtot = 0.0; tot_red_a=tot_red_b=tot_red_c=0.0
    for r in rows:
        t = r["IV_total"]; s = r["IV_sel"]
        red_a = 0.5*s; red_b = 0.7*s; red_c = 1.0*s
        body.append([r["Mes"], int(round(t)), int(round(s)),
                     int(round(red_a)), pct(red_a, t),
                     int(round(red_b)), pct(red_b, t),
                     int(round(red_c)), pct(red_c, t)])
        tot_ivtot += t
        tot_red_a += red_a; tot_red_b += red_b; tot_red_c += red_c

    total_row = ["*Total*", int(round(tot_ivtot)), int(round(sum(r['IV_sel'] for r in rows))),
                 int(round(tot_red_a)), pct(tot_red_a, tot_ivtot),
                 int(round(tot_red_b)), pct(tot_red_b, tot_ivtot),
                 int(round(tot_red_c)), pct(tot_red_c, tot_ivtot)]

    # grava CSV (opcional)
    if path_csv:
        with open(path_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(header); w.writerows(body); w.writerow(total_row)

    # grava fragmento .org
    lines = []
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|-" + "-+-".join("-"*len(h) for h in header) + "-|")
    for row in body:
        lines.append("| " + " | ".join(str(x) for x in row) + " |")
    lines.append("| " + " | ".join(str(x) for x in total_row) + " |")

    with open(path_org, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", "-i", default=DEFAULT_IN,
                    help="CSV com colunas Mes,IV_total,IV_sel")
    ap.add_argument("--fig", default=OUT_FIG)
    ap.add_argument("--org", default=OUT_ORG)
    ap.add_argument("--csv", default=OUT_CSV)
    args = ap.parse_args()

    ensure_dirs()
    data = load_csv(args.input)
    mes, ivtot, ivsel, cen_a, cen_b, cen_c = build_series(data)
    save_figure(mes, ivtot, cen_a, cen_b, cen_c, args.fig)
    write_org_table(data, args.org, args.csv)

if __name__ == "__main__":
    main()

