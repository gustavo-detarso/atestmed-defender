#!/usr/bin/env python3
import os
import sys
import argparse
import sqlite3
import pandas as pd
import numpy as np
from tabulate import tabulate
import plotext as plt
import io
from datetime import datetime

# ---- openai opcional ----
try:
    import openai
except ImportError:
    openai = None

BASE_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

DB_PATH    = os.path.join(ROOT_DIR, "db", "atestmed.db")
EXPORT_DIR = os.path.join(ROOT_DIR, "exports")
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(description="Ranking de CRs por Score Médio")
    p.add_argument("--start","-s", required=True, help="Data inicial (YYYY-MM-DD)")
    p.add_argument("--end",  "-e", required=True, help="Data final   (YYYY-MM-DD)")
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument("--table", action="store_true", help="Mostrar somente a tabela")
    grp.add_argument("--chart", action="store_true", help="Mostrar somente o gráfico")
    p.add_argument("--export-md",  action="store_true", help="Exportar tabela para MD sem prompt")
    p.add_argument("--export-png", action="store_true", help="Exportar gráfico para PNG sem prompt")
    p.add_argument("--export-comment", action="store_true", help="Exporta comentário do ChatGPT sobre a tabela MD")
    return p.parse_args()

def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    df["start"] = pd.to_datetime(df["dataHoraIniPericia"])
    df["end"]   = pd.to_datetime(df["dataHoraFimPericia"])
    df["dur"]   = (df["end"] - df["start"]).dt.total_seconds()
    grp = df.groupby(["siapePerito","nomePerito","cr"])
    stats = grp.agg(
        total       = ("dur","count"),
        nc_count    = ("motivoNaoConformado","sum"),
        short_count = ("dur", lambda x: (x<=15).sum()),
        total_secs  = ("dur","sum")
    ).reset_index()
    stats["hours"] = stats["total_secs"] / 3600
    stats["prod"]  = stats["total"] / stats["hours"].replace(0, np.nan)

    def has_overlap(sub: pd.DataFrame) -> bool:
        times = sorted(zip(sub["start"], sub["end"]))
        return any(times[i+1][0] < times[i][1] for i in range(len(times)-1))

    overlap = grp.apply(has_overlap, include_groups=False).rename("overlap").reset_index()
    stats = stats.merge(overlap, on=["siapePerito","nomePerito","cr"])

    stats["nc_ratio"] = stats["nc_count"] / stats["total"]
    avg = stats["nc_ratio"].mean()

    stats["icra"] = 0.0
    stats.loc[stats["prod"]>=50,        "icra"] += 3.0
    stats.loc[stats["overlap"],         "icra"] += 2.5
    stats.loc[stats["short_count"]>=10, "icra"] += 2.0
    stats.loc[stats["nc_ratio"]>=2*avg,  "icra"] += 1.0

    stats["iatd"]        = 1 - stats["nc_ratio"]
    stats["score_final"] = stats["icra"] + (1 - stats["iatd"])
    return stats[["cr","score_final"]]

def rank_cr(start: str, end: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    sql = """
        SELECT p.siapePerito,p.nomePerito,p.cr,
               a.dataHoraIniPericia,a.dataHoraFimPericia,
               a.motivoNaoConformado
          FROM peritos p
          JOIN analises a ON p.siapePerito = a.siapePerito
         WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
    """
    df = pd.read_sql(sql, conn, params=(start, end))
    conn.close()
    if df.empty:
        print(f"⚠️  Nenhum dado entre {start} e {end}.")
        sys.exit(0)
    cr_scores = compute_scores(df)
    return (
        cr_scores.groupby("cr")["score_final"]
                 .mean()
                 .reset_index(name="Score Médio")
                 .sort_values("Score Médio", ascending=False)
    )

def _export_md(md: str, start: str, end: str):
    path = os.path.join(EXPORT_DIR, f"rank_cr_score_{start}_{end}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# Ranking de CRs ({start} até {end})\n\n")
        f.write(md)
    print(f"✅ Tabela exportada em {path}")

def _export_png(labels, vals, start, end):
    import matplotlib.pyplot as mplt
    fig, ax = mplt.subplots(figsize=(6,4))
    x = np.arange(len(labels))
    ax.bar(x, vals, width=0.5, color="#5A9")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_title(f"Score Médio por CR ({start} até {end})")
    ax.set_ylabel("Score Médio")
    ax.set_xlabel("CR")
    mplt.tight_layout()
    path = os.path.join(EXPORT_DIR, f"rank_cr_score_{start}_{end}.png")
    fig.savefig(path)
    mplt.close(fig)
    print(f"✅ Gráfico salvo em {path}")

def print_table(cr_rank: pd.DataFrame, args):
    md   = tabulate(cr_rank, headers="keys", tablefmt="github", floatfmt=".2f")
    print(md)
    if args.export_md:
        _export_md(md, args.start, args.end)
    else:
        ans = input("\nDeseja exportar tabela para Markdown? (Y/n) ").strip().lower()
        if ans in ("","y","s","sim"):
            _export_md(md, args.start, args.end)

def print_chart(cr_rank: pd.DataFrame, args):
    buf = io.StringIO()
    old = sys.stdout; sys.stdout = buf
    labels = cr_rank["cr"].tolist()
    vals   = cr_rank["Score Médio"].tolist()
    plt.clear_data()
    plt.bar(labels, vals, width=0.5, fill=True)
    plt.title(f"Score Médio por CR ({args.start} até {args.end})")
    plt.xlabel("CR"); plt.ylabel("Score Médio")
    plt.plotsize(min(80, len(labels)*8), 15)
    plt.show()
    sys.stdout = old

    ascii_chart = buf.getvalue()
    print(ascii_chart)

    if args.export_png:
        _export_png(labels, vals, args.start, args.end)
    else:
        ans = input("\nDeseja exportar gráfico para PNG? (Y/n) ").strip().lower()
        if ans in ("","y","s","sim"):
            _export_png(labels, vals, args.start, args.end)

def export_comment_md(start, end, md_path):
    if openai is None:
        print("openai não instalado!")
        return
    with open(md_path, "r", encoding="utf-8") as f:
        tabela = f.read()
    prompt = (
        f"Gere um comentário analítico e direto para gestão sobre a tabela de score médio por CR no período de {start} até {end}:\n\n{tabela}"
    )
    openai.api_key = os.environ.get("OPENAI_API_KEY", "SUA_API_KEY")
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    comentario = response["choices"][0]["message"]["content"].strip()
    out_path = md_path.replace(".md", "_comment.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("# Comentário automático do ChatGPT\n\n")
        f.write(comentario)
    print(f"✅ Comentário ChatGPT salvo em {out_path}")

if __name__ == "__main__":
    args = parse_args()
    md_path = os.path.join(EXPORT_DIR, f"rank_cr_score_{args.start}_{args.end}.md")
    # Gera a tabela e/ou gráfico primeiro!
    if args.table or args.chart or args.export_md or args.export_png:
        cr = rank_cr(args.start, args.end)
        if args.table:
            print_table(cr, args)
        if args.chart:
            print_chart(cr, args)
    # Só gera comentário se o arquivo MD já existir!
    if args.export_comment:
        if not os.path.isfile(md_path):
            print("Arquivo MD não encontrado! Gere a tabela (MD) antes de gerar o comentário.")
            sys.exit(1)
        export_comment_md(args.start, args.end, md_path)

