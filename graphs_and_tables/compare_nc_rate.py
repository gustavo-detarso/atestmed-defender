#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sqlite3
import argparse
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import plotext as p
from utils.comentarios import comentar_nc100  # ajuste aqui se for comentar_compare_nc_rate

# Caminhos absolutos
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    p_ = argparse.ArgumentParser(
        description="Compara taxa de não conformação do perito com demais"
    )
    p_.add_argument('--start',      required=True, help='Data inicial YYYY-MM-DD')
    p_.add_argument('--end',        required=True, help='Data final   YYYY-MM-DD')
    grp = p_.add_mutually_exclusive_group(required=True)
    grp.add_argument('--perito',   help='Nome exato do perito')
    grp.add_argument('--nome',     help='Nome exato do perito')
    p_.add_argument('--export-md',      action='store_true', help='Exporta tabela em Markdown')
    p_.add_argument('--export-png',     action='store_true', help='Exporta gráfico em PNG')
    p_.add_argument('--export-comment', action='store_true', help='Exporta comentário GPT')
    p_.add_argument('--add-comments',   action='store_true', help='Gera comentário automaticamente (modo PDF)')
    return p_.parse_args()

def calcular_taxas(start, end, perito):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        """
        SELECT 
            p.nomePerito,
            SUM(CASE WHEN a.motivoNaoConformado != 0 THEN 1 ELSE 0 END) AS nc_count,
            COUNT(*) AS total
        FROM analises a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
        GROUP BY p.nomePerito
        """,
        conn, params=(start, end)
    )
    conn.close()

    df['rate'] = 100 * df['nc_count'] / df['total']
    pct_p = df.loc[df['nomePerito'] == perito, 'rate'].iloc[0]
    pct_o = df.loc[df['nomePerito'] != perito, 'rate'].mean()
    return pct_p, pct_o

def exportar_md(perito, pct_p, pct_o):
    safe = perito.replace(' ', '_')
    path = os.path.join(EXPORT_DIR, f"compare_nc_rate_{safe}.md")
    with open(path, 'w', encoding='utf-8') as f:
        f.write(f"| Categoria | Taxa de NC (%) |\n")
        f.write(f"|-----------|----------------|\n")
        f.write(f"| **{perito}** | {pct_p:.1f} |\n")
        f.write(f"| Demais      | {pct_o:.1f} |\n")
    print("✅ Markdown salvo em", path)
    return (
        "| Categoria | Taxa de NC (%) |\n"
        "|-----------|----------------|\n"
        f"| **{perito}** | {pct_p:.1f} |\n"
        f"| Demais      | {pct_o:.1f} |\n"
    )

def exportar_png(perito, pct_p, pct_o):
    safe = perito.replace(' ', '_')
    fig, ax = plt.subplots(figsize=(10, 6), dpi=400)
    ax.bar([perito, 'Demais'], [pct_p, pct_o])
    ax.set_title(f"Taxa de Não Conformação: {perito} vs Demais", pad=15)
    ax.set_ylabel("NC (%)")
    ax.grid(axis='y', linestyle='--', alpha=0.6)
    for bar, pct in zip(ax.patches, [pct_p, pct_o]):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2,
                height + 2,
                f"{pct:.1f}%",
                ha='center', va='bottom', fontsize=10)
    plt.tight_layout()
    out = os.path.join(EXPORT_DIR, f"compare_nc_rate_{safe}.png")
    fig.savefig(out)
    plt.close(fig)
    print("✅ PNG salvo em", out)

def exportar_comment(perito, pct_p, pct_o, start, end):
    safe = perito.replace(' ', '_')
    # Gera tabela markdown para prompt
    tabela_md = (
        "| Categoria | Taxa de NC (%) |\n"
        "|-----------|----------------|\n"
        f"| **{perito}** | {pct_p:.1f} |\n"
        f"| Demais      | {pct_o:.1f} |\n"
    )
    # Gráfico ASCII para prompt
    p.clear_data()
    p.bar([perito, 'Demais'], [pct_p, pct_o])
    p.title(f"Taxa de Não Conformação")
    p.plotsize(80, 15)
    chart_ascii = p.build()

    # Comentário GPT
    comentario = comentar_nc100(tabela_md, start, end)  # ajuste para comentar_compare_nc_rate se necessário

    path = os.path.join(EXPORT_DIR, f"compare_nc_rate_{safe}_comment.md")
    with open(path, 'w', encoding='utf-8') as f:
        f.write(comentario)
    print("🗒️ Comentário ChatGPT salvo em", path)

def main():
    args = parse_args()
    perito = args.perito or args.nome

    pct_p, pct_o = calcular_taxas(args.start, args.end, perito)

    if args.export_md:
        exportar_md(perito, pct_p, pct_o)
    if args.export_png:
        exportar_png(perito, pct_p, pct_o)
    if args.export_comment or args.add_comments:
        exportar_comment(perito, pct_p, pct_o, args.start, args.end)

    print(f"\n📊 {perito}: {pct_p:.1f}% vs Demais: {pct_o:.1f}%\n")

if __name__ == '__main__':
    main()

