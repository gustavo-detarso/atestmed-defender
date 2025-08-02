#!/usr/bin/env python3
import os
import sqlite3
import argparse
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# Caminhos absolutos
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(
        description="Compara taxa de não conformação do perito com demais"
    )
    p.add_argument('--start',      required=True, help='Data inicial YYYY-MM-DD')
    p.add_argument('--end',        required=True, help='Data final   YYYY-MM-DD')
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument('--perito',   help='Nome exato do perito')
    grp.add_argument('--nome',     help='Nome exato do perito')
    p.add_argument('--export-md',      action='store_true', help='Exporta tabela em Markdown')
    p.add_argument('--export-png',     action='store_true', help='Exporta gráfico em PNG')
    p.add_argument('--export-comment', action='store_true', help='Exporta comentário base')
    return p.parse_args()

def calcular_taxas(start, end, perito):
    dt_start = datetime.fromisoformat(start)
    dt_end   = datetime.fromisoformat(end)

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

def exportar_png(perito, pct_p, pct_o):
    safe = perito.replace(' ', '_')
    fig, ax = plt.subplots(figsize=(8, 5), dpi=300)
    ax.bar([perito, 'Demais'], [pct_p, pct_o])
    ax.set_title(f"Taxa de Não Conformação: {perito} vs Demais", pad=15)
    ax.set_ylabel("NC (%)")
    ax.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()
    out = os.path.join(EXPORT_DIR, f"compare_nc_rate_{safe}.png")
    fig.savefig(out)
    plt.close(fig)
    print("✅ PNG salvo em", out)

def exportar_comment(perito, pct_p, pct_o, start, end):
    safe = perito.replace(' ', '_')
    path = os.path.join(EXPORT_DIR, f"compare_nc_rate_{safe}_comment.md")
    texto = (
        f"O perito **{perito}** apresentou taxa de não conformação de "
        f"{pct_p:.1f}% no período {start} a {end}, "
        f"enquanto a média dos demais foi de {pct_o:.1f}%."
    )
    with open(path, 'w', encoding='utf-8') as f:
        f.write(texto)
    print("🗒️ Comentário salvo em", path)

def main():
    args = parse_args()
    perito = args.perito or args.nome

    pct_p, pct_o = calcular_taxas(args.start, args.end, perito)

    if args.export_md:
        exportar_md(perito, pct_p, pct_o)
    if args.export_png:
        exportar_png(perito, pct_p, pct_o)
    if args.export_comment:
        exportar_comment(perito, pct_p, pct_o, args.start, args.end)

    # saída simples para CLI
    print(f"\n📊 {perito}: {pct_p:.1f}% vs Demais: {pct_o:.1f}%\n")

if __name__ == '__main__':
    main()

