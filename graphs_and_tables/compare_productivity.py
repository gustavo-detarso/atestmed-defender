#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sqlite3
import argparse
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import plotext as p
from utils.comentarios import comentar_produtividade  # Integração GPT

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    p_ = argparse.ArgumentParser(description="Compara produtividade ≥threshold/h: perito vs demais")
    p_.add_argument('--start',     required=True, help='Data inicial YYYY-MM-DD')
    p_.add_argument('--end',       required=True, help='Data final   YYYY-MM-DD')
    p_.add_argument('--perito',    required=True, help='Nome do perito a destacar')
    p_.add_argument('--threshold', '-t', type=int, default=50, help='Limite de análises/hora')
    p_.add_argument('--chart',      action='store_true', help='Exibe gráfico na tela (plotext)')
    p_.add_argument('--export-md',      action='store_true', help='Exporta tabela em Markdown')
    p_.add_argument('--export-png',     action='store_true', help='Exporta gráfico em PNG')
    p_.add_argument('--export-comment', action='store_true', help='Exporta comentário GPT')
    p_.add_argument('--add-comments',   action='store_true', help='Gera comentário automaticamente (modo PDF)')
    return p_.parse_args()

def calcular_produtividade(start, end):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT p.nomePerito,
               COUNT(*) AS total,
               SUM((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400) AS segs
        FROM peritos p
        JOIN analises a ON p.siapePerito = a.siapePerito
        WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
        GROUP BY p.nomePerito
    """, (start, end))
    rows = cur.fetchall()
    conn.close()

    return {nome: (total / (segs / 3600) if segs and segs > 0 else 0)
            for nome, total, segs in rows}

def comparar(produtividade, perito, threshold):
    if perito not in produtividade:
        print(f"❌ Perito '{perito}' não encontrado.")
        exit(1)
    val = produtividade[perito]
    flag = 100.0 if val >= threshold else 0.0

    outros_vals = [v for k, v in produtividade.items() if k != perito]
    pct_outros = (sum(1 for v in outros_vals if v >= threshold) / len(outros_vals) * 100) if outros_vals else 0.0

    return val, flag, pct_outros

def exportar_md(perito, valor, flag, pct_outros, threshold, start, end):
    md = f"""# Comparativo de Produtividade ≥ {threshold}/h

**Período:** {start} a {end}  
**Perito:** {perito}

| Categoria   | % ≥ {threshold}/h |
|-------------|-------------------:|
| **{perito}** | {flag:.1f}%        |
| Demais      | {pct_outros:.1f}%  |

**Produtividade do perito:** {valor:.2f} análises/hora
"""
    fname = f"produtividade_{perito.replace(' ', '_')}.md"
    path = os.path.join(EXPORT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"✅ Markdown salvo em: {path}")
    return md

def exportar_png(perito, flag, pct_outros, threshold):
    safe = perito.replace(' ', '_')
    fig, ax = plt.subplots(figsize=(8, 5), dpi=300)
    bars = ax.bar([perito, 'Demais'], [flag, pct_outros],
                  color=["#2ca02c", "#ff7f0e"], edgecolor='black')
    ax.set_title(f"Produtividade ≥ {threshold} análises/h", pad=15)
    ax.set_ylabel("Percentual de peritos (%)")
    ax.set_ylim(0, 100)
    ax.grid(axis='y', linestyle='--', alpha=0.6)

    for bar, pct in zip(bars, [flag, pct_outros]):
        ax.text(bar.get_x() + bar.get_width()/2,
                h := bar.get_height(),
                f"{pct:.1f}%",
                ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    filename = os.path.join(EXPORT_DIR, f"produtividade_{safe}.png")
    fig.savefig(filename)
    plt.close(fig)
    print(f"✅ PNG salvo em: {filename}")

def exportar_comment(perito, valor, flag, pct_outros, threshold, start, end):
    tabela_md = f"""| Categoria   | % ≥ {threshold}/h |
|-------------|-------------------:|
| **{perito}** | {flag:.1f}%        |
| Demais      | {pct_outros:.1f}%  |

**Produtividade do perito:** {valor:.2f} análises/hora
"""
    p.clear_data()
    p.bar([perito, 'Demais'], [flag, pct_outros])
    p.title(f"Produtividade ≥ {threshold}/h")
    p.plotsize(80, 15)
    chart_ascii = p.build()

    comentario = comentar_produtividade(tabela_md, chart_ascii, start, end, threshold)

    fname = f"produtividade_{perito.replace(' ', '_')}_comment.md"
    path = os.path.join(EXPORT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(comentario)
    print(f"✅ Comentário ChatGPT salvo em: {path}")

def exibir_plotext(perito, flag, pct_outros, threshold):
    labels = [f"{perito} ({flag:.0f}%)", f"Demais ({pct_outros:.0f}%)"]
    values = [flag, pct_outros]
    p.clear_data()
    p.bar(labels, values)
    p.title(f"Produtividade ≥ {threshold}/h")
    p.plotsize(80, 15)
    p.show()

if __name__ == '__main__':
    args = parse_args()
    produtividade = calcular_produtividade(args.start, args.end)
    valor, flag, pct_outros = comparar(produtividade, args.perito, args.threshold)

    print(f"\n📊 {args.perito}: {valor:.2f} análises/h ({flag:.0f}%) vs Demais: {pct_outros:.1f}%\n")

    if args.export_md:
        exportar_md(args.perito, valor, flag, pct_outros, args.threshold, args.start, args.end)
    if args.export_png:
        exportar_png(args.perito, flag, pct_outros, args.threshold)
    if args.export_comment or args.add_comments:
        exportar_comment(args.perito, valor, flag, pct_outros, args.threshold, args.start, args.end)
    if args.chart:
        exibir_plotext(args.perito, flag, pct_outros, args.threshold)

