#!/usr/bin/env python3
import os
import sqlite3
import argparse
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(os.path.dirname(__file__), '..', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(description="Compara produtividade ≥50/h: perito vs demais")
    p.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    p.add_argument('--end', required=True, help='Data final   YYYY-MM-DD')
    p.add_argument('--perito', required=True, help='Nome do perito a destacar')
    p.add_argument('--threshold', '-t', type=int, default=50, help='Limite de análises/hora')
    p.add_argument('--chart', action='store_true', help='Exibe gráfico na tela (plotext)')
    p.add_argument('--export-md', action='store_true', help='Exporta tabela em Markdown')
    p.add_argument('--export-png', action='store_true', help='Exporta gráfico em PNG')
    p.add_argument('--export-comment', action='store_true', help='Exporta comentário base')
    return p.parse_args()

def calcular_produtividade(start, end):
    dt_start = datetime.fromisoformat(start)
    dt_end   = datetime.fromisoformat(end)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT p.nomePerito,
               COUNT(*) AS total,
               SUM((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400) AS segs
        FROM peritos p
         JOIN analises a ON p.siapePerito = a.siapePerito
        WHERE a.dataHoraIniPericia BETWEEN ? AND ?
        GROUP BY p.nomePerito
    """, (dt_start, dt_end))
    rows = cur.fetchall()
    conn.close()

    prod = {nome: (total / (segs / 3600) if segs and segs > 0 else 0) for nome, total, segs in rows}
    return prod

def comparar(produtividade, perito, threshold):
    if perito not in produtividade:
        print(f"❌ Perito '{perito}' não encontrado.")
        exit(1)

    perito_val = produtividade[perito]
    perito_flag = 100 if perito_val >= threshold else 0

    outros = [v for k, v in produtividade.items() if k != perito]
    pct_outros = sum(1 for v in outros if v >= threshold) / len(outros) * 100 if outros else 0

    return perito_val, perito_flag, pct_outros

def exportar_md(perito, valor, flag, pct_outros, threshold, start, end):
    md = f"""# Comparativo de Produtividade ≥ {threshold}/h

**Período:** {start} a {end}  
**Perito:** {perito}

| Categoria | % ≥ {threshold}/h |
|-----------|-------------------|
| {perito} | {flag:.1f}% |
| Demais    | {pct_outros:.1f}% |

**Produtividade do perito:** {valor:.2f} análises/hora
"""
    path = os.path.join(EXPORT_DIR, f"produtividade_{perito.replace(' ', '_')}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"✅ Markdown salvo em: {path}")

def exportar_comment(perito, threshold, start, end):
    texto = f"Comparativo de produtividade do perito {perito}, com base no limite ≥ {threshold} análises/hora entre {start} e {end}."
    path = os.path.join(EXPORT_DIR, f"produtividade_{perito.replace(' ', '_')}_comment.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(texto)
    print(f"✅ Comentário salvo em: {path}")

def exportar_png(perito, flag, pct_outros, threshold):
    labels = [perito, "Outros"]
    values = [flag, pct_outros]
    plt.figure(figsize=(6, 4))
    plt.bar(labels, values, color=["#2ca02c", "#ff7f0e"])
    plt.title(f"Produtividade ≥{threshold}/h")
    plt.ylabel("% Peritos com produtividade ≥ limiar")
    plt.ylim(0, 100)
    plt.tight_layout()
    path = os.path.join(EXPORT_DIR, f"produtividade_{perito.replace(' ', '_')}.png")
    plt.savefig(path)
    print(f"✅ PNG salvo em: {path}")

def exibir_plotext(perito, flag, pct_outros, threshold):
    import plotext as p
    labels = [f"{perito} ({flag:.0f}%)", f"Outros ({pct_outros:.0f}%)"]
    values = [flag, pct_outros]
    p.clear_data()
    p.bar(labels, values)
    p.title(f"Produtividade ≥{threshold}/h")
    p.plotsize(80, 15)
    p.show()

if __name__ == '__main__':
    args = parse_args()
    produtividade = calcular_produtividade(args.start, args.end)
    valor, flag, pct_outros = comparar(produtividade, args.perito, args.threshold)

    print(f"\n📊 {args.perito}: {valor:.2f} análises/hora ({flag:.0f}%) vs Demais: {pct_outros:.1f}%\n")

    if args.chart:
        exibir_plotext(args.perito, flag, pct_outros, args.threshold)
    if args.export_png:
        exportar_png(args.perito, flag, pct_outros, args.threshold)
    if args.export_md:
        exportar_md(args.perito, valor, flag, pct_outros, args.threshold, args.start, args.end)
    if args.export_comment:
        exportar_comment(args.perito, args.threshold, args.start, args.end)

