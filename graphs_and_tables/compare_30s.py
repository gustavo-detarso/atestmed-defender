#!/usr/bin/env python3
import os
import sqlite3
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Caminho absoluto para a raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Caminho absoluto para o banco de dados
DB_PATH = os.path.join(BASE_DIR, 'db', 'atestmed.db')

# Caminho absoluto para exports
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(description="Compara perícias ≤ threshold s: perito vs demais")
    p.add_argument('--start',     required=True, help='Data inicial YYYY-MM-DD')
    p.add_argument('--end',       required=True, help='Data final   YYYY-MM-DD')
    p.add_argument('--threshold', '-t', type=int, default=30, help='Limite em segundos')
    p.add_argument('--chart',      action='store_true', help='Exibe gráfico na tela (plotext)')
    p.add_argument('--export-md',      action='store_true', help='Exporta tabela em Markdown')
    p.add_argument('--export-png',     action='store_true', help='Exporta gráfico em PNG')
    p.add_argument('--export-comment', action='store_true', help='Exporta comentário para GPT')
    p.add_argument('--perito',    required=True, help='Nome do perito')
    return p.parse_args()

def comparar(start, end, threshold, perito=None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Se não recebeu perito, escolhe quem tem mais perícias ≤ threshold
    if not perito:
        cur.execute("""
            SELECT p.nomePerito, COUNT(*) AS qtd
              FROM analises a
              JOIN peritos p ON a.siapePerito = p.siapePerito
             WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
               AND (julianday(a.dataHoraFimPericia)-julianday(a.dataHoraIniPericia))*86400 <= ?
             GROUP BY p.nomePerito
             ORDER BY qtd DESC
             LIMIT 1
        """, (start, end, threshold))
        row = cur.fetchone()
        if not row:
            print("❌ Nenhum dado encontrado para o período e threshold informados.")
            conn.close()
            exit(1)
        perito = row[0]

    # Conta perícias ≤ threshold para o perito
    cur.execute("""
        SELECT COUNT(*)
          FROM analises a
          JOIN peritos p ON a.siapePerito = p.siapePerito
         WHERE p.nomePerito = ?
           AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
           AND (julianday(a.dataHoraFimPericia)-julianday(a.dataHoraIniPericia))*86400 <= ?
    """, (perito, start, end, threshold))
    cnt_p = cur.fetchone()[0] or 0

    # Conta perícias ≤ threshold para os demais
    cur.execute("""
        SELECT COUNT(*)
          FROM analises a
          JOIN peritos p ON a.siapePerito = p.siapePerito
         WHERE p.nomePerito <> ?
           AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
           AND (julianday(a.dataHoraFimPericia)-julianday(a.dataHoraIniPericia))*86400 <= ?
    """, (perito, start, end, threshold))
    cnt_o = cur.fetchone()[0] or 0

    conn.close()
    return perito, cnt_p, cnt_o

def exportar_md(perito, cnt_p, cnt_o, threshold, start, end):
    md = f"""# Comparação de Perícias ≤ {threshold}s

**Período:** {start} a {end}  
**Perito:** {perito}

| Categoria   | Qtd. Perícias |
|-------------|---------------:|
| **{perito}** | {cnt_p}        |
| Demais      | {cnt_o}        |
"""
    fname = f"compare_30s_{perito.replace(' ', '_')}.md"
    path = os.path.join(EXPORT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"✅ Markdown salvo em: {path}")

def exportar_png(perito, cnt_p, cnt_o, threshold):
    safe = perito.replace(' ', '_')
    fig, ax = plt.subplots(figsize=(8, 5), dpi=300)
    bars = ax.bar([perito, 'Demais'], [cnt_p, cnt_o],
                  color=["#1f77b4", "#ff7f0e"], edgecolor='black')
    ax.set_title(f"Perícias com duração ≤ {threshold}s", pad=15)
    ax.set_ylabel("Quantidade")
    max_val = max(cnt_p, cnt_o, 1)
    ax.set_ylim(0, max_val * 1.1)
    ax.grid(axis='y', linestyle='--', alpha=0.6)

    # Anotação dos valores
    for bar, val in zip(bars, [cnt_p, cnt_o]):
        ax.text(bar.get_x() + bar.get_width()/2,
                val + max_val*0.02,
                f"{val}",
                ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    filename = os.path.join(EXPORT_DIR, f"compare_30s_{safe}.png")
    fig.savefig(filename)
    plt.close(fig)
    print(f"✅ PNG salvo em: {filename}")

def exportar_comment(perito, cnt_p, cnt_o, threshold, start, end):
    comentario = (
        f"O perito **{perito}** realizou {cnt_p} perícias com duração ≤ {threshold}s "
        f"no período {start}–{end}, enquanto os demais realizaram {cnt_o}."
    )
    fname = f"compare_30s_{perito.replace(' ', '_')}_comment.md"
    path = os.path.join(EXPORT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(comentario)
    print(f"✅ Comentário salvo em: {path}")

def exibir_chart(perito, cnt_p, cnt_o, threshold):
    import plotext as p
    labels = [perito, 'Demais']
    values = [cnt_p, cnt_o]
    p.clear_data()
    p.bar(labels, values)
    p.title(f"Perícias ≤ {threshold}s")
    p.plotsize(80, 15)
    p.show()

if __name__ == '__main__':
    args = parse_args()
    perito, cnt_p, cnt_o = comparar(args.start, args.end, args.threshold, args.perito)

    print(f"\n📊 {perito}: {cnt_p} vs Demais: {cnt_o}\n")

    if args.export_md:
        exportar_md(perito, cnt_p, cnt_o, args.threshold, args.start, args.end)
    if args.export_png:
        exportar_png(perito, cnt_p, cnt_o, args.threshold)
    if args.export_comment:
        exportar_comment(perito, cnt_p, cnt_o, args.threshold, args.start, args.end)
    if args.chart:
        exibir_chart(perito, cnt_p, cnt_o, args.threshold)

