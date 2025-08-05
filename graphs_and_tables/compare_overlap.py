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
from utils.comentarios import comentar_overlap  # Integração GPT

# Caminho absoluto para a raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    p_ = argparse.ArgumentParser(description="Compara sobreposição de tarefas")
    p_.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    p_.add_argument('--end',   required=True, help='Data final   YYYY-MM-DD')
    p_.add_argument('--perito',required=True, help='Nome do perito a destacar')
    p_.add_argument('--chart', action='store_true', help='Exibe gráfico na tela (plotext)')
    p_.add_argument('--export-md',      action='store_true', help='Exporta tabela em Markdown')
    p_.add_argument('--export-png',     action='store_true', help='Exporta gráfico em PNG')
    p_.add_argument('--export-comment', action='store_true', help='Exporta comentário GPT')
    p_.add_argument('--add-comments',   action='store_true', help='Gera comentário automaticamente (modo PDF)')
    return p_.parse_args()

def detectar_sobreposicao(start, end):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT p.siapePerito, p.nomePerito, a.dataHoraIniPericia, a.dataHoraFimPericia
        FROM analises a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
        ORDER BY p.siapePerito, a.dataHoraIniPericia
    """, (start, end))
    rows = cur.fetchall()
    conn.close()

    flags = {}
    current_siape = None
    current_nome = None
    current_periods = []

    def check_overlap(periods):
        return any(periods[i+1][0] < periods[i][1] for i in range(len(periods)-1))

    for siape, nome, ini, fim in rows:
        if siape != current_siape:
            if current_siape is not None:
                flags[current_nome] = check_overlap(current_periods)
            current_siape = siape
            current_nome = nome
            current_periods = [(ini, fim)]
        else:
            current_periods.append((ini, fim))
    if current_siape is not None:
        flags[current_nome] = check_overlap(current_periods)

    return flags

def calcular_percentuais(flags, perito):
    if perito not in flags:
        print(f"❌ Perito '{perito}' não encontrado.")
        exit(1)
    total = len(flags)
    sobrepostos = sum(1 for v in flags.values() if v)
    pct_geral = sobrepostos / total * 100 if total else 0
    pct_perito = 100.0 if flags[perito] else 0.0
    return pct_perito, pct_geral

def exibir_plotext(perito, pct_p, pct_o):
    labels = [f"{perito} ({pct_p:.0f}%)", f"Outros ({pct_o:.0f}%)"]
    values = [pct_p, pct_o]
    p.clear_data()
    p.bar(labels, values)
    p.title("Sobreposição de Tarefas")
    p.plotsize(80, 15)
    p.show()

def exportar_png(perito, pct_p, pct_o):
    safe = perito.replace(' ', '_')
    fig, ax = plt.subplots(figsize=(10, 6), dpi=400)
    bars = ax.bar([perito, 'Demais'], [pct_p, pct_o], edgecolor='black')
    ax.set_title("Taxa de Sobreposição de Tarefas (%)", pad=15)
    ax.set_ylabel("Percentual (%)")
    ax.set_ylim(0, 100)
    ax.grid(axis='y', linestyle='--', alpha=0.6)
    for bar, pct in zip(bars, [pct_p, pct_o]):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2,
                height + 2,
                f"{pct:.1f}%",
                ha='center', va='bottom', fontsize=10)
    plt.tight_layout()
    filename = os.path.join(EXPORT_DIR, f"sobreposicao_{safe}.png")
    fig.savefig(filename)
    plt.close(fig)
    print(f"✅ PNG salvo em: {filename}")

def exportar_md(perito, pct_p, pct_o, start, end):
    md = f"""# Comparativo de Sobreposição de Tarefas

**Período:** {start} a {end}  
**Perito:** {perito}

| Categoria  | % com Sobreposição |
|------------|---------------------|
| **{perito}** | {pct_p:.1f}%       |
| Demais     | {pct_o:.1f}%       |
"""
    path = os.path.join(EXPORT_DIR, f"sobreposicao_{perito.replace(' ', '_')}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"✅ Markdown salvo em: {path}")
    return md

def exportar_comment(perito, pct_p, pct_o, start, end):
    tabela_md = f"""| Categoria  | % com Sobreposição |
|------------|---------------------|
| **{perito}** | {pct_p:.1f}%       |
| Demais     | {pct_o:.1f}%       |
"""
    p.clear_data()
    p.bar([perito, 'Demais'], [pct_p, pct_o])
    p.title("Sobreposição de Tarefas")
    p.plotsize(80, 15)
    chart_ascii = p.build()

    comentario = comentar_overlap(tabela_md, chart_ascii, start, end)
    path = os.path.join(EXPORT_DIR, f"sobreposicao_{perito.replace(' ', '_')}_comment.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(comentario)
    print(f"✅ Comentário ChatGPT salvo em: {path}")

if __name__ == '__main__':
    args = parse_args()
    flags = detectar_sobreposicao(args.start, args.end)
    pct_p, pct_o = calcular_percentuais(flags, args.perito)

    print(f"\n📊 {args.perito}: {pct_p:.1f}% vs Demais: {pct_o:.1f}%\n")

    if args.chart:
        exibir_plotext(args.perito, pct_p, pct_o)
    if args.export_png:
        exportar_png(args.perito, pct_p, pct_o)
    if args.export_md:
        exportar_md(args.perito, pct_p, pct_o, args.start, args.end)
    if args.export_comment or args.add_comments:
        exportar_comment(args.perito, pct_p, pct_o, args.start, args.end)

