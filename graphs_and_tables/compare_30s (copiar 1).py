#!/usr/bin/env python3
import os
import sqlite3
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import sys
print("DEBUG sys.argv:", sys.argv)

# Caminho absoluto para a raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Caminho absoluto para o banco de dados
DB_PATH = os.path.join(BASE_DIR, 'db', 'atestmed.db')

# Caminho absoluto para exports
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(description="Compara perícias ≤ threshold s: perito vs demais")
    p.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    p.add_argument('--end', required=True, help='Data final YYYY-MM-DD')
    p.add_argument('--threshold', '-t', type=int, default=30, help='Limite em segundos')
    p.add_argument('--chart', action='store_true', help='Exibe gráfico na tela')
    p.add_argument('--export-md', action='store_true', help='Exporta tabela em Markdown')
    p.add_argument('--export-png', action='store_true', help='Exporta gráfico em PNG')
    p.add_argument('--export-comment', action='store_true', help='Exporta comentário vazio para GPT')
    p.add_argument('--perito', required=True, help='Nome do perito (se não fornecido, usa primeiro do ranking)')
    return p.parse_args()

def comparar(start, end, threshold, perito=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if not perito:
        # Pega o perito com mais análises curtas no período
        cursor.execute("""
            SELECT p.nomePerito, COUNT(*) as qtd
              FROM analises a
              JOIN peritos p ON a.siapePerito = p.siapePerito
             WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
               AND (julianday(a.dataHoraFimPericia)-julianday(a.dataHoraIniPericia))*86400 <= ?
             GROUP BY p.nomePerito
             ORDER BY qtd DESC LIMIT 1
        """, (start, end, threshold))
        row = cursor.fetchone()
        if not row:
            print("❌ Nenhum dado encontrado para o período e threshold informados.")
            conn.close()
            return None
        perito = row[0]

    # Conta análises curtas para o perito
    cursor.execute("""
        SELECT COUNT(*) 
          FROM analises a
          JOIN peritos p ON a.siapePerito = p.siapePerito
         WHERE p.nomePerito = ?
           AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
           AND (julianday(a.dataHoraFimPericia)-julianday(a.dataHoraIniPericia))*86400 <= ?
    """, (perito, start, end, threshold))
    cnt_eleito = cursor.fetchone()[0] or 0

    # Conta análises curtas para os demais
    cursor.execute("""
        SELECT COUNT(*) 
          FROM analises a
          JOIN peritos p ON a.siapePerito = p.siapePerito
         WHERE p.nomePerito <> ?
           AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
           AND (julianday(a.dataHoraFimPericia)-julianday(a.dataHoraIniPericia))*86400 <= ?
    """, (perito, start, end, threshold))
    cnt_outros = cursor.fetchone()[0] or 0

    conn.close()
    return perito, cnt_eleito, cnt_outros

def exportar_md(perito, cnt_p, cnt_o, threshold, start, end):
    md = f"""# Comparação de Perícias ≤ {threshold}s

**Período:** {start} a {end}  
**Perito:** {perito}

| Categoria | Qtd. Perícias |
|-----------|----------------|
| {perito} | {cnt_p} |
| Demais    | {cnt_o} |

"""
    filename = f"compare_30s_{perito.replace(' ', '_')}.md"
    with open(os.path.join(EXPORT_DIR, filename), "w", encoding="utf-8") as f:
        f.write(md)
    print(f"✅ Tabela Markdown salva em {filename}")

def exportar_png(perito, cnt_p, cnt_o, threshold):
    plt.figure(figsize=(6, 4))
    plt.bar([perito, 'Demais'], [cnt_p, cnt_o], color=["#1f77b4", "#ff7f0e"])
    plt.title(f"Perícias ≤ {threshold}s")
    plt.ylabel("Quantidade")
    plt.tight_layout()
    filename = f"compare_30s_{perito.replace(' ', '_')}.png"
    plt.savefig(os.path.join(EXPORT_DIR, filename))
    print(f"✅ Gráfico PNG salvo em {filename}")

def exportar_comentario(perito, cnt_p, cnt_o, threshold):
    comentario = f"Análise comparativa de perícias com duração até {threshold}s entre o perito {perito} e os demais."
    filename = f"compare_30s_{perito.replace(' ', '_')}_comment.md"
    with open(os.path.join(EXPORT_DIR, filename), "w", encoding="utf-8") as f:
        f.write(comentario)
    print(f"✅ Comentário base salvo em {filename}")

def exibir_chart(perito, cnt_p, cnt_o, threshold):
    import plotext as p
    p.clear_data()
    p.bar([perito, 'Demais'], [cnt_p, cnt_o])
    p.title(f"Perícias ≤ {threshold}s")
    p.plotsize(80, 15)
    p.show()

if __name__ == '__main__':
    args = parse_args()
    resultado = comparar(args.start, args.end, args.threshold, args.perito)
    if resultado:
        perito, cnt_p, cnt_o = resultado
        if args.export_md:
            exportar_md(perito, cnt_p, cnt_o, args.threshold, args.start, args.end)
        if args.export_png:
            exportar_png(perito, cnt_p, cnt_o, args.threshold)
        if args.export_comment:
            exportar_comentario(perito, cnt_p, cnt_o, args.threshold)
        if args.chart:
            exibir_chart(perito, cnt_p, cnt_o, args.threshold)

