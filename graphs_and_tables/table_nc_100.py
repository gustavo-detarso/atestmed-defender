#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sys
import argparse
import sqlite3
import pandas as pd
from tabulate import tabulate
from utils.comentarios import comentar_nc100  # integração GPT

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(
        description="Lista peritos com 100% de não conformidade no período."
    )
    p.add_argument('--start', required=True, help='Data inicial (YYYY-MM-DD)')
    p.add_argument('--end', required=True, help='Data final   (YYYY-MM-DD)')
    p.add_argument('--export-md', action='store_true', help='Exporta para Markdown')
    p.add_argument('--export-csv', action='store_true', help='Exporta para CSV')
    p.add_argument('--export-comment', action='store_true', help='Exporta comentário base')
    p.add_argument('--add-comments', action='store_true', help='Gera comentário automaticamente (modo PDF)')
    return p.parse_args()

def export_md(df: pd.DataFrame, start: str, end: str):
    fname = f"peritos_100nc_{start}_{end}.md"
    path = os.path.join(EXPORT_DIR, fname)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(f"# Peritos com 100% de Não Conformidade\n\n")
        f.write(f"**Período:** {start} até {end}\n\n")
        f.write(df.to_markdown(index=False))
    print(f"✅ Exportado Markdown para {path}")
    return df.to_markdown(index=False)

def export_csv(df: pd.DataFrame, start: str, end: str):
    fname = f"peritos_100nc_{start}_{end}.csv"
    path = os.path.join(EXPORT_DIR, fname)
    df.to_csv(path, index=False)
    print(f"✅ Exportado CSV para {path}")

def export_comment(df: pd.DataFrame, start: str, end: str):
    tabela_md = df.to_markdown(index=False)
    comentario = comentar_nc100(tabela_md, start, end)
    fname = f"peritos_100nc_{start}_{end}_comment.md"
    path = os.path.join(EXPORT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(comentario)
    print(f"✅ Comentário ChatGPT salvo em: {path}")

def main():
    args = parse_args()
    conn = sqlite3.connect(DB_PATH)
    sql = '''
        SELECT p.nomePerito, p.siapePerito, p.cr, p.dr, a.motivoNaoConformado
          FROM peritos p
          JOIN analises a ON p.siapePerito = a.siapePerito
         WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
    '''
    df = pd.read_sql(sql, conn, params=(args.start, args.end))
    conn.close()

    if df.empty:
        print(f"⚠️ Nenhum registro encontrado entre {args.start} e {args.end}.")
        sys.exit(0)

    grp = df.groupby(['nomePerito', 'siapePerito', 'cr', 'dr'])
    stats = grp.agg(
        total_tarefas = ('motivoNaoConformado','count'),
        nc_soma       = ('motivoNaoConformado','sum')
    ).reset_index()

    result = stats[stats['nc_soma'] == stats['total_tarefas']].copy()
    if result.empty:
        print("⚠️ Nenhum perito com 100% de não conformidade no período.")
        sys.exit(0)

    result = result[['nomePerito','siapePerito','cr','dr','total_tarefas']]
    result.columns = ['Nome', 'SIAPE', 'CR', 'DR', 'Total Tarefas']
    result = result.sort_values(['Total Tarefas', 'Nome'], ascending=[False, True])

    print(tabulate(result, headers='keys', tablefmt='github', showindex=False))

    if args.export_md:
        export_md(result, args.start, args.end)
    if args.export_csv:
        export_csv(result, args.start, args.end)
    if args.export_comment or args.add_comments:
        export_comment(result, args.start, args.end)

if __name__ == '__main__':
    main()

