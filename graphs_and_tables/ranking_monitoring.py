#!/usr/bin/env python3
import os
import sys
import argparse
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from tabulate import tabulate

BASE_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, '..'))
DB_PATH = os.path.join(ROOT_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(ROOT_DIR, 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(
        description="Ranking detalhado de peritos: tarefas ≥ 50, por Score Final"
    )
    p.add_argument('--start', required=True, help='Data inicial (YYYY-MM-DD)')
    p.add_argument('--end', required=True, help='Data final   (YYYY-MM-DD)')
    p.add_argument('--export-md', action='store_true', help='Exporta para Markdown')
    p.add_argument('--export-csv', action='store_true', help='Exporta para CSV')
    p.add_argument('--export-comment', action='store_true', help='Exporta comentário base')
    return p.parse_args()

def export_md(df: pd.DataFrame, start: str, end: str):
    fname = f"ranking_score_detalhado_{start}_{end}.md"
    path = os.path.join(EXPORT_DIR, fname)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(f"# Ranking detalhado de peritos: tarefas ≥ 50\n\n")
        f.write(f"**Período:** {start} até {end}\n\n")
        f.write(df.to_markdown(index=False))
    print(f"✅ Exportado Markdown para {path}")

def export_csv(df: pd.DataFrame, start: str, end: str):
    fname = f"ranking_score_detalhado_{start}_{end}.csv"
    path = os.path.join(EXPORT_DIR, fname)
    df.to_csv(path, index=False)
    print(f"✅ Exportado CSV para {path}")

def export_comment(start: str, end: str):
    texto = f"Ranking detalhado de peritos com 50 ou mais tarefas no período de {start} a {end}, ordenado pelo Score Final calculado com base em critérios de produtividade, sobreposição, duração curta e não conformidade."
    fname = f"ranking_score_detalhado_{start}_{end}_comment.md"
    path = os.path.join(EXPORT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(texto)
    print(f"✅ Comentário salvo em: {path}")

def main():
    args = parse_args()
    conn = sqlite3.connect(DB_PATH)
    sql = '''
        SELECT p.siapePerito, p.nomePerito, p.cr, p.dr,
               a.dataHoraIniPericia, a.dataHoraFimPericia, a.motivoNaoConformado
          FROM peritos p
          JOIN analises a ON p.siapePerito = a.siapePerito
         WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
    '''
    df = pd.read_sql(sql, conn, params=(args.start, args.end))
    conn.close()

    if df.empty:
        print(f"⚠️ Nenhum registro entre {args.start} e {args.end}.")
        sys.exit(0)

    df['start'] = pd.to_datetime(df['dataHoraIniPericia'])
    df['end']   = pd.to_datetime(df['dataHoraFimPericia'])
    df['dur']   = (df['end'] - df['start']).dt.total_seconds()

    grp = df.groupby(['siapePerito','nomePerito','cr','dr'], as_index=False)
    stats = grp.agg(
        total_tasks=('dur','count'),
        nc_count=('motivoNaoConformado','sum'),
        short_count=('dur', lambda x: (x<=15).sum()),
        total_secs=('dur','sum')
    )

    stats['hours'] = stats['total_secs'] / 3600
    stats['prod']  = stats['total_tasks'] / stats['hours'].replace(0, np.nan)

    def has_overlap(subdf):
        times = sorted(zip(subdf['start'], subdf['end']))
        return any(times[i+1][0] < times[i][1] for i in range(len(times)-1))
    overlap = df.groupby(['siapePerito','nomePerito','cr','dr']).apply(has_overlap)
    overlap = overlap.to_frame('overlap').reset_index()
    stats = stats.merge(overlap, on=['siapePerito','nomePerito','cr','dr'])

    stats['nc_ratio'] = stats['nc_count'] / stats['total_tasks']
    avg_nc = stats['nc_ratio'].mean()

    stats['icra'] = 0.0
    stats.loc[stats['prod'] >= 50,        'icra'] += 3.0
    stats.loc[stats['overlap'],           'icra'] += 2.5
    stats.loc[stats['short_count'] >= 10, 'icra'] += 2.0
    stats.loc[stats['nc_ratio'] >= 2*avg_nc,'icra'] += 1.0

    stats['iatd'] = 1 - stats['nc_ratio']
    stats['score_final'] = stats['icra'] + (1 - stats['iatd'])

    stats = stats[stats['total_tasks'] >= 50]
    if stats.empty:
        print(f"⚠️ Nenhum perito com ≥50 tarefas entre {args.start} e {args.end}.")
        sys.exit(0)

    result = stats[['nomePerito','siapePerito','cr','dr','total_tasks','icra','iatd','score_final']].copy()
    result.columns = ['Nome','SIAPE','CR','DR','Total Tarefas','ICRA','IATD','Score Final']
    result = result.sort_values('Score Final', ascending=False)
    print(tabulate(result, headers='keys', tablefmt='github', showindex=False, floatfmt='.2f'))

    if args.export_md:
        export_md(result, args.start, args.end)
    if args.export_csv:
        export_csv(result, args.start, args.end)
    if args.export_comment:
        export_comment(args.start, args.end)

if __name__ == '__main__':
    main()

