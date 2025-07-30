#!/usr/bin/env python3
import os
import sys
import argparse
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from tabulate import tabulate
import matplotlib.pyplot as plt

# Diretórios e paths
BASE_DIR   = os.path.dirname(__file__)
DB_PATH    = os.path.join(BASE_DIR, '..', 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, '..', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(
        description="Ranking de CRs por Score Final médio dos peritos"
    )
    p.add_argument('--start', required=True, help='Data inicial (YYYY-MM-DD)')
    p.add_argument('--end', required=True, help='Data final   (YYYY-MM-DD)')
    p.add_argument('--export-md', action='store_true', help='Exporta tabela em Markdown')
    p.add_argument('--export-png', action='store_true', help='Exporta gráfico em PNG')
    p.add_argument('--export-comment', action='store_true', help='Exporta comentário explicativo')
    return p.parse_args()

def compute_perito_scores(df):
    df['start'] = pd.to_datetime(df['dataHoraIniPericia'])
    df['end']   = pd.to_datetime(df['dataHoraFimPericia'])
    df['dur']   = (df['end'] - df['start']).dt.total_seconds()

    grp = df.groupby(['siapePerito','nomePerito','cr'])
    stats = grp.agg(
        total       = ('dur','count'),
        nc_count    = ('motivoNaoConformado','sum'),
        short_count = ('dur', lambda x: (x<=15).sum()),
        total_secs  = ('dur','sum')
    ).reset_index()

    stats['hours'] = stats['total_secs'] / 3600
    stats['prod']  = stats['total'] / stats['hours'].replace(0,np.nan)

    def has_overlap(sub):
        times = sorted(zip(sub['start'], sub['end']))
        return any(times[i+1][0] < times[i][1] for i in range(len(times)-1))
    overlap = grp.apply(has_overlap).rename('overlap').reset_index()
    stats = stats.merge(overlap, on=['siapePerito','nomePerito','cr'])

    stats['nc_ratio'] = stats['nc_count'] / stats['total']
    avg_nc = stats['nc_ratio'].mean()

    stats['icra'] = 0.0
    stats.loc[stats['prod'] >= 50,        'icra'] += 3.0
    stats.loc[stats['overlap'],           'icra'] += 2.5
    stats.loc[stats['short_count'] >= 10, 'icra'] += 2.0
    stats.loc[stats['nc_ratio'] >= 2*avg_nc, 'icra'] += 1.0

    stats['iatd'] = 1 - stats['nc_ratio']
    stats['score_final'] = stats['icra'] + (1 - stats['iatd'])

    return stats[['cr','score_final']]

def rank_cr(start, end):
    conn = sqlite3.connect(DB_PATH)
    sql = """
        SELECT p.siapePerito, p.nomePerito, p.cr,
               a.dataHoraIniPericia, a.dataHoraFimPericia,
               a.motivoNaoConformado
          FROM peritos p
          JOIN analises a ON p.siapePerito = a.siapePerito
         WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
    """
    df = pd.read_sql(sql, conn, params=(start, end))
    conn.close()

    if df.empty:
        print(f"⚠️  Nenhum dado entre {start} e {end}.")
        return None

    perito_scores = compute_perito_scores(df)
    cr_rank = (
        perito_scores
        .groupby('cr')['score_final']
        .mean()
        .reset_index(name='Score Médio')
        .sort_values('Score Médio', ascending=False)
    )

    # Exibe no terminal
    print(tabulate(cr_rank, headers='keys', tablefmt='github', showindex=False, floatfmt='.2f'))
    return cr_rank

def export_md(df, start, end):
    fname = f"ranking_cr_score_{start}_{end}.md"
    path  = os.path.join(EXPORT_DIR, fname)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(f"# Ranking de CRs por Score Médio\n\n")
        f.write(f"**Período:** {start} até {end}\n\n")
        f.write(df.to_markdown(index=False))
    print(f"✅ Markdown salvo em: {path}")

def export_png(df, start, end):
    fname = f"ranking_cr_score_{start}_{end}.png"
    path  = os.path.join(EXPORT_DIR, fname)
    plt.figure(figsize=(10, 6))
    plt.barh(df['cr'], df['Score Médio'], color="#1f77b4")
    plt.xlabel("Score Médio")
    plt.title(f"Score Médio por CR ({start} a {end})")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig(path)
    print(f"✅ Gráfico PNG salvo em: {path}")

def export_comment(start, end):
    texto = f"Ranking das CRs baseado no Score Final médio dos peritos entre {start} e {end}, refletindo critérios de produtividade, sobreposição, tempo de análise e não conformidade."
    fname = f"ranking_cr_score_{start}_{end}_comment.md"
    path = os.path.join(EXPORT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(texto)
    print(f"✅ Comentário salvo em: {path}")

if __name__ == '__main__':
    args = parse_args()
    df = rank_cr(args.start, args.end)
    if df is None:
        sys.exit(0)
    if args.export_md:
        export_md(df, args.start, args.end)
    if args.export_png:
        export_png(df, args.start, args.end)
    if args.export_comment:
        export_comment(args.start, args.end)

