#!/usr/bin/env python3
import os
import sys
import sqlite3
import argparse
import pandas as pd
import numpy as np
import questionary
from tabulate import tabulate
from datetime import datetime

# Caminho absoluto para a raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Caminho absoluto para o banco de dados
DB_PATH = os.path.join(BASE_DIR, 'db', 'atestmed.db')

# Caminho absoluto para exports
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

def parse_args():
    parser = argparse.ArgumentParser(
        description="Detalhamento dos protocolos acionados nos critérios do ICRA para um perito"
    )
    parser.add_argument('--start', required=True, help='Data inicial (YYYY-MM-DD)')
    parser.add_argument('--end',   required=True, help='Data final   (YYYY-MM-DD)')
    parser.add_argument('--nome', required=True, help='Nome do perito (required)')
    parser.add_argument('--export-md', action="store_true", help="Exportar direto para Markdown")
    parser.add_argument('--export-xlsx', action="store_true", help="Exportar direto para Excel")
    parser.add_argument('--export-comment', action="store_true", help="Exporta comentário ChatGPT no Markdown")
    return parser.parse_args()

def export_md(nome, start, end, prod_rate, hours, total, protocolos_short, protocolos_nc,
              protocolos_overlap, df_all):
    safe = nome.replace(' ', '_')
    fname = f"protocolos_icra_{safe}_{start}_{end}.md"
    path = os.path.join(EXPORT_DIR, fname)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(f"# Detalhamento ICRA - {nome}\n\n")
        f.write(f"**Período:** {start} até {end}\n\n")
        f.write(f"**Produtividade:** {prod_rate:.1f}/h (total {total}, horas {hours:.2f})\n\n")
        if not (protocolos_short or protocolos_nc or protocolos_overlap or prod_rate >= 50):
            f.write("**Fora de risco**\n")
        else:
            f.write("## Análises ≤15s ({})\n".format(len(protocolos_short)))
            f.write("```\n{}\n```\n\n".format(protocolos_short))
            f.write("## Não conformados ({})\n".format(len(protocolos_nc)))
            f.write("```\n{}\n```\n\n".format(protocolos_nc))
            f.write("## Sobreposição ({})\n".format(len(protocolos_overlap)))
            f.write("```\n{}\n```\n\n".format(protocolos_overlap))
        f.write("## Todos os protocolos\n")
        f.write("```\n{}\n```\n".format(df_all['protocolo'].tolist()))
    print(f"✅ Exportado Markdown para {path}")
    return path

def export_excel(nome, start, end, protocolos_short, protocolos_nc, protocolos_overlap, df_all):
    safe = nome.replace(' ', '_')
    fname = f"protocolos_icra_{safe}_{start}_{end}.xlsx"
    path = os.path.join(EXPORT_DIR, fname)
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
        pd.DataFrame({'Protocolo': protocolos_short}).to_excel(writer, sheet_name='<=15s', index=False)
        pd.DataFrame({'Protocolo': protocolos_nc}).to_excel(writer, sheet_name='NC', index=False)
        pd.DataFrame({'Protocolo': protocolos_overlap}).to_excel(writer, sheet_name='Overlap', index=False)
        if not (protocolos_short or protocolos_nc or protocolos_overlap):
            pd.DataFrame({'Status': ['Fora de risco']}).to_excel(writer, sheet_name='Fora_de_risco', index=False)
        df_all[['protocolo']].drop_duplicates().to_excel(writer, sheet_name='Todos', index=False)
    print(f"✅ Exportado Excel para {path}")
    return path

def export_comment_md(nome, start, end):
    # Busca nome seguro de arquivo
    safe = nome.replace(' ', '_')
    md_path = os.path.join(EXPORT_DIR, f"protocolos_icra_{safe}_{start}_{end}.md")
    if not os.path.isfile(md_path):
        print("Arquivo Markdown não encontrado para gerar comentário.")
        sys.exit(1)
    try:
        from openai import OpenAI
    except ImportError:
        print("Pacote openai não instalado.")
        sys.exit(1)
    with open(md_path, "r", encoding="utf-8") as f:
        conteudo = f.read()
    prompt = f"Gere um comentário profissional e sucinto para gestores sobre o detalhamento do ICRA do perito '{nome}' no período {start} a {end}:\n\n{conteudo}"
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", "SUA_API_KEY"))
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    comentario = response.choices[0].message.content.strip()
    out_path = md_path.replace(".md", "_comment.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("# Comentário automático do ChatGPT\n\n")
        f.write(comentario)
    print(f"✅ Comentário ChatGPT salvo em {out_path}")

def main():
    args = parse_args()
    start, end = args.start, args.end

    # --- fluxo para geração de comentário (sem consultar BD, só gera comentário!) ---
    if args.export_comment:
        if not args.nome:
            print("Precisa informar --nome para exportar comentário!")
            sys.exit(1)
        export_comment_md(args.nome, start, end)
        sys.exit(0)

    # Nome do perito: interativo ou não
    if args.nome:
        nome = args.nome
    else:
        conn = sqlite3.connect(DB_PATH)
        nomes = [r[0] for r in conn.execute("SELECT nomePerito FROM peritos")]
        nome = questionary.autocomplete(
            "Selecione o perito:",
            choices=sorted(nomes),
            match_middle=True
        ).ask()
        conn.close()
        if not nome:
            sys.exit(1)

    sql = '''
        SELECT a.protocolo, a.dataHoraIniPericia, a.dataHoraFimPericia, a.motivoNaoConformado
          FROM analises a
          JOIN peritos p ON a.siapePerito = p.siapePerito
         WHERE p.nomePerito = ?
           AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
    '''
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(sql, conn, params=(nome, start, end))
    conn.close()

    print(f"→ {len(df)} análises para {nome} entre {start}-{end}.")
    if df.empty:
        print("⚠️  Sem análises.")
        sys.exit(0)

    df['start'] = pd.to_datetime(df['dataHoraIniPericia'])
    df['end']   = pd.to_datetime(df['dataHoraFimPericia'])
    df['dur']   = (df['end'] - df['start']).dt.total_seconds()

    total = len(df)
    hours = df['dur'].sum() / 3600 if df['dur'].sum() > 0 else 0
    prod = total / hours if hours > 0 else 0
    protocolos_short = df.loc[df['dur'] <= 15, 'protocolo'].tolist()

    conn = sqlite3.connect(DB_PATH)
    avg_nc = pd.read_sql(
        """
        SELECT AVG(nc_ratio) AS avg_nc FROM (
          SELECT siapePerito,
                 SUM(motivoNaoConformado)*1.0/COUNT(*) AS nc_ratio
            FROM analises
           WHERE date(dataHoraIniPericia) BETWEEN ? AND ?
           GROUP BY siapePerito
        )
        """, conn, params=(start, end)
    )['avg_nc'].iat[0]
    conn.close()
    protocolos_nc = df.loc[df['motivoNaoConformado'] == 1, 'protocolo'].tolist()

    overlaps = set()
    sd = df.sort_values('start')
    for i in range(len(sd) - 1):
        if sd.iloc[i + 1]['start'] < sd.iloc[i]['end']:
            overlaps.update([sd.iloc[i]['protocolo'], sd.iloc[i + 1]['protocolo']])
    protocolos_overlap = sorted(overlaps)

    print(f"Prod: {prod:.1f}/h | ≤15s:{len(protocolos_short)} | NC:{len(protocolos_nc)} | Ov:{len(protocolos_overlap)} | Total:{total}")

    # Lógica de exportação automática:
    any_export = args.export_md or args.export_xlsx
    md_path = None
    if any_export:
        if args.export_md:
            md_path = export_md(nome, start, end, prod, hours, total,
                               protocolos_short, protocolos_nc, protocolos_overlap, df)
        if args.export_xlsx:
            export_excel(nome, start, end, protocolos_short,
                         protocolos_nc, protocolos_overlap, df)
    else:
        # Se não há flags, segue modo interativo tradicional
        choice = questionary.select(
            "Exportar em:",
            choices=["Markdown", "Excel", "Ambos"],
            use_arrow_keys=True
        ).ask()
        if choice in ("Markdown", "Ambos"):
            md_path = export_md(nome, start, end, prod, hours, total,
                               protocolos_short, protocolos_nc, protocolos_overlap, df)
        if choice in ("Excel", "Ambos"):
            export_excel(nome, start, end, protocolos_short,
                         protocolos_nc, protocolos_overlap, df)

if __name__ == '__main__':
    main()

