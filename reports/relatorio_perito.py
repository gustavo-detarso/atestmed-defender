#!/usr/bin/env python3
import os
import sys
import subprocess
import sqlite3
import glob
import shutil
import pandas as pd
from fpdf import FPDF
from PyPDF2 import PdfMerger

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'db', 'atestmed.db')
GRAPHS_DIR = os.path.join(BASE_DIR, 'graphs_and_tables')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
OUTPUTS_DIR = os.path.join(BASE_DIR, 'reports', 'outputs')
MISC_DIR = os.path.join(BASE_DIR, 'misc')
os.makedirs(OUTPUTS_DIR, exist_ok=True)

# ——— Mapeamento de nomes exatos de arquivos gerados pelos scripts —————————
MAPA_ARQUIVOS = {
    'compare_overlap': {
        'png':    'sobreposicao_{perito}.png',
        'md':     'sobreposicao_{perito}.md',
        'comment':'sobreposicao_{perito}_comment.md',
    },
    'compare_30s': {
        'png':    'compare_30s_{perito}.png',
        'md':     'compare_30s_{perito}.md',
        'comment':'compare_30s_{perito}_comment.md',
    },
    'compare_productivity': {
        'png':    'produtividade_{perito}.png',
        'md':     'produtividade_{perito}.md',
        'comment':'produtividade_{perito}_comment.md',
    },
    'protocolos_icra': {
        'md':     'protocolos_icra_{perito}_{start}_{end}.md',
        'xlsx':   'protocolos_icra_{perito}_{start}_{end}.xlsx',
        'comment':'protocolos_icra_{perito}_{start}_{end}_comment.md',
    },
    'table_nc_100': {
        'md':     'peritos_100nc_{start}_{end}.md',
        'csv':    'peritos_100nc_{start}_{end}.csv',
        'comment':'peritos_100nc_{start}_{end}_comment.md',
    },
    'rank_score_final': {
        'md':     'ranking_cr_score_{start}_{end}.md',
        'png':    'ranking_cr_score_{start}_{end}.png',
        'comment':'ranking_cr_score_{start}_{end}_comment.md',
    },
    'ranking_monitoring': {
        'md':     'ranking_score_detalhado_{start}_{end}.md',
        'csv':    'ranking_score_detalhado_{start}_{end}.csv',
        'comment':'ranking_score_detalhado_{start}_{end}_comment.md',
    }
}

def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Relatório consolidado do perito")
    p.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    p.add_argument('--end',   required=True, help='Data final   YYYY-MM-DD')
    p.add_argument('--perito', required=True, help='Nome do perito (exato)')
    p.add_argument('--export-pdf', action='store_true', help='Exporta relatório em PDF')
    p.add_argument('--add-comments', action='store_true', help='Inclui comentários GPT nos gráficos')
    return p.parse_args()

def script_aceita_argumento(script_path, argumento):
    try:
        proc = subprocess.run(
            [sys.executable, script_path, '--help'],
            capture_output=True, text=True
        )
        return argumento in proc.stdout
    except Exception:
        return False

def gerar_graficos_do_perito(perito, start, end, add_comments):
    """
    Executa cada script, força exportações e copia os arquivos mapeados para tmp_graphs.
    """
    temp = os.path.join(OUTPUTS_DIR, "tmp_graphs")
    os.makedirs(temp, exist_ok=True)

    lista_pngs, lista_comentarios, lista_tabelas = [], [], []

    for script in sorted(os.listdir(GRAPHS_DIR)):
        if not script.endswith('.py') or script.startswith('_'):
            continue
        base = os.path.splitext(script)[0]
        path = os.path.join(GRAPHS_DIR, script)

        # detecta qual flag de "nome" o script aceita
        if script_aceita_argumento(path, '--perito'):
            arg_per = '--perito'
        elif script_aceita_argumento(path, '--nome'):
            arg_per = '--nome'
        else:
            print(f"Pulando {script}: não aceita --perito/--nome")
            continue

        cmd = [sys.executable, path,
               '--start', start, '--end', end,
               arg_per, perito]

        # flags automatizadas
        if script_aceita_argumento(path, '--export-md'):
            cmd.append('--export-md')
        if script_aceita_argumento(path, '--export-csv'):
            cmd.append('--export-csv')
            want_csv = True
        else:
            want_csv = False
        if script_aceita_argumento(path, '--export-png'):
            cmd.append('--export-png')
        if add_comments and script_aceita_argumento(path, '--export-comment'):
            cmd.append('--export-comment')

        print(f"Executando: {' '.join(cmd)}")
        subprocess.run(cmd, check=False)

        # agora copia cada saída conforme MAPA_ARQUIVOS
        m = MAPA_ARQUIVOS.get(base, {})
        # PNG
        png_name = m.get('png')
        if png_name:
            fn = png_name.format(perito=perito.replace(' ', '_'),
                                  start=start, end=end)
            src = os.path.join(EXPORT_DIR, fn)
            if os.path.exists(src):
                dst = os.path.join(temp, fn)
                shutil.copy(src, dst)
                lista_pngs.append(dst)
            else:
                print(f"⚠️ PNG não encontrado: {src}")
        else:
            lista_pngs.append(None)

        # comentário MD
        if add_comments:
            cm_name = m.get('md', '').replace('.md','_comment.md')
            src = os.path.join(EXPORT_DIR, cm_name)
            if os.path.exists(src):
                with open(src, encoding='utf-8') as f:
                    lista_comentarios.append(f.read())
            else:
                lista_comentarios.append("")
        else:
            lista_comentarios.append("")

        # tabela CSV / XLSX
        if want_csv and 'csv' in m:
            tbl = m['csv'].format(perito=perito.replace(' ', '_'),
                                  start=start, end=end)
            src = os.path.join(EXPORT_DIR, tbl)
            if os.path.exists(src):
                dst = os.path.join(temp, tbl)
                shutil.copy(src, dst)
                lista_tabelas.append(dst)
            else:
                lista_tabelas.append(None)
        else:
            lista_tabelas.append(None)

    return lista_pngs, lista_comentarios, lista_tabelas

def gerar_apendice_nc(perito, start, end):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        """
        SELECT protocolo, motivoNaoConformado
        FROM analises a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        WHERE p.nomePerito = ? AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
        ORDER BY protocolo
        """,
        conn, params=(perito, start, end)
    )
    conn.close()
    return df

def inserir_tabela_pdf(pdf, table_path):
    df = pd.read_csv(table_path)
    pdf.add_page()
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, f"Tabela: {os.path.basename(table_path)}", ln=True, align="C")
    pdf.set_font("Arial", "", 10)
    col_w = pdf.w / (len(df.columns) + 1)
    row_h = pdf.font_size + 2
    # cabeçalho
    for col in df.columns:
        pdf.cell(col_w, row_h, str(col), border=1)
    pdf.ln(row_h)
    # linhas
    for _, row in df.iterrows():
        for item in row:
            pdf.cell(col_w, row_h, str(item), border=1)
        pdf.ln(row_h)

def gerar_pdf_final(perito, start, end,
                    lista_pngs, lista_comentarios, lista_tabelas, apendice_df):
    # inicializa
    capa = os.path.join(MISC_DIR, 'capa.pdf')
    merger = PdfMerger() if os.path.exists(capa) else None
    if merger:
        merger.append(capa)

    # monta PDF principal em temp
    temp = FPDF()
    temp.set_auto_page_break(True, 15)
    temp.add_page()
    temp.set_font('Arial','B',16)
    temp.cell(0,20,'Relatório Consolidado do Perito',ln=True,align='C')
    temp.set_font('Arial','',12)
    temp.ln(10)
    temp.cell(0,10,f'Perito: {perito}',ln=True)
    temp.cell(0,10,f'Período: {start} a {end}',ln=True)
    temp.ln(10)

    # gráficos + comentários
    for i,(img, comm) in enumerate(zip(lista_pngs, lista_comentarios), start=1):
        temp.add_page()
        temp.set_font('Arial','B',13)
        temp.cell(0,10,f'Gráfico {i}',ln=True)
        try:
            temp.image(img, x=15, y=25, w=180)
            temp.ln(95)
        except Exception:
            temp.cell(0,10,'Erro ao carregar imagem',ln=True)
        if comm:
            temp.set_font('Arial','I',10)
            temp.ln(10)
            temp.multi_cell(0,8, comm)

    # tabelas
    for tbl in lista_tabelas:
        if tbl:
            inserir_tabela_pdf(temp, tbl)

    # apêndice NC
    temp.add_page()
    temp.set_font('Arial','B',12)
    temp.cell(0,10,'Apêndice: Protocolos Não Conformes',ln=True)
    temp.set_font('Arial','',10)
    if not apendice_df.empty:
        for _, r in apendice_df.iterrows():
            temp.cell(0,8,f"Protocolo: {r['protocolo']}  Motivo: {r['motivoNaoConformado']}",ln=True)
    else:
        temp.cell(0,10,'Nenhum protocolo com Não Conformidade encontrado.',ln=True)

    # grava temp e fecha merger
    tmp_file = os.path.join(OUTPUTS_DIR, f'temp_{perito.replace(" ","_")}.pdf')
    temp.output(tmp_file)
    if merger:
        merger.append(tmp_file)
        out = os.path.join(OUTPUTS_DIR,
            f'relatorio_{perito.replace(" ","_")}_{start}_a_{end}.pdf')
        merger.write(out)
        merger.close()
        os.remove(tmp_file)
    else:
        out = tmp_file
    print(f"✅ Relatório salvo em: {out}")
    return out

def main():
    args = parse_args()
    print("=== ARGS DEBUG:", vars(args))
    if not args.export_pdf:
        print("Nada a fazer. Use --export-pdf para gerar relatório.")
        return

    pngs, comms, tabs = gerar_graficos_do_perito(
        args.perito, args.start, args.end, args.add_comments
    )
    ap_df = gerar_apendice_nc(args.perito, args.start, args.end)

    gerar_pdf_final(
        args.perito, args.start, args.end,
        pngs, comms, tabs, ap_df
    )

if __name__ == '__main__':
    main()

