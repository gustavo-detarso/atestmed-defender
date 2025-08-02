#!/usr/bin/env python3
import os
import sys
import subprocess
import sqlite3
import pandas as pd
from fpdf import FPDF
from PyPDF2 import PdfMerger

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'db', 'atestmed.db')
GRAPHS_DIR = os.path.join(BASE_DIR, 'graphs_and_tables')
OUTPUTS_DIR = os.path.join(BASE_DIR, 'reports', 'outputs')
MISC_DIR = os.path.join(BASE_DIR, 'misc')
os.makedirs(OUTPUTS_DIR, exist_ok=True)

def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Relatório consolidado do perito")
    p.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    p.add_argument('--end', required=True, help='Data final YYYY-MM-DD')
    p.add_argument('--perito', required=True, help='Nome do perito (exato)')
    p.add_argument('--export-pdf', action='store_true', help='Exporta relatório em PDF')
    p.add_argument('--add-comments', action='store_true', help='Inclui comentários GPT')
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
    lista_pngs = []
    lista_comentarios = []
    lista_tables = []

    temp_graph_dir = os.path.join(OUTPUTS_DIR, "tmp_graphs")
    os.makedirs(temp_graph_dir, exist_ok=True)

    scripts = [f for f in os.listdir(GRAPHS_DIR) if f.endswith(".py") and not f.startswith("_")]

    for script in scripts:
        script_path = os.path.join(GRAPHS_DIR, script)
        nome_base = os.path.splitext(script)[0]

        img_path = os.path.join(temp_graph_dir, f"{nome_base}_{perito.replace(' ','_')}.png")
        comment_path = os.path.join(temp_graph_dir, f"{nome_base}_{perito.replace(' ','_')}_comment.md")
        table_path = os.path.join(temp_graph_dir, f"{nome_base}_{perito.replace(' ','_')}.csv")

        # Detecta argumento correto para o perito
        if script_aceita_argumento(script_path, '--perito'):
            arg_perito = '--perito'
        elif script_aceita_argumento(script_path, '--nome'):
            arg_perito = '--nome'
        else:
            print(f"Pulando {script} (não aceita --perito ou --nome)")
            continue

        # Monta comando base
        cmd = [sys.executable, script_path, '--start', start, '--end', end, arg_perito, perito]

        # Verifica exportação PNG
        if script_aceita_argumento(script_path, '--export-png'):
            cmd.append('--export-png')

        # Verifica exportação comentário
        if add_comments and script_aceita_argumento(script_path, '--export-comment'):
            cmd.append('--export-comment')

        # Verifica exportação tabela CSV
        export_csv = False
        if script_aceita_argumento(script_path, '--export-csv'):
            cmd.append('--export-csv')
            export_csv = True

        print(f"Executando: {' '.join(cmd)}")
        subprocess.run(cmd)

        # Adiciona PNG se gerado
        if os.path.exists(img_path):
            lista_pngs.append(img_path)
        else:
            print(f"⚠️ PNG não encontrado: {img_path}")

        # Adiciona comentário se gerado e solicitado
        if add_comments and os.path.exists(comment_path):
            with open(comment_path, encoding='utf-8') as f:
                lista_comentarios.append(f.read())
        else:
            lista_comentarios.append("")

        # Adiciona tabela se gerada
        if export_csv and os.path.exists(table_path):
            lista_tables.append(table_path)
        else:
            lista_tables.append(None)

    return lista_pngs, lista_comentarios, lista_tables

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
        conn,
        params=(perito, start, end)
    )
    conn.close()
    return df

def inserir_tabela_pdf(pdf, table_path):
    df = pd.read_csv(table_path)
    pdf.add_page()
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, f"Tabela: {os.path.basename(table_path)}", ln=True, align="C")
    pdf.set_font("Arial", "", 10)
    col_width = pdf.w / (len(df.columns) + 1)
    row_height = pdf.font_size + 2
    # Cabeçalho
    for col in df.columns:
        pdf.cell(col_width, row_height, str(col), border=1)
    pdf.ln(row_height)
    # Dados
    for _, row in df.iterrows():
        for item in row:
            pdf.cell(col_width, row_height, str(item), border=1)
        pdf.ln(row_height)

def gerar_pdf_final(perito, start, end, lista_pngs, lista_comentarios, lista_tables, apendice_df):
    pdf = FPDF()
    # Adiciona capa
    capa_path = os.path.join(MISC_DIR, "capa.pdf")
    if os.path.exists(capa_path):
        merger = PdfMerger()
        merger.append(capa_path)
        # Cria pdf temporário dos gráficos e tabelas
        temp_pdf_path = os.path.join(OUTPUTS_DIR, f"temp_{perito.replace(' ','_')}_{start}_a_{end}.pdf")
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 20, "Relatório Consolidado do Perito", ln=True, align="C")
        pdf.set_font("Arial", "", 12)
        pdf.ln(10)
        pdf.cell(0, 10, f"Perito: {perito}", ln=True)
        pdf.cell(0, 10, f"Período: {start} a {end}", ln=True)
        pdf.ln(10)

        # Insere gráficos e comentários
        for i, img in enumerate(lista_pngs):
            pdf.add_page()
            pdf.set_font("Arial", "B", 13)
            pdf.cell(0, 10, f"Gráfico {i+1}", ln=True)
            try:
                pdf.image(img, x=15, y=25, w=180)
                pdf.ln(95)
            except RuntimeError:
                pdf.cell(0, 10, "Erro ao carregar imagem", ln=True)
            if lista_comentarios[i]:
                pdf.set_font("Arial", "I", 10)
                pdf.ln(10)
                pdf.multi_cell(0, 8, lista_comentarios[i])

        # Insere tabelas
        for table_path in lista_tables:
            if table_path:
                inserir_tabela_pdf(pdf, table_path)

        # Insere apêndice
        pdf.add_page()
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "Apêndice: Protocolos Não Conformes", ln=True)
        pdf.set_font("Arial", "", 10)
        if apendice_df is not None and not apendice_df.empty:
            for _, row in apendice_df.iterrows():
                pdf.cell(0, 8, f"Protocolo: {row['protocolo']} - Motivo: {row['motivoNaoConformado']}", ln=True)
        else:
            pdf.cell(0, 10, "Nenhum protocolo com Não Conformidade encontrado.", ln=True)

        pdf.output(temp_pdf_path)
        merger.append(temp_pdf_path)

        pdf_saida = os.path.join(OUTPUTS_DIR, f"relatorio_{perito.replace(' ','_')}_{start}_a_{end}.pdf")
        merger.write(pdf_saida)
        merger.close()
        os.remove(temp_pdf_path)
        print("✅ Relatório final salvo em:", pdf_saida)
        return pdf_saida
    else:
        # Caso não tenha capa, gera pdf direto
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 20, "Relatório Consolidado do Perito", ln=True, align="C")
        pdf.set_font("Arial", "", 12)
        pdf.ln(10)
        pdf.cell(0, 10, f"Perito: {perito}", ln=True)
        pdf.cell(0, 10, f"Período: {start} a {end}", ln=True)
        pdf.ln(10)

        for i, img in enumerate(lista_pngs):
            pdf.add_page()
            pdf.set_font("Arial", "B", 13)
            pdf.cell(0, 10, f"Gráfico {i+1}", ln=True)
            try:
                pdf.image(img, x=15, y=25, w=180)
                pdf.ln(95)
            except RuntimeError:
                pdf.cell(0, 10, "Erro ao carregar imagem", ln=True)
            if lista_comentarios[i]:
                pdf.set_font("Arial", "I", 10)
                pdf.ln(10)
                pdf.multi_cell(0, 8, lista_comentarios[i])

        for table_path in lista_tables:
            if table_path:
                inserir_tabela_pdf(pdf, table_path)

        pdf.add_page()
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "Apêndice: Protocolos Não Conformes", ln=True)
        pdf.set_font("Arial", "", 10)
        if apendice_df is not None and not apendice_df.empty:
            for _, row in apendice_df.iterrows():
                pdf.cell(0, 8, f"Protocolo: {row['protocolo']} - Motivo: {row['motivoNaoConformado']}", ln=True)
        else:
            pdf.cell(0, 10, "Nenhum protocolo com Não Conformidade encontrado.", ln=True)

        pdf_saida = os.path.join(OUTPUTS_DIR, f"relatorio_{perito.replace(' ','_')}_{start}_a_{end}.pdf")
        pdf.output(pdf_saida)
        print("✅ Relatório final salvo em:", pdf_saida)
        return pdf_saida

def main():
    args = parse_args()
    print("=== ARGS DEBUG:", vars(args))

    if not args.export_pdf:
        print("Nada a fazer. Use --export-pdf para gerar relatório.")
        return

    # 1. Gera gráficos do perito (PNG + comentários + tabelas)
    lista_pngs, lista_comentarios, lista_tables = gerar_graficos_do_perito(
        args.perito, args.start, args.end, args.add_comments
    )

    # 2. Gera apêndice de NC (DataFrame)
    apendice_df = gerar_apendice_nc(args.perito, args.start, args.end)

    # 3. Monta e salva PDF final
    gerar_pdf_final(
        args.perito, args.start, args.end,
        lista_pngs, lista_comentarios, lista_tables, apendice_df
    )

if __name__ == '__main__':
    main()

