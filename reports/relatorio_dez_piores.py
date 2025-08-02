#!/usr/bin/env python3
import os
import sys
import subprocess
import sqlite3
import shutil
import pandas as pd
from fpdf import FPDF
from PyPDF2 import PdfMerger

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH     = os.path.join(BASE_DIR, 'db', 'atestmed.db')
GRAPHS_DIR  = os.path.join(BASE_DIR, 'graphs_and_tables')
EXPORT_DIR  = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
OUTPUTS_DIR = os.path.join(BASE_DIR, 'reports', 'outputs')
MISC_DIR    = os.path.join(BASE_DIR, 'misc')
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
    p = argparse.ArgumentParser(description="Relatório dos 10 piores peritos no período")
    p.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    p.add_argument('--end', required=True, help='Data final   YYYY-MM-DD')
    p.add_argument('--export-pdf', action='store_true', help='Exporta relatório em PDF')
    p.add_argument('--add-comments', action='store_true', help='Inclui comentários GPT nos gráficos')
    return p.parse_args()

def pegar_10_piores_peritos(start, end, min_analises=50):
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT
        p.nomePerito,
        i.scoreFinal,
        COUNT(a.protocolo) as total_analises
    FROM indicadores i
    JOIN peritos p ON i.perito = p.siapePerito
    JOIN analises a ON a.siapePerito = i.perito
    WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
    GROUP BY p.nomePerito, i.scoreFinal
    HAVING total_analises >= ?
    ORDER BY i.scoreFinal DESC
    LIMIT 10
    """
    df = pd.read_sql(query, conn, params=(start, end, min_analises))
    conn.close()
    return df

def script_aceita_argumento(script_path, argumento):
    """
    Retorna True se o help do script listar o argumento pedido.
    """
    try:
        out = subprocess.run(
            [sys.executable, script_path, "--help"],
            capture_output=True, text=True
        )
        return argumento in out.stdout
    except Exception:
        return False

def gerar_graficos_e_tabelas(perito, start, end, add_comments):
    """
    Executa todos os scripts de graphs_and_tables para um perito,
    obrigatoriamente em Markdown, e opcionalmente em comentário.
    Retorna listas paralelas de (png_paths, comentários, csv_paths).
    """
    lista_pngs = []
    lista_comentarios = []
    lista_tables = []

    temp = os.path.join(OUTPUTS_DIR, "tmp_graphs")
    os.makedirs(temp, exist_ok=True)

    scripts = [f for f in os.listdir(GRAPHS_DIR)
               if f.endswith(".py") and not f.startswith("_")]

    for script in scripts:
        script_path = os.path.join(GRAPHS_DIR, script)
        base = os.path.splitext(script)[0]
        safe = perito.replace(" ", "_")

        # aonde cada script salva PNG e MD habitualmente
        png_out = os.path.join(
            GRAPHS_DIR, "exports", f"{base}_{safe}.png"
        )
        md_out  = os.path.join(
            GRAPHS_DIR, "exports", f"{base}_{safe}.md"
        )
        csv_out = os.path.join(
            GRAPHS_DIR, "exports", f"{base}_{safe}.csv"
        )

        # detecta qual flag usar para o nome/perito
        if script_aceita_argumento(script_path, "--perito"):
            arg_nome = "--perito"
        elif script_aceita_argumento(script_path, "--nome"):
            arg_nome = "--nome"
        else:
            print(f"Pulando {script} (não aceita --perito/--nome)")
            continue

        # monta comando: start, end, nome, sempre --export-md
        cmd = [
            sys.executable, script_path,
            "--start", start,
            "--end",   end,
            arg_nome,  perito,
            "--export-md"
        ]

        # se o script aceitar PNG, força --export-png
        if script_aceita_argumento(script_path, "--export-png"):
            cmd.append("--export-png")

        # se você optou pelos comentários e o script suportar, passa --export-comment
        if add_comments and script_aceita_argumento(script_path, "--export-comment"):
            cmd.append("--export-comment")

        print("EXEC:", " ".join(cmd))
        subprocess.run(cmd)

        # coleta os resultados
        lista_pngs.append(png_out if os.path.exists(png_out) else None)
        lista_comentarios.append(
            open(md_out, encoding="utf-8").read()
            if add_comments and os.path.exists(md_out)
            else ""
        )
        lista_tables.append(csv_out if os.path.exists(csv_out) else None)

    return lista_pngs, lista_comentarios, lista_tables

def gerar_apendice_nc(perito, start, end):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT protocolo, motivoNaoConformado
          FROM analises a
          JOIN peritos p ON a.siapePerito = p.siapePerito
         WHERE p.nomePerito = ? AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
         ORDER BY protocolo
    """, conn, params=(perito, start, end))
    conn.close()
    return df

def inserir_tabela_pdf(pdf, table_path):
    # tenta ler CSV ou XLSX:
    try:
        if table_path.lower().endswith(".csv"):
            df = pd.read_csv(table_path, encoding='utf-8', errors='replace')
        else:
            df = pd.read_excel(table_path)
    except Exception:
        return  # pula se falhar

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
    # Linhas
    for _, row in df.iterrows():
        for item in row:
            pdf.cell(col_width, row_height, str(item), border=1)
        pdf.ln(row_height)

def gerar_pdf_final(peritos_df, start, end, add_comments):
    # prepara capa
    capa = os.path.join(MISC_DIR, "capa.pdf")
    merger = PdfMerger() if os.path.exists(capa) else PdfMerger()
    if os.path.exists(capa):
        merger.append(capa)

    for _, row in peritos_df.iterrows():
        perito = row['nomePerito']
        safe   = perito.replace(" ", "_")
        print("Gerando relatório para perito:", perito)

        pngs, cmts, tbls = gerar_graficos_e_tabelas(perito, start, end, add_comments)
        apdf = gerar_apendice_nc(perito, start, end)

        tmp_pdf = FPDF()
        tmp_pdf.set_auto_page_break(auto=True, margin=15)
        tmp_pdf.add_page()
        tmp_pdf.set_font("Arial", "B", 16)
        tmp_pdf.cell(0, 20, f"Relatório Consolidado: {perito}", ln=True, align="C")
        tmp_pdf.set_font("Arial", "", 12)
        tmp_pdf.ln(10)
        tmp_pdf.cell(0, 10, f"Período: {start} a {end}", ln=True)
        tmp_pdf.ln(10)

        # insere gráficos e comentários
        for i, img in enumerate(pngs):
            if not img:
                continue
            tmp_pdf.add_page()
            tmp_pdf.set_font("Arial", "B", 13)
            tmp_pdf.cell(0, 10, f"Gráfico {i+1}", ln=True)
            try:
                tmp_pdf.image(img, x=15, y=25, w=180)
                tmp_pdf.ln(95)
            except Exception:
                tmp_pdf.cell(0, 10, "Erro ao carregar imagem", ln=True)
            if cmts[i]:
                tmp_pdf.set_font("Arial", "I", 10)
                tmp_pdf.ln(10)
                tmp_pdf.multi_cell(0, 8, cmts[i])

        # insere tabelas
        for tbl in tbls:
            if tbl:
                inserir_tabela_pdf(tmp_pdf, tbl)

        # apêndice
        tmp_pdf.add_page()
        tmp_pdf.set_font("Arial", "B", 12)
        tmp_pdf.cell(0, 10, "Apêndice: Protocolos NC", ln=True)
        tmp_pdf.set_font("Arial", "", 10)
        if not apdf.empty:
            for _, r in apdf.iterrows():
                tmp_pdf.cell(0, 8,
                    f"Protocolo: {r['protocolo']} - Motivo: {r['motivoNaoConformado']}",
                    ln=True)
        else:
            tmp_pdf.cell(0, 10, "Nenhum protocolo com NC encontrado.", ln=True)

        # salva e mescla
        tmp_path = os.path.join(OUTPUTS_DIR, f"tmp_{safe}.pdf")
        tmp_pdf.output(tmp_path)
        merger.append(tmp_path)

    # grava o PDF final
    out_pdf = os.path.join(OUTPUTS_DIR, f"relatorio_dez_piores_{start}_a_{end}.pdf")
    merger.write(out_pdf)
    merger.close()
    print("✅ Relatório final salvo em:", out_pdf)
    return out_pdf

def main():
    args = parse_args()
    # DEBUG: mostra todos os argumentos recebidos
    print("=== ARGS DEBUG:", vars(args))

    # se não pediu para exportar em PDF, sai sem fazer nada
    if not args.export_pdf:
        print("Nada a fazer. Use --export-pdf para gerar relatório.")
        return

    # add_comments virá do TUI (via --add-comments) ou do CLI
    add_comments = args.add_comments

    # 1. Busca os 10 piores peritos (já filtra >=50 análises)
    peritos_df = pegar_10_piores_peritos(args.start, args.end)
    if peritos_df.empty:
        print("Nenhum perito encontrado com pelo menos 50 análises no período.")
        return

    # 2. Gera o PDF final (capa + um sub-relatório por perito)
    gerar_pdf_final(peritos_df, args.start, args.end, add_comments)

if __name__ == '__main__':
    main()

