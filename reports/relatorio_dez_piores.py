#!/usr/bin/env python3
import os
import sys
import subprocess
import sqlite3
import pandas as pd
from fpdf import FPDF
from PyPDF2 import PdfMerger

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH     = os.path.join(BASE_DIR, 'db', 'atestmed.db')
GRAPHS_DIR  = os.path.join(BASE_DIR, 'graphs_and_tables')
EXPORT_DIR  = os.path.join(GRAPHS_DIR, 'exports')
OUTPUTS_DIR = os.path.join(BASE_DIR, 'reports', 'outputs')
MISC_DIR    = os.path.join(BASE_DIR, 'misc')

os.makedirs(EXPORT_DIR,  exist_ok=True)
os.makedirs(OUTPUTS_DIR, exist_ok=True)

# ——— Mapa de padrões de nome de arquivo por script —————————
MAPA_ARQUIVOS = {
    'compare_nc_rate': {
        'png':     'compare_nc_rate_{perito}.png',
        'md':      'compare_nc_rate_{perito}.md',
        'comment': 'compare_nc_rate_{perito}_comment.md',
    },
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
    
}

def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Relatório dos 10 piores peritos no período")
    p.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    p.add_argument('--end',   required=True, help='Data final   YYYY-MM-DD')
    p.add_argument('--export-pdf', action='store_true', help='Exporta relatório em PDF')
    p.add_argument('--add-comments', action='store_true', help='Inclui comentários GPT nos gráficos')
    return p.parse_args()

def pegar_10_piores_peritos(start, end, min_analises=50):
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT
        p.nomePerito,
        i.scoreFinal,
        COUNT(a.protocolo) AS total_analises
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

def get_summary_stats(perito, start, end):
    """
    Retorna (total_analises, pct_nc, cr, dr) para aquele perito no intervalo.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # busca CR e DR
    cur.execute("""
        SELECT cr, dr
          FROM peritos
         WHERE nomePerito = ?
    """, (perito,))
    row = cur.fetchone()
    cr, dr = (row if row else ("-", "-"))

    # conta total e não-conformados
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN motivoNaoConformado != 0 THEN 1 ELSE 0 END) AS nc_count
        FROM analises a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        WHERE p.nomePerito = ?
          AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
    """, (perito, start, end))
    total, nc_count = cur.fetchone()
    conn.close()

    pct_nc = (nc_count or 0) / (total or 1) * 100
    return total or 0, pct_nc, cr, dr
    
def gerar_graficos_e_tabelas(perito, start, end, add_comments):
    """
    Para cada entrada em MAPA_ARQUIVOS (sem Rank CR Score):
      - monta o nome de saída usando o padrão do mapa
      - executa o script correspondente
      - retorna as listas de caminhos (pngs, md-comments, csvs/xlsx)
    """
    lista_pngs       = []
    lista_comentarios = []
    lista_tables     = []

    safe = perito.replace(" ", "_")

    # iteramos apenas sobre os scripts que estão em MAPA_ARQUIVOS
    for base, info in MAPA_ARQUIVOS.items():
        script = f"{base}.py"
        script_path = os.path.join(GRAPHS_DIR, script)
        if not os.path.isfile(script_path):
            # ignora se não existir o arquivo
            continue

        # caminhos de saída (formatados com perito/start/end)
        png_pattern = info.get('png')
        md_pattern  = info.get('md')
        csv_pattern = info.get('csv') or info.get('xlsx')

        png_out = png_pattern and os.path.join(
            EXPORT_DIR, png_pattern.format(perito=safe, start=start, end=end)
        )
        md_out = md_pattern and os.path.join(
            EXPORT_DIR, md_pattern.format(perito=safe, start=start, end=end)
        )
        table_out = csv_pattern and os.path.join(
            EXPORT_DIR, csv_pattern.format(perito=safe, start=start, end=end)
        )

        # monta comando
        cmd = [
            sys.executable, script_path,
            "--start", start,
            "--end",   end
        ]
        # nome do perito
        if script_aceita_argumento(script_path, "--perito"):
            cmd += ["--perito", perito]
        elif script_aceita_argumento(script_path, "--nome"):
            cmd += ["--nome", perito]

        # flags de export
        if script_aceita_argumento(script_path, "--export-md"):
            cmd.append("--export-md")
        if script_aceita_argumento(script_path, "--export-png"):
            cmd.append("--export-png")
        if add_comments and script_aceita_argumento(script_path, "--export-comment"):
            cmd.append("--export-comment")

        print("EXEC:", " ".join(cmd))
        subprocess.run(cmd, check=False)

        # armazena apenas os arquivos que de fato existirem
        lista_pngs.append(
            png_out       if (png_out       and os.path.exists(png_out))       else None
        )
        lista_comentarios.append(
            open(md_out, encoding="utf-8").read()
            if (add_comments and md_out and os.path.exists(md_out)) else ""
        )
        lista_tables.append(
            table_out if (table_out and os.path.exists(table_out)) else None
        )

    return lista_pngs, lista_comentarios, lista_tables

def gerar_apendice_nc(perito, start, end):
    """
    Busca apenas os protocolos não-conformados e retorna DataFrame com
    protocolo + texto de motivo (protocolos.motivo).
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT
            a.protocolo,
            pr.motivo AS motivo_text
        FROM analises a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        JOIN protocolos pr ON a.protocolo = pr.protocolo
        WHERE p.nomePerito = ?
          AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
          AND a.motivoNaoConformado != 0
        ORDER BY a.protocolo
    """, conn, params=(perito, start, end))
    conn.close()
    return df

def inserir_tabela_pdf(pdf, table_path):
    try:
        if table_path.lower().endswith(".csv"):
            df = pd.read_csv(table_path, encoding='utf-8', errors='replace')
        else:
            df = pd.read_excel(table_path)
    except Exception:
        return

    pdf.add_page()
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, f"Tabela: {os.path.basename(table_path)}", ln=True, align="C")
    pdf.set_font("Arial", "", 10)

    col_width  = pdf.w / (len(df.columns) + 1)
    row_height = pdf.font_size + 2

    # cabeçalho
    for col in df.columns:
        pdf.cell(col_width, row_height, str(col), border=1)
    pdf.ln(row_height)

    # linhas
    for _, row in df.iterrows():
        for item in row:
            pdf.cell(col_width, row_height, str(item), border=1)
        pdf.ln(row_height)

def gerar_pdf_final(peritos_df, start, end, add_comments):
    # capa
    capa   = os.path.join(MISC_DIR, "capa.pdf")
    merger = PdfMerger()
    if os.path.exists(capa):
        merger.append(capa)

    for _, row in peritos_df.iterrows():
        perito = row['nomePerito']
        safe   = perito.replace(" ", "_")

        pngs, cmts, tbls = gerar_graficos_e_tabelas(perito, start, end, add_comments)
        apdf             = gerar_apendice_nc(perito, start, end)

        # resumo de estatísticas
        total, pct_nc, cr, dr = get_summary_stats(perito, start, end)

        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()

        # título
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 20, f"Relatório Consolidado: {perito}", ln=True, align="C")

        # período
        pdf.set_font("Arial", "", 12)
        pdf.cell(0, 10, f"Período: {start} a {end}", ln=True)

        # estatísticas resumo: total | % NC | CR | DR
        pdf.ln(5)
        pdf.set_font("Arial", "I", 11)
        pdf.cell(
            0, 8,
            f"Tarefas: {total} | % NC: {pct_nc:.1f}% | CR: {cr} | DR: {dr}",
            ln=True
        )
        pdf.ln(10)
        pdf.set_font("Arial", "", 12)

        # gráficos
        for i, img in enumerate(pngs):
            if not img:
                continue
            pdf.add_page()
            pdf.set_font("Arial", "B", 13)
            pdf.cell(0, 10, f"Gráfico {i+1}", ln=True)
            try:
                pdf.image(img, x=15, y=25, w=180)
                pdf.ln(95)
            except Exception:
                pdf.cell(0, 10, "Erro ao carregar imagem", ln=True)
            if cmts[i]:
                pdf.set_font("Arial", "I", 10)
                pdf.ln(10)
                pdf.multi_cell(0, 8, cmts[i])

        # tabelas
        for tbl in tbls:
            if tbl:
                inserir_tabela_pdf(pdf, tbl)

        # apêndice por motivo
        pdf.add_page()
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "Apêndice: Protocolos Não-Conformados por Motivo", ln=True)
        pdf.set_font("Arial", "", 10)
        if not apdf.empty:
            grouped = apdf.groupby('motivo_text')['protocolo'] \
                          .apply(lambda seq: ', '.join(map(str, seq))) \
                          .reset_index()
            for _, grp in grouped.iterrows():
                pdf.set_font("Arial", "B", 11)
                pdf.multi_cell(0, 8, grp['motivo_text'])
                pdf.set_font("Arial", "", 10)
                pdf.multi_cell(0, 8, grp['protocolo'])
                pdf.ln(5)
        else:
            pdf.cell(0, 10, "Nenhum protocolo não-conformado encontrado.", ln=True)

        tmp_path = os.path.join(OUTPUTS_DIR, f"tmp_{safe}.pdf")
        pdf.output(tmp_path)
        merger.append(tmp_path)

    out_pdf = os.path.join(OUTPUTS_DIR, f"relatorio_dez_piores_{start}_a_{end}.pdf")
    merger.write(out_pdf)
    merger.close()
    print("✅ Relatório final salvo em:", out_pdf)
    return out_pdf

def main():
    args = parse_args()
    if not args.export_pdf:
        print("Nada a fazer. Use --export-pdf para gerar relatório.")
        return

    peritos_df = pegar_10_piores_peritos(args.start, args.end)
    if peritos_df.empty:
        print("Nenhum perito encontrado com pelo menos 50 análises no período.")
        return

    gerar_pdf_final(peritos_df, args.start, args.end, args.add_comments)

if __name__ == '__main__':
    main()

