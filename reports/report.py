#!/usr/bin/env python3
import os
import sys
import subprocess
import sqlite3
import json
import shutil
import pandas as pd
from PyPDF2 import PdfMerger

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, 'db', 'atestmed.db')
GRAPHS_DIR = os.path.join(BASE_DIR, 'graphs_and_tables')
EXPORT_DIR = os.path.join(GRAPHS_DIR, 'exports')
OUTPUTS_DIR = os.path.join(BASE_DIR, 'reports', 'outputs')
MISC_DIR = os.path.join(BASE_DIR, 'misc')

os.makedirs(EXPORT_DIR, exist_ok=True)
os.makedirs(OUTPUTS_DIR, exist_ok=True)

# ——— Mapeamento dos arquivos de saída dos scripts de gráficos ———
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
    p = argparse.ArgumentParser(description="Relatório ATESTMED - Individual ou Top 10")
    p.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    p.add_argument('--end',   required=True, help='Data final   YYYY-MM-DD')
    p.add_argument('--perito', help='Nome do perito (relatório individual)')
    p.add_argument('--top10', action='store_true', help='Gera relatório para os 10 piores peritos do período')
    p.add_argument('--min-analises', type=int, default=50, help='Mínimo de análises para ser elegível ao Top 10')
    p.add_argument('--export-org', action='store_true', help='Exporta relatório consolidado em Org-mode')
    p.add_argument('--export-pdf', action='store_true', help='Exporta relatório consolidado em PDF (a partir do Org)')
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
    try:
        out = subprocess.run(
            [sys.executable, script_path, "--help"],
            capture_output=True, text=True
        )
        return argumento in out.stdout
    except Exception:
        return False

def gerar_graficos_e_tabelas(perito, start, end, add_comments, pdf_only=False):
    if not perito or not isinstance(perito, str) or not perito.strip():
        print(f"[ERRO] Nome do perito inválido! Valor recebido: {perito!r}")
        return [], [], []
    safe = perito.replace(" ", "_")
    for base, info in MAPA_ARQUIVOS.items():
        script = f"{base}.py"
        script_path = os.path.join(GRAPHS_DIR, script)
        if not os.path.isfile(script_path):
            print(f"[AVISO] Script não encontrado: {script_path}")
            continue
        cmd = [sys.executable, script_path, "--start", start, "--end", end]
        if script_aceita_argumento(script_path, "--perito"):
            cmd += ["--perito", perito]
        elif script_aceita_argumento(script_path, "--nome"):
            cmd += ["--nome", perito]
        if script_aceita_argumento(script_path, "--export-md"):
            cmd.append("--export-md")
        if script_aceita_argumento(script_path, "--export-png"):
            cmd.append("--export-png")
        if add_comments and script_aceita_argumento(script_path, "--export-comment"):
            cmd.append("--export-comment")
        print(f"[DEBUG] Comando montado: {' '.join(map(str, cmd))}")
        try:
            subprocess.run(cmd, check=False)
        except Exception as e:
            print(f"[ERRO] Falha ao rodar {script}: {e}")

def copiar_recursos_perito(perito, start, end):
    safe = perito.replace(" ", "_")
    for base, info in MAPA_ARQUIVOS.items():
        # Copia PNG
        png = info.get('png', '').format(perito=safe, start=start, end=end)
        src_png = os.path.join(EXPORT_DIR, png)
        dst_png = os.path.join(IMGS_DIR, png)
        if os.path.exists(src_png):
            shutil.copy2(src_png, dst_png)
            try:
                os.remove(src_png)
            except Exception as e:
                print(f"[AVISO] Não foi possível apagar PNG: {src_png} -> {e}")

        # Copia comentários (md e _comment.md)
        for md_key in ['md', 'comment']:
            if md_key in info:
                md_file = info[md_key].format(perito=safe, start=start, end=end)
                src_md = os.path.join(EXPORT_DIR, md_file)
                dst_md = os.path.join(COMMENTS_DIR, md_file)
                if os.path.exists(src_md):
                    shutil.copy2(src_md, dst_md)
                    try:
                        os.remove(src_md)
                    except Exception as e:
                        print(f"[AVISO] Não foi possível apagar MD: {src_md} -> {e}")

def get_summary_stats(perito, start, end):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT cr, dr
          FROM peritos
         WHERE nomePerito = ?
    """, (perito,))
    row = cur.fetchone()
    cr, dr = (row if row else ("-", "-"))

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

def perito_tem_dados(perito, start, end):
    conn = sqlite3.connect(DB_PATH)
    count = conn.execute("""
        SELECT COUNT(*) FROM analises a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        WHERE p.nomePerito = ? AND date(a.dataHoraIniPericia) BETWEEN ? AND ?
    """, (perito, start, end)).fetchone()[0]
    conn.close()
    return count > 0

def gerar_apendice_nc(perito, start, end):
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

def markdown_para_org(texto_md):
    import tempfile, subprocess
    with tempfile.NamedTemporaryFile("w+", suffix=".md", delete=False) as fmd:
        fmd.write(texto_md)
        fmd.flush()
        org_path = fmd.name.replace(".md", ".org")
        subprocess.run(["pandoc", fmd.name, "-t", "org", "-o", org_path])
        with open(org_path, encoding="utf-8") as forg:
            org_text = forg.read()
    return org_text

def gerar_org_perito(perito, start, end, add_comments=False, output_dir=None):
    safe = perito.replace(" ", "_")
    if output_dir is None:
        output_dir = OUTPUTS_DIR
    org_path = os.path.join(output_dir, f"{safe}.org")
    lines = []
    lines.append(f"** {perito}")
    total, pct_nc, cr, dr = get_summary_stats(perito, start, end)
    lines.append(f"- Tarefas: {total}")
    lines.append(f"- % NC: {pct_nc:.1f}")
    lines.append(f"- CR: {cr} | DR: {dr}")
    lines.append("")  # Linha em branco após cabeçalho

    for base, info in MAPA_ARQUIVOS.items():
        # Caminho do gráfico
        png = info['png'].format(perito=safe, start=start, end=end)
        dst_png = os.path.join(IMGS_DIR, png)
        # Caminho do comentário
        comment_path = info['comment'].format(perito=safe, start=start, end=end)
        comment_path = os.path.join(COMMENTS_DIR, comment_path)
        bloco_tem_grafico = False

        if os.path.exists(dst_png):
            rel_png_path = os.path.join("imgs", os.path.basename(dst_png))
            lines.append(f"#+CAPTION: Gráfico gerado ({base})")
            lines.append(f"[[file:{rel_png_path}]]")
            bloco_tem_grafico = True

        # Conversão automática do comentário de md para org
        if add_comments and os.path.exists(comment_path):
            if bloco_tem_grafico:
                lines.append("")  # Em branco entre gráfico e comentário
            with open(comment_path, encoding='utf-8') as f:
                comment_md = f.read().strip()
            comment_org = markdown_para_org(comment_md)
            # Remove linhas de título indesejadas (opcional)
            comment_org = "\n".join(
                line for line in comment_org.splitlines()
                if not line.strip().lower().startswith('#+title')
            ).strip()
            lines.append("#+BEGIN_QUOTE")
            lines.append(comment_org)
            lines.append("#+END_QUOTE")

        if bloco_tem_grafico or (add_comments and os.path.exists(comment_path)):
            # Sempre força nova página após cada par gráfico+comentário!
            lines.append("\n#+LATEX: \\newpage\n")

    # Apêndice NC (se houver)
    apdf = gerar_apendice_nc(perito, start, end)
    if not apdf.empty:
        lines.append(f"*** Apêndice: Protocolos Não-Conformados por Motivo")
        grouped = apdf.groupby('motivo_text')['protocolo'] \
                      .apply(lambda seq: ', '.join(map(str, seq))) \
                      .reset_index()
        for _, grp in grouped.iterrows():
            lines.append(f"- *{grp['motivo_text']}*: {grp['protocolo']}")
        lines.append("")

    with open(org_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"✅ Org individual salvo em: {org_path}")
    return org_path

def exportar_org_para_pdf(org_path, font="DejaVu Sans"):
    import shutil
    import os
    output_dir = os.path.dirname(org_path)
    org_name = os.path.basename(org_path)
    pdf_name = org_name.replace('.org', '.pdf')
    log_path = org_path + ".log"
    pandoc = shutil.which("pandoc")
    if not pandoc:
        print("❌ Pandoc não encontrado no PATH. Instale com: sudo apt install pandoc texlive-xetex")
        return None
    cmd = [
        "pandoc",
        org_name,
        "-o", pdf_name,
        "--pdf-engine=xelatex",
        "--variable", f"mainfont={font}",
        "--variable", "geometry:margin=2cm",
        "--highlight-style=zenburn"
    ]
    print(f"[Pandoc] Gerando PDF do Org: {' '.join(cmd)} (cwd={output_dir})")
    prev_cwd = os.getcwd()
    try:
        os.chdir(output_dir)
        with open(log_path, "w", encoding="utf-8") as flog:
            result = subprocess.run(cmd, stdout=flog, stderr=flog, text=True)
    finally:
        os.chdir(prev_cwd)
    pdf_path = os.path.join(output_dir, pdf_name)
    if result.returncode == 0 and os.path.exists(pdf_path):
        print(f"✅ PDF gerado a partir do Org: {pdf_path}")
    else:
        print(f"❌ Erro ao gerar PDF pelo Pandoc. Veja o log em: {log_path}")
    return pdf_path

def adicionar_capa_pdf(pdf_final_path):
    capa_path = os.path.join(MISC_DIR, "capa.pdf")
    if not os.path.exists(capa_path):
        print(f"[AVISO] Capa não encontrada: {capa_path}. Pulando adição de capa.")
        return
    if not os.path.exists(pdf_final_path):
        print(f"[ERRO] PDF base não encontrado: {pdf_final_path}. Não é possível adicionar capa.")
        return
    output_path = pdf_final_path.replace(".pdf", "_com_capa.pdf")
    merger = PdfMerger()
    try:
        merger.append(capa_path)
        merger.append(pdf_final_path)
        merger.write(output_path)
        merger.close()
        print(f"✅ Relatório final gerado com capa: {output_path}")
    except Exception as e:
        print(f"[ERRO] Falha ao adicionar capa ao PDF: {e}")

def perguntar_modo_interativo():
    print("\n=== GERAÇÃO DE RELATÓRIO ATESTMED ===")
    print("Escolha o modo de geração do relatório:")
    print("1. Top 10 piores peritos")
    print("2. Individual (por perito)")
    while True:
        escolha = input("Digite 1 para Top 10 ou 2 para Individual: ").strip()
        if escolha == "1":
            return {"top10": True}
        elif escolha == "2":
            nome = input("Digite o nome COMPLETO do perito: ").strip()
            if nome:
                return {"perito": nome}
            else:
                print("Nome não pode ser vazio. Tente novamente.")
        else:
            print("Opção inválida! Digite 1 ou 2.")

def main():
    args = parse_args()

    # HÍBRIDO: Pergunta se não foi passado nem top10 nem perito
    if not args.top10 and not args.perito:
        modo = perguntar_modo_interativo()
        if "top10" in modo:
            args.top10 = True
        if "perito" in modo:
            args.perito = modo["perito"]

    PERIODO_DIR = os.path.join(OUTPUTS_DIR, f"{args.start}_a_{args.end}")

    # Define subpasta adequada para top10 ou individual
    if args.top10:
        RELATORIO_DIR = os.path.join(PERIODO_DIR, "top10")
    elif args.perito:
        RELATORIO_DIR = os.path.join(PERIODO_DIR, "individual")
    else:
        print("É obrigatório informar --perito (individual) ou --top10")
        sys.exit(1)

    global IMGS_DIR, COMMENTS_DIR
    IMGS_DIR = os.path.join(RELATORIO_DIR, "imgs")
    COMMENTS_DIR = os.path.join(RELATORIO_DIR, "comments")

    os.makedirs(EXPORT_DIR, exist_ok=True)
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    os.makedirs(PERIODO_DIR, exist_ok=True)
    os.makedirs(RELATORIO_DIR, exist_ok=True)
    os.makedirs(IMGS_DIR, exist_ok=True)
    os.makedirs(COMMENTS_DIR, exist_ok=True)

    # Lista de peritos a processar
    if args.top10:
        peritos_df = pegar_10_piores_peritos(args.start, args.end, min_analises=args.min_analises)
        if peritos_df.empty:
            print("Nenhum perito encontrado com os critérios.")
            return
        lista_peritos = peritos_df['nomePerito'].tolist()
        print(f"Gerando para os 10 piores: {lista_peritos}")
    elif args.perito:
        lista_peritos = [args.perito]
    else:
        print("É obrigatório informar --perito (individual) ou --top10")
        sys.exit(1)

    # Geração para cada perito individual, agora com checagem de existência de dados
    org_paths = []
    for perito in lista_peritos:
        if not perito_tem_dados(perito, args.start, args.end):
            print(f"⚠️  Perito '{perito}' não possui análises no período! Pulando geração.")
            continue
        gerar_graficos_e_tabelas(perito, args.start, args.end, args.add_comments, pdf_only=False)
        copiar_recursos_perito(perito, args.start, args.end)
        org_path = gerar_org_perito(perito, args.start, args.end, args.add_comments, output_dir=RELATORIO_DIR)
        org_paths.append(org_path)

    if not org_paths:
        print("Nenhum relatório gerado (todos os peritos estão sem dados no período).")
        return

    # Se for top10 e export_org, faz consolidado (org de todos)
    if args.top10 and (args.export_org or args.export_pdf):
        org_final = os.path.join(RELATORIO_DIR, f"relatorio_dez_piores_{args.start}_a_{args.end}.org")
        lines = [f"* Relatório dos 10 piores peritos ({args.start} a {args.end})",
                 "  :PROPERTIES:",
                 f"  :DATA: {args.start} a {args.end}",
                 "  :END:",
                 ""]
        for org_path in org_paths:
            with open(org_path, encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    lines.append(content)
                    lines.append("#+LATEX: \\newpage\n")
        with open(org_final, "w", encoding="utf-8") as f:
            f.write("\n".join(lines).strip() + "\n")
        print(f"✅ Org consolidado salvo em: {org_final}")
        org_to_export = org_final
    else:
        # Individual
        org_to_export = org_paths[0]

    # Gera PDF, se solicitado
    if args.export_pdf:
        pdf_path = exportar_org_para_pdf(org_to_export, font="DejaVu Sans")
        if pdf_path and os.path.exists(pdf_path):
            print(f"✅ PDF gerado a partir do Org: {pdf_path}")
            adicionar_capa_pdf(pdf_path)
        else:
            print(f"[ERRO] Falha ao converter Org para PDF. Veja o log gerado.")

if __name__ == '__main__':
    main()

