#!/usr/bin/env python3
import os
import sys
import subprocess
import sqlite3
import json
import shutil
import pandas as pd
from PyPDF2 import PdfMerger

BASE_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH       = os.path.join(BASE_DIR, 'db', 'atestmed.db')
GRAPHS_DIR    = os.path.join(BASE_DIR, 'graphs_and_tables')
EXPORT_DIR    = os.path.join(GRAPHS_DIR, 'exports')
OUTPUTS_DIR   = os.path.join(BASE_DIR, 'reports', 'outputs')
MISC_DIR      = os.path.join(BASE_DIR, 'misc')

os.makedirs(EXPORT_DIR,    exist_ok=True)
os.makedirs(OUTPUTS_DIR,   exist_ok=True)

def ascii_clean(text):
    if not text:
        return text
    replace_map = {
        '≤': '<=',
        '≥': '>=',
        '–': '-',
        '—': '-',
        '•': '-',
        '“': '"',
        '”': '"',
        '’': "'",
        '‘': "'",
        '…': '...',
        '→': '->',
        '←': '<-',
        '°': ' deg ',
        '×': 'x',
        '®': '(R)',
        '™': '(TM)',
        '·': '-',
    }
    for k, v in replace_map.items():
        text = text.replace(k, v)
    return text.encode('latin-1', 'replace').decode('latin-1')

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
    p.add_argument('--export-pdf', action='store_true', help='Exporta relatório consolidado em PDF (a partir do Org)')
    p.add_argument('--export-org', action='store_true', help='Exporta relatório consolidado em Org-mode')
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

def prompt_arquivo_orgs(peritos_df, output_dir=None):
    if output_dir is None:
        output_dir = OUTPUTS_DIR

    nomes_esperados = set([row['nomePerito'] for _, row in peritos_df.iterrows()])

    orgs_na_pasta = [
        f for f in os.listdir(output_dir)
        if f.endswith('.org')
        and not f.startswith('relatorio_dez_piores')  # evita consolidado do período
        and not f.endswith('.org~')                   # ignora backups
    ]
    nomes_orgs = set(f[:-4].replace('_', ' ') for f in orgs_na_pasta)
    nomes_faltam = nomes_esperados - nomes_orgs

    if orgs_na_pasta and len(nomes_orgs & nomes_esperados) > 0:
        print(f"\n[RESUME] Existem arquivos .org individuais na pasta {output_dir} para estes peritos:\n  - " +
              "\n  - ".join(nomes_orgs & nomes_esperados))
        print(f"\nDeseja usar os arquivos já existentes e gerar apenas os que faltam? (s/N)")
        resp = input().strip().lower()
        if resp == 's':
            return nomes_faltam
        else:
            # Apaga todos os .org individuais (menos o consolidado do período)
            for f in orgs_na_pasta:
                try:
                    os.remove(os.path.join(output_dir, f))
                except Exception:
                    pass
            return nomes_esperados
    return nomes_esperados

def peritos_nc100_df(start, end, min_tarefas=50):
    conn = sqlite3.connect(DB_PATH)
    sql = '''
        SELECT p.nomePerito, p.siapePerito, p.cr, p.dr, a.motivoNaoConformado
          FROM peritos p
          JOIN analises a ON p.siapePerito = a.siapePerito
         WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
    '''
    df = pd.read_sql(sql, conn, params=(start, end))
    conn.close()
    if df.empty:
        return pd.DataFrame()
    grp = df.groupby(['nomePerito', 'siapePerito', 'cr', 'dr'])
    stats = grp.agg(
        total_tarefas = ('motivoNaoConformado','count'),
        nc_soma       = ('motivoNaoConformado','sum')
    ).reset_index()
    result = stats[(stats['nc_soma'] == stats['total_tarefas']) & (stats['total_tarefas'] >= min_tarefas)].copy()
    result = result[['nomePerito','siapePerito','cr','dr','total_tarefas']]
    result.columns = ['Nome', 'SIAPE', 'CR', 'DR', 'Total Tarefas']
    result = result.sort_values(['Total Tarefas', 'Nome'], ascending=[False, True])
    return result

def gerar_org_final(peritos_df, start, end, add_comments=False):
    org_path = os.path.join(OUTPUTS_DIR, f"relatorio_dez_piores_{start}_a_{end}.org")
    lines = []
    lines.append(f"* Relatório dos 10 piores peritos ({start} a {end})")
    lines.append("  :PROPERTIES:")
    lines.append(f"  :DATA: {start} a {end}")
    lines.append("  :END:")
    lines.append("")

    faltando = []
    for _, row in peritos_df.iterrows():
        safe = row['nomePerito'].replace(" ", "_")
        org_file = os.path.join(OUTPUTS_DIR, f"{safe}.org")
        if os.path.exists(org_file):
            with open(org_file, encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    lines.append(content)
                    lines.append("")
        else:
            print(f"[AVISO] Org do perito não encontrado: {org_file}")
            faltando.append(row['nomePerito'])

    # ========== BÔNUS: Peritos com 100% de Não Conformidade ==========
    bonus_df = peritos_nc100_df(start, end)
    lines.append("* BÔNUS: Peritos com 100% de Não Conformidade (mín. 50 tarefas)")
    lines.append("")
    if not bonus_df.empty:
        # Tabela org-mode
        lines.append("| Nome | SIAPE | CR | DR | Total Tarefas |")
        lines.append("|------+-------+----+----+---------------|")
        for _, row in bonus_df.iterrows():
            lines.append(f"| {row['Nome']} | {row['SIAPE']} | {row['CR']} | {row['DR']} | {row['Total Tarefas']} |")
        lines.append("")
        # Comentário GPT, se solicitado
        if add_comments:
            try:
                from utils.comentarios import comentar_nc100
                tabela_md = bonus_df.to_markdown(index=False)
                comentario = comentar_nc100(tabela_md, start, end)
                if comentario.strip():
                    lines.append("#+BEGIN_QUOTE")
                    lines.append(comentario.strip())
                    lines.append("#+END_QUOTE")
                    lines.append("")
            except Exception as e:
                print(f"[AVISO] Não foi possível gerar comentário bônus: {e}")
    else:
        lines.append("_Nenhum perito com 100% de não conformidade (mín. 50 tarefas) no período._")
        lines.append("")

    with open(org_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")
    print(f"✅ Org consolidado salvo em: {org_path}")
    if faltando:
        print("⚠️  ATENÇÃO: Não foi possível incluir os seguintes peritos (org ausente):")
        for nome in faltando:
            print(f"   - {nome}")
    return org_path

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

def gerar_org_final(peritos_df, start, end, org_path=None, output_dir=None):
    if output_dir is None:
        output_dir = OUTPUTS_DIR
    if org_path is None:
        org_path = os.path.join(output_dir, f"relatorio_dez_piores_{start}_a_{end}.org")
    lines = []
    lines.append(f"* Relatório dos 10 piores peritos ({start} a {end})")
    lines.append("  :PROPERTIES:")
    lines.append(f"  :DATA: {start} a {end}")
    lines.append("  :END:")
    lines.append("")

    faltando = []
    total = len(peritos_df)
    for idx, row in enumerate(peritos_df.itertuples()):
        safe = row.nomePerito.replace(" ", "_")
        org_file = os.path.join(output_dir, f"{safe}.org")
        if os.path.exists(org_file):
            with open(org_file, encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    lines.append(content)
                    lines.append("")
                    if idx < total - 1:
                        lines.append("#+LATEX: \\newpage")
                        lines.append("")
        else:
            print(f"[AVISO] Org do perito não encontrado: {org_file}")
            faltando.append(row.nomePerito)

    with open(org_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")
    print(f"✅ Org consolidado salvo em: {org_path}")
    if faltando:
        print("⚠️  ATENÇÃO: Não foi possível incluir os seguintes peritos (org ausente):")
        for nome in faltando:
            print(f"   - {nome}")
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
        # (Opcional) Se quiser sobrescrever o original, faça:
        # shutil.move(output_path, pdf_final_path)
    except Exception as e:
        print(f"[ERRO] Falha ao adicionar capa ao PDF: {e}")
        
def main():
    args = parse_args()

    # Diretório específico do ciclo/período
    PERIODO_DIR = os.path.join(OUTPUTS_DIR, f"{args.start}_a_{args.end}")
    global IMGS_DIR, COMMENTS_DIR
    IMGS_DIR = os.path.join(PERIODO_DIR, "imgs")
    COMMENTS_DIR = os.path.join(PERIODO_DIR, "comments")

    os.makedirs(EXPORT_DIR, exist_ok=True)
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    os.makedirs(PERIODO_DIR, exist_ok=True)
    os.makedirs(IMGS_DIR, exist_ok=True)
    os.makedirs(COMMENTS_DIR, exist_ok=True)

    peritos_df = pegar_10_piores_peritos(args.start, args.end)
    if peritos_df.empty:
        print("Nenhum perito encontrado com pelo menos 50 análises no período.")
        return

    nomes_a_gerar = prompt_arquivo_orgs(peritos_df, output_dir=PERIODO_DIR)  # <-- Corrigido aqui!

    print("🔄 Gerando todos os gráficos e arquivos necessários para Org/PDF (apenas os que faltam)...")
    for _, row in peritos_df.iterrows():
        if row['nomePerito'] in nomes_a_gerar:
            gerar_graficos_e_tabelas(row['nomePerito'], args.start, args.end, args.add_comments, pdf_only=False)
            copiar_recursos_perito(row['nomePerito'], args.start, args.end)
            gerar_org_perito(row['nomePerito'], args.start, args.end, args.add_comments, output_dir=PERIODO_DIR)
        else:
            print(f"[RESUME] .org de {row['nomePerito']} já existe, pulando geração.")

    # Salva JSON dos elegíveis no diretório do período
    json_path = os.path.join(PERIODO_DIR, f"relatorio_dez_piores_{args.start}_a_{args.end}_elegiveis.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump([row['nomePerito'] for _, row in peritos_df.iterrows()], f, ensure_ascii=False, indent=2)
    print(f"📝 Lista de elegíveis salva em: {json_path}")

    # Gera Org consolidado e PDF no diretório do período
    if args.export_org or args.export_pdf:
        org_path = os.path.join(PERIODO_DIR, f"relatorio_dez_piores_{args.start}_a_{args.end}.org")
        gerar_org_final(peritos_df, args.start, args.end, org_path=org_path, output_dir=PERIODO_DIR)
        if args.export_pdf:
            pdf_path = exportar_org_para_pdf(org_path, font="DejaVu Sans")
            if pdf_path and os.path.exists(pdf_path):
                print(f"✅ PDF gerado a partir do Org: {pdf_path}")
                adicionar_capa_pdf(pdf_path)
            else:
                print(f"[ERRO] Falha ao converter Org para PDF. Veja o log gerado.")

if __name__ == '__main__':
    main()

