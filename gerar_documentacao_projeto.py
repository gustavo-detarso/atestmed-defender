#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera um relatório institucional (Org-mode) usando IA com:
- Texto discursivo por seções (sem bullets), conforme diretrizes do projeto;
- Seção extra explicando, em parágrafos, o que cada arquivo usado por reports/make_report.py gera;
- Exportação automática do .org para PDF via Emacs (batch).

Uso:
  python gerar_documentacao_projeto.py
  DOC_STREAM=1 python gerar_documentacao_projeto.py      # imprime a resposta aos poucos (stream)
  DOC_NO_PDF=1 python gerar_documentacao_projeto.py      # NÃO exporta para PDF
"""

import os
import re
import shutil
import subprocess
import tempfile
from datetime import datetime
import dotenv

# ---------------- Configuração ----------------
TIPOS_ARQUIVOS = (".py", ".r", ".toml", ".md")
MAX_CARACTERES_POR_ARQ = 12000               # leitura por arquivo
MAX_TEXTO_PROJETO = MAX_CARACTERES_POR_ARQ * 10  # teto global do "corpus" enviado
MAX_TOKENS_SAIDA = 3500

PROJETO_NOME = os.path.basename(os.path.abspath("."))

# Carrega chave do .env ou do ambiente
if os.path.exists(".env"):
    env = dotenv.dotenv_values(".env")
    OPENAI_API_KEY = env.get("OPENAI_API_KEY")
else:
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

USE_STREAM = os.getenv("DOC_STREAM", "0") not in ("0", "", "false", "False")
NO_PDF = os.getenv("DOC_NO_PDF", "0") not in ("0", "", "false", "False")

# --------------- Utilidades FS ---------------

def ler_texto_caminho(caminho: str, limite: int = MAX_CARACTERES_POR_ARQ) -> str:
    try:
        with open(caminho, encoding="utf-8", errors="ignore") as f:
            return f.read(limite)
    except Exception:
        return ""

def coletar_codigo_do_projeto(raiz="."):
    """
    Lê um subconjunto do repositório para dar contexto geral à IA.
    Evita pastas volumosas/logs/venv/git.
    """
    blocos = []
    total = 0
    for root, dirs, files in os.walk(raiz):
        if any(x in root for x in ["logs", "debug_logs", "__pycache__", ".venv", ".git", "node_modules", "exports"]):
            continue
        for fname in files:
            if fname.lower().endswith(TIPOS_ARQUIVOS):
                caminho = os.path.join(root, fname)
                trecho = ler_texto_caminho(caminho, MAX_CARACTERES_POR_ARQ)
                if trecho:
                    bloco = f"\n### Arquivo: {os.path.relpath(caminho)}\n{trecho}"
                    blocos.append(bloco)
                    total += len(bloco)
                if total > MAX_TEXTO_PROJETO:
                    break
        if total > MAX_TEXTO_PROJETO:
            break
    return "\n".join(blocos)[:MAX_TEXTO_PROJETO]

def formatar_nomes_arquivos(texto: str) -> str:
    """
    Envolve nomes de arquivos comuns com =nome.ext= para compatibilidade Org/LaTeX.
    Evita duplicar se já estiver entre sinais de igual.
    """
    # protege os já formatados
    texto = re.sub(r'=(\w[\w\-/]+\.\w+)=', r'=\1=', texto)
    # formata os demais
    regex = r'(?<![=\w])([\w\-/]+?\.(py|r|toml|md|json|txt|csv|yml|yaml))(?![\w=])'
    return re.sub(regex, lambda m: f'={m.group(1)}=', texto)

# --------- Descobrir scripts usados por make_report ---------

def _achar_make_report_path():
    candidatos = ["reports/make_report.py", "report/make_report.py"]
    for c in candidatos:
        if os.path.exists(c):
            return c
    for root, _, files in os.walk("."):
        for f in files:
            if f == "make_report.py" and "reports" in root:
                return os.path.join(root, f)
    return None

def extrair_scripts_usados_pelo_make():
    """
    Varre 'reports/make_report.py' e retorna o conjunto de caminhos
    dos scripts realmente invocados pelo pipeline (python e R).
    """
    usados = set()
    make_path = _achar_make_report_path()
    if not make_path:
        return []

    texto = ler_texto_caminho(make_path, 200_000)

    # Padrões esperados nos comandos montados
    py_scripts = re.findall(r'graphs_and_tables/([A-Za-z0-9_\/\-]*?\.py)', texto)
    r_scripts  = re.findall(r'r_checks/([A-Za-z0-9_\-]*?\.R)', texto)

    for s in py_scripts:
        caminho = os.path.join("graphs_and_tables", s)
        if os.path.exists(caminho):
            usados.add(caminho)
    for s in r_scripts:
        caminho = os.path.join("r_checks", s)
        if os.path.exists(caminho):
            usados.add(caminho)

    # Fallback com a lista conhecida (apenas se nada encontrado)
    if not usados:
        conhecidos = [
            # Python (grupo e individual)
            "graphs_and_tables/compare_nc_rate.py",
            "graphs_and_tables/compare_fifteen_seconds.py",
            "graphs_and_tables/compare_overlap.py",
            "graphs_and_tables/compare_productivity.py",
            "graphs_and_tables/compare_indicadores_composto.py",
            "graphs_and_tables/compare_motivos_perito_vs_brasil.py",
            # R (grupo)
            "r_checks/g01_top10_nc_rate_check.R",
            "r_checks/g02_top10_le15s_check.R",
            "r_checks/g03_top10_productivity_check.R",
            "r_checks/g04_top10_overlap_check.R",
            "r_checks/g05_top10_motivos_chisq.R",
            "r_checks/g06_top10_composite_robustness.R",
            # R (individual)
            "r_checks/01_nc_rate_check.R",
            "r_checks/02_le15s_check.R",
            "r_checks/03_productivity_check.R",
            "r_checks/04_overlap_check.R",
            "r_checks/05_motivos_chisq.R",
            "r_checks/06_composite_robustness.R",
        ]
        for c in conhecidos:
            if os.path.exists(c):
                usados.add(c)

    return sorted(usados)

def coletar_codigo_especifico(arquivos):
    """
    Coleta o conteúdo (limitado) de arquivos específicos, preservando ordem.
    """
    blocos = []
    for caminho in arquivos:
        conteudo = ler_texto_caminho(caminho, MAX_CARACTERES_POR_ARQ)
        if conteudo:
            blocos.append(f"\n### Arquivo-alvo: {caminho}\n{conteudo}")
    return "\n".join(blocos)

# --------------- IA (OpenAI v1) ----------------

def _client_openai_v1():
    """
    Retorna cliente OpenAI v1. Falha com mensagem clara se faltar pacote/chave.
    """
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY não encontrado (.env ou variável de ambiente).")
    try:
        # pacote openai>=1.0
        from openai import OpenAI
        return OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        try:
            import openai
            if hasattr(openai, "OpenAI"):
                return openai.OpenAI(api_key=OPENAI_API_KEY)
        except Exception:
            pass
        raise RuntimeError(
            "Falha ao importar cliente OpenAI v1. Instale/atualize com: pip install -U openai\n"
            f"Detalhe: {e}"
        )

def _prompt_relatorio(corpus_projeto: str, lista_arquivos_usados: list, corpus_arquivos_alvo: str) -> str:
    """
    Monta um prompt pedindo:
    - relatório institucional (seções org-mode, sem bullets)
    - seção extra explicando o que cada arquivo usado por make_report gera.
    """
    lista_formatada = "\n".join(f"- {a}" for a in lista_arquivos_usados)

    return (
        "Você é um especialista em documentação institucional de sistemas públicos de automação.\n"
        "Esta iniciativa foi do Coordenador-Geral de Assuntos Corporativos e Disseminação de Conhecimento, "
        "Gustavo Magalhães Mendes de Tarso, sendo este o único desenvolvedor. "
        "Nunca use a expressão 'perícia médica', 'pericial' ou similares. "
        "Sempre utilize 'análises de tarefas' ao descrever o funcionamento do sistema.\n\n"
        "Gere um relatório institucional discursivo, estruturado por seções e subtítulos org-mode "
        "(use apenas subtítulos como * Introdução, * Contexto do Problema, * Soluções Desenvolvidas, "
        "* Indicadores Estratégicos, * Importância para a Gestão Estratégica, * Importância do Uso de Inteligência Artificial, "
        "* Impactos Institucionais, * Conclusão), com texto corrido em cada seção (NÃO use tópicos, bullets ou listas).\n\n"
        "Requisitos obrigatórios do relatório:\n"
        "- Explique o desafio real: a extração dos dados do Portal PMF em CSVs complexos, dificultando a filtragem de profissionais conforme KPIs.\n"
        "- Descreva as soluções para automação da filtragem, cálculo de indicadores e geração de relatórios.\n"
        "- Explique discursivamente os KPIs (ICRA, IATD, Score Final), sua função estratégica e impacto para a gestão.\n"
        "- Traga o impacto institucional e social, incluindo melhoria para a população e economia para o governo.\n"
        "- Deixe claro o papel exclusivo de Gustavo Magalhães Mendes de Tarso em toda a concepção e desenvolvimento.\n"
        "- Inclua um parágrafo claro sobre a importância deste sistema para a gestão estratégica (monitoramento em tempo real, suporte à decisão, transformação de dados em indicadores objetivos e acionáveis).\n"
        "- Inclua um parágrafo sobre o uso de inteligência artificial (IA) no sistema (automação de análises, geração de relatórios institucionais, inovação na administração pública).\n"
        "- O nome do sistema é exatamente o nome da pasta raiz do projeto.\n"
        "- Sempre que mencionar nomes de arquivos (.py, .csv, .md, .json etc.), escreva-os entre sinais de igual, como =gen_qa_chatgptfromtxt.py=.\n\n"
        "ADICIONALMENTE, inclua uma seção org-mode intitulada * Arquivos e Saídas Utilizados pelo Relatório, "
        "com um parágrafo para CADA arquivo listado abaixo (sem bullets), explicando o que ele gera (gráficos, tabelas, métricas), "
        "quais principais parâmetros são usados por =reports/make_report.py= (por exemplo =--start=, =--end=, =--top10=, =--perito=, =--threshold=, =--mode=), "
        "e que tipo de artefatos ele salva (PNG, MD, ORG). Liste exatamente estes arquivos, e somente estes:\n"
        f"{lista_formatada}\n\n"
        "— Código do projeto (amostra):\n"
        f"{corpus_projeto}\n\n"
        "— Código detalhado dos arquivos-alvo:\n"
        f"{corpus_arquivos_alvo}\n\n"
        "---\nRelatório:"
    )

def chamar_ia(prompt: str) -> str:
    client = _client_openai_v1()
    if USE_STREAM:
        print(">> streaming ligado (OpenAI v1):\n")
        stream = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.13,
            max_tokens=MAX_TOKENS_SAIDA,
            stream=True,
        )
        partes = []
        for chunk in stream:
            delta = getattr(chunk.choices[0].delta, "content", None)
            if delta:
                print(delta, end="", flush=True)
                partes.append(delta)
        print("\n")
        return "".join(partes).strip()
    else:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.13,
            max_tokens=MAX_TOKENS_SAIDA,
        )
        return resp.choices[0].message.content.strip()

# --------------- Pós-processamento ---------------

def limpar_bloco_org(texto: str) -> str:
    """
    Remove fence ```...``` caso a IA gere por engano.
    """
    linhas = texto.strip().splitlines()
    if linhas and linhas[0].strip().startswith("```org"):
        linhas = linhas[1:]
    if linhas and linhas[-1].strip() == "```":
        linhas = linhas[:-1]
    return "\n".join(linhas).strip()

def salvar_relatorio_org(texto: str, nome_projeto: str):
    os.makedirs("docs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    caminho = f"docs/Relatorio_{nome_projeto}_IA_{timestamp}.org"
    with open(caminho, "w", encoding="utf-8") as f:
        f.write('#+INCLUDE: "/home/gustavodetarso/Documentos/.share/header_mps_org/header_mps.org"\n\n')
        f.write(f'*RELATÓRIO INSTITUCIONAL – SISTEMA {nome_projeto}*\n\n')
        f.write(texto)
    print(f"\n✅ Relatório final gerado em: {caminho}")
    return caminho

# --------------- Exportação Org -> PDF com Emacs ---------------

def exportar_org_para_pdf_emacs(org_path: str) -> str:
    """
    Exporta um arquivo .org para PDF usando Emacs em modo batch.
    Requer: emacs (ou emacs-nox), LaTeX (texlive-xetex), Org mode (embutido).
    Retorna caminho do PDF esperado.
    """
    if not shutil.which("emacs"):
        raise RuntimeError("Emacs não encontrado no PATH. Instale 'emacs-nox' (ou 'emacs').")

    org_path_abs = os.path.abspath(org_path)
    pdf_path = os.path.splitext(org_path_abs)[0] + ".pdf"

    elisp = f"""
(require 'org)
(setq org-export-in-background nil)
(find-file "{org_path_abs}")
(org-latex-export-to-pdf)
"""

    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".el") as tf:
        tf.write(elisp)
        el_path = tf.name

    try:
        # -Q para não carregar init do usuário; --batch para modo não interativo
        subprocess.run(
            ["emacs", "-Q", "--batch", "-l", el_path],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        print("Saída do Emacs:\n", e.stdout)
        raise RuntimeError("Falha na exportação via Emacs. Veja a saída acima.") from e
    finally:
        try:
            os.remove(el_path)
        except Exception:
            pass

    if not os.path.exists(pdf_path):
        raise RuntimeError(f"Exportação concluída mas o PDF não foi encontrado: {pdf_path}")

    print(f"📄 PDF gerado: {pdf_path}")
    return pdf_path

# ---------------------- Main ----------------------

def main():
    print(f"==> Lendo código do projeto '{PROJETO_NOME}' ...")
    corpus_projeto = coletar_codigo_do_projeto(".")

    print("==> Descobrindo scripts usados por reports/make_report.py ...")
    arquivos_usados = extrair_scripts_usados_pelo_make()
    if not arquivos_usados:
        print("⚠️  Não consegui detectar scripts a partir de reports/make_report.py; seguirei só com o texto geral.")
    else:
        print("   Scripts alvo:")
        for a in arquivos_usados:
            print("   -", a)

    print("==> Coletando código específico dos arquivos-alvo ...")
    corpus_alvo = coletar_codigo_especifico(arquivos_usados)

    print("==> Preparando prompt para IA ...")
    prompt = _prompt_relatorio(corpus_projeto, arquivos_usados, corpus_alvo)

    print("==> Consultando IA... (pode demorar alguns segundos/minutos)")
    texto = chamar_ia(prompt)
    texto = limpar_bloco_org(texto)
    texto = formatar_nomes_arquivos(texto)

    print("==> Salvando relatório final em Org-mode...")
    org_path = salvar_relatorio_org(texto, PROJETO_NOME)

    if not NO_PDF:
        print("==> Exportando via Emacs para PDF...")
        exportar_org_para_pdf_emacs(org_path)
    else:
        print("ℹ️ DOC_NO_PDF=1: exportação para PDF desativada.")

if __name__ == "__main__":
    main()

