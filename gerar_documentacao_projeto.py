import os
import re
from datetime import datetime
import dotenv

TIPOS_ARQUIVOS = (".py", ".r", ".toml", ".md")
MAX_CARACTERES = 12000
MAX_TOKENS = 2000

PROJETO_NOME = os.path.basename(os.path.abspath("."))

try:
    import openai
    from packaging import version
    openai_version = version.parse(openai.__version__)
except ImportError:
    openai = None
    openai_version = None

if os.path.exists(".env"):
    env = dotenv.dotenv_values(".env")
    openai_key = env.get("OPENAI_API_KEY")
else:
    openai_key = None

def coletar_codigo_do_projeto(raiz="."):
    codigos = []
    for root, dirs, files in os.walk(raiz):
        if any(x in root for x in ["logs", "debug_logs", "__pycache__", ".venv", ".git"]):
            continue
        for fname in files:
            if fname.lower().endswith(TIPOS_ARQUIVOS):
                caminho = os.path.join(root, fname)
                try:
                    with open(caminho, encoding="utf-8", errors="ignore") as f:
                        trecho = f.read(MAX_CARACTERES)
                        codigos.append(f"\n### Arquivo: {os.path.relpath(caminho)}\n{trecho}")
                except Exception:
                    pass
    return "\n".join(codigos)[:MAX_CARACTERES * 10]

def limpar_bloco_org(texto):
    linhas = texto.strip().splitlines()
    if linhas and linhas[0].strip().startswith("```org"):
        linhas = linhas[1:]
    if linhas and linhas[-1].strip() == "```":
        linhas = linhas[:-1]
    return "\n".join(linhas).strip()

def formatar_nomes_arquivos(texto):
    # Regex para nomes de arquivos comuns
    regex = r'(?<![=\w])([\w\-/]+?\.(py|r|toml|md|json|txt|csv|yml|yaml))(?![\w=])'
    def repl(match):
        arquivo = match.group(1)
        # Não aplica se já está entre sinais de igual
        antes = texto[max(0, match.start()-1)]
        depois = texto[min(len(texto)-1, match.end())]
        if antes == '=' or depois == '=':
            return arquivo
        return f'={arquivo}='
    return re.sub(regex, repl, texto)

def gerar_relatorio_ia(codigo):
    if not (openai and openai_key):
        print("OpenAI/Chave não disponível. Gere um .env com sua OPENAI_API_KEY.")
        return "ERRO: IA não disponível"
    prompt = (
        f"Você é um especialista em documentação institucional de sistemas públicos de automação. "
        f"Esta iniciativa foi do Coordenador-Geral de Assuntos Corporativos e Disseminação de Conhecimento, Gustavo Magalhães Mendes de Tarso, sendo este o único desenvolvedor. "
        "Nunca use a expressão 'perícia médica', 'pericial' ou similares. Sempre utilize 'análises de tarefas' ao descrever o funcionamento do sistema."
        "\n\nGere um relatório institucional discursivo, estruturado por seções e subtítulos org-mode (use títulos como * Introdução, * Contexto do Problema, * Soluções Desenvolvidas, * Indicadores Estratégicos, * Importância para a Gestão Estratégica, * Importância do Uso de Inteligência Artificial, * Impactos Institucionais, * Conclusão), mas sem tópicos ou bullets, apenas texto corrido e explicativo em cada seção."
        "\n\nO relatório deve abordar:"
        "\n- O desafio real enfrentado: a extração dos dados do Portal PMF em CSVs complexos, dificultando a filtragem de profissionais conforme KPIs."
        "\n- As soluções desenvolvidas para automação da filtragem, cálculo dos indicadores e geração de relatórios."
        "\n- Uma explicação discursiva dos KPIs (ICRA, IATD, Score Final), sua função estratégica e seu impacto para a gestão."
        "\n- O impacto institucional e social: melhoria para a população (serviço mais justo, célere e de qualidade) e economia para o governo."
        "\n- O papel exclusivo de Gustavo Magalhães Mendes de Tarso em toda a concepção e desenvolvimento."
        "\n- Inclua obrigatoriamente um parágrafo explicando de forma clara a importância deste sistema para a gestão estratégica, enfatizando como ele permite o monitoramento em tempo real, apoio à tomada de decisão e transformação de grandes volumes de dados em indicadores objetivos e acionáveis."
        "\n- Inclua obrigatoriamente um parágrafo explicando e enfatizando o uso de inteligência artificial (IA) no sistema, ressaltando os benefícios em automação de análises, geração de relatórios institucionais e inovação na administração pública."
        "\n- NUNCA escreva tópicos, bullets ou listas. Use apenas texto corrido dentro de cada seção."
        "\n- NUNCA coloque um título principal (o script já coloca). Use apenas os subtítulos/seções org-mode."
        "\n- NUNCA coloque texto em bloco de código markdown (não use ```org nem ```)."
        "\n- O nome do sistema é sempre o nome da pasta raiz do projeto."
        "\n- Sempre que mencionar nomes de arquivos (com extensão .py, .csv, .md, .json etc.), escreva-os entre sinais de igual, como =gen_qa_chatgptfromtxt.py=, para garantir a formatação correta na exportação LaTeX/Org-mode."
        "\n\nSegue o código a ser analisado:"
        f"\n\n{codigo}"
        "\n\n---\nRelatório:"
    )
    try:
        if openai_version and openai_version >= version.parse("1.0.0"):
            client = openai.OpenAI(api_key=openai_key)
            resposta = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.13,
                max_tokens=MAX_TOKENS,
            )
            texto = resposta.choices[0].message.content.strip()
            return limpar_bloco_org(texto)
        else:
            openai.api_key = openai_key
            resposta = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.13,
                max_tokens=MAX_TOKENS,
            )
            texto = resposta.choices[0].message.content.strip()
            return limpar_bloco_org(texto)
    except Exception as e:
        print(f"Erro IA: {e}")
        return f"Falha IA: {e}"

def salvar_relatorio_org(texto, nome_projeto):
    os.makedirs("docs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    caminho = f"docs/Relatorio_{nome_projeto}_IA_{timestamp}.org"
    with open(caminho, "w", encoding="utf-8") as f:
        f.write('#+INCLUDE: "/home/gustavodetarso/Documentos/.share/header_mps_org/header_mps.org"\n\n')
        f.write(f'*RELATÓRIO INSTITUCIONAL – SISTEMA {nome_projeto}*\n\n')
        f.write(texto)
    print(f"\nRelatório final gerado em: {caminho}")

if __name__ == "__main__":
    print(f"==> Lendo e sumarizando o código do projeto '{PROJETO_NOME}' ...")
    codigos = coletar_codigo_do_projeto(".")
    print("==> Enviando para IA... (pode demorar alguns segundos/minutos)")
    relatorio_ia = gerar_relatorio_ia(codigos)
    # Pós-processamento para garantir nomes de arquivos corretamente formatados
    relatorio_ia = formatar_nomes_arquivos(relatorio_ia)
    print("==> Salvando relatório final em Org-mode...")
    salvar_relatorio_org(relatorio_ia, PROJETO_NOME)

