#!/usr/bin/env python3
"""
Módulo de funções de comentário para scripts ATESTMED.
Primeiro parágrafo: descreve só os dados/indicador.
Segundo: sempre reforça que a ausência/presença em um KPI não descarta risco.
Agora robusto para casos em que perito_valor ou similares são None.
"""

from dotenv import load_dotenv
load_dotenv()
import os
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")

BLOCO_CONCLUSAO = (
    "É importante ressaltar que o enquadramento ou não de um perito em um determinado critério de risco "
    "não é suficiente para afastar ou confirmar risco de atuação inadequada. A avaliação de risco deve "
    "sempre considerar o contexto completo e a análise integrada de todos os indicadores relevantes do "
    "período, garantindo monitoramento contínuo para todos os profissionais."
)

def comentar_compare_30s(
    tabela_md: str, chart_ascii: str, start: str, end: str, threshold: int,
    perito_nome: str = None, perito_valor: float = None
) -> str:
    valor_str = (
        f"{int(perito_valor)}" if perito_valor is not None
        else "não disponível"
    )
    primeira_parte = f"""
Analise os dados referentes à quantidade de perícias com duração igual ou inferior a {threshold} segundos, realizadas entre {start} e {end}.

Tabela:
{tabela_md}

Gráfico ASCII:
{chart_ascii}

No indicador de duração ≤ {threshold}s, o perito {perito_nome or '[não informado]'} realizou {valor_str} análises no período considerado.
Esse KPI visa identificar potenciais automatizações, execuções mecânicas ou julgamentos sumários. Altos valores para esse indicador sinalizam risco, mas a ausência não elimina a necessidade de monitoramento.
""".strip()
    segunda_parte = BLOCO_CONCLUSAO

    prompt = f"""
Você é um auditor técnico do Ministério da Previdência Social, especialista em riscos do ATESTMED.

{primeira_parte}

{segunda_parte}

Responda sempre em texto corrido, **sem listas, tópicos ou enumerações**.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um auditor de integridade do ATESTMED."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.4, max_tokens=1000
    )
    return resp.choices[0].message.content.strip()

def comentar_nc100(
    tabela_md: str, start: str, end: str,
    perito_nome: str = None, perito_valor: float = None
) -> str:
    primeira_parte = f"""
A tabela abaixo apresenta a relação de peritos que registraram 100% de não conformidade no período de {start} a {end}.

{tabela_md}

Esse resultado significa que todas as análises realizadas pelo(s) perito(s) foram consideradas não conformes. Trata-se de uma situação de risco máximo, pois pode indicar total desvio do protocolo técnico, fraude ou desconhecimento das normas institucionais.
""".strip()
    segunda_parte = BLOCO_CONCLUSAO

    prompt = f"""
Você é um auditor do Ministério da Previdência Social, especializado em integridade e controle do ATESTMED.

{primeira_parte}

{segunda_parte}

Responda sempre em texto corrido, **sem listas, tópicos ou enumerações**.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um auditor institucional do ATESTMED."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.4, max_tokens=1000
    )
    return resp.choices[0].message.content.strip()

def comentar_produtividade(
    tabela_md: str, chart_ascii: str, start: str, end: str, threshold: int,
    perito_nome: str = None, perito_valor: float = None
) -> str:
    if perito_valor is None:
        valor_str = "não disponível"
    else:
        valor_str = f"{perito_valor:.2f}"

    primeira_parte = f"""
Abaixo está a análise objetiva da produtividade do perito {perito_nome or '[não informado]'} em {start} a {end}.

Tabela:
{tabela_md}

Gráfico ASCII:
{chart_ascii}

O KPI de produtividade utiliza o limiar de {threshold} análises/hora para indicar risco de execução mecânica, automatização ou revezamento de credenciais. O valor observado para o perito foi de {valor_str} análises/hora, o que deve ser comparado com esse referencial. A superação do limiar sugere potencial risco, mas estar abaixo não exclui a necessidade de monitoramento.
""".strip()
    segunda_parte = BLOCO_CONCLUSAO

    prompt = f"""
Você é um auditor técnico do Ministério da Previdência Social, especialista em detecção de riscos operacionais do ATESTMED.

{primeira_parte}

{segunda_parte}

Responda sempre em texto corrido, **sem listas, tópicos ou enumerações**.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um auditor técnico especializado em riscos do ATESTMED."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.4, max_tokens=1000
    )
    return resp.choices[0].message.content.strip()

def comentar_overlap(
    tabela_md: str, chart_ascii: str, start: str, end: str,
    perito_nome: str = None, perito_valor: float = None
) -> str:
    valor_str = (
        f"{perito_valor:.2f}%" if perito_valor is not None else "não disponível"
    )
    primeira_parte = f"""
A análise a seguir detalha a ocorrência de sobreposição de protocolos de perícia para o perito {perito_nome or '[não informado]'}, no período de {start} a {end}.

Tabela:
{tabela_md}

Gráfico ASCII:
{chart_ascii}

O KPI de sobreposição de protocolos identifica execução paralela tecnicamente inviável, sinalizando possível fraude ou revezamento de credenciais. O percentual de sobreposição observado foi {valor_str}. Mesmo a ausência de sobreposição não elimina a necessidade de revisão da atuação do perito.
""".strip()
    segunda_parte = BLOCO_CONCLUSAO

    prompt = f"""
Você é um auditor de integridade operacional do Ministério da Previdência Social.

{primeira_parte}

{segunda_parte}

Responda sempre em texto corrido, **sem listas, tópicos ou enumerações**.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um auditor de processos do ATESTMED."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.4, max_tokens=1000
    )
    return resp.choices[0].message.content.strip()

def comentar_rank_score(
    result_md: str, chart_ascii: str, start: str, end: str,
    perito_nome: str = None, perito_valor: float = None
) -> str:
    valor_str = (
        f"{perito_valor:.2f}" if perito_valor is not None else "não disponível"
    )
    primeira_parte = f"""
O ranking abaixo mostra os scores finais dos peritos no período de {start} a {end}.

Tabela:
{result_md}

Gráfico ASCII:
{chart_ascii}

Score final elevado representa maior acúmulo de fatores de risco (produtividade, sobreposição, NC alta, etc). O score observado foi {valor_str}. Os peritos com maior score são priorizados para auditoria, mas a ausência nesse grupo não exclui monitoramento.
""".strip()
    segunda_parte = BLOCO_CONCLUSAO

    prompt = f"""
Você é um auditor institucional do Ministério da Previdência Social, especialista em integridade do ATESTMED.

{primeira_parte}

{segunda_parte}

Responda sempre em texto corrido, **sem listas, tópicos ou enumerações**.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um auditor institucional do ATESTMED."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.4, max_tokens=1000
    )
    return resp.choices[0].message.content.strip()

def comentar_rank_cr(
    result_md: str, chart_ascii: str, start: str, end: str,
    cr_nome: str = None, cr_valor: float = None
) -> str:
    valor_str = (
        f"{cr_valor:.2f}" if cr_valor is not None else "não disponível"
    )
    primeira_parte = f"""
Segue a análise do score médio das CRs entre {start} e {end}.

Tabela:
{result_md}

Gráfico ASCII:
{chart_ascii}

Scores médios elevados para determinada CR sinalizam maior risco institucional e necessidade de intervenção, mas todas as CRs devem ser acompanhadas. O score médio observado foi {valor_str}.
""".strip()
    segunda_parte = BLOCO_CONCLUSAO

    prompt = f"""
Você é um gestor estratégico do Ministério da Previdência Social, especialista em integridade do ATESTMED.

{primeira_parte}

{segunda_parte}

Responda sempre em texto corrido, **sem listas, tópicos ou enumerações**.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um gestor de operações públicas do ATESTMED."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.4, max_tokens=1000
    )
    return resp.choices[0].message.content.strip()

def comentar_icra(
    conteudo_md: str, nome: str, start: str, end: str,
    any_criterio_acionado: bool = True
) -> str:
    status = "Algum critério de risco foi acionado neste período." if any_criterio_acionado else "Nenhum critério de risco foi acionado neste período."
    primeira_parte = f"""
Segue o detalhamento completo do ICRA para o perito {nome}, no período de {start} a {end}.

{conteudo_md}

{status} O ICRA reúne critérios que, quando acionados, sinalizam risco elevado (produtividade alta, curtas, NC alta, sobreposição). Mesmo se nenhum critério for acionado, é recomendada revisão periódica e acompanhamento institucional.
""".strip()
    segunda_parte = BLOCO_CONCLUSAO

    prompt = f"""
Você é um auditor do ATESTMED responsável por avaliação técnica individualizada de peritos.

{primeira_parte}

{segunda_parte}

Responda sempre em texto corrido, **sem listas, tópicos ou enumerações**.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um auditor técnico do ATESTMED."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.4, max_tokens=1000
    )
    return resp.choices[0].message.content.strip()

