#!/usr/bin/env python3
"""
Módulo de funções de comentário para scripts ATESTMED.
Todos os comentários consideram: se o perito analisado NÃO atinge o limiar de risco,
o GPT instrui que mesmo fora do limiar deve-se monitorar/revisar toda a atuação.
"""
from dotenv import load_dotenv
load_dotenv()
import os
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")

def comentar_compare_30s(
    tabela_md: str, chart_ascii: str, start: str, end: str, threshold: int,
    perito_nome: str = None, perito_valor: float = None
) -> str:
    extra = ""
    if perito_nome and perito_valor is not None and perito_valor < threshold:
        extra = (
            f"\nObservação: O perito {perito_nome} NÃO atingiu o limiar de {threshold} análises com ≤{threshold}s. "
            "Apesar disso, recomenda-se monitorar e revisar regularmente a atuação de todos os peritos, "
            "incluindo aqueles fora do grupo de maior risco, conforme política de integridade do ATESTMED."
        )
    prompt = f"""
Você é um auditor técnico do Ministério da Previdência Social, especialista em riscos operacionais do ATESTMED.

Analise a tabela e o gráfico abaixo sobre perícias com duração ≤ {threshold}s entre {start} e {end}.

Tabela:
{tabela_md}

Gráfico ASCII:
{chart_ascii}

IMPORTANTE:
- O indicador 'análises ≤ {threshold}s' identifica potenciais automatizações, execuções mecânicas ou julgamentos sem análise devida.
- Realizar várias análises em poucos segundos é sempre um sinal de risco, nunca de eficiência.
- Frequência elevada de perícias extremamente curtas foge ao padrão técnico e pode indicar fraude ou uso indevido do sistema.
- Quanto mais análises ≤ {threshold}s, maior o risco de conduta inadequada.
{extra}

Comente tecnicamente, de modo objetivo e institucional:
- Destaque o(s) perito(s) em risco.
- Reforce que todos devem ser monitorados — inclusive aqueles fora do grupo de maior risco — para garantir integridade e qualidade.
Responda em texto corrido, **SEM usar listas ou tópicos** (nem numeradas, nem bullets). O comentário deve ser composto apenas por frases e parágrafos contínuos.
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
    extra = ""
    prompt = f"""
Você é um auditor do Ministério da Previdência Social, especializado em integridade e controle do ATESTMED.

Analise a lista de peritos com 100% de não conformidade no período de {start} a {end}:

{tabela_md}

INSTRUÇÕES:
- 100% de não conformidade representa desvio total do procedimento e risco máximo para o sistema.
- Indicador serve para detectar fraudes, atuação irregular ou desconhecimento dos protocolos.
- Todos os casos devem ser considerados para revisão integral e possível bloqueio, conforme política institucional.
Responda em texto corrido, **SEM usar listas ou tópicos** (nem numeradas, nem bullets). O comentário deve ser composto apenas por frases e parágrafos contínuos.
{extra}

Gere um comentário objetivo, orientando medidas imediatas de auditoria e reforçando que nenhum caso deve ser negligenciado, independentemente de outras métricas.
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
    extra = ""
    if perito_nome and perito_valor is not None:
        if perito_valor < threshold:
            extra = (
                f"\nObservação: O perito {perito_nome} NÃO atingiu o limiar de {threshold} análises/hora neste período. "
                "Mesmo assim, recomenda-se revisar e monitorar todas as análises periodicamente, inclusive as de peritos abaixo do limiar, conforme política de integridade."
            )
        else:
            extra = (
                f"\nO perito {perito_nome} atingiu ou superou o limiar de {threshold} análises/hora, "
                "o que acende um alerta de risco conforme explicado abaixo."
            )
    prompt = f"""
Você é um auditor técnico do Ministério da Previdência Social, especialista em detecção de riscos operacionais no ATESTMED.

Analise a tabela e o gráfico abaixo, referentes ao comparativo de produtividade (≥ {threshold} análises/hora) entre {start} e {end}.

Tabela:
{tabela_md}

Gráfico ASCII:
{chart_ascii}

IMPORTANTE:
- O KPI de produtividade (≥ {threshold} análises/hora) serve para identificar práticas automáticas, revezamento de credenciais ou ausência de análise real.
- Maior produtividade acima deste limiar = maior risco de fraude e não conformidade.
- **Atingir ou superar 50 análises/hora NÃO é meta institucional.** É um alerta para conduta incompatível.
Responda em texto corrido, **SEM usar listas ou tópicos** (nem numeradas, nem bullets). O comentário deve ser composto apenas por frases e parágrafos contínuos.
{extra}

Comente tecnicamente:
- Destaque se o perito está acima ou abaixo do limiar e o que isso representa.
- Reforce que todos, inclusive fora do limiar, devem ser analisados regularmente para garantir integridade do sistema.
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
    extra = ""
    if perito_nome and perito_valor is not None and not perito_valor:
        extra = (
            f"\nObservação: Não foram detectadas sobreposições relevantes para o perito {perito_nome} neste período. "
            "Mesmo assim, toda atuação deve ser acompanhada de modo preventivo."
        )
    prompt = f"""
Você é um auditor de integridade operacional do Ministério da Previdência Social.

Analise a tabela e o gráfico de sobreposição de tarefas de perícia entre {start} e {end}:

{tabela_md}

{chart_ascii}

INSTRUÇÕES:
- Sobreposição de tarefas indica execução paralela tecnicamente inviável e potencial fraude.
- KPI serve para identificar múltiplos logins, revezamento de senha ou manipulação.
- Incidência alta = bloqueio preventivo e revisão.
Responda em texto corrido, **SEM usar listas ou tópicos** (nem numeradas, nem bullets). O comentário deve ser composto apenas por frases e parágrafos contínuos.
{extra}

Comente de modo claro, destacando o risco institucional, e sempre recomende monitoramento mesmo quando não há sobreposição para o perito analisado.
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
    extra = ""
    if perito_nome and perito_valor is not None and perito_valor < 4.0:  # ajuste seu threshold real!
        extra = (
            f"\nO perito {perito_nome} ficou fora do grupo com scores finais mais elevados neste período, "
            "mas, conforme política institucional, todos devem ser acompanhados para prevenir desvios."
        )
    prompt = f"""
Você é um auditor institucional do Ministério da Previdência Social, especialista em integridade do ATESTMED.

Analise o ranking de peritos por Score Final (ICRA + [1 – IATD]) entre {start} e {end}.

Tabela:
{result_md}

{chart_ascii}

INSTRUÇÕES:
- Score Final alto = desempenho pior, acúmulo de fatores de risco (produtividade, sobreposição, NC, etc).
- Peritos com scores elevados = prioridade de supervisão, treinamento ou auditoria.
- Monitoramento é obrigatório para todos, mesmo fora do grupo crítico.
Responda em texto corrido, **SEM usar listas ou tópicos** (nem numeradas, nem bullets). O comentário deve ser composto apenas por frases e parágrafos contínuos.
{extra}

Comente tecnicamente, sempre destacando que a gestão de risco é para todos, não só para os casos críticos.
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
    extra = ""
    if cr_nome and cr_valor is not None and cr_valor < 4.0:  # ajuste seu threshold
        extra = (
            f"\nA CR {cr_nome} ficou fora do grupo de maior score médio neste período, "
            "mas recomenda-se revisão periódica para garantir integridade e qualidade institucional."
        )
    prompt = f"""
Você é um gestor estratégico do Ministério da Previdência Social, especialista em integridade do ATESTMED.

Analise o ranking de CRs por Score Médio no período de {start} até {end}.

Tabela:
{result_md}

{chart_ascii}

INSTRUÇÕES:
- Score Médio alto = maior risco institucional, maior prioridade de intervenção.
- Ranking orienta auditorias, supervisão e treinamento.
- Não é prêmio: todos devem ser monitorados periodicamente.
Responda em texto corrido, **SEM usar listas ou tópicos** (nem numeradas, nem bullets). O comentário deve ser composto apenas por frases e parágrafos contínuos.
{extra}

Comente de modo claro e estratégico, sempre reforçando que a gestão de risco vale para toda a estrutura.
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
    extra = ""
    if not any_criterio_acionado:
        extra = (
            "\nNenhum critério de risco foi acionado para o perito neste período. "
            "Mesmo assim, recomenda-se monitoramento institucional e revisão periódica para garantir padrões de integridade."
        )
    prompt = f"""
Você é um auditor do ATESTMED responsável por avaliação técnica individualizada de peritos.

Abaixo está o detalhamento completo do ICRA para o perito {nome}, no período de {start} a {end}:

{conteudo_md}

INSTRUÇÕES:
- O ICRA reúne critérios que, quando acionados, sinalizam risco elevado (produtividade alta, curtas, NC alta, sobreposição).
- Cada ocorrência é indício objetivo de desvio e demanda avaliação técnica.
- Monitoramento é necessário mesmo quando nenhum critério é acionado.
Responda em texto corrido, **SEM usar listas ou tópicos** (nem numeradas, nem bullets). O comentário deve ser composto apenas por frases e parágrafos contínuos.
{extra}

Comente sucintamente, voltado à gestão de risco individual, e sempre sugerindo revisão e acompanhamento de todos os casos.
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

