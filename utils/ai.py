#!/usr/bin/env python3
"""
Módulo de funções de comentário para scripts ATESTMED.
Cada função usa a API OpenAI v1 para gerar insights sobre tabelas e gráficos ASCII.
"""
from dotenv import load_dotenv
load_dotenv()
import os
import openai

# Configurar sua chave de API
openai.api_key = os.getenv("OPENAI_API_KEY")


def comentar_compare_30s(tabela_md: str, chart_ascii: str, start: str, end: str, threshold: int) -> str:
    """
    Gera comentário para o script compare_30s.py
    """
    prompt = f"""
Você é um analista de dados. Abaixo está o comparativo de perícias ≤ {threshold}s
no período de {start} até {end}.

Tabela (perito vs demais):
{tabela_md}

Gráfico ASCII:
{chart_ascii}

Por favor, faça um comentário ressaltando:
- O desempenho absoluto do perito em relação aos demais.
- Qualquer padrão visível na distribuição.
- Insight prático ou recomendação imediata.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um analista de dados especializado em interpretação de gráficos ASCII."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.5,
        max_tokens=250
    )
    return resp.choices[0].message.content.strip()


def comentar_nc100(tabela_md: str, start: str, end: str) -> str:
    """
    Gera comentário para o script table_nc100.py
    """
    prompt = f"""
Você é um auditor de qualidade. Abaixo está a lista de peritos com 100% de não conformidade
entre {start} e {end}:

{tabela_md}

Identifique riscos ou padrões nesta lista e sugira ações de mitigação.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um auditor de perícias experiente."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.5,
        max_tokens=250
    )
    return resp.choices[0].message.content.strip()


def comentar_produtividade(tabela_md: str, chart_ascii: str, start: str, end: str, threshold: int) -> str:
    """
    Gera comentário para o script compare_productivity.py
    """
    prompt = f"""
Você é um especialista em produtividade. Comparativo de produtividade ≥ {threshold}/h
entre {start} e {end}:

{tabela_md}

{chart_ascii}

Comente sobre possíveis sinais de automação ou necessidade de treinamento.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um consultor de produtividade."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.5,
        max_tokens=250
    )
    return resp.choices[0].message.content.strip()


def comentar_overlap(tabela_md: str, chart_ascii: str, start: str, end: str) -> str:
    """
    Gera comentário para o script compare_overlap.py
    """
    prompt = f"""
Você é um analista de fluxo de trabalho. Comparativo de sobreposição de tarefas
entre {start} e {end}:

{tabela_md}

{chart_ascii}

Explique o que a sobreposição indica e possíveis causas.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um especialista em análise de processos."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.5,
        max_tokens=250
    )
    return resp.choices[0].message.content.strip()


def comentar_rank_score(result_md: str, chart_ascii: str, start: str, end: str) -> str:
    """
    Gera comentário para o script rank_score_final.py
    """
    prompt = f"""
Você é um analista de risco. Ranking de Score Final no período {start} a {end}:

{result_md}

{chart_ascii}

Destaque os peritos de maior risco (score alto) e sugira prioridades de auditoria.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um analista de risco técnico."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.5,
        max_tokens=250
    )
    return resp.choices[0].message.content.strip()


def comentar_rank_cr(result_md: str, chart_ascii: str, start: str, end: str) -> str:
    """
    Gera comentário para o script rank_cr_score.py
    """
    prompt = f"""
Você é um gestor estratégico. Ranking de CRs por Score Médio de {start} a {end}:

{result_md}

{chart_ascii}

Comente quais regiões têm pior desempenho médio e implicações organizacionais.
""".strip()
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um gestor de operações públicas."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.5,
        max_tokens=250
    )
    return resp.choices[0].message.content.strip()

