#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
make_kpi_report_fluxo_b.py — compatível com ATESTMED, ajustado ao esquema SQLite fornecido.
Banco esperado:
  - analises(protocolo, siapePerito, conformado, motivoNaoConformado, dataHoraIniPericia,
             dataHoraFimPericia, duracaoPericia, duracao_seg, ...)
  - protocolos(protocolo, siapePerito, uf, cr, dr, nomePerito, ...)
  - peritos(siapePerito, nomePerito, cr, dr)

Regras/funcionalidades:
  • Pré-processamento: converte datas/durações; descarta durações > 1h e sem duração calculável.
  • Score Ponderado v2 e Fluxo B (elegibilidade automática).
  • Impacto na fila (prod_shortfall, nc_rework, combined).
  • Gráficos (matplotlib, sem seaborn) + legendas automáticas (OpenAI opcional via .env).
  • Export .org + PDF (Emacs por padrão; fallback pandoc). --emit-classic-figs cria fragmento e manifesto JSON.
  • Interpretação adicional: --org-append-file melhorias.org (ou --org-append-auto).
"""

import os
import sys
import csv
import json
import math
import shutil
import sqlite3
import argparse
import subprocess
import re
import gzip
import hashlib
from statistics import median
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Tuple, Optional

# garante que o diretório raiz do repositório entre no sys.path
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

try:
    from utils import comentarios  # fornece comentar_artefato, ai_table_captions, etc.
except Exception:
    comentarios = None  # mantém os recursos como opcionais

# Plotting (matplotlib puro; 1 gráfico por figura; sem cores específicas)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# ── Debug AI ───────────────────────────────────────────────────────────────────
DEBUG_AI = False

def _dbg(msg: str):
    """Log enxuto quando --debug-ai estiver ativo."""
    if DEBUG_AI:
        try:
            print(f"🔎 [debug-ai] {msg}")
        except Exception:
            pass

# Pastas padrão
BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
OUT_DIR     = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
ORG_DIR     = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
PDF_DIR     = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports', 'pdf')
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(ORG_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(
        description="Relatório KPI — Fluxo B + Impacto + Gráficos + Probabilidades (schema ATESTMED)"
    )

    # Debug das chamadas de IA (OpenAI)
    p.add_argument("--debug-ai", action="store_true",
                   help="Imprime diagnósticos das chamadas à OpenAI (carregamento de .env, chave, modelo, import).")

    # DB + período
    p.add_argument("--db", required=True, help="Caminho para o SQLite (ex.: db/atestmed.db)")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end",   required=True, help="YYYY-MM-DD")

    # Embutir tabelas no .org/.pdf
    p.add_argument("--embed-tables", action="store_true",
                   help="Incorpora tabelas (CSV) como tabelas Org no relatório (PDF).")
    p.add_argument("--embed-rows", type=int, default=40,
                   help="Máximo de linhas para a tabela principal e plano de ação (apêndice trará o completo).")

    # Precedência do melhorias.org (entra ANTES do resto, sem repetir título/data/autor)
    p.add_argument("--prepend-org-file", type=str, default=None,
                   help="Arquivo .org a ser inserido no topo do relatório, sem repetir cabeçalho.")

    # LaTeX/Org para tabelas grandes
    p.add_argument("--table-font-size", choices=["small","footnotesize","scriptsize","tiny"],
                   default="scriptsize", help="Tamanho da fonte das tabelas embutidas no PDF.")
    p.add_argument("--table-use-longtable", action="store_true",
                   help="Usa ambiente longtable (quebra automática entre páginas).")
    p.add_argument("--table-landscape", action="store_true",
                   help="Gira TODAS as tabelas embutidas em landscape (pdflscape).")
    p.add_argument("--no-landscape", action="store_true",
                   help="Força NÃO usar landscape, mesmo se outras flags tiverem sido passadas.")

    # Landscape fino por seção (sobrescreve quando usados, e respeita --no-landscape)
    p.add_argument("--landscape-main", action="store_true",
                   help="Gira SOMENTE a tabela principal (ranking).")
    p.add_argument("--landscape-cenarios", action="store_true",
                   help="Gira SOMENTE a tabela de cenários mensais.")
    p.add_argument("--landscape-plan", action="store_true",
                   help="Gira SOMENTE a tabela-resumo do plano de ação.")
    p.add_argument("--landscape-appendix", action="store_true",
                   help="Gira SOMENTE a tabela completa (apêndice).")

    # Cenários (conforme melhorias.org)
    p.add_argument("--scenarios-follow-melhorias", action="store_true",
                   help="Reproduz A/B/C (50/70/100%) sobre IV_sel (Top-10 mensal), como no melhorias.org")
    p.add_argument("--scenarios-topk", type=int, default=10,
                   help="K do Top-K mensal para IV_sel (padrão 10)")
    p.add_argument("--scenarios-reductions", type=str, default="0.5,0.7,1.0",
                   help="Reduções sobre IV_sel (ex: 0.5,0.7,1.0)")
    p.add_argument("--scenarios-labels", type=str, default="A,B,C",
                   help="Rótulos dos cenários (ex: A,B,C)")

    # Propostas diretamente do melhorias.org
    p.add_argument("--propostas-from-file", type=str, default=None,
                   help="Extrai a seção 'Medidas propostas' deste .org e usa na seção Propostas")

    # Saídas
    p.add_argument("--export-org", action="store_true")
    p.add_argument("--export-pdf", action="store_true")
    p.add_argument("--pdf-engine", choices=["emacs","pandoc"], default="emacs",
                   help="Gerar PDF via Emacs (org-export) ou via pandoc (default: emacs)")
    p.add_argument("--emit-classic-figs", action="store_true",
                   help="Gera fragmento .org e manifesto JSON para inclusão no relatório clássico")
    p.add_argument("--out-dir", default=OUT_DIR)
    p.add_argument("--org-dir", default=ORG_DIR)
    p.add_argument("--pdf-dir", default=PDF_DIR)
    
    # Fluxo B
    p.add_argument("--fluxo-b", action="store_true", help="Selecionar automaticamente os elegíveis ao Fluxo B")
    p.add_argument("--fluxo-b-topk", type=int, default=20, help="Número de peritos para o Fluxo B")
    p.add_argument("--fluxo-b-all", action="store_true", help="Usa TODOS os elegíveis (ignora --fluxo-b-topk)")
    p.add_argument("--mark-fluxo-b", action="store_true", help="Adicionar flag de elegibilidade a todos os peritos exportados")

    # Impacto
    p.add_argument("--with-impact", action="store_true", help="Calcula e inclui impacto na fila")
    p.add_argument("--impact-mode", choices=["prod_shortfall","nc_rework","combined"], default="combined")

    # 3) CR por peritos vs análises
    p.add_argument("--cr-mode", choices=["peritos","analises"], default="peritos",
                   help="Como contar distribuição por CR: por peritos elegíveis ou por análises dos elegíveis.")

    # 4) Anotar comentários da OpenAI nos PNGs
    p.add_argument("--annotate-figures", action="store_true",
                   help="Escreve a legenda da OpenAI no rodapé de cada PNG gerado.")

    # 5) Zip do pacote final
    p.add_argument("--zip-bundle", action="store_true",
                   help="Gera um .zip com PDF, ORG, CSVs, figuras e manifesto.")

    # 6) Seção com links para todos os arquivos gerados
    p.add_argument("--with-files-section", action="store_true",
                   help="Adiciona uma seção 'Arquivos gerados' com links clicáveis para todos os artefatos.")

    # 7) Pareto por impacto
    p.add_argument("--with-pareto", action="store_true",
                   help="Inclui gráfico e tabela Pareto (Top-10) do impacto na fila.")

    # 8) Estatísticas robustas (mediana/P90)
    p.add_argument("--with-robust-stats", action="store_true",
                   help="Inclui gráfico e resumo de mediana/P90 de duração por perito.")

    # 10) Distribuições extras
    p.add_argument("--with-dist-dr", action="store_true", help="Inclui gráfico de distribuição por DR.")
    p.add_argument("--with-dist-uf", action="store_true", help="Inclui gráfico de distribuição por UF.")

    # 11) Controle fino de seções
    p.add_argument("--no-main-table-section", action="store_true", help="Oculta a seção 'Tabela principal'.")
    p.add_argument("--no-plan-section", action="store_true", help="Oculta a seção 'Propostas/Plano de ação'.")
    p.add_argument("--no-scenarios-section", action="store_true", help="Oculta a seção de 'Cenários'.")
    p.add_argument("--no-graphs", action="store_true", help="Oculta a seção 'Gráficos' (todas as figuras).")

    # 12) Reprodutibilidade e cache
    p.add_argument("--seed", type=int, default=42, help="Semente para empates estáveis.")
    p.add_argument("--use-cache", action="store_true",
                   help="Cacheia o dataset joinado por período e DB para reexecução rápida.")
                   
    # Lorenz do impacto dos elegíveis
    p.add_argument("--with-impact-lorenz", action="store_true",
                   help="Inclui Curva de Lorenz do impacto dos elegíveis (e Gini) no relatório.")

    # Probabilidades (cenários internos por perito)
    p.add_argument("--prob-nc-success", type=float, default=0.6, help="Prob. de sucesso da medida de redução de NC")
    p.add_argument("--eff-nc-reduction", type=float, default=0.2, help="Tamanho do efeito: redução relativa da taxa de NC")
    p.add_argument("--prob-le15s-success", type=float, default=0.5, help="Prob. de sucesso para reduzir ≤15s")
    p.add_argument("--eff-le15s-reduction", type=float, default=0.5, help="Redução relativa nos ≤15s")
    p.add_argument("--prob-overlap-success", type=float, default=0.7, help="Prob. de sucesso para reduzir sobreposição")
    p.add_argument("--eff-overlap-reduction", type=float, default=0.7, help="Redução relativa no overlap")
    p.add_argument("--prob-prod-success", type=float, default=0.55, help="Prob. de sucesso para aumentar produtividade")
    p.add_argument("--eff-prod-increase", type=float, default=10.0, help="Aumento absoluto esperado em análises/h (até 50/h)")

    return p.parse_args()

def _load_dotenv_from_base():
    """Carrega variáveis do arquivo .env na raiz do projeto, se existir (com debug)."""
    try:
        env_path = os.path.join(BASE_DIR, ".env")
        _dbg(f"Tentando ler .env em {env_path}")
        loaded = False
        if os.path.isfile(env_path):
            with open(env_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k and (k not in os.environ):
                        os.environ[k] = v
                        loaded = True
        if DEBUG_AI:
            has_key = bool(os.getenv("OPENAI_API_KEY", "").strip())
            model = os.getenv("ATESTMED_OPENAI_MODEL", "gpt-4o-mini")
            _dbg(f".env lido? {loaded}; OPENAI_API_KEY presente? {has_key}; modelo={model}")
    except Exception as e:
        _dbg(f"falha ao ler .env: {e!r}")

def to_dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")

def business_days(year: int, month: int) -> int:
    """Dias úteis (seg–sex) do mês (desconsidera feriados)."""
    from calendar import monthrange
    n = monthrange(year, month)[1]
    count = 0
    for d in range(1, n+1):
        wd = date(year, month, d).weekday()
        if wd < 5:
            count += 1
    return count

def daterange_days(start: date, end: date) -> List[Tuple[int,int]]:
    """Lista (ano, mês) cobertos no intervalo [start, end] (inclusive)."""
    ym: List[Tuple[int,int]] = []
    cur = date(start.year, start.month, 1)
    endm = date(end.year, end.month, 1)
    while cur <= endm:
        ym.append((cur.year, cur.month))
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return ym

def ensure_fname(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("_","-") else "_" for ch in name)

# Palavras que devem permanecer minúsculas (quando não forem a 1ª do nome)
LOWER_WORDS = {"da", "de", "di", "do", "du", "e"}

def format_nome_pessoa(nome: str) -> str:
    """Formata nomes como Título, mantendo {da,de,di,do,du,e} minúsculas (exceto 1ª palavra)."""
    s = "" if nome is None else str(nome)
    s = re.sub(r"\s+", " ", s.strip())
    if not s:
        return s

    def cap_hyphen(word: str) -> str:
        # Capitaliza partes hifenizadas: maria-eduarda -> Maria-Eduarda
        parts = word.split("-")
        return "-".join(p[:1].upper() + p[1:].lower() if p else p for p in parts)

    tokens = s.split(" ")
    out = []
    for i, t in enumerate(tokens):
        low = t.lower()
        if i > 0 and low in LOWER_WORDS:
            out.append(low)
        else:
            out.append(cap_hyphen(low))
    return " ".join(out)

def short_label(name: str, maxlen: int = 24) -> str:
    s = "" if name is None else str(name)
    s = re.sub(r"\s+", " ", s.strip())
    return s if len(s) <= maxlen else s[:maxlen-1] + "…"

def load_period_data(conn: sqlite3.Connection, dt_start: str, dt_end: str) -> List[Dict[str, Any]]:
    """
    Carrega análises do período (join em protocolos/peritos) e:
      • descarta duração > 1h e sem duração calculável;
      • calcula duracao_segundos (prioriza analises.duracao_seg; senão fim-ini; senão HH:MM:SS).
    """
    q = """
    SELECT
      a.protocolo AS protocolo,
      a.siapePerito AS siapePerito,
      COALESCE(pe.nomePerito, p.nomePerito, '') AS nomePerito,
      COALESCE(p.cr, pe.cr, '') AS cr,
      COALESCE(p.dr, pe.dr, '') AS dr,
      COALESCE(p.uf, '') AS uf,
      a.dataHoraIniPericia,
      a.dataHoraFimPericia,
      a.duracaoPericia,
      a.duracao_seg,
      CAST(a.conformado AS INTEGER) AS conformado,
      CAST(a.motivoNaoConformado AS INTEGER) AS motivoNaoConformado
    FROM analises a
    LEFT JOIN peritos pe    ON pe.siapePerito = a.siapePerito
    LEFT JOIN protocolos p  ON p.protocolo    = a.protocolo
    WHERE DATE(a.dataHoraIniPericia) >= DATE(?)
      AND DATE(a.dataHoraIniPericia) <= DATE(?)
    """
    cur = conn.execute(q, (dt_start, dt_end))
    rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]

    out = []
    for r in rows:
        ini = r.get("dataHoraIniPericia")
        fim = r.get("dataHoraFimPericia")
        dur_str = r.get("duracaoPericia")
        dur_sec_db = r.get("duracao_seg")

        dur_s = None
        try:
            if dur_sec_db is not None:
                ds = float(dur_sec_db)
                if ds > 0:
                    dur_s = ds
            if dur_s is None and ini and fim:
                dt_i = datetime.fromisoformat(str(ini))
                dt_f = datetime.fromisoformat(str(fim))
                dsec = (dt_f - dt_i).total_seconds()
                if dsec > 0:
                    dur_s = dsec
            if dur_s is None and dur_str:
                if isinstance(dur_str, str) and ":" in dur_str:
                    h, m, s = dur_str.split(":")
                    dur_s = int(h)*3600 + int(m)*60 + int(float(s))
                else:
                    ds = float(dur_str)
                    if ds > 0:
                        dur_s = ds
        except Exception:
            dur_s = None

        if dur_s is None:
            continue
        if dur_s > 3600:
            continue

        r["duracao_segundos"] = float(dur_s)
        out.append(r)
    return out

def detect_overlaps(intervals: List[Tuple[float, float]]) -> int:
    """Conta quantas análises participam de sobreposição (qualquer interseção)."""
    if not intervals:
        return 0
    intervals = sorted(intervals, key=lambda x: x[0])
    overlapped_idx = set()
    prev_start, prev_end = intervals[0]
    for i in range(1, len(intervals)):
        s, e = intervals[i]
        if s < prev_end:
            overlapped_idx.add(i-1); overlapped_idx.add(i)
            prev_end = max(prev_end, e)
        else:
            prev_start, prev_end = s, e
    return len(overlapped_idx)

@dataclass
class PeritoAgg:
    nome: str
    siape: str
    cr: str
    dr: str
    uf: str
    total: int
    nc: int
    horas_efetivas: float
    prod_por_hora: float
    le15s: int
    overlaps: int
    pct_nc: float
    pct_le15s: float
    pct_overlap: float

def aggregate_perito(rows: List[Dict[str, Any]]) -> Dict[str, PeritoAgg]:
    """
    Agrega indicadores por perito a partir das linhas pré-processadas (load_period_data):
      - normaliza nome (Title Case preservando partículas/sufixos),
      - soma horas efetivas (duracao_segundos <= 1h já garantido no load),
      - produtividade = total / horas_efetivas,
      - contagem de ≤15s, sobreposição de sessões e %s.
    Retorna dict { "Nome [SIAPE]": PeritoAgg }.
    """
    def _s(v) -> str:
        try:
            return ("" if v is None else str(v)).strip()
        except Exception:
            return ""

    def _to_int(v) -> int:
        try:
            return int(v)
        except Exception:
            return 0

    def _to_float(v) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    # Agrupa registros por (nome_formatado, siape)
    by_perito: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        raw_nome = _s(r.get("nomePerito"))
        nome_fmt = human_title_name(raw_nome)  # <<< preserva "da, de, di, do, du, e" em minúsculas
        siape = _s(r.get("siapePerito"))
        if not nome_fmt and not siape:
            continue
        rr = dict(r)
        rr["nomePerito"] = nome_fmt  # padroniza adiante
        by_perito.setdefault((nome_fmt, siape), []).append(rr)

    out: Dict[str, PeritoAgg] = {}

    for (nome, siape), items in by_perito.items():
        # Campos fixos (usa o primeiro item como "amostra" para cr/dr/uf)
        def _sf(k: str) -> str:
            return _s(items[0].get(k))

        cr = _sf("cr")
        dr = _sf("dr")
        uf = _sf("uf")

        total = len(items)
        nc = sum(1 for x in items if _to_int(x.get("conformado")) == 0)
        le15s = sum(1 for x in items if _to_float(x.get("duracao_segundos")) <= 15.0)
        horas = sum(_to_float(x.get("duracao_segundos")) for x in items) / 3600.0

        # Janelas para detectar sobreposição (qualquer interseção)
        intervals: List[Tuple[float, float]] = []
        for x in items:
            try:
                dt_i = datetime.fromisoformat(_s(x.get("dataHoraIniPericia")))
                if x.get("dataHoraFimPericia"):
                    dt_f = datetime.fromisoformat(_s(x.get("dataHoraFimPericia")))
                else:
                    dt_f = dt_i + timedelta(seconds=_to_float(x.get("duracao_segundos")))
                intervals.append((dt_i.timestamp(), dt_f.timestamp()))
            except Exception:
                # ignora itens sem datas parseáveis (já filtrados na carga em geral)
                pass

        overlaps = detect_overlaps(intervals)

        prod_h = (total / horas) if horas > 0 else 0.0
        pct_nc = (nc / total * 100.0) if total > 0 else 0.0
        pct_le15s = (le15s / total * 100.0) if total > 0 else 0.0
        pct_overlap = (overlaps / total * 100.0) if total > 0 else 0.0

        key = f"{nome} [{siape}]"
        out[key] = PeritoAgg(
            nome=nome,
            siape=siape,
            cr=cr,
            dr=dr,
            uf=uf,
            total=total,
            nc=nc,
            horas_efetivas=horas,
            prod_por_hora=prod_h,
            le15s=le15s,
            overlaps=overlaps,
            pct_nc=pct_nc,
            pct_le15s=pct_le15s,
            pct_overlap=pct_overlap,
        )

    return out

def national_nc_mean(rows: List[Dict[str, Any]]) -> float:
    total = len(rows)
    nc = sum(1 for x in rows if int(x.get("conformado") or 0) == 0)
    return (nc/total*100) if total > 0 else 0.0

def fluxo_b_eligible(agg: PeritoAgg, mean_nc_br: float, dias_uteis_total: int) -> bool:
    # Requerimentos > 2 × dias úteis (aprox.: total de análises do período)
    req_ok = agg.total > (2 * dias_uteis_total)
    # %NC do perito > 2 × média nacional
    nc_ok = agg.pct_nc > (2.0 * mean_nc_br)
    return req_ok and nc_ok

def impacto_prod_shortfall(agg: PeritoAgg, alvo_por_hora: float = 50.0) -> float:
    target = agg.horas_efetivas * alvo_por_hora
    return max(0.0, target - agg.total)

def impacto_nc_rework(agg: PeritoAgg, fator_retrabalho: float = 1.0) -> float:
    return agg.nc * fator_retrabalho

def compute_impact(agg: PeritoAgg, mode: str) -> float:
    if mode == "prod_shortfall":
        return impacto_prod_shortfall(agg)
    elif mode == "nc_rework":
        return impacto_nc_rework(agg)
    else:
        return impacto_prod_shortfall(agg) + impacto_nc_rework(agg)

def score_ponderado_v2(agg: PeritoAgg, mean_nc_br: float) -> float:
    score = 0.0
    if agg.prod_por_hora >= 50.0:
        score += 3.0
    if agg.overlaps > 0:
        score += 2.5
    if agg.le15s >= 10:
        score += 2.0
    if agg.pct_nc >= 2.0 * mean_nc_br:
        score += 1.0
    return round(score, 2)

def export_table_csv(path: str, rows: List[Dict[str,Any]]):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)

def org_file_link(path: str, label: Optional[str] = None) -> str:
    """Cria link clicável estilo Org para um arquivo local."""
    if not path:
        return ""
    lab = label or os.path.basename(path)
    return f"[[file:{path}][{lab}]]"


def annotate_png_with_text(png_path: str, text: str):
    """Acrescenta uma linha de texto discreta no rodapé do PNG, in-place (matplotlib-only)."""
    try:
        img = plt.imread(png_path)
        h, w = img.shape[0], img.shape[1]
        fig = plt.figure(frameon=False, figsize=(w/100, h/100), dpi=100)
        ax = plt.Axes(fig, [0., 0.08, 1., 0.92])  # deixa espaço no rodapé
        fig.add_axes(ax)
        ax.imshow(img)
        ax.axis('off')
        # texto no rodapé
        fig.text(0.01, 0.005, text[:280], ha='left', va='bottom', fontsize=8, wrap=True)
        fig.savefig(png_path, dpi=100)
        plt.close(fig)
    except Exception:
        # Fallback: não interrompe pipeline
        pass


def create_zip_bundle(zip_path: str, paths: List[str]):
    """Cria um .zip com os arquivos existentes na lista paths."""
    import zipfile
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for p in paths:
            if p and os.path.exists(p):
                arc = os.path.relpath(p, os.path.dirname(zip_path))
                zf.write(p, arc)


def human_title_name(name: str) -> str:
    """Nome com Title Case preservando partículas e sufixos comuns."""
    if not name:
        return ""
    parts_low = {"da","de","di","do","du","e"}
    sufixos = {"jr": "Jr.", "jr.": "Jr.", "filho":"Filho", "neto":"Neto"}
    toks = str(name).strip().split()
    out = []
    for i, t in enumerate(toks):
        low = t.lower()
        if i>0 and low in parts_low:
            out.append(low)
        elif low in sufixos:
            out.append(sufixos[low])
        else:
            out.append(low.capitalize())
    return " ".join(out)


def robust_stats(values: List[float]) -> Tuple[float, float]:
    """Retorna (mediana, P90) em segundos."""
    if not values:
        return 0.0, 0.0
    vals = sorted(values)
    med = float(median(vals))
    p90_idx = max(0, int(math.ceil(0.9*len(vals)))-1)
    return med, float(vals[p90_idx])


def hash_for_cache(db_path: str, dt_start: str, dt_end: str) -> str:
    """Cria um hash do caminho, mtime do DB e período, para cache de rows."""
    h = hashlib.sha256()
    h.update(os.path.abspath(db_path).encode())
    try:
        mt = os.path.getmtime(db_path)
    except Exception:
        mt = 0
    h.update(str(mt).encode())
    h.update(dt_start.encode()); h.update(dt_end.encode())
    return h.hexdigest()[:16]


def load_or_cache_period_data(db_path: str, dt_start: str, dt_end: str, use_cache: bool) -> List[Dict[str, Any]]:
    """Wrapper para cachear o resultado de load_period_data (JSON.gz)."""
    cache_dir = os.path.join(OUT_DIR, "_cache")
    os.makedirs(cache_dir, exist_ok=True)
    key = hash_for_cache(db_path, dt_start, dt_end)
    cache_file = os.path.join(cache_dir, f"rows_{key}.json.gz")

    # válido se cache mais novo que DB
    try:
        if use_cache and os.path.exists(cache_file):
            if os.path.getmtime(cache_file) >= os.path.getmtime(db_path):
                with gzip.open(cache_file, "rt", encoding="utf-8") as f:
                    return json.load(f)
    except Exception:
        pass

    with sqlite3.connect(db_path) as conn:
        rows = load_period_data(conn, dt_start, dt_end)

    try:
        if use_cache and rows:
            with gzip.open(cache_file, "wt", encoding="utf-8") as f:
                json.dump(rows, f, ensure_ascii=False)
    except Exception:
        pass
    return rows

def csv_to_org_table(csv_path: str, max_rows: Optional[int] = None) -> List[str]:
    """Lê um CSV e gera linhas de tabela Org-Mode (| a | b | ... |)."""
    lines: List[str] = []
    if not csv_path or not os.path.exists(csv_path):
        return lines
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return lines

    header = rows[0]
    body = rows[1:]
    if max_rows is not None and max_rows > 0:
        body = body[:max_rows]

    # header
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|-" + "-+-".join("-"*len(h) for h in header) + "-|")
    # body
    for r in body:
        # garante mesmo nº de colunas (evita quebrar o Org se houver linha curta)
        r2 = list(r) + [""] * (len(header) - len(r))
        lines.append("| " + " | ".join(r2[:len(header)]) + " |")
    return lines

def emit_org_table_block(table_lines: List[str],
                         font_size: str = "scriptsize",
                         use_longtable: bool = False,
                         landscape: bool = False) -> List[str]:
    r"""
    Constrói um bloco Org para a tabela com ajustes LaTeX:
      - fonte reduzida (\small/\footnotesize/\scriptsize/\tiny)
      - ambiente longtable (quebra páginas)
      - landscape opcional
    """
    if not table_lines:
        return []
    out = []
    if landscape:
        out.append("#+LATEX: \\begin{landscape}")
    # atributos do LaTeX para a tabela
    if use_longtable:
        out.append("#+ATTR_LATEX: :environment longtable")
    # fonte menor
    out.append(f"#+LATEX: \\begingroup\\{font_size}")
    out.extend(table_lines)
    out.append("#+LATEX: \\endgroup")
    if landscape:
        out.append("#+LATEX: \\end{landscape}")
    return out

# ---------- Gráficos ----------
def save_bar(values, labels, title, out_path, ylabel="Valor"):
    plt.figure()
    x = np.arange(len(values))
    plt.bar(x, values)
    plt.xticks(x, labels, rotation=45, ha="right")
    plt.title(title)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()

def save_heatmap(matrix, row_labels, col_labels, title, out_path,
                 y_fontsize=None, x_fontsize=None):
    n_rows = max(1, len(row_labels))
    n_cols = max(1, len(col_labels))

    # fonte dinâmica (fica menor conforme cresce a quantidade)
    if y_fontsize is None:
        y_fontsize = max(6, min(10, 14 - 0.12 * n_rows))
    if x_fontsize is None:
        x_fontsize = max(6, min(10, 12 - 0.08 * n_cols))

    # tamanho da figura também ajustável pra ajudar a não sobrepor
    fig_w = max(6.0, min(16.0, 0.60 * n_cols + 2.0))
    fig_h = max(4.0, min(24.0, 0.35 * n_rows + 2.0))

    plt.figure(figsize=(fig_w, fig_h))
    mat = np.array(matrix, dtype=float) if matrix else np.zeros((1, n_cols))
    im = plt.imshow(mat, aspect="auto")

    plt.xticks(np.arange(n_cols), col_labels, rotation=0, fontsize=x_fontsize)
    plt.yticks(np.arange(n_rows), row_labels, fontsize=y_fontsize)

    plt.title(title)
    cbar = plt.colorbar(im)
    cbar.ax.tick_params(labelsize=max(6, int(min(x_fontsize, y_fontsize) - 1)))

    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()

def count_by_dimension_peritos(records: List[Dict[str,Any]], field: str) -> Dict[str,int]:
    out = {}
    for r in records:
        v = ("" if r.get(field) is None else str(r.get(field))).strip() or "N/D"
        out[v] = out.get(v, 0) + 1
    return out

def count_by_dimension_analises(rows: List[Dict[str,Any]], elegiveis_keyset: set, field: str) -> Dict[str,int]:
    out = {}
    for r in rows:
        # normaliza igual ao aggregate_perito
        nome = human_title_name(str(r.get("nomePerito") or "").strip())
        siape = str(r.get("siapePerito") or "").strip()
        if (nome, siape) not in elegiveis_keyset:
            continue
        v = ("" if r.get(field) is None else str(r.get(field))).strip() or "N/D"
        out[v] = out.get(v, 0) + 1
    return out

def build_pareto_table(values: List[Tuple[str,float]], topk: int = 10) -> List[List[Any]]:
    values = sorted(values, key=lambda x: x[1], reverse=True)
    total = sum(v for _, v in values) or 1.0
    rows = [["Rank","Perito","Impacto","%","Cumul%"]]
    cum = 0.0
    for i, (name, val) in enumerate(values[:topk], start=1):
        pct = (val/total)*100.0
        cum += pct
        rows.append([i, name, int(round(val)), round(pct,2), round(cum,2)])
    return rows

def save_lorenz_impact(values: List[float], out_path: str,
                       title: str = "Curva de Lorenz — Impacto entre elegíveis") -> Optional[float]:
    """
    Gera a Curva de Lorenz para a distribuição de 'values' (impacto por perito) e
    retorna o índice de Gini. Se não houver variação positiva, não gera a figura.
    """
    vals = [float(v) for v in values if v is not None and float(v) >= 0.0]
    if not vals or sum(vals) <= 0:
        return None

    v = np.sort(np.array(vals, dtype=float))
    n = v.size

    # Eixos da Lorenz
    x = np.arange(n + 1) / n
    y = np.concatenate([[0.0], np.cumsum(v) / v.sum()])

    # Gini = 1 - 2 * área sob a curva de Lorenz
    try:
        area = np.trapezoid(y, x)  # numpy >= 2.0
    except Exception:
        area = np.trapz(y, x)      # fallback para numpy < 2.0
    gini = max(0.0, min(1.0, 1.0 - 2.0 * area))

    # Plot
    plt.figure()
    plt.plot(x, y, marker="o", linewidth=2, label=f"Lorenz (Gini={gini:.3f})")
    plt.plot([0, 1], [0, 1], linestyle="--", label="Igualdade perfeita")
    plt.title(title)
    plt.xlabel("Proporção acumulada de peritos")
    plt.ylabel("Proporção acumulada do impacto")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()

    return float(gini)

# ---------- Cenários probabilísticos ----------
def expected_impact_after_measures(agg: PeritoAgg, base_impact: float, args) -> Tuple[float, Dict[str, float]]:
    """
    Impacto esperado após medidas independentes:
      - Redução %NC, ≤15s, Overlap; Aumento Produtividade (limitado a 50/h).
    Heurística: recalcula o impacto (modo combined como referência).
    """
    prod_h = agg.prod_por_hora
    nc = agg.nc
    le15s = agg.le15s
    overlaps = agg.overlaps
    horas = agg.horas_efetivas

    nc_redux = args.prob_nc_success * args.eff_nc_reduction
    nc_after = max(0.0, nc * (1.0 - nc_redux))

    le15s_redux = args.prob_le15s_success * args.eff_le15s_reduction
    le15s_after = max(0.0, le15s * (1.0 - le15s_redux))

    overlap_redux = args.prob_overlap_success * args.eff_overlap_reduction
    overlaps_after = max(0.0, overlaps * (1.0 - overlap_redux))

    prod_gain = args.prob_prod_success * args.eff_prod_increase
    prod_after = min(50.0, prod_h + prod_gain)

    target = horas * 50.0
    expected_total = min(prod_after * horas, target)
    prod_shortfall_after = max(0.0, target - expected_total)
    nc_rework_after = nc_after
    expected_combined = prod_shortfall_after + nc_rework_after

    deltas = {
        "delta_nc": nc - nc_after,
        "delta_le15s": le15s - le15s_after,
        "delta_overlaps": overlaps - overlaps_after,
        "delta_prod_shortfall": (horas*50.0 - (agg.total)) - prod_shortfall_after
    }
    return expected_combined, deltas

def month_key_from_iso(dt_iso: str) -> str:
    # Espera "YYYY-MM-DD..." ; se vier só "YYYY-MM-DD HH:MM:SS", funciona igual
    s = str(dt_iso or "").strip()
    return s[:7] if len(s) >= 7 else s

def compute_monthly_impacts(rows: List[Dict[str, Any]], impact_mode: str, topk: int) -> List[Dict[str, Any]]:
    """
    Para cada mês do período: calcula IV_total (soma dos impactos de todos os peritos)
    e IV_sel (soma dos impactos dos TOP-K peritos no mês).
    """
    # bucket por mês
    by_month: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        ym = month_key_from_iso(r.get("dataHoraIniPericia"))
        if not ym:
            continue
        by_month.setdefault(ym, []).append(r)

    monthly: List[Dict[str, Any]] = []
    for ym in sorted(by_month.keys()):
        subset = by_month[ym]
        aggs = aggregate_perito(subset)
        impacts = []
        for k, agg in aggs.items():
            imp = compute_impact(agg, impact_mode)
            impacts.append(imp)
        iv_total = float(sum(impacts))
        impacts.sort(reverse=True)
        iv_sel = float(sum(impacts[:max(1, int(topk))])) if impacts else 0.0
        monthly.append({"mes": ym, "IV_total": round(iv_total, 2), "IV_sel": round(iv_sel, 2)})
    return monthly

def build_cenarios_and_plot(monthly: List[Dict[str, Any]], reductions: List[float],
                            labels: List[str], out_path: str, title: str) -> Tuple[str, List[List[Any]]]:
    """
    Cria série Real (IV_total) e cenários aplicando reduções sobre IV_sel.
    Retorna caminho do gráfico e uma tabela-resumo (inclui totais).
    """
    if not monthly:
        return out_path, []
    mes = [m["mes"] for m in monthly]
    ivtot = [float(m["IV_total"]) for m in monthly]
    ivsel = [float(m["IV_sel"]) for m in monthly]

    cen_series = []
    for r in reductions:
        cen_series.append([t - r*s for t, s in zip(ivtot, ivsel)])

    # Gráfico
    plt.figure()
    plt.plot(mes, ivtot, marker="o", linewidth=2, label="Real (IV_total)")
    for i, s in enumerate(cen_series):
        lab = f"Cenário {labels[i]} ({int(round(100*reductions[i]))}% IV_sel)" if i < len(labels) else f"C{i+1}"
        plt.plot(mes, s, marker="o", linestyle="--", label=lab)
    plt.title(title)
    plt.xlabel("Mês")
    plt.ylabel("Impacto na fila (unid. do indicador)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()

    # Tabela resumo por mês + totais e % de redução relativa a IV_total
    def pct(n, d): return round((n*100.0)/d, 2) if d else 0.0
    header = ["Mes","IV_total","IV_sel"]
    for i, lab in enumerate(labels):
        header += [f"Cen_{lab}", f"Red_{lab}", f"Red_{lab}_%"]
    table = [header]
    tot_ivtot = sum(ivtot); tot_ivsel = sum(ivsel)
    tot_reds = [0.0]*len(reductions)
    for i in range(len(mes)):
        row = [mes[i], int(round(ivtot[i])), int(round(ivsel[i]))]
        for j, r in enumerate(reductions):
            cen_val = cen_series[j][i]
            red_abs = ivtot[i] - cen_val
            row += [int(round(cen_val)), int(round(red_abs)), pct(red_abs, ivtot[i])]
            tot_reds[j] += red_abs
        table.append(row)
    # Totais
    tot_row = ["*Total*", int(round(tot_ivtot)), int(round(tot_ivsel))]
    for j, r in enumerate(reductions):
        tot_row += ["—", int(round(tot_reds[j])), pct(tot_reds[j], tot_ivtot)]
    table.append(tot_row)
    return out_path, table

def export_table_org_lines(table: List[List[Any]]) -> List[str]:
    """Converte tabela (lista de listas) em linhas Org-Mode | a | b | c |."""
    if not table:
        return []
    lines = []
    for i, row in enumerate(table):
        cells = [str(x) for x in row]
        lines.append("| " + " | ".join(cells) + " |")
        if i == 0:
            lines.append("|-" + "-+-".join("-"*len(c) for c in cells) + "-|")
    return lines

def extract_proposals_from_org(path: str) -> Optional[str]:
    """
    Extrai a seção de 'Medidas propostas' do arquivo .org.
    Heurística: pega a partir da linha que contém 'Medidas propostas' até a próxima
    linha que começa com '*' (cabeçalho) no mesmo nível ou EOF.
    """
    try:
        text = open(path, "r", encoding="utf-8").read().splitlines()
    except Exception:
        return None
    start = None
    for i, L in enumerate(text):
        if "Medidas propostas" in L:
            start = i
            break
    if start is None:
        return None
    buf = []
    for L in text[start:]:
        if L.strip().startswith("* ") and len(buf) > 0:
            break
        buf.append(L)
    return "\n".join(buf).strip() if buf else None

def ai_explanations(summary_payload: Dict[str, Any]) -> str:
    _dbg("ai_explanations() iniciou")
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("ATESTMED_OPENAI_MODEL", "gpt-4o-mini")
    _dbg(f"key? {bool(api_key)} | model={model}")

    if not api_key:
        _dbg("sem OPENAI_API_KEY — usando fallback")
        return _fallback_explanations(summary_payload)

    prompt = (
        "Você é analista de dados do projeto ATESTMED. Explique as métricas (%NC, Prod/h, ≤15s, Overlap, Score v2) "
        "SEM inventar números (use só o JSON), as regras do Fluxo B e o cálculo do 'Impacto na fila'. "
        "Comente os gráficos e os cenários probabilísticos, incluindo hipóteses, limitações e recomendações. "
        "Baseie-se no JSON a seguir.\n\n"
        f"{json.dumps(summary_payload, ensure_ascii=False, indent=2)}\n"
    )

    # Tentativa 1: SDK novo (classe OpenAI)
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=api_key)
        _dbg("usando SDK OpenAI (classe OpenAI) para explanations")
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Você é um(a) analista sênior de dados do ATESTMED."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=900,
        )
        txt = resp.choices[0].message.content.strip() if getattr(resp, "choices", None) else ""
        _dbg(f"OpenAI completou? {bool(txt)}")
        if txt:
            return txt
    except Exception as e:
        _dbg(f"exceção OpenAI(OpenAI): {e!r}")

    # Tentativa 2: SDK legacy (openai.ChatCompletion)
    try:
        import openai  # type: ignore
        openai.api_key = api_key
        _dbg("usando SDK openai.ChatCompletion (fallback) para explanations")
        resp = openai.ChatCompletion.create(
            model=model,
            messages=[
                {"role": "system", "content": "Você é um(a) analista sênior de dados do ATESTMED."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=900,
        )
        txt = resp["choices"][0]["message"]["content"].strip()
        _dbg(f"ChatCompletion completou? {bool(txt)}")
        if txt:
            return txt
    except Exception as e:
        _dbg(f"exceção OpenAI(ChatCompletion): {e!r}")

    _dbg("ai_explanations() usando fallback (sem IA)")
    return _fallback_explanations(summary_payload)

def _fallback_explanations(payload: Dict[str, Any]) -> str:
    mean_br = payload.get("media_nc_brasil", 0.0)
    eleg = payload.get("elegiveis", 0)
    soma_impacto = payload.get("soma_impacto", 0.0)
    return (
        "Aplicamos pré-tratamento (exclusão de durações > 1h e inválidas) e agregação por perito. "
        "Produtividade efetiva = total de análises / horas efetivas (soma das durações válidas ≤ 1h). "
        f"A elegibilidade do Fluxo B requer volume > 2× dias úteis e %NC ≥ 2× média nacional ({mean_br:.2f}%). "
        "Score v2 soma pesos para: Prod≥50/h (3), Overlap>0 (2.5), ≥10 análises ≤15s (2), %NC ≥2×BR (1). "
        f"Foram elegíveis {eleg} peritos; o impacto estimado total foi {soma_impacto:.0f} análises. "
        "Gráficos: heatmap de critérios, impacto Top 20, distribuição por CR (com 'N/D' para ausência), "
        "indicadores médios vs alvo e impacto esperado pós-medidas."
    )

# --- AI helpers --------------------------------------------------------------

def _json_from_maybe_markdown(txt: str) -> Optional[dict]:
    """Tenta extrair JSON válido mesmo se vier cercado por markdown/código."""
    if not txt:
        return None
    s = txt.strip()

    # Remove cercas de código ``` ou ```json
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*```$", "", s)

    # Recorta do primeiro '{' até o último '}' (evita preâmbulos)
    i, j = s.find("{"), s.rfind("}")
    if i != -1 and j != -1 and j >= i:
        s = s[i:j+1]

    # 1) JSON direto
    try:
        return json.loads(s)
    except Exception:
        pass

    # 2) Tenta literal_eval (aceita aspas simples)
    try:
        import ast
        d = ast.literal_eval(s)
        if isinstance(d, dict):
            return d
    except Exception:
        pass

    return None
    
def ai_figure_captions(summary_payload: Dict[str, Any], figs: List[str]) -> Dict[str, str]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()

    base_fallbacks = {
        "impacto_top20": "Ranking dos peritos elegíveis com maior contribuição ao impacto total estimado na fila.",
        "heatmap_score_flags": "Mapa de calor: por perito, quais critérios do Score v2 foram acionados.",
        "dist_por_CR": "Contagem por CR; 'N/D' indica CR não informado.",
        "dist_por_DR": "Distribuição por DR entre os elegíveis.",
        "dist_por_UF": "Distribuição por UF entre os elegíveis.",
        "indicadores_medios": "Comparativo dos indicadores médios dos elegíveis com o alvo de produtividade (50 análises/h).",
        "impacto_esperado_pos_medidas_top20": "Impacto esperado após medidas, para os 20 maiores impactos.",
        "lorenz_impacto_elegiveis": "Curva de Lorenz do impacto entre os elegíveis; a diagonal indica igualdade perfeita.",
        "cenarios_mensais": "Real (IV_total) comparado a cenários A/B/C aplicando reduções sobre IV_sel.",
        "pareto_impacto": "Diagrama de Pareto do impacto: barras (impacto) e linha (acumulado).",
        "duracao_mediana_p90_top20": "Duração por perito — mediana e P90 (Top-20 por volume).",
    }

    captions: Dict[str, str] = {}
    if not figs:
        return captions

    # Fallback sem chave
    if not api_key:
        for f in figs:
            base = os.path.basename(f)
            found = next((txt for k, txt in base_fallbacks.items() if k in base), None)
            captions[f] = found or "Gráfico gerado automaticamente a partir do conjunto reportado."
        return captions

    def _fill_from_map(fig_list: List[str], d: Dict[str, str]) -> Dict[str, str]:
        out: Dict[str,str] = {}
        for f in fig_list:
            base = os.path.basename(f)
            # tenta por basename exato; depois tenta por substring conhecida
            out[f] = d.get(base) or d.get(f) or next(
                (txt for k, txt in base_fallbacks.items() if k in base),
                "Gráfico gerado automaticamente.")
        return out

    # Prompt “estrito” pedindo JSON puro
    payload = {'payload': summary_payload, 'figs': [os.path.basename(x) for x in figs]}
    sysmsg = "Você redige legendas técnicas, curtas e objetivas."
    user_prompt = (
        "Escreva UMA legenda curta (1–2 frases) por figura. "
        "Responda EXCLUSIVAMENTE com um JSON válido (sem markdown, sem crases), "
        "mapeando {fig_basename: legenda}. "
        "Não invente números; comente padrões, comparações e leituras corretas.\n\n"
        f"{json.dumps(payload, ensure_ascii=False)}"
    )

    model = os.getenv("ATESTMED_OPENAI_MODEL", "gpt-4o-mini")

    # Tenta 2 vezes: (1) com response_format json_object; (2) sem, mas reforçando a instrução
    raw_text = None
    for attempt in (1, 2):
        try:
            from openai import OpenAI  # type: ignore
            client = OpenAI(api_key=api_key)
            kwargs = dict(
                model=model,
                messages=[{"role": "system", "content": sysmsg},
                          {"role": "user", "content": user_prompt}],
                temperature=0.0,
                max_tokens=700,
            )
            if attempt == 1:
                # Em SDKs recentes isso força JSON puro; se não suportar, cai no except
                kwargs["response_format"] = {"type": "json_object"}  # type: ignore[assignment]
            resp = client.chat.completions.create(**kwargs)  # type: ignore[arg-type]
            raw_text = (resp.choices[0].message.content or "").strip()
            _dbg(f"ai_figure_captions: tentativa {attempt} | len(raw)={len(raw_text)}")
            d = _json_from_maybe_markdown(raw_text)
            if isinstance(d, dict) and d:
                parsed = _fill_from_map(figs, d)
                _dbg(f"ai_figure_captions: JSON OK na tentativa {attempt} | itens={len(parsed)}")
                return parsed
            else:
                _dbg("ai_figure_captions: parse falhou nesta tentativa; re-tentando…")
        except Exception as e:
            _dbg(f"ai_figure_captions: exceção tentativa {attempt}: {e!r}")

    # Se chegou aqui, caiu no fallback: usa mapa base + (se possível) heurística do que veio
    if raw_text:
        # tenta ao menos arrancar algo que pareça pares "nome.png": "legenda"
        try:
            approx = _json_from_maybe_markdown(raw_text) or {}
            approx = approx if isinstance(approx, dict) else {}
            if approx:
                _dbg(f"ai_figure_captions: usando approx extraído do raw | itens={len(approx)}")
                return _fill_from_map(figs, approx)
        except Exception:
            pass

    # fallback final
    _dbg("ai_figure_captions: fallback base_fallbacks acionado")
    return _fill_from_map(figs, {})

def ai_table_captions(summary_payload: Dict[str, Any],
                      tables_meta: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Gera uma frase curta de comentário para cada tabela/lista (mesmo que seja só link).
    tables_meta: [{"id":"main"|"plan"|"appendix"|"cenarios"|"pareto_table", "title":str, "path":str|None, "embedded":bool}, ...]
    Retorna dict {id: comentário}.
    """
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    base_defaults = {
        "main": "Tabela principal com ranking dos elegíveis e seus indicadores-chave e impacto estimado.",
        "plan": "Resumo do plano de ação com gargalos por perito e ganho esperado pós-medidas.",
        "appendix": "Tabela completa de peritos do período (sem cortes), para referência e auditoria.",
        "cenarios": "Resumo mensal: impacto real vs. cenários A/B/C aplicando reduções sobre IV_sel.",
        "pareto_table": "Top-10 de impacto com cumulativo — identificação dos poucos mais críticos (80/20).",
    }
    # fallback sem API
    if not tables_meta:
        return {}
    if not api_key:
        return {t["id"]: base_defaults.get(t["id"], f"Tabela: {t.get('title','')}") for t in tables_meta}

    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=api_key)
        model = os.getenv("ATESTMED_OPENAI_MODEL", "gpt-4o-mini")
        prompt = (
            "Você receberá um JSON com 'payload' (resumo do relatório) e 'tables' (lista de {id,title}). "
            "Escreva UMA frase curta (máx. 25 palavras) por item, explicando o que a tabela mostra e como usar. "
            "Não invente números. Responda somente um JSON {id: comentario}.\n\n"
            f"{json.dumps({'payload': summary_payload, 'tables': [{'id': t['id'], 'title': t.get('title','')} for t in tables_meta]}, ensure_ascii=False)}"
        )
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Você escreve legendas/observações técnicas objetivas e curtas."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=500,
        )
        txt = (resp.choices[0].message.content or "").strip()
        data = json.loads(txt)
        out = {}
        for t in tables_meta:
            out[t["id"]] = data.get(t["id"]) or base_defaults.get(t["id"], f"Tabela: {t.get('title','')}")
        return out
    except Exception:
        return {t["id"]: base_defaults.get(t["id"], f"Tabela: {t.get('title','')}") for t in tables_meta}

def build_action_plan(focus_records, peritos, args, mean_nc_br: float):
    """Gera plano de ação para os elegíveis focados, com impacto esperado e gargalos."""
    plan = []
    for r in focus_records:
        key = f"{r['nome']} [{r['siape']}]"
        agg = peritos.get(key)
        if not agg:
            continue
        base_impact = r.get("impacto_fila", 0.0)
        exp_imp, deltas = expected_impact_after_measures(agg, base_impact, args)
        ganho = max(0.0, base_impact - exp_imp)

        gargalos = []
        if agg.prod_por_hora < 50.0:
            gargalos.append("Produtividade < 50/h")
        if agg.pct_nc >= 2.0 * mean_nc_br:
            gargalos.append("%NC ≥ 2× média nacional")
        if agg.le15s >= 10:
            gargalos.append("≥10 análises ≤15s")
        if agg.overlaps > 0:
            gargalos.append("Sobreposição detectada")

        acoes = []
        if agg.prod_por_hora < 50.0:
            acoes.append("Coaching de produtividade e balanceamento de fila (metas e tempos padrão)")
        if agg.pct_nc >= 2.0 * mean_nc_br:
            acoes.append("Auditoria técnica de causas de NC + checklist padronizado")
        if agg.le15s >= 10:
            acoes.append("Revisão de triagem/limiares para ≤15s; amostragem 100% por 2 semanas")
        if agg.overlaps > 0:
            acoes.append("Bloqueio de sessões concorrentes e alerta de sobreposição no sistema")

        plan.append({
            "nome": r["nome"],
            "siape": r["siape"],
            "cr": r.get("cr",""),
            "dr": r.get("dr",""),
            "impacto_base": round(base_impact, 2),
            "impacto_esperado": round(exp_imp, 2),
            "ganho_esperado": round(ganho, 2),
            "gargalos": "; ".join(gargalos) if gargalos else "—",
            "acoes_recomendadas": "; ".join(acoes) if acoes else "—",
        })
    plan.sort(key=lambda x: (x["ganho_esperado"], x["impacto_base"]), reverse=True)
    return plan

def ai_proposals(summary_payload: Dict[str, Any], action_plan_csv: str) -> str:
    """Texto de propostas usando OpenAI (se houver), com instrução para NÃO inventar números (com debug)."""
    _dbg("ai_proposals() iniciou")
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("ATESTMED_OPENAI_MODEL", "gpt-4o-mini")
    _dbg(f"key? {bool(api_key)} | model={model} | action_plan_csv={action_plan_csv}")

    base = (
        "Propostas de intervenção organizadas por alavanca: produtividade, não conformidade, ≤15s e sobreposição. "
        "Priorizar peritos com maior ganho esperado (ver 'plano_acao.csv'). "
        "Medidas transversais: (i) metas de throughput e coaching semanal; (ii) auditoria técnica e checklist; "
        "(iii) revisão de triagem e limites operacionais p/ ≤15s; (iv) alerta/bloqueio de sessões sobrepostas; "
        "(v) monitoramento contínuo com reavaliação quinzenal do impacto na fila."
    )

    if not api_key:
        _dbg("sem OPENAI_API_KEY — retornando texto base (fallback)")
        return base + f"\n\nArquivo do plano de ação: {action_plan_csv}"

    prompt = (
        "Você é consultor(a) operacional. Escreva a seção 'Propostas' do relatório ATESTMED, "
        "com 5–8 recomendações acionáveis, prazos e responsáveis sugeridos. "
        "Use SOMENTE os números fornecidos no JSON (não invente valores). "
        "Inclua referência ao CSV do plano de ação.\n\n"
        f"JSON:\n{json.dumps(summary_payload, ensure_ascii=False, indent=2)}\n"
        f"Caminho do plano de ação: {action_plan_csv}\n"
    )

    # Tentativa 1: SDK novo (classe OpenAI)
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=api_key)
        _dbg("usando SDK OpenAI (classe OpenAI) para propostas")
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Você escreve recomendações operacionais claras e práticas."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=700,
        )
        txt = resp.choices[0].message.content.strip() if getattr(resp, "choices", None) else ""
        _dbg(f"OpenAI completou propostas? {bool(txt)}")
        if txt:
            return txt + f"\n\nArquivo do plano de ação: {action_plan_csv}"
    except Exception as e:
        _dbg(f"exceção proposals(OpenAI): {e!r} — tentando fallback ChatCompletion")

    # Tentativa 2: SDK legacy (openai.ChatCompletion)
    try:
        import openai  # type: ignore
        openai.api_key = api_key
        _dbg("usando SDK openai.ChatCompletion (fallback) para propostas")
        resp = openai.ChatCompletion.create(
            model=model,
            messages=[
                {"role": "system", "content": "Você escreve recomendações operacionais claras e práticas."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=700,
        )
        txt = resp["choices"][0]["message"]["content"].strip()
        _dbg(f"ChatCompletion completou propostas? {bool(txt)}")
        if txt:
            return txt + f"\n\nArquivo do plano de ação: {action_plan_csv}"
    except Exception as e:
        _dbg(f"exceção proposals(ChatCompletion): {e!r} — usando base local")

    return base + f"\n\nArquivo do plano de ação: {action_plan_csv}"

def build_org_report(path: str, titulo: str, periodo: str, tabela_csv_path: str,
                     resumo_txt: str, figs: List[str], explicacoes_txt: str,
                     fig_captions: Optional[Dict[str,str]] = None, extra_org_text: Optional[str] = None,
                     propostas_txt: Optional[str] = None, plano_acao_csv: Optional[str] = None,
                     cen_table_lines: Optional[List[str]] = None,
                     prepend_org_text: Optional[str] = None,
                     embed_tables: bool = False, embed_rows: int = 40,
                     full_table_csv_path: Optional[str] = None,
                     action_plan_csv_path: Optional[str] = None,
                     table_font_size: str = "scriptsize",
                     table_use_longtable: bool = False,
                     table_landscape: bool = False,
                     # Flags finos de landscape
                     landscape_main: Optional[bool] = None,
                     landscape_cenarios: Optional[bool] = None,
                     landscape_plan: Optional[bool] = None,
                     landscape_appendix: Optional[bool] = None,
                     # Ocultar seções
                     no_main_table_section: bool = False,
                     no_plan_section: bool = False,
                     no_scenarios_section: bool = False,
                     no_graphs: bool = False,
                     # Pareto
                     pareto_fig: Optional[str] = None,
                     pareto_table_lines: Optional[List[List[Any]]] = None,
                     # Files section
                     files_links: Optional[List[Tuple[str, str]]] = None,
                     # NOVO: comentários curtos para tabelas/listas
                     table_captions: Optional[Dict[str, str]] = None):
    lines: List[str] = []

    # Helper link
    def _org_link(p: Optional[str], label: Optional[str] = None) -> str:
        if not p:
            return ""
        try:
            return org_file_link(p, label)  # usa helper já definido no módulo
        except Exception:
            lab = label or os.path.basename(p)
            return f"[[file:{p}][{lab}]]"

    # Landscape resolve
    land_main = bool(table_landscape) or bool(landscape_main)
    land_cen  = bool(table_landscape) or bool(landscape_cenarios)
    land_plan = bool(table_landscape) or bool(landscape_plan)
    land_app  = bool(table_landscape) or bool(landscape_appendix)

    # Cabeçalho
    if prepend_org_text:
        lines.append(prepend_org_text.strip())
        lines.append("")
    else:
        lines.append(f"#+TITLE: {titulo}")
        lines.append("#+LANGUAGE: pt_BR")
        lines.append("#+OPTIONS: toc:nil num:t")
        lines.append("#+LATEX_CLASS: article")
        lines.append("#+LATEX_COMPILER: xelatex")
        lines.append("")

    if table_use_longtable:
        lines.append("#+LATEX_HEADER: \\usepackage{longtable}")
        lines.append("#+LATEX_HEADER: \\usepackage{booktabs}")
    if land_main or land_cen or land_plan or land_app:
        lines.append("#+LATEX_HEADER: \\usepackage{pdflscape}")
    lines.append("")

    # Período & resumo
    lines.append("* Período")
    lines.append(periodo)
    lines.append("")
    lines.append("* Resumo")
    lines.append(resumo_txt)
    lines.append("")

    # Tabela principal
    if not no_main_table_section:
        lines.append("* Tabela principal")
        if embed_tables:
            main_tbl = csv_to_org_table(tabela_csv_path, max_rows=embed_rows)
            lines.extend(emit_org_table_block(main_tbl,
                                              font_size=table_font_size,
                                              use_longtable=table_use_longtable,
                                              landscape=land_main))
            if table_captions and table_captions.get("main"):
                lines.append(table_captions["main"])
            lines.append("")
            lines.append(f"Arquivo completo: {_org_link(tabela_csv_path, 'CSV principal (completo)')}")
        else:
            lines.append(f"Arquivo: {_org_link(tabela_csv_path, 'CSV principal (ranking)')}")
            if table_captions and table_captions.get("main"):
                lines.append(table_captions["main"])
        lines.append("")

    # Gráficos
    if not no_graphs:
        lines.append("* Gráficos")
        for fig in figs:
            lines.append(f"[[{fig}]]")
            cap = (fig_captions or {}).get(fig)
            if cap:
                lines.append(cap)
        # Pareto (opcional)
        if pareto_fig:
            lines.append("")
            lines.append("* Pareto por impacto")
            lines.append(f"[[{pareto_fig}]]")
            if pareto_table_lines:
                lines.append("")
                lines.extend(emit_org_table_block(pareto_table_lines,
                                                  font_size=table_font_size,
                                                  use_longtable=table_use_longtable,
                                                  landscape=False))
                if table_captions and table_captions.get("pareto_table"):
                    lines.append(table_captions["pareto_table"])
        lines.append("")

    # Cenários
    if (cen_table_lines and (not no_scenarios_section)):
        lines.append("* Cenários (conforme melhorias.org)")
        lines.append("Aplicação das reduções A/B/C sobre IV_sel (Top-10 mensal).")
        lines.extend(emit_org_table_block(cen_table_lines,
                                          font_size=table_font_size,
                                          use_longtable=table_use_longtable,
                                          landscape=land_cen))
        if table_captions and table_captions.get("cenarios"):
            lines.append(table_captions["cenarios"])
        lines.append("")

    # Propostas
    if not no_plan_section:
        lines.append("* Propostas")
        if propostas_txt:
            lines.append(propostas_txt)
        if plano_acao_csv:
            if embed_tables and action_plan_csv_path and os.path.exists(action_plan_csv_path):
                lines.append("")
                lines.append("** Plano de ação (resumo)")
                ap_tbl = csv_to_org_table(action_plan_csv_path, max_rows=embed_rows)
                lines.extend(emit_org_table_block(ap_tbl,
                                                  font_size=table_font_size,
                                                  use_longtable=table_use_longtable,
                                                  landscape=land_plan))
                if table_captions and table_captions.get("plan"):
                    lines.append(table_captions["plan"])
                lines.append("")
                lines.append(f"Arquivo (plano de ação completo): {_org_link(action_plan_csv_path, 'plano_acao.csv')}")
            else:
                lines.append("")
                lines.append(f"Arquivo (plano de ação): {_org_link(plano_acao_csv, 'plano_acao.csv')}")
                if table_captions and table_captions.get("plan"):
                    lines.append(table_captions["plan"])
        lines.append("")

    # Explicações e metodologia
    lines.append("* Explicações e Metodologia")
    lines.append(explicacoes_txt)
    lines.append("")

    # Apêndice FULL (embed ou link)
    if embed_tables and full_table_csv_path and os.path.exists(full_table_csv_path):
        lines.append("* Apêndice — Tabela completa de peritos")
        full_tbl = csv_to_org_table(full_table_csv_path, max_rows=None)
        lines.extend(emit_org_table_block(full_tbl,
                                          font_size=table_font_size,
                                          use_longtable=table_use_longtable,
                                          landscape=land_app))
        if table_captions and table_captions.get("appendix"):
            lines.append(table_captions["appendix"])
        lines.append("")
    if (not embed_tables) and full_table_csv_path and os.path.exists(full_table_csv_path):
        lines.append("* Apêndice — Tabela completa de peritos (arquivo)")
        lines.append(_org_link(full_table_csv_path, os.path.basename(full_table_csv_path)))
        if table_captions and table_captions.get("appendix"):
            lines.append(table_captions["appendix"])
        lines.append("")

    # Apêndice de texto adicional
    if extra_org_text:
        lines.append("* Apêndice — Texto adicional (melhorias.org)")
        lines.append(extra_org_text)
        lines.append("")

    # Files section
    if files_links:
        lines.append("* Arquivos gerados")
        for label, fpath in files_links:
            lines.append(f"- {_org_link(fpath, label)}")
        lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def maybe_export_pdf(org_path: str, pdf_dir: str, engine: str = "emacs") -> Optional[str]:
    """Gera PDF via Emacs por padrão; fallback para pandoc com xelatex."""
    try:
        os.makedirs(pdf_dir, exist_ok=True)
        base = os.path.splitext(os.path.basename(org_path))[0]
        pdf_dst = os.path.join(pdf_dir, base + ".pdf")

        if engine == "emacs" and shutil.which("emacs"):
            env = os.environ.copy()
            texbin = "/usr/local/texlive/2024/bin/x86_64-linux"  # ajuste se necessário
            env["PATH"] = texbin + ":" + env.get("PATH", "")
            cmd = [
                "emacs", "--batch", org_path,
                "--eval", "(require 'ox-latex)",
                "--eval", "(setq org-export-in-background nil org-confirm-babel-evaluate nil "
                          "org-latex-pdf-process (list "
                          "\"xelatex -interaction nonstopmode -shell-escape -output-directory %o %f\" "
                          "\"xelatex -interaction nonstopmode -shell-escape -output-directory %o %f\"))",
                "-f", "org-latex-export-to-pdf",
            ]
            rc = subprocess.run(cmd, env=env).returncode
            if rc != 0:
                print("⚠️  Emacs export falhou; fallback para pandoc…")
            else:
                pdf_src = os.path.splitext(org_path)[0] + ".pdf"
                if os.path.exists(pdf_src):
                    try:
                        if os.path.abspath(pdf_src) != os.path.abspath(pdf_dst):
                            shutil.move(pdf_src, pdf_dst)
                    except Exception:
                        pass
                if os.path.exists(pdf_dst):
                    return pdf_dst
                if os.path.exists(pdf_src):
                    return pdf_src

        # Fallback: pandoc (força xelatex)
        pdf_path = pdf_dst
        rc = os.system(f'pandoc -f org -t pdf --pdf-engine=xelatex -o "{pdf_path}" "{org_path}"')
        if rc == 0 and os.path.exists(pdf_path):
            return pdf_path
        else:
            print("⚠️  pandoc retornou código", rc)
    except Exception as e:
        print("⚠️  Erro ao gerar PDF:", e)
    return None

# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────

def main():
    _load_dotenv_from_base()
    args = parse_args()

    # DEBUG_AI
    try:
        dbg_flag = bool(getattr(args, "debug_ai", False))
    except Exception:
        dbg_flag = False
    try:
        env_flag = (os.getenv("DEBUG_AI", "").strip() in ("1", "true", "True", "yes", "on"))
    except Exception:
        env_flag = False
    try:
        global DEBUG_AI
        DEBUG_AI = bool(dbg_flag or env_flag)
    except Exception:
        pass

    _dbg("main() iniciou")
    _dbg(f"args: {vars(args)}")

    # Semente
    try:
        np.random.seed(args.seed)
        _dbg(f"np.random.seed({args.seed}) aplicado")
    except Exception as e:
        _dbg(f"falha ao aplicar seed: {e!r}")

    # DB
    db_path = args.db if os.path.isabs(args.db) else os.path.join(BASE_DIR, args.db)
    _dbg(f"db_path resolvido: {db_path}")
    if not os.path.exists(db_path):
        print(f"❌ Banco não encontrado: {db_path}")
        sys.exit(2)

    # Período e dias úteis
    dt_start, dt_end = args.start, args.end
    ds = to_dt(dt_start).date()
    de = to_dt(dt_end).date()
    du_total = sum(business_days(y, m) for (y, m) in daterange_days(ds, de))
    _dbg(f"período: {dt_start}..{dt_end} | dias úteis (aprox): {du_total}")

    # Carrega linhas do período (com cache opcional)
    rows = load_or_cache_period_data(db_path, dt_start, dt_end, use_cache=bool(getattr(args, "use_cache", False)))
    _dbg(f"linhas brutas carregadas (pós-filtro de duração <=1h): {len(rows)}")
    if not rows:
        print("⚠️  Nenhuma análise no período após filtros (duração inválida/>1h removida).")
        sys.exit(0)

    # Agregação por perito
    mean_br = national_nc_mean(rows)
    peritos = aggregate_perito(rows)
    _dbg(f"peritos agregados: {len(peritos)} | média nacional %NC: {mean_br:.4f}")

    # Base de registros
    records: List[Dict[str, Any]] = []
    for _, agg in peritos.items():
        sc = score_ponderado_v2(agg, mean_br)
        impact = compute_impact(agg, args.impact_mode) if args.with_impact else 0.0
        records.append({
            "nome": agg.nome,
            "siape": agg.siape,
            "cr": agg.cr,
            "dr": agg.dr,
            "uf": agg.uf,
            "total_analises": agg.total,
            "horas_efetivas": round(agg.horas_efetivas, 4),
            "prod_por_hora": round(agg.prod_por_hora, 4),
            "le15s": agg.le15s,
            "overlaps": agg.overlaps,
            "pct_nc": round(agg.pct_nc, 4),
            "pct_le15s": round(agg.pct_le15s, 4),
            "pct_overlap": round(agg.pct_overlap, 4),
            "score_v2": sc,
            "impacto_fila": round(impact, 4),
        })
    _dbg(f"records construídos: {len(records)} (with_impact={args.with_impact}, mode={args.impact_mode})")

    # Seleção Fluxo B
    fluxo_b_tagged: set = set()
    if args.fluxo_b:
        eligibles: List[Dict[str, Any]] = []
        for r in records:
            k = f"{r['nome']} [{r['siape']}]"
            agg = peritos.get(k)
            if agg and fluxo_b_eligible(agg, mean_br, du_total):
                eligibles.append(r)
        eligibles.sort(key=lambda x: (x["pct_nc"], x["total_analises"]), reverse=True)
        if not args.fluxo_b_all:
            eligibles = eligibles[:max(1, int(args.fluxo_b_topk))]
        fluxo_b_tagged = {(e["nome"], e["siape"]) for e in eligibles}
        focus_records = eligibles
        _dbg(f"Fluxo B ON | elegíveis={len(eligibles)} | topk={args.fluxo_b_topk} | all={args.fluxo_b_all}")
    else:
        focus_records = records
        _dbg("Fluxo B OFF | foco=records completos")

    # Flag de elegibilidade
    if args.mark_fluxo_b:
        for r in records:
            r["fluxo_b_elegivel"] = int((r["nome"], r["siape"]) in fluxo_b_tagged)
        for r in focus_records:
            r["fluxo_b_elegivel"] = int((r["nome"], r["siape"]) in fluxo_b_tagged)
        _dbg("flag fluxo_b_elegivel adicionada às tabelas")

    # Ordenação final
    focus_records.sort(key=lambda x: (x["score_v2"], x["pct_nc"], x["total_analises"]), reverse=True)
    _dbg(f"focus_records ordenados | n={len(focus_records)}")

    # Exports: CSVs principal e completo
    periodo_txt = f"{dt_start} a {dt_end}"
    base_name = ("relatorio_fluxo_b_" if args.fluxo_b else "relatorio_kpi_") + f"{dt_start}_a_{dt_end}"
    csv_path = os.path.join(args.out_dir, base_name + ".csv")
    export_table_csv(csv_path, focus_records)
    full_csv = os.path.join(args.out_dir, base_name + "_FULL.csv")
    export_table_csv(full_csv, records)
    _dbg(f"CSVs exportados: csv={csv_path} | full={full_csv}")

    # Plano de ação
    action_plan = build_action_plan(focus_records, peritos, args, mean_nc_br=mean_br)
    plano_acao_csv = os.path.join(args.out_dir, base_name + "_plano_acao.csv")
    export_table_csv(plano_acao_csv, action_plan)
    _dbg(f"plano de ação gerado: n={len(action_plan)} | {plano_acao_csv}")

    # ---------------------------------------------
    # [COMENTÁRIOS] contexto-base e helper opcional
    # ---------------------------------------------
    comments_by_fig: Dict[str, str] = {}

    fb_ctx_base = {
        "periodo_inicio": dt_start,
        "periodo_fim": dt_end,
        "impact_mode": args.impact_mode,
        "n_elegiveis": len(focus_records),
        "soma_impacto": float(sum(r.get("impacto_fila", 0.0) for r in focus_records)) if args.with_impact else 0.0,
        "media_nc_brasil": float(mean_br),
    }

    def _coment(kind: str, extra_ctx: Optional[Dict[str, Any]] = None) -> str:
        """Opcional: usa utils.comentarios.comentar_artefato se existir; senão retorna ''. """
        fn = getattr(comentarios, "comentar_artefato", None)
        if callable(fn):
            try:
                ctx = dict(fb_ctx_base)
                if extra_ctx:
                    ctx.update(extra_ctx)
                return fn(kind, ctx, model=os.getenv("ATESTMED_OPENAI_MODEL", "gpt-4o-mini"))
            except Exception as e:
                _dbg(f"comentario falhou ({kind}): {e!r}")
                return ""
        return ""

    # Cenários (conforme melhorias.org)
    cen_table_org_lines: List[str] = []
    payload_extra: Dict[str, Any] = {}
    figs_for_cenarios: List[str] = []
    if getattr(args, "scenarios_follow_melhorias", False):
        reds = [float(x) for x in (args.scenarios_reductions.split(","))]
        labs = [x.strip() for x in (args.scenarios_labels.split(","))]
        monthly = compute_monthly_impacts(rows, args.impact_mode, args.scenarios_topk)
        cen_csv = os.path.join(args.out_dir, base_name + "_cenarios_mensais.csv")
        export_table_csv(cen_csv, monthly)
        cen_fig = os.path.join(args.out_dir, base_name + "_cenarios_mensais.png")
        title = "Impacto na fila — Real vs Cenários (Top-10 mensal; A=50%, B=70%, C=100% IV_sel)"
        fig_path, cen_table = build_cenarios_and_plot(monthly, reds, labs, cen_fig, title)
        if fig_path:
            figs_for_cenarios.append(fig_path)
            reds_str = ", ".join([f"{int(r*100)}%" for r in reds])
            comments_by_fig[fig_path] = _coment(
                "cenarios_mensais",
                {"topk_mensal": int(args.scenarios_topk),
                 "rotulos_cenarios": ", ".join(labs),
                 "reducoes_descritas": reds_str}
            )
        cen_table_org_lines = export_table_org_lines(cen_table)
        payload_extra = {
            "cenarios": {
                "labels": labs,
                "reductions": reds,
                "topk": int(args.scenarios_topk),
                "mensal_csv": cen_csv,
                "fig": fig_path
            }
        }
        _dbg(f"cenários gerados: meses={len(monthly)} | fig={fig_path} | csv={cen_csv}")

    # Gráficos
    figs: List[str] = []
    elegiveis_keyset = {(r["nome"], r["siape"]) for r in focus_records}
    _dbg(f"keyset elegíveis para gráficos: {len(elegiveis_keyset)}")

    # Impacto Top-20
    if args.with_impact and focus_records and not args.no_graphs:
        top_imp = sorted(focus_records, key=lambda x: x["impacto_fila"], reverse=True)[:20]
        vals = [r["impacto_fila"] for r in top_imp]
        lbls = [short_label(r["nome"]) for r in top_imp]
        f1 = os.path.join(args.out_dir, base_name + "_impacto_top20.png")
        save_bar(vals, lbls, "Impacto na fila — Top 20 elegíveis", f1, ylabel="Análises")
        figs.append(f1)
        comments_by_fig[f1] = _coment("impacto_top20")
        _dbg(f"fig impacto_top20 gerada: {f1} | n={len(vals)}")

    # Heatmap de critérios (até 10)
    if focus_records and not args.no_graphs:
        rows_hm, rlabels = [], []
        N_HM = min(10, len(focus_records))
        for r in focus_records[:N_HM]:
            k = f"{r['nome']} [{r['siape']}]"
            agg = peritos.get(k)
            if not agg:
                continue
            rows_hm.append([
                1.0 if agg.prod_por_hora >= 50.0 else 0.0,
                1.0 if agg.overlaps > 0 else 0.0,
                1.0 if agg.le15s >= 10 else 0.0,
                1.0 if agg.pct_nc >= 2.0 * mean_br else 0.0,
            ])
            rlabels.append(short_label(r["nome"]))
        f2 = os.path.join(args.out_dir, base_name + "_heatmap_score_flags.png")
        old_xtick = plt.rcParams.get("xtick.labelsize", None)
        old_ytick = plt.rcParams.get("ytick.labelsize", None)
        try:
            plt.rcParams["xtick.labelsize"] = 9
            plt.rcParams["ytick.labelsize"] = 8
            save_heatmap(rows_hm, rlabels, ["Prod≥50/h", "Overlap", "≤15s≥10", "%NC≥2×BR"],
                         "Heatmap — Critérios acionados (até 10 peritos)", f2)
        finally:
            if old_xtick is not None: plt.rcParams["xtick.labelsize"] = old_xtick
            if old_ytick is not None: plt.rcParams["ytick.labelsize"] = old_ytick
        figs.append(f2)
        comments_by_fig[f2] = _coment("heatmap_score_flags")
        _dbg(f"fig heatmap gerada: {f2} | linhas={len(rows_hm)}")

    # Distribuições por CR/DR/UF
    def plot_dist_by(field: str, title: str, suffix: str):
        if args.no_graphs or not focus_records:
            return
        if field == "cr" and args.cr_mode == "analises":
            counts = count_by_dimension_analises(rows, elegiveis_keyset, field)
            ylab = "Análises"; contagem_base = "análises"
        else:
            counts = count_by_dimension_peritos(focus_records, field)
            ylab = "Peritos"; contagem_base = "peritos"
        items = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        TOPN = 20
        if len(items) > TOPN:
            top = items[:TOPN-1]
            outros = sum(v for _, v in items[TOPN-1:])
            items = top + [("Outros", outros)]
        labels = [k for k, _ in items]
        values = [v for _, v in items]
        fpath = os.path.join(args.out_dir, base_name + f"_{suffix}.png")
        save_bar(values, labels, title, fpath, ylabel=ylab)
        figs.append(fpath)
        kind = {"cr": "dist_por_CR", "dr": "dist_por_DR", "uf": "dist_por_UF"}[field]
        comments_by_fig[fpath] = _coment(kind, {"contagem_base": contagem_base})
        _dbg(f"fig {suffix} gerada: {fpath} | categorias={len(items)}")

    if args.fluxo_b and focus_records:
        plot_dist_by("cr", "Distribuição por CR", "dist_por_CR")
        if getattr(args, "with_dist_dr", False):
            plot_dist_by("dr", "Distribuição por DR", "dist_por_DR")
        if getattr(args, "with_dist_uf", False):
            plot_dist_by("uf", "Distribuição por UF", "dist_por_UF")

    # Indicadores médios vs alvo
    if focus_records and not args.no_graphs:
        prod = float(np.mean([r["prod_por_hora"] for r in focus_records]))
        nc = float(np.mean([r["pct_nc"] for r in focus_records]))
        le15 = float(np.mean([r["pct_le15s"] for r in focus_records]))
        ov = float(np.mean([r["pct_overlap"] for r in focus_records]))
        vals = [prod, 50.0, nc, le15, ov]
        lbls = ["Prod/h (média)", "Alvo (50/h)", "%NC (média)", "≤15s% (média)", "Overlap% (média)"]
        f4 = os.path.join(args.out_dir, base_name + "_indicadores_medios.png")
        save_bar(vals, lbls, "Indicadores médios — Elegíveis", f4, ylabel="Valor / %")
        figs.append(f4)
        comments_by_fig[f4] = _coment("indicadores_medios")
        _dbg(f"fig indicadores_medios gerada: {f4}")

    # Impacto esperado pós-medidas — Top 20
    if args.with_impact and focus_records and not args.no_graphs:
        top_imp = sorted(focus_records, key=lambda x: x["impacto_fila"], reverse=True)[:20]
        exp_vals, lbls = [], []
        for r in top_imp:
            k = f"{r['nome']} [{r['siape']}]"
            agg = peritos.get(k)
            if not agg:
                exp_vals.append(r["impacto_fila"])
            else:
                exp_imp, _ = expected_impact_after_measures(agg, r["impacto_fila"], args)
                exp_vals.append(exp_imp)
            lbls.append(short_label(r["nome"]))
        f5 = os.path.join(args.out_dir, base_name + "_impacto_esperado_pos_medidas_top20.png")
        save_bar(exp_vals, lbls, "Impacto esperado pós-medidas — Top 20", f5, ylabel="Análises (esperado)")
        figs.append(f5)
        comments_by_fig[f5] = _coment("impacto_esperado_pos_medidas_top20")
        _dbg(f"fig impacto_esperado gerada: {f5}")

    # Curva de Lorenz do impacto
    impact_gini = None
    if args.with_impact and getattr(args, "with_impact_lorenz", False) and focus_records and not args.no_graphs:
        impacts_all = [float(r.get("impacto_fila", 0.0)) for r in focus_records]
        f_lorenz = os.path.join(args.out_dir, base_name + "_lorenz_impacto_elegiveis.png")
        gini = save_lorenz_impact(impacts_all, f_lorenz, title="Curva de Lorenz — Impacto entre elegíveis")
        if gini is not None:
            figs.append(f_lorenz)
            impact_gini = float(gini)
            comments_by_fig[f_lorenz] = _coment("lorenz_impacto_elegiveis", {"gini": float(gini)})
        _dbg(f"fig lorenz gerada: {f_lorenz} | gini={impact_gini}")

    # Anexa figura de cenários
    figs.extend(figs_for_cenarios)

    # Pareto (opcional)
    pareto_fig = None
    pareto_table_lines = None
    if args.with_impact and getattr(args, "with_pareto", False) and focus_records and not args.no_graphs:
        vals = [(short_label(r["nome"]), float(r.get("impacto_fila", 0.0))) for r in focus_records]
        vals = [v for v in vals if v[1] > 0]
        if vals:
            vals_sorted = sorted(vals, key=lambda x: x[1], reverse=True)[:20]
            labels = [n for n, _ in vals_sorted]
            values = [v for _, v in vals_sorted]
            pareto_fig = os.path.join(args.out_dir, base_name + "_pareto_impacto.png")
            plt.figure()
            x = np.arange(len(values))
            plt.bar(x, values)
            cum = np.cumsum(values) / sum(values)
            plt.plot(x, cum * max(values), marker="o", linestyle="--")
            plt.xticks(x, labels, rotation=45, ha="right")
            plt.title("Pareto por impacto (Top-20)")
            plt.ylabel("Impacto (barras) / Cumulativo (linha)")
            plt.tight_layout()
            plt.savefig(pareto_fig, dpi=160)
            plt.close()
            pareto_table = build_pareto_table(vals, topk=10)
            pareto_table_lines = export_table_org_lines(pareto_table)
            figs.append(pareto_fig)
            comments_by_fig[pareto_fig] = _coment("pareto_impacto")
            _dbg(f"fig pareto gerada: {pareto_fig} | tabela pareto top10 montada")

    # Estatísticas robustas (mediana/P90)
    robust_summary = None
    if getattr(args, "with_robust_stats", False) and focus_records and not args.no_graphs:
        by_key_durs: Dict[Tuple[str, str], List[float]] = {}
        for r in rows:
            nome = human_title_name(str(r.get("nomePerito", "")).strip())
            siape = str(r.get("siapePerito", "")).strip()
            if (nome, siape) not in elegiveis_keyset:
                continue
            try:
                d = float(r.get("duracao_segundos"))
            except Exception:
                continue
            by_key_durs.setdefault((nome, siape), []).append(d)

        stats = []
        for (nome, siape), dlist in by_key_durs.items():
            med, p90 = robust_stats(dlist)
            stats.append((nome, med, p90, len(dlist)))

        if stats:
            stats.sort(key=lambda x: x[3], reverse=True)
            top = stats[:20]
            labels = [short_label(n) for (n, _, _, _) in top]
            med_vals = [m for (_, m, _, _) in top]
            p90_vals = [p for (_, _, p, _) in top]
            robust_fig = os.path.join(args.out_dir, base_name + "_duracao_mediana_p90_top20.png")
            x = np.arange(len(labels))
            plt.figure()
            plt.bar(x - 0.2, med_vals, width=0.4, label="Mediana (s)")
            plt.bar(x + 0.2, p90_vals, width=0.4, label="P90 (s)")
            plt.xticks(x, labels, rotation=45, ha="right")
            plt.title("Duração por perito — Mediana e P90 (Top-20 por volume)")
            plt.ylabel("segundos")
            plt.legend()
            plt.tight_layout()
            plt.savefig(robust_fig, dpi=160)
            plt.close()
            figs.append(robust_fig)

            all_durs = [d for dl in by_key_durs.values() for d in dl]
            gmed, gp90 = robust_stats(all_durs)
            robust_summary = f"Mediana global: {gmed:.1f}s; P90 global: {gp90:.1f}s."
            comments_by_fig[robust_fig] = _coment("duracao_mediana_p90_top20",
                                                  {"mediana_global": float(gmed), "p90_global": float(gp90)})
            _dbg(f"fig robust_stats gerada: {robust_fig} | resumo: {robust_summary}")

    # Resumo + payload (para IA de legendas/textos)
    resumo_lines = [
        f"Média nacional de não conformidade no período: {mean_br:.2f}%.",
        f"Dias úteis (aprox.): {du_total}.",
        "Produtividade efetiva = total de análises / horas efetivas (soma das durações válidas ≤ 1h).",
    ]
    if args.fluxo_b:
        resumo_lines.append(f"Fluxo B habilitado. Elegíveis considerados: {len(focus_records)}.")
    if args.with_impact:
        resumo_lines.append(f"Soma do impacto (conjunto reportado): {sum(r['impacto_fila'] for r in focus_records):.0f} análises.")
    if robust_summary:
        resumo_lines.append(robust_summary)
    if 'impact_gini' in locals() and impact_gini is not None:
        resumo_lines.append(f"Índice de Gini do impacto (elegíveis): {impact_gini:.3f}.")
    resumo_txt = "\n".join(resumo_lines)

    payload = {
        "periodo": {"inicio": dt_start, "fim": dt_end},
        "media_nc_brasil": mean_br,
        "dias_uteis_aprox": du_total,
        "fluxo_b": bool(args.fluxo_b),
        "elegiveis": len(focus_records),
        "soma_impacto": float(sum(r.get("impacto_fila", 0.0) for r in focus_records)) if args.with_impact else 0.0,
        "indicadores_medios": {
            "prod_h": float(np.mean([r["prod_por_hora"] for r in focus_records])) if focus_records else 0.0,
            "pct_nc": float(np.mean([r["pct_nc"] for r in focus_records])) if focus_records else 0.0,
            "pct_le15s": float(np.mean([r["pct_le15s"] for r in focus_records])) if focus_records else 0.0,
            "pct_overlap": float(np.mean([r["pct_overlap"] for r in focus_records])) if focus_records else 0.0,
        },
        "graficos": [os.path.basename(f) for f in figs],
    }
    payload.update(payload_extra)
    _dbg(f"payload p/ IA montado | figs={len(figs)} | with_files_section={getattr(args,'with_files_section',False)}")

    # Propostas
    propostas_txt = None
    if getattr(args, "propostas_from_file", None):
        propostas_txt = extract_proposals_from_org(args.propostas_from_file)
        _dbg(f"propostas_from_file: {bool(propostas_txt)}")
    if not propostas_txt:
        propostas_txt = ai_proposals(payload, plano_acao_csv)
        _dbg(f"propostas geradas via IA? {bool(propostas_txt)}")

    # Explicações + legendas
    explicacoes = ai_explanations(payload)
    _dbg(f"explicações IA obtidas? {bool(explicacoes)}")

    fc_auto = ai_figure_captions(payload, figs) if figs else {}
    # Manual/“opcional” sobrescreve o automático
    fig_captions = {**fc_auto, **{k: v for k, v in comments_by_fig.items() if v}}
    _dbg(f"legendas de figuras (merge): {len(fig_captions)} itens")

    # Anotar legendas nos PNGs (se pedido)
    if getattr(args, "annotate_figures", False) and fig_captions:
        ann_count = 0
        for f in figs:
            cap = fig_captions.get(f)
            if cap:
                annotate_png_with_text(f, cap)
                ann_count += 1
        _dbg(f"anotações em PNGs realizadas: {ann_count}")

    # PREPEND .org (melhorias.org)
    prepend_org_text = None
    if getattr(args, "prepend_org_file", None):
        try:
            with open(args.prepend_org_file, "r", encoding="utf-8") as f:
                prepend_org_text = f.read()
            _dbg(f"prepend_org_file carregado: {args.prepend_org_file}")
        except Exception as e:
            _dbg(f"falha ao ler prepend_org_file: {e!r}")
            prepend_org_text = None

    # Seção “Arquivos gerados”
    files_links: List[Tuple[str, str]] = []
    if getattr(args, "with_files_section", False):
        files_links = [
            ("Relatório ORG", os.path.join(args.org_dir, base_name + ".org")),
            ("Relatório PDF", os.path.join(args.pdf_dir, base_name + ".pdf")),
            ("CSV principal (ranking)", csv_path),
            ("CSV completo", full_csv),
            ("Plano de ação", plano_acao_csv),
        ]
        if getattr(args, "scenarios_follow_melhorias", False):
            files_links.append(("Cenários (CSV)", os.path.join(args.out_dir, base_name + "_cenarios_mensais.csv")))
            files_links.append(("Cenários (PNG)", os.path.join(args.out_dir, base_name + "_cenarios_mensais.png")))
        for f in figs:
            files_links.append((os.path.basename(f), f))
        _dbg(f"files section: {len(files_links)} links")

    # Comentários de TABELAS (tenta AI centralizada; fallback para _coment)
    table_captions: Dict[str, str] = {}
    tables_meta = [
        {"id": "main",        "title": "Tabela principal (ranking)"},
        {"id": "plan",        "title": "Plano de ação (resumo)"},
        {"id": "appendix",    "title": "Apêndice — tabela completa"},
        {"id": "cenarios",    "title": "Cenários mensais"},
        {"id": "pareto_table","title": "Pareto do impacto"},
    ]
    fn_ai_tab = getattr(comentarios, "ai_table_captions", None)
    if callable(fn_ai_tab):
        try:
            table_captions = fn_ai_tab(payload, tables_meta)
        except Exception as e:
            _dbg(f"ai_table_captions falhou: {e!r}")
            table_captions = {}
    # Completa faltantes com _coment() e defaults curtos
    defaults_tab = {
        "main": "Tabela principal com ranking dos elegíveis e impacto estimado.",
        "plan": "Resumo do plano de ação por perito com ganho esperado.",
        "appendix": "Tabela completa de peritos do período (referência/auditoria).",
        "cenarios": "Resumo mensal: impacto real vs. cenários A/B/C sobre IV_sel.",
        "pareto_table": "Top-10 de impacto com % acumulado.",
    }
    table_captions.setdefault("main", _coment("tabela_principal_csv") or defaults_tab["main"])
    table_captions.setdefault("appendix", _coment("tabela_full_csv") or defaults_tab["appendix"])
    table_captions.setdefault("plan", _coment("plano_acao_csv") or defaults_tab["plan"])
    if cen_table_org_lines:
        red_list = (payload_extra.get("cenarios", {}) or {}).get("reductions", [])
        lab_list = (payload_extra.get("cenarios", {}) or {}).get("labels", [])
        reds_str = ", ".join([f"{int(r*100)}%" for r in red_list]) if red_list else "50%, 70%, 100%"
        labs_str = ", ".join(lab_list) if lab_list else "A, B, C"
        table_captions.setdefault("cenarios",
            _coment("cenarios_csv", {
                "topk_mensal": int(getattr(args, "scenarios_topk", 10)),
                "rotulos_cenarios": labs_str,
                "reducoes_descritas": reds_str
            }) or defaults_tab["cenarios"]
        )
    if pareto_table_lines:
        table_captions.setdefault("pareto_table", _coment("pareto_impacto") or defaults_tab["pareto_table"])

    # Export .org / PDF
    if args.export_org or args.export_pdf:
        org_path = os.path.join(args.org_dir, base_name + ".org")
        _dbg(f"gerando ORG em {org_path}")
        build_org_report(
            org_path,
            titulo="Relatório KPI — Fluxo B + Impacto + Cenários",
            periodo=periodo_txt,
            tabela_csv_path=csv_path,
            resumo_txt=resumo_txt,
            figs=figs if not args.no_graphs else [],
            explicacoes_txt=explicacoes,
            fig_captions=fig_captions,
            extra_org_text=None,
            propostas_txt=(None if args.no_plan_section else propostas_txt),
            plano_acao_csv=(None if args.no_plan_section else plano_acao_csv),
            cen_table_lines=(None if args.no_scenarios_section else cen_table_org_lines),
            prepend_org_text=prepend_org_text,
            embed_tables=bool(getattr(args, "embed_tables", False)),
            embed_rows=int(getattr(args, "embed_rows", 40)),
            full_table_csv_path=full_csv,
            action_plan_csv_path=plano_acao_csv,
            table_font_size=getattr(args, "table_font_size", "scriptsize"),
            table_use_longtable=bool(getattr(args, "table_use_longtable", False)),
            table_landscape=False if getattr(args, "no_landscape", False) else bool(getattr(args, "table_landscape", False)),
            landscape_main=False if getattr(args, "no_landscape", False) else bool(getattr(args, "landscape_main", False)),
            landscape_cenarios=False if getattr(args, "no_landscape", False) else bool(getattr(args, "landscape_cenarios", False)),
            landscape_plan=False if getattr(args, "no_landscape", False) else bool(getattr(args, "landscape_plan", False)),
            landscape_appendix=False if getattr(args, "no_landscape", False) else bool(getattr(args, "landscape_appendix", False)),
            no_main_table_section=bool(getattr(args, "no_main_table_section", False)),
            no_plan_section=bool(getattr(args, "no_plan_section", False)),
            no_scenarios_section=bool(getattr(args, "no_scenarios_section", False)),
            no_graphs=bool(getattr(args, "no_graphs", False)),
            pareto_fig=pareto_fig,
            pareto_table_lines=pareto_table_lines,
            files_links=files_links if getattr(args, "with_files_section", False) else None,
            table_captions=table_captions,
        )
        print(f"✅ Org gerado: {org_path}")
        if args.export_pdf:
            _dbg(f"exportando PDF via engine={args.pdf_engine}")
            pdf_path = maybe_export_pdf(org_path, args.pdf_dir, engine=args.pdf_engine)
            if pdf_path:
                print(f"✅ PDF gerado: {pdf_path}")
            else:
                print("⚠️  Falha ao gerar PDF (Emacs/pandoc). Verifique PATH e engines.")

    # ZIP bundle (opcional)
    if getattr(args, "zip_bundle", False):
        zip_path = os.path.join(args.out_dir, base_name + "_bundle.zip")
        bundle_files = [csv_path, full_csv, plano_acao_csv] + figs
        org_candidate = os.path.join(args.org_dir, base_name + ".org")
        pdf_candidate = os.path.join(args.pdf_dir, base_name + ".pdf")
        if os.path.exists(org_candidate): bundle_files.append(org_candidate)
        if os.path.exists(pdf_candidate): bundle_files.append(pdf_candidate)
        cen_csv = os.path.join(args.out_dir, base_name + "_cenarios_mensais.csv")
        cen_png = os.path.join(args.out_dir, base_name + "_cenarios_mensais.png")
        if os.path.exists(cen_csv): bundle_files.append(cen_csv)
        if os.path.exists(cen_png): bundle_files.append(cen_png)
        manifest_path = os.path.join(args.out_dir, base_name + "_figs_manifest.json")
        if os.path.exists(manifest_path): bundle_files.append(manifest_path)
        bundle_files = list(dict.fromkeys(p for p in bundle_files if p and os.path.exists(p)))
        create_zip_bundle(zip_path, bundle_files)
        print(f"✅ Bundle ZIP gerado: {zip_path}")
        _dbg(f"ZIP criado com {len(bundle_files)} itens")

    # Sumário final
    print(f"✅ Tabelas salvas:\n   - {csv_path}\n   - {full_csv}\n   - {plano_acao_csv}")
    if getattr(args, "scenarios_follow_melhorias", False):
        print("✅ Cenários gerados (mensal):")
        print(f"   - {os.path.join(args.out_dir, base_name + '_cenarios_mensais.csv')}")
        print(f"   - {os.path.join(args.out_dir, base_name + '_cenarios_mensais.png')}")
    if figs and not args.no_graphs:
        print("✅ Gráficos salvos:")
        for f in figs:
            print(f"   - {f}")
    _dbg("main() finalizado")

if __name__ == "__main__":
    main()
