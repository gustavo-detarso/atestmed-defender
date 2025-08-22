#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Impacto na Fila (ATESTMED) — IV do período, ΔTMEA, Bootstrap, Sensibilidade, Estratos,
Testes estatísticos (Binomial, Beta-binomial, Permutação do Peso, CMH) e ORG Final.

NOVIDADES DESTE ARQUIVO:
- --test-binomial           → teste binomial unilateral p_i > p_BR + IC Wilson + FDR
- --betabin                 → teste beta-binomial (ρ por MoM) + FDR
- --permute-weight N        → permutação do peso w (p-valor) + histograma
- --cmh BY / "by=BY"        → CMH 2×2×K por CR/DR/UO (OR_MH, X2_MH, p)
- --psa N                   → sensibilidade probabilística (α ~ Beta, p_BR ~ Beta) + histograma e ICs de w
- --all-tests               → executa tudo acima (permute=5000, psa=10000, cmh=cr)

NOVIDADES (INTEGRAÇÕES/SEÇÕES):
- Comentário para CADA teste com utils.comentarios (quando disponível)
- --per-top10-sections      → subseções por perito do TopN com impacto e estatísticas (comentadas)
- --top10-aggregate         → bloco agregado do TopN (IV_top, peso_top, ΔTMEA_top) + comentário
- --explain-methods         → seção opcional de metodologia (fórmulas e definições)

Mantém: --bootstrap-peso, --bootstrap-recalc-sstar, --sens-plot, --sens-alpha-frac, --sens-pbr-pp,
--by (sumários estratificados), --final-org (header + front + org gerado) etc.

Arredondamentos (ceil): E_i, IV_i, IV_sel, IV_período e ΔTMEA.
"""

import os, sys, re, json, math, sqlite3, argparse, subprocess, shutil
from typing import Dict, Any, Tuple, Optional, List
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import numpy as np
import pandas as pd
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    import plotext as p  # opcional para gráfico ASCII
except Exception:
    p = None

BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# ──────────────────────────────────────────────────────────────────────
# Integração GPT (comentários)
# ──────────────────────────────────────────────────────────────────────
_COMENT_FUNC = None
_COMENT_BIN = _COMENT_BB = _COMENT_PERM = _COMENT_CMH = _COMENT_PSA = None
_COMENT_TOPN = _COMENT_PERITO = None
try:
    from utils.comentarios import (
        comentar_impacto_fila as _COMENT_FUNC,              # visão geral
        comentar_teste_binomial as _COMENT_BIN,             # teste binomial
        comentar_teste_betabin as _COMENT_BB,               # teste beta-binomial
        comentar_teste_permutacao as _COMENT_PERM,          # permutação do peso
        comentar_teste_cmh as _COMENT_CMH,                  # CMH
        comentar_psa as _COMENT_PSA,                        # PSA
        comentar_topn_aggregate as _COMENT_TOPN,            # agregado TopN
        comentar_perito_top as _COMENT_PERITO,              # comentário por perito
    )
except Exception:
    # fallback seguro se o módulo não estiver disponível
    pass

# ──────────────────────────────────────────────────────────────────────
# Pequenas utilidades estatísticas (sem SciPy/Statsmodels)
# ──────────────────────────────────────────────────────────────────────

def _log_binom_pmf(k: int, n: int, p: float) -> float:
    if p <= 0.0:
        return 0.0 if k == 0 else -np.inf
    if p >= 1.0:
        return 0.0 if k == n else -np.inf
    from math import lgamma, log
    return (lgamma(n+1) - lgamma(k+1) - lgamma(n-k+1) +
            k*log(p) + (n-k)*log(1.0-p))

def _binom_sf_one_sided(k_obs: int, n: int, p0: float) -> float:
    """P[X >= k_obs] para Bin(n,p0). Usa log-sum-exp para estabilidade."""
    ks = np.arange(k_obs, n+1, dtype=int)
    lps = np.array([_log_binom_pmf(int(k), n, p0) for k in ks], dtype=float)
    m = np.max(lps)
    return float(np.exp(lps - m).sum() * np.exp(m))

def _wilson_ci(k: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    denom = 1 + z*z/n
    center = (p + z*z/(2*n)) / denom
    half = (z * math.sqrt((p*(1-p) + z*z/(4*n))/n)) / denom
    low = max(0.0, center - half)
    high = min(1.0, center + half)
    return (low, high)

def _p_adjust_bh(pvals: np.ndarray) -> np.ndarray:
    """Benjamini–Hochberg FDR."""
    n = len(pvals)
    order = np.argsort(pvals)
    ranked = pvals[order]
    q = np.empty(n, dtype=float)
    prev = 1.0
    for i in range(n-1, -1, -1):
        val = ranked[i] * n / (i+1)
        prev = min(prev, val)
        q[i] = prev
    out = np.empty(n, dtype=float)
    out[order] = np.minimum(q, 1.0)
    return out

def _log_beta(a: float, b: float) -> float:
    from math import lgamma
    return lgamma(a) + lgamma(b) - lgamma(a+b)

def _log_betabinom_pmf(k: int, n: int, a: float, b: float) -> float:
    """log PMF: C(n,k) * B(k+a, n-k+b) / B(a,b)"""
    from math import lgamma
    return (lgamma(n+1) - lgamma(k+1) - lgamma(n-k+1)
            + _log_beta(k + a, n - k + b) - _log_beta(a, b))

def _betabin_sf_one_sided(k_obs: int, n: int, a: float, b: float) -> float:
    ks = np.arange(k_obs, n+1, dtype=int)
    lps = np.array([_log_betabinom_pmf(int(k), n, a, b) for k in ks], dtype=float)
    m = np.max(lps)
    return float(np.exp(lps - m).sum() * np.exp(m))

def _estimate_rho_mom(N: np.ndarray, NC: np.ndarray, p: float) -> float:
    """ρ (ICC) por method-of-moments no beta-binomial: Var[Y] = N p (1-p) (1 + (N-1) ρ)."""
    Y = NC.astype(float)
    num = float(((Y - N*p)**2 - N*p*(1-p)).sum())
    den = float((N*p*(1-p)*(N-1)).sum())
    if den <= 0:
        return 0.0
    rho = num / den
    return float(min(max(rho, 0.0), 0.9999))

def _mh_cmh_test(strata_tables: List[Tuple[int,int,int,int]]) -> Tuple[float,float,float]:
    """
    Cochran–Mantel–Haenszel para 2x2xK.
    Cada estrato: (a,b,c,d) com:
      a=NC_sel, b=Conf_sel, c=NC_nsel, d=Conf_nsel
    Retorna (OR_MH, X2_MH, p_value).
    """
    from math import erf
    eps = 1e-9
    sum_num = 0.0
    sum_den = 0.0
    sum_E = 0.0
    sum_V = 0.0
    A_sum = 0.0
    for (a,b,c,d) in strata_tables:
        a=float(a); b=float(b); c=float(c); d=float(d)
        n = a+b+c+d
        if n <= 0:
            continue
        sum_num += (a*d)/max(n, eps)
        sum_den += (b*c)/max(n, eps)
        row1 = a + b; row2 = c + d
        col1 = a + c; col2 = b + d
        Ek = (row1 * col1) / max(n, eps)
        Vk = (row1 * row2 * col1 * col2) / max(n*n*(n-1), eps)
        sum_E += Ek
        sum_V += Vk
        A_sum += a
    OR_MH = (sum_num / max(sum_den, 1e-12)) if sum_den > 0 else float('inf')
    X2_MH = ((A_sum - sum_E)**2) / sum_V if sum_V > 0 else 0.0
    try:
        pval = float(1.0 - (erf(math.sqrt(X2_MH/2.0))))
    except Exception:
        pval = float(np.exp(-0.5*X2_MH))
    return (OR_MH, X2_MH, max(min(pval,1.0),0.0))

_PT_MONTHS = {
    1:"janeiro", 2:"fevereiro", 3:"março", 4:"abril", 5:"maio", 6:"junho",
    7:"julho", 8:"agosto", 9:"setembro", 10:"outubro", 11:"novembro", 12:"dezembro"
}
def _date_pt_br(dt: datetime) -> str:
    try:
        return f"{dt.day:02d} de {_PT_MONTHS.get(dt.month, dt.month)} de {dt.year}"
    except Exception:
        return dt.strftime("%Y-%m-%d")

# ──────────────────────────────────────────────────────────────────────
# OpenAI helpers (para comentários fallback internos)
# ──────────────────────────────────────────────────────────────────────

def _load_openai_key_from_dotenv(env_path: str) -> Optional[str]:
    if not os.path.exists(env_path):
        return os.getenv("OPENAI_API_KEY")
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(env_path, override=False)
        if os.getenv("OPENAI_API_KEY"):
            return os.getenv("OPENAI_API_KEY")
    except Exception:
        pass
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k,v = line.split("=",1)
                if k.strip()=="OPENAI_API_KEY":
                    v=v.strip().strip('"').strip("'")
                    if v:
                        os.environ.setdefault("OPENAI_API_KEY", v)
                        return v
    except Exception:
        pass
    return os.getenv("OPENAI_API_KEY")

def _call_openai_chat(messages: List[Dict[str, str]], model: str = "gpt-4o-mini", temperature: float = 0.2) -> Optional[str]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(model=model, messages=messages, temperature=temperature)
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        pass
    try:
        import openai  # type: ignore
        openai.api_key = api_key
        resp = openai.ChatCompletion.create(model=model, messages=messages, temperature=temperature)
        return (resp["choices"][0]["message"]["content"] or "").strip()
    except Exception:
        pass
    return None

def _protect_comment_text(text: str, word_cap: int = 220) -> str:
    if not text:
        return ""
    text = re.sub(r"^```.*?$", "", text, flags=re.M)
    text = re.sub(r"^~~~.*?$", "", text, flags=re.M)
    kept=[]
    for ln in text.splitlines():
        t=ln.strip()
        if not t or t.startswith("[") and t.endswith("]") or t.startswith("|") or t.startswith("#+"):
            continue
        kept.append(ln)
    out=" ".join(" ".join(kept).split())
    words=out.split()
    if len(words)>word_cap:
        out=" ".join(words[:word_cap]).rstrip()+"…"
    return out

def _fmt2(x):
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "—"

def _fmtp2(x):
    """Duas casas decimais; usa notação científica se < 0,01."""
    try:
        v = float(x)
        return f"{v:.2f}" if v >= 0.01 else f"{v:.2e}"
    except Exception:
        return "—"
        
# ──────────────────────────────────────────────────────────────────────
# Schema helpers
# ──────────────────────────────────────────────────────────────────────

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",(name,)).fetchone()
    return row is not None

def _cols(conn: sqlite3.Connection, table: str) -> set:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}

def _detect_schema(conn: sqlite3.Connection) -> Dict[str, Any]:
    table=None
    for t in ('analises','analises_atestmed'):
        if _table_exists(conn,t):
            table=t; break
    if not table:
        raise RuntimeError("Não encontrei as tabelas 'analises' nem 'analises_atestmed'.")
    cset=_cols(conn,table)
    if not _table_exists(conn,'peritos') or 'nomePerito' not in _cols(conn,'peritos'):
        raise RuntimeError("Tabela 'peritos' ausente ou sem 'nomePerito'.")
    if not _table_exists(conn,'indicadores'):
        raise RuntimeError("Tabela 'indicadores' ausente (precisa de scoreFinal).")
    motivo_col     = 'motivoNaoConformado' if 'motivoNaoConformado' in cset else None
    has_conformado = 'conformado' in cset
    date_col       = 'dataHoraIniPericia' if 'dataHoraIniPericia' in cset else None
    if not date_col:
        raise RuntimeError(f"Tabela '{table}' sem 'dataHoraIniPericia'.")
    has_protocolo  = 'protocolo' in cset
    has_protocolos = _table_exists(conn,'protocolos')
    return {
        'table':table,'motivo_col':motivo_col,'has_conformado':has_conformado,
        'date_col':date_col,'has_protocolo':has_protocolo,'has_protocolos_table':has_protocolos
    }

def _cond_nc_total(has_conf: bool, motivo_col: Optional[str]) -> str:
    if has_conf and motivo_col:
        return (" (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
                " OR (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
                "     AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) ")
    elif has_conf and not motivo_col:
        return " (CAST(COALESCE(NULLIF(TRIM(a.conformado),''),'1') AS INTEGER) = 0) "
    elif (not has_conf) and motivo_col:
        return (" (TRIM(COALESCE(a.motivoNaoConformado,'')) <> '' "
                "  AND CAST(COALESCE(NULLIF(TRIM(a.motivoNaoConformado),''),'0') AS INTEGER) <> 0) ")
    return " 0 "

# ──────────────────────────────────────────────────────────────────────
# Queries e cálculos principais
# ──────────────────────────────────────────────────────────────────────

def _fetch_perito_n_nc(conn: sqlite3.Connection, start: str, end: str, schema: Dict[str, Any]) -> pd.DataFrame:
    t=schema['table']; date_col=schema['date_col']; cond_nc=_cond_nc_total(schema['has_conformado'], schema['motivo_col'])
    use_pr=bool(schema.get('has_protocolo') and schema.get('has_protocolos_table'))
    join_prot="LEFT JOIN protocolos pr ON pr.protocolo = a.protocolo" if use_pr else ""
    sel_cr = "MAX(pr.cr) AS cr" if use_pr else "MAX(p.cr) AS cr"
    sel_dr = "MAX(pr.dr) AS dr" if use_pr else "MAX(p.dr) AS dr"
    sel_uo = "MAX(pr.uo) AS uo" if use_pr else "NULL AS uo"
    sql=f"""
        SELECT p.nomePerito,
               COUNT(*) AS N,
               SUM(CASE WHEN {cond_nc} THEN 1 ELSE 0 END) AS NC,
               {sel_cr}, {sel_dr}, {sel_uo}
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
          {join_prot}
         WHERE substr(a.{date_col},1,10) BETWEEN ? AND ?
         GROUP BY p.nomePerito
    """
    df=pd.read_sql_query(sql, conn, params=(start,end))
    for col in ("N","NC"):
        if col in df.columns: df[col]=df[col].astype(int)
    return df

def _fetch_scores(conn: sqlite3.Connection) -> pd.DataFrame:
    sql = """
        SELECT i.perito AS siape, i.scoreFinal AS score_final, p.nomePerito
          FROM indicadores i
          JOIN peritos p ON p.siapePerito = i.perito
    """
    df=pd.read_sql_query(sql, conn)
    return df[["nomePerito","score_final"]].drop_duplicates()

def _compute_p_br_and_totals(conn: sqlite3.Connection, start: str, end: str, schema: Dict[str, Any]) -> Tuple[float,int,int]:
    t=schema['table']; date_col=schema['date_col']; cond_nc=_cond_nc_total(schema['has_conformado'], schema['motivo_col'])
    row=conn.execute(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN {cond_nc} THEN 1 ELSE 0 END) AS nc
          FROM {t} a
         WHERE substr(a.{date_col},1,10) BETWEEN ? AND ?
    """,(start,end)).fetchone()
    total=int(row[0] or 0); nc=int(row[1] or 0)
    p_br = (nc/total) if total>0 else 0.0
    return p_br,total,nc

def _prep_base(df_n: pd.DataFrame, df_scores: pd.DataFrame, p_br: float, alpha: float, min_analises: int) -> pd.DataFrame:
    m=df_n.merge(df_scores, on="nomePerito", how="left")
    if min_analises and "N" in m.columns:
        m=m.loc[m["N"]>=int(min_analises)].copy()
    m["E_raw"]=m["NC"] - m["N"]*float(p_br)
    m["E"]=np.maximum(0, m["E_raw"])
    m["E"]=np.ceil(m["E"]).astype(int)
    m["IV_vagas"]=np.ceil(float(alpha)*m["E"]).astype(int)
    return m

def _elbow_cutoff_score(df: pd.DataFrame) -> Optional[float]:
    tmp=df.dropna(subset=["score_final"]).copy()
    if tmp.empty: return None
    ss=np.sort(tmp["score_final"].unique())[::-1]
    if len(ss)==1: return float(ss[0])
    yy=[tmp.loc[tmp["score_final"]>=s,"IV_vagas"].sum() for s in ss]
    y=np.array(yy,dtype=float)
    y=(y - y.min())/(y.max()-y.min()+1e-9)
    x=np.linspace(0,1,num=len(ss))
    x0,y0=x[0],y[0]; x1,y1=x[-1],y[-1]
    denom=math.hypot(x1-x0,y1-y0)+1e-9
    dist=np.abs((y1-y0)*x - (x1-x0)*y + x1*y0 - y1*x0)/denom
    return float(ss[int(dist.argmax())])

def _calc_delta_tmea(iv_total: float, tmea_br: Optional[float]=None, cap_br: Optional[float]=None, att_br: Optional[float]=None) -> Optional[int]:
    try:
        iv=float(iv_total)
        if cap_br is not None and cap_br>0:
            return int(math.ceil(iv/float(cap_br)))
        if (att_br is not None and att_br>0) and (tmea_br is not None and tmea_br>0):
            return int(math.ceil(float(tmea_br)*(iv/float(att_br))))
    except Exception:
        pass
    return None

def _recompute_with_params(df_all_base: pd.DataFrame, alpha: float, p_br: float, s_star: Optional[float]) -> Tuple[pd.DataFrame,pd.DataFrame]:
    m=df_all_base.copy()
    m["E_raw"]=m["NC"] - m["N"]*float(p_br)
    m["E"]=np.maximum(0, m["E_raw"])
    m["E"]=np.ceil(m["E"]).astype(int)
    m["IV_vagas"]=np.ceil(float(alpha)*m["E"]).astype(int)
    df_sel = m if s_star is None else m.loc[m["score_final"]>=s_star].copy()
    return m, df_sel

# ──────────────────────────────────────────────────────────────────────
# Blocos auxiliares (TopN agregado e Metodologia)
# ──────────────────────────────────────────────────────────────────────

def _topn_subset(df_sel: pd.DataFrame, topn: int) -> pd.DataFrame:
    if df_sel.empty: return df_sel.copy()
    return df_sel.sort_values("IV_vagas", ascending=False).head(int(topn)).copy()

def _calc_topn_aggregate(df_sel: pd.DataFrame, iv_total_period: int, topn: int,
                         tmea_br: Optional[float]=None, cap_br: Optional[float]=None, att_br: Optional[int]=None) -> Dict[str, Any]:
    top = _topn_subset(df_sel, topn)
    iv_top = int(top["IV_vagas"].sum()) if not top.empty else 0
    peso_top = (iv_top / iv_total_period) if iv_total_period > 0 else 0.0
    delta_top = _calc_delta_tmea(iv_top, tmea_br=tmea_br, cap_br=cap_br, att_br=att_br)
    return {"iv_top": iv_top, "peso_top": float(peso_top), "delta_tmea_top": None if delta_top is None else int(delta_top), "n_top": int(top.shape[0])}

def _metodologia_block() -> str:
    return r"""
* Metodologia
** Excesso por perito
E_i = max(0, NC_i − N_i · p_BR).

** Impacto (vagas)
IV_i = ceil(α · E_i).
IV_período = ceil(α · Σ_i NC_i).
Peso (w) = IV_sel / IV_período.

** Projeção de tempo (ΔTMEA)
Aproximações usadas:
1) ΔTMEA ≈ ceil( IV / capacidade_diária )  (quando cap_br informado)
2) ΔTMEA ≈ ceil( TMEA_base · IV / atendimento_diário )  (quando att_br e TMEA_base informados)

** IC de Wilson (p̂)
Para k sucessos em n, z=1,96.
center = (p̂ + z²/(2n)) / (1 + z²/n)
half = z * sqrt((p̂(1−p̂) + z²/(4n))/n) / (1 + z²/n)
IC95% = [center − half, center + half].

** Beta-binomial e ρ (overdispersão)
Var[Y] = n p (1−p) (1 + (n−1) ρ); ρ por momentos.
p ~ Beta(a,b) com média p_BR e ρ ≈ 1/(a+b+1).
Teste: P(Y ≥ k | BetaBin(n,a,b)) unilateral.

** Permutação do peso (w)
Mantém o tamanho do grupo selecionado e (opcionalmente) a distribuição por estrato.

** CMH (2×2×K)
Estratos K (ex.: CR/DR/UO), linha = Selecionado/Não, coluna = NC/Conforme.
Relata OR_MH, X²_MH e p.

** PSA (sensibilidade probabilística)
Amostra α ~ Beta(αK,(1−α)K) e p_BR ~ Beta(nc+1, total−nc+1); recalcula w em R réplicas.
"""

# ──────────────────────────────────────────────────────────────────────
# Exports comuns (PNG/ORG/MD)
# ──────────────────────────────────────────────────────────────────────

def exportar_png_top(df_sel: pd.DataFrame, meta: Dict[str, Any], label_maxlen: int=18, label_fontsize: int=8) -> Optional[str]:
    if df_sel.empty: return None
    topn=int(meta.get("topn",10))
    g=df_sel.sort_values("IV_vagas", ascending=False).head(topn)
    labels=[str(x) for x in g["nomePerito"].tolist()]
    labels=[x if len(x)<=label_maxlen else (x[:max(1,label_maxlen-1)]+"…") for x in labels]
    vals=g["IV_vagas"].astype(int).tolist()
    fig,ax=plt.subplots(figsize=(max(7.8,len(labels)*0.60),5.2), dpi=300)
    ax.bar(labels, vals, edgecolor='black')
    ax.set_ylabel("Vagas presenciais (IV)")
    ax.set_title(f"Impacto na Fila — Top {topn} Peritos (impacto negativo) | {meta['start']} a {meta['end']}")
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    for i,v in enumerate(vals):
        ax.text(i, v + (max(vals)*0.01 if max(vals) else 0.1), f"{int(v)}", ha='center', va='bottom', fontsize=max(7,label_fontsize-1))
    plt.xticks(rotation=45, ha='right')
    parts=[f"IV sel: {int(meta.get('iv_total_sel',0))}"]
    if meta.get("iv_total_period") is not None: parts.append(f"IV período: {int(meta['iv_total_period'])}")
    if meta.get("peso_sel") is not None and meta.get("iv_total_period"): parts.append(f"peso: {meta['peso_sel']*100:.1f}%")
    if meta.get("delta_tmea_sel") is not None: parts.append(f"ΔTMEA sel≈ {int(meta['delta_tmea_sel'])} d")
    if meta.get("delta_tmea_period") is not None: parts.append(f"ΔTMEA período≈ {int(meta['delta_tmea_period'])} d")
    if meta.get("tmea_br") is not None: parts.append(f"TMEA base: {meta['tmea_br']:.0f} d")
    ax.text(0.98, 0.98, " | ".join(parts), transform=ax.transAxes, ha='right', va='top', fontsize=9,
            bbox=dict(facecolor='white', alpha=0.8, edgecolor='black'))
    plt.tight_layout()
    path=os.path.join(EXPORT_DIR, f"impacto_top_peritos_{meta['start']}_a_{meta['end']}.png")
    fig.savefig(path, bbox_inches='tight')
    plt.close(fig); plt.close('all')
    return path

def plot_curva_cotovelo(df: pd.DataFrame, s_star: Optional[float], start: str, end: str,
                        iv_selected: Optional[int]=None, iv_periodo: Optional[int]=None,
                        peso_selected: Optional[float]=None,
                        delta_tmea_sel: Optional[int]=None, delta_tmea_periodo: Optional[int]=None,
                        tmea_br: Optional[float]=None) -> Optional[str]:
    tmp=df.dropna(subset=["score_final"]).copy()
    if tmp.empty: return None
    ss=np.sort(tmp["score_final"].unique())[::-1]
    y=np.array([tmp.loc[tmp["score_final"]>=s,"IV_vagas"].sum() for s in ss], dtype=float)
    fig,ax=plt.subplots(figsize=(7.8,5.0), dpi=300)
    ax.plot(ss, y, marker="o")
    if s_star is not None:
        ax.axvline(s_star, linestyle="--")
        ax.text(s_star, y.max()*0.05 if y.max()>0 else 0.05, f"S*={s_star:.2f}", rotation=90, va="bottom", ha="right", fontsize=9)
    ax.set_xlabel("Score Final (corte)"); ax.set_ylabel("Impacto acumulado (vagas)")
    ax.set_title(f"Curva de Impacto negativo x Score — {start} a {end}")
    ax.grid(True, axis="both", linestyle="--", alpha=0.4)
    parts=[]
    if iv_selected is not None: parts.append(f"IV sel: {int(iv_selected)}")
    if iv_periodo is not None: parts.append(f"IV período: {int(iv_periodo)}")
    if peso_selected is not None and iv_periodo: parts.append(f"peso: {peso_selected*100:.1f}%")
    if delta_tmea_sel is not None: parts.append(f"ΔTMEA sel≈ {int(delta_tmea_sel)} d")
    if delta_tmea_periodo is not None: parts.append(f"ΔTMEA período≈ {int(delta_tmea_periodo)} d")
    if tmea_br is not None: parts.append(f"TMEA base: {tmea_br:.0f} d")
    if parts:
        ax.text(0.98,0.98," | ".join(parts), transform=ax.transAxes, ha='right', va='top', fontsize=9,
                bbox=dict(facecolor='white', alpha=0.8, edgecolor='black'))
    plt.tight_layout()
    path=os.path.join(EXPORT_DIR, f"impacto_curva_cotovelo_{start}_a_{end}.png")
    fig.savefig(path, bbox_inches="tight"); plt.close(fig); plt.close('all')
    return path

def exportar_png_tornado(meta: Dict[str, Any], delta: Dict[str, float], alpha_frac: float, pbr_pp: float) -> Optional[str]:
    labels=[f"α -{alpha_frac*100:.0f}%", f"α +{alpha_frac*100:.0f}%",
            f"p_BR -{pbr_pp*100:.0f} p.p.", f"p_BR +{pbr_pp*100:.0f} p.p."]
    base=meta["peso_sel"]*100.0
    vals=[ (delta.get("alpha_minus",base)-base), (delta.get("alpha_plus",base)-base),
           (delta.get("pbr_minus",base)-base), (delta.get("pbr_plus",base)-base) ]
    y=np.arange(len(labels))
    fig,ax=plt.subplots(figsize=(7.2,4.8), dpi=300)
    ax.barh(y, vals, edgecolor='black'); ax.set_yticks(y); ax.set_yticklabels(labels)
    ax.axvline(0, linestyle='--', linewidth=1)
    ax.set_xlabel("Variação no peso (p.p.)"); ax.set_title(f"Sensibilidade do peso — {meta['start']} a {meta['end']}")
    for i,v in enumerate(vals):
        ax.text(v + (0.2 if v>=0 else -0.2), i, f"{v:+.1f}", va='center', ha='left' if v>=0 else 'right', fontsize=9)
    plt.tight_layout()
    path=os.path.join(EXPORT_DIR, f"impacto_tornado_{meta['start']}_a_{meta['end']}.png")
    fig.savefig(path, bbox_inches='tight'); plt.close(fig); plt.close('all')
    return path

def exportar_png_strat(df_strat_tot: pd.DataFrame, df_strat_sel: pd.DataFrame, by: str, meta: Dict[str, Any]) -> Optional[str]:
    if df_strat_tot.empty: return None
    idx=f"{by}_val"; m=df_strat_tot.merge(df_strat_sel, on=idx, how="left").fillna(0)
    labels=m[idx].astype(str).tolist(); iv_tot=m["IV_tot"].astype(int).tolist(); iv_sel=m["IV_sel"].astype(int).tolist()
    x=np.arange(len(labels)); width=0.4
    fig,ax=plt.subplots(figsize=(max(7.8,len(labels)*0.6),5.0), dpi=300)
    ax.bar(x - width/2, iv_tot, width, label="IV_total", edgecolor="black")
    ax.bar(x + width/2, iv_sel, width, label="IV_sel", edgecolor="black")
    ax.set_xticks(x); ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Vagas (IV)"); ax.set_title(f"Impacto negativo por {by.upper()} — {meta['start']} a {meta['end']}")
    ax.legend(); ax.grid(axis="y", linestyle="--", alpha=0.5)
    for i,(a,b) in enumerate(zip(iv_tot,iv_sel)):
        ax.text(i - width/2, a + max(iv_tot+iv_sel+[1])*0.01, str(int(a)), ha="center", va="bottom", fontsize=8)
        ax.text(i + width/2, b + max(iv_tot+iv_sel+[1])*0.01, str(int(b)), ha="center", va="bottom", fontsize=8)
    plt.tight_layout()
    path=os.path.join(EXPORT_DIR, f"impacto_estratos_{by}_{meta['start']}_a_{meta['end']}.png")
    fig.savefig(path, bbox_inches='tight'); plt.close(fig); plt.close("all")
    return path

# ──────────────────────────────────────────────────────────────────────
# Comentários automáticos (fallback interno)
# ──────────────────────────────────────────────────────────────────────

def _comment_from_gpt_or_fallback(title: str, data: dict, fallback: str,
                                  model: str = "gpt-4o-mini", max_words: int = 120, temperature: float = 0.2) -> str:
    """Pede um parágrafo curto para o gráfico `title` com base em `data`. Fallback caso não haja API."""
    prompt = (
        f"Escreva um único parágrafo em português (≤ {max_words} palavras), claro e objetivo, "
        f"explicando o gráfico '{title}'. Diga o que o gráfico mostra, destaque 2–3 pontos e conclua com 1 frase interpretativa. "
        f"Evite jargão e causalidade. Interprete 'selecionados' como **perfis de alto risco** e trate o impacto como **negativo** para a fila. "
        f"Dados JSON:\n{json.dumps(data, ensure_ascii=False)}"
    )
    try:
        txt = _call_openai_chat(
            [
                {
                    "role":"system",
                    "content":"Você é um analista do ATESTMED, conciso e técnico. IMPORTANTÍSSIMO: trate métricas elevadas de produtividade ≥ limiar, tarefas ≤15s e sobreposição como indicadores de risco; e considere que 'selecionados' = perfis de maior probabilidade de não conformidade (impacto negativo)."
                },
                {"role":"user","content":prompt}
            ],
            model=model, temperature=temperature
        )
        if txt:
            return _protect_comment_text(txt, word_cap=max_words+20)
    except Exception:
        pass
    return fallback

def _comment_fig_top(df_sel: pd.DataFrame, meta: Dict[str,Any],
                     model="gpt-4o-mini", max_words=120, temperature=0.2) -> str:
    top = df_sel.sort_values("IV_vagas", ascending=False).head(meta.get("topn",10)).copy()
    payload = {
        "periodo": [meta["start"], meta["end"]],
        "iv_sel": int(meta["iv_total_sel"]),
        "iv_periodo": int(meta["iv_total_period"]),
        "peso_sel_pct": round(meta["peso_sel"]*100, 2),
        "delta_tmea_sel": meta.get("delta_tmea_sel"),
        "tmea_base": meta.get("tmea_br"),
        "top": [
            {"perito": r["nomePerito"], "iv": int(r["IV_vagas"]), "score": float(r.get("score_final") or 0.0),
             "cr": (r.get("cr") or "—"), "dr": (r.get("dr") or "—")}
            for _, r in top.iterrows()
        ]
    }
    fb = (f"As barras ranqueiam peritos pelo **impacto negativo (IV)** na fila. O grupo **selecionado** somou "
          f"{int(meta['iv_total_sel'])} vagas (~{meta['peso_sel']*100:.1f}% do IV do período), concentrando o **risco de não conformidade**. "
          f"Os nomes à esquerda respondem pela maior parcela do aumento da fila; a cauda agrega volumes menores. "
          f"Recomenda-se priorizar **auditoria e ações corretivas** nesses perfis.")
    return _comment_from_gpt_or_fallback("Top peritos por impacto negativo (vagas)", payload, fb, model, max_words, temperature)

def _comment_fig_cotovelo(df_all: pd.DataFrame, meta: Dict[str,Any], s_star: Optional[float],
                          iv_selected: int, iv_periodo: int,
                          model="gpt-4o-mini", max_words=120, temperature=0.2) -> str:
    payload = {
        "periodo": [meta["start"], meta["end"]],
        "s_star": s_star, "iv_sel": int(iv_selected), "iv_periodo": int(iv_periodo),
        "peso_sel_pct": round((iv_selected/max(1,iv_periodo))*100, 2)
    }
    fb = (f"A curva mostra como o **impacto negativo** se acumula ao afrouxar o corte de score. O joelho em S*={s_star:.2f} "
          f"captura {iv_selected} de {iv_periodo} vagas (~{(iv_selected/max(1,iv_periodo))*100:.1f}%), concentrando os perfis de **maior risco**. "
          f"Reduzir o corte após esse ponto **dilui o foco** e passa a incluir casos de menor prioridade para fiscalização.")
    return _comment_from_gpt_or_fallback("Curva de impacto negativo x score (S*)", payload, fb, model, max_words, temperature)

def _comment_fig_tornado(meta: Dict[str,Any], sens_delta: Dict[str,float],
                         model="gpt-4o-mini", max_words=120, temperature=0.2) -> str:
    base = meta["peso_sel"]*100.0
    payload = {"periodo":[meta["start"],meta["end"]], "peso_base_pct": round(base,2),
               "alpha_minus": sens_delta.get("alpha_minus"), "alpha_plus": sens_delta.get("alpha_plus"),
               "pbr_minus": sens_delta.get("pbr_minus"), "pbr_plus": sens_delta.get("pbr_plus")}
    fb = (f"O tornado avalia a sensibilidade do **peso** (participação dos selecionados no **impacto negativo**) em torno da base ({base:.1f}%). "
          f"Variações plausíveis em α e p_BR alteram o resultado por poucos pontos percentuais, "
          f"sugerindo **robustez** da ordem de grandeza estimada do risco.")
    return _comment_from_gpt_or_fallback("Tornado de sensibilidade do peso", payload, fb, model, max_words, temperature)

def _comment_fig_permutation(pval: Optional[float], R: int,
                             model="gpt-4o-mini", max_words=120, temperature=0.2) -> str:
    payload = {"R": R, "p_perm": None if pval is None else float(pval)}
    fb = (f"O teste de permutação compara o peso observado a grupos aleatórios de mesmo tamanho. "
          f"**P-valor baixo** indica que o agrupamento pelo score concentra **risco acima do acaso**, "
          f"corroborando o foco nos perfis selecionados.")
    return _comment_from_gpt_or_fallback("Permutação do peso (w)", payload, fb, model, max_words, temperature)

def _comment_fig_psa(ci: Optional[Tuple[float,float,float]], R: int,
                     model="gpt-4o-mini", max_words=120, temperature=0.2) -> str:
    med, lo, hi = (None, None, None) if (not ci or any(v is None for v in ci)) else (ci[0]*100, ci[1]*100, ci[2]*100)
    payload = {"R": R, "median_pct": med, "ci95_pct": [lo, hi] if lo is not None else None}
    fb = ("A análise de sensibilidade probabilística propaga incertezas de α e p_BR. "
          "A mediana e o IC95% do **peso (risco)** mostram a faixa provável da participação dos selecionados, "
          "indicando estabilidade do resultado central.")
    return _comment_from_gpt_or_fallback("PSA do peso (w)", payload, fb, model, max_words, temperature)

def _conclusao_text(meta: Dict[str,Any], tests: Dict[str,Any],
                    model="gpt-4o-mini", max_words=140, temperature=0.2) -> str:
    resumo = {
        "periodo": [meta["start"], meta["end"]],
        "iv_periodo": int(meta["iv_total_period"]),
        "iv_sel": int(meta["iv_total_sel"]),
        "peso_sel_pct": round(meta["peso_sel"]*100,2),
        "delta_tmea_period": meta.get("delta_tmea_period"),
        "delta_tmea_sel": meta.get("delta_tmea_sel"),
        "binomial_sig": int(tests.get("binomial_df", pd.DataFrame()).pipe(lambda d: (d["q"]<0.05).sum() if "q" in d else 0)),
        "betabin_sig": int(tests.get("betabin_df", pd.DataFrame()).pipe(lambda d: (d["q_bb"]<0.05).sum() if "q_bb" in d else 0)),
        "perm_p": tests.get("perm_p"),
        "cmh": tests.get("cmh"),
        "psa_ci": tests.get("psa_ci"),
    }
    fb = (
        f"No período, estimamos {int(meta['iv_total_period'])} **vagas adicionadas à fila** (α={meta['alpha']}). "
        f"O **grupo selecionado** reúne {meta['peso_sel']*100:.1f}% desse impacto e representa os **perfis de maior probabilidade de não conformidade**. "
        f"A projeção sugere acréscimo de {meta.get('delta_tmea_period','n/d')} dia(s) no TMEA "
        f"(dos quais {meta.get('delta_tmea_sel','n/d')} atribuíveis ao grupo). "
        f"Os testes (binomial/beta-binomial, permutação e CMH) indicam concentração de risco **acima do acaso**. "
        f"**Recomenda-se auditoria direcionada, supervisão clínica, reforço de padrões e monitoramento contínuo** priorizando esses perfis."
    )
    return _comment_from_gpt_or_fallback("Conclusão do Impacto na Fila", resumo, fb, model, max_words, temperature)

# ──────────────────────────────────────────────────────────────────────
# Export ORG/MD (agora com seção de TESTES e blocos novos)
# ──────────────────────────────────────────────────────────────────────

def exportar_md(df_sel: pd.DataFrame, meta: Dict[str, Any], cuts: Dict[str, Any]) -> str:
    fname=f"impacto_fila_{meta['start']}_a_{meta['end']}.md"; path=os.path.join(EXPORT_DIR,fname)
    if df_sel.empty:
        md=(f"# Impacto na Fila — {meta['start']} a {meta['end']}\n\nSem dados elegíveis no período.\n")
    else:
        delta_sel=f"**ΔTMEA sel≈:** {int(meta['delta_tmea_sel'])}d  " if meta.get('delta_tmea_sel') is not None else ""
        delta_per=f"**ΔTMEA período≈:** {int(meta['delta_tmea_period'])}d  " if meta.get('delta_tmea_period') is not None else ""
        boot=meta.get('peso_ci'); boot_str=f"  **IC95% peso:** [{boot[0]*100:.1f}%; {boot[1]*100:.1f}%]" if boot else ""
        header=(f"# Impacto na Fila — {meta['start']} a {meta['end']}\n\n"
                f"**α:** {meta['alpha']}  **p_BR:** {meta['p_br']*100:.2f}%  **Score-cut:** {meta['score_cut'] if meta['score_cut'] is not None else 'auto/nd'}  \n"
                f"**IV sel (vagas):** {int(meta['iv_total_sel'])}  {delta_sel}\n"
                f"**IV período (vagas):** {int(meta['iv_total_period'])}  **peso sel:** {meta['peso_sel']*100:.2f}%{boot_str}  {delta_per}\n\n"
                f"**Peritos selecionados:** {meta['n_sel']}/{meta['n_all']}  **Cortes:** {cuts}\n\n"
                f"**Interpretação:** os selecionados representam **perfis de alto risco** (maior probabilidade de não conformidade e maior contribuição ao IV).\n\n")
        tbl=["| Perito | CR | DR | UO | N | NC | Excesso | IV (vagas) | Score |",
             "|--------|----|----|----|---:|---:|--------:|-----------:|------:|"]
        for _,r in df_sel.sort_values("IV_vagas",ascending=False).head(meta.get("topn",10)).iterrows():
            tbl.append(f"| {r['nomePerito']} | {r.get('cr','—')} | {r.get('dr','—')} | {r.get('uo','—')} | {int(r['N'])} | {int(r['NC'])} | {int(r['E'])} | {int(r['IV_vagas'])} | {float(r.get('score_final') or 0.0):.2f} |")
        md=header+"\n".join(tbl)+"\n"
    with open(path,"w",encoding="utf-8") as f: f.write(md)
    return path

def exportar_org(df_sel: pd.DataFrame, meta: Dict[str, Any], cuts: Dict[str, Any],
                 png_top: Optional[str], comment_text: Optional[str] = None,
                 png_tornado: Optional[str] = None, by: Optional[str] = None,
                 df_strat_tot: Optional[pd.DataFrame] = None, df_strat_sel: Optional[pd.DataFrame] = None,
                 png_strat: Optional[str] = None,
                 tests: Optional[Dict[str, Any]] = None,
                 png_cotovelo: Optional[str] = None,
                 topn_agg: Optional[Dict[str, Any]] = None,
                 per_top10_sections: bool = False,
                 explain_methods: bool = False,
                 model: str = "gpt-4o-mini", max_words: int = 120, temperature: float = 0.2,
                 table_font: str = "\\scriptsize",
                 firstcol_width: str = "4.2cm",
                 estrato_width: str = "3.6cm"
                 ) -> str:
    fname = f"impacto_fila_{meta['start']}_a_{meta['end']}.org"
    path = os.path.join(EXPORT_DIR, fname)
    tests = tests or {}

    L = []
    L.append(f"* Impacto na Fila — {meta['start']} a {meta['end']}")
    L.append(":PROPERTIES:")
    L.append(f":ALPHA: {meta['alpha']}"); L.append(f":P_BR: {meta['p_br']*100:.2f}%")
    L.append(f":SCORE_CUT: {meta['score_cut'] if meta['score_cut'] is not None else 'auto/nd'}")
    L.append(f":IV_TOTAL_SELECIONADO: {int(meta['iv_total_sel'])}")
    L.append(f":IV_TOTAL_PERIODO: {int(meta['iv_total_period'])}")
    L.append(f":PESO_SELECIONADOS: {meta['peso_sel']*100:.2f}%")
    if meta.get('peso_ci'): L.append(f":PESO_IC95: {meta['peso_ci'][0]*100:.1f}%..{meta['peso_ci'][1]*100:.1f}%")
    L.append(f":TMEA_BR: {meta.get('tmea_br', float('nan')):.1f}d")
    L.append(f":DELTA_TMEA_SEL_DIAS: {int(meta['delta_tmea_sel'])}" if meta.get('delta_tmea_sel') is not None else ":DELTA_TMEA_SEL_DIAS: n/d")
    L.append(f":DELTA_TMEA_PERIODO_DIAS: {int(meta['delta_tmea_period'])}" if meta.get('delta_tmea_period') is not None else ":DELTA_TMEA_PERIODO_DIAS: n/d")
    cuts_str = ", ".join([f"{k}={v}" for k, v in cuts.items() if v is not None]) or "nenhum"
    L.append(f":CUTS: {cuts_str}"); L.append(f":SELECIONADOS: {meta['n_sel']}/{meta['n_all']}"); L.append(":END:\n")

    # Resumo
    resumo = (f"No período analisado, o impacto total estimado na fila foi de *{int(meta['iv_total_period'])}* vagas "
              f"(α={meta['alpha']}). O grupo selecionado respondeu por *{int(meta['iv_total_sel'])}* vagas "
              f"(*{meta['peso_sel']*100:.1f}%* do total"
              + (f", IC95% {meta['peso_ci'][0]*100:.1f}%–{meta['peso_ci'][1]*100:.1f}%" if meta.get('peso_ci') else "") + "). "
              f"Os *selecionados* representam *perfis de alto risco* (impacto negativo concentrado).")
    L.append("** Resumo"); L.append(resumo + "\n")

    # TopN (agregado)
    if topn_agg is not None:
        L.append("** TopN (agregado)")
        L.append(f"IV_top{meta['topn']}: *{int(topn_agg['iv_top'])}*  |  peso_top{meta['topn']}: *{topn_agg['peso_top']*100:.2f}%*  "
                 + (f"|  ΔTMEA_top≈ *{int(topn_agg['delta_tmea_top'])}* d" if topn_agg['delta_tmea_top'] is not None else ""))
        L.append("")
        if _COMENT_TOPN is not None:
            try:
                top_df = df_sel.sort_values("IV_vagas", ascending=False).head(meta.get("topn", 10))
                L.append(_COMENT_TOPN(top_df, int(meta['iv_total_period']), int(meta['topn']), meta,
                                      model=model, temperature=temperature, max_words=max_words))
                L.append("")
            except Exception:
                pass

    # Tabela Top — com fonte menor e coluna 1 como p{...} para quebrar linha
    L.append("** Peritos selecionados (alto risco por impacto)")
    L.append(f"#+ATTR_LATEX: :font {table_font} :align p{{{firstcol_width}}} l l l r r r r r")
    L.append("| Perito | CR | DR | UO | N | NC | Excesso | IV (vagas) | Score |"); L.append("|-")
    if df_sel.empty:
        L.append("| — | — | — | — | — | — | — | — | — |")
    else:
        for _, r in df_sel.sort_values("IV_vagas", ascending=False).head(meta.get("topn", 10)).iterrows():
            L.append("| {nome} | {cr} | {dr} | {uo} | {N} | {NC} | {E} | {IV} | {S:.2f} |".format(
                nome=r["nomePerito"], cr=str(r.get("cr","") or "—"), dr=str(r.get("dr","") or "—"),
                uo=str(r.get("uo","") or "—"), N=int(r["N"]), NC=int(r["NC"]),
                E=int(r["E"]), IV=int(r["IV_vagas"]), S=float(r.get('score_final') or 0.0)
            ))

    # Gráfico Top
    if png_top and os.path.exists(png_top):
        L.append("\n#+CAPTION: Top peritos por impacto negativo (vagas); banner com IV sel, IV período, peso e ΔTMEA.")
        L.append(f"[[file:{os.path.basename(png_top)}]]")
        L.append("")
        L.append(_comment_fig_top(df_sel, meta, model=model, max_words=max_words, temperature=temperature))
        L.append("")

    # Curva cotovelo
    if png_cotovelo and os.path.exists(png_cotovelo):
        L.append("\n#+CAPTION: Curva de impacto negativo acumulado x corte de score (S*).")
        L.append(f"[[file:{os.path.basename(png_cotovelo)}]]")
        L.append("")
        L.append(_comment_fig_cotovelo(df_sel if 'score_final' in df_sel.columns else pd.DataFrame(),
                                       meta, meta.get('score_cut'), int(meta['iv_total_sel']), int(meta['iv_total_period']),
                                       model=model, max_words=max_words, temperature=temperature))
        L.append("")

    # Tornado
    if png_tornado and os.path.exists(png_tornado):
        L.append("\n#+CAPTION: Tornado de sensibilidade do peso (α± e p_BR±).")
        L.append(f"[[file:{os.path.basename(png_tornado)}]]")
        L.append("")
        L.append(_comment_fig_tornado(meta, sens_delta={}, model=model, max_words=max_words, temperature=temperature))
        L.append("")

    # Estratos
    if by and (df_strat_tot is not None) and (df_strat_sel is not None):
        L.append(f"\n** Sumário estratificado por {by.upper()}")
        L.append(f"#+ATTR_LATEX: :font {table_font} :align p{{{estrato_width}}} r r r r r")
        L.append("| Estrato | N_total | NC_total | E_total | IV_total | IV_sel |"); L.append("|-")
        idx = f"{by}_val"
        merged = df_strat_tot.merge(df_strat_sel, on=idx, how="left", suffixes=("_tot", "_sel")).fillna(0)
        for _, r in merged.iterrows():
            L.append("| {estr} | {N} | {NC} | {E} | {IV} | {IVs} |".format(
                estr=str(r[idx] or "—"), N=int(r["N_tot"]), NC=int(r["NC_tot"]),
                E=int(r["E_tot"]), IV=int(r["IV_tot"]), IVs=int(r["IV_sel"])
            ))
        if png_strat and os.path.exists(png_strat):
            L.append("\n#+CAPTION: Impacto negativo por estrato (IV_total e IV_sel).")
            L.append(f"[[file:{os.path.basename(png_strat)}]]")
            L.append("")
            L.append(_comment_fig_strat(by, df_strat_tot, df_strat_sel, model=model, max_words=max_words, temperature=temperature))
            L.append("")

    # Seções por perito do TopN
    if per_top10_sections and not df_sel.empty:
        L.append("** TopN — impacto individual (cada perito)")
        df_bin = tests.get("binomial_df"); df_bb  = tests.get("betabin_df")
        top = df_sel.sort_values('IV_vagas', ascending=False).head(meta.get('topn', 10))
        for _, r in top.iterrows():
            nome = r['nomePerito']
            N=int(r['N']); NC=int(r['NC']); E=int(r['E']); IVi=int(r['IV_vagas'])
            p_hat = (NC / max(1, N)); wl, wh = _wilson_ci(NC, N)
            p_b = q_b = p_bb = q_bb = None
            if isinstance(df_bin, pd.DataFrame) and not df_bin.empty:
                row = df_bin.loc[df_bin['nomePerito']==nome]
                if not row.empty: p_b=float(row.iloc[0]['p']); q_b=float(row.iloc[0]['q'])
            if isinstance(df_bb, pd.DataFrame) and not df_bb.empty:
                row = df_bb.loc[df_bb['nomePerito']==nome]
                if not row.empty: p_bb=float(row.iloc[0]['p_bb']); q_bb=float(row.iloc[0]['q_bb'])
            L.append(f"*** {nome}")
            L.append(f"- N={N}, NC={NC}, p̂={p_hat:.3f}, IC95%=[{wl:.3f};{wh:.3f}]")
            L.append(f"- E_i=max(0, NC−N·p_BR)={E}  |  IV_i=ceil(α·E_i)={IVi}")
            L.append(f"- score={float(r.get('score_final') or 0.0):.2f}  |  CR={r.get('cr','—')}  DR={r.get('dr','—')}  UO={r.get('uo','—')}")
            if p_b is not None:  L.append(f"- Binomial: p={p_b:.3g}  q={q_b:.3g}")
            if p_bb is not None: L.append(f"- Beta-binomial: p_bb={p_bb:.3g}  q_bb={q_bb:.3g}")
            if _COMENT_PERITO is not None:
                try:
                    row_payload = {"nomePerito": nome, "N": N, "NC": NC, "E": E, "IV_vagas": IVi,
                                   "p_hat": p_hat, "wilson_low": wl, "wilson_high": wh,
                                   "p": p_b, "q": q_b, "p_bb": p_bb, "q_bb": q_bb,
                                   "score": float(r.get('score_final') or 0.0),
                                   "cr": r.get('cr','—'), "dr": r.get('dr','—'), "uo": r.get('uo','—')}
                    L.append(_COMENT_PERITO(row_payload, float(meta['p_br']), float(meta['alpha']),
                                            model=model, temperature=temperature, max_words=max_words))
                except Exception:
                    pass
            L.append("")

    # Comentário geral (se existir)
    if comment_text:
        L.append("\n** Comentário")
        L.append(comment_text.strip())

    # TESTES (com tabelas menores)
    if tests:
        L.append("\n* Validação estatística")
        # Binomial
        if tests.get("binomial_df") is not None:
            df = tests["binomial_df"].copy()
            L.append("** Teste binomial (unilateral, p_i > p_BR) com FDR (BH)")
            L.append(f":P_BR: {meta['p_br']*100:.2f}%")
            # usa longtable e fonte menor; primeira coluna p{...} permite quebra de linha no nome
            L.append(f"#+ATTR_LATEX: :environment longtable :font {table_font} :align p{{{firstcol_width}}} r r r l r r r")
            L.append("| Perito | N | NC | p̂_i | IC95%(Wilson) | Excesso | p | q(BH) |")
            L.append("|-")

            # mostra TODAS as linhas, ordenando por p (mais significativo primeiro)
            show = df.sort_values("p", ascending=True)
            for _, r in show.iterrows():
                L.append("| {nome} | {N} | {NC} | {ph} | [{l}; {u}] | {E} | {pval} | {qval} |".format(
                    nome=r["nomePerito"],
                    N=int(r["N"]), NC=int(r["NC"]),
                    ph=_fmt2(r["p_hat"]),
                    l=_fmt2(r["wilson_low"]), u=_fmt2(r["wilson_high"]),
                    E=int(r["E"]),
                    pval=_fmtp2(r["p"]), qval=_fmtp2(r["q"])
                ))

            L.append(f"\nPeritos com q<0,05: {int((df['q']<0.05).sum())} de {df.shape[0]}.\n")
            if _COMENT_BIN is not None:
                try:
                    L.append(_COMENT_BIN(df, float(meta['p_br']), model=model, temperature=temperature, max_words=max_words))
                    L.append("")
                except Exception:
                    pass

        # Beta-binomial
        if tests.get("betabin_df") is not None:
            df = tests["betabin_df"].copy()
            L.append("** Teste beta-binomial (overdispersão) com FDR (BH)")
            L.append(f":RHO_MOM: {tests.get('rho_mom', float('nan')):.4f}")
            L.append(f"#+ATTR_LATEX: :font {table_font} :align p{{{firstcol_width}}} r r r r r r")
            L.append("| Perito | N | NC | p̂_i | Excesso | p_betaBin | q(BH) |")
            L.append("|-")
            show = df.sort_values("p_bb", ascending=True).head(25)
            for _, r in show.iterrows():
                L.append("| {nome} | {N} | {NC} | {ph:.3f} | {E} | {pbb:.3g} | {qval:.3g} |".format(
                    nome=r["nomePerito"], N=int(r["N"]), NC=int(r["NC"]), ph=float(r["p_hat"]),
                    E=int(r["E"]), pbb=float(r["p_bb"]), qval=float(r["q_bb"])
                ))
            L.append(f"\nPeritos com q<0,05: {int((df['q_bb']<0.05).sum())} de {df.shape[0]}.\n")
            if _COMENT_BB is not None:
                try:
                    L.append(_COMENT_BB(df, float(tests.get('rho_mom', float('nan'))), model=model, temperature=temperature, max_words=max_words))
                    L.append("")
                except Exception:
                    pass

        # Permutação
        if tests.get("perm_png") is not None and os.path.exists(tests["perm_png"]):
            L.append("** Teste de permutação do peso (w)")
            L.append(f"Observado: *w = {meta['peso_sel']*100:.2f}%*; réplicas: {int(tests.get('perm_R',0))}; p-valor: {tests.get('perm_p',float('nan')):.4f}")
            L.append(f"[[file:{os.path.basename(tests['perm_png'])}]]")
            L.append("")
            if _COMENT_PERM is not None:
                try:
                    w_obs_pct = float(meta.get('peso_sel', 0.0))*100.0
                    L.append(_COMENT_PERM(tests.get("perm_p"), int(tests.get("perm_R",0)), w_obs_pct,
                                          model=model, temperature=temperature, max_words=max_words))
                    L.append("")
                except Exception:
                    pass
            else:
                L.append(_comment_fig_permutation(tests.get("perm_p"), int(tests.get("perm_R",0)),
                                                  model=model, max_words=max_words, temperature=temperature))
                L.append("")

        # CMH
        if tests.get("cmh") is not None:
            ormh, x2, pval, by_key = tests["cmh"]
            L.append(f"** CMH por {by_key.upper()} (2×2×K, selecionado vs. não; NC vs. conforme)")
            L.append(f"OR_MH: {ormh:.3f} | X2_MH: {x2:.3f} | p: {pval:.4g}\n")
            if _COMENT_CMH is not None:
                try:
                    L.append(_COMENT_CMH(ormh, x2, pval, by_key, model=model, temperature=temperature, max_words=max_words))
                    L.append("")
                except Exception:
                    pass

        # PSA
        if tests.get("psa_png") is not None and os.path.exists(tests["psa_png"]):
            L.append("** Sensibilidade probabilística (PSA) para o peso (w)")
            ci = tests.get("psa_ci", (None,None,None))
            L.append(f"Réplicas: {int(tests.get('psa_R',0))} | Mediana: {ci[0]*100:.2f}% | IC95%: [{ci[1]*100:.2f}%; {ci[2]*100:.2f}%]")
            L.append(f"[[file:{os.path.basename(tests['psa_png'])}]]")
            L.append("")
            if _COMENT_PSA is not None:
                try:
                    L.append(_COMENT_PSA(ci, int(tests.get("psa_R",0)), model=model, temperature=temperature, max_words=max_words))
                    L.append("")
                except Exception:
                    pass
            else:
                L.append(_comment_fig_psa(ci, int(tests.get("psa_R",0)),
                                          model=model, max_words=max_words, temperature=temperature))
                L.append("")

    # Conclusão
    L.append("\n* Conclusão")
    L.append(_conclusao_text(meta, tests, model=model, max_words=140, temperature=temperature))

    # Metodologia
    if explain_methods:
        L.append(_metodologia_block())

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    return path


# Montagem ORG final (header + front + org)
def _read_text_or_empty(path: Optional[str]) -> str:
    if not path: return ""
    try:
        with open(path,"r",encoding="utf-8") as f:
            return f.read().rstrip()+"\n\n"
    except Exception:
        return f"* Aviso\nNão foi possível ler o arquivo: {path}\n\n"

def montar_org_final(header_org: str, front_org: str, org_gerado: str,
                     start: str, end: str, final_org_name: Optional[str] = None,
                     author: Optional[str] = None, doc_date: Optional[str] = None) -> str:
    """
    Monta o ORG final:
      header_mps.org  +  impacto_fila.org (front)  +  org_gerado (inline)
    - Força TITLE/AUTHOR/DATE e EXPORT_FILE_NAME.
    - Converte links [[file:...]] para basename e copia as imagens p/ a pasta destino.
    """
    safe_name = final_org_name or f"impacto_fila_FINAL_{start}_a_{end}"
    dest_dir  = EXPORT_DIR
    os.makedirs(dest_dir, exist_ok=True)
    final_org = os.path.join(dest_dir, f"{safe_name}.org")

    def _read(path: str) -> str:
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        except Exception:
            return ""

    head = _read(header_org) if header_org and os.path.exists(header_org) else ""
    front = _read(front_org) if front_org and os.path.exists(front_org) else ""
    body  = _read(org_gerado) if org_gerado and os.path.exists(org_gerado) else ""

    # Remove metadados conflitantes do front
    def _strip_conflicting_meta(s: str) -> str:
        out = []
        for ln in s.splitlines():
            l = ln.strip().lower()
            if l.startswith("#+export_file_name:") or l.startswith("#+title:") or l.startswith("#+author:") or l.startswith("#+date:"):
                continue
            out.append(ln)
        return "\n".join(out)

    front_clean = _strip_conflicting_meta(front)

    # Cabeçalho fixo do final — ATENÇÃO: aqui garantimos “o que tem que sair”
    doc_title  = f"Impacto na Fila — {start} a {end}"
    doc_author = (author or "").strip()
    if not doc_author:
        doc_author = "Gustavo M. Mendes de Tarso"
    doc_date_s = (doc_date or _date_pt_br(datetime.today()))

    header_final = [
        f"#+TITLE: {doc_title}",
        f"#+AUTHOR: {doc_author}",
        f"#+DATE: {doc_date_s}",
        f"#+EXPORT_FILE_NAME: {safe_name}",
        "#+OPTIONS: toc:nil",
        "#+LATEX_HEADER: \\usepackage{array}",
        "#+LATEX_HEADER: \\setlength{\\tabcolsep}{4pt}",
        "#+LATEX_HEADER: \\renewcommand{\\arraystretch}{1.05}",
        ""
    ]

    # Conteúdo combinado
    combined = "\n".join([
        head.strip(),
        "\n".join(header_final),
        front_clean.strip(),
        "\n* Resultados",
        body.strip(),
        ""
    ]).strip() + "\n"

    # Normaliza links de imagem → basename e copia assets
    img_paths = set()
    def _norm_link(m):
        pth = m.group(1).strip()
        base = os.path.basename(pth)
        img_paths.add((pth, base))
        return f"[[file:{base}]]"
    combined = re.sub(r"\[\[file:([^\]\|]+)\]\]", _norm_link, combined)

    with open(final_org, "w", encoding="utf-8") as f:
        f.write(combined)

    # Copia imagens
    src_dir_generated = os.path.dirname(os.path.abspath(org_gerado or dest_dir))
    for src, base in img_paths:
        cand = [
            os.path.join(src_dir_generated, src),
            os.path.join(src_dir_generated, base),
            os.path.join(os.path.dirname(header_org or ""), src),
            os.path.join(os.path.dirname(front_org or ""), src),
            src
        ]
        src_exist = next((c for c in cand if os.path.exists(c)), None)
        if not src_exist:
            print(f"⚠️ Não encontrei asset '{src}'.")
            continue
        dst = os.path.join(dest_dir, os.path.basename(base))
        if os.path.abspath(src_exist) != os.path.abspath(dst):
            try:
                shutil.copy2(src_exist, dst)
            except Exception as e:
                print(f"⚠️ Falha ao copiar '{src_exist}' → '{dst}': {e}")

    print(f"✅ ORG final salvo em: {final_org}")
    return final_org


# ──────────────────────────────────────────────────────────────────────
# Funções de TESTES
# ──────────────────────────────────────────────────────────────────────

def run_test_binomial(df_all: pd.DataFrame, p_br: float) -> pd.DataFrame:
    """Retorna DF com p, q(BH), IC Wilson e excesso E."""
    out=df_all[["nomePerito","N","NC","E"]].copy()
    out["p_hat"]=out["NC"]/out["N"].replace(0,np.nan)
    pvals=[]; low=[]; high=[]
    for _,r in out.iterrows():
        n=int(r["N"]); k=int(r["NC"])
        pval=_binom_sf_one_sided(k, n, p_br) if n>0 else 1.0
        lo,hi=_wilson_ci(k,n)
        pvals.append(pval); low.append(lo); high.append(hi)
    out["p"]=np.array(pvals, dtype=float)
    out["wilson_low"]=np.array(low,dtype=float); out["wilson_high"]=np.array(high,dtype=float)
    out["q"]=_p_adjust_bh(out["p"].values)
    return out

def run_test_betabin(df_all: pd.DataFrame, p_br: float) -> Tuple[pd.DataFrame, float]:
    """Estima ρ via MoM; testa p_i > p_BR no beta-binomial. Retorna DF e ρ."""
    N=df_all["N"].astype(int).values; NC=df_all["NC"].astype(int).values
    rho=_estimate_rho_mom(N, NC, p_br)
    if rho <= 1e-9:
        rho = 1e-9
    ab = (1.0/rho) - 1.0
    a = float(p_br*ab); b=float((1.0-p_br)*ab)
    pvals=[]
    for n,k in zip(N,NC):
        pval=_betabin_sf_one_sided(int(k), int(n), a, b) if n>0 else 1.0
        pvals.append(pval)
    out=df_all[["nomePerito","N","NC","E"]].copy()
    out["p_hat"]=out["NC"]/out["N"].replace(0,np.nan)
    out["p_bb"]=np.array(pvals,dtype=float)
    out["q_bb"]=_p_adjust_bh(out["p_bb"].values)
    return out, float(rho)

def run_permutation_weight(df_all: pd.DataFrame, df_sel: pd.DataFrame, alpha: float, p_br: float,
                           R: int, stratify_by: Optional[str]=None) -> Tuple[float, str]:
    """
    Permuta grupos de mesmo tamanho (e opcionalmente mesma distribuição por estrato),
    devolve p-valor e caminho do histograma.
    """
    if df_sel.empty or df_all.empty or R<=0:
        return (float('nan'), "")
    n_sel = df_sel.shape[0]
    iv_period = int(math.ceil(float(alpha)*float(df_all["NC"].sum())))
    rng=np.random.default_rng()
    weights=[]
    if stratify_by and stratify_by in df_all.columns:
        counts=df_sel[stratify_by].fillna("—").value_counts().to_dict()
        strata_groups={}
        for g,sub in df_all.groupby(df_all[stratify_by].fillna("—")):
            strata_groups[g]=sub.index.values
        for _ in range(int(R)):
            idx=[]
            for g,c in counts.items():
                pool=strata_groups.get(g, np.array([],dtype=int))
                if pool.size==0: continue
                pick=rng.choice(pool, size=min(c, pool.size), replace=False)
                idx.extend(list(pick))
            if len(idx)<n_sel:
                pool_extra=np.setdiff1d(df_all.index.values, np.array(idx,dtype=int), assume_unique=False)
                if pool_extra.size>0:
                    pick=rng.choice(pool_extra, size=(n_sel - len(idx)), replace=False)
                    idx.extend(list(pick))
            iv_s = int(df_all.loc[idx, "IV_vagas"].sum())
            w = (iv_s / iv_period) if iv_period>0 else 0.0
            weights.append(w)
    else:
        idx_all=df_all.index.values
        for _ in range(int(R)):
            idx=np.random.choice(idx_all, size=n_sel, replace=False)
            iv_s=int(df_all.loc[idx,"IV_vagas"].sum())
            w=(iv_s/iv_period) if iv_period>0 else 0.0
            weights.append(w)
    weights=np.array(weights, dtype=float)
    w_obs = (df_sel["IV_vagas"].sum()/iv_period) if iv_period>0 else 0.0
    pval = float((1.0 + (weights >= w_obs).sum()) / (len(weights)+1.0))
    # histograma
    fig,ax=plt.subplots(figsize=(7.2,4.6), dpi=300)
    ax.hist(weights*100, bins=40, edgecolor='black')
    ax.axvline(w_obs*100, color='red', linestyle='--', label=f"w obs = {w_obs*100:.2f}%")
    ax.set_xlabel("Peso permutado (%)"); ax.set_ylabel("Frequência"); ax.set_title("Permutação do peso (w)")
    ax.legend(); plt.tight_layout()
    perm_png=os.path.join(EXPORT_DIR, f"perm_weight_hist_{n_sel}_{R}.png")
    fig.savefig(perm_png, bbox_inches='tight'); plt.close(fig); plt.close('all')
    return (pval, perm_png)

def run_cmh(df_all: pd.DataFrame, df_sel: pd.DataFrame, by: str) -> Tuple[float,float,float]:
    """
    CMH 2×2×K:
      linha: Selecionado vs Não
      coluna: NC vs Conforme
      estratos: valores distintos de 'by'
    Usa agregados por estrato.
    """
    if by not in ("cr","dr","uo"):
        raise ValueError("CMH requer by em {cr,dr,uo}")
    df_all2=df_all.copy(); df_all2[by]=df_all2[by].fillna("—").astype(str)
    df_sel2=df_sel.copy(); df_sel2[by]=df_sel2[by].fillna("—").astype(str)
    tabs=[]
    for g, sub_all in df_all2.groupby(by):
        sub_sel = df_sel2.loc[df_sel2[by]==g]
        N_sel = int(sub_sel["N"].sum()); NC_sel=int(sub_sel["NC"].sum())
        N_all = int(sub_all["N"].sum()); NC_all=int(sub_all["NC"].sum())
        N_nsel = N_all - N_sel; NC_nsel = NC_all - NC_sel
        conf_sel = max(N_sel - NC_sel, 0); conf_nsel = max(N_nsel - NC_nsel, 0)
        tabs.append( (NC_sel, conf_sel, NC_nsel, conf_nsel) )
    return _mh_cmh_test(tabs)

def run_psa(df_all_base: pd.DataFrame, df_sel_base: pd.DataFrame, alpha: float, p_br: float,
            total: int, nc: int, R: int, s_star: Optional[float], alpha_strength: float=50.0) -> Tuple[Tuple[float,float,float], str]:
    """
    PSA para w: amostra α~Beta(αK,(1-α)K) e p_BR~Beta(nc+1, total-nc+1); mantém S* fixo (mais estável).
    """
    if R<=0 or df_all_base.empty:
        return ((float('nan'),float('nan'),float('nan')), "")
    rng=np.random.default_rng()
    a_al = max(alpha*alpha_strength, 1e-3); b_al=max((1.0-alpha)*alpha_strength, 1e-3)
    a_p  = nc + 1.0; b_p = (total - nc) + 1.0
    ws=[]
    for _ in range(int(R)):
        a_s = float(rng.beta(a_al, b_al))
        p_s = float(rng.beta(a_p,  b_p))
        m, sel = _recompute_with_params(df_all_base, a_s, p_s, s_star)
        iv_period = int(math.ceil(a_s * float(m["NC"].sum())))
        iv_sel    = int(sel["IV_vagas"].sum()) if not sel.empty else 0
        w = (iv_sel/iv_period) if iv_period>0 else 0.0
        ws.append(w)
    ws=np.array(ws,dtype=float)
    p50=float(np.percentile(ws,50)); p2=float(np.percentile(ws,2.5)); p97=float(np.percentile(ws,97.5))
    fig,ax=plt.subplots(figsize=(7.2,4.6), dpi=300)
    ax.hist(ws*100, bins=40, edgecolor='black')
    ax.axvline(p50*100, color='red', linestyle='--', label=f"mediana = {p50*100:.2f}%")
    ax.set_xlabel("Peso (w) em %"); ax.set_ylabel("Frequência"); ax.set_title("PSA — distribuição do peso (w)")
    ax.legend(); plt.tight_layout()
    psa_png=os.path.join(EXPORT_DIR, f"psa_weight_hist_{R}.png")
    fig.savefig(psa_png, bbox_inches='tight'); plt.close(fig); plt.close('all')
    return ((p50,p2,p97), psa_png)

# ──────────────────────────────────────────────────────────────────────
# Estratos
# ──────────────────────────────────────────────────────────────────────

def _compute_strata(df_all: pd.DataFrame, df_sel: pd.DataFrame, by: str, alpha: float, p_br: float) -> Tuple[pd.DataFrame,pd.DataFrame]:
    col_map={"cr":"cr","dr":"dr","uo":"uo"}; col=col_map.get(by)
    if not col or col not in df_all.columns:
        return (pd.DataFrame(columns=[f"{by}_val","N_tot","NC_tot","E_tot","IV_tot"]),
                pd.DataFrame(columns=[f"{by}_val","IV_sel"]))
    def agg_block(df):
        g=df.groupby(col, dropna=False).agg(N_tot=("N","sum"), NC_tot=("NC","sum")).reset_index()
        g[f"{by}_val"]=g[col].fillna("—").astype(str)
        g["E_tot"]=np.ceil(np.maximum(0, g["NC_tot"] - g["N_tot"]*float(p_br))).astype(int)
        g["IV_tot"]=np.ceil(float(alpha)*g["E_tot"]).astype(int)
        return g[[f"{by}_val","N_tot","NC_tot","E_tot","IV_tot"]]
    tot=agg_block(df_all.copy())
    sel=df_sel.groupby(col, dropna=False)["IV_vagas"].sum().reset_index().rename(columns={"IV_vagas":"IV_sel"})
    sel[f"{by}_val"]=sel[col].fillna("—").astype(str)
    sel=sel[[f"{by}_val","IV_sel"]].copy(); sel["IV_sel"]=sel["IV_sel"].astype(int)
    return tot, sel

# Comentário de estratos (fallback genérico)
def _comment_fig_strat(by: str, df_strat_tot: pd.DataFrame, df_strat_sel: pd.DataFrame,
                       model="gpt-4o-mini", max_words=120, temperature=0.2) -> str:
    m = df_strat_tot.merge(df_strat_sel, on=f"{by}_val", how="left").fillna(0)
    m["share_sel"] = m["IV_sel"]/m["IV_tot"].replace(0,np.nan)
    dom = m.sort_values("IV_tot", ascending=False).head(3)
    payload = {
        "by": by, "estratos": [
            {"estrato": str(r[f"{by}_val"]), "iv_tot": int(r["IV_tot"]), "iv_sel": int(r["IV_sel"]),
             "share_sel": None if pd.isna(r["share_sel"]) else round(float(r["share_sel"]*100),2)}
            for _, r in dom.iterrows()
        ]
    }
    fb = ("O sumário estratificado indica **onde o impacto negativo (IV_total)** se concentra e em quais estratos o esforço "
          "(IV_sel) está direcionado. Os primeiros grupos respondem pela maior parcela do risco, "
          "reforçando a prioridade de **mitigação segmentada**.")
    return _comment_from_gpt_or_fallback(f"Impacto por {by.upper()}", payload, fb, model, max_words, temperature)

# ──────────────────────────────────────────────────────────────────────
# Shipping/movimentação de saídas
# ──────────────────────────────────────────────────────────────────────
def _ship_outputs(paths: List[Optional[str]], out_root: str, out_date: Optional[str], out_subdir: str) -> str:
    """
    Move os arquivos de `paths` para BASE_DIR/out_root/YYYY-MM-DD/out_subdir.
    Ignora caminhos inexistentes/None. Retorna o diretório final.
    """
    date_str = (out_date or "").strip()
    if not date_str:
        date_str = datetime.today().strftime("%Y-%m-%d")
    dest_dir = os.path.join(BASE_DIR, out_root, date_str, out_subdir)
    os.makedirs(dest_dir, exist_ok=True)

    moved = 0
    for pth in paths:
        if not pth:
            continue
        try:
            if not os.path.exists(pth):
                continue
            dst = os.path.join(dest_dir, os.path.basename(pth))
            if os.path.abspath(pth) == os.path.abspath(dst):
                continue
            shutil.move(pth, dst)
            moved += 1
        except Exception as e:
            print(f"⚠️ Falha ao mover '{pth}': {e}")
    print(f"📦 Movidos {moved} arquivo(s) para: {dest_dir}")
    return dest_dir


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    ap=argparse.ArgumentParser(description="Impacto na Fila com testes estatísticos e ORG final.")
    ap.add_argument('--start', required=True); ap.add_argument('--end', required=True)
    g=ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--perito'); g.add_argument('--top10', action='store_true')

    ap.add_argument('--min-analises', type=int, default=50)
    ap.add_argument('--topn', type=int, default=10)

    # parâmetros de impacto
    ap.add_argument('--alpha', type=float, default=0.8)
    ap.add_argument('--pbr', type=float, default=None)
    ap.add_argument('--minimo-duplo-nc', action='store_true')

    # TMEA e projeções
    ap.add_argument('--tmea-br', type=float, default=60.0)
    ap.add_argument('--cap-br', type=float, default=None)
    ap.add_argument('--att-br', type=int, default=None)

    # Bootstrap / Sensibilidade / Estratos
    ap.add_argument('--bootstrap-peso', type=int, default=0)
    ap.add_argument('--bootstrap-recalc-sstar', action='store_true')
    ap.add_argument('--sens-plot', action='store_true')
    ap.add_argument('--sens-alpha-frac', type=float, default=0.10)
    ap.add_argument('--sens-pbr-pp', type=float, default=0.02)
    ap.add_argument('--by', choices=['cr','dr','uo'])

    # TESTES
    ap.add_argument('--test-binomial', action='store_true')
    ap.add_argument('--betabin', action='store_true')
    ap.add_argument('--permute-weight', type=int, default=0, metavar="N")
    ap.add_argument('--permute-stratify', action='store_true', help="Permuta preservando contagem por estrato de --by (se existir).")
    ap.add_argument('--cmh', type=str, default=None, help="Informe BY ou 'by=BY' (BY∈{cr,dr,uo}). Ex.: --cmh cr  ou  --cmh by=cr")
    ap.add_argument('--psa', type=int, default=0, metavar="N", help="N réplicas para PSA (α~Beta, p_BR~Beta)")
    ap.add_argument('--psa-alpha-strength', type=float, default=50.0, help="Força da Beta de α (equiv. amostral)")

    # layout / outputs
    ap.add_argument('--label-maxlen', type=int, default=18); ap.add_argument('--label-fontsize', type=int, default=8)
    ap.add_argument('--chart', action='store_true')
    ap.add_argument('--export-md', action='store_true'); ap.add_argument('--export-org', action='store_true')
    ap.add_argument('--export-png', action='store_true')
    ap.add_argument('--export-comment', action='store_true'); ap.add_argument('--export-comment-org', action='store_true')
    ap.add_argument('--add-comments', action='store_true')

    # Shipping de saídas
    ap.add_argument('--ship-outputs', action='store_true',
                    help='Move os arquivos gerados para reports/outputs/PERIODO/impacto_fila (limpa EXPORT_DIR).')
    ap.add_argument('--out-date', default=None,
                    help='Data da pasta destino (YYYY-MM-DD). Se vazio, usa --end.')
    ap.add_argument('--out-root', default='reports/outputs',
                    help='Raiz dos relatórios (relativo ao projeto).')
    ap.add_argument('--out-subdir', default='impacto_fila',
                    help='Subpasta do módulo. Padrão: impacto_fila')

    # ORG FINAL
    ap.add_argument('--final-org', action='store_true')
    ap.add_argument('--header-org', default='/home/gustavodetarso/Documentos/.share/header_mps_org/header_mps.org')
    ap.add_argument('--front-org',  default='/home/gustavodetarso/Documentos/.share/header_mps_org/impacto_fila.org')
    ap.add_argument('--final-org-name', default=None)
    ap.add_argument('--export-pdf', action='store_true', help='Converte o ORG final para PDF (tenta pandoc; fallback Emacs/ox-latex).')

    # OpenAI
    ap.add_argument('--model', default='gpt-4o-mini'); ap.add_argument('--max-words', type=int, default=200)
    ap.add_argument('--temperature', type=float, default=0.2)

    # Blocos novos
    ap.add_argument('--per-top10-sections', action='store_true', help='Cria subseções por perito do TopN com comentários e estatísticas.')
    ap.add_argument('--top10-aggregate', action='store_true', help='Inclui bloco agregado do TopN (IV_top, peso_top, ΔTMEA_top).')
    ap.add_argument('--explain-methods', action='store_true', help='Anexa seção de metodologia.')

    # ⚠️ NOVA FLAG: não aplicar S*
    ap.add_argument('--no-sstar', action='store_true', help='Desliga o corte S*; usa TODOS os peritos (TopN apenas por IV).')

    # atalho para tudo
    ap.add_argument('--all-tests', action='store_true', help="Executa: --test-binomial --betabin --permute-weight 5000 --cmh by=cr --psa 10000")
    return ap.parse_args()


# ──────────────────────────────────────────────────────────────────────
# Build (single / group)
# ──────────────────────────────────────────────────────────────────────

def _build_single(start: str, end: str, perito: str, alpha: float, p_br_opt: Optional[float]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        schema=_detect_schema(conn)
        df_n=_fetch_perito_n_nc(conn,start,end,schema)
        if df_n.empty:
            return pd.DataFrame(columns=['nomePerito','N','NC','cr','dr','uo','E','IV_vagas','score_final']), {
                'mode':'single','perito':perito,'start':start,'end':end,'alpha':alpha,'p_br':0.0,'score_cut':None,
                'n_all':0,'n_sel':0,'label_lhs':perito,'safe_stub':perito,'topn':10
            }
        p_br,_,_= _compute_p_br_and_totals(conn,start,end,schema) if p_br_opt is None else (p_br_opt,None,None)
        df_scores=_fetch_scores(conn)
        df=_prep_base(df_n, df_scores, p_br, alpha, min_analises=0)
        m=df.loc[df["nomePerito"].str.strip().str.upper()==perito.strip().upper()].copy()
        meta={'mode':'single','perito':perito,'start':start,'end':end,'alpha':alpha,'p_br':p_br,'score_cut':None,
              'n_all':int(df.shape[0]),'n_sel':int(m.shape[0]),'label_lhs':perito,'safe_stub':perito,'topn':10}
        return m, meta

def _build_group(start: str, end: str, alpha: float, p_br_opt: Optional[float],
                 min_analises: int, topn: int, minimo_duplo_nc: bool=False) -> Tuple[pd.DataFrame, Dict[str, Any], pd.DataFrame]:
    with sqlite3.connect(DB_PATH) as conn:
        schema=_detect_schema(conn)
        df_n=_fetch_perito_n_nc(conn,start,end,schema)
        if df_n.empty:
            return (pd.DataFrame(columns=['nomePerito','N','NC','cr','dr','uo','E','IV_vagas','score_final']),
                    {'mode':'top10','start':start,'end':end,'alpha':alpha,'p_br':0.0,'score_cut':None,
                     'n_all':0,'n_sel':0,'label_lhs':'Grupo S*','safe_stub':'Grupo','topn': topn},
                    pd.DataFrame(columns=['nomePerito']))
        p_br_calc,total_calc,nc_calc=_compute_p_br_and_totals(conn,start,end,schema)
        p_br=p_br_opt if p_br_opt is not None else p_br_calc
        df_scores=_fetch_scores(conn)
        df_all=_prep_base(df_n, df_scores, p_br, alpha, min_analises=min_analises)
    s_star=_elbow_cutoff_score(df_all)
    if minimo_duplo_nc and s_star is not None and not df_all.dropna(subset=["score_final"]).empty:
        ss=np.sort(df_all["score_final"].dropna().unique())[::-1]
        for s in ss:
            grp=df_all.loc[df_all["score_final"]>=s]
            N=grp["N"].sum(); NC=grp["NC"].sum()
            p_grp=(NC/max(1,N))
            if p_grp >= 2*p_br:
                s_star=float(s); break
    df_sel = df_all if s_star is None else df_all.loc[df_all["score_final"]>=s_star].copy()
    meta={'mode':'top10','start':start,'end':end,'alpha':alpha,'p_br':p_br,'score_cut':s_star,
          'n_all':int(df_all.shape[0]),'n_sel':int(df_sel.shape[0]),
          'label_lhs':'Grupo (score ≥ S*)','safe_stub':'grupo_score','topn':topn,
          'total_period':int(total_calc), 'nc_period':int(nc_calc)}
    return df_sel, meta, df_all

# ──────────────────────────────────────────────────────────────────────
# Exportação para PDF
# ──────────────────────────────────────────────────────────────────────

def _export_org_to_pdf(org_path: str) -> Optional[str]:
    """
    Converte um arquivo .org para .pdf.
    - Se detectar 'babel' no .org, prioriza Emacs/ox-latex (evita conflito do Pandoc).
    - Caso contrário, tenta Pandoc (com xelatex); fallback Emacs.
    Retorna caminho absoluto do PDF ou None.
    """
    try:
        if not org_path or not os.path.exists(org_path):
            print("⚠️ ORG não encontrado para PDF.")
            return None

        cwd = os.path.dirname(os.path.abspath(org_path)) or "."
        org_basename = os.path.basename(org_path)
        pdf_basename = os.path.splitext(org_basename)[0] + ".pdf"
        out_pdf_abs_expected = os.path.join(cwd, pdf_basename)

        try:
            with open(org_path, "r", encoding="utf-8", errors="ignore") as f:
                content_lower = f.read().lower()
        except Exception:
            content_lower = ""

        prefers_emacs = ("babel" in content_lower)

        def _try_pandoc() -> Optional[str]:
            if not shutil.which("pandoc"):
                return None
            engine = None
            for e in ("xelatex", "tectonic", "pdflatex", "lualatex"):
                if shutil.which(e):
                    engine = e
                    break
            cmd = ["pandoc", org_basename, "-o", pdf_basename]
            if engine:
                cmd.append(f"--pdf-engine={engine}")
            cmd.extend(["-V", "lang=pt-BR"])
            r = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if r.returncode == 0 and os.path.exists(out_pdf_abs_expected):
                print(f"🖨️ PDF gerado (pandoc/{engine or 'default'}): {out_pdf_abs_expected}")
                return out_pdf_abs_expected
            else:
                print(f"⚠️ Falha pandoc ({r.returncode}). Detalhe (parcial): {r.stderr[:400]}")
                return None

        def _try_emacs() -> Optional[str]:
            if not shutil.which("emacs"):
                return None
            import time, glob
            t0 = time.time()
            elisp = (
                "(progn "
                "(require 'ox-latex) "
                "(when (executable-find \"latexmk\") "
                "  (setq org-latex-pdf-process "
                "        (list \"latexmk -pdf -quiet -shell-escape -interaction=nonstopmode -f %f\"))) "
                f"(find-file \"{org_basename}\") "
                "(let ((outfile (org-latex-export-to-pdf))) "
                "  (princ (or outfile \"\"))))"
            )
            r = subprocess.run(
                ["emacs", "--batch", "-Q", "--eval", elisp],
                cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            out_path = (r.stdout or "").strip().splitlines()[-1] if r.stdout else ""
            if out_path:
                out_pdf_abs = out_path if os.path.isabs(out_path) else os.path.join(cwd, out_path)
                if os.path.exists(out_pdf_abs):
                    print(f"🖨️ PDF gerado (Emacs): {out_pdf_abs}")
                    return out_pdf_abs
            pdfs = glob.glob(os.path.join(cwd, "*.pdf"))
            pdfs_after = [p for p in pdfs if os.path.getmtime(p) >= (t0 - 1.0)]
            if pdfs_after:
                newest = max(pdfs_after, key=os.path.getmtime)
                print(f"🖨️ PDF gerado (Emacs, detectado por mtime): {newest}")
                return newest
            print(f"⚠️ Falha Emacs/ox-latex ({r.returncode}). Detalhe (parcial): {r.stderr[:400]}")
            return None

        pdf_path = _try_emacs() or _try_pandoc() if prefers_emacs else _try_pandoc() or _try_emacs()
        if not pdf_path:
            print("⚠️ Não foi possível gerar PDF (verifique Pandoc/Emacs+LaTeX).")
        return pdf_path

    except Exception as e:
        print(f"⚠️ Erro ao gerar PDF: {e}")
        return None

# ──────────────────────────────────────────────────────────────────────
# Main 
# ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    # Atalho: --all-tests liga um conjunto de testes e parâmetros padrão
    if args.all_tests:
        args.test_binomial = True
        args.betabin = True
        args.permute_weight = args.permute_weight or 5000
        args.cmh = args.cmh or "by=cr"
        args.psa = args.psa or 10000

    # Build bases
    if args.perito:
        with sqlite3.connect(DB_PATH) as conn:
            schema = _detect_schema(conn)
            df_n = _fetch_perito_n_nc(conn, args.start, args.end, schema)
            if df_n.empty:
                df_all = pd.DataFrame(columns=['nomePerito','N','NC','cr','dr','uo','E','IV_vagas','score_final'])
                df_sel = df_all.copy()
                meta = {
                    'mode': 'single',
                    'perito': args.perito,
                    'start': args.start,
                    'end': args.end,
                    'alpha': args.alpha,
                    'p_br': 0.0,
                    'score_cut': None,
                    'n_all': 0,
                    'n_sel': 0,
                    'label_lhs': args.perito,
                    'safe_stub': args.perito,
                    'topn': args.topn
                }
                total_calc = 0
                nc_calc = 0
            else:
                p_br_calc, total_calc, nc_calc = _compute_p_br_and_totals(conn, args.start, args.end, schema) if args.pbr is None else (args.pbr, None, None)
                p_br = p_br_calc if args.pbr is None else args.pbr
                df_scores = _fetch_scores(conn)
                df_all = _prep_base(df_n, df_scores, p_br, args.alpha, min_analises=0)
                df_sel = df_all.loc[df_all["nomePerito"].str.strip().str.upper() == args.perito.strip().upper()].copy()
                meta = {
                    'mode': 'single',
                    'perito': args.perito,
                    'start': args.start,
                    'end': args.end,
                    'alpha': args.alpha,
                    'p_br': p_br,
                    'score_cut': None,
                    'n_all': int(df_all.shape[0]),
                    'n_sel': int(df_sel.shape[0]),
                    'label_lhs': args.perito,
                    'safe_stub': args.perito,
                    'topn': args.topn
                }
        s_star = None
    else:
        df_sel, meta, df_all = _build_group(
            args.start, args.end, args.alpha, args.pbr,
            min_analises=args.min_analises, topn=args.topn,
            minimo_duplo_nc=args.minimo_duplo_nc
        )
        s_star = meta.get("score_cut", None)

        # ⚠️ NOVO: se --no-sstar, desliga o corte e usa TODOS os peritos
        if args.no_sstar:
            s_star = None
            df_sel = df_all.copy()
            meta['score_cut'] = None
            meta['n_sel'] = int(df_sel.shape[0])
            meta['label_lhs'] = 'Grupo (sem corte de score)'

        total_calc = meta.get("total_period", None)
        nc_calc = meta.get("nc_period", None)

    # Métricas de impacto
    NC_period = int(df_all["NC"].sum()) if not df_all.empty else int(nc_calc or 0)
    iv_total_period = int(math.ceil(float(args.alpha) * float(NC_period)))
    iv_total_sel = int(df_sel["IV_vagas"].sum()) if not df_sel.empty else 0
    peso_sel = (iv_total_sel / iv_total_period) if iv_total_period > 0 else 0.0

    delta_tmea_sel = _calc_delta_tmea(iv_total_sel, tmea_br=args.tmea_br, cap_br=args.cap_br, att_br=args.att_br)
    delta_tmea_period = _calc_delta_tmea(iv_total_period, tmea_br=args.tmea_br, cap_br=args.cap_br, att_br=args.att_br)

    meta.update({
        'iv_total_sel': iv_total_sel,
        'iv_total_period': iv_total_period,
        'peso_sel': peso_sel,
        'delta_tmea_sel': delta_tmea_sel,
        'delta_tmea_period': delta_tmea_period,
        'tmea_br': args.tmea_br,
        'topn': args.topn,
        'n_all': int(df_all.shape[0]),
        'n_sel': int(df_sel.shape[0])
    })

    # Bootstrap do peso (IC95%)
    if args.bootstrap_peso and not df_all.empty:
        R = int(args.bootstrap_peso)
        rng = np.random.default_rng()
        weights = []
        base_idx = df_all.index.values
        n_sel = df_sel.shape[0]
        for _ in range(R):
            boot_idx = rng.choice(base_idx, size=len(base_idx), replace=True)
            boot = df_all.loc[boot_idx].copy()
            if args.bootstrap_recalc_sstar and 'score_final' in boot.columns and not boot['score_final'].isna().all():
                m_all, sel = _recompute_with_params(boot, args.alpha, meta['p_br'], _elbow_cutoff_score(boot))
            else:
                m_all, sel_ref = _recompute_with_params(boot, args.alpha, meta['p_br'], s_star)
                if s_star is None and n_sel > 0 and sel_ref.shape[0] != n_sel:
                    sel = m_all.sort_values("IV_vagas", ascending=False).head(n_sel).copy()
                else:
                    sel = sel_ref
            iv_period_b = int(math.ceil(float(args.alpha) * float(m_all["NC"].sum())))
            iv_sel_b = int(sel["IV_vagas"].sum()) if not sel.empty else 0
            w_b = (iv_sel_b / iv_period_b) if iv_period_b > 0 else 0.0
            weights.append(w_b)
        ws = np.array(weights, dtype=float)
        lo = float(np.percentile(ws, 2.5))
        hi = float(np.percentile(ws, 97.5))
        meta['peso_ci'] = (lo, hi)
    else:
        meta['peso_ci'] = None

    # Sensibilidade determinística (tornado)
    png_tornado = None
    sens_delta = {}
    if args.sens_plot and not df_all.empty:
        for sign, key in [(-1, "alpha_minus"), (1, "alpha_plus")]:
            a2 = max(min(args.alpha * (1.0 + sign * args.sens_alpha_frac), 0.9999), 1e-4)
            m2, sel2 = _recompute_with_params(df_all, a2, meta['p_br'], s_star)
            ivp2 = int(math.ceil(a2 * float(m2["NC"].sum())))
            ivs2 = int(sel2["IV_vagas"].sum()) if not sel2.empty else 0
            sens_delta[key] = (ivs2 / ivp2) * 100.0 if ivp2 > 0 else 0.0
        for sign, key in [(-1, "pbr_minus"), (1, "pbr_plus")]:
            p2 = max(min(meta['p_br'] + sign * args.sens_pbr_pp, 0.9999), 1e-6)
            m2, sel2 = _recompute_with_params(df_all, args.alpha, p2, s_star)
            ivp2 = int(math.ceil(args.alpha * float(m2["NC"].sum())))
            ivs2 = int(sel2["IV_vagas"].sum()) if not sel2.empty else 0
            sens_delta[key] = (ivs2 / ivp2) * 100.0 if ivp2 > 0 else 0.0
        png_tornado = exportar_png_tornado(meta, sens_delta, args.sens_alpha_frac, args.sens_pbr_pp)

    # Estratos
    df_strat_tot = pd.DataFrame()
    df_strat_sel = pd.DataFrame()
    png_strat = None
    by_key = None
    if args.by and not df_all.empty:
        by_key = args.by
        df_strat_tot, df_strat_sel = _compute_strata(df_all, df_sel, by_key, args.alpha, meta['p_br'])
        png_strat = exportar_png_strat(df_strat_tot, df_strat_sel, by_key, meta)

    # TESTES estatísticos
    tests: Dict[str, Any] = {}
    if args.test_binomial and not df_all.empty:
        tests['binomial_df'] = run_test_binomial(df_all, meta['p_br'])

    if args.betabin and not df_all.empty:
        bb_df, rho_mom = run_test_betabin(df_all, meta['p_br'])
        tests['betabin_df'] = bb_df
        tests['rho_mom'] = rho_mom

    if args.permute_weight and not df_all.empty and not df_sel.empty:
        stratify = by_key if (args.permute_stratify and by_key in ('cr','dr','uo')) else None
        perm_p, perm_png = run_permutation_weight(df_all, df_sel, args.alpha, meta['p_br'], args.permute_weight, stratify_by=stratify)
        tests['perm_p'] = perm_p
        tests['perm_png'] = perm_png
        tests['perm_R'] = int(args.permute_weight)

    if args.cmh:
        cmh_arg = args.cmh.strip()
        if cmh_arg.lower().startswith("by="):
            cmh_by = cmh_arg.split("=", 1)[1].strip().lower()
        else:
            cmh_by = cmh_arg.strip().lower()
        if cmh_by in ("cr","dr","uo") and not df_all.empty:
            ormh, x2, pval = run_cmh(df_all, df_sel, cmh_by)
            tests['cmh'] = (ormh, x2, pval, cmh_by)

    if args.psa and not df_all.empty:
        ci, psa_png = run_psa(df_all, df_sel, args.alpha, meta['p_br'],
                              total=int(total_calc or df_all["N"].sum()),
                              nc=int(nc_calc or df_all["NC"].sum()),
                              R=int(args.psa), s_star=s_star, alpha_strength=args.psa_alpha_strength)
        tests['psa_ci'] = ci
        tests['psa_png'] = psa_png
        tests['psa_R'] = int(args.psa)

    # Comentário geral adicional (via utils.comentarios se disponível)
    comment_text = None
    if args.add_comments and _COMENT_FUNC is not None:
        try:
            comment_text = _COMENT_FUNC(df_all.copy(), df_sel.copy(), dict(meta)) or None
            if comment_text:
                comment_text = _protect_comment_text(comment_text, word_cap=args.max_words)
        except Exception:
            comment_text = None

    # PNGs
    png_top = exportar_png_top(df_sel, meta, args.label_maxlen, args.label_fontsize) if args.export_png else None
    png_cotovelo = None
    if 'score_final' in df_all.columns and not df_all['score_final'].isna().all():
        png_cotovelo = plot_curva_cotovelo(
            df_all.copy(), s_star,
            args.start, args.end,
            iv_selected=iv_total_sel, iv_periodo=iv_total_period,
            peso_selected=peso_sel, delta_tmea_sel=delta_tmea_sel,
            delta_tmea_periodo=delta_tmea_period, tmea_br=args.tmea_br
        )

    cuts = {
        "alpha": args.alpha,
        "p_br": meta['p_br'],
        "score_cut": None if s_star is None else round(float(s_star), 6),
        "min_analises": args.min_analises if not args.perito else None,
        "minimo_duplo_nc": args.minimo_duplo_nc if not args.perito else None,
        "by": by_key
    }

    # Agregado TopN (opcional)
    topn_agg = None
    if args.top10_aggregate and not df_sel.empty:
        topn_agg = _calc_topn_aggregate(df_sel, iv_total_period, args.topn, tmea_br=args.tmea_br, cap_br=args.cap_br, att_br=args.att_br)
        if _COMENT_TOPN is None:
            try:
                payload = dict(topn_agg); payload.update({"topn": int(args.topn), "periodo":[args.start,args.end]})
                _ = _comment_from_gpt_or_fallback("TopN agregado", payload,
                    f"O Top{args.topn} concentra {topn_agg['iv_top']} vagas (~{topn_agg['peso_top']*100:.1f}% do período), "
                    f"com estimativa de ΔTMEA≈ {topn_agg['delta_tmea_top'] or 'n/d'} dia(s).")
            except Exception:
                pass

    # Exportar MD/ORG/PDF
    md_path = org_path = final_org = pdf_path = None

    if args.export_md:
        md_path = exportar_md(df_sel, meta, cuts)

    if args.export_org:
        org_path = exportar_org(
            df_sel, meta, cuts,
            png_top=png_top,
            comment_text=comment_text if args.export_comment_org or args.add_comments else None,
            png_tornado=png_tornado,
            by=by_key,
            df_strat_tot=df_strat_tot if not df_strat_tot.empty else None,
            df_strat_sel=df_strat_sel if not df_strat_sel.empty else None,
            png_strat=png_strat,
            tests=tests,
            png_cotovelo=png_cotovelo,
            topn_agg=topn_agg,
            per_top10_sections=bool(args.per_top10_sections),
            explain_methods=bool(args.explain_methods),
            model=args.model, max_words=args.max_words, temperature=args.temperature
        )

    if args.final_org and org_path:
        final_org = montar_org_final(
            header_org=args.header_org,
            front_org=args.front_org,
            org_gerado=org_path,
            start=args.start,
            end=args.end,
            final_org_name=args.final_org_name
        )
        if args.export_pdf:
            pdf_path = _export_org_to_pdf(final_org)

    # Shipping (movimenta e limpa EXPORT_DIR)
    if args.ship_outputs:
        out_period = f"{args.start}_a_{args.end}"
        dest_root = os.path.join(BASE_DIR, args.out_root, out_period, args.out_subdir)
        os.makedirs(dest_root, exist_ok=True)
        # subpastas padrão (png/org/pdf/md/testes)
        sub_png = os.path.join(dest_root, "png"); os.makedirs(sub_png, exist_ok=True)
        sub_org = os.path.join(dest_root, "org"); os.makedirs(sub_org, exist_ok=True)
        sub_pdf = os.path.join(dest_root, "pdf"); os.makedirs(sub_pdf, exist_ok=True)
        sub_md  = os.path.join(dest_root, "md");  os.makedirs(sub_md,  exist_ok=True)
        sub_tests = os.path.join(dest_root, "testes"); os.makedirs(sub_tests, exist_ok=True)

        def _mv(p, subdir):
            if not p or not os.path.exists(p): return
            shutil.move(p, os.path.join(subdir, os.path.basename(p)))

        # move artefatos
        for pth in [png_top, png_tornado, png_strat, png_cotovelo]:
            _mv(pth, sub_png)
        for pth in [org_path, final_org]:
            _mv(pth, sub_org)
        if pdf_path: _mv(pdf_path, sub_pdf)
        if md_path:  _mv(md_path, sub_md)
        for pth in [tests.get("perm_png"), tests.get("psa_png")]:
            _mv(pth, sub_tests)

        # limpa EXPORT_DIR (somente o que sobrar)
        try:
            for name in os.listdir(EXPORT_DIR):
                try:
                    os.remove(os.path.join(EXPORT_DIR, name))
                except Exception:
                    pass
        except Exception:
            pass

        print(f"📦 Artefatos organizados em: {dest_root}")

    # Resumo console
    print("\n=== Impacto na Fila — Resumo ===")
    if args.perito:
        print(f"Perito: {args.perito}")
    else:
        sstar_str = "sem corte" if args.no_sstar else (meta.get('score_cut', 'auto/nd'))
        print(f"Grupo (score ≥ S*): S* = {sstar_str}")
    print(f"Período: {args.start} a {args.end}")
    print(f"α = {args.alpha} | p_BR = {meta['p_br']:.4f}")
    print(f"IV período (vagas): {iv_total_period} | IV selecionado: {iv_total_sel} | peso: {peso_sel*100:.2f}%")
    if meta.get('peso_ci'):
        lo, hi = meta['peso_ci']
        print(f"IC95% (peso): [{lo*100:.2f}%; {hi*100:.2f}%]")
    if delta_tmea_period is not None:
        print(f"ΔTMEA período ≈ {delta_tmea_period} dia(s)")
    if delta_tmea_sel is not None:
        print(f"ΔTMEA selecionado ≈ {delta_tmea_sel} dia(s)")
    if topn_agg:
        print(f"Top{args.topn}: IV={topn_agg['iv_top']} | peso={topn_agg['peso_top']*100:.2f}% | ΔTMEA≈ {topn_agg['delta_tmea_top'] or 'n/d'} d")
    if png_top: print(f"PNG Top: {png_top}")
    if png_cotovelo: print(f"PNG Cotovelo: {png_cotovelo}")
    if png_tornado: print(f"PNG Tornado: {png_tornado}")
    if png_strat: print(f"PNG Estratos: {png_strat}")
    if md_path: print(f"Markdown: {md_path}")
    if org_path: print(f"ORG (gerado): {org_path}")
    if final_org: print(f"ORG Final: {final_org}")
    if pdf_path: print(f"PDF: {pdf_path}")


if __name__ == "__main__":
    main()
