#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Indicadores (composto) — Perito OU Top 10 piores vs Brasil (excl.)
Barras (4 indicadores) + linhas (média, mediana, média+DP do BR-excl.)
e quadro de “cortes atingidos” (Top10: X/Y; Individual: Sim/Não).

Indicadores (%):
1) % NC                         → (motivoNaoConformado=1) / total * 100
2) Produtividade (% do alvo)   → (total / horas_efetivas) / alvo * 100
3) ≤ 15s (%)                    → (dur <= 15s) / total * 100
4) Sobreposição (%)            → % de análises que participam de overlap

Regras:
- Exclui análises com duração > 1h
- Duração: (fim - ini). Se fim faltar, tenta `duracaoPericia` 'HH:MM:SS'
- Sobreposição: análise participa se seu intervalo cruza outro (mesmo perito)

Cortes (opcionais):
--cut-nc-pct, --cut-prod-pct (em % do alvo), --cut-le15s-pct, --cut-overlap-pct
Alvo de produtividade: --alvo-prod (padrão 50 análises/h)

Saídas:
--export-png, --export-org, (opcionais: --chart ASCII)
"""

import os
import sys
import argparse
import sqlite3
from typing import List, Dict, Any, Tuple, Optional

# permitir imports de utils/*
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import plotext as p

BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)


# -----------------------
# Utilidades de schema
# -----------------------

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?",
        (name,)
    ).fetchone() is not None

def detect_analises_table(conn: sqlite3.Connection) -> str:
    for t in ('analises', 'analises_atestmed'):
        if table_exists(conn, t):
            return t
    raise RuntimeError("Não encontrei 'analises' nem 'analises_atestmed'.")

# -----------------------
# Carga de dados
# -----------------------

def load_period(conn: sqlite3.Connection, start: str, end: str) -> pd.DataFrame:
    t = detect_analises_table(conn)
    sql = f"""
        SELECT a.protocolo, a.siapePerito,
               a.dataHoraIniPericia AS ini,
               a.dataHoraFimPericia AS fim,
               a.duracaoPericia     AS dur_txt,
               a.motivoNaoConformado AS nc,
               p.nomePerito
          FROM {t} a
          JOIN peritos p ON p.siapePerito = a.siapePerito
         WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
    """
    df = pd.read_sql(sql, conn, params=(start, end))
    return df

def parse_durations(df: pd.DataFrame) -> pd.DataFrame:
    # parse ini/fim
    df = df.copy()
    df['ini_dt'] = pd.to_datetime(df['ini'], errors='coerce')
    df['fim_dt'] = pd.to_datetime(df['fim'], errors='coerce')

    # duração por coluna dur_txt se necessário
    def hhmmss_to_sec(s: str) -> Optional[float]:
        if not isinstance(s, str) or not s:
            return None
        parts = s.strip().split(':')
        if len(parts) != 3:
            return None
        try:
            h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
            return h*3600 + m*60 + sec
        except Exception:
            return None

    # duração em segundos
    dur = (df['fim_dt'] - df['ini_dt']).dt.total_seconds()
    need_fallback = dur.isna()
    if need_fallback.any():
        dur_fb = df.loc[need_fallback, 'dur_txt'].apply(hhmmss_to_sec)
        dur.loc[need_fallback] = dur_fb.values

    df['dur_s'] = pd.to_numeric(dur, errors='coerce')
    # filtros: remover <=0, e >1h
    df = df[df['dur_s'].notna()]
    df = df[df['dur_s'] > 0]
    df = df[df['dur_s'] <= 3600]  # regra do projeto
    # NC normalizar (0/1)
    df['nc'] = pd.to_numeric(df['nc'], errors='coerce').fillna(0).astype(int).clip(0, 1)
    return df

# -----------------------
# Métricas por perito
# -----------------------

def overlap_percent_for_perito(rows: pd.DataFrame) -> float:
    # marca análises que participam de qualquer overlap
    x = rows[['ini_dt', 'fim_dt']].sort_values('ini_dt').reset_index(drop=True)
    if x.empty:
        return 0.0
    overlapped = np.zeros(len(x), dtype=bool)
    # sweep linear
    current_end = pd.Timestamp.min
    last_idx = -1
    for i, (ini, fim) in enumerate(zip(x['ini_dt'], x['fim_dt'])):
        if pd.isna(ini) or pd.isna(fim):
            continue
        if ini < current_end:  # overlap com anterior recente
            overlapped[i] = True
            if last_idx >= 0:
                overlapped[last_idx] = True
            current_end = max(current_end, fim)
        else:
            current_end = fim
            last_idx = i
    pct = overlapped.sum() / len(x) * 100.0
    return float(pct)

def perito_metrics(rows: pd.DataFrame, alvo_prod: float) -> Dict[str, float]:
    # rows: apenas um perito
    total = len(rows)
    if total == 0:
        return dict(nc_pct=0.0, prod_pct=0.0, le15s_pct=0.0, overlap_pct=0.0,
                    prod_abs=0.0)

    horas = rows['dur_s'].sum() / 3600.0
    prod_abs = (total / horas) if horas > 0 else 0.0
    prod_pct = (prod_abs / alvo_prod * 100.0) if alvo_prod > 0 else 0.0

    nc_pct = rows['nc'].sum() / total * 100.0
    le15s_pct = (rows['dur_s'] <= 15).sum() / total * 100.0
    # precisa das datas para overlap
    overlap_pct = overlap_percent_for_perito(rows)

    return dict(nc_pct=float(nc_pct),
                prod_pct=float(prod_pct),
                le15s_pct=float(le15s_pct),
                overlap_pct=float(overlap_pct),
                prod_abs=float(prod_abs))

def build_panels(df: pd.DataFrame,
                 grupo: List[str],
                 alvo_prod: float) -> Tuple[Dict[str, float], Dict[str, float],
                                            pd.DataFrame]:
    """
    Retorna:
      - painéis do GRUPO (selected)   → {'nc_pct', 'prod_pct', 'le15s_pct', 'overlap_pct'}
      - painéis do BRASIL-EXCL        → mesmas chaves
      - dataframe com métricas por perito do BR-excl (para média/mediana/DP)
    """
    df_g = df[df['nomePerito'].str.upper().isin([g.upper() for g in grupo])]
    df_b = df[~df['nomePerito'].str.upper().isin([g.upper() for g in grupo])]

    # métricas agregadas de GRUPO: (agregação correta: total/horas para prod)
    def panel_from(df_any: pd.DataFrame) -> Dict[str, float]:
        if df_any.empty:
            return dict(nc_pct=0.0, prod_pct=0.0, le15s_pct=0.0, overlap_pct=0.0)
        # por perito para overlap (% de análises com overlap ponderado por contagem)
        metrics = []
        for nome, sub in df_any.groupby('nomePerito'):
            metrics.append(perito_metrics(sub, alvo_prod))
        mdf = pd.DataFrame(metrics)
        # agregação ponderada por volume para NC e ≤15s
        total = len(df_any)
        nc_pct = df_any['nc'].sum() / total * 100.0
        le15s_pct = (df_any['dur_s'] <= 15).sum() / total * 100.0
        # prod: total / horas
        horas = df_any['dur_s'].sum() / 3600.0
        prod_abs = (total / horas) if horas > 0 else 0.0
        prod_pct = (prod_abs / alvo_prod * 100.0) if alvo_prod > 0 else 0.0
        # overlap: média ponderada pelo nº de análises de cada perito
        weights = df_any.groupby('nomePerito').size()
        ov = 0.0
        for (nome, sub), w in zip(df_any.groupby('nomePerito'), weights):
            ov += overlap_percent_for_perito(sub) * (w / total)
        return dict(nc_pct=float(nc_pct),
                    prod_pct=float(prod_pct),
                    le15s_pct=float(le15s_pct),
                    overlap_pct=float(ov))

    grp_panel = panel_from(df_g)
    br_panel  = panel_from(df_b)

    # métricas por perito do BR-excl (p/ média/mediana/DP das linhas)
    metrics_b = []
    for nome, sub in df_b.groupby('nomePerito'):
        m = perito_metrics(sub, alvo_prod)
        m['nomePerito'] = nome
        metrics_b.append(m)
    mdf_b = pd.DataFrame(metrics_b)
    return grp_panel, br_panel, mdf_b

# -----------------------
# Contagem de “cortes”
# -----------------------

def count_cut_hits(df: pd.DataFrame,
                   grupo: List[str],
                   alvo_prod: float,
                   cut_nc_pct: Optional[float],
                   cut_prod_pct: Optional[float],
                   cut_le15s_pct: Optional[float],
                   cut_overlap_pct: Optional[float]) -> Dict[str, Any]:
    """
    Para Top10: retorna {'nc': x_y, 'prod': x_y, 'le15s': x_y, 'overlap': x_y}
    Para Individual: {'nc': True/False, ...}
    """
    peritos = sorted(set([n for n in df['nomePerito'].unique() if n]))
    sel = [p for p in peritos if p.upper() in {g.upper() for g in grupo}]

    def hit_row(sub: pd.DataFrame) -> Dict[str, bool]:
        m = perito_metrics(sub, alvo_prod)
        return dict(
            nc = (cut_nc_pct is not None and m['nc_pct'] >= cut_nc_pct),
            prod = (cut_prod_pct is not None and m['prod_pct'] >= cut_prod_pct),
            le15s = (cut_le15s_pct is not None and m['le15s_pct'] >= cut_le15s_pct),
            overlap = (cut_overlap_pct is not None and m['overlap_pct'] >= cut_overlap_pct),
        )

    if len(sel) == 1:
        sub = df[df['nomePerito'].str.upper()==sel[0].upper()]
        return hit_row(sub)

    # grupo
    hits = dict(nc=0, prod=0, le15s=0, overlap=0)
    for nome in sel:
        sub = df[df['nomePerito'].str.upper()==nome.upper()]
        h = hit_row(sub)
        for k in hits.keys():
            hits[k] += int(bool(h[k]))
    return dict(
        nc=f"{hits['nc']}/{len(sel)}" if cut_nc_pct is not None else "—",
        prod=f"{hits['prod']}/{len(sel)}" if cut_prod_pct is not None else "—",
        le15s=f"{hits['le15s']}/{len(sel)}" if cut_le15s_pct is not None else "—",
        overlap=f"{hits['overlap']}/{len(sel)}" if cut_overlap_pct is not None else "—",
        _n=len(sel)
    )

# -----------------------
# Gráfico
# -----------------------

def plot_png(start: str, end: str,
             grp_title: str,
             grp_panel: Dict[str, float],
             br_panel: Dict[str, float],
             mdf_b: pd.DataFrame,
             alvo_prod: float,
             cut_prod_pct: Optional[float],
             cut_hits: Dict[str, Any],
             out_path: str) -> str:

    labels  = ['% NC', 'Produtividade (% alvo)', '≤ 15s (%)', 'Sobreposição (%)']
    grp_vals = [grp_panel['nc_pct'], grp_panel['prod_pct'], grp_panel['le15s_pct'], grp_panel['overlap_pct']]
    br_vals  = [br_panel['nc_pct'],  br_panel['prod_pct'],  br_panel['le15s_pct'],  br_panel['overlap_pct']]

    def line_stat(col):
        v = mdf_b[col].dropna().values if col in mdf_b.columns else np.array([])
        if v.size == 0:
            return 0.0, 0.0, 0.0
        mu = float(np.mean(v)); sd = float(np.std(v)); med = float(np.median(v))
        return mu, med, mu + sd

    mean_nc, med_nc, dp_nc = line_stat('nc_pct')
    mean_pr, med_pr, dp_pr = line_stat('prod_pct')
    mean_15, med_15, dp_15 = line_stat('le15s_pct')
    mean_ov, med_ov, dp_ov = line_stat('overlap_pct')

    means, meds, meanp1 = [mean_nc, mean_pr, mean_15, mean_ov], [med_nc, med_pr, med_15, med_ov], [dp_nc, dp_pr, dp_15, dp_ov]

    fig, ax = plt.subplots(figsize=(11, 6.5), dpi=220)
    x = np.arange(len(labels)); width = 0.34

    ax.bar(x - width/2, grp_vals, width, label=grp_title, edgecolor='black')
    ax.bar(x + width/2, br_vals,  width, label='Brasil (excl.)', edgecolor='black')

    ax.plot(x, means,  'o--', label='Média (BR-excl.)')
    ax.plot(x, meds,   's:',  label='Mediana (BR-excl.)')
    ax.plot(x, meanp1, '^-.', label='Média+DP (BR-excl.)')

    if cut_prod_pct is not None:
        ax.plot([x[1]], [cut_prod_pct], marker='D', color='red', linestyle='None', label='Corte Prod. (% alvo)')

    ax.set_xticks(x, labels, rotation=20, ha='right')
    ax.set_ylabel("%")
    ax.set_title(f"Indicadores (composto) – {grp_title} vs Brasil (excl.)\n"
                 f"{start} a {end}  |  alvo prod: {int(alvo_prod)}/h")
    ax.grid(axis='y', linestyle='--', alpha=0.4)

    # legenda principal DENTRO do eixo (top-right)
    leg = ax.legend(loc='upper right', framealpha=0.95)

    # rótulos nas barras
    ymax = max(grp_vals + br_vals + [1.0])
    for i in range(len(labels)):
        ax.text(x[i] - width/2, grp_vals[i] + ymax*0.02, f"{grp_vals[i]:.1f}%", ha='center', va='bottom', fontsize=9)
        ax.text(x[i] + width/2, br_vals[i]  + ymax*0.02, f"{br_vals[i]:.1f}%",  ha='center', va='bottom', fontsize=9)

    # --- caixa “cortes atingidos” logo ABAIXO da legenda, dentro do eixo ---
    if '_n' in cut_hits:  # Top10
        n = cut_hits['_n']
        lines = [
            f"Cortes atingidos (grupo, {n} peritos):",
            f" • % NC ≥ corte .......... {cut_hits['nc']}",
            f" • Prod (% alvo) ≥ corte . {cut_hits['prod']}",
            f" • ≤ 15s ≥ corte .......... {cut_hits['le15s']}",
            f" • Sobreposição ≥ corte .. {cut_hits['overlap']}",
        ]
    else:
        def fmt(v): return "Sim" if v else "Não"
        lines = [
            "Perito atingiu os cortes?",
            f" • % NC ≥ corte .......... {fmt(cut_hits.get('nc'))}",
            f" • Prod (% alvo) ≥ corte . {fmt(cut_hits.get('prod'))}",
            f" • ≤ 15s ≥ corte .......... {fmt(cut_hits.get('le15s'))}",
            f" • Sobreposição ≥ corte .. {fmt(cut_hits.get('overlap'))}",
        ]
    box_text = "\n".join(lines)

    # medir a legenda e posicionar a caixa logo abaixo, em coordenadas do AXES
    fig.canvas.draw()  # necessário pra medir
    bbox_px = leg.get_window_extent(fig.canvas.get_renderer())
    bbox_ax = bbox_px.transformed(ax.transAxes.inverted())

    x_right = min(0.98, bbox_ax.x1)   # 0.98 para não colar na borda
    y_below = max(0.05, bbox_ax.y0 - 0.02)

    ax.text(x_right, y_below, box_text,
            transform=ax.transAxes, ha='right', va='top', fontsize=9,
            bbox=dict(facecolor='white', alpha=0.92, edgecolor='#999'))

    plt.tight_layout()
    out = os.path.abspath(out_path)
    fig.savefig(out, bbox_inches='tight')
    plt.close(fig)
    print(f"✅ PNG salvo em: {out}")
    return out

# -----------------------
# Export ORG
# -----------------------

def export_org(path_png: Optional[str],
               start: str, end: str,
               grp_title: str,
               grp_panel: Dict[str, float],
               br_panel: Dict[str, float],
               alvo_prod: float,
               cuts: Dict[str, Optional[float]],
               cut_hits: Dict[str, Any],
               out_name: str) -> str:

    out_path = os.path.join(EXPORT_DIR, out_name)
    lines = []
    lines.append(f"* Indicadores (composto) – {grp_title} vs Brasil (excl.)")
    lines.append(":PROPERTIES:")
    lines.append(f":PERIODO: {start} a {end}")
    lines.append(f":ALVO_PROD: {alvo_prod}/h")
    cuts_str = ", ".join([f"{k}={v}" for k, v in cuts.items() if v is not None]) or "nenhum"
    lines.append(f":CUTS: {cuts_str}")
    lines.append(":END:\n")

    # Tabela de valores (barras)
    lines.append("| Indicador | " + grp_title + " | Brasil (excl.) |")
    lines.append("|-")
    lines.append(f"| % NC | {grp_panel['nc_pct']:.2f}% | {br_panel['nc_pct']:.2f}% |")
    lines.append(f"| Prod (% alvo) | {grp_panel['prod_pct']:.2f}% | {br_panel['prod_pct']:.2f}% |")
    lines.append(f"| ≤ 15s (%) | {grp_panel['le15s_pct']:.2f}% | {br_panel['le15s_pct']:.2f}% |")
    lines.append(f"| Sobreposição (%) | {grp_panel['overlap_pct']:.2f}% | {br_panel['overlap_pct']:.2f}% |\n")

    # Tabela cortes atingidos
    lines.append("** Cortes atingidos")
    if '_n' in cut_hits:
        lines.append(f"Grupo com {cut_hits['_n']} peritos.")
        lines.append("| Corte | Atingiram |")
        lines.append("|-")
        lines.append(f"| % NC | {cut_hits['nc']} |")
        lines.append(f"| Prod (% alvo) | {cut_hits['prod']} |")
        lines.append(f"| ≤ 15s | {cut_hits['le15s']} |")
        lines.append(f"| Sobreposição | {cut_hits['overlap']} |\n")
    else:
        def sym(b): return "Sim" if b else "Não"
        lines.append("| Corte | Atingiu? |")
        lines.append("|-")
        lines.append(f"| % NC | {sym(cut_hits.get('nc'))} |")
        lines.append(f"| Prod (% alvo) | {sym(cut_hits.get('prod'))} |")
        lines.append(f"| ≤ 15s | {sym(cut_hits.get('le15s'))} |")
        lines.append(f"| Sobreposição | {sym(cut_hits.get('overlap'))} |\n")

    # Imagem
    if path_png and os.path.exists(path_png):
        lines.append("#+CAPTION: Indicadores compostos (barras) e estatísticas do BR-excl. (linhas).")
        lines.append(f"[[file:{os.path.basename(path_png)}]]\n")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"✅ Org salvo em: {out_path}")
    return out_path

# -----------------------
# CLI e pipeline
# -----------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Indicadores (composto): Perito OU Top10 piores vs Brasil (excl.).")
    ap.add_argument('--start', required=True)
    ap.add_argument('--end',   required=True)

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--perito', help='Nome exato do perito')
    g.add_argument('--top10', action='store_true', help='Usa os 10 piores por scoreFinal (no período)')

    ap.add_argument('--min-analises', type=int, default=50, help='Mínimo p/ elegibilidade ao Top10 (padrão: 50)')
    ap.add_argument('--alvo-prod', type=float, default=50.0, help='Alvo de produtividade (análises/h) [50]')

    # cortes (opcionais)
    ap.add_argument('--cut-prod-pct', type=float, default=100.0, help='Corte Produtividade (% do alvo) [100]')
    ap.add_argument('--cut-nc-pct', type=float, default=None, help='Corte % NC (opcional)')
    ap.add_argument('--cut-le15s-pct', type=float, default=None, help='Corte % ≤15s (opcional)')
    ap.add_argument('--cut-overlap-pct', type=float, default=None, help='Corte % sobreposição (opcional)')

    ap.add_argument('--export-png', action='store_true')
    ap.add_argument('--export-org', action='store_true')
    ap.add_argument('--chart', action='store_true', help='Gráfico ASCII no terminal')

    return ap.parse_args()

def top10_names(conn: sqlite3.Connection, start: str, end: str, min_analises: int) -> List[str]:
    t = detect_analises_table(conn)
    sql = f"""
        SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
          FROM indicadores i
          JOIN peritos p ON p.siapePerito = i.perito
          JOIN {t} a     ON a.siapePerito = i.perito
         WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
         GROUP BY p.nomePerito, i.scoreFinal
        HAVING total_analises >= ?
         ORDER BY i.scoreFinal DESC, total_analises DESC
         LIMIT 10
    """
    rows = conn.execute(sql, (start, end, min_analises)).fetchall()
    return [r[0] for r in rows]

def main():
    args = parse_args()

    with sqlite3.connect(DB_PATH) as conn:
        df = load_period(conn, args.start, args.end)

    df = parse_durations(df)
    if df.empty:
        print("⚠️ Sem dados no período.")
        return

    if args.top10:
        with sqlite3.connect(DB_PATH) as conn:
            grupo = top10_names(conn, args.start, args.end, args.min_analises)
        if not grupo:
            print("⚠️ Nenhum perito elegível ao Top10 no período.")
            return
        grp_title = "Top 10 piores"
    else:
        if not args.perito:
            print("ERRO: informe --perito ou --top10.")
            return
        grupo = [args.perito]
        grp_title = args.perito

    # painéis e estatísticas BR-excl.
    grp_panel, br_panel, mdf_b = build_panels(df, grupo, args.alvo_prod)

    # contagem de cortes
    cut_hits = count_cut_hits(
        df, grupo, args.alvo_prod,
        cut_nc_pct=args.cut_nc_pct,
        cut_prod_pct=args.cut_prod_pct,
        cut_le15s_pct=args.cut_le15s_pct,
        cut_overlap_pct=args.cut_overlap_pct
    )

    # montar arquivos
    safe = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in grp_title).strip("_")
    png_name = "indicadores_composto_top10.png" if args.top10 else f"indicadores_composto_{safe}.png"
    org_name = "indicadores_composto_top10.org" if args.top10 else f"indicadores_composto_{safe}.org"

    png_path = None
    if args.export_png:
        png_path = plot_png(
            start=args.start, end=args.end,
            grp_title=grp_title,
            grp_panel=grp_panel, br_panel=br_panel, mdf_b=mdf_b,
            alvo_prod=args.alvo_prod,
            cut_prod_pct=args.cut_prod_pct,
            cut_hits=cut_hits,
            out_path=os.path.join(EXPORT_DIR, png_name)
        )

    if args.export_org:
        cuts_dict = dict(cut_prod_pct=args.cut_prod_pct,
                         cut_nc_pct=args.cut_nc_pct,
                         cut_le15s_pct=args.cut_le15s_pct,
                         cut_overlap_pct=args.cut_overlap_pct)
        export_org(
            path_png=png_path,
            start=args.start, end=args.end,
            grp_title=grp_title,
            grp_panel=grp_panel, br_panel=br_panel,
            alvo_prod=args.alvo_prod,
            cuts=cuts_dict,
            cut_hits=cut_hits,
            out_name=org_name
        )

    if args.chart:
        # gráfico ASCII rápido das barras do painel (grupo vs BR-excl.)
        p.clear_data()
        labels = ['%NC', 'Prod(%alvo)', '≤15s', 'Sobrep.']
        p.multi_bar(labels,
                    [[grp_panel['nc_pct'], grp_panel['prod_pct'], grp_panel['le15s_pct'], grp_panel['overlap_pct']],
                     [br_panel['nc_pct'],  br_panel['prod_pct'],  br_panel['le15s_pct'],  br_panel['overlap_pct']]],
                    label=[grp_title, 'Brasil (excl.)'])
        p.title(f"Indicadores (composto) — {grp_title} vs BR-excl.")
        p.plotsize(90, 20)
        p.show()


if __name__ == "__main__":
    main()

