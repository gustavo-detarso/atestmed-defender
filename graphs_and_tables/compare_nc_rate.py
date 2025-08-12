#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sqlite3
import argparse
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # gerar PNG em ambiente headless
import matplotlib.pyplot as plt

try:
    import plotext as p  # para --chart (ASCII)
except Exception:
    p = None

from utils.comentarios import comentar_nc100  # ajuste se preferir outro gerador

# Caminhos absolutos
BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH    = os.path.join(BASE_DIR, 'db', 'atestmed.db')
EXPORT_DIR = os.path.join(BASE_DIR, 'graphs_and_tables', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
def parse_args():
    p_ = argparse.ArgumentParser(
        description="Compara taxa de não conformação do perito ou Top 10 com Brasil (excl.)."
    )
    p_.add_argument('--start', required=True, help='Data inicial YYYY-MM-DD')
    p_.add_argument('--end',   required=True, help='Data final   YYYY-MM-DD')

    g = p_.add_mutually_exclusive_group(required=True)
    g.add_argument('--perito', help='Nome exato do perito')
    g.add_argument('--nome',   help='Nome exato do perito (alias)')
    g.add_argument('--top10',  action='store_true', help='Comparar o grupo dos 10 piores por scoreFinal')

    p_.add_argument('--min-analises', type=int, default=50,
                    help='Elegibilidade p/ Top 10 (mínimo de análises no período)')

    # Exportações
    p_.add_argument('--export-md',      action='store_true', help='Exporta tabela em Markdown')
    p_.add_argument('--export-png',     action='store_true', help='Exporta gráfico em PNG')
    p_.add_argument('--export-org',     action='store_true', help='Exporta resumo em Org-mode (.org) com a imagem')
    p_.add_argument('--chart',          action='store_true', help='Exibe gráfico ASCII no terminal')
    p_.add_argument('--export-comment', action='store_true', help='Exporta comentário GPT')
    p_.add_argument('--add-comments',   action='store_true', help='Gera comentário automaticamente (modo PDF)')

    return p_.parse_args()

# ────────────────────────────────────────────────────────────────────────────────
# DB helpers
# ────────────────────────────────────────────────────────────────────────────────
def _detect_tables(conn: sqlite3.Connection) -> tuple[str, bool]:
    def has_table(name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
        ).fetchone()
        return row is not None

    analises_tbl = None
    for t in ("analises", "analises_atestmed"):
        if has_table(t):
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({t})").fetchall()}
            if {"siapePerito", "dataHoraIniPericia"}.issubset(cols):
                analises_tbl = t
                break
    if not analises_tbl:
        raise RuntimeError("Não encontrei 'analises' nem 'analises_atestmed' com colunas mínimas.")
    indicadores_ok = has_table("indicadores")
    return analises_tbl, indicadores_ok

def _load_nc_tot(conn: sqlite3.Connection, tbl: str, start: str, end: str) -> pd.DataFrame:
    """
    Carrega, por perito no período, contagens de NC e Total.
    """
    sql = f"""
        SELECT 
            p.nomePerito,
            SUM(CASE WHEN a.motivoNaoConformado != 0 THEN 1 ELSE 0 END) AS nc_count,
            COUNT(*) AS total
        FROM {tbl} a
        JOIN peritos p ON a.siapePerito = p.siapePerito
        WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
        GROUP BY p.nomePerito
    """
    df = pd.read_sql_query(sql, conn, params=(start, end))
    df["nomePerito"] = df["nomePerito"].astype(str).str.strip()
    return df

def _top10_names(conn: sqlite3.Connection, tbl: str, start: str, end: str, min_analises: int) -> list[str]:
    """
    Seleciona 10 piores (maior scoreFinal) com pelo menos min_analises no período.
    """
    sql = f"""
        SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
          FROM indicadores i
          JOIN peritos p  ON i.perito = p.siapePerito
          JOIN {tbl} a    ON a.siapePerito = i.perito
         WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
         GROUP BY p.nomePerito, i.scoreFinal
        HAVING total_analises >= ?
         ORDER BY i.scoreFinal DESC, total_analises DESC
         LIMIT 10
    """
    rows = conn.execute(sql, (start, end, min_analises)).fetchall()
    return [r[0] for r in rows]

# ────────────────────────────────────────────────────────────────────────────────
# Cálculo (ponderado pelo total)
# ────────────────────────────────────────────────────────────────────────────────
def _safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(name)).strip("_") or "output"

def _rate(nc: int, tot: int) -> float:
    return (100.0 * nc / tot) if tot > 0 else 0.0

def calcular_taxas_perito(df: pd.DataFrame, perito: str) -> tuple[float, float, dict]:
    """
    df: colunas [nomePerito, nc_count, total]
    Retorna (% perito, % demais ponderado, resumo dict)
    """
    if perito not in set(df["nomePerito"]):
        similares = df[df["nomePerito"].str.contains(perito, case=False, na=False)]["nomePerito"].unique().tolist()
        sugest = f" Peritos semelhantes: {', '.join(similares)}." if similares else ""
        raise ValueError(f"Perito '{perito}' não encontrado no período.{sugest}")

    lin = df.loc[df["nomePerito"] == perito].iloc[0]
    nc_p, tot_p = int(lin["nc_count"]), int(lin["total"])
    pct_p = _rate(nc_p, tot_p)

    outros = df.loc[df["nomePerito"] != perito]
    nc_o, tot_o = int(outros["nc_count"].sum()), int(outros["total"].sum())
    pct_o = _rate(nc_o, tot_o)

    resumo = {
        "label_left": perito,
        "nc_left": nc_p, "tot_left": tot_p, "pct_left": pct_p,
        "label_right": "Demais",
        "nc_right": nc_o, "tot_right": tot_o, "pct_right": pct_o,
    }
    return pct_p, pct_o, resumo

def calcular_taxas_top10(df: pd.DataFrame, top_names: list[str]) -> tuple[float, float, dict]:
    """
    df: colunas [nomePerito, nc_count, total]
    top_names: lista de nomes (grupo)
    """
    mask_left = df["nomePerito"].isin(set(top_names))
    left = df.loc[mask_left]
    right = df.loc[~mask_left]

    nc_l, tot_l = int(left["nc_count"].sum()), int(left["total"].sum())
    nc_r, tot_r = int(right["nc_count"].sum()), int(right["total"].sum())

    pct_l, pct_r = _rate(nc_l, tot_l), _rate(nc_r, tot_r)

    resumo = {
        "label_left": "Top 10 piores",
        "nc_left": nc_l, "tot_left": tot_l, "pct_left": pct_l,
        "label_right": "Brasil (excl.)",
        "nc_right": nc_r, "tot_right": tot_r, "pct_right": pct_r,
        "top_names": top_names,
    }
    return pct_l, pct_r, resumo

# ────────────────────────────────────────────────────────────────────────────────
# Exportações
# ────────────────────────────────────────────────────────────────────────────────
def exportar_md(grp_title: str, start: str, end: str, resumo: dict) -> str:
    safe = _safe(grp_title.replace(" ", "_"))
    path = os.path.join(EXPORT_DIR, f"compare_nc_rate_{safe}.md")
    with open(path, 'w', encoding='utf-8') as f:
        f.write(f"# Taxa de Não Conformação — {resumo['label_left']} vs {resumo['label_right']}\n\n")
        f.write(f"- Período: {start} a {end}\n\n")
        f.write("| Categoria | NC (n) | Total | Taxa de NC (%) |\n")
        f.write("|-----------|--------|-------|-----------------|\n")
        f.write(f"| {resumo['label_left']} | {resumo['nc_left']} | {resumo['tot_left']} | {resumo['pct_left']:.1f} |\n")
        f.write(f"| {resumo['label_right']} | {resumo['nc_right']} | {resumo['tot_right']} | {resumo['pct_right']:.1f} |\n")
    print("✅ Markdown salvo em", path)
    return path

def exportar_png(title: str, resumo: dict, outfile: str) -> str:
    import matplotlib.pyplot as plt

    # cores padrão usadas nos outros gráficos
    colors = ["#1f77b4", "#ff7f0e"]

    fig, ax = plt.subplots(figsize=(10, 6), dpi=400)
    cats = [resumo["label_left"], resumo["label_right"]]
    vals = [resumo["pct_left"], resumo["pct_right"]]

    bars = ax.bar(cats, vals, color=colors, edgecolor="black")
    ax.set_title(title, pad=15)
    ax.set_ylabel("NC (%)")
    ax.grid(axis='y', linestyle='--', alpha=0.6)

    # limite vertical (cap em 100%, com margem quando possível)
    ymax = max(10.0, min(100.0, max(vals) * 1.15))
    ax.set_ylim(0, ymax)

    left_tuple  = (resumo["nc_left"],  resumo["tot_left"])
    right_tuple = (resumo["nc_right"], resumo["tot_right"])

    # escreve os rótulos; se não couber em cima, escreve DENTRO da barra
    for bar, pct, (nc, tot) in zip(bars, vals, [left_tuple, right_tuple]):
        text = f"{pct:.1f}%\n(n={nc}/{tot})"
        x = bar.get_x() + bar.get_width() / 2
        offset = ymax * 0.02  # margem visual

        # cabe fora?
        if pct + offset * 3 <= ymax:
            y = pct + offset
            va = "bottom"
            color = "black"
        else:
            # coloca dentro da barra, próximo ao topo
            y = max(pct - offset * 1.5, offset * 1.2)
            va = "top"
            color = "white"

        ax.text(x, y, text, ha="center", va=va, fontsize=9, color=color)

    plt.tight_layout()
    fig.savefig(outfile, bbox_inches='tight')
    plt.close(fig)
    print("✅ PNG salvo em", outfile)
    return outfile

def exportar_org(path_png: str, start: str, end: str, resumo: dict, out_name: str) -> str:
    out_path = os.path.join(EXPORT_DIR, out_name)
    lines = []
    lines.append(f"* Taxa de Não Conformação — {resumo['label_left']} vs {resumo['label_right']}")
    lines.append(":PROPERTIES:")
    lines.append(f":PERIODO: {start} a {end}")
    lines.append(":METRICA: Taxa de NC (motivoNaoConformado != 0)")
    lines.append(":AGRUPAMENTO: taxa ponderada pelo total (∑NC/∑Total)")
    if "top_names" in resumo:
        lines.append(f":TOP10: {', '.join(resumo['top_names'])}")
    lines.append(":END:\n")

    lines.append("| Categoria | NC (n) | Total | Taxa de NC (%) |")
    lines.append("|-")
    lines.append(f"| {resumo['label_left']} | {resumo['nc_left']} | {resumo['tot_left']} | {resumo['pct_left']:.2f}% |")
    lines.append(f"| {resumo['label_right']} | {resumo['nc_right']} | {resumo['tot_right']} | {resumo['pct_right']:.2f}% |\n")

    if path_png and os.path.exists(path_png):
        lines.append("#+CAPTION: Comparação da taxa de não conformação (ponderada).")
        lines.append(f"[[file:{os.path.basename(path_png)}]]\n")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("✅ Org salvo em", out_path)
    return out_path

def render_chart_ascii(resumo: dict, title: str) -> None:
    if p is None:
        print("plotext não instalado; pulei o gráfico ASCII.")
        return
    p.clear_data()
    p.bar([resumo["label_left"], resumo["label_right"]],
          [resumo["pct_left"], resumo["pct_right"]])
    p.title(title)
    p.xlabel("")
    p.ylabel("NC (%)")
    p.plotsize(80, 18)
    p.show()

def exportar_comment(tabela_md: str, start: str, end: str, stem: str) -> str:
    comentario = comentar_nc100(tabela_md, start, end)
    path = os.path.join(EXPORT_DIR, f"{stem}_comment.md")
    with open(path, 'w', encoding='utf-8') as f:
        f.write(comentario)
    print("🗒️ Comentário ChatGPT salvo em", path)
    return path

# ────────────────────────────────────────────────────────────────────────────────
# Execução
# ────────────────────────────────────────────────────────────────────────────────
def run_perito(start: str, end: str, perito: str,
               export_md: bool, export_png: bool, export_org: bool,
               chart: bool, export_comment: bool, add_comments: bool) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        tbl, _ = _detect_tables(conn)
        df = _load_nc_tot(conn, tbl, start, end)

    pct_p, pct_o, resumo = calcular_taxas_perito(df, perito)

    title = f"Taxa de NC: {perito} vs Demais"
    safe  = _safe(perito)
    png   = os.path.join(EXPORT_DIR, f"compare_nc_rate_{safe}.png")
    org   = f"compare_nc_rate_{safe}.org"

    md_tbl = None
    if export_md or export_comment or add_comments:
        md_path = exportar_md(perito, start, end, resumo)
        with open(md_path, "r", encoding="utf-8") as f:
            md_tbl = f.read().splitlines()[-4:]  # pega a tabela
        md_tbl = "\n".join(md_tbl)

    if export_png:
        exportar_png(title, resumo, png)

    if export_org:
        if not (os.path.exists(png)):
            exportar_png(title, resumo, png)
        exportar_org(png, start, end, resumo, org)

    if chart:
        render_chart_ascii(resumo, title)

    if export_comment or add_comments:
        exportar_comment(md_tbl, start, end, f"compare_nc_rate_{safe}")

    print(f"\n📊 {perito}: {resumo['pct_left']:.1f}%  |  Demais (ponderado): {resumo['pct_right']:.1f}%")
    print(f"   n={resumo['nc_left']}/{resumo['tot_left']} (perito)  |  "
          f"n={resumo['nc_right']}/{resumo['tot_right']} (demais)\n")

def run_top10(start: str, end: str, min_analises: int,
              export_md: bool, export_png: bool, export_org: bool,
              chart: bool, export_comment: bool, add_comments: bool) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        tbl, has_ind = _detect_tables(conn)
        if not has_ind:
            raise RuntimeError("Tabela 'indicadores' não encontrada — calcule indicadores antes de usar --top10.")
        names = _top10_names(conn, tbl, start, end, min_analises)
        if not names:
            print("⚠️ Nenhum perito elegível para Top 10 nesse período.")
            return
        df = _load_nc_tot(conn, tbl, start, end)

    pct_l, pct_r, resumo = calcular_taxas_top10(df, names)

    title = "Taxa de NC: Top 10 piores vs Brasil (excl.)"
    png   = os.path.join(EXPORT_DIR, "compare_nc_rate_top10.png")
    org   = "compare_nc_rate_top10.org"

    # Monta tabela MD inline p/ comentário/export
    md_tbl = (
        "| Categoria | NC (n) | Total | Taxa de NC (%) |\n"
        "|-----------|--------|-------|-----------------|\n"
        f"| {resumo['label_left']} | {resumo['nc_left']} | {resumo['tot_left']} | {resumo['pct_left']:.1f} |\n"
        f"| {resumo['label_right']} | {resumo['nc_right']} | {resumo['tot_right']} | {resumo['pct_right']:.1f} |\n"
    )

    if export_md or export_comment or add_comments:
        md_path = os.path.join(EXPORT_DIR, "compare_nc_rate_top10.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(f"# {title}\n\n- Período: {start} a {end}\n\n{md_tbl}")
        print("✅ Markdown salvo em", md_path)

    if export_png:
        exportar_png(title, resumo, png)

    if export_org:
        if not os.path.exists(png):
            exportar_png(title, resumo, png)
        exportar_org(png, start, end, resumo, org)

    if chart:
        render_chart_ascii(resumo, title)

    if export_comment or add_comments:
        exportar_comment(md_tbl, start, end, "compare_nc_rate_top10")

    print(f"\n📊 Top 10: {resumo['pct_left']:.1f}%  |  Brasil (excl.): {resumo['pct_right']:.1f}%")
    print(f"   n={resumo['nc_left']}/{resumo['tot_left']} (grupo)  |  "
          f"n={resumo['nc_right']}/{resumo['tot_right']} (Brasil excl.)\n")

# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    if args.top10:
        run_top10(args.start, args.end, args.min_analises,
                  args.export_md, args.export_png, args.export_org,
                  args.chart, args.export_comment, args.add_comments)
    else:
        perito = args.perito or args.nome
        run_perito(args.start, args.end, perito,
                   args.export_md, args.export_png, args.export_org,
                   args.chart, args.export_comment, args.add_comments)

if __name__ == '__main__':
    main()

