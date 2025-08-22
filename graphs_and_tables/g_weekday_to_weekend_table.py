#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tabela por perito: Perito, Matrícula, CR, DR, Quantidade de tarefas
que começaram em dia útil (seg–sex) e terminaram no fim de semana (sáb/dom)
no período informado.

Novidades:
  - Gráfico por CR (ordem decrescente) → rcheck_weekday_to_weekend_by_cr.png
  - Exporta lista de protocolos por perito → rcheck_weekday_to_weekend_protocols.org
  - .org principal + .org de comentário

Saídas (em --out-dir):
  - rcheck_weekday_to_weekend_table.csv
  - rcheck_weekday_to_weekend_table.org
  - rcheck_weekday_to_weekend_table_comment.org
  - rcheck_weekday_to_weekend_table.pdf        [via Emacs, opcional]
  - rcheck_weekday_to_weekend_by_cr.png        [opcional]
  - rcheck_weekday_to_weekend_protocols.org    [opcional, com --export-protocols]

Exemplo:
  python g_weekday_to_weekend_table.py \
    --db db/atestmed.db \
    --start 2025-05-01 --end 2025-05-31 \
    --export-csv --export-org --export-pdf --export-png --export-protocols --verbose
"""

import os
import sys
import argparse
import sqlite3
import shutil
import tempfile
import subprocess
from datetime import datetime

import pandas as pd

import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt


# ───────────────────────── Helpers ─────────────────────────

def log(msg, verbose=False):
    if verbose:
        print(f"[INFO] {msg}", flush=True)

def ensure_dir(p: str):
    if not os.path.isdir(p):
        os.makedirs(p, exist_ok=True)

def table_exists(con, name: str) -> bool:
    cur = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
        (name,),
    )
    return cur.fetchone() is not None

def detect_analises_table(con) -> str:
    for t in ("analises", "analises_atestmed"):
        if table_exists(con, t):
            return t
    raise RuntimeError("Não encontrei 'analises' nem 'analises_atestmed'.")

def pick_col(cols, candidates):
    """Tenta achar a melhor correspondência, por igualdade (case-insensitive) ou contains."""
    low = {c.lower(): c for c in cols}
    for c in candidates:
        if c in cols:
            return c
        if c.lower() in low:
            return low[c.lower()]
    # tentativa por 'contains'
    low_list = [c.lower() for c in cols]
    for cand in candidates:
        for i, lc in enumerate(low_list):
            if cand.lower() in lc:
                return cols[i]
    return None

def pick_duration_columns(cols):
    num = pick_col(cols, ["tempoAnaliseSeg","tempoAnalise","duracaoSegundos","duracao_seg","tempo_seg"])
    txt = pick_col(cols, ["duracaoPericia","duracao_txt","tempoFmt","tempo_formatado","duracao_texto"])
    return num, txt

def pick_protocol_column(cols):
    return pick_col(cols, [
        "protocolo", "numeroProtocolo", "numProtocolo",
        "idProtocolo", "idAtendimento", "protocoloAtendimento",
        "protocolo_pericia", "num_protocolo", "n_protocolo"
    ])

def org_table_from_df(df: pd.DataFrame) -> str:
    headers = list(df.columns)
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join("---" for _ in headers) + "|")
    for _, row in df.iterrows():
        vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)

def write_org_files(out_org: str, out_comment: str, table_text: str,
                    metodo_txt: str, interpreta_txt: str, caption: str,
                    extra_images=None):
    """
    extra_images: lista de tuplas (caption_str, path_png)
    """
    lines = []
    lines.append(f"#+CAPTION: {caption}")
    lines.append("")
    lines.append(table_text)
    lines.append("")
    if extra_images:
        for cap, path in extra_images:
            if path and os.path.exists(path):
                lines.append(f"#+CAPTION: {cap}")
                lines.append(f"[[file:{os.path.basename(path)}]]")
                lines.append("")
    lines.append(metodo_txt)
    lines.append("")
    lines.append(interpreta_txt)
    lines.append("")

    with open(out_org, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    with open(out_comment, "w", encoding="utf-8") as f:
        f.write(metodo_txt + "\n\n" + interpreta_txt + "\n")

def _escape_lisp_string(p: str) -> str:
    return p.replace("\\", "\\\\").replace('"', '\\"')

def export_pdf_with_emacs(org_path: str, pdf_path: str,
                          emacs_bin: str = "emacs",
                          engine: str = "xelatex",
                          timeout_sec: int = 120,
                          verbose: bool = False) -> bool:
    # evita travar se Emacs não existe
    if shutil.which(emacs_bin) is None:
        print(f"❌ Emacs não encontrado no PATH (tentado: '{emacs_bin}').")
        return False

    org_dir  = os.path.dirname(os.path.abspath(org_path))
    base_noext = os.path.splitext(os.path.basename(org_path))[0]
    produced_pdf = os.path.join(org_dir, base_noext + ".pdf")

    have_latexmk = shutil.which("latexmk") is not None
    engine = engine.lower()
    if engine not in ("xelatex","lualatex","pdflatex"):
        engine = "xelatex"

    if have_latexmk:
        pdf_process = f"latexmk -{engine} -shell-escape -f -quiet -halt-on-error -interaction=nonstopmode %f"
    else:
        cmd = f"{engine} -shell-escape -halt-on-error -interaction=nonstopmode %f"
        pdf_process = " ".join([cmd, cmd])  # duas passagens

    elisp = f"""
(setq inhibit-startup-message t)
(require 'org)
(require 'ox-latex)
(setq org-latex-compiler "{engine}")
(setq org-latex-pdf-process '("{pdf_process}"))
(let ((default-directory "{_escape_lisp_string(org_dir)}"))
  (find-file "{_escape_lisp_string(os.path.abspath(org_path))}")
  (org-latex-export-to-pdf))
"""
    with tempfile.NamedTemporaryFile("w", suffix=".el", delete=False, encoding="utf-8") as fh:
        fh.write(elisp)
        el_path = fh.name

    env = os.environ.copy()
    env.setdefault("HOME", org_dir)           # evita 1º uso criar ~/.emacs.d
    env.setdefault("LANG", "C.UTF-8")

    try:
        log("Invocando Emacs em modo batch…", verbose)
        proc = subprocess.run(
            [emacs_bin, "--batch", "-Q", "-l", el_path],
            cwd=org_dir,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            env=env,
        )
        if proc.returncode != 0:
            print("❌ Emacs export falhou.")
            if proc.stdout: print("STDOUT:\n", proc.stdout)
            if proc.stderr: print("STDERR:\n", proc.stderr)
            return False
        if not os.path.exists(produced_pdf):
            print("❌ Emacs terminou sem gerar o PDF esperado.")
            if proc.stdout: print("STDOUT:\n", proc.stdout)
            if proc.stderr: print("STDERR:\n", proc.stderr)
            return False
        if os.path.abspath(produced_pdf) != os.path.abspath(pdf_path):
            try: shutil.move(produced_pdf, pdf_path)
            except Exception: shutil.copy2(produced_pdf, pdf_path)
        return os.path.exists(pdf_path)
    except subprocess.TimeoutExpired:
        print(f"❌ Timeout de {timeout_sec}s ao exportar PDF via Emacs.")
        return False
    finally:
        try: os.remove(el_path)
        except Exception: pass


# ───────────────────────── CLI ─────────────────────────

def build_parser():
    p = argparse.ArgumentParser(
        description="Tabela: perito, matrícula, CR, DR e quantidade de tarefas (ini: dia útil; fim: fim de semana)."
    )
    p.add_argument("--db", required=True, help="Caminho do SQLite (.db)")
    p.add_argument("--start", required=True, help="Data inicial YYYY-MM-DD")
    p.add_argument("--end", required=True, help="Data final   YYYY-MM-DD")
    p.add_argument("--perito", help="(opcional) filtro case-insensitive por nome do perito")
    p.add_argument("--out-dir", default=None, help="Diretório de saída (default: graphs_and_tables/exports)")

    p.add_argument("--export-csv", action="store_true", help="Exporta CSV da tabela")
    p.add_argument("--export-org", action="store_true", help="Exporta .org (principal e comentário)")
    p.add_argument("--export-pdf", action="store_true", help="Exporta PDF do .org via Emacs")
    p.add_argument("--export-png", action="store_true", help="Exporta o gráfico por CR em PNG")
    p.add_argument("--export-protocols", action="store_true",
                   help="Gera rcheck_weekday_to_weekend_protocols.org com os protocolos por perito")

    p.add_argument("--emacs-bin", default="emacs", help="Binário do Emacs [default: emacs]")
    p.add_argument("--latex-engine", default="xelatex",
                   choices=["xelatex","lualatex","pdflatex"], help="LaTeX engine [default: xelatex]")
    p.add_argument("--emacs-timeout", type=int, default=120, help="Timeout (s) para exportação no Emacs [default: 120]")
    p.add_argument("--verbose", action="store_true", help="Logs de progresso")
    return p


# ───────────────────────── Vectorizado: parse/derivação ─────────────────────────

def vectorized_finish_times(df: pd.DataFrame, verbose=False) -> pd.DataFrame:
    """Cria ini_dt/fim_dt de forma vetorizada (rápida)."""
    log("Convertendo datas (ini/fim)…", verbose)
    ini = pd.to_datetime(df["ini"], errors="coerce", utc=False)
    fim = pd.to_datetime(df["fim"], errors="coerce", utc=False) if "fim" in df.columns else pd.Series(pd.NaT, index=df.index)

    # Se não há fim, tenta duração numérica/textual (vetorizado)
    need = fim.isna()

    if "dur_num" in df.columns:
        log("Aplicando duração numérica…", verbose)
        sec_num = pd.to_numeric(df["dur_num"], errors="coerce")
        add_num = pd.to_timedelta(sec_num, unit="s")
        fim = fim.where(~need, ini + add_num)

    # Ainda faltou? tenta HH:MM:SS / MM:SS via to_timedelta (super rápido)
    need = fim.isna()
    if "dur_txt" in df.columns and need.any():
        log("Aplicando duração textual (HH:MM:SS/MM:SS)…", verbose)
        text = df["dur_txt"].astype(str)
        mask_colon = text.str.contains(":", regex=False, na=False)
        td = pd.to_timedelta(text.where(mask_colon, None), errors="coerce")
        fim = fim.where(~need, ini + td)

    df = df.copy()
    df["ini_dt"] = ini
    df["fim_dt"] = fim
    return df


# ───────────────────────── Gráfico por CR ─────────────────────────

def render_cr_bar(by_cr: pd.DataFrame, outfile: str, title: str):
    if by_cr.empty:
        return False
    n = len(by_cr)
    width = max(8.0, min(20.0, 0.6 * n + 3.0))
    fig, ax = plt.subplots(figsize=(width, 5), dpi=160)
    xidx = list(range(n))
    ax.bar(xidx, by_cr["Quantidade"], color="#1f77b4", edgecolor="black")
    ax.set_title(title)
    ax.set_ylabel("Quantidade")
    ax.set_xlabel("CR")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    ymax = float(by_cr["Quantidade"].max()) if n else 0.0
    for i, y in enumerate(by_cr["Quantidade"].tolist()):
        ax.text(i, y + max(1.0, 0.02 * ymax), str(int(y)), ha="center", va="bottom", fontsize=9)

    ax.set_xticks(xidx)
    ax.set_xticklabels(by_cr["CR"].tolist(), rotation=45, ha="right")
    plt.tight_layout()
    fig.savefig(outfile, bbox_inches="tight")
    plt.close(fig)
    print(f"✅ PNG salvo: {outfile}")
    return True


# ───────────────────────── Protocolos .org ─────────────────────────

def write_protocols_org(out_path: str, df_sel: pd.DataFrame, start: str, end: str):
    """
    df_sel: subconjunto já filtrado (ini em dia útil e fim no fds) com colunas
            nomePerito e protocolo (se disponível).
    Escreve bullets por perito com a lista de protocolos.
    """
    lines = []
    lines.append("* Protocolos envolvidos (por perito)")
    lines.append(f"_Janela: {start} a {end}. Somente tarefas iniciadas em dia útil e concluídas no fim de semana._\n")

    if "protocolo" not in df_sel.columns:
        lines.append("_Não foi possível identificar a coluna de protocolo nesta base._\n")
        lines.append("- **Total de protocolos:** 0\n")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return

    grupos = (
        df_sel[["nomePerito", "protocolo"]]
        .dropna(subset=["nomePerito", "protocolo"])
        .astype({"nomePerito":"string"})
        .groupby("nomePerito")["protocolo"]
        .apply(lambda s: sorted({str(x) for x in s if pd.notna(x)}))
        .reset_index()
    )

    total = int(sum(len(v) for v in grupos["protocolo"])) if not grupos.empty else 0

    if grupos.empty:
        lines.append("_Não há protocolos no conjunto filtrado._\n")
    else:
        for _, row in grupos.iterrows():
            per = row["nomePerito"]
            protos = row["protocolo"]
            if protos:
                lines.append(f"- *{per}* ({len(protos)}): {', '.join(protos)}")

    lines.append(f"\n- **Total de protocolos:** {total}\n")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ───────────────────────── Main ─────────────────────────

def main():
    args = build_parser().parse_args()

    base_dir = os.path.abspath(os.path.join(os.path.dirname(args.db), ".."))
    export_dir = os.path.abspath(args.out_dir) if args.out_dir else os.path.join(base_dir, "graphs_and_tables", "exports")
    ensure_dir(export_dir)

    stem = "rcheck_weekday_to_weekend_table"
    out_csv = os.path.join(export_dir, f"{stem}.csv")
    out_org = os.path.join(export_dir, f"{stem}.org")
    out_org_comment = os.path.join(export_dir, f"{stem}_comment.org")
    out_pdf = os.path.join(export_dir, f"{stem}.pdf")
    out_cr_png = os.path.join(export_dir, "rcheck_weekday_to_weekend_by_cr.png")
    out_protocols_org = os.path.join(export_dir, "rcheck_weekday_to_weekend_protocols.org")

    con = sqlite3.connect(args.db)
    try:
        a_tbl = detect_analises_table(con)
        if not table_exists(con, "peritos"):
            raise RuntimeError("Tabela 'peritos' não encontrada.")

        # Detectar colunas em analises/peritos
        a_cols = [r[1] for r in con.execute(f"PRAGMA table_info({a_tbl})").fetchall()]
        has_fim = "dataHoraFimPericia" in a_cols or pick_col(a_cols, ["dataHoraFimPericia","data_fim","fimPericia"]) is not None
        dur_num_col, dur_txt_col = pick_duration_columns(a_cols)
        proto_col = pick_protocol_column(a_cols)

        p_cols = [r[1] for r in con.execute("PRAGMA table_info(peritos)").fetchall()]
        nome_col  = pick_col(p_cols, ["nomePerito","nome_perito","nome"]) or "nomePerito"
        matr_col  = pick_col(p_cols, ["siapePerito","matricula","matric","siape"]) or "siapePerito"
        cr_col    = pick_col(p_cols, ["CR","cr","codCR","codigoCR","cr_codigo"])
        dr_col    = pick_col(p_cols, ["DR","dr","codDR","codigoDR","dr_codigo"])

        # Seleção (nome, matr, CR/DR) + ini/fim/duração/protocolo
        fim_col = pick_col(a_cols, ["dataHoraFimPericia","data_fim","fimPericia"])
        ini_col = pick_col(a_cols, ["dataHoraIniPericia","data_inicio","iniPericia"]) or "dataHoraIniPericia"

        sel = [
            f"p.{nome_col} AS nomePerito",
            f"p.{matr_col} AS matricula",
            f"a.{ini_col} AS ini",
        ]
        if cr_col:   sel.append(f"p.{cr_col} AS CR")
        else:        sel.append("NULL AS CR")
        if dr_col:   sel.append(f"p.{dr_col} AS DR")
        else:        sel.append("NULL AS DR")

        if fim_col:       sel.append(f"a.{fim_col} AS fim")
        if dur_num_col:   sel.append(f"CAST(a.{dur_num_col} AS REAL) AS dur_num")
        if dur_txt_col:   sel.append(f"a.{dur_txt_col} AS dur_txt")
        if proto_col:     sel.append(f"a.{proto_col} AS protocolo")

        sql = f"""
            SELECT {", ".join(sel)}
              FROM {a_tbl} a
              JOIN peritos p ON a.siapePerito = p.siapePerito
             WHERE substr(a.{ini_col},1,10) BETWEEN ? AND ?
        """
        params = [args.start, args.end]
        if args.perito:
            sql += " AND LOWER(p.{}) LIKE ?".format(nome_col)
            params.append(f"%{args.perito.lower()}%")

        log("Lendo dados do SQLite…", args.verbose)
        df = pd.read_sql(sql, con, params=params)
    finally:
        con.close()

    if df.empty:
        print("Sem registros no período/critério.")
        # Mesmo assim, se pediu protocolos, escrevemos um org vazio/explicativo
        if args.export_protocols:
            write_protocols_org(out_protocols_org, df, args.start, args.end)
            print(f"✅ Org(protocolos) salvo: {out_protocols_org}")
        if args.export_org:
            metodo_txt = (f"*Método.* Período {args.start} a {args.end}. "
                          "Contamos tarefas com início em dia útil e término no fim de semana. "
                          "Quando não há coluna de término, inferimos via duração (numérica ou HH:MM:SS).")
            interpreta_txt = "*Interpretação.* Nenhum registro encontrado."
            table_text = org_table_from_df(pd.DataFrame(columns=["Perito","Matrícula","CR","DR","Quantidade"]))
            write_org_files(out_org, out_org_comment, table_text, metodo_txt, interpreta_txt,
                            "Tarefas: início em dia útil, fim no fim de semana (por perito)")
            print(f"✅ Org salvo: {out_org}\n✅ Org(comment) salvo: {out_org_comment}")
        return

    # Deriva fim_dt vetorizado e filtra válidos
    df = vectorized_finish_times(df, verbose=args.verbose)
    log("Filtrando intervalos válidos (ini/fim) e calculando flags…", args.verbose)
    df = df[(df["ini_dt"].notna()) & (df["fim_dt"].notna())].copy()

    # Flags de negócio
    ini_wd = df["ini_dt"].dt.weekday  # 0=seg … 6=dom
    fim_wd = df["fim_dt"].dt.weekday
    df["created_business"]   = ini_wd.isin([0,1,2,3,4])
    df["completed_weekend"]  = fim_wd.isin([5,6])

    sel = df[df["created_business"] & df["completed_weekend"]].copy()

    # Se pediu protocolos, gera o .org de protocolos (mesmo se vazio)
    if args.export_protocols:
        try:
            write_protocols_org(out_protocols_org, sel, args.start, args.end)
            print(f"✅ Org(protocolos) salvo: {out_protocols_org}")
        except Exception as e:
            print(f"❌ Falha ao gerar protocolos .org: {e}")

    # Agrupa por perito
    if sel.empty:
        print("Nenhuma tarefa criada em dia útil e concluída no fim de semana.")
        empty = pd.DataFrame(columns=["Perito","Matrícula","CR","DR","Quantidade"])
        if args.export_csv:
            empty.to_csv(out_csv, index=False); print(f"📝 CSV salvo: {out_csv}")

        # Gráfico por CR (não há o que plotar)
        if args.export_org:
            metodo_txt = (f"*Método.* Período {args.start} a {args.end}. "
                          "Início em dia útil (seg–sex) e término no fim de semana (sáb/dom).")
            interpreta_txt = "*Interpretação.* Nenhum registro encontrado."
            table_text = org_table_from_df(empty)
            write_org_files(out_org, out_org_comment, table_text, metodo_txt, interpreta_txt,
                            "Tarefas: início em dia útil, fim no fim de semana (por perito)")
            print(f"✅ Org salvo: {out_org}\n✅ Org(comment) salvo: {out_org_comment}")
            if args.export_pdf:
                ok = export_pdf_with_emacs(out_org, out_pdf, args.emacs_bin, args.latex_engine, args.emacs_timeout, args.verbose)
                print(f"{'✅' if ok else '❌'} PDF gerado: {out_pdf}")
        return

    group_cols = [c for c in ["nomePerito","matricula","CR","DR"] if c in sel.columns]
    agg = (
        sel.groupby(group_cols, dropna=False)
           .size()
           .reset_index(name="Quantidade")
           .sort_values(["Quantidade","nomePerito"], ascending=[False, True])
    )
    agg = agg.rename(columns={"nomePerito":"Perito","matricula":"Matrícula"})

    # Preenche visual
    for c in ["Perito","Matrícula","CR","DR"]:
        if c in agg.columns:
            agg[c] = agg[c].fillna("")

    # ── Agregação e gráfico por CR (ordem decrescente) ──
    cr_png_created = False
    if "CR" in sel.columns:
        cr_series = sel["CR"].fillna("").astype(str).str.strip()
        cr_series = cr_series.replace("", "SEM_CR")
        by_cr_df = (
            cr_series.to_frame("CR")
            .assign(_n=1)
            .groupby("CR", dropna=False)["_n"].sum()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"_n":"Quantidade"})
        )
        if not by_cr_df.empty and (args.export_png or args.export_org or args.export_pdf):
            cr_png_created = render_cr_bar(
                by_cr_df, out_cr_png,
                "Tarefas (início em dia útil, fim no fim de semana) — por CR"
            )

    # Preview perito
    print(agg.to_string(index=False))

    # CSV (tabela por perito)
    if args.export_csv:
        agg.to_csv(out_csv, index=False); print(f"📝 CSV salvo: {out_csv}")

    # ORG (+ PDF) — inclui o gráfico por CR se gerado
    if args.export_org:
        metodo_txt = (
            f"*Método.* Intervalo {args.start} a {args.end}. "
            "Selecionamos tarefas com *início* em dia útil (seg–sex) e *término* no *fim de semana* (sáb/dom). "
            "Quando não há coluna de término, usamos *início + duração* (numérica ou HH:MM:SS) de forma vetorizada."
        )
        interpreta_txt = (
            "*Interpretação.* A contagem por perito e por CR pode indicar encerramentos deslocados para o fim de semana—"
            "observe volumes e contexto antes de qualquer inferência causal."
        )
        ordered_cols = [c for c in ["Perito","Matrícula","CR","DR","Quantidade"] if c in agg.columns]
        table_text = org_table_from_df(agg[ordered_cols])

        extra_imgs = []
        if cr_png_created:
            extra_imgs.append(("Distribuição por CR (ordem decrescente)", out_cr_png))

        write_org_files(out_org, out_org_comment, table_text, metodo_txt, interpreta_txt,
                        "Tarefas: início em dia útil, fim no fim de semana (por perito)",
                        extra_images=extra_imgs)
        print(f"✅ Org salvo: {out_org}\n✅ Org(comment) salvo: {out_org_comment}")

        if args.export_pdf:
            ok = export_pdf_with_emacs(out_org, out_pdf, args.emacs_bin, args.latex_engine, args.emacs_timeout, args.verbose)
            print(f"{'✅' if ok else '❌'} PDF gerado: {out_pdf}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERRO: {e}")
        sys.exit(2)

