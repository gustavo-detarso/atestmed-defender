#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, re, sys, shutil
from pathlib import Path

PROLOG_CLOSE = "# ==== ATESTMED PROLOGO (FIM) ===="
CLI_MARK     = "# ---- CLI fallback (ATESTMED) ----"
FIX_MARK     = "# ---- Pequenos fixes automáticos (ATESTMED) ----"

CLI_BLOCK = f"""{CLI_MARK}
if (!exists("start_d", inherits=TRUE) ||
    !exists("end_d", inherits=TRUE) ||
    !exists("top10", inherits=TRUE) ||
    !exists("perito", inherits=TRUE) ||
    !exists("min_n", inherits=TRUE) ||
    !exists("threshold", inherits=TRUE) ||
    !exists("measure", inherits=TRUE) ||
    !exists("export_dir", inherits=TRUE) ||
    !exists("con", inherits=TRUE) ||
    !exists("a_tbl", inherits=TRUE)) {{

  .args <- commandArgs(trailingOnly=TRUE)
  .kv <- list(); i <- 1L; n <- length(.args)
  while (i <= n) {{
    k <- .args[[i]]
    if (startsWith(k, "--")) {{
      v <- if (i+1L <= n && !startsWith(.args[[i+1L]], "--")) .args[[i+1L]] else TRUE
      .kv[[sub("^--","",k)]] <- v
      i <- i + (if (identical(v, TRUE)) 1L else 2L)
    }} else i <- i + 1L
  }}

  `%||%` <- function(a,b) if (is.null(a)) b else a

  start_d   <- if (!exists("start_d", inherits=TRUE))   .kv[["start"]]   else start_d
  end_d     <- if (!exists("end_d", inherits=TRUE))     .kv[["end"]]     else end_d
  perito    <- if (!exists("perito", inherits=TRUE))    (.kv[["perito"]] %||% NULL) else perito
  top10     <- if (!exists("top10", inherits=TRUE))     isTRUE(.kv[["top10"]]) else top10
  min_n     <- if (!exists("min_n", inherits=TRUE))     suppressWarnings(as.integer(.kv[["min-analises"]] %||% .kv[["min_analises"]] %||% "50")) else min_n
  threshold <- if (!exists("threshold", inherits=TRUE)) suppressWarnings(as.numeric(.kv[["threshold"]] %||% "15")) else threshold
  measure   <- if (!exists("measure", inherits=TRUE))   as.character((.kv[["measure"]] %||% "nc")) else measure
  if (is.character(measure)) measure <- measure[[1L]]

  if (!exists("export_dir", inherits=TRUE) || is.null(export_dir)) {{
    db_path <- .kv[["db"]]
    if (is.null(db_path) || !nzchar(db_path)) stop("Faltou --db <path> (CLI fallback).")
    base_dir <- normalizePath(file.path(dirname(db_path), ".."), mustWork=FALSE)
    out_dir  <- .kv[["out-dir"]] %||% .kv[["out"]]
    export_dir <- if (!is.null(out_dir) && nzchar(out_dir)) normalizePath(out_dir, mustWork=FALSE) else file.path(base_dir, "graphs_and_tables","exports")
    if (!dir.exists(export_dir)) dir.create(export_dir, recursive=TRUE, showWarnings=FALSE)

    con <- tryCatch(DBI::dbConnect(RSQLite::SQLite(), dbname=normalizePath(db_path, mustWork=TRUE)), error=function(e) NULL)
    if (is.null(con) || !DBI::dbIsValid(con)) stop("Conexão DB inválida (fallback).")

    if (!exists("am_detect_analises_table", mode="function", inherits=TRUE)) {{
      am_detect_analises_table <<- function(con) {{
        has <- function(nm) {{
          out <- tryCatch(DBI::dbGetQuery(con,
            "SELECT 1 FROM sqlite_master WHERE type in ('table','view') AND name=? LIMIT 1",
            params=list(nm)), error=function(e) NULL)
          !is.null(out) && is.data.frame(out) && nrow(out) > 0
        }}
        for (t in c("analises","analises_atestmed")) if (has(t)) return(t)
        stop("Não encontrei 'analises' nem 'analises_atestmed'.")
      }}
    }}
    if (!exists("am_detect_columns", mode="function", inherits=TRUE)) {{
      am_detect_columns <<- function(con, tbl) {{
        if (is.na(tbl) || !nzchar(tbl)) return(character(0))
        DBI::dbGetQuery(con, sprintf("PRAGMA table_info(%s)", tbl))$name
      }}
    }}

    a_tbl <- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
    cols  <- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))

    on.exit({{
      try({{ if (exists("con", inherits=TRUE)) DBI::dbDisconnect(con) }}, silent=TRUE)
      try({{ conns <- DBI::dbListConnections(RSQLite::SQLite()); for (cc in conns) try(DBI::dbDisconnect(cc), silent=TRUE) }}, silent=TRUE)
    }}, add=TRUE)
  }}
}}
"""

# corrige 'orgc <- file.file <- file.path(' -> 'orgc <- file.path('
RE_FIX_FILEFILE = re.compile(r"(\borgc\s*<-\s*)file\.file\s*<-\s*(file\.path\()", re.IGNORECASE)

def insert_cli_after_prologue(src: str) -> tuple[str, bool]:
    if CLI_MARK in src:
        return src, False
    idx = src.find(PROLOG_CLOSE)
    if idx == -1:
        # sem prólogo? insere no topo
        new_src = CLI_BLOCK + "\n\n" + src
        return new_src, True
    # pula a linha do marcador e quebra de linha seguinte
    line_end = src.find("\n", idx)
    pos = len(src) if line_end == -1 else line_end + 1
    # insere bloco com uma linha em branco antes, se necessário
    prefix = "" if (pos > 0 and src[pos-1] == "\n") else "\n"
    new_src = src[:pos] + prefix + CLI_BLOCK + "\n" + src[pos:]
    return new_src, True

def small_fixes(src: str) -> tuple[str, bool]:
    changed = False
    if RE_FIX_FILEFILE.search(src):
        src = RE_FIX_FILEFILE.sub(r"\1\2", src)
        changed = True
    if FIX_MARK not in src and changed:
        # apenas para marcar que aplicamos um fix
        src = src.replace("# ---------------- Execução ----------------",
                          FIX_MARK + "\n# ---------------- Execução ----------------")
    return src, changed

def patch_file(path: Path) -> bool:
    txt = path.read_text(encoding="utf-8", errors="ignore")
    changed_any = False

    # 1) insere CLI fallback se necessário
    txt2, changed = insert_cli_after_prologue(txt)
    if changed:
        changed_any = True
        txt = txt2

    # 2) pequenos fixes (file.file)
    txt2, changed = small_fixes(txt)
    if changed:
        changed_any = True
        txt = txt2

    if changed_any:
        bak = path.with_suffix(path.suffix + ".bak")
        if not bak.exists():
            shutil.copy2(path, bak)
        path.write_text(txt, encoding="utf-8")
    return changed_any

def resolve_target(args) -> Path:
    if args.file:
        return args.file
    base = args.dir or Path("r_checks")
    cand = base / "08_weighted_props.R"
    if cand.exists():
        return cand
    matches = list(base.glob("**/08_weighted_props*.R"))
    if not matches:
        print("❌ Não achei 08_weighted_props.R. Use --file r_checks/08_weighted_props.R ou --dir r_checks.", file=sys.stderr)
        sys.exit(2)
    return matches[0]

def main():
    ap = argparse.ArgumentParser(description="Patch específico do 08_weighted_props.R (injeção de CLI fallback + pequenos fixes).")
    ap.add_argument("--file", type=Path, default=None, help="Caminho do 08_weighted_props.R")
    ap.add_argument("--dir", type=Path, default=None, help="Pasta onde está o 08 (default: r_checks)")
    args = ap.parse_args()

    target = resolve_target(args)
    try:
        changed = patch_file(target)
    except Exception as e:
        print(f"❌ Falha ao aplicar patch em {target}:\n  {e}", file=sys.stderr)
        sys.exit(1)

    print(("✓ patched: " if changed else "= ok     : ") + str(target))
    if changed:
        print(f"  • Backup: {target.with_suffix(target.suffix + '.bak')}")

if __name__ == "__main__":
    main()

