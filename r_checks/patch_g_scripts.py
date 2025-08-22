#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, re, shutil
from pathlib import Path

PROLOG_MARK_OPEN  = "# ==== ATESTMED PROLOGO (INICIO) ===="
PROLOG_MARK_CLOSE = "# ==== ATESTMED PROLOGO (FIM) ===="

PROLOGUE = f"""{PROLOG_MARK_OPEN}
local({{
  .am_loaded <- FALSE
  for (pp in c("r_checks/_common.R","./_common.R","../r_checks/_common.R")) {{
    if (file.exists(pp)) {{ source(pp, local=TRUE); .am_loaded <- TRUE; break }}
  }}
  if (!.am_loaded) message("[prolog] _common.R não encontrado — usando fallbacks internos.")

  `%||%` <- function(a,b) if (is.null(a)) b else a

  # ---- Fallbacks essenciais (se _common.R não definiu) ----
  if (!exists("am_normalize_cli", mode="function", inherits=TRUE)) {{
    am_normalize_cli <<- function(x) as.character(x)
  }}
  if (!exists("am_parse_args", mode="function", inherits=TRUE)) {{
    am_parse_args <<- function() {{
      a <- am_normalize_cli(commandArgs(trailingOnly=TRUE))
      kv <- list(); i <- 1L; n <- length(a)
      while (i <= n) {{
        k <- a[[i]]
        if (startsWith(k, "--")) {{
          v <- if (i+1L <= n && !startsWith(a[[i+1L]], "--")) a[[i+1L]] else TRUE
          kv[[sub("^--","",k)]] <- v
          i <- i + (if (identical(v, TRUE)) 1L else 2L)
        }} else i <- i + 1L
      }}
      kv
    }}
  }}
  if (!exists("am_open_db", mode="function", inherits=TRUE)) {{
    am_open_db <<- function(path) {{
      p <- normalizePath(path, mustWork=TRUE)
      DBI::dbConnect(RSQLite::SQLite(), dbname=p)
    }}
  }}
  if (!exists("am_resolve_export_dir", mode="function", inherits=TRUE)) {{
    am_resolve_export_dir <<- function(out_dir=NULL) {{
      if (!is.null(out_dir) && nzchar(out_dir)) {{
        od <- normalizePath(out_dir, mustWork=FALSE)
      }} else {{
        dbp <- am_args[["db"]] %||% ""
        base_dir <- if (nzchar(dbp)) normalizePath(file.path(dirname(dbp), ".."), mustWork=FALSE) else getwd()
        od <- file.path(base_dir, "graphs_and_tables", "exports")
      }}
      if (!dir.exists(od)) dir.create(od, recursive=TRUE, showWarnings=FALSE)
      od
    }}
  }}
  if (!exists("am_detect_analises_table", mode="function", inherits=TRUE)) {{
    am_detect_analises_table <<- function(con) {{
      has <- function(nm) {{
        nrow(DBI::dbGetQuery(con,
          "SELECT 1 FROM sqlite_master WHERE type in ('table','view') AND name=? LIMIT 1",
          params=list(nm))) > 0
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

  # 1) args → lista nomeada (sem rebind de objetos bloqueados)
  .raw <- NULL
  if (exists("args", inherits=TRUE)) {{
    .cand <- get("args", inherits=TRUE)
    if (!is.function(.cand)) .raw <- .cand
  }}
  .kv <- tryCatch(am_parse_args(), error=function(e) list())
  if (is.character(.raw)) {{
    .kv2 <- list(); i <- 1L; n <- length(.raw)
    while (i <= n) {{
      k <- .raw[[i]]
      if (startsWith(k, "--")) {{
        v <- if (i+1L <= n && !startsWith(.raw[[i+1L]], "--")) .raw[[i+1L]] else TRUE
        .kv2[[sub("^--","",k)]] <- v
        i <- i + (if (identical(v, TRUE)) 1L else 2L)
      }} else i <- i + 1L
    }}
    if (length(.kv2)) .kv <- utils::modifyList(.kv, .kv2)
  }} else if (is.environment(.raw)) {{
    .kv <- utils::modifyList(.kv, as.list(.raw))
  }} else if (is.list(.raw)) {{
    .kv <- utils::modifyList(.kv, .raw)
  }}
  am_args <<- .kv

  # 2) Conexão ao DB
  db_path <- am_args[["db"]]
  if (is.null(db_path) || !nzchar(db_path)) stop("Faltou --db <path>")
  con <<- am_open_db(db_path)

  # Fecha TODAS as conexões SQLite ao sair (remove avisos)
  on.exit({{
    try({{
      if (exists("con", inherits=TRUE)) try(DBI::dbDisconnect(con), silent=TRUE)
      conns <- try(DBI::dbListConnections(RSQLite::SQLite()), silent=TRUE)
      if (!inherits(conns, "try-error")) for (cc in conns) try(DBI::dbDisconnect(cc), silent=TRUE)
    }}, silent=TRUE)
  }}, add=TRUE)

  # 3) Paths e schema
  export_dir <<- am_resolve_export_dir(am_args[["out-dir"]])
  a_tbl <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
  cols  <<- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))

  # 4) Args derivados
  start_d   <<- am_args[["start"]]
  end_d     <<- am_args[["end"]]
  min_n     <<- suppressWarnings(as.integer(am_args[["min-analises"]]))
  threshold <<- suppressWarnings(as.numeric(am_args[["threshold"]]))
  measure   <<- as.character(am_args[["measure"]] %||% NA_character_); if (!is.na(measure)) measure <<- measure[[1L]]
  top10     <<- isTRUE(am_args[["top10"]])
  perito    <<- as.character(am_args[["perito"]] %||% NA_character_); if (!is.na(perito)) perito <<- perito[[1L]]

  # 5) Wrapper seguro para consultas (evita "Expected string vector of length 1")
  am_dbGetQuery <<- (function(.f) {{
    force(.f)
    function(con, statement, ...) {{
      st <- statement
      if (length(st) != 1L) st <- paste(st, collapse=" ")
      .f(con, st, ...)
    }}
  }})(DBI::dbGetQuery)
}})
{PROLOG_MARK_CLOSE}
"""

# ───────────── regexes ─────────────
# Troca "slice_head(n = min(3, n()))" (ou variantes) por "dplyr::slice_head(n = 3)"
SLICE_PATTERNS = [
    r"slice_head\s*\(\s*n\s*=\s*min\s*\(\s*3L?\s*,\s*n\s*\(\s*\)\s*\)\s*\)",
    r"dplyr::slice_head\s*\(\s*n\s*=\s*min\s*\(\s*3L?\s*,\s*(?:dplyr::)?n\s*\(\s*\)\s*\)\s*\)",
]
SLICE_REGEXES = [re.compile(p) for p in SLICE_PATTERNS]
SLICE_REPLACEMENT = "dplyr::slice_head(n = 3)"

# Também converte qualquer am_slice_head_min3() legado
AM_SLICE_RE = re.compile(r"\bam_slice_head_min3\s*\(\s*\)")

# comenta dbDisconnect(con) (qualificado ou não)
DBDISC_RE = re.compile(r"(?:^|\s)(?:DBI::|RSQLite::)?dbDisconnect\s*\(\s*con\s*\)\s*;?\s*$")

# cur_data() → pick(dplyr::everything())
CUR_DATA_BARE_RE = re.compile(r"\bcur_data\s*\(\s*\)", re.DOTALL)
CUR_DATA_NS_RE   = re.compile(r"\bdplyr::cur_data\s*\(\s*\)", re.DOTALL)

# força uso do wrapper de query:
DBIQ_NS_RE   = re.compile(r"\bDBI::dbGetQuery\s*\(")
DBIQ_BARE_RE = re.compile(r"(?<![A-Za-z0-9_:])dbGetQuery\s*\(")  # evita capturar am_dbGetQuery(

def upsert_prologue(txt: str) -> str:
    if PROLOG_MARK_OPEN in txt and PROLOG_MARK_CLOSE in txt:
        pattern = re.compile(re.escape(PROLOG_MARK_OPEN) + r".*?" + re.escape(PROLOG_MARK_CLOSE), flags=re.S)
        return pattern.sub(PROLOGUE, txt, count=1)
    m = re.search(r"suppressPackageStartupMessages\s*\(\s*\{.*?\}\s*\)", txt, re.S)
    if m:
        end = m.end()
        return txt[:end] + "\n\n" + PROLOGUE + "\n" + txt[end:]
    m2 = re.search(r"(?:^|\n)(?:library\([^\)]*\).*\n)+", txt)
    if m2:
        end = m2.end()
        return txt[:end] + "\n" + PROLOGUE + "\n" + txt[end:]
    return PROLOGUE + "\n" + txt

def replace_slice_head(txt: str) -> str:
    new = txt
    for rgx in SLICE_REGEXES:
        new = rgx.sub(SLICE_REPLACEMENT, new)
    new = AM_SLICE_RE.sub(SLICE_REPLACEMENT, new)
    return new

def comment_dbdisconnect(txt: str) -> str:
    out = []
    for ln in txt.splitlines():
        out.append("# (patched) " + ln if DBDISC_RE.search(ln) else ln)
    if not txt.endswith("\n"): out.append("")
    return "\n".join(out)

def replace_cur_data(txt: str) -> str:
    new = CUR_DATA_NS_RE.sub("pick(dplyr::everything())", txt)
    new = CUR_DATA_BARE_RE.sub("pick(dplyr::everything())", new)
    return new

def route_dbgetquery_to_wrapper(txt: str) -> str:
    new = DBIQ_NS_RE.sub("am_dbGetQuery(", txt)
    new = DBIQ_BARE_RE.sub("am_dbGetQuery(", new)
    return new

def process_file(path: Path) -> bool:
    src = path.read_text(encoding="utf-8", errors="ignore")
    changed = False

    new = upsert_prologue(src)
    if new != src: src, changed = new, True

    new = replace_slice_head(src)
    if new != src: src, changed = new, True

    new = replace_cur_data(src)
    if new != src: src, changed = new, True

    new = route_dbgetquery_to_wrapper(src)
    if new != src: src, changed = new, True

    new = comment_dbdisconnect(src)
    if new != src: src, changed = new, True

    if changed:
        backup = path.with_suffix(path.suffix + ".bak")
        if not backup.exists():
            shutil.copy2(path, backup)
        path.write_text(src, encoding="utf-8")
    return changed

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="r_checks")
    args = ap.parse_args()
    root = Path(args.dir)
    if not root.exists():
        print(f"❌ Pasta não encontrada: {root}"); raise SystemExit(2)
    total = touched = 0
    for p in sorted(root.glob("*.R")):
        if p.name in {"_common.R"} or p.name.startswith("__wrap_"): continue
        total += 1
        changed = process_file(p)
        print(("✓ patched: " if changed else "= ok     : ") + str(p))
        if changed: touched += 1
    print(f"\nResumo: {touched}/{total} alterado(s). Backups *.bak criados quando necessário.")

if __name__ == "__main__":
    main()

