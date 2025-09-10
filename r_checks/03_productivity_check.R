#!/usr/bin/env Rscript
# ---- Auto-normalized header (injected by run_r_checks_autofix.py) ----
options(stringsAsFactors = FALSE)
options(encoding = "UTF-8")
options(warn = 1)
options(repos = c(CRAN = "https://cloud.r-project.org"))
Sys.setlocale(category = "LC_ALL", locale = "C.UTF-8")
# ----------------------------------------------------------------------

suppressPackageStartupMessages({

# --- hardening: garanta am_resolve_export_dir mesmo sem _common.R ---
if (!exists("am_resolve_export_dir", mode = "function", inherits = TRUE)

) {
  `%||%` <- function(a,b) if (is.null(a)) b else a
  am_resolve_export_dir <- function(out_dir = NULL) {
    od <- if (!is.null(out_dir) && nzchar(out_dir)) {
      normalizePath(out_dir, mustWork = FALSE)
    } else {
      dbp <- tryCatch(am_args[["db"]], error = function(e) NULL) %||% ""
      base_dir <- if (nzchar(dbp)) normalizePath(file.path(dirname(dbp), ".."), mustWork = FALSE) else getwd()
      file.path(base_dir, "graphs_and_tables", "exports")
    }
    if (!dir.exists(od)) dir.create(od, recursive = TRUE, showWarnings = FALSE)
    od
  }
}
  library(optparse); library(DBI); library(RSQLite)
  library(dplyr); library(ggplot2); library(scales); library(stringr)
})

# --- begin: am_db_reconnect_helpers ---
if (!exists("am_safe_disconnect", mode="function", inherits=TRUE)) {
  am_safe_disconnect <- function(con) {
    try({
      if (inherits(con, "DBIConnection") && DBI::dbIsValid(con)) DBI::dbDisconnect(con)
    }, silent=TRUE)
  }
}
if (!exists("am__db_path_guess", mode="function", inherits=TRUE)) {
  am__db_path_guess <- function() {
    tryCatch({
      if (exists("am_args", inherits=TRUE)) {
        p <- tryCatch(am_args[["db"]], error=function(e) NULL)
        if (!is.null(p) && nzchar(p)) return(p)
      }
      if (exists("opt", inherits=TRUE)) {
        p <- tryCatch(opt$db, error=function(e) NULL)
        if (!is.null(p) && nzchar(p)) return(p)
      }
      NULL
    }, error=function(e) NULL)
  }
}
if (!exists("am_ensure_con", mode="function", inherits=TRUE)) {
  am_ensure_con <- function(con) {
    if (inherits(con, "DBIConnection") && DBI::dbIsValid(con)) return(con)
    dbp <- am__db_path_guess()
    if (is.null(dbp) || !nzchar(dbp)) stop("am_ensure_con: caminho do DB desconhecido")
    DBI::dbConnect(RSQLite::SQLite(), dbname = dbp)
  }
}
if (!exists("am_dbGetQuery", mode="function", inherits=TRUE)) {
  am_dbGetQuery <- function(con, ...) { con <- am_ensure_con(con); DBI::dbGetQuery(con, ...) }
}
if (!exists("am_dbReadTable", mode="function", inherits=TRUE)) {
  am_dbReadTable <- function(con, ...) { con <- am_ensure_con(con); DBI::dbReadTable(con, ...) }
}
if (!exists("am_dbListFields", mode="function", inherits=TRUE)) {
  am_dbListFields <- function(con, ...) { con <- am_ensure_con(con); DBI::dbListFields(con, ...) }
}
# --- fix: am_dbQuoteIdentifier sem recursão ---
if (!exists("am_dbQuoteIdentifier", mode="function", inherits=TRUE)) {
  am_dbQuoteIdentifier <<- (function(.f){
    force(.f)
    function(con, ...) {
      con <- am_ensure_con(con)
      as.character(.f(con, ...))
    }
  })(DBI::dbQuoteIdentifier)
}
# --- fim do fix ---

# --- end: am_db_reconnect_helpers ---







# ==== ATESTMED PROLOGO (INICIO) ====
local({
  .am_loaded <- FALSE
  for (pp in c("r_checks/_common.R","./_common.R","../r_checks/_common.R")) {
    if (file.exists(pp)) { source(pp, local=TRUE); .am_loaded <- TRUE; break }
  }
  if (!.am_loaded) message("[prolog] _common.R não encontrado — usando fallbacks internos.")
  `%||%` <- function(a,b) if (is.null(a)) b else a

  # Fallback p/ am_dbGetQuery (aceita vetor de strings)
  if (!exists("am_dbGetQuery", mode="function", inherits=TRUE)) {
    am_dbGetQuery <<- (function(.f){
      force(.f)
      function(con, statement, ...) {
        st <- if (length(statement)!=1L) paste(statement, collapse=" ") else statement
        .f(con, st, ...)
      }
    })(DBI::dbGetQuery)
  }

  if (!exists("am_normalize_cli", mode="function", inherits=TRUE)) am_normalize_cli <<- function(x) as.character(x)
  if (!exists("am_parse_args", mode="function", inherits=TRUE)) {
    am_parse_args <<- function() {
      a <- am_normalize_cli(base::commandArgs(TRUE))
      kv <- list(); i <- 1L; n <- length(a)
      while (i <= n) {
        k <- a[[i]]
        if (startsWith(k, "--")) {
          v <- if (i+1L <= n && !startsWith(a[[i+1L]], "--")) a[[i+1L]] else TRUE
          kv[[sub("^--","",k)]] <- v
          i <- i + (if (identical(v, TRUE)) 1L else 2L)
        } else i <- i + 1L
      }
      kv
    }
  }
  if (!exists("am_open_db", mode="function", inherits=TRUE)) {
    am_open_db <<- function(path) { p <- normalizePath(path, mustWork=TRUE); DBI::dbConnect(RSQLite::SQLite(), dbname=p) }
  }
  if (!exists("am_resolve_export_dir", mode="function", inherits=TRUE)) {
    am_resolve_export_dir <<- function(out_dir=NULL) {
      if (!is.null(out_dir) && nzchar(out_dir)) od <- normalizePath(out_dir, mustWork=FALSE) else {
        dbp <- am_args[["db"]] %||% ""
        base_dir <- if (nzchar(dbp)) normalizePath(file.path(dirname(dbp), ".."), mustWork=FALSE) else getwd()
        od <- file.path(base_dir, "graphs_and_tables", "exports")
      }
      if (!dir.exists(od)) dir.create(od, recursive=TRUE, showWarnings=FALSE); od
    }
  }
  if (!exists("am_detect_analises_table", mode="function", inherits=TRUE)) {
    am_detect_analises_table <<- function(con) {
      has <- function(nm) nrow(am_dbGetQuery(con,
        "SELECT 1 FROM sqlite_master WHERE type in ('table','view') AND name=? LIMIT 1",
        params=list(nm))) > 0
      for (t in c("analises","analises_atestmed")) if (has(t)) return(t)
      stop("Não encontrei 'analises' nem 'analises_atestmed'.")
    }
  }
  if (!exists("am_detect_columns", mode="function", inherits=TRUE)) {
    am_detect_columns <<- function(con, tbl) {
      if (is.na(tbl) || !nzchar(tbl)) return(character(0))
      am_dbGetQuery(con, sprintf("PRAGMA table_info(%s)", tbl))$name
    }
  }

  .raw <- NULL
  if (exists("args", inherits=TRUE)) { .cand <- get("args", inherits=TRUE); if (!is.function(.cand)) .raw <- .cand }
  .kv <- tryCatch(am_parse_args(), error=function(e) list())
  if (is.character(.raw)) {
    .kv2 <- list(); i <- 1L; n <- length(.raw)
    while (i <= n) {
      k <- .raw[[i]]
      if (startsWith(k, "--")) {
        v <- if (i+1L <= n && !startsWith(.raw[[i+1L]], "--")) .raw[[i+1L]] else TRUE
        .kv2[[sub("^--","",k)]] <- v
        i <- i + (if (identical(v, TRUE)) 1L else 2L)
      } else i <- i + 1L
    }
    if (length(.kv2)) .kv <- utils::modifyList(.kv, .kv2)
  } else if (is.environment(.raw)) .kv <- utils::modifyList(.kv, as.list(.raw))
  else if (is.list(.raw))          .kv <- utils::modifyList(.kv, .raw)
  am_args <<- .kv

  db_path <- am_args[["db"]]
  if (is.null(db_path) || !nzchar(db_path)) stop("Faltou --db <path>")
  con <<- am_open_db(db_path)
  # (patch) removido on.exit de desconexão precoce

  export_dir <<- am_resolve_export_dir(am_args[["out-dir"]])
  a_tbl     <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
  am_cols   <<- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))  # (renomeado)
})
# ==== ATESTMED PROLOGO (FIM) ====


# ----------------------------- CLI --------------------------------------------
opt_list <- list(
  make_option("--db",          type="character"),
  make_option("--start",       type="character"),
  make_option("--end",         type="character"),
  make_option("--perito",      type="character"),
  make_option("--threshold",   type="double", default=50),
  make_option("--out-dir",     type="character", default=NULL, help="Diretório de saída"),
  make_option("--scope-csv",   type="character", default=NULL, help="CSV com nomes (escopo Fluxo B)")
)
opt <- optparse::parse_args(OptionParser(option_list = opt_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))

# --------------------------- helpers -------------------------------------------
safe      <- function(x) { gsub("(^_+|_+$)", "", gsub("[^A-Za-z0-9_-]+","_", x)) }
percent_s <- function(x) ifelse(is.finite(x), percent(x, accuracy = .1), "NA")
num_s     <- function(x, d=2) format(round(x, d), big.mark=".", decimal.mark=",", nsmall=d, trim=TRUE)

perito_safe <- safe(opt$perito)

base_dir   <- normalizePath(file.path(dirname(opt$db), ".."))
export_dir <- if (!is.null(opt$`out-dir`)) normalizePath(opt$`out-dir`, mustWork = FALSE) else
              file.path(base_dir, "graphs_and_tables", "exports")
dir.create(export_dir, showWarnings = FALSE, recursive = TRUE)

load_names_csv <- function(path) {
  if (is.null(path) || !nzchar(path) || !file.exists(path)) return(NULL)
  df <- tryCatch(read.csv(path, stringsAsFactors=FALSE, fileEncoding="UTF-8-BOM"), error=function(e) NULL)
  if (is.null(df) || !nrow(df)) return(NULL)
  key <- if ("nomePerito" %in% names(df)) "nomePerito" else names(df)[1]
  out <- trimws(as.character(df[[key]])); out <- out[nzchar(out)]
  if (length(out)) unique(out) else NULL
}

# ------------------------------ DB / duração -----------------------------------
con <- dbConnect(RSQLite::SQLite(), opt$db)
on.exit(try(am_safe_disconnect(con), silent=TRUE), add=TRUE)

# tabela de análises
a_tbl <- if (exists("a_tbl", inherits=TRUE) && nzchar(a_tbl)) a_tbl else {
  tabs <- am_dbGetQuery(con, "SELECT name FROM sqlite_master WHERE type IN ('table','view')")
  hit  <- intersect(c("analises","analises_atestmed"), tabs$name)
  if (!length(hit)) stop("Não encontrei tabela de análises."); hit[[1]]
}

# coluna de duração ou fallback
cols <- am_dbGetQuery(con, sprintf("PRAGMA table_info(%s)", am_dbQuoteIdentifier(con, a_tbl)))$name
cand <- intersect(cols, c("tempoAnaliseSeg","tempoAnalise","duracaoSegundos","duracao_seg","tempo_seg"))
dur_col <- if (length(cand)) cand[[1]] else NA_character_

scope_names <- load_names_csv(opt$`scope-csv`)
if (!is.null(scope_names)) scope_names <- unique(c(scope_names, opt$perito))  # garante perito
in_clause <- if (!is.null(scope_names) && length(scope_names)) {
  paste(sprintf("'%s'", gsub("'", "''", scope_names)), collapse=",")
} else NULL
scope_sql <- if (is.null(in_clause)) "" else sprintf(" AND p.nomePerito IN (%s) ", in_clause)
scope_label <- if (is.null(in_clause)) "distribuição nacional" else "distribuição (escopo)"

if (is.na(dur_col)) {
  qry <- sprintf("
    SELECT p.nomePerito AS perito,
           ((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400.0) AS dur
      FROM %s a
      JOIN peritos p ON a.siapePerito = p.siapePerito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ? %s
       AND a.dataHoraIniPericia IS NOT NULL
       AND a.dataHoraFimPericia   IS NOT NULL
  ", am_dbQuoteIdentifier(con, a_tbl), scope_sql)
  raw <- am_dbGetQuery(con, qry, params = list(opt$start, opt$end))
} else {
  qry <- sprintf("
    SELECT p.nomePerito AS perito, CAST(a.%s AS REAL) AS dur
      FROM %s a
      JOIN peritos p ON a.siapePerito = p.siapePerito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ? %s
  ", dur_col, am_dbQuoteIdentifier(con, a_tbl), scope_sql)
  raw <- am_dbGetQuery(con, qry, params = list(opt$start, opt$end))
}
stopifnot(nrow(raw) > 0)

df <- raw %>%
  mutate(dur = suppressWarnings(as.numeric(dur))) %>%
  group_by(perito) %>%
  summarise(
    n_valid = sum(is.finite(dur) & dur > 0 & dur <= 3600, na.rm=TRUE),
    segs    = sum(ifelse(is.finite(dur) & dur > 0 & dur <= 3600, dur, 0), na.rm=TRUE),
    .groups = "drop"
  ) %>%
  mutate(prod = ifelse(segs > 0, n_valid / (segs/3600), NA_real_))

if (!(opt$perito %in% df$perito)) {
  sim <- df %>% filter(grepl(opt$perito, perito, ignore.case = TRUE)) %>% pull(perito)
  msg <- if (length(sim)) paste0(" Peritos semelhantes: ", paste(sim, collapse=", "), ".") else ""
  stop(sprintf("Perito '%s' não encontrado no período/escopo.%s", opt$perito, msg))
}

p_val <- df %>% filter(perito==opt$perito) %>% pull(prod) %>% .[1]
vals  <- df %>% filter(is.finite(prod)) %>% pull(prod)
n_eff <- length(vals)
if (n_eff == 0) {
  gg <- ggplot() + annotate("text", x=0, y=0, label="Sem produtividades válidas no escopo", size=5) + theme_void()
  png_path <- file.path(export_dir, sprintf("rcheck_productivity_%s.png", perito_safe))
  ggsave(png_path, gg, width=8, height=5, dpi=160); quit(save="no")
}

pctile         <- if (is.finite(p_val)) mean(vals <= p_val) else NA_real_
share_above_thr<- mean(vals >= opt$threshold)

# ----------------------------- gráfico -----------------------------------------
gg <- ggplot(data.frame(prod = vals), aes(prod)) +
  geom_histogram(bins=40, fill="#1f77b4", alpha=.85) +
  geom_vline(xintercept = p_val,          color="#d62728",  linewidth=1, na.rm = TRUE) +
  geom_vline(xintercept = opt$threshold,  color="#2ca02c", linetype="dashed") +
  labs(
    title    = sprintf("Produtividade (análises/h) — %s", scope_label),
    subtitle = sprintf("%s a %s | perito=%s (%s/h) | threshold=%.0f/h",
                       opt$start, opt$end, opt$perito,
                       ifelse(is.finite(p_val), num_s(p_val,2), "NA"), opt$threshold),
    x="análises/h", y="freq.",
    caption = "Produtividade = perícias válidas ÷ horas válidas; duração válida: 0 < dur ≤ 3600s."
  ) +
  theme_minimal(base_size=11)

png_path <- file.path(export_dir, sprintf("rcheck_productivity_%s.png", perito_safe))
ggsave(png_path, gg, width=8, height=5, dpi=160)
cat(sprintf("✓ salvo: %s\n", png_path))

# ------------------------ comentários em .org ----------------------------------
metodo_txt <- sprintf(
  paste0(
    "*Método.* Calculamos *produtividade* = total de perícias *válidas* ÷ horas *válidas* ",
    "(soma das durações; duração válida: 0<dur≤3600s) para cada perito no período %s a %s. ",
    "A duração vem de coluna dedicada (se existir) ou de (fim−início)*86400. ",
    "Construímos a %s e destacamos o perito alvo (linha vermelha). ",
    "Marcamos também o *limiar* de %.0f análises/h (linha tracejada) e reportamos o *percentil empírico*."
  ),
  opt$start, opt$end, scope_label, opt$threshold
)

interpret_txt <- {
  dir_txt <- if (is.finite(p_val)) {
    if      (p_val >  opt$threshold) "acima do limiar"
    else if (p_val <  opt$threshold) "abaixo do limiar"
    else                             "no limiar"
  } else "indeterminado (sem tempo total válido)"
  pct_txt   <- if (is.finite(pctile)) sprintf("percentil ≈ %s", percent_s(pctile)) else "percentil indisponível"
  share_txt <- sprintf("%s do grupo ≥ limiar", percent_s(share_above_thr))
  sprintf("*Interpretação.* A produtividade do perito é %s (≈ %s/h). %s. %s.",
          dir_txt, ifelse(is.finite(p_val), num_s(p_val,2), "NA"), pct_txt, share_txt)
}

org_main <- file.path(export_dir, sprintf("rcheck_productivity_%s.org", perito_safe))
org_main_txt <- paste(
  "#+CAPTION: Produtividade — distribuição com destaque do perito",
  sprintf("[[file:%s]]", basename(png_path)),
  "", metodo_txt, "", interpret_txt, "", sep = "\n"
)
writeLines(org_main_txt, org_main); cat(sprintf("✓ org: %s\n", org_main))

org_comment <- file.path(export_dir, sprintf("rcheck_productivity_%s_comment.org", perito_safe))
org_comment_txt <- paste(metodo_txt, "", interpret_txt, "", sep = "\n")
writeLines(org_comment_txt, org_comment); cat(sprintf("✓ org(comment): %s\n", org_comment))
