#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(dplyr)
})

# ──────────────────────────────────────────────────────────────────────────────
# Helpers de conexão (unificados)
# ──────────────────────────────────────────────────────────────────────────────

# Operador "a ou b"
`%||%` <- function(a, b) if (!is.null(a) && !is.na(a) && nzchar(a)) a else b

# Fecha conexão com segurança
if (!exists("am_safe_disconnect", mode="function", inherits=TRUE)) {
  am_safe_disconnect <- function(con) {
    try({
      if (inherits(con, "DBIConnection") && DBI::dbIsValid(con)) DBI::dbDisconnect(con)
    }, silent=TRUE)
  }
}

# Tenta adivinhar o caminho do DB a partir de estruturas comuns (args globais)
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

# Normalizador de caminho tolerante
am_norm <- function(p, mustWork = FALSE) {
  if (is.null(p) || is.na(p) || !nzchar(p)) return(p)
  tryCatch(normalizePath(p, winslash = "/", mustWork = mustWork), error = function(e) p)
}

# Conector único: prioridade --db > guess > env KPI_DB
if (!exists("am_db_connect", mode="function", inherits=TRUE)) {
  am_db_connect <- function(db_arg = NULL) {
    dbp <- db_arg %||% tryCatch(am__db_path_guess(), error=function(e) NULL) %||% Sys.getenv("KPI_DB", "")
    if (!nzchar(dbp)) stop("Caminho do banco não informado. Use --db ou defina KPI_DB.", call. = FALSE)
    dbp <- am_norm(dbp, mustWork = TRUE)
    DBI::dbConnect(RSQLite::SQLite(), dbname = dbp)
  }
}

# Garante conexão válida; se não houver/expirou, reconecta via am_db_connect()
if (!exists("am_ensure_con", mode="function", inherits=TRUE)) {
  am_ensure_con <- function(con = NULL, db_arg = NULL) {
    if (inherits(con, "DBIConnection") && DBI::dbIsValid(con)) return(con)
    am_db_connect(db_arg)
  }
}

# Wrappers que sempre validam/reconectam
if (!exists("am_dbGetQuery", mode="function", inherits=TRUE)) {
  am_dbGetQuery <- function(con = NULL, ...) { con <- am_ensure_con(con); DBI::dbGetQuery(con, ...) }
}
if (!exists("am_dbReadTable", mode="function", inherits=TRUE)) {
  am_dbReadTable <- function(con = NULL, ...) { con <- am_ensure_con(con); DBI::dbReadTable(con, ...) }
}
if (!exists("am_dbListFields", mode="function", inherits=TRUE)) {
  am_dbListFields <- function(con = NULL, ...) { con <- am_ensure_con(con); DBI::dbListFields(con, ...) }
}

# dbQuoteIdentifier sem recursão, com ensure_con
if (!exists("am_dbQuoteIdentifier", mode="function", inherits=TRUE)) {
  am_dbQuoteIdentifier <<- (function(.f){
    force(.f)
    function(con = NULL, ...) {
      con <- am_ensure_con(con)
      as.character(.f(con, ...))
    }
  })(DBI::dbQuoteIdentifier)
}

# ──────────────────────────────────────────────────────────────────────────────
# Utilidades gerais
# ──────────────────────────────────────────────────────────────────────────────

am_log <- function(...) {
  cat(sprintf(paste0("[common] ", paste(rep("%s", length(list(...))), collapse=""), "\n"), ...))
}

am_parse_args <- function() {
  args <- base::commandArgs(TRUE)
  kv <- list(); i <- 1L
  while (i <= length(args)) {
    k <- args[[i]]
    if (startsWith(k, "--")) {
      v <- if (i + 1L <= length(args) && !startsWith(args[[i+1]], "--")) args[[i+1]] else TRUE
      kv[[sub("^--", "", k)]] <- v
      i <- i + if (identical(v, TRUE)) 1L else 2L
    } else i <- i + 1L
  }
  kv
}

# Abre conexão explicitamente (mas quem chama deve fechar com on.exit)
am_open_db <- function(db_path = NULL) {
  con <- am_db_connect(db_path)
  ok <- tryCatch(DBI::dbIsValid(con), error = function(e) FALSE)
  if (!isTRUE(ok)) stop("am_open_db: conexão inválida")
  # Sanity check
  tryCatch(DBI::dbGetQuery(con, "SELECT 1"),
           error = function(e) stop("am_open_db: SELECT 1 falhou: ", conditionMessage(e)))
  con
}

am_table_exists <- function(con = NULL, name) {
  q <- "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ? LIMIT 1"
  nrow(am_dbGetQuery(con, q, params = list(name))) > 0
}

am_detect_analises_table <- function(con = NULL) {
  for (t in c("analises", "analises_atestmed")) if (am_table_exists(con, t)) return(t)
  stop("am_detect_analises_table: não encontrei tabela de análises.")
}

am_detect_columns <- function(con = NULL, tbl) {
  am_dbGetQuery(con, sprintf("PRAGMA table_info(%s)", tbl))$name
}

am_resolve_export_dir <- function(out_dir_arg) {
  if (!is.null(out_dir_arg) && nzchar(out_dir_arg)) {
    od <- am_norm(out_dir_arg, mustWork = FALSE)
  } else {
    od <- file.path(getwd(), "graphs_and_tables", "exports")
  }
  if (!dir.exists(od)) dir.create(od, recursive = TRUE, showWarnings = FALSE)
  am_norm(od, mustWork = FALSE)
}

am_slice_head_min3 <- function(.data) {
  k <- min(3L, nrow(.data))
  if (inherits(.data, "grouped_df")) dplyr::filter(.data, dplyr::row_number() <= k) else utils::head(.data, k)
}

am_slice_head_n <- function(.data, k = 3L) {
  k <- as.integer(k[1]); if (!is.finite(k) || k < 0L) k <- 0L
  if (inherits(.data, "grouped_df")) dplyr::filter(.data, dplyr::row_number() <= k) else utils::head(.data, k)
}

# ──────────────────────────────────────────────────────────────────────────────
# Exemplo de uso (comentado):
# opt <- am_parse_args()
# con <- am_open_db(opt$db); on.exit(try(am_safe_disconnect(con), silent=TRUE), add=TRUE)
# tbl <- am_detect_analises_table(con)
# cols <- am_detect_columns(con, tbl)
# am_log("Tabela detectada: ", tbl, " | Colunas: ", paste(cols, collapse=", "))
# ──────────────────────────────────────────────────────────────────────────────

