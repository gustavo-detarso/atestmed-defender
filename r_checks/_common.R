#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(DBI); library(RSQLite)
  library(dplyr)
})

am_log <- function(...) cat(sprintf(paste0("[common] ", paste(rep("%s", length(list(...))), collapse=""), "\n"), ...))
am_norm <- function(p, mustWork = FALSE) {
  if (is.null(p) || is.na(p) || !nzchar(p)) return(p)
  tryCatch(normalizePath(p, winslash = "/", mustWork = mustWork), error = function(e) p)
}

am_parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  kv <- list(); i <- 1L
  while (i <= length(args)) {
    k <- args[[i]]
    if (startsWith(k, "--")) {
      v <- if (i + 1L <= length(args) && !startsWith(args[[i+1L]], "--")) args[[i+1L]] else TRUE
      kv[[sub("^--", "", k)]] <- v
      i <- i + if (identical(v, TRUE)) 1L else 2L
    } else i <- i + 1L
  }
  kv
}

am_open_db <- function(db_path) {
  if (is.null(db_path) || !nzchar(db_path)) stop("am_open_db: db_path vazio")
  dbp <- am_norm(db_path)
  con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbp)
  ok <- tryCatch(DBI::dbIsValid(con), error = function(e) FALSE)
  if (!isTRUE(ok)) stop("am_open_db: conexão inválida")
  tryCatch(DBI::dbGetQuery(con, "SELECT 1"), error = function(e) stop("am_open_db: SELECT 1 falhou: ", conditionMessage(e)))
  con
}

am_table_exists <- function(con, name) {
  q <- "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ? LIMIT 1"
  nrow(DBI::dbGetQuery(con, q, params = list(name))) > 0
}

am_detect_analises_table <- function(con) {
  for (t in c("analises", "analises_atestmed")) if (am_table_exists(con, t)) return(t)
  stop("am_detect_analises_table: não encontrei tabela de análises.")
}

am_detect_columns <- function(con, tbl) {
  DBI::dbGetQuery(con, sprintf("PRAGMA table_info(%s)", tbl))$name
}

am_resolve_export_dir <- function(out_dir_arg) {
  if (!is.null(out_dir_arg) && nzchar(out_dir_arg)) od <- am_norm(out_dir_arg, mustWork = FALSE) else
    od <- file.path(getwd(), "graphs_and_tables", "exports")
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

