#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
})

# Operador "a ou b"
`%||%` <- function(a, b) if (!is.null(a) && !is.na(a) && nzchar(a)) a else b

# ── Descobrir diretório do script de forma robusta (Rscript usa --file=) ───────
args_all   <- commandArgs(trailingOnly = FALSE)
file_arg   <- sub("^--file=", "", args_all[grep("^--file=", args_all)])
script_dir <- if (length(file_arg)) dirname(normalizePath(file_arg)) else getwd()

# Candidatos para localizar o _common.R (mesmo dir do script ou pasta r_checks/)
common_candidates <- c(
  file.path(script_dir, "_common.R"),
  file.path(script_dir, "r_checks", "_common.R"),
  file.path(getwd(), "_common.R"),
  file.path(getwd(), "r_checks", "_common.R")
)
common_path <- common_candidates[file.exists(common_candidates)][1]
if (is.na(common_path)) {
  stop("Não encontrei _common.R nas localizações: ",
       paste(common_candidates, collapse = " | "))
}
cat("→ Carregando:", common_path, "\n")
source(common_path, local = TRUE)

# ── Args e DB ──────────────────────────────────────────────────────────────────
args <- am_parse_args()
dbp  <- args$db %||% Sys.getenv("KPI_DB", "db/atestmed.db")

cat("→ Abrindo DB:", dbp, "\n")
con <- am_open_db(dbp)
on.exit(try(am_safe_disconnect(con), silent = TRUE), add = TRUE)

# ── Checks básicos ─────────────────────────────────────────────────────────────
cat("→ SELECT 1...\n")
print(DBI::dbGetQuery(con, "SELECT 1 AS ok"))

cat("→ PRAGMA database_list...\n")
print(DBI::dbGetQuery(con, "PRAGMA database_list"))

cat("→ PRAGMA schema_version...\n")
print(DBI::dbGetQuery(con, "PRAGMA schema_version"))

cat("→ Tabelas disponíveis...\n")
print(DBI::dbListTables(con))

cat("→ Detectando tabela de análises...\n")
tab <- am_detect_analises_table(con)
cat("   tabela:", tab, "\n")

cat("→ Colunas da tabela de análises...\n")
print(am_detect_columns(con, tab))

cat("→ Testando wrappers sem con explícito (via KPI_DB)...\n")
Sys.setenv(KPI_DB = dbp)
print(am_dbGetQuery(NULL, "SELECT 99 AS ok_wrapper"))

cat("✓ Tudo certo.\n")

