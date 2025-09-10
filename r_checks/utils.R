# reports/r_checks/utils.R
# ---- Auto-normalized header (injected by run_r_checks_autofix.py) ----
options(stringsAsFactors = FALSE)
options(encoding = "UTF-8")
options(warn = 1)
options(repos = c(CRAN = "https://cloud.r-project.org"))
Sys.setlocale(category = "LC_ALL", locale = "C.UTF-8")
# ----------------------------------------------------------------------
suppressWarnings(suppressMessages({
  if (!requireNamespace("optparse", quietly = TRUE)) install.packages("optparse")
  library(optparse)



# --- hardening: garanta am_resolve_export_dir mesmo sem _common.R ---
if (!exists("am_resolve_export_dir", mode = "function", inherits = TRUE)) {
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
  library(dplyr)
  library(readr)
  library(stringr)
  library(ggplot2)suppressPackageStartupMessages(library(DBI))

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


suppressPackageStartupMessages(library(RSQLite))

}))

# ---------- parser comum ----------
common_option_list <- list(
  make_option("--start", type="character"),
  make_option("--end",   type="character"),
  make_option("--perito", type="character", default=NULL),
  make_option("--top10", action="store_true", default=FALSE),

  # flags de export
  make_option("--export-png", action="store_true", default=FALSE),
  make_option("--export-md",  action="store_true", default=FALSE),

  # manifests para alinhar com fluxo A/B
  make_option("--peritos-csv", type="character", default=NULL,
              help="Arquivo CSV (coluna nomePerito) com a lista do Top10 selecionada no Python."),
  make_option("--scope-csv", type="character", default=NULL,
              help="CSV com nomes em escopo (gate do fluxo B)."),

  # parâmetros específicos de cada check (ficam aqui para herdar em todos)
  make_option("--min-analises", type="integer", default=50),
  make_option("--alpha", type="double", default=0.8)
)

read_common_opts <- function(extra_options = list()) {
  parser <- OptionParser(option_list = c(common_option_list, extra_options))
  optparse::parse_args(parser)
}

# ---------- I/O helpers ----------
export_dir <- function() {
  d <- Sys.getenv("EXPORT_DIR", unset = "exports")
  if (!dir.exists(d)) dir.create(d, recursive = TRUE, showWarnings = FALSE)
  normalizePath(d, mustWork = FALSE)
}

imgs_dir <- function() {
  d <- file.path(export_dir(), "imgs")
  if (!dir.exists(d)) dir.create(d, recursive = TRUE, showWarnings = FALSE)
  normalizePath(d, mustWork = FALSE)
}

# ---------- leitura/aplicação de escopo ----------
load_names_from_csv <- function(path) {
  if (is.null(path) || is.na(path) || !nzchar(path) || !file.exists(path)) return(NULL)
  tryCatch({
    df <- suppressWarnings(readr::read_csv(path, show_col_types = FALSE))
    nm <- df[[1]]
    nm <- nm[!is.na(nm)]
    nm <- unique(trimws(as.character(nm)))
    if (length(nm) == 0) NULL else nm
  }, error = function(e) NULL)
}

apply_scope <- function(df_all, scope_csv = NULL) {
  scope <- load_names_from_csv(scope_csv)
  if (is.null(scope)) return(df_all)
  df_all %>% dplyr::filter(.data$nomePerito %in% scope)
}

apply_peritos_filter <- function(df_all, peritos_csv = NULL) {
  top <- load_names_from_csv(peritos_csv)
  if (is.null(top)) return(df_all)
  df_all %>% dplyr::filter(.data$nomePerito %in% top)
}

# ---------- nome seguro ----------
safe_perito <- function(x) {
  x <- gsub("[^[:alnum:]]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  ifelse(nzchar(x), x, "perito")
}

# ---------- salvar png ----------
save_png <- function(gg, filename, width=8, height=5, dpi=300) {
  path <- file.path(imgs_dir(), filename)
  ggplot2::ggsave(path, gg, width = width, height = height, dpi = dpi, limitsize = FALSE)
  path
}
