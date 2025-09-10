# auto-gerado pelo make_report.py ─ não editar manualmente
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

suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RSQLite))
options(warn = 1)

repos <- getOption("repos"); repos["CRAN"] <- "https://cloud.r-project.org"; options(repos = repos)

# Biblioteca do usuário primeiro, se existir
user_lib <- Sys.getenv("R_LIBS_USER")
if (nzchar(user_lib)) {
  dir.create(user_lib, showWarnings = FALSE, recursive = TRUE)
  .libPaths(unique(c(user_lib, .libPaths())))
}

message("[deps] .libPaths(): ", paste(.libPaths(), collapse = " | "))

need <- c(
  "dplyr","tidyr","readr","stringr","purrr","forcats","lubridate",
  "ggplot2","scales","broom",
  "DBI","RSQLite",
  "ggtext","gridtext","ragg","textshaping",
  "cli","glue","curl","httr"
)

have <- rownames(installed.packages())
to_install <- setdiff(need, have)

ncpus <- 1L
try({ ncpus <- max(1L, parallel::detectCores(logical = TRUE) - 1L) }, silent = TRUE)

if (length(to_install)) {
  message("[deps] Instalando: ", paste(to_install, collapse = ", "))
  tryCatch({
    install.packages(
      to_install,
      dependencies = TRUE, Ncpus = ncpus,
      lib = if (nzchar(user_lib)) user_lib else .libPaths()[1]
    )
  }, error = function(e) {
    message("[deps][ERRO] Falha ao instalar: ", conditionMessage(e))
    quit(status = 1L)
  })
} else {
  message("[deps] Todos os pacotes já presentes.")
}

ok <- TRUE
for (pkg in c("dplyr","ggplot2","DBI","RSQLite")) ok <- ok && requireNamespace(pkg, quietly = TRUE)
ok <- ok && requireNamespace("ggtext", quietly = TRUE) && requireNamespace("ragg", quietly = TRUE)

if (!ok) {
  message("[deps][AVISO] Verifique dependências de sistema (ex.: libcurl, harfbuzz, fribidi, freetype).")
} else {
  message("[deps] OK")
}

message(capture.output(sessionInfo()), sep = "\n")


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
