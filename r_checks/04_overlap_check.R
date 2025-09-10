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
  library(dplyr); library(ggplot2); library(scales); library(lubridate); library(readr)
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

  # Fallback p/ am_dbGetQuery (precisa existir antes das funções que o usam)
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
      a <- am_normalize_cli(base::commandArgs(TRUE)); kv <- list(); i <- 1L; n <- length(a)
      while (i <= n) { k <- a[[i]]
        if (startsWith(k, "--")) { v <- if (i+1L <= n && !startsWith(a[[i+1L]], "--")) a[[i+1L]] else TRUE
          kv[[sub("^--","",k)]] <- v; i <- i + (if (identical(v, TRUE)) 1L else 2L) } else i <- i + 1L
      }; kv
    }
  }
  if (!exists("am_open_db", mode="function", inherits=TRUE)) {
    am_open_db <<- function(path) { p <- normalizePath(path, mustWork=TRUE); DBI::dbConnect(RSQLite::SQLite(), dbname=p) }
  }
  if (!exists("am_resolve_export_dir", mode="function", inherits=TRUE)) {
    am_resolve_export_dir <<- function(out_dir=NULL) {
      if (!is.null(out_dir) && nzchar(out_dir)) od <- normalizePath(out_dir, mustWork=FALSE) else {
        dbp <- am_args[["db"]] %||% ""; base_dir <- if (nzchar(dbp)) normalizePath(file.path(dirname(dbp), ".."), mustWork=FALSE) else getwd()
        od <- file.path(base_dir, "graphs_and_tables", "exports")
      }
      if (!dir.exists(od)) dir.create(od, recursive=TRUE, showWarnings=FALSE); od
    }
  }
  if (!exists("am_detect_analises_table", mode="function", inherits=TRUE)) {
    am_detect_analises_table <<- function(con) {
      has <- function(nm) nrow(am_dbGetQuery(con, "SELECT 1 FROM sqlite_master WHERE type in ('table','view') AND name=? LIMIT 1", params=list(nm))) > 0
      for (t in c("analises","analises_atestmed")) if (has(t)) return(t); stop("Não encontrei 'analises' nem 'analises_atestmed'.")
    }
  }
  if (!exists("am_detect_columns", mode="function", inherits=TRUE)) {
    am_detect_columns <<- function(con, tbl) { if (is.na(tbl) || !nzchar(tbl)) return(character(0)); am_dbGetQuery(con, sprintf("PRAGMA table_info(%s)", tbl))$name }
  }

  am_args <<- tryCatch(am_parse_args(), error=function(e) list())
  db_path <- am_args[["db"]]; if (is.null(db_path) || !nzchar(db_path)) stop("Faltou --db <path>")
  con <<- am_open_db(db_path)
  # (patch) removido on.exit de desconexão precoce

  export_dir <<- am_resolve_export_dir(am_args[["out-dir"]])
  a_tbl     <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
  am_cols   <<- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))  # <- evita colisão com readr::cols
})
# ==== ATESTMED PROLOGO (FIM) ====


# ----------------------------- CLI --------------------------------------------
opt_list <- list(
  make_option("--db",        type="character"),
  make_option("--start",     type="character"),
  make_option("--end",       type="character"),
  make_option("--perito",    type="character"),
  make_option("--out-dir",   type="character", default=NULL),
  make_option("--scope-csv", type="character", default=NULL, help="CSV com nomes do escopo (Fluxo B)")
)
opt <- optparse::parse_args(OptionParser(option_list = opt_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))

# --------------------------- helpers -------------------------------------------
safe       <- function(x) gsub("(^_+|_+$)", "", gsub("[^A-Za-z0-9_-]+","_", x))
percent_s  <- function(x, acc=.1) ifelse(is.finite(x), percent(x, accuracy=acc), "NA")
load_names <- function(path){
  if (is.null(path) || !nzchar(path) || !file.exists(path)) return(NULL)
  df <- tryCatch(readr::read_csv(path, show_col_types=FALSE), error=function(e) NULL)
  if (is.null(df) || !nrow(df)) return(NULL)
  key <- if ("nomePerito" %in% names(df)) "nomePerito" else names(df)[1]
  out <- trimws(as.character(df[[key]])); out <- out[nzchar(out)]
  if (length(out)) unique(out) else NULL
}

perito_safe <- safe(opt$perito)
base_dir    <- normalizePath(file.path(dirname(opt$db), ".."))
export_dir  <- if (!is.null(opt$`out-dir`)) normalizePath(opt$`out-dir`, mustWork = FALSE) else file.path(base_dir, "graphs_and_tables", "exports")
dir.create(export_dir, showWarnings = FALSE, recursive = TRUE)

# ------------------------------ DB / janela ------------------------------------
con  <- dbConnect(SQLite(), opt$db); on.exit(try(am_safe_disconnect(con), silent=TRUE), add=TRUE)
a_tbl <- if (exists("a_tbl", inherits=TRUE) && nzchar(a_tbl)) a_tbl else "analises"

# Evita usar o nome 'cols' (que é função do readr)
tbl_cols  <- am_dbGetQuery(con, sprintf("PRAGMA table_info(%s)", am_dbQuoteIdentifier(con, a_tbl)))$name
has_ini   <- "dataHoraIniPericia" %in% tbl_cols
has_fim   <- "dataHoraFimPericia" %in% tbl_cols
cand_dur  <- intersect(tbl_cols, c("tempoAnaliseSeg","tempoAnalise","duracaoSegundos","duracao_seg","tempo_seg"))
dur_col   <- if (length(cand_dur)) cand_dur[[1]] else NA_character_
if (!has_ini) stop("Coluna 'dataHoraIniPericia' não encontrada.")

scope <- load_names(opt$`scope-csv`)
if (!is.null(scope)) scope <- unique(c(scope, opt$perito))  # garante perito no escopo
in_clause   <- if (!is.null(scope)) paste(sprintf("'%s'", gsub("'", "''", scope)), collapse=",") else NULL
scope_sql   <- if (is.null(in_clause)) "" else sprintf(" AND p.nomePerito IN (%s) ", in_clause)
scope_label <- if (is.null(in_clause)) "Demais (excl.)" else "Demais (escopo, excl.)"

sel_cols <- c("p.nomePerito AS perito", "a.dataHoraIniPericia AS ini")
if (has_fim) sel_cols <- c(sel_cols, "a.dataHoraFimPericia AS fim")
if (!has_fim && !is.na(dur_col)) sel_cols <- c(sel_cols, sprintf("CAST(a.%s AS REAL) AS dur", dur_col))

qry <- sprintf("
SELECT %s
  FROM %s a
  JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ? %s
", paste(sel_cols, collapse=", "), am_dbQuoteIdentifier(con, a_tbl), scope_sql)

raw <- am_dbGetQuery(con, qry, params = list(opt$start, opt$end))
if (!nrow(raw)) stop("Sem dados no período/escopo.")

raw$ini <- suppressWarnings(lubridate::ymd_hms(raw$ini, quiet=TRUE))
if ("fim" %in% names(raw)) raw$fim <- suppressWarnings(lubridate::ymd_hms(raw$fim, quiet=TRUE))
if (!("fim" %in% names(raw)) && "dur" %in% names(raw)) raw$fim <- raw$ini + dseconds(suppressWarnings(as.numeric(raw$dur)))

df <- raw %>%
  filter(!is.na(ini), !is.na(fim), fim >= ini) %>%
  mutate(dur_s = as.numeric(difftime(fim, ini, units="secs"))) %>%
  filter(is.finite(dur_s), dur_s > 0, dur_s <= 3600)

if (!nrow(df)) {
  gg <- ggplot() + annotate("text", x=0, y=0, label="Sem janelas válidas (0<dur≤3600s).", size=5) + theme_void()
  png_path <- file.path(export_dir, sprintf("rcheck_overlap_%s.png", perito_safe))
  ggsave(png_path, gg, width=8, height=5, dpi=160); quit(save="no")
}

# --------------------------- cálculo -------------------------------------------
has_overlap <- function(dfp) {
  if (nrow(dfp) < 2) return(FALSE)
  dfp <- dfp[order(dfp$ini), , drop = FALSE]
  any(dfp$ini[-1] < dfp$fim[-nrow(dfp)])
}

flag_by_perito <- df %>%
  group_by(perito) %>%
  summarise(overlap = has_overlap(pick(everything())), .groups="drop")

if (!(opt$perito %in% flag_by_perito$perito)) {
  sim <- flag_by_perito %>% filter(grepl(opt$perito, perito, ignore.case=TRUE)) %>% pull(perito)
  msg <- if (length(sim)) paste0(" Peritos semelhantes: ", paste(sim, collapse=", "), ".") else ""
  stop(sprintf("Perito '%s' não encontrado no período/escopo.%s", opt$perito, msg))
}

p_flag   <- flag_by_perito %>% filter(perito == opt$perito) %>% pull(overlap) %>% .[1]
others   <- flag_by_perito %>% filter(perito != opt$perito)
n_others <- nrow(others)
o_rate   <- if (n_others > 0) mean(others$overlap, na.rm=TRUE) else NA_real_

ylim_max <- max(c(as.numeric(p_flag), o_rate), na.rm=TRUE)
if (!is.finite(ylim_max) || ylim_max <= 0) ylim_max <- 0.05
ylim_max <- min(1, ylim_max * 1.15)

plot_df <- tibble::tibble(
  Grupo = factor(c(opt$perito, scope_label), levels=c(opt$perito, scope_label)),
  pct   = c(ifelse(isTRUE(p_flag), 1, 0), o_rate)
)

gg <- ggplot(plot_df, aes(Grupo, pct)) +
  geom_col(fill=c("#ff7f0e", "#1f77b4"), width=.6) +
  geom_text(aes(label=scales::percent(pct, accuracy=.1)), vjust=-.4, size=3.3, na.rm=TRUE) +
  scale_y_continuous(labels=percent_format(accuracy=1), limits=c(0, ylim_max)) +
  labs(
    title = "Sobreposição de tarefas — Perito (indicador) vs Demais",
    subtitle = sprintf("Período: %s a %s  |  peritos (demais) = %d", opt$start, opt$end, n_others),
    y = "Percentual de peritos com ≥1 sobreposição", x = NULL,
    caption = "Janela válida: 0<dur≤3600s. Indicador por perito: 1 se ocorreu ≥1 interseção de intervalos no período."
  ) +
  theme_minimal(base_size=11) +
  theme(panel.grid.major.x = element_blank())

png_path <- file.path(export_dir, sprintf("rcheck_overlap_%s.png", perito_safe))
ggsave(png_path, gg, width=8, height=5, dpi=160); cat(sprintf("✓ salvo: %s\n", png_path))

# ------------------------ comentários em .org ----------------------------------
metodo_txt <- paste0(
  "*Método.* Construímos intervalos [início,fim] por tarefa (fim nativo ou início+duração ",
  "quando não há fim), validamos 0<dur≤3600s, e marcamos *sobreposição* quando algum início ",
  "ocorre antes do fim da tarefa anterior (ordenado por início). O indicador do perito é binário ",
  "(houve/não houve). Para os demais, reportamos a fração de peritos com ocorrência. Se fornecido, ",
  "*--scope-csv* restringe os 'demais' ao *escopo (Fluxo B)*."
)

interpret_txt <- {
  p_str <- if (isTRUE(p_flag)) "houve sobreposição" else "não houve sobreposição"
  o_str <- if (is.finite(o_rate)) sprintf("%s dos demais apresentaram sobreposição", percent_s(o_rate)) else
    "a taxa entre os demais é indeterminada (amostra vazia)"
  paste0("*Interpretação.* Para o perito analisado, ", p_str, ". ", o_str,
         ". Este indicador capta *ocorrência* (≥1 evento), não duração/gravidade.")
}

org_main <- file.path(export_dir, sprintf("rcheck_overlap_%s.org", perito_safe))
writeLines(paste(
  "#+CAPTION: Sobreposição de tarefas — indicador de ocorrência",
  sprintf("[[file:%s]]", basename(png_path)), "", metodo_txt, "", interpret_txt, "", sep="\n"
), org_main); cat(sprintf("✓ org: %s\n", org_main))

org_comment <- file.path(export_dir, sprintf("rcheck_overlap_%s_comment.org", perito_safe))
writeLines(paste(metodo_txt, "", interpret_txt, "", sep="\n"), org_comment); cat(sprintf("✓ org(comment): %s\n", org_comment))
