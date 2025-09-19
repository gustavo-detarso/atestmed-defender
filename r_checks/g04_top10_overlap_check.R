#!/usr/bin/env Rscript
# ---- Auto-normalized header (injected by run_r_checks_autofix.py) ----
options(stringsAsFactors = FALSE)
options(encoding = "UTF-8")
options(warn = 1)
options(repos = c(CRAN = "https://cloud.r-project.org"))
Sys.setlocale(category = "LC_ALL", locale = "C.UTF-8")
# ----------------------------------------------------------------------

suppressPackageStartupMessages({
# ── Localizar e carregar _common.R de forma robusta ────────────────────────────
args_all   <- commandArgs(trailingOnly = FALSE)
file_arg   <- sub("^--file=", "", args_all[grep("^--file=", args_all)])
script_dir <- if (length(file_arg)) dirname(normalizePath(file_arg)) else getwd()
common_candidates <- c(
  file.path(script_dir, "_common.R"),
  file.path(script_dir, "r_checks", "_common.R"),
  file.path(getwd(), "_common.R"),
  file.path(getwd(), "r_checks", "_common.R")
)
common_path <- common_candidates[file.exists(common_candidates)][1]
if (!is.na(common_path)) {
  source(common_path, local = TRUE)
} else {
  message("[g04_top10_overlap_check] _common.R não encontrado — usando fallbacks internos.")
}

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
  library(DBI); library(RSQLite)
  library(ggplot2); library(dplyr); library(lubridate); library(scales); library(readr)
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
  if (!exists("am_dbGetQuery", mode="function", inherits=TRUE)) {
    am_dbGetQuery <<- (function(.f){ force(.f); function(con, statement, ...) {
      st <- statement; if (length(st)!=1L) st <- paste(st, collapse=" "); .f(con, st, ...)
    } })(DBI::dbGetQuery)
  }

  .kv <- tryCatch(am_parse_args(), error=function(e) list()); am_args <<- .kv
  db_path <- am_args[["db"]]; if (is.null(db_path) || !nzchar(db_path)) stop("Faltou --db <path>")
  con <<- am_open_db(db_path)
  # (patch) removido on.exit de desconexão precoce
  export_dir <<- am_resolve_export_dir(am_args[["out-dir"]])
  a_tbl     <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
  cols <- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))
})
# ==== ATESTMED PROLOGO (FIM) ====


`%||%` <- function(a,b) if (is.null(a)) b else a
to_upper  <- function(xs) unique(toupper(trimws(xs)))

parse_args <- function() {
  a <- am_normalize_cli(base::commandArgs(TRUE)); kv <- list(); i <- 1L; n <- length(a)
  while (i <= n) { k <- a[[i]]
    if (startsWith(k, "--")) { v <- if (i+1L <= n && !startsWith(a[[i+1L]], "--")) a[[i+1L]] else TRUE
      kv[[substr(k, 3, nchar(k))]] <- v; i <- i + (if (isTRUE(v) || identical(v, TRUE)) 1 else 2) } else i <- i + 1
  }; kv
}
ensure_dir <- function(p) if (!dir.exists(p)) dir.create(p, recursive=TRUE, showWarnings=FALSE)
fail_plot  <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()

# ───────────────────────── Args/paths ─────────────────────────
args <- parse_args()
db_path     <- args$db
start_d     <- args$start
end_d       <- args$end
min_n       <- as.integer(args[["min-analises"]] %||% "50")
out_dir     <- args[["out-dir"]]
peritos_csv <- args[["peritos-csv"]] %||% NULL
scope_csv   <- args[["scope-csv"]]   %||% NULL
flow_opt    <- toupper(args[["flow"]] %||% "")         # "A" ou "B"
rank_by_opt <- tolower(args[["rank-by"]] %||% "")      # "scorefinal" ou "harm"
rank_by     <- if (nzchar(rank_by_opt)) rank_by_opt else if (identical(flow_opt, "B")) "harm" else "scorefinal"

if (is.null(db_path) || is.null(start_d) || is.null(end_d)) {
  stop("Uso: --db <path> --start YYYY-MM-DD --end YYYY-MM-DD [--min-analises 50] [--peritos-csv <csv>] [--scope-csv <csv>] [--flow A|B] [--rank-by scoreFinal|harm] [--out-dir <dir>]")
}

base_dir   <- normalizePath(file.path(dirname(db_path), ".."), mustWork=FALSE)
export_dir <- if (!is.null(out_dir)) normalizePath(out_dir, mustWork=FALSE) else file.path(base_dir, "graphs_and_tables", "exports")
ensure_dir(export_dir)

png_file <- file.path(export_dir, "rcheck_top10_overlap.png")
org_main <- file.path(export_dir, "rcheck_top10_overlap.org")
org_comm <- file.path(export_dir, "rcheck_top10_overlap_comment.org")

# ─────────────── Helpers de schema ───────────────
con <- am_open_db(db_path)
on.exit(try(am_safe_disconnect(con), silent=TRUE), add=TRUE)
on.exit(try(am_safe_disconnect(con), silent=TRUE), add=TRUE)
table_exists <- function(con, name) nrow(am_dbGetQuery(con, "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1", params=list(name))) > 0
detect_analises_table <- function(con) { for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t); stop("Não encontrei 'analises' nem 'analises_atestmed'.") }

a_tbl <- detect_analises_table(con)

# 'indicadores' é necessário apenas quando NÃO houver manifesto
need_indicadores <- is.null(peritos_csv)
if (need_indicadores && !table_exists(con, "indicadores")) {
  ggsave(png_file, fail_plot("Tabela 'indicadores' não encontrada (e sem --peritos-csv)."), width=9, height=5, dpi=150); quit(save="no")
}

# Checa coluna da métrica quando seleção interna
if (need_indicadores) {
  ind_cols <- am_dbGetQuery(con, "PRAGMA table_info(indicadores)")$name
  need_col <- if (rank_by == "harm") "harm" else "scoreFinal"
  if (!(need_col %in% ind_cols)) {
    ggsave(png_file, fail_plot(sprintf("Coluna '%s' ausente em 'indicadores'.", need_col)), width=9, height=5, dpi=150); quit(save="no")
  }
}

# ─────────────── Leitura de CSVs auxiliares ───────────────
load_names <- function(path){
  if (is.null(path) || !nzchar(path) || !file.exists(path)) return(NULL)
  df <- tryCatch(readr::read_csv(path, show_col_types=FALSE), error=function(e) NULL)
  if (is.null(df) || !nrow(df)) return(NULL)
  key <- if ("nomePerito" %in% names(df)) "nomePerito" else names(df)[1]
  out <- trimws(as.character(df[[key]])); out <- out[nzchar(out)]
  if (length(out)) unique(out) else NULL
}

# ─────────────── ESCOPO (coorte) ───────────────
scope_names  <- to_upper(load_names(scope_csv))
scope_clause <- ""
scope_params <- list()
if (length(scope_names)) {
  placeholders_scope <- paste(rep("?", length(scope_names)), collapse=",")
  scope_clause <- sprintf(" AND TRIM(UPPER(p.nomePerito)) IN (%s) ", placeholders_scope)
  scope_params <- as.list(scope_names)
}

# ─────────────── Seleção dos peritos ───────────────
sel_caption <- NULL
top10 <- NULL
names_manifest <- load_names(peritos_csv)

if (!is.null(names_manifest)) {
  # Revalida elegibilidade (n ≥ min_n) preservando a ordem do CSV (aplica ESCOPO)
  in_list <- paste(sprintf("'%s'", gsub("'", "''", names_manifest)), collapse=",")
  qry_cnt <- sprintf("
    SELECT p.nomePerito AS nomePerito, COUNT(a.protocolo) AS total_analises
      FROM %s a
      JOIN peritos p ON a.siapePerito = p.siapePerito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
       AND p.nomePerito IN (%s) %s
     GROUP BY p.nomePerito
  ", am_dbQuoteIdentifier(con, a_tbl), in_list, scope_clause)
  cnt  <- do.call(am_dbGetQuery, c(list(con, qry_cnt), list(params = c(list(start_d, end_d), scope_params))))
  elig <- cnt %>% filter(total_analises >= min_n) %>%
    arrange(match(nomePerito, names_manifest)) %>% pull(nomePerito)
  top10 <- tibble::tibble(nomePerito = head(elig, 10L))
  if (!nrow(top10)) { ggsave(png_file, fail_plot("Manifesto sem peritos elegíveis (após escopo/min-analises)."), width=9, height=5, dpi=150); quit(save="no") }
  sel_caption <- sprintf("Seleção: manifesto (revalidado: n ≥ %d%s).", min_n, if (length(scope_names)) "; escopo aplicado" else "")
} else {
  # Seleção interna — Fluxo A/B (aplica ESCOPO)
  order_col <- if (rank_by == "harm") "i.harm" else "i.scoreFinal"
  qry_top10 <- sprintf("
    SELECT p.nomePerito, %s AS rank_metric, COUNT(a.protocolo) AS total_analises
      FROM indicadores i
      JOIN peritos   p ON i.perito = p.siapePerito
      JOIN %s        a ON a.siapePerito = i.perito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ? %s
     GROUP BY p.nomePerito, rank_metric
    HAVING total_analises >= ?
     ORDER BY rank_metric DESC, total_analises DESC
     LIMIT 10
  ", order_col, am_dbQuoteIdentifier(con, a_tbl), scope_clause)
  params_top10 <- c(list(start_d, end_d), scope_params, list(min_n))
  top10 <- do.call(am_dbGetQuery, c(list(con, qry_top10), list(params = params_top10)))
  if (!nrow(top10)) { ggsave(png_file, fail_plot("Sem Top 10 para o período/critério (após escopo)."), width=9, height=5, dpi=150); quit(save="no") }
  sel_caption <- sprintf("Seleção: %s (n ≥ %d%s).",
                         ifelse(rank_by=="harm", "harm (Fluxo B)", "scoreFinal (Fluxo A)"),
                         min_n, if (length(scope_names)) "; escopo aplicado" else "")
}

peritos_in <- paste(sprintf("'%s'", gsub("'", "''", top10$nomePerito)), collapse=",")

# ─────────────── Carrega janelas por perito ───────────────
cols <- am_dbGetQuery(con, sprintf("PRAGMA table_info(%s)", am_dbQuoteIdentifier(con, a_tbl)))$name
has_ini <- "dataHoraIniPericia" %in% cols
has_fim <- "dataHoraFimPericia" %in% cols
cand_dur<- intersect(cols, c("tempoAnaliseSeg","tempoAnalise","duracaoSegundos","duracao_seg","tempo_seg"))
dur_col <- if (length(cand_dur)) cand_dur[[1]] else NA_character_
if (!has_ini) { ggsave(png_file, fail_plot("Coluna 'dataHoraIniPericia' não encontrada."), width=9, height=5, dpi=150); quit(save="no") }

sel_cols <- c("p.nomePerito AS nomePerito", "a.dataHoraIniPericia AS ini")
if (has_fim) sel_cols <- c(sel_cols, "a.dataHoraFimPericia AS fim")
if (!has_fim && !is.na(dur_col)) sel_cols <- c(sel_cols, sprintf("CAST(a.%s AS REAL) AS dur", dur_col))

qry <- sprintf("
SELECT %s
  FROM %s a
  JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
   AND p.nomePerito IN (%s) %s
", paste(sel_cols, collapse=", "), am_dbQuoteIdentifier(con, a_tbl), peritos_in, scope_clause)

df <- do.call(am_dbGetQuery, c(list(con, qry), list(params = c(list(start_d, end_d), scope_params))))
if (!nrow(df)) { ggsave(png_file, fail_plot("Sem timestamps para sobreposição."), width=9, height=5, dpi=150); quit(save="no") }

df$ini <- suppressWarnings(lubridate::ymd_hms(df$ini, quiet=TRUE))
if ("fim" %in% names(df)) df$fim <- suppressWarnings(lubridate::ymd_hms(df$fim, quiet=TRUE))
if (!("fim" %in% names(df)) && "dur" %in% names(df)) df$fim <- df$ini + lubridate::dseconds(suppressWarnings(as.numeric(df$dur)))

df <- df %>%
  filter(!is.na(ini), !is.na(fim), fim >= ini) %>%
  mutate(dur_s = as.numeric(difftime(fim, ini, units="secs"))) %>%
  filter(is.finite(dur_s), dur_s > 0, dur_s <= 3600)

if (!nrow(df)) { ggsave(png_file, fail_plot("Todas as janelas inválidas (0<dur≤3600s)."), width=9, height=5, dpi=150); quit(save="no") }

# ─────────────── Cálculo de share de sobreposição ───────────────
overlap_share <- function(tb) {
  tb <- tb %>% arrange(ini, fim); n <- nrow(tb); if (n <= 1) return(0)
  overl <- logical(n); last_end <- tb$fim[1]
  for (i in 2:n) { overl[i] <- tb$ini[i] < last_end; last_end <- max(last_end, tb$fim[i], na.rm=TRUE) }
  mean(overl, na.rm=TRUE) * 100
}

res <- df %>%
  group_by(nomePerito) %>%
  group_modify(~tibble(pct_overlap = overlap_share(.x), total = nrow(.x))) %>%
  ungroup() %>%
  arrange(desc(pct_overlap)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito))

# ─────────────── Plot ───────────────
p <- ggplot(res, aes(x = nomePerito, y = pct_overlap)) +
  geom_col(fill="#1f77b4") +
  geom_text(aes(label = sprintf("%.1f%% (n=%d)", pct_overlap, total)), vjust=-0.3, size=3) +
  labs(
    title    = "Top 10 — Tarefas sobrepostas (%)",
    subtitle = sprintf("%s a %s | %s", start_d, end_d, sel_caption),
    x = "Perito", y = "% sobrepostas",
    caption  = "Janela válida: 0<dur≤3600s. Fim nativo quando existe; senão, início+duração (s)."
  ) +
  theme_minimal(base_size=11) +
  theme(axis.text.x = element_text(angle=45, hjust=1))

ymax <- suppressWarnings(max(res$pct_overlap, na.rm=TRUE)); if (!is.finite(ymax)) ymax <- 1
ggsave(png_file, p + coord_cartesian(ylim=c(0, ymax*1.15)), width=9, height=5, dpi=150); base::cat(sprintf("✅ Figura salva: %s\n", png_file))

# ─────────────── Comentários (.org) ───────────────
top3 <- res %>% slice_head(n=3) %>% transmute(txt = sprintf("%s: %.1f%% (n=%d)", as.character(nomePerito), pct_overlap, total))
rng_pct <- range(res$pct_overlap, na.rm=TRUE); media <- mean(res$pct_overlap, na.rm=TRUE); mediana <- median(res$pct_overlap, na.rm=TRUE)

escopo_txt <- if (length(scope_names)) {
  paste0("\n*Escopo.* Coorte com ", length(scope_names), " peritos; primeiros: ",
         paste(head(scope_names, 5), collapse = ", "),
         if (length(scope_names) > 5) ", …" else "", ".")
} else ""

metodo_txt <- paste0(
  "*Método.* Seleção de peritos: ", sel_caption, " ",
  "Construímos intervalos [início,fim] por tarefa e calculamos o *percentual de tarefas sobrepostas* ",
  "como a fração de tarefas cujo início ocorre antes do fim acumulado anterior (ordenado por início). ",
  "Consideramos *válidas* janelas com 0<dur≤3600s; quando não há coluna de fim, usamos início+duração.", escopo_txt
)

interpreta_txt <- paste0(
  "*Interpretação.* Entre os selecionados, a sobreposição variou de ",
  sprintf("%.1f%%", min(rng_pct)), " a ", sprintf("%.1f%%", max(rng_pct)),
  "; média=", sprintf("%.1f%%", media), ", mediana=", sprintf("%.1f%%", mediana), ".\n",
  if (nrow(top3)) paste0("- Maiores shares: ", paste(top3$txt, collapse="; "), ".\n") else "",
  "Valores altos podem indicar *concorrência/simultaneidade*; avaliar volume (n) e contexto."
)

org_main_txt <- paste(
  "#+CAPTION: Top 10 — Tarefas sobrepostas (%)",
  sprintf("[[file:%s]]", basename(png_file)),
  "", metodo_txt, "", interpreta_txt, "", sep="\n"
)
writeLines(org_main_txt, org_main); base::cat(sprintf("✅ Org salvo: %s\n", org_main))

org_comm_txt <- paste(metodo_txt, "", interpreta_txt, "", sep="\n")
writeLines(org_comm_txt, org_comm); base::cat(sprintf("✅ Org(comment) salvo: %s\n", org_comm))
