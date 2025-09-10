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
  library(DBI)
  library(RSQLite)
  library(ggplot2)
  library(dplyr)
  library(stringr)
  library(scales)
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
  cols <- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))

  start_d   <<- am_args[["start"]]
  end_d     <<- am_args[["end"]]
  min_n     <<- suppressWarnings(as.integer(am_args[["min-analises"]]))
  threshold <<- suppressWarnings(as.numeric(am_args[["threshold"]]))
  measure   <<- as.character(am_args[["measure"]] %||% NA_character_); if (!is.na(measure)) measure <<- measure[[1L]]
  top10     <<- isTRUE(am_args[["top10"]])
  perito    <<- as.character(am_args[["perito"]] %||% NA_character_); if (!is.na(perito)) perito <<- perito[[1L]]

  am_dbGetQuery <<- (function(.f) { force(.f); function(con, statement, ...) {
    st <- statement; if (length(st) != 1L) st <- paste(st, collapse=" "); .f(con, st, ...) } })(DBI::dbGetQuery)
})
# ==== ATESTMED PROLOGO (FIM) ====


`%||%` <- function(a,b) if (is.null(a)) b else a

parse_args <- function() {
  args <- am_normalize_cli(base::commandArgs(TRUE))
  kv <- list(); i <- 1
  while (i <= length(args)) {
    k <- args[[i]]
    if (startsWith(k, "--")) {
      v <- if (i + 1 <= length(args) && !startsWith(args[[i+1]], "--")) args[[i+1]] else TRUE
      kv[[substr(k, 3, nchar(k))]] <- v
      i <- i + if (isTRUE(v) || identical(v, TRUE)) 1 else 2
    } else i <- i + 1
  }
  kv
}

ensure_dir <- function(p) if (!dir.exists(p)) dir.create(p, recursive = TRUE, showWarnings = FALSE)
fail_plot <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()
safe_slug <- function(x){ x <- gsub("[^A-Za-z0-9\\-_]+","_", x); x <- gsub("_+","_", x); x <- gsub("^_|_$","", x); ifelse(nchar(x)>0, x, "output") }
percent_s <- function(x, acc=.1) ifelse(is.finite(x), percent(x, accuracy=acc), "NA")
to_upper  <- function(xs) unique(toupper(trimws(xs)))

# ----------------------------------------------------------------------
# Args e paths
# ----------------------------------------------------------------------
args <- parse_args()
db_path <- args$db
start_d <- args$start
end_d   <- args$end
min_n   <- as.integer(args[["min-analises"]] %||% "50")
out_dir <- args[["out-dir"]]

peritos_csv <- args[["peritos-csv"]] %||% NULL   # lista externa (Fluxo A/B)
scope_csv   <- args[["scope-csv"]]   %||% NULL   # define ESCOPO (coorte)

# NOVO: escolha do critério de ranqueamento quando não há manifest
flow_arg    <- toupper(trimws(args[["flow"]] %||% ""))     # "A" ou "B"
rank_by_arg <- tolower(trimws(args[["rank-by"]] %||% ""))  # "scorefinal" ou "harm"

if (is.null(db_path) || is.null(start_d) || is.null(end_d)) {
  stop("Uso: --db <path> --start YYYY-MM-DD --end YYYY-MM-DD [--min-analises 50] [--peritos-csv <csv>] [--scope-csv <csv>] [--flow A|B | --rank-by scoreFinal|harm] [--out-dir <dir>]")
}

base_dir   <- normalizePath(file.path(dirname(db_path), ".."))
export_dir <- if (!is.null(out_dir)) normalizePath(out_dir, mustWork = FALSE) else
  file.path(base_dir, "graphs_and_tables", "exports")
ensure_dir(export_dir)

png_file <- file.path(export_dir, "rcheck_top10_nc_rate.png")
org_main <- file.path(export_dir, "rcheck_top10_nc_rate.org")
org_comm <- file.path(export_dir, "rcheck_top10_nc_rate_comment.org")

# ----------------------------------------------------------------------
# Conexão e helpers
# ----------------------------------------------------------------------
con <- dbConnect(RSQLite::SQLite(), db_path)
on.exit(try(am_safe_disconnect(con), silent=TRUE), add=TRUE)

table_exists <- function(con, name) {
  nrow(am_dbGetQuery(con, "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1", params=list(name))) > 0
}
detect_analises_table <- function(con) { for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t); stop("Não encontrei 'analises' nem 'analises_atestmed'.") }
a_tbl <- detect_analises_table(con)
if (!table_exists(con, "indicadores")) {
  ggsave(png_file, fail_plot("Tabela 'indicadores' não encontrada"), width=9, height=5, dpi=150)
  quit(save="no")
}

# utilitário: carregar lista do CSV (se existir)
load_names_csv <- function(path) {
  if (is.null(path) || !nzchar(path) || !file.exists(path)) return(NULL)
  df <- tryCatch(read.csv(path, stringsAsFactors=FALSE, fileEncoding="UTF-8-BOM"), error=function(e) NULL)
  if (is.null(df) || !nrow(df)) return(NULL)
  key <- if ("nomePerito" %in% names(df)) "nomePerito" else names(df)[1]
  out <- trimws(as.character(df[[key]])); out <- out[nzchar(out)]
  if (length(out)) unique(out) else NULL
}

scope_names <- to_upper(load_names_csv(scope_csv))
scope_clause <- ""
scope_params <- list()
if (length(scope_names)) {
  placeholders_scope <- paste(rep("?", length(scope_names)), collapse = ",")
  scope_clause <- sprintf(" AND TRIM(UPPER(p.nomePerito)) IN (%s) ", placeholders_scope)
  scope_params <- as.list(scope_names)
}

# ----------------------------------------------------------------------
# Top 10 — fonte: (1) manifest CSV; senão (2) rank por scoreFinal (Fluxo A) OU harm (Fluxo B)
#      — com ESCOPO aplicado quando fornecido
# ----------------------------------------------------------------------
top10_names <- load_names_csv(peritos_csv)

# decide métrica quando não houver manifest
metric <- "scoreFinal"  # padrão Fluxo A
if (nzchar(rank_by_arg)) {
  if (rank_by_arg %in% c("harm","scorefinal","score_final","score")) {
    metric <- if (startsWith(rank_by_arg,"harm")) "harm" else "scoreFinal"
  }
} else if (nzchar(flow_arg)) {
  metric <- if (flow_arg == "B") "harm" else "scoreFinal"
}

if (!is.null(top10_names)) {
  # Fonte = manifest (lista externa)
  top10 <- tibble::tibble(nomePerito = head(top10_names, 10L))
  sel_caption <- "Seleção: lista externa (manifest) — fornecida pelo make_kpi_report.py."
} else {
  # Fonte = indicadores.<metric> (aplica ESCOPO se houver)
  ind_cols <- am_dbGetQuery(con, "PRAGMA table_info(indicadores)")$name
  if (!(metric %in% ind_cols)) {
    ggsave(png_file, fail_plot(sprintf("A coluna '%s' não existe em 'indicadores'.", metric)), width=9, height=5, dpi=150)
    quit(save="no")
  }
  qry_top10 <- sprintf("
  SELECT p.nomePerito, i.%s AS metric_val, COUNT(a.protocolo) AS total_analises
    FROM indicadores i
    JOIN peritos   p ON i.perito = p.siapePerito
    JOIN %s  a ON a.siapePerito = i.perito
   WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ? %s
   GROUP BY p.nomePerito, i.%s
  HAVING total_analises >= ?
   ORDER BY i.%s DESC, total_analises DESC
   LIMIT 10
  ", metric, a_tbl, scope_clause, metric, metric)
  params_top10 <- c(list(start_d, end_d), scope_params, list(min_n))
  top10 <- do.call(am_dbGetQuery, c(list(con, qry_top10), list(params = params_top10)))
  if (nrow(top10) == 0) {
    ggsave(png_file, fail_plot("Sem Top 10 para o período/critério (após escopo)."), width=9, height=5, dpi=150)
    quit(save="no")
  }
  sel_caption <- sprintf("Seleção: %s (indicadores.%s)%s.",
                         if (metric=="harm") "harm (Fluxo B)" else "scoreFinal (Fluxo A)",
                         metric,
                         if (length(scope_names)) " — escopo aplicado" else "")
}

# ----------------------------------------------------------------------
# %NC (robusto) para os peritos selecionados — com ESCOPO se houver
# ----------------------------------------------------------------------
peritos <- paste(sprintf("'%s'", gsub("'", "''", top10$nomePerito)), collapse=",")

nc_expr <- "
CASE
  WHEN CAST(IFNULL(a.conformado,1) AS INTEGER)=0 THEN 1
  WHEN TRIM(IFNULL(a.motivoNaoConformado,'')) <> ''
       AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0 THEN 1
  ELSE 0
END
"

qry_nc <- sprintf("
SELECT p.nomePerito,
       COUNT(*) AS total,
       SUM(%s) AS nc
  FROM %s a
  JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
   AND p.nomePerito IN (%s) %s
 GROUP BY p.nomePerito
", nc_expr, a_tbl, peritos, scope_clause)

params_nc <- c(list(start_d, end_d), scope_params)
df <- do.call(am_dbGetQuery, c(list(con, qry_nc), list(params = params_nc)))

if (nrow(df) == 0) {
  ggsave(png_file, fail_plot("Sem dados de NC para os selecionados (após escopo)."), width=9, height=5, dpi=150)
  quit(save="no")
}

df <- df %>%
  mutate(prop_nc = ifelse(total > 0, nc/total, NA_real_),
         pct_nc  = prop_nc * 100) %>%
  arrange(desc(pct_nc)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito))

# ----------------------------------------------------------------------
# Plot
# ----------------------------------------------------------------------
p <- ggplot(df, aes(x = nomePerito, y = pct_nc)) +
  geom_col(fill = "#d62728") +
  geom_text(aes(label = sprintf("%.1f%% (n=%d/%d)", pct_nc, nc, total)),
            vjust = -0.3, size = 3) +
  labs(
    title = "Top 10 — Taxa de Não Conformidade (NC robusto) [%]",
    subtitle = sprintf("%s a %s | mínimo de análises: %d | %s", start_d, end_d, min_n, sel_caption),
    x = "Perito", y = "% NC (robusto)",
    caption = "NC robusto: conformado=0 OU (motivoNaoConformado ≠ '' E CAST(motivoNaoConformado) ≠ 0)."
  ) +
  theme_minimal(base_size = 11) +
  theme(axis.text.x = element_text(angle=45, hjust=1))

ymax <- suppressWarnings(max(df$pct_nc, na.rm = TRUE)); if (!is.finite(ymax)) ymax <- 0
ggsave(png_file, p + coord_cartesian(ylim = c(0, ymax * 1.15)),
       width=9, height=5, dpi=150)
base::cat(sprintf("✅ Figura salva: %s\n", png_file))

# ----------------------------------------------------------------------
# Comentários .org
# ----------------------------------------------------------------------
ord <- df %>% arrange(desc(prop_nc))
top3 <- ord %>% dplyr::slice_head(n = 3) %>%
  transmute(txt = sprintf("%s: %s (n=%d/%d)",
                          as.character(nomePerito),
                          percent_s(prop_nc, .1), nc, total))
rng     <- range(df$prop_nc, na.rm = TRUE)
media   <- mean(df$prop_nc, na.rm = TRUE)
mediana <- median(df$prop_nc, na.rm = TRUE)

sel_txt <- if (!is.null(top10_names)) {
  paste0("lista externa (manifest) — fornecida pelo make_kpi_report.py",
         if (length(scope_names)) " — escopo aplicado." else ".")
} else if (metric == "harm") {
  paste0("Top-10 por *harm* (Fluxo B)",
         if (length(scope_names)) " — com escopo." else ".")
} else {
  paste0("Top-10 por *scoreFinal* (Fluxo A)",
         if (length(scope_names)) " — com escopo." else ".")
}

escopo_txt <- if (length(scope_names)) {
  paste0("\n*Escopo.* Coorte com ", length(scope_names), " peritos; primeiros: ",
         paste(head(scope_names, 5), collapse = ", "),
         if (length(scope_names) > 5) ", …" else "", ".")
} else ""

metodo_txt <- paste0(
  "*Método.* Seleção de peritos: ", sel_txt, " ",
  "Para cada selecionado, calculamos a *taxa de NC robusto* usando ",
  shQuote(a_tbl), " no período ", start_d, " a ", end_d, ".", escopo_txt
)

interpreta_txt <- paste0(
  "*Interpretação.* Entre os selecionados, a %NC variou de ",
  percent_s(min(rng), .1), " a ", percent_s(max(rng), .1),
  "; média=", percent_s(media, .1), ", mediana=", percent_s(mediana, .1), ".\n",
  if (nrow(top3)) paste0("- Maiores %NC: ", paste(top3$txt, collapse = "; "), ".\n") else "",
  "Observação: seleção e %NC são conceitos distintos; avalie sempre o volume (n) e o contexto."
)

org_main_txt <- paste(
  "#+CAPTION: Top 10 — %NC (robusto) no período",
  sprintf("[[file:%s]]", basename(png_file)),
  "", metodo_txt, "", interpreta_txt, "", sep = "\n"
)
writeLines(org_main_txt, org_main); base::cat(sprintf("✅ Org salvo: %s\n", org_main))

org_comm_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comm_txt, org_comm); base::cat(sprintf("✅ Org(comment) salvo: %s\n", org_comm))
