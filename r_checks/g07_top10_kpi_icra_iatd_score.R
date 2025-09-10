#!/usr/bin/env Rscript
# ---- Auto-normalized header (injected by run_r_checks_autofix.py) ----
options(stringsAsFactors = FALSE)
options(encoding = "UTF-8")
options(warn = 1)
options(repos = c(CRAN = "https://cloud.r-project.org"))
Sys.setlocale(category = "LC_ALL", locale = "C.UTF-8")
# ----------------------------------------------------------------------

# -*- coding: utf-8 -*-
# KPIs (ICRA, IATD, ScoreFinal) — Top 10 (grupo) vs. Resto
# Saídas:
#   - rcheck_top10_kpi_icra_iatd_score.png
#   - rcheck_top10_kpi_icra_iatd_score.org
#   - rcheck_top10_kpi_icra_iatd_score_comment.org

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
  library(optparse)
  library(DBI); library(RSQLite)
  library(dplyr); library(tidyr); library(forcats)
  library(ggplot2); library(scales); library(stringr); library(readr)
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

  if (!exists("am_normalize_cli", mode="function", inherits=TRUE)) {
    am_normalize_cli <<- function(x) as.character(x)
  }
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
    am_open_db <<- function(path) {
      p <- normalizePath(path, mustWork=TRUE)
      DBI::dbConnect(RSQLite::SQLite(), dbname=p)
    }
  }
  if (!exists("am_resolve_export_dir", mode="function", inherits=TRUE)) {
    am_resolve_export_dir <<- function(out_dir=NULL) {
      if (!is.null(out_dir) && nzchar(out_dir)) {
        od <- normalizePath(out_dir, mustWork=FALSE)
      } else {
        dbp <- am_args[["db"]] %||% ""
        base_dir <- if (nzchar(dbp)) normalizePath(file.path(dirname(dbp), ".."), mustWork=FALSE) else getwd()
        od <- file.path(base_dir, "graphs_and_tables", "exports")
      }
      if (!dir.exists(od)) dir.create(od, recursive=TRUE, showWarnings=FALSE)
      od
    }
  }
  if (!exists("am_detect_analises_table", mode="function", inherits=TRUE)) {
    am_detect_analises_table <<- function(con) {
      has <- function(nm) {
        nrow(am_dbGetQuery(con,
          "SELECT 1 FROM sqlite_master WHERE type in ('table','view') AND name=? LIMIT 1",
          params=list(nm))) > 0
      }
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
  if (exists("args", inherits=TRUE)) {
    .cand <- get("args", inherits=TRUE)
    if (!is.function(.cand)) .raw <- .cand
  }
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
  a_tbl <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
  cols <- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))

  am_dbGetQuery <<- (function(.f) {
    force(.f)
    function(con, statement, ...) {
      st <- statement
      if (length(st) != 1L) st <- paste(st, collapse=" ")
      .f(con, st, ...)
    }
  })(DBI::dbGetQuery)
})
# ==== ATESTMED PROLOGO (FIM) ====


# ───────────────────────── CLI ─────────────────────────
option_list <- list(
  make_option("--db",    type="character", help="Caminho do SQLite (.db)"),
  make_option("--start", type="character", help="Data inicial YYYY-MM-DD"),
  make_option("--end",   type="character", help="Data final   YYYY-MM-DD"),
  make_option("--min-analises", type="integer", default = 50L, help="Elegibilidade Top 10 [default: %default]"),
  make_option("--peritos-csv",  type="character", default=NULL, help="Manifesto (ordem preservada; revalidado por n≥min)"),
  make_option("--scope-csv",    type="character", default=NULL, help="Escopo/coorte de peritos a considerar em TODAS as consultas"),
  make_option("--flow",         type="character", default=NULL, help="A=scoreFinal (padrão), B=harm"),
  make_option("--rank-by",      type="character", default=NULL, help="scoreFinal|harm (prioriza sobre --flow)"),
  make_option("--out-dir", type="character", default=NULL, help="Diretório de saída")
)
opt <- optparse::parse_args(OptionParser(option_list = option_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end))

out_dir <- am_resolve_export_dir(opt$`out-dir`)
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# ───────────────────────── Helpers ─────────────────────────
fail_plot <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()

pick_col <- function(cols, candidates) {
  for (c in candidates) if (c %in% cols) return(c)
  low <- tolower(cols)
  for (c in candidates) {
    idx <- which(low == tolower(c))
    if (length(idx) == 1) return(cols[[idx]])
  }
  for (c in candidates) {
    hit <- grep(paste0("^", tolower(c), "$|", tolower(c)), low, value = FALSE)
    if (length(hit)) return(cols[[hit[1]]])
  }
  NA_character_
}

table_exists <- function(con, name) {
  nrow(am_dbGetQuery(con, "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
                     params=list(name))) > 0
}
detect_analises_table <- function(con) {
  for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t)
  stop("Não encontrei 'analises' nem 'analises_atestmed'.")
}
load_names <- function(path){
  if (is.null(path) || !nzchar(path) || !file.exists(path)) return(NULL)
  df <- tryCatch(readr::read_csv(path, show_col_types=FALSE), error=function(e) NULL)
  if (is.null(df) || !nrow(df)) return(NULL)
  key <- if ("nomePerito" %in% names(df)) "nomePerito" else names(df)[1]
  u <- unique(trimws(as.character(df[[key]]))); u[nzchar(u)]
}
to_upper <- function(xs) unique(toupper(trimws(xs)))

# ───────────────────────── Conexão/colunas ─────────────────────────
if (!table_exists(con, "indicadores")) {
  out_png <- file.path(out_dir, "rcheck_top10_kpi_icra_iatd_score.png")
  ggsave(out_png, fail_plot("Tabela 'indicadores' não encontrada"), width=8.5, height=10, dpi=160)
  quit(save="no")
}

a_tbl <- detect_analises_table(con)
q_a_tbl <- am_dbQuoteIdentifier(con, a_tbl)

cols_ind <- am_dbGetQuery(con, "PRAGMA table_info(indicadores)")$name
col_icra  <- pick_col(cols_ind, c("ICRA","icra","kpi_icra"))
col_iatd  <- pick_col(cols_ind, c("IATD","iatd","kpi_iatd"))
col_score <- pick_col(cols_ind, c("scoreFinal","score_final","ScoreFinal","score","scorefinal"))
col_harm  <- pick_col(cols_ind, c("harm","HARM","kpi_harm","harmScore","scoreHarm","harm_score"))

out_png  <- file.path(out_dir, "rcheck_top10_kpi_icra_iatd_score.png")
org_main <- file.path(out_dir, "rcheck_top10_kpi_icra_iatd_score.org")
org_comm <- file.path(out_dir, "rcheck_top10_kpi_icra_iatd_score_comment.org")

needed <- c(col_icra, col_iatd, col_score)
if (any(is.na(needed))) {
  miss <- c("ICRA"  = is.na(col_icra),
            "IATD"  = is.na(col_iatd),
            "Score" = is.na(col_score))
  msg <- paste("Colunas ausentes em 'indicadores':", paste(names(miss)[miss], collapse=", "))
  ggsave(out_png, fail_plot(msg), width=8.5, height=10, dpi=160)
  writeLines(paste0("*Erro.* ", msg), org_main)
  writeLines(paste0("*Erro.* ", msg), org_comm)
  quit(save="no")
}

# ───────────────────────── Escopo (coorte) ─────────────────────────
scope_names  <- to_upper(load_names(opt$`scope-csv`))
scope_clause <- ""
scope_params <- list()
if (length(scope_names)) {
  placeholders <- paste(rep("?", length(scope_names)), collapse=",")
  scope_clause <- sprintf(" AND TRIM(UPPER(p.nomePerito)) IN (%s) ", placeholders)
  scope_params <- as.list(scope_names)
}

# ───────────────────────── Seleção do grupo (Manifesto / Fluxo) ─────────────────────────
flow_opt    <- toupper(opt$flow %||% "")
rank_by_opt <- tolower(opt$`rank-by` %||% "")
rank_by <- if (nzchar(rank_by_opt)) rank_by_opt else if (identical(flow_opt,"B")) "harm" else "scorefinal"

names_manifest <- load_names(opt$`peritos-csv`)
sel_caption <- NULL

if (!is.null(names_manifest)) {
  # revalida n ≥ min-analises preservando a ordem do manifesto (APLICA ESCOPO)
  in_list <- paste(sprintf("'%s'", gsub("'", "''", names_manifest)), collapse=",")
  sql_cnt <- sprintf("
    SELECT p.nomePerito AS nomePerito, COUNT(a.protocolo) AS total_analises
      FROM %s a
      JOIN peritos p ON a.siapePerito = p.siapePerito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
       AND p.nomePerito IN (%s) %s
     GROUP BY p.nomePerito
  ", q_a_tbl, in_list, scope_clause)
  cnt <- do.call(am_dbGetQuery, c(list(con, sql_cnt), list(params=c(list(opt$start, opt$end), scope_params))))
  top10 <- cnt %>%
    filter(total_analises >= opt$`min-analises`) %>%
    arrange(match(nomePerito, names_manifest)) %>%
    slice_head(n=10) %>% select(nomePerito)
  if (!nrow(top10)) {
    ggsave(out_png, fail_plot("Manifesto sem peritos elegíveis (n < min-analises / escopo)."), width=8.5, height=10, dpi=160)
    writeLines("*Manifesto sem peritos elegíveis.*", org_main)
    writeLines("*Manifesto sem peritos elegíveis.*", org_comm)
    quit(save="no")
  }
  sel_caption <- sprintf("Seleção: manifesto (revalidado: n ≥ %d%s).",
                         opt$`min-analises`, if (length(scope_names)) "; escopo aplicado" else "")
} else {
  # ranking interno por scoreFinal (Fluxo A) ou harm (Fluxo B) — (APLICA ESCOPO)
  order_col <- if (rank_by == "harm") {
    if (is.na(col_harm)) {
      ggsave(out_png, fail_plot("Coluna 'harm' não encontrada em 'indicadores'."), width=8.5, height=10, dpi=160)
      writeLines("*Erro.* Coluna 'harm' ausente em 'indicadores'.", org_main)
      writeLines("*Erro.* Coluna 'harm' ausente em 'indicadores'.", org_comm)
      quit(save="no")
    }
    col_harm
  } else col_score

  sql_top10 <- sprintf("
    SELECT p.nomePerito AS nomePerito, i.%s AS rank_metric, COUNT(a.protocolo) AS total_analises
      FROM indicadores i
      JOIN peritos p ON i.perito = p.siapePerito
      JOIN %s a ON a.siapePerito = i.perito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ? %s
     GROUP BY p.nomePerito, i.%s
    HAVING total_analises >= ?
     ORDER BY i.%s DESC, total_analises DESC
     LIMIT 10
  ", order_col, q_a_tbl, scope_clause, order_col, order_col)

  params_top10 <- c(list(opt$start, opt$end), scope_params, list(opt$`min-analises`))
  top10 <- do.call(am_dbGetQuery, c(list(con, sql_top10), list(params=params_top10)))
  if (nrow(top10) == 0) {
    ggsave(out_png, fail_plot("Nenhum Top 10 para o período/critério (após escopo)."), width=8.5, height=10, dpi=160)
    writeLines("*Sem Top 10 no período/critério informado.*", org_main)
    writeLines("*Sem Top 10 no período/critério informado.*", org_comm)
    quit(save="no")
  }
  sel_caption <- sprintf("Seleção: Top10 por %s (n ≥ %d%s).",
                         if (rank_by=="harm") "harm (Fluxo B)" else "scoreFinal (Fluxo A)",
                         opt$`min-analises`, if (length(scope_names)) "; escopo aplicado" else "")
}

top10_set <- unique(top10$nomePerito)

# ───────────────────────── Coleta KPIs para ativos (APLICA ESCOPO) ─────────────────────────
sql_ativos <- sprintf("
WITH ativos AS (
  SELECT DISTINCT a.siapePerito AS siape
    FROM %s a
    JOIN peritos p ON a.siapePerito = p.siapePerito
   WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ? %s
)
SELECT p.nomePerito,
       CAST(i.%s AS REAL) AS icra,
       CAST(i.%s AS REAL) AS iatd,
       CAST(i.%s AS REAL) AS score
  FROM indicadores i
  JOIN peritos p ON i.perito = p.siapePerito
  JOIN ativos  s ON s.siape   = i.perito
", q_a_tbl, scope_clause, col_icra, col_iatd, col_score)

df <- do.call(am_dbGetQuery, c(list(con, sql_ativos), list(params=c(list(opt$start, opt$end), scope_params)))) %>%
  mutate(grupo = if_else(nomePerito %in% top10_set, "Top10", "Resto"))

if (nrow(df) == 0) {
  ggsave(out_png, fail_plot("Sem peritos ativos no período (após escopo)."), width=8.5, height=10, dpi=160)
  writeLines("*Sem peritos ativos no período informado.*", org_main)
  writeLines("*Sem peritos ativos no período informado.*", org_comm)
  quit(save="no")
}

# ───────────────────────── Long + testes ─────────────────────────
long <- df %>%
  select(grupo, icra, iatd, score) %>%
  pivot_longer(cols = -grupo, names_to = "kpi", values_to = "valor") %>%
  mutate(kpi = recode(kpi, icra="ICRA", iatd="IATD", score="Score Final")) %>%
  filter(is.finite(valor))

if (nrow(long) == 0) {
  ggsave(out_png, fail_plot("Sem valores numéricos de KPIs para comparar."), width=8.5, height=10, dpi=160)
  writeLines("*Sem valores numéricos de KPIs para comparar.*", org_main)
  writeLines("*Sem valores numéricos de KPIs para comparar.*", org_comm)
  quit(save="no")
}

tests <- long %>%
  group_by(kpi) %>%
  summarise(
    p_wilcox = tryCatch({
      if (length(unique(grupo)) < 2) NA_real_ else
        unname(wilcox.test(valor ~ grupo, exact = FALSE)$p.value)
    }, error = function(e) NA_real_),
    n_top10   = sum(grupo=="Top10"),
    n_resto   = sum(grupo=="Resto"),
    med_top10 = median(valor[grupo=="Top10"], na.rm=TRUE),
    med_resto = median(valor[grupo=="Resto"], na.rm=TRUE),
    .groups = "drop"
  ) %>%
  mutate(lbl = sprintf("p=%.3g | med Top10=%.3f | med Resto=%.3f | n=(%d,%d)",
                       p_wilcox, med_top10, med_resto, n_top10, n_resto))

# ───────────────────────── Plot ─────────────────────────
titulo <- sprintf("Top 10 — KPIs (ICRA, IATD, Score Final) vs. %s\n%s a %s",
                  if (length(scope_names)) "Coorte (resto)" else "Brasil (resto)",
                  opt$start, opt$end)

g <- ggplot(long, aes(x = grupo, y = valor, fill = grupo)) +
  geom_boxplot(outlier.shape = NA, width = 0.55) +
  geom_jitter(width = 0.12, alpha = 0.5, size = 1.6) +
  facet_wrap(~kpi, scales = "free_y", ncol = 1) +
  labs(
    title = titulo, x = NULL, y = NULL,
    subtitle = sel_caption,
    caption = sprintf("Top 10 conforme seleção (%s). Teste: Mann-Whitney por KPI. Elegibilidade: n ≥ %d.",
                      if (!is.null(names_manifest)) "manifesto revalidado" else
                        if (rank_by=="harm") "ranking por harm (Fluxo B)" else "ranking por scoreFinal (Fluxo A)",
                      opt$`min-analises`)
  ) +
  theme_minimal(base_size = 11) +
  theme(panel.grid.minor = element_blank(),
        legend.position = "none")

ggsave(out_png, g, width = 8.5, height = 10, dpi = 160)
message(sprintf("✅ Figura salva: %s", out_png))

# ───────────────────────── Comentários (.org) ─────────────────────────
# Método
escopo_txt <- if (length(scope_names)) {
  paste0("\n*Escopo.* Coorte com ", length(scope_names), " peritos; primeiros: ",
         paste(head(scope_names, 5), collapse = ", "),
         if (length(scope_names) > 5) ", …" else "", ".")
} else ""

metodo_txt <- paste0(
  "*Método.* Seleção do grupo Top10: ",
  if (!is.null(names_manifest))
    sprintf("por manifesto (ordem preservada) revalidado com n ≥ %d. ", opt$`min-analises`)
  else if (rank_by=="harm")
    sprintf("Top 10 por *harm* (desc) com n ≥ %d. ", opt$`min-analises`)
  else
    sprintf("Top 10 por *scoreFinal* (desc) com n ≥ %d. ", opt$`min-analises`),
  "O grupo *Resto* inclui peritos *ativos* no período (com alguma análise em ", shQuote(a_tbl), "). ",
  "Comparamos *ICRA*, *IATD* e *Score Final* entre *Top10* e *Resto* usando o teste *Mann-Whitney* (não-paramétrico).",
  escopo_txt
)

# Interpretação resumida por KPI
kpi_lines <- apply(tests, 1, function(r){
  sprintf("- %s: med Top10=%.3f vs med Resto=%.3f (p=%s; nTop10=%s, nResto=%s)",
          r[["kpi"]], as.numeric(r[["med_top10"]]), as.numeric(r[["med_resto"]]),
          formatC(as.numeric(r[["p_wilcox"]]), format="fg", digits=3),
          r[["n_top10"]], r[["n_resto"]])
})
interpreta_txt <- paste0(
  "*Interpretação.* As medianas e p-valores resumem diferenças entre grupos por KPI. ",
  "Considere o tamanho amostral e a direção esperada de cada métrica no seu contexto.\n",
  paste(kpi_lines, collapse = "\n")
)

# .org principal (imagem + texto)
org_main_txt <- paste(
  "#+CAPTION: KPIs (ICRA, IATD, Score Final) — Top 10 vs. Resto",
  sprintf("[[file:%s]]", basename(out_png)),
  "",
  metodo_txt, "",
  interpreta_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
message(sprintf("✅ Org salvo: %s", org_main))

# .org apenas comentário
org_comm_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comm_txt, org_comm)
message(sprintf("✅ Org(comment) salvo: %s", org_comm))
