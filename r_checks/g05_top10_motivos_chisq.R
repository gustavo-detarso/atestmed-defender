#!/usr/bin/env Rscript
# -*- coding: utf-8 -*-
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
  message("[g05_top10_motivos_chisq] _common.R não encontrado — usando fallbacks internos.")
}

# --- hardening: garanta am_resolve_export_dir mesmo sem _common.R ---
if (!exists("am_resolve_export_dir", mode = "function", inherits = TRUE)

) {
  `%||%` <- function(a,b) if (is.null(a)) b else a
  am_resolve_export_dir <- function(out_dir = NULL) {
    if (!is.null(out_dir) && nzchar(out_dir)) {
      normalizePath(out_dir, mustWork = FALSE)
    } else {
      dbp <- tryCatch(am_args[["db"]], error = function(e) NULL) %||% ""
      base_dir <- if (nzchar(dbp)) normalizePath(file.path(dirname(dbp), ".."), mustWork = FALSE) else getwd()
      od <- file.path(base_dir, "graphs_and_tables", "exports")
      if (!dir.exists(od)) dir.create(od, recursive = TRUE, showWarnings = FALSE)
      od
    }
  }
}
  library(optparse); library(DBI); library(RSQLite)
  library(dplyr); library(ggplot2); library(stringr); library(forcats); library(tidyr); library(scales); library(readr)
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
      while (i <= n) { k <- a[[i]]
        if (startsWith(k, "--")) {
          v <- if (i+1L <= n && !startsWith(a[[i+1L]], "--")) a[[i+1L]] else TRUE
          kv[[sub("^--","",k)]] <- v; i <- i + (if (identical(v, TRUE)) 1L else 2L)
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
      if (!is.null(out_dir) && nzchar(out_dir)) {
        normalizePath(out_dir, mustWork=FALSE)
      } else {
        dbp <- am_args[["db"]] %||% ""
        base_dir <- if (nzchar(dbp)) normalizePath(file.path(dirname(dbp), ".."), mustWork=FALSE) else getwd()
        od <- file.path(base_dir, "graphs_and_tables", "exports")
      }
      if (!dir.exists(od)) dir.create(od, recursive=TRUE, showWarnings=FALSE); od
    }
  }
  if (!exists("am_detect_analises_table", mode="function", inherits=TRUE)) {
    am_detect_analises_table <<- function(con) {
      has <- function(nm) nrow(am_dbGetQuery(con,"SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1", params=list(nm)))>0
      for (t in c("analises","analises_atestmed")) if (has(t)) return(t)
      stop("Não encontrei 'analises' nem 'analises_atestmed'.")
    }
  }
  am_dbGetQuery <<- (function(.f){ force(.f); function(con, statement, ...){ st <- statement; if (length(st)!=1L) st <- paste(st, collapse=" "); .f(con, st, ...) } })(DBI::dbGetQuery)
  am_args <<- tryCatch(am_parse_args(), error=function(e) list())
  db_path <- am_args[["db"]]; if (is.null(db_path)||!nzchar(db_path)) stop("Faltou --db <path>")
  con <<- am_open_db(db_path)
  # (patch) removido on.exit de desconexão precoce
  export_dir <<- am_resolve_export_dir(am_args[["out-dir"]])
  a_tbl     <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
})
# ==== ATESTMED PROLOGO (FIM) ====


# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
option_list <- list(
  make_option("--db",            type="character"),
  make_option("--start",         type="character"),
  make_option("--end",           type="character"),
  make_option("--min-analises",  type="integer",  default=50L),
  make_option("--out-dir",       type="character", default=NULL),
  make_option("--min-count",     type="integer",  default=10L),
  make_option("--topn",          type="integer",  default=15L),
  make_option("--peritos-csv",   type="character", default=NULL, help="Manifesto de peritos para o grupo-alvo (Fluxo)"),
  make_option("--scope-csv",     type="character", default=NULL, help="ESCOPO/coorte (limita TODAS as queries ao conjunto deste CSV)"),
  make_option("--flow",          type="character", default=NULL, help="A (scoreFinal) ou B (harm) — usado se não houver --peritos-csv"),
  make_option("--rank-by",       type="character", default=NULL, help="scoreFinal|harm — sobrescreve --flow se fornecido")
)
opt <- optparse::parse_args(OptionParser(option_list = option_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end))
out_dir <- am_resolve_export_dir(opt$`out-dir`)
if (!dir.exists(out_dir)) dir.create(out_dir, recursive=TRUE, showWarnings=FALSE)

png_path <- file.path(out_dir, "rcheck_top10_motivos_chisq.png")
org_main <- file.path(out_dir, "rcheck_top10_motivos_chisq.org")
org_comm <- file.path(out_dir, "rcheck_top10_motivos_chisq_comment.org")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
percent_s <- function(x, acc=.1) ifelse(is.finite(x), scales::percent(x, accuracy=acc), "NA")
fail_plot <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()
table_exists <- function(con, name) nrow(am_dbGetQuery(con,"SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1", params=list(name)))>0
detect_analises_table <- function(con){ for (t in c("analises","analises_atestmed")) if (table_exists(con,t)) return(t); stop("Não encontrei 'analises' nem 'analises_atestmed'.") }
lump_rare <- function(tbl, min_count=10L){ tbl %>% mutate(motivo=if_else(n<min_count,"OUTROS",motivo)) %>% group_by(motivo) %>% summarise(n=sum(n), .groups="drop") }
load_names <- function(path){
  if (is.null(path)||!nzchar(path)||!file.exists(path)) return(NULL)
  df <- tryCatch(readr::read_csv(path, show_col_types=FALSE), error=function(e) NULL)
  if (is.null(df)||!nrow(df)) return(NULL)
  key <- if ("nomePerito" %in% names(df)) "nomePerito" else names(df)[1]
  u <- unique(trimws(as.character(df[[key]]))); u[nzchar(u)]
}
to_upper <- function(xs) unique(toupper(trimws(xs)))

# ────────────────────────────────────────────────────────────────────────────────
# Seleção do grupo-alvo (manifesto OU Top10 interno por scoreFinal/harm) + ESCOPO
# ────────────────────────────────────────────────────────────────────────────────
con <- am_open_db(opt$db)
on.exit(try(am_safe_disconnect(con), silent=TRUE), add=TRUE)
on.exit(try(am_safe_disconnect(con), silent=TRUE), add=TRUE)
a_tbl <- detect_analises_table(con)

rank_by_opt <- tolower(opt$`rank-by` %||% "")
flow_opt    <- toupper(opt$flow %||% "")
rank_by <- if (nzchar(rank_by_opt)) rank_by_opt else if (identical(flow_opt,"B")) "harm" else "scorefinal"

# ESCOPO (opcional): restringe todas as consultas à coorte fornecida
scope_names  <- to_upper(load_names(opt$`scope-csv`))
scope_clause <- ""
scope_params <- list()
if (length(scope_names)) {
  placeholders_scope <- paste(rep("?", length(scope_names)), collapse=",")
  scope_clause <- sprintf(" AND TRIM(UPPER(p.nomePerito)) IN (%s) ", placeholders_scope)
  scope_params <- as.list(scope_names)
}

need_indicadores <- is.null(opt$`peritos-csv`)
if (need_indicadores && !table_exists(con,"indicadores")) {
  ggsave(png_path, fail_plot("Tabela 'indicadores' não encontrada e sem --peritos-csv."), width=10, height=6, dpi=160); quit(save="no", status=0)
}
if (need_indicadores) {
  ind_cols <- am_dbGetQuery(con, "PRAGMA table_info(indicadores)")$name
  need_col <- if (rank_by=="harm") "harm" else "scoreFinal"
  if (!(need_col %in% ind_cols)) {
    ggsave(png_path, fail_plot(sprintf("Coluna '%s' ausente em 'indicadores'.", need_col)), width=10, height=6, dpi=160); quit(save="no", status=0)
  }
}

names_manifest <- load_names(opt$`peritos-csv`)
sel_caption <- NULL
sel_title   <- NULL

if (!is.null(names_manifest)) {
  # Revalida elegibilidade n≥min-analises preservando a ordem (aplica ESCOPO)
  in_list <- paste(sprintf("'%s'", gsub("'", "''", names_manifest)), collapse=",")
  sql_cnt <- sprintf("
    SELECT p.nomePerito AS nomePerito, COUNT(a.protocolo) AS total_analises
      FROM %s a
      JOIN peritos p ON a.siapePerito = p.siapePerito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
       AND p.nomePerito IN (%s) %s
     GROUP BY p.nomePerito
  ", am_dbQuoteIdentifier(con, a_tbl), in_list, scope_clause)
  cnt  <- do.call(am_dbGetQuery, c(list(con, sql_cnt), list(params=c(list(opt$start, opt$end), scope_params))))
  alvo <- cnt %>% filter(total_analises >= opt$`min-analises`) %>%
    arrange(match(nomePerito, names_manifest)) %>% slice_head(n=10) %>%
    select(nomePerito)
  if (!nrow(alvo)) { ggsave(png_path, fail_plot("Manifesto sem peritos elegíveis (min-analises/escopo)."), width=10, height=6, dpi=160); quit(save="no", status=0) }
  sel_caption <- sprintf("Seleção: manifesto (revalidado: n ≥ %d%s).", opt$`min-analises`, if (length(scope_names)) "; escopo aplicado" else "")
  sel_title   <- "Grupo (manifesto)"
} else {
  # Seleção interna — Fluxo A/B (aplica ESCOPO)
  order_col <- if (rank_by=="harm") "i.harm" else "i.scoreFinal"
  sql_top10 <- sprintf("
    SELECT p.nomePerito AS nomePerito, %s AS rank_metric, COUNT(a.protocolo) AS total_analises
      FROM indicadores i
      JOIN peritos p ON i.perito = p.siapePerito
      JOIN %s a ON a.siapePerito = i.perito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ? %s
     GROUP BY p.nomePerito, rank_metric
    HAVING total_analises >= ?
     ORDER BY rank_metric DESC, total_analises DESC
     LIMIT 10
  ", order_col, am_dbQuoteIdentifier(con, a_tbl), scope_clause)
  params_top10 <- c(list(opt$start, opt$end), scope_params, list(opt$`min-analises`))
  alvo <- do.call(am_dbGetQuery, c(list(con, sql_top10), list(params=params_top10)))
  if (!nrow(alvo)) { ggsave(png_path, fail_plot("Nenhum perito atende ao critério Top 10 no período (após escopo)."), width=10, height=6, dpi=160); quit(save="no", status=0) }
  sel_caption <- sprintf("Seleção: %s (n ≥ %d%s).", if (rank_by=="harm") "Top 10 por harm (Fluxo B)" else "Top 10 por scoreFinal (Fluxo A)", opt$`min-analises`, if (length(scope_names)) "; escopo aplicado" else "")
  sel_title   <- if (rank_by=="harm") "Top 10 (harm)" else "Top 10 (scoreFinal)"
}
alvo_set <- unique(alvo$nomePerito)

# ────────────────────────────────────────────────────────────────────────────────
# NC robusto (preferindo protocolos.motivo quando houver) — aplica ESCOPO
# ────────────────────────────────────────────────────────────────────────────────
has_protocolos <- table_exists(con, "protocolos")
desc_expr <- if (has_protocolos) {
  "COALESCE(NULLIF(TRIM(pr.motivo), ''), CAST(IFNULL(a.motivoNaoConformado,'') AS TEXT)) AS motivo_text"
} else {
  "CAST(IFNULL(a.motivoNaoConformado,'') AS TEXT) AS motivo_text"
}
join_prot <- if (has_protocolos) "LEFT JOIN protocolos pr ON a.protocolo = pr.protocolo" else ""

sql_nc <- sprintf("
SELECT p.nomePerito AS perito, %s
FROM %s a
JOIN peritos p ON a.siapePerito = p.siapePerito
%s
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ? %s
  AND (
        CAST(IFNULL(a.conformado,1) AS INTEGER)=0
        OR (TRIM(IFNULL(a.motivoNaoConformado,'')) <> '' AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0)
      )
", desc_expr, am_dbQuoteIdentifier(con, a_tbl), join_prot, scope_clause)

all_nc <- do.call(am_dbGetQuery, c(list(con, sql_nc), list(params=c(list(opt$start, opt$end), scope_params)))) %>%
  mutate(motivo_text = as.character(motivo_text),
         motivo = if_else(is.na(motivo_text) | trimws(motivo_text)=="" | trimws(motivo_text)=="0",
                          "MOTIVO_DESCONHECIDO", trimws(motivo_text))) %>%
  select(perito, motivo)

if (!nrow(all_nc)) { ggsave(png_path, fail_plot("Nenhuma análise NC (robusto) no período (após escopo)."), width=10, height=6, dpi=160); quit(save="no", status=0) }

tab <- all_nc %>%
  mutate(grupo = if_else(perito %in% alvo_set, "Alvo", "Resto")) %>%
  count(grupo, motivo, name="n") %>%
  tidyr::pivot_wider(names_from=grupo, values_from=n) %>%
  mutate(across(c(Alvo,Resto), ~ dplyr::coalesce(.x, 0L))) %>%
  arrange(desc(Alvo+Resto))

tab <- tab %>%
  rename(n = Alvo) %>% select(motivo, n, Resto) %>%
  lump_rare(min_count = opt$`min-count`) %>%
  rename(Alvo = n) %>%
  left_join(tab %>% select(motivo, Resto), by="motivo") %>%
  mutate(Resto = dplyr::coalesce(Resto, 0L)) %>%
  arrange(desc(Alvo + Resto))

total_alvo  <- sum(tab$Alvo)
total_resto <- sum(tab$Resto)
if (total_alvo==0 || total_resto==0) { ggsave(png_path, fail_plot("Sem dados suficientes para χ² (um grupo com total 0)."), width=10, height=6, dpi=160); quit(save="no", status=0) }

mat  <- rbind(Alvo = tab$Alvo, Resto = tab$Resto)
chs  <- suppressWarnings(chisq.test(mat))
if (any(chs$expected < 5, na.rm=TRUE)) chs <- suppressWarnings(chisq.test(mat, simulate.p.value=TRUE, B=5000))
pval <- chs$p.value

resumo <- tab %>%
  mutate(prop_alvo = Alvo/total_alvo, prop_resto = Resto/total_resto, diff = prop_alvo - prop_resto) %>%
  arrange(desc(abs(diff))) %>%
  slice_head(n = opt$topn) %>%
  mutate(motivo_plot = forcats::fct_reorder(motivo, diff))

# ────────────────────────────────────────────────────────────────────────────────
# Plot
# ────────────────────────────────────────────────────────────────────────────────
titulo <- sprintf("Motivos NC (robusto) — %s vs. Resto\n%s a %s  |  χ² p=%.3g",
                  sel_title, opt$start, opt$end, pval)

g <- ggplot(resumo, aes(x = motivo_plot, y = diff)) +
  geom_hline(yintercept = 0, linewidth = 0.4) +
  geom_col() + coord_flip() +
  labs(
    title = titulo,
    subtitle = sel_caption,
    x = NULL, y = "Diferença de proporções (Grupo − Resto)",
    caption = "NC robusto: conformado=0 OU (motivoNaoConformado ≠ '' E CAST(motivoNaoConformado) ≠ 0)."
  ) +
  theme_minimal(base_size=11) +
  theme(panel.grid.minor = element_blank(), plot.title = element_text(face="bold", hjust=0))

ggsave(png_path, g, width=10, height=6, dpi=160); message(sprintf("✅ Figura salva: %s", png_path))

# ────────────────────────────────────────────────────────────────────────────────
# Comentários (.org)
# ────────────────────────────────────────────────────────────────────────────────
top_pos <- resumo %>% filter(diff > 0) %>% slice_max(order_by=diff, n=min(3L, sum(resumo$diff>0))) %>%
  transmute(txt = sprintf("%s (+%s p.p.)", motivo, percent_s(diff, .1)))
top_neg <- resumo %>% filter(diff < 0) %>% slice_min(order_by=diff, n=min(3L, sum(resumo$diff<0))) %>%
  transmute(txt = sprintf("%s (%s p.p.)", motivo, percent_s(diff, .1)))

escopo_txt <- if (length(scope_names)) {
  paste0("\n*Escopo.* Coorte com ", length(scope_names), " peritos; primeiros: ",
         paste(head(scope_names, 5), collapse = ", "),
         if (length(scope_names) > 5) ", …" else "", ".")
} else ""

metodo_txt <- paste0(
  "*Método.* Grupo-alvo: ",
  if (!is.null(names_manifest)) paste0("manifesto (revalidado: n ≥ ", opt$`min-analises`, "). ")
  else if (rank_by=="harm")     paste0("Top 10 por *harm* (mín. ", opt$`min-analises`, " análises). ")
  else                           paste0("Top 10 por *scoreFinal* (mín. ", opt$`min-analises`, " análises). "),
  "Tabela motivo × grupo (*Alvo* vs *Resto*), com motivos raros (< ", opt$`min-count`, ") em 'OUTROS'. ",
  "Aplicamos *qui-quadrado* global (χ²; simulado se esperados <5). Exibimos os ",
  min(nrow(resumo), opt$topn), " maiores |Grupo − Resto|.",
  escopo_txt
)

interpreta_txt <- {
  sig <- if (is.finite(pval) && pval < 0.05) "diferenças *estatisticamente significativas*" else "diferenças não significativas ao nível 5%"
  pos_str <- if (nrow(top_pos)) paste("- Mais frequentes no grupo:", paste(top_pos$txt, collapse=", "), ".") else NULL
  neg_str <- if (nrow(top_neg)) paste("- Menos frequentes no grupo:", paste(top_neg$txt, collapse=", "), ".") else NULL
  paste0(
    "*Interpretação.* O teste global indica ", sig, " (p = ", formatC(pval, format="fg", digits=3), "). ",
    "Barras *positivas* = mais comuns no grupo-alvo; *negativas* = menos comuns. ",
    "Use como *pistas* para auditoria qualitativa, considerando volume e contexto.\n",
    paste(na.omit(c(pos_str, neg_str)), collapse="\n")
  )
}

org_main_txt <- paste(
  "#+CAPTION: Motivos de NC (robusto) — Diferença de proporções (Grupo − Resto)",
  sprintf("[[file:%s]]", basename(png_path)),
  "",
  metodo_txt, "",
  interpreta_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main); message(sprintf("✅ Org salvo: %s", org_main))

org_comment_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comment_txt, org_comm); message(sprintf("✅ Org(comment) salvo: %s", org_comm))

