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
  library(dplyr); library(ggplot2); library(stringr); library(forcats); library(scales); library(readr)
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
if (!exists("am_dbQuoteIdentifier", mode="function", inherits=TRUE)) {
  am_dbQuoteIdentifier <- function(con, x) {
    DBI::dbQuoteIdentifier(DBI::ANSI(), x)
  }
}

# --- end: am_db_reconnect_helpers ---







#!/usr/bin/env Rscript
# -*- coding: utf-8 -*-

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite)
  library(dplyr); library(ggplot2); library(stringr); library(forcats); library(scales); library(readr)
})

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
      if (!is.null(out_dir) && nzchar(out_dir)) normalizePath(out_dir, mustWork=FALSE) else {
        dbp <- am_args[["db"]] %||% ""; base_dir <- if (nzchar(dbp)) normalizePath(file.path(dirname(dbp), ".."), mustWork=FALSE) else getwd()
        od <- file.path(base_dir, "graphs_and_tables", "exports"); if (!dir.exists(od)) dir.create(od, recursive=TRUE, showWarnings=FALSE); od
      }
    }
  }
  if (!exists("am_detect_analises_table", mode="function", inherits=TRUE)) {
    am_detect_analises_table <<- function(con) {
      has <- function(nm) nrow(am_dbGetQuery(con,"SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1", params=list(nm)))>0
      for (t in c("analises","analises_atestmed")) if (has(t)) return(t)
      stop("Não encontrei 'analises' nem 'analises_atestmed'.")
    }
  }
  if (!exists("am_detect_columns", mode="function", inherits=TRUE)) {
    am_detect_columns <<- function(con, tbl) { if (is.na(tbl)||!nzchar(tbl)) return(character(0)); am_dbGetQuery(con, sprintf("PRAGMA table_info(%s)", tbl))$name }
  }
  am_dbGetQuery <<- (function(.f){ force(.f); function(con, statement, ...){ st <- statement; if (length(st)!=1L) st <- paste(st, collapse=" "); .f(con, st, ...) } })(DBI::dbGetQuery)
  am_args <<- tryCatch(am_parse_args(), error=function(e) list())
  db_path <- am_args[["db"]]; if (is.null(db_path)||!nzchar(db_path)) stop("Faltou --db <path>")
  con <<- am_open_db(db_path)
  # (patch) removido on.exit de desconexão precoce
  export_dir <<- am_resolve_export_dir(am_args[["out-dir"]])
  a_tbl     <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
  am_cols   <<- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))  # <- renomeado para não colidir com readr::cols
})
# ==== ATESTMED PROLOGO (FIM) ====

# ── CLI ────────────────────────────────────────────────────────────────────────
option_list <- list(
  make_option("--db",        type="character"),
  make_option("--start",     type="character"),
  make_option("--end",       type="character"),
  make_option("--perito",    type="character"),
  make_option("--out-dir",   type="character", default=NULL),
  make_option("--min-count", type="integer",  default=5L),
  make_option("--topn",      type="integer",  default=12L),
  make_option("--scope-csv", type="character", default=NULL, help="CSV com nomes do escopo p/ compor 'Demais'")
)
opt <- optparse::parse_args(OptionParser(option_list = option_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))
out_dir <- am_resolve_export_dir(opt$`out-dir`)
if (!dir.exists(out_dir)) dir.create(out_dir, recursive=TRUE, showWarnings=FALSE)

# ── Helpers ────────────────────────────────────────────────────────────────────
safe_slug <- function(x){ x <- gsub("[^A-Za-z0-9\\-_]+","_",x); x <- gsub("_+","_",x); x <- gsub("^_|_$","",x); ifelse(nchar(x)>0,x,"output") }
percent_s <- function(x, acc=.1) ifelse(is.finite(x), scales::percent(x, accuracy=acc), "NA")
table_exists <- function(con, name) nrow(am_dbGetQuery(con, "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1", params=list(name)))>0
detect_analises_table <- function(con){ for (t in c("analises","analises_atestmed")) if (table_exists(con,t)) return(t); stop("Não encontrei 'analises' nem 'analises_atestmed'.") }
lump_rare <- function(tbl, min_count=5L){ tbl %>% mutate(motivo=if_else(n<min_count,"OUTROS",motivo)) %>% group_by(motivo) %>% summarise(n=sum(n), .groups="drop") }
load_scope <- function(path){
  if (is.null(path)||!nzchar(path)||!file.exists(path)) return(NULL)
  df <- tryCatch(readr::read_csv(path, show_col_types=FALSE), error=function(e) NULL)
  if (is.null(df)||!nrow(df)) return(NULL)
  key <- if ("nomePerito" %in% names(df)) "nomePerito" else names(df)[1]
  u <- unique(trimws(as.character(df[[key]]))); u[nzchar(u)]
}

# ── Dados (NC robusto) ────────────────────────────────────────────────────────
a_tbl <- detect_analises_table(con)
has_protocolos <- table_exists(con, "protocolos")
desc_expr <- if (has_protocolos) {
  "COALESCE(NULLIF(TRIM(pr.motivo), ''), CAST(IFNULL(a.motivoNaoConformado,'') AS TEXT)) AS motivo_text"
} else {
  "CAST(IFNULL(a.motivoNaoConformado,'') AS TEXT) AS motivo_text"
}
join_prot <- if (has_protocolos) "LEFT JOIN protocolos pr ON pr.protocolo = a.protocolo" else ""

nc_expr <- "
CASE
  WHEN CAST(IFNULL(a.conformado,1) AS INTEGER)=0 THEN 1
  WHEN TRIM(IFNULL(a.motivoNaoConformado,'')) <> '' AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0 THEN 1
  ELSE 0
END
"

scope <- load_scope(opt$`scope-csv`)
if (!is.null(scope)) scope <- unique(c(scope, opt$perito))  # garante perito no escopo
in_clause <- if (is.null(scope)) "" else sprintf(" AND p.nomePerito IN (%s) ", paste(sprintf("'%s'", gsub("'", "''", scope)), collapse=","))

sql_nc <- sprintf("
SELECT p.nomePerito AS perito, %s
FROM %s a
JOIN peritos p ON a.siapePerito = p.siapePerito
%s
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
  AND (%s) = 1
  %s
;", desc_expr, am_dbQuoteIdentifier(con, a_tbl), join_prot, nc_expr, in_clause)

all_nc <- am_dbGetQuery(con, sql_nc, params=list(opt$start, opt$end)) %>%
  mutate(motivo_text = as.character(motivo_text),
         motivo = if_else(is.na(motivo_text) | trimws(motivo_text)=="" | trimws(motivo_text)=="0",
                          "MOTIVO_DESCONHECIDO", trimws(motivo_text))) %>%
  select(perito, motivo)

if (!nrow(all_nc)) { message("Nenhuma análise NC (robusto) no período/escopo."); quit(save="no", status=0) }

perito_alvo <- opt$perito
if (!(perito_alvo %in% all_nc$perito)) {
  sim <- unique(all_nc$perito[grepl(perito_alvo, all_nc$perito, ignore.case=TRUE)])
  msg <- if (length(sim)) paste0(" Peritos semelhantes: ", paste(sim, collapse=", "), ".") else ""
  stop(sprintf("Perito '%s' sem NC no período/escopo (ou não encontrado).%s", perito_alvo, msg))
}

tab_perito <- all_nc %>% filter(perito == perito_alvo) %>% count(motivo, name="n_p")
tab_outros <- all_nc %>% filter(perito != perito_alvo) %>% count(motivo, name="n_o")
if (!nrow(tab_perito)) { message("Perito sem NC (robusto) no período."); quit(save="no", status=0) }

base_join <- full_join(tab_perito, tab_outros, by="motivo") %>%
  mutate(across(c(n_p,n_o), ~ dplyr::coalesce(.x, 0L))) %>% arrange(desc(n_p+n_o))

base_join <- base_join %>%
  rename(n = n_p) %>% select(motivo, n, n_o) %>%
  lump_rare(min_count = opt$`min-count`) %>%
  rename(n_p = n) %>% left_join(base_join %>% select(motivo, n_o), by="motivo") %>%
  mutate(n_o = dplyr::coalesce(n_o, 0L)) %>% arrange(desc(n_p+n_o))

total_p <- sum(base_join$n_p); total_o <- sum(base_join$n_o)
if (total_p == 0 || total_o == 0) { message("Sem dados suficientes para qui-quadrado."); quit(save="no", status=0) }

mat <- rbind(Perito = base_join$n_p, Outros = base_join$n_o)
chs <- suppressWarnings(chisq.test(mat))
if (any(chs$expected < 5, na.rm=TRUE)) chs <- suppressWarnings(chisq.test(mat, simulate.p.value=TRUE, B=5000))
pval <- chs$p.value

resumo <- base_join %>%
  mutate(prop_p = n_p/total_p, prop_o = n_o/total_o, diff = prop_p - prop_o) %>%
  arrange(desc(abs(diff))) %>% slice_head(n = opt$topn) %>%
  mutate(motivo_plot = forcats::fct_reorder(motivo, diff))

# ── Plot ───────────────────────────────────────────────────────────────────────
scope_lbl <- if (is.null(scope)) "Demais (excl.)" else "Demais (escopo, excl.)"
titulo <- sprintf("Motivos NC (robusto) — %s vs. %s\n%s a %s  |  χ² p=%.3g",
                  perito_alvo, scope_lbl, opt$start, opt$end, pval)

g <- ggplot(resumo, aes(x = motivo_plot, y = diff)) +
  geom_hline(yintercept = 0, linewidth = 0.4) +
  geom_col() + coord_flip() +
  labs(title = titulo, x = NULL, y = "Diferença de proporções (Perito − Demais)",
       caption = "NC robusto: conformado=0 OU (motivoNaoConformado ≠ '' E CAST(motivoNaoConformado) ≠ 0).") +
  theme_minimal(base_size = 11) +
  theme(panel.grid.minor = element_blank(), plot.title = element_text(face="bold", hjust=0))

perito_safe <- safe_slug(perito_alvo)
png_path <- file.path(out_dir, sprintf("rcheck_motivos_chisq_%s.png", perito_safe))
ggsave(png_path, g, width=10, height=6, dpi=160); message(sprintf("✅ Figura salva: %s", png_path))

# ── Comentários (.org) ─────────────────────────────────────────────────────────
pos_tbl <- resumo %>% filter(is.finite(diff), diff > 0) %>% arrange(desc(diff)) %>%
  transmute(txt = sprintf("%s (+%s p.p.)", motivo, percent_s(diff, .1)))
neg_tbl <- resumo %>% filter(is.finite(diff), diff < 0) %>% arrange(diff) %>%
  transmute(txt = sprintf("%s (%s p.p.)", motivo, percent_s(diff, .1)))

metodo_txt <- paste0(
  "*Método.* Construímos uma tabela motivo × grupo (Perito vs Demais), ",
  "após agrupar motivos raros (< ", opt$`min-count`, ") em 'OUTROS'. ",
  "Teste *qui-quadrado* global (χ²) com simulação se há esperados <5. ",
  "No gráfico, mostramos os ", min(nrow(resumo), opt$topn),
  " motivos com maior |diferença| (Perito − Demais).",
  if (!is.null(scope)) " (Demais restrito via --scope-csv.)" else ""
)

interpreta_txt <- {
  sig <- if (is.finite(pval) && pval < 0.05) "diferenças *estatisticamente significativas*" else "diferenças não significativas ao nível 5%"
  pos_str <- if (nrow(pos_tbl)) paste("- Mais frequentes no perito:", paste(pos_tbl$txt, collapse=", "), ".") else NULL
  neg_str <- if (nrow(neg_tbl)) paste("- Menos frequentes no perito:", paste(neg_tbl$txt, collapse=", "), ".") else NULL
  paste0(
    "*Interpretação.* O teste global indica ", sig, " (p = ", formatC(pval, format="fg", digits=3), "). ",
    "Barras *positivas* = mais comuns no perito; *negativas* = menos comuns. ",
    "Use como *pistas* para auditoria qualitativa, considerando volume e contexto.\n",
    paste(na.omit(c(pos_str, neg_str)), collapse = "\n")
  )
}

org_main <- file.path(out_dir, sprintf("rcheck_motivos_chisq_%s.org", perito_safe))
writeLines(paste(
  "#+CAPTION: Motivos de NC (robusto) — Diferença de proporções (Perito − Demais)",
  sprintf("[[file:%s]]", basename(png_path)), "", metodo_txt, "", interpreta_txt, "", sep="\n"
), org_main); message(sprintf("✅ Org salvo: %s", org_main))

org_comment <- file.path(out_dir, sprintf("rcheck_motivos_chisq_%s_comment.org", perito_safe))
writeLines(paste(metodo_txt, "", interpreta_txt, "", sep="\n"), org_comment); message(sprintf("✅ Org(comment) salvo: %s", org_comment))
