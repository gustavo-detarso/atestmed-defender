#!/usr/bin/env Rscript
# ---- Auto-normalized header (injected by run_r_checks_autofix.py) ----
options(stringsAsFactors = FALSE)
options(encoding = "UTF-8")
options(warn = 1)
options(repos = c(CRAN = "https://cloud.r-project.org"))
Sys.setlocale(category = "LC_ALL", locale = "C.UTF-8")
# ----------------------------------------------------------------------

# -*- coding: utf-8 -*-
# KPIs (ICRA, IATD, ScoreFinal) — Perito vs. Brasil (resto)
# Saídas:
#   - rcheck_kpi_icra_iatd_score_<SAFE>.png
#   - rcheck_kpi_icra_iatd_score_<SAFE>.org
#   - rcheck_kpi_icra_iatd_score_<SAFE>_comment.org
#   - rcheck_kpi_icra_iatd_score_<SAFE>.md

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
  library(ggplot2); library(scales); library(stringr)
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
      if (!is.null(out_dir) && nzchar(out_dir)) {
        od <- normalizePath(out_dir, mustWork=FALSE)
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

  # am_dbGetQuery deve existir ANTES de qualquer uso
  if (!exists("am_dbGetQuery", mode="function", inherits=TRUE)) {
    am_dbGetQuery <<- (function(.f) { force(.f); function(con, statement, ...) {
      st <- statement; if (length(st) != 1L) st <- paste(st, collapse=" "); .f(con, st, ...)
    } })(DBI::dbGetQuery)
  }

  # args + conexão única
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

  db_path <- am_args[["db"]]; if (is.null(db_path) || !nzchar(db_path)) stop("Faltou --db <path>")
  con <<- am_open_db(db_path)

  # (patch) removido on.exit de desconexão precoce

  export_dir <<- am_resolve_export_dir(am_args[["out-dir"]])
  a_tbl     <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
  cols <- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))
})
# ==== ATESTMED PROLOGO (FIM) ====


# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
option_list <- list(
  make_option("--db",       type="character", help="Caminho do SQLite (.db)", metavar="FILE"),
  make_option("--start",    type="character", help="Data inicial YYYY-MM-DD"),
  make_option("--end",      type="character", help="Data final   YYYY-MM-DD"),
  make_option("--perito",   type="character", help="Nome do perito"),
  make_option("--out-dir",  type="character", default=NULL, help="Diretório de saída (PNG/ORG)")
)
opt <- optparse::parse_args(OptionParser(option_list = option_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
safe_slug <- function(x) { x <- gsub("[^A-Za-z0-9\\-_]+","_",x); x <- gsub("_+","_",x); x <- gsub("^_|_$","",x); ifelse(nchar(x)>0,x,"output") }
percent_s <- function(x, acc=.1) ifelse(is.finite(x), scales::percent(x, accuracy=acc), "NA")
fail_plot <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()
pick_col <- function(cols, candidates) {
  for (c in candidates) if (c %in% cols) return(c)
  low <- tolower(cols)
  for (c in candidates) { idx <- which(low == tolower(c)); if (length(idx)==1) return(cols[[idx]]) }
  for (c in candidates) { hit <- grep(paste0("^",tolower(c),"$|",tolower(c)), low, value=FALSE); if (length(hit)) return(cols[[hit[1]]]) }
  NA_character_
}

# Deriva export_dir pelo prólogo (respeita --out-dir)
export_dir <- am_resolve_export_dir(opt$`out-dir`)
if (!dir.exists(export_dir)) dir.create(export_dir, recursive=TRUE, showWarnings=FALSE)
safe <- safe_slug(opt$perito)

png_base <- file.path(export_dir, sprintf("rcheck_kpi_icra_iatd_score_%s.png", safe))
org_main <- file.path(export_dir, sprintf("rcheck_kpi_icra_iatd_score_%s.org", safe))
org_comm <- file.path(export_dir, sprintf("rcheck_kpi_icra_iatd_score_%s_comment.org", safe))
md_out   <- file.path(export_dir, sprintf("rcheck_kpi_icra_iatd_score_%s.md", safe))

write_failure_orgs <- function(msg) {
  main_txt <- paste(
    "#+CAPTION: KPIs — ICRA, IATD e Score Final (distribuição) — mensagem do script",
    sprintf("[[file:%s]]", basename(png_base)),
    "",
    sprintf("*Método.* Este apêndice compara a posição do perito (%s) frente à distribuição nacional dos KPIs ICRA, IATD e Score Final, no período %s a %s. Por indisponibilidade de dados/colunas, apenas a mensagem de erro é exibida.",
            opt$perito, opt$start, opt$end),
    "",
    paste0("*Interpretação.* ", msg),
    "",
    sep = "\n"
  )
  writeLines(main_txt, org_main, useBytes=TRUE)
  writeLines(paste0(msg, "\n"), md_out, useBytes=TRUE)
  writeLines(paste(
    paste0("*Método.* Este apêndice compara a posição do perito (", opt$perito, ") frente à distribuição nacional dos KPIs ICRA, IATD e Score Final, no período ", opt$start, " a ", opt$end, "."),
    paste0("*Interpretação.* ", msg),
    "",
    sep = "\n"
  ), org_comm, useBytes=TRUE)
}

# ────────────────────────────────────────────────────────────────────────────────
# Colunas de indicadores
# ────────────────────────────────────────────────────────────────────────────────
cols_ind <- am_dbGetQuery(con, "PRAGMA table_info(indicadores)")$name
if (length(cols_ind) == 0) {
  ggsave(png_base, fail_plot("Tabela 'indicadores' não encontrada"), width=10, height=6, dpi=160)
  write_failure_orgs("Tabela 'indicadores' não encontrada no banco.")
  quit(save="no")
}
col_icra  <- pick_col(cols_ind, c("ICRA","icra","kpi_icra"))
col_iatd  <- pick_col(cols_ind, c("IATD","iatd","kpi_iatd"))
col_score <- pick_col(cols_ind, c("scoreFinal","score_final","ScoreFinal","score","scorefinal"))

needed <- c(col_icra, col_iatd, col_score)
if (any(is.na(needed))) {
  miss <- c("ICRA"=is.na(col_icra), "IATD"=is.na(col_iatd), "Score"=is.na(col_score))
  msg <- paste("Colunas ausentes em 'indicadores':", paste(names(miss)[miss], collapse=", "))
  ggsave(png_base, fail_plot(msg), width=10, height=6, dpi=160)
  write_failure_orgs(msg)
  quit(save="no")
}

# ────────────────────────────────────────────────────────────────────────────────
# Coleta de peritos ativos (Brasil) no período — usa a_tbl detectada
# ────────────────────────────────────────────────────────────────────────────────
q_a_tbl <- am_dbQuoteIdentifier(con, a_tbl)
sql_ativos <- sprintf("
WITH ativos AS (
  SELECT DISTINCT a.siapePerito AS siape
    FROM %s a
   WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
)
SELECT p.siapePerito AS siape, p.nomePerito,
       CAST(i.%s AS REAL) AS icra,
       CAST(i.%s AS REAL) AS iatd,
       CAST(i.%s AS REAL) AS score
  FROM indicadores i
  JOIN peritos p ON i.perito = p.siapePerito
  JOIN ativos  s ON s.siape  = i.perito
", q_a_tbl, col_icra, col_iatd, col_score)

resto_df <- am_dbGetQuery(con, sql_ativos, params=list(opt$start, opt$end)) %>%
  mutate(across(c(icra,iatd,score), as.numeric))

if (nrow(resto_df) == 0) {
  ggsave(png_base, fail_plot("Sem peritos ativos no período."), width=10, height=6, dpi=160)
  write_failure_orgs("Sem peritos ativos no período para compor a distribuição nacional.")
  quit(save="no")
}

# Separa perito alvo
this <- resto_df %>% filter(nomePerito == opt$perito) %>% slice_tail(n=1)
if (nrow(this) == 0) {
  ggsave(png_base, fail_plot("Perito sem indicador em 'indicadores'."), width=10, height=6, dpi=160)
  write_failure_orgs("Perito sem registro correspondente em 'indicadores' no período.")
  quit(save="no")
}

resto <- resto_df %>% filter(nomePerito != opt$perito)

if (nrow(resto) == 0) {
  ggsave(png_base, fail_plot("Sem 'resto' (apenas o perito-alvo ativo no período)."), width=10, height=6, dpi=160)
  write_failure_orgs("Não há outros peritos ativos além do perito-alvo para formar a distribuição nacional.")
  quit(save="no")
}

# ────────────────────────────────────────────────────────────────────────────────
# Distribuições e percentis
# ────────────────────────────────────────────────────────────────────────────────
dist <- resto %>%
  select(icra, iatd, score) %>%
  pivot_longer(everything(), names_to="kpi", values_to="valor") %>%
  mutate(kpi = recode(kpi, icra="ICRA", iatd="IATD", score="Score Final")) %>%
  filter(is.finite(valor))

if (nrow(dist) == 0) {
  ggsave(png_base, fail_plot("Sem valores numéricos no 'resto' para construir densidades."), width=10, height=6, dpi=160)
  write_failure_orgs("Os KPIs do 'resto' não possuem valores numéricos válidos no período.")
  quit(save="no")
}

calc_percentil <- function(x, v) { x <- x[is.finite(x)]; if (!is.finite(v) || !length(x)) return(NA_real_); stats::ecdf(x)(v) * 100 }

p_icra  <- calc_percentil(resto$icra,  this$icra)
p_iatd  <- calc_percentil(resto$iatd,  this$iatd)
p_score <- calc_percentil(resto$score, this$score)

this_long <- tibble::tibble(
  kpi   = c("ICRA","IATD","Score Final"),
  valor = c(this$icra, this$iatd, this$score),
  pctl  = c(p_icra,    p_iatd,     p_score)
)

# ────────────────────────────────────────────────────────────────────────────────
# Plot
# ────────────────────────────────────────────────────────────────────────────────
titulo <- sprintf("KPIs — ICRA, IATD e Score Final\n%s vs. Brasil (resto) | %s a %s",
                  opt$perito, opt$start, opt$end)

g <- ggplot(dist, aes(x = valor)) +
  geom_density(fill="grey80", color=NA, alpha=0.8, adjust=1.2, na.rm=TRUE) +
  geom_vline(data=this_long, aes(xintercept=valor), linewidth=0.7) +
  geom_text(data=this_long,
            aes(x=valor, y=0, label=sprintf("valor=%.3f\npctl=%.1f%%", valor, pctl)),
            vjust=-0.5, size=3.3) +
  facet_wrap(~kpi, scales="free", ncol=1) +
  labs(title=titulo, x=NULL, y="Densidade (Brasil, exceto alvo)") +
  theme_minimal(base_size=11) +
  theme(panel.grid.minor = element_blank())

ggsave(png_base, g, width=9, height=9, dpi=160)
message(sprintf("✅ Figura salva: %s", png_base))

# ────────────────────────────────────────────────────────────────────────────────
# Comentários (.org) e Markdown
# ────────────────────────────────────────────────────────────────────────────────
icra_txt  <- ifelse(is.finite(this$icra),  sprintf("%.3f", this$icra),  "NA")
iatd_txt  <- ifelse(is.finite(this$iatd),  sprintf("%.3f", this$iatd),  "NA")
score_txt <- ifelse(is.finite(this$score), sprintf("%.3f", this$score), "NA")
p_icra_txt  <- ifelse(is.finite(p_icra),  sprintf("%.1f%%", p_icra),  "NA")
p_iatd_txt  <- ifelse(is.finite(p_iatd),  sprintf("%.1f%%", p_iatd),  "NA")
p_score_txt <- ifelse(is.finite(p_score), sprintf("%.1f%%", p_score), "NA")

metodo_txt <- paste0(
  "*Método.* Para o período ", opt$start, "–", opt$end,
  ", comparamos a posição do perito (", opt$perito, ") na distribuição nacional dos KPIs ",
  "ICRA, IATD e Score Final. Construímos curvas de densidade com todos os peritos ativos ",
  "no período (Brasil, exceto o perito-alvo), e marcamos o valor individual do perito com ",
  "uma linha vertical. Também calculamos o *percentil* do valor do perito em cada distribuição. ",
  "Os percentis indicam a fração de peritos com valores menores ou iguais ao observado; ",
  "interpretações de 'melhor/pior' dependem da convenção institucional de cada KPI."
)

interpreta_txt <- paste0(
  "*Interpretação.* Valores do perito: ICRA=", icra_txt, " (pctl=", p_icra_txt, "); ",
  "IATD=", iatd_txt, " (pctl=", p_iatd_txt, "); ",
  "Score Final=", score_txt, " (pctl=", p_score_txt, "). ",
  "Percentis mais altos significam que o valor do perito está mais à direita na distribuição nacional. ",
  "Use esta leitura em conjunto com o significado operacional de cada KPI para priorizar ações."
)

# .org (imagem + texto)
org_main_txt <- paste(
  "#+CAPTION: KPIs — ICRA, IATD e Score Final (distribuição nacional e posição do perito)",
  sprintf("[[file:%s]]", basename(png_base)),
  "",
  metodo_txt, "",
  interpreta_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main, useBytes=TRUE); message(sprintf("📝 Org salvo: %s", org_main))

# .org (apenas comentário)
org_comm_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comm_txt, org_comm, useBytes=TRUE); message(sprintf("📝 Org(comment) salvo: %s", org_comm))

# Retrocompat: resumo em Markdown
sum_md <- sprintf(
  paste0("*KPIs — %s (%s a %s)*\n",
         "- ICRA: %s  (percentil no Brasil: %s)\n",
         "- IATD: %s  (percentil no Brasil: %s)\n",
         "- Score Final: %s  (percentil no Brasil: %s)\n"),
  opt$perito, opt$start, opt$end,
  icra_txt,  p_icra_txt,
  iatd_txt,  p_iatd_txt,
  score_txt, p_score_txt
)
writeLines(sum_md, md_out, useBytes=TRUE); message(sprintf("📝 Markdown salvo: %s", md_out))
