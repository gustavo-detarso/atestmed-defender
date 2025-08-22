#!/usr/bin/env Rscript

# -*- coding: utf-8 -*-
# KPIs (ICRA, IATD, ScoreFinal) — Top 10 (grupo) vs. Brasil (resto)
# Saídas:
#   - rcheck_top10_kpi_icra_iatd_score.png
#   - rcheck_top10_kpi_icra_iatd_score.org            (imagem + comentário)
#   - rcheck_top10_kpi_icra_iatd_score_comment.org    (apenas comentário)

suppressPackageStartupMessages({
  library(optparse)
  library(DBI); library(RSQLite)
  library(dplyr); library(tidyr); library(forcats)
  library(ggplot2); library(scales); library(stringr)
})

# ==== ATESTMED PROLOGO (INICIO) ====
local({
  .am_loaded <- FALSE
  for (pp in c("r_checks/_common.R","./_common.R","../r_checks/_common.R")) {
    if (file.exists(pp)) { source(pp, local=TRUE); .am_loaded <- TRUE; break }
  }
  if (!.am_loaded) message("[prolog] _common.R não encontrado — usando fallbacks internos.")

  `%||%` <- function(a,b) if (is.null(a)) b else a

  # ---- Fallbacks essenciais (se _common.R não definiu) ----
  if (!exists("am_normalize_cli", mode="function", inherits=TRUE)) {
    am_normalize_cli <<- function(x) as.character(x)
  }
  if (!exists("am_parse_args", mode="function", inherits=TRUE)) {
    am_parse_args <<- function() {
      a <- am_normalize_cli(commandArgs(trailingOnly=TRUE))
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

  # 1) args → lista nomeada (sem rebind de objetos bloqueados)
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
  } else if (is.environment(.raw)) {
    .kv <- utils::modifyList(.kv, as.list(.raw))
  } else if (is.list(.raw)) {
    .kv <- utils::modifyList(.kv, .raw)
  }
  am_args <<- .kv

  # 2) Conexão ao DB
  db_path <- am_args[["db"]]
  if (is.null(db_path) || !nzchar(db_path)) stop("Faltou --db <path>")
  con <<- am_open_db(db_path)

  # Fecha TODAS as conexões SQLite ao sair (remove avisos)
  on.exit({
    try({
      if (exists("con", inherits=TRUE)) try(DBI::dbDisconnect(con), silent=TRUE)
      conns <- try(DBI::dbListConnections(RSQLite::SQLite()), silent=TRUE)
      if (!inherits(conns, "try-error")) for (cc in conns) try(DBI::dbDisconnect(cc), silent=TRUE)
    }, silent=TRUE)
  }, add=TRUE)

  # 3) Paths e schema
  export_dir <<- am_resolve_export_dir(am_args[["out-dir"]])
  a_tbl <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
  cols  <<- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))

  # 4) Args derivados
  start_d   <<- am_args[["start"]]
  end_d     <<- am_args[["end"]]
  min_n     <<- suppressWarnings(as.integer(am_args[["min-analises"]]))
  threshold <<- suppressWarnings(as.numeric(am_args[["threshold"]]))
  measure   <<- as.character(am_args[["measure"]] %||% NA_character_); if (!is.na(measure)) measure <<- measure[[1L]]
  top10     <<- isTRUE(am_args[["top10"]])
  perito    <<- as.character(am_args[["perito"]] %||% NA_character_); if (!is.na(perito)) perito <<- perito[[1L]]

  # 5) Wrapper seguro para consultas (evita "Expected string vector of length 1")
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
  make_option("--db",    type="character", help="Caminho do SQLite (.db)", metavar="FILE"),
  make_option("--start", type="character", help="Data inicial YYYY-MM-DD"),
  make_option("--end",   type="character", help="Data final   YYYY-MM-DD"),
  make_option("--min-analises", type="integer", default = 50L,
              help="Elegibilidade Top 10 [default: %default]"),
  make_option("--out-dir", type="character", default=".", help="Diretório de saída [default: %default]")
)
opt <- parse_args(OptionParser(option_list = option_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end))
if (!dir.exists(opt$`out-dir`)) dir.create(opt$`out-dir`, recursive = TRUE, showWarnings = FALSE)

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

# ───────────────────────── Conexão/colunas ─────────────────────────
con <- dbConnect(RSQLite::SQLite(), opt$db)
# (patched) # (patched) on.exit(try(dbDisconnect(con), silent = TRUE))

if (!table_exists(con, "indicadores")) {
  out_png <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.png")
  ggsave(out_png, fail_plot("Tabela 'indicadores' não encontrada"), width=8.5, height=10, dpi=160)
  quit(save="no")
}

a_tbl <- detect_analises_table(con)

cols_ind <- am_dbGetQuery(con, "PRAGMA table_info(indicadores)")$name
col_icra  <- pick_col(cols_ind, c("ICRA","icra","kpi_icra"))
col_iatd  <- pick_col(cols_ind, c("IATD","iatd","kpi_iatd"))
col_score <- pick_col(cols_ind, c("scoreFinal","score_final","ScoreFinal","score","scorefinal"))

out_png  <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.png")
org_main <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.org")
org_comm <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score_comment.org")

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

# ───────────────────────── Top 10 por Score ─────────────────────────
sql_top10 <- sprintf("
SELECT p.nomePerito AS nomePerito, i.%s AS scoreFinal, COUNT(a.protocolo) AS total_analises
  FROM indicadores i
  JOIN peritos p ON i.perito = p.siapePerito
  JOIN %s a ON a.siapePerito = i.perito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
 GROUP BY p.nomePerito, i.%s
HAVING total_analises >= ?
 ORDER BY i.%s DESC, total_analises DESC
 LIMIT 10;
", col_score, a_tbl, col_score, col_score)

top10 <- am_dbGetQuery(con, sql_top10, params = list(opt$start, opt$end, opt$`min-analises`))
if (nrow(top10) == 0) {
  ggsave(out_png, fail_plot("Nenhum Top 10 para o período/critério."), width=8.5, height=10, dpi=160)
  writeLines("*Sem Top 10 no período/critério informado.*", org_main)
  writeLines("*Sem Top 10 no período/critério informado.*", org_comm)
  quit(save="no")
}
top10_set <- unique(top10$nomePerito)

# ───────────────────────── Coleta KPIs para ativos ─────────────────────────
sql_ativos <- sprintf("
WITH ativos AS (
  SELECT DISTINCT a.siapePerito AS siape
    FROM %s a
   WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
)
SELECT p.nomePerito,
       CAST(i.%s AS REAL) AS icra,
       CAST(i.%s AS REAL) AS iatd,
       CAST(i.%s AS REAL) AS score
  FROM indicadores i
  JOIN peritos p ON i.perito = p.siapePerito
  JOIN ativos  s ON s.siape   = i.perito
", a_tbl, col_icra, col_iatd, col_score)

df <- am_dbGetQuery(con, sql_ativos, params = list(opt$start, opt$end)) %>%
  mutate(grupo = if_else(nomePerito %in% top10_set, "Top10", "Resto"))

if (nrow(df) == 0) {
  ggsave(out_png, fail_plot("Sem peritos ativos no período."), width=8.5, height=10, dpi=160)
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
titulo <- sprintf("Top 10 — KPIs (ICRA, IATD, Score Final) vs. Brasil (resto)\n%s a %s",
                  opt$start, opt$end)

g <- ggplot(long, aes(x = grupo, y = valor, fill = grupo)) +
  geom_boxplot(outlier.shape = NA, width = 0.55) +
  geom_jitter(width = 0.12, alpha = 0.5, size = 1.6) +
  facet_wrap(~kpi, scales = "free_y", ncol = 1) +
  labs(
    title = titulo, x = NULL, y = NULL,
    subtitle = paste(tests$kpi, tests$lbl, collapse = "   |   "),
    caption = sprintf("Top 10 por %s (desc), exigindo ao menos %d análises no período. Teste: Mann-Whitney por KPI.",
                      col_score, opt$`min-analises`)
  ) +
  theme_minimal(base_size = 11) +
  theme(panel.grid.minor = element_blank(),
        legend.position = "none")

ggsave(out_png, g, width = 8.5, height = 10, dpi = 160)
message(sprintf("✅ Figura salva: %s", out_png))

# ───────────────────────── Comentários (.org) ─────────────────────────
# Método
metodo_txt <- paste0(
  "*Método.* Selecionamos os *Top 10 piores* por *", col_score, "* em `indicadores`, ",
  "exigindo ao menos *", opt$`min-analises`, "* análises no período (", opt$start, " a ", opt$end, "). ",
  "Definimos o grupo *Resto* como peritos *ativos* no período (possuem alguma análise em ", shQuote(a_tbl), "). ",
  "Comparamos *ICRA*, *IATD* e *Score Final* entre *Top10* e *Resto* usando o teste *Mann-Whitney* (não-paramétrico)."
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
  "Leve em conta o tamanho amostral de cada grupo e a direção esperada de cada métrica no seu contexto.\n",
  paste(kpi_lines, collapse = "\n")
)

# .org principal (imagem + texto)
org_main_txt <- paste(
  "#+CAPTION: KPIs (ICRA, IATD, Score Final) — Top 10 vs. Brasil (resto)",
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
