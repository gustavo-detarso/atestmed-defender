#!/usr/bin/env Rscript

library(DBI); library(RSQLite); library(ggplot2); library(dplyr); library(lubridate); library(purrr)

`%||%` <- function(a,b) if (is.null(a)) b else a

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  kv <- list(); i <- 1
  while (i <= length(args)) {
    k <- args[[i]]
    if (startsWith(k, "--")) {
      v <- if (i + 1 <= length(args) && !startsWith(args[[i+1]], "--")) args[[i+1]] else TRUE
      kv[[substr(k, 3, nchar(k))]] <- v; i <- i + if (isTRUE(v) || identical(v, TRUE)) 1 else 2
    } else i <- i + 1
  }
  kv
}

ensure_dir <- function(p) if (!dir.exists(p)) dir.create(p, recursive = TRUE, showWarnings = FALSE)
fail_plot <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()

args <- parse_args()
db_path <- args$db; start_d <- args$start; end_d <- args$end
min_n   <- as.integer(args[["min-analises"]] %||% "50")
out_dir <- args[["out-dir"]]

if (is.null(db_path) || is.null(start_d) || is.null(end_d)) {
  stop("Uso: --db <path> --start YYYY-MM-DD --end YYYY-MM-DD [--min-analises 50] [--out-dir <dir>]")
}

base_dir <- normalizePath(file.path(dirname(db_path), ".."))
export_dir <- if (!is.null(out_dir)) normalizePath(out_dir, mustWork = FALSE) else
  file.path(base_dir, "graphs_and_tables", "exports")
ensure_dir(export_dir)
outfile <- file.path(export_dir, "rcheck_top10_overlap.png")

con <- dbConnect(RSQLite::SQLite(), db_path); on.exit(dbDisconnect(con), add = TRUE)

# Detecta colunas
cols <- dbGetQuery(con, "PRAGMA table_info(analises)")$name
cand_dur <- intersect(cols, c("tempoAnaliseSeg","tempoAnalise","duracaoSegundos","duracao_seg","tempo_seg"))
dur_col <- if (length(cand_dur)>0) cand_dur[[1]] else NA_character_

# Para fim: preferir dataHoraFimPericia; se não houver, usar ini + duração
has_end <- "dataHoraFimPericia" %in% cols

qry_top10 <- sprintf("
SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
  FROM indicadores i
  JOIN peritos   p ON i.perito = p.siapePerito
  JOIN analises  a ON a.siapePerito = i.perito
 WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
 GROUP BY p.nomePerito, i.scoreFinal
HAVING total_analises >= %d
 ORDER BY i.scoreFinal DESC
 LIMIT 10
", start_d, end_d, min_n)
top10 <- dbGetQuery(con, qry_top10)
if (nrow(top10) == 0) {
  ggsave(outfile, fail_plot("Sem Top 10 para o período/critério"), width=9, height=5, dpi=150); quit(save="no")
}

peritos <- paste(sprintf("'%s'", gsub("'", "''", top10$nomePerito)), collapse=",")

sel_cols <- c("p.nomePerito AS nomePerito", "a.dataHoraIniPericia AS ini")
if (has_end) sel_cols <- c(sel_cols, "a.dataHoraFimPericia AS fim")
if (!has_end && !is.na(dur_col)) sel_cols <- c(sel_cols, sprintf("a.%s AS dur", dur_col))
qry <- sprintf("
SELECT %s
  FROM analises a JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
   AND p.nomePerito IN (%s)
", paste(sel_cols, collapse=", "), start_d, end_d, peritos)

df <- dbGetQuery(con, qry)
if (nrow(df) == 0) {
  ggsave(outfile, fail_plot("Sem timestamps para sobreposição"), width=9, height=5, dpi=150); quit(save="no")
}

df$ini <- ymd_hms(df$ini, quiet=TRUE)
if (has_end) df$fim <- ymd_hms(df$fim, quiet=TRUE)
if (!has_end && "dur" %in% names(df)) df$fim <- df$ini + dseconds(as.numeric(df$dur))

if (!"fim" %in% names(df) || all(!is.finite(df$fim))) {
  ggsave(outfile, fail_plot("Sem dado de fim/duração para calcular sobreposição"), width=9, height=5, dpi=150); quit(save="no")
}

overlap_share <- function(tb) {
  tb <- tb %>% arrange(ini, fim)
  if (nrow(tb) <= 1) return(0)
  # marca uma análise como sobreposta se começo < fim da anterior (na ordenação)
  overl <- logical(nrow(tb))
  last_end <- tb$fim[1]
  for (i in 2:nrow(tb)) {
    overl[i] <- tb$ini[i] < last_end
    last_end <- max(last_end, tb$fim[i], na.rm=TRUE)
  }
  mean(overl, na.rm=TRUE) * 100
}

res <- df %>% group_by(nomePerito) %>% group_modify(~{
  tibble(pct_overlap = overlap_share(.x), total = nrow(.x))
}) %>% arrange(desc(pct_overlap)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito))

p <- ggplot(res, aes(x=nomePerito, y=pct_overlap)) +
  geom_col() +
  geom_text(aes(label=sprintf("%.1f%% (n=%d)", pct_overlap, total)), vjust=-0.3, size=3) +
  labs(title="Top 10 — Tarefas sobrepostas (%)",
       subtitle=sprintf("%s a %s | mínimo de análises: %d", start_d, end_d, min_n),
       x="Perito", y="% sobrepostas") +
  theme_minimal() + theme(axis.text.x = element_text(angle=45, hjust=1))

ggsave(outfile, p, width=9, height=5, dpi=150)

