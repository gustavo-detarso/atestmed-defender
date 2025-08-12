#!/usr/bin/env Rscript

library(DBI); library(RSQLite); library(ggplot2); library(dplyr); library(lubridate)

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
thr     <- as.numeric(args[["threshold"]] %||% "15")
out_dir <- args[["out-dir"]]

if (is.null(db_path) || is.null(start_d) || is.null(end_d)) {
  stop("Uso: --db <path> --start YYYY-MM-DD --end YYYY-MM-DD [--min-analises 50] [--threshold 15] [--out-dir <dir>]")
}

base_dir <- normalizePath(file.path(dirname(db_path), ".."))
export_dir <- if (!is.null(out_dir)) normalizePath(out_dir, mustWork = FALSE) else
  file.path(base_dir, "graphs_and_tables", "exports")
ensure_dir(export_dir)
outfile <- file.path(export_dir, "rcheck_top10_le15s.png")

con <- dbConnect(RSQLite::SQLite(), db_path); on.exit(dbDisconnect(con), add = TRUE)

# Descobre coluna de duração em segundos
cols <- dbGetQuery(con, "PRAGMA table_info(analises)")$name
cand <- intersect(cols, c("tempoAnaliseSeg", "tempoAnalise", "duracaoSegundos", "duracao_seg", "tempo_seg"))
dur_col <- if (length(cand) > 0) cand[[1]] else NA_character_

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

if (is.na(dur_col)) {
  ggsave(outfile, fail_plot("Coluna de duração não encontrada em 'analises'"), width=9, height=5, dpi=150); quit(save="no")
}

qry <- sprintf("
SELECT p.nomePerito, a.%s AS dur
  FROM analises a
  JOIN peritos  p ON a.siapePerito = p.siapePerito
 WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
   AND p.nomePerito IN (%s)
", dur_col, start_d, end_d, peritos)

df <- dbGetQuery(con, qry)
if (nrow(df) == 0) {
  ggsave(outfile, fail_plot("Sem dados de duração para o Top 10"), width=9, height=5, dpi=150); quit(save="no")
}

df <- df %>% group_by(nomePerito) %>%
  summarise(total=n(), n_le15 = sum(as.numeric(dur) <= thr, na.rm = TRUE)) %>%
  mutate(pct = ifelse(total>0, 100*n_le15/total, 0)) %>%
  arrange(desc(pct)) %>%
  mutate(nomePerito=factor(nomePerito, levels=nomePerito))

p <- ggplot(df, aes(x=nomePerito, y=pct)) +
  geom_col() +
  geom_text(aes(label=sprintf("%.1f%% (n=%d/%d)", pct, n_le15, total)),
            vjust=-0.3, size=3) +
  labs(title=sprintf("Top 10 — Perícias ≤ %.0fs (%%)", thr),
       subtitle=sprintf("%s a %s | mínimo de análises: %d", start_d, end_d, min_n),
       x="Perito", y="% ≤ 15s") +
  theme_minimal() + theme(axis.text.x = element_text(angle=45, hjust=1))

ggsave(outfile, p, width=9, height=5, dpi=150)

