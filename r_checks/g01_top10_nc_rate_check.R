#!/usr/bin/env Rscript

library(DBI)
library(RSQLite)
library(ggplot2)
library(dplyr)
library(stringr)

`%||%` <- function(a,b) if (is.null(a)) b else a

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  kv <- list()
  i <- 1
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

fail_plot <- function(msg) {
  ggplot() + annotate("text", x=0, y=0, label=msg, size=5) +
    theme_void()
}

args <- parse_args()
db_path <- args$db
start_d <- args$start
end_d   <- args$end
min_n   <- as.integer(args[["min-analises"]] %||% "50")
out_dir <- args[["out-dir"]]

if (is.null(db_path) || is.null(start_d) || is.null(end_d)) {
  stop("Uso: --db <path> --start YYYY-MM-DD --end YYYY-MM-DD [--min-analises 50] [--out-dir <dir>]")
}

# Deriva BASE/exports a partir do caminho do DB (fallback)
base_dir <- normalizePath(file.path(dirname(db_path), ".."))
export_dir <- if (!is.null(out_dir)) normalizePath(out_dir, mustWork = FALSE) else
  file.path(base_dir, "graphs_and_tables", "exports")
ensure_dir(export_dir)
outfile <- file.path(export_dir, "rcheck_top10_nc_rate.png")

con <- dbConnect(RSQLite::SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

# Top10 pelo mesmo critério do Python (scoreFinal DESC, mínimo de análises)
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
  ggsave(outfile, fail_plot("Sem Top 10 para o período/critério"), width=9, height=5, dpi=150)
  quit(save="no")
}

# %NC por perito (Top 10)
peritos <- paste(sprintf("'%s'", gsub("'", "''", top10$nomePerito)), collapse=",")
qry_nc <- sprintf("
SELECT p.nomePerito,
       COUNT(*) AS total,
       SUM(CASE WHEN a.motivoNaoConformado != 0 THEN 1 ELSE 0 END) AS nc
  FROM analises a
  JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
   AND p.nomePerito IN (%s)
 GROUP BY p.nomePerito
", start_d, end_d, peritos)

df <- dbGetQuery(con, qry_nc)
if (nrow(df) == 0) {
  ggsave(outfile, fail_plot("Sem dados de NC para o Top 10"), width=9, height=5, dpi=150)
  quit(save="no")
}

df <- df %>% mutate(pct_nc = ifelse(total > 0, 100*nc/total, 0)) %>%
  arrange(desc(pct_nc)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito))

p <- ggplot(df, aes(x = nomePerito, y = pct_nc)) +
  geom_col() +
  geom_text(aes(label = sprintf("%.1f%% (n=%d/%d)", pct_nc, nc, total)),
            vjust = -0.3, size = 3) +
  labs(title = "Top 10 — Taxa de Não Conformidade (%)",
       subtitle = sprintf("%s a %s | mínimo de análises: %d", start_d, end_d, min_n),
       x = "Perito", y = "% NC") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle=45, hjust=1))

ggsave(outfile, p, width=9, height=5, dpi=150)

