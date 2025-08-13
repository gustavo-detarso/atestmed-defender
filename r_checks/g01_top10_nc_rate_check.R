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

# ----------------------------------------------------------------------
# Args e paths
# ----------------------------------------------------------------------
args <- parse_args()
db_path <- args$db
start_d <- args$start
end_d   <- args$end
min_n   <- as.integer(args[["min-analises"]] %||% "50")
out_dir <- args[["out-dir"]]

if (is.null(db_path) || is.null(start_d) || is.null(end_d)) {
  stop("Uso: --db <path> --start YYYY-MM-DD --end YYYY-MM-DD [--min-analises 50] [--out-dir <dir>]")
}

base_dir <- normalizePath(file.path(dirname(db_path), ".."))
export_dir <- if (!is.null(out_dir)) normalizePath(out_dir, mustWork = FALSE) else
  file.path(base_dir, "graphs_and_tables", "exports")
ensure_dir(export_dir)
outfile <- file.path(export_dir, "rcheck_top10_nc_rate.png")

# ----------------------------------------------------------------------
# Conexão e helpers de schema
# ----------------------------------------------------------------------
con <- dbConnect(RSQLite::SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

table_exists <- function(con, name) {
  nrow(dbGetQuery(con,
                  "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
                  params=list(name))) > 0
}
detect_analises_table <- function(con) {
  for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t)
  stop("Não encontrei 'analises' nem 'analises_atestmed'.")
}

a_tbl <- detect_analises_table(con)
if (!table_exists(con, "indicadores")) {
  ggsave(outfile, fail_plot("Tabela 'indicadores' não encontrada"), width=9, height=5, dpi=150)
  quit(save="no")
}

# ----------------------------------------------------------------------
# Top 10 pelo critério: scoreFinal DESC, mínimo de análises no período
# (usa a tabela detectada de análises)
# ----------------------------------------------------------------------
qry_top10 <- sprintf("
SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
  FROM indicadores i
  JOIN peritos   p ON i.perito = p.siapePerito
  JOIN %s  a ON a.siapePerito = i.perito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
 GROUP BY p.nomePerito, i.scoreFinal
HAVING total_analises >= ?
 ORDER BY i.scoreFinal DESC, total_analises DESC
 LIMIT 10
", a_tbl)

top10 <- dbGetQuery(con, qry_top10, params = list(start_d, end_d, min_n))

if (nrow(top10) == 0) {
  ggsave(outfile, fail_plot("Sem Top 10 para o período/critério"), width=9, height=5, dpi=150)
  quit(save="no")
}

# ----------------------------------------------------------------------
# %NC (robusto) por perito — apenas para os Top 10
# NC robusto:
#   conformado=0  OR  (TRIM(motivoNaoConformado) <> '' AND CAST(motivoNaoConformado AS INTEGER) <> 0)
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
   AND p.nomePerito IN (%s)
 GROUP BY p.nomePerito
", nc_expr, a_tbl, peritos)

df <- dbGetQuery(con, qry_nc, params = list(start_d, end_d))

if (nrow(df) == 0) {
  ggsave(outfile, fail_plot("Sem dados de NC para o Top 10"), width=9, height=5, dpi=150)
  quit(save="no")
}

df <- df %>%
  mutate(pct_nc = ifelse(total > 0, 100*nc/total, 0)) %>%
  arrange(desc(pct_nc)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito))

# ----------------------------------------------------------------------
# Plot
# ----------------------------------------------------------------------
p <- ggplot(df, aes(x = nomePerito, y = pct_nc)) +
  geom_col(fill = "#d62728") +
  geom_text(aes(label = sprintf("%.1f%% (n=%d/%d)", pct_nc, nc, total)),
            vjust = -0.3, size = 3) +
  labs(title = "Top 10 — Taxa de Não Conformidade (NC robusto) [%]",
       subtitle = sprintf("%s a %s | mínimo de análises: %d", start_d, end_d, min_n),
       x = "Perito", y = "% NC (robusto)") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle=45, hjust=1))

# deixa a cabeça com folga para o rótulo
ymax <- max(df$pct_nc, na.rm = TRUE)
ggsave(outfile, p + coord_cartesian(ylim = c(0, ymax * 1.15)),
       width=9, height=5, dpi=150)

