#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite); library(dplyr); library(ggplot2); library(scales)
})

opt_list <- list(
  make_option("--db",          type="character"),
  make_option("--start",       type="character"),
  make_option("--end",         type="character"),
  make_option("--perito",      type="character"),
  make_option("--threshold",   type="double", default=50),
  make_option("--out-dir",     type="character", default=NULL, help="Diretório de saída (PNG)")
)
opt <- parse_args(OptionParser(option_list = opt_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))

safe <- function(x) { gsub("(^_+|_+$)", "", gsub("[^A-Za-z0-9_-]+","_", x)) }
perito_safe <- safe(opt$perito)

base_dir   <- normalizePath(file.path(dirname(opt$db), ".."))
export_dir <- if (!is.null(opt$`out-dir`)) normalizePath(opt$`out-dir`, mustWork = FALSE) else
              file.path(base_dir, "graphs_and_tables", "exports")
dir.create(export_dir, showWarnings = FALSE, recursive = TRUE)

con <- dbConnect(SQLite(), opt$db)

sql <- sprintf("
SELECT p.nomePerito AS perito,
       COUNT(*) AS total,
       SUM((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400) AS segs
FROM peritos p
JOIN analises a ON p.siapePerito = a.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
GROUP BY p.nomePerito
", opt$start, opt$end)

df <- dbGetQuery(con, sql)
dbDisconnect(con)

df <- df %>% mutate(prod = ifelse(segs>0, total/(segs/3600), NA_real_))
stopifnot(opt$perito %in% df$perito)

p_val <- df %>% filter(perito==opt$perito) %>% pull(prod) %>% .[1]

gg <- ggplot(df %>% filter(is.finite(prod)), aes(prod)) +
  geom_histogram(bins=40, fill="#1f77b4", alpha=.85) +
  geom_vline(xintercept = p_val, color="#d62728", size=1) +
  geom_vline(xintercept = opt$threshold, color="#2ca02c", linetype="dashed") +
  labs(title="Produtividade (análises/h) — distribuição nacional",
       subtitle=sprintf("%s a %s | perito=%s (%.2f/h) | threshold=%.0f/h",
                        opt$start, opt$end, opt$perito, p_val, opt$threshold),
       x="análises/h", y="freq.") +
  theme_minimal(base_size=11)

out <- file.path(export_dir, sprintf("rcheck_productivity_%s.png", perito_safe))
ggsave(out, gg, width=8, height=5, dpi=160)
cat(sprintf("✓ salvo: %s\n", out))

