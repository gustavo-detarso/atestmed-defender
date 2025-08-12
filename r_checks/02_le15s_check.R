#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite); library(dplyr); library(ggplot2); library(scales)
})

opt_list <- list(
  make_option("--db",        type="character"),
  make_option("--start",     type="character"),
  make_option("--end",       type="character"),
  make_option("--perito",    type="character"),
  make_option("--threshold", type="integer", default=15L),
  make_option("--out-dir",   type="character", default=NULL, help="Diretório de saída (PNG)")
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
       SUM( (julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400 <= %d ) AS leT,
       COUNT(*) AS n
FROM analises a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
GROUP BY p.nomePerito
", opt$threshold, opt$start, opt$end)

df <- dbGetQuery(con, sql)
dbDisconnect(con)

stopifnot(opt$perito %in% df$perito)

p_row <- df %>% filter(perito == opt$perito) %>% slice(1)
o_row <- df %>% filter(perito != opt$perito) %>% summarise(leT=sum(leT), n=sum(n))

p_pct <- ifelse(p_row$n > 0, p_row$leT / p_row$n, 0)
o_pct <- ifelse(o_row$n > 0, o_row$leT / o_row$n, 0)

p_ci <- prop.test(p_row$leT, p_row$n)$conf.int
o_ci <- prop.test(o_row$leT, o_row$n)$conf.int

plot_df <- tibble::tibble(
  Grupo = factor(c(opt$perito, "Brasil (excl.)"), levels=c(opt$perito, "Brasil (excl.)")),
  pct   = c(p_pct, o_pct),
  lo    = c(p_ci[1], o_ci[1]),
  hi    = c(p_ci[2], o_ci[2])
)

gg <- ggplot(plot_df, aes(Grupo, pct)) +
  geom_col(fill=c("#9467bd","#1f77b4"), width=.6) +
  geom_errorbar(aes(ymin=lo, ymax=hi), width=.15, size=.4) +
  geom_text(aes(label=scales::percent(pct, accuracy=.1)), vjust=-.4, size=3.3) +
  scale_y_continuous(labels=percent_format(accuracy=1), limits=c(0, max(plot_df$hi, na.rm=TRUE)*1.15)) +
  labs(title=sprintf("Perícias ≤ %ds – Perito vs Brasil (excl.)", opt$threshold),
       subtitle=sprintf("Período: %s a %s  |  n=%d vs %d", opt$start, opt$end, p_row$n, o_row$n),
       y="Percentual", x=NULL) +
  theme_minimal(base_size=11) +
  theme(panel.grid.major.x = element_blank())

out <- file.path(export_dir, sprintf("rcheck_le%ds_%s.png", opt$threshold, perito_safe))
ggsave(out, gg, width=8, height=5, dpi=160)
cat(sprintf("✓ salvo: %s\n", out))

