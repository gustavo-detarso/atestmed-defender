#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite); library(dplyr); library(ggplot2); library(scales); library(lubridate)
})

opt_list <- list(
  make_option("--db",        type="character"),
  make_option("--start",     type="character"),
  make_option("--end",       type="character"),
  make_option("--perito",    type="character"),
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
SELECT p.nomePerito AS perito, a.dataHoraIniPericia AS ini, a.dataHoraFimPericia AS fim
FROM analises a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
ORDER BY p.nomePerito, a.dataHoraIniPericia
", opt$start, opt$end)
df <- dbGetQuery(con, sql)
dbDisconnect(con)

df <- df %>%
  mutate(ini = ymd_hms(ini, quiet=TRUE),
         fim = ymd_hms(fim, quiet=TRUE)) %>%
  filter(!is.na(ini), !is.na(fim), fim >= ini)

has_overlap <- function(dfp) {
  if(nrow(dfp) < 2) return(FALSE)
  dfp <- dfp %>% arrange(ini)
  any(dfp$ini[-1] < dfp$fim[-nrow(dfp)])
}

flag_by_perito <- df %>% group_by(perito) %>% summarise(overlap = has_overlap(cur_data_all()), .groups="drop")

stopifnot(opt$perito %in% flag_by_perito$perito)

p_flag <- flag_by_perito %>% filter(perito==opt$perito) %>% pull(overlap) %>% .[1]
o_rate <- mean((flag_by_perito %>% filter(perito!=opt$perito))$overlap, na.rm=TRUE)

plot_df <- tibble::tibble(
  Grupo=c(opt$perito, "Demais (excl.)"),
  pct=c(ifelse(p_flag,1,0), o_rate)
)

gg <- ggplot(plot_df, aes(Grupo, pct)) +
  geom_col(fill=c("#ff7f0e","#1f77b4"), width=.6) +
  geom_text(aes(label=scales::percent(pct, accuracy=.1)), vjust=-.4, size=3.3) +
  scale_y_continuous(labels=percent_format(accuracy=1), limits=c(0, 1.05*max(plot_df$pct, na.rm=TRUE)+0.02)) +
  labs(title="Sobreposição de tarefas – Perito (indicador de ocorrência) vs Demais",
       subtitle=sprintf("%s a %s", opt$start, opt$end),
       y="Percentual de peritos com sobreposição", x=NULL) +
  theme_minimal(base_size=11) +
  theme(panel.grid.major.x = element_blank())

out <- file.path(export_dir, sprintf("rcheck_overlap_%s.png", perito_safe))
ggsave(out, gg, width=8, height=5, dpi=160)
cat(sprintf("✓ salvo: %s\n", out))

