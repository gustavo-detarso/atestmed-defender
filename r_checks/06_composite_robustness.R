#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite); library(dplyr); library(ggplot2); library(scales)
})

opt_list <- list(
  make_option("--db",              type="character"),
  make_option("--start",           type="character"),
  make_option("--end",             type="character"),
  make_option("--perito",          type="character"),
  make_option("--prod-threshold",  type="double", default=50),
  make_option("--le-threshold",    type="integer", default=15),
  make_option("--out-dir",         type="character", default=NULL, help="Diretório de saída (PNG)")
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

# NC rate
sql_nc <- sprintf("
SELECT p.nomePerito AS perito,
       SUM(CASE WHEN a.motivoNaoConformado != 0 THEN 1 ELSE 0 END) AS nc,
       COUNT(*) AS n
FROM analises a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
GROUP BY p.nomePerito
", opt$start, opt$end)
df_nc <- dbGetQuery(con, sql_nc) %>% mutate(nc_rate = ifelse(n>0, nc/n, NA_real_))

# le15s
sql_le <- sprintf("
SELECT p.nomePerito AS perito,
       SUM( (julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400 <= %d ) AS le,
       COUNT(*) AS n
FROM analises a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
GROUP BY p.nomePerito
", opt$`le-threshold`, opt$start, opt$end)
df_le <- dbGetQuery(con, sql_le) %>% mutate(le_rate = ifelse(n>0, le/n, NA_real_))

# produtividade
sql_pd <- sprintf("
SELECT p.nomePerito AS perito,
       COUNT(*) AS total,
       SUM((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400) AS segs
FROM peritos p
JOIN analises a ON p.siapePerito = a.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
GROUP BY p.nomePerito
", opt$start, opt$end)
df_pd <- dbGetQuery(con, sql_pd) %>% mutate(prod = ifelse(segs>0, total/(segs/3600), NA_real_))

# overlap flag
sql_ov <- sprintf("
SELECT p.nomePerito AS perito, a.dataHoraIniPericia AS ini, a.dataHoraFimPericia AS fim
FROM analises a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
ORDER BY p.nomePerito, a.dataHoraIniPericia
", opt$start, opt$end)
df_ov_raw <- dbGetQuery(con, sql_ov)
dbDisconnect(con)

overlap_flag <- function(ini, fim) {
  if(length(ini) < 2) return(FALSE)
  o <- any(ini[-1] < fim[-length(fim)])
  isTRUE(o)
}

df_ov <- df_ov_raw %>%
  mutate(ini = as.POSIXct(ini, tz="UTC"), fim = as.POSIXct(fim, tz="UTC")) %>%
  filter(!is.na(ini), !is.na(fim), fim >= ini) %>%
  arrange(perito, ini) %>%
  group_by(perito) %>%
  summarise(overlap = overlap_flag(ini, fim), .groups="drop")

# Junta tudo
df <- df_nc %>%
  select(perito, nc_rate) %>%
  left_join(df_le %>% select(perito, le_rate), by="perito") %>%
  left_join(df_pd %>% select(perito, prod), by="perito") %>%
  left_join(df_ov, by="perito") %>%
  mutate(overlap = ifelse(is.na(overlap), FALSE, overlap))

stopifnot(opt$perito %in% df$perito)

# Normalizações (quanto mais alto, "pior")
df <- df %>%
  mutate(prod_inv = max(prod, na.rm=TRUE) - prod) %>%
  mutate(across(c(nc_rate, le_rate, prod_inv), ~ (. - min(., na.rm=TRUE)) / (max(., na.rm=TRUE) - min(., na.rm=TRUE)), .names="{.col}_norm"),
         overlap_norm = ifelse(overlap, 1, 0))

# Score simples (média das normalizadas)
df$score <- rowMeans(df[,c("nc_rate_norm","le_rate_norm","prod_inv_norm","overlap_norm")], na.rm=TRUE)

p_row <- df %>% filter(perito==opt$perito) %>% slice(1)

plot_df <- tibble::tibble(
  Indicador = c("NC rate", sprintf("≤%ds", opt$`le-threshold`), "Prod (invertida)", "Overlap"),
  Valor     = c(p_row$nc_rate_norm, p_row$le_rate_norm, p_row$prod_inv_norm, p_row$overlap_norm)
)

gg <- ggplot(plot_df, aes(Indicador, Valor)) +
  geom_col(fill="#d62728", width=.6) +
  geom_hline(yintercept = mean(df$score, na.rm=TRUE), linetype="dashed", color="#1f77b4") +
  coord_cartesian(ylim=c(0,1.05)) +
  labs(title="Robustez do Composto — posição do perito (normalizado 0–1)",
       subtitle=sprintf("%s a %s | score do perito = %.2f (média ref. tracejada)", opt$start, opt$end, p_row$score),
       y="Escala normalizada (0–1)", x=NULL) +
  theme_minimal(base_size=11) +
  theme(panel.grid.major.x = element_blank())

out <- file.path(export_dir, sprintf("rcheck_composite_%s.png", perito_safe))
ggsave(out, gg, width=8.5, height=5, dpi=160)
cat(sprintf("✓ salvo: %s\n", out))

