#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite); library(dplyr); library(ggplot2); library(scales)
})

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
opt_list <- list(
  make_option("--db",              type="character"),
  make_option("--start",           type="character"),
  make_option("--end",             type="character"),
  make_option("--perito",          type="character"),
  make_option("--prod-threshold",  type="double",  default=50),
  make_option("--le-threshold",    type="integer", default=15),
  make_option("--out-dir",         type="character", default=NULL, help="Diretório de saída (PNG + ORG)")
)
opt <- parse_args(OptionParser(option_list = opt_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
safe <- function(x) { gsub("(^_+|_+$)", "", gsub("[^A-Za-z0-9_-]+","_", x)) }
percent_s <- function(x, acc=.1) ifelse(is.finite(x), percent(x, accuracy = acc), "NA")

perito_safe <- safe(opt$perito)

base_dir   <- normalizePath(file.path(dirname(opt$db), ".."))
export_dir <- if (!is.null(opt$`out-dir`)) normalizePath(opt$`out-dir`, mustWork = FALSE) else
              file.path(base_dir, "graphs_and_tables", "exports")
dir.create(export_dir, showWarnings = FALSE, recursive = TRUE)

table_exists <- function(con, name) {
  nrow(dbGetQuery(con, "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
                  params=list(name))) > 0
}
detect_analises_table <- function(con) {
  for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t)
  stop("Não encontrei 'analises' nem 'analises_atestmed'.")
}
normalize01 <- function(v) {
  if (all(is.na(v))) return(v)
  mn <- suppressWarnings(min(v, na.rm=TRUE)); mx <- suppressWarnings(max(v, na.rm=TRUE))
  if (!is.finite(mn) || !is.finite(mx) || mx <= mn) return(ifelse(is.na(v), NA_real_, 0))
  (v - mn) / (mx - mn)
}

# ────────────────────────────────────────────────────────────────────────────────
# Conexão
# ────────────────────────────────────────────────────────────────────────────────
con <- dbConnect(SQLite(), opt$db)
on.exit(try(dbDisconnect(con), silent=TRUE))

a_tbl <- detect_analises_table(con)

# ────────────────────────────────────────────────────────────────────────────────
# NC robusto (rate por perito)
# ────────────────────────────────────────────────────────────────────────────────
nc_expr <- "
CASE
  WHEN CAST(IFNULL(a.conformado,1) AS INTEGER)=0 THEN 1
  WHEN TRIM(IFNULL(a.motivoNaoConformado,'')) <> ''
       AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0 THEN 1
  ELSE 0
END
"

sql_nc <- sprintf("
SELECT p.nomePerito AS perito,
       SUM(%s) AS nc,
       COUNT(*) AS n
FROM %s a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN '%s' AND '%s'
GROUP BY p.nomePerito
", nc_expr, dbQuoteIdentifier(con, a_tbl), opt$start, opt$end)
df_nc <- dbGetQuery(con, sql_nc) %>% mutate(nc_rate = ifelse(n>0, nc/n, NA_real_))

# ────────────────────────────────────────────────────────────────────────────────
# Base de durações válidas (0 < dur ≤ 3600) — reutilizada em ≤threshold e produtividade
# ────────────────────────────────────────────────────────────────────────────────
sql_valid <- sprintf("
SELECT p.nomePerito AS perito,
       a.dataHoraIniPericia AS ini,
       a.dataHoraFimPericia AS fim,
       ((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400.0) AS dur_s
FROM %s a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN '%s' AND '%s'
  AND a.dataHoraIniPericia IS NOT NULL
  AND a.dataHoraFimPericia IS NOT NULL
  AND ((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400.0) > 0
  AND ((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400.0) <= 3600
", dbQuoteIdentifier(con, a_tbl), opt$start, opt$end)
df_valid <- dbGetQuery(con, sql_valid)

# ≤ threshold (entre válidas)
df_le <- df_valid %>%
  group_by(perito) %>%
  summarise(
    le = sum(dur_s <= opt$`le-threshold`, na.rm=TRUE),
    n  = n(),
    .groups = "drop"
  ) %>%
  mutate(le_rate = ifelse(n>0, le/n, NA_real_))

# Produtividade (entre válidas)
df_pd <- df_valid %>%
  group_by(perito) %>%
  summarise(
    total = n(),
    segs  = sum(dur_s, na.rm=TRUE),
    .groups = "drop"
  ) %>%
  mutate(prod = ifelse(segs>0, total/(segs/3600), NA_real_))

# ────────────────────────────────────────────────────────────────────────────────
# Overlap (flag por perito, entre válidas)
# ────────────────────────────────────────────────────────────────────────────────
df_ov_raw <- df_valid %>% select(perito, ini, fim) %>%
  mutate(ini = as.POSIXct(ini, tz="UTC"),
         fim = as.POSIXct(fim, tz="UTC")) %>%
  filter(!is.na(ini), !is.na(fim), fim >= ini) %>%
  arrange(perito, ini)

overlap_flag <- function(ini, fim) {
  if(length(ini) < 2) return(FALSE)
  any(ini[-1] < fim[-length(fim)])
}
df_ov <- df_ov_raw %>%
  group_by(perito) %>%
  summarise(overlap = overlap_flag(ini, fim), .groups="drop")

# ────────────────────────────────────────────────────────────────────────────────
# Junta tudo
# ────────────────────────────────────────────────────────────────────────────────
df <- df_nc %>%
  select(perito, nc_rate) %>%
  left_join(df_le %>% select(perito, le_rate), by="perito") %>%
  left_join(df_pd %>% select(perito, prod), by="perito") %>%
  left_join(df_ov, by="perito") %>%
  mutate(overlap = ifelse(is.na(overlap), FALSE, overlap))

stopifnot(opt$perito %in% df$perito)

# ────────────────────────────────────────────────────────────────────────────────
# Normalizações (quanto mais alto, "pior")
# ────────────────────────────────────────────────────────────────────────────────
max_prod <- suppressWarnings(max(df$prod, na.rm=TRUE)); if (!is.finite(max_prod)) max_prod <- 0
df <- df %>%
  mutate(prod_inv = max_prod - prod) %>%
  mutate(
    nc_rate_norm  = normalize01(nc_rate),
    le_rate_norm  = normalize01(le_rate),
    prod_inv_norm = normalize01(prod_inv),
    overlap_norm  = ifelse(overlap, 1, 0)
  )

# Score simples (média das normalizadas)
df$score <- rowMeans(df[,c("nc_rate_norm","le_rate_norm","prod_inv_norm","overlap_norm")], na.rm=TRUE)

p_row <- df %>% filter(perito==opt$perito) %>% slice(1)
mean_score <- mean(df$score, na.rm=TRUE)

plot_df <- tibble::tibble(
  Indicador = c("NC rate (robusto)", sprintf("≤%ds", opt$`le-threshold`), "Prod (invertida)", "Overlap"),
  Valor     = c(p_row$nc_rate_norm, p_row$le_rate_norm, p_row$prod_inv_norm, p_row$overlap_norm)
)

gg <- ggplot(plot_df, aes(Indicador, Valor)) +
  geom_col(fill="#d62728", width=.6) +
  geom_hline(yintercept = mean_score, linetype="dashed", color="#1f77b4") +
  coord_cartesian(ylim=c(0,1.05)) +
  labs(title="Robustez do Composto — posição do perito (normalizado 0–1)",
       subtitle=sprintf("%s a %s | score do perito = %.2f (média ref. tracejada) | prod≥%.0f/h, ≤%ds",
                        opt$start, opt$end, p_row$score, opt$`prod-threshold`, opt$`le-threshold`),
       y="Escala normalizada (0–1)", x=NULL,
       caption="NC robusto: conformado=0 OU (motivoNaoConformado≠'' e CAST(...)≠0). Durações válidas: 0<dur≤3600s.") +
  theme_minimal(base_size=11) +
  theme(panel.grid.major.x = element_blank())

png_path <- file.path(export_dir, sprintf("rcheck_composite_%s.png", perito_safe))
ggsave(png_path, gg, width=8.5, height=5, dpi=160)
cat(sprintf("✓ salvo: %s\n", png_path))

# ────────────────────────────────────────────────────────────────────────────────
# Comentários em .org (método + interpretação)
# ────────────────────────────────────────────────────────────────────────────────
nc_txt  <- percent_s(p_row$nc_rate, acc = .1)
le_txt  <- percent_s(p_row$le_rate, acc = .1)
prod_tx <- ifelse(is.finite(p_row$prod), sprintf("%.2f/h", p_row$prod), "NA")
ov_txt  <- ifelse(isTRUE(p_row$overlap), "Sim", "Não")
pos_txt <- ifelse(is.finite(p_row$score) & is.finite(mean_score) & p_row$score > mean_score,
                  "acima da média nacional (pior)", "abaixo/na média nacional (melhor)")

metodo_txt <- paste0(
  "*Método.* Combinamos quatro indicadores calculados no período (", opt$start, "–", opt$end, "): ",
  "(i) taxa de NC *robusto*; (ii) proporção de perícias ≤ ", opt$`le-threshold`, "s entre durações válidas; ",
  "(iii) produtividade (análises/h), invertida para que valores maiores signifiquem pior desempenho; ",
  "(iv) ocorrência de *sobreposição* (flag 0/1). ",
  "Cada indicador é normalizado por min–max para a escala 0–1 e o *score* é a média simples das normalizadas. ",
  "As métricas dependentes de tempo usam apenas tarefas com duração válida (0<dur≤3600s). ",
  "A linha tracejada do gráfico representa a *média nacional* do score."
)

interpreta_txt <- paste0(
  "*Interpretação.* Barras mais próximas de 1 indicam pior posição relativa do perito frente aos pares; ",
  "próximas de 0, melhor. Para o período: ",
  "NC=", nc_txt, "; ≤", opt$`le-threshold`, "s=", le_txt, "; Prod=", prod_tx, "; Overlap=", ov_txt, ". ",
  "Score do perito = ", sprintf("%.2f", p_row$score), " (", pos_txt, "). ",
  "Use estes sinais para priorizar auditorias: métricas com barras altas são prováveis *drivers* do composto."
)

# arquivo .org principal (imagem + comentário)
org_main <- file.path(export_dir, sprintf("rcheck_composite_%s.org", perito_safe))
org_main_txt <- paste(
  "#+CAPTION: Composto (normalizado 0–1) — NC, ≤limiar, Prod (invertida) e Overlap",
  sprintf("[[file:%s]]", basename(png_path)),  # make_report reescreve para ../imgs/
  "",
  metodo_txt, "",
  interpreta_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
cat(sprintf("✓ salvo: %s\n", org_main))

# arquivo .org somente comentário (para injeção no PDF)
org_comment <- file.path(export_dir, sprintf("rcheck_composite_%s_comment.org", perito_safe))
org_comment_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comment_txt, org_comment)
cat(sprintf("✓ salvo: %s\n", org_comment))

