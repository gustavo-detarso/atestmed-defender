#!/usr/bin/env Rscript

# -*- coding: utf-8 -*-
# KPIs (ICRA, IATD, ScoreFinal) — Top 10 (grupo) vs. Brasil (resto)
# Saídas: rcheck_top10_kpi_icra_iatd_score.png e .md no --out-dir

suppressPackageStartupMessages({
  library(optparse)
  library(DBI); library(RSQLite)
  library(dplyr); library(tidyr); library(forcats)
  library(ggplot2); library(scales); library(stringr); library(broom)
})

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
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

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
fail_plot <- function(msg) {
  ggplot() +
    annotate("text", x = 0, y = 0, label = msg, size = 5) +
    theme_void()
}
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

# ────────────────────────────────────────────────────────────────────────────────
# Conexão e detecção de colunas
# ────────────────────────────────────────────────────────────────────────────────
con <- dbConnect(RSQLite::SQLite(), opt$db)
on.exit(try(dbDisconnect(con), silent = TRUE))

cols_ind <- dbGetQuery(con, "PRAGMA table_info(indicadores)")$name
if (length(cols_ind) == 0) {
  out_png <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.png")
  ggsave(out_png, fail_plot("Tabela 'indicadores' não encontrada"), width=10, height=6, dpi=160)
  quit(save="no")
}

col_icra  <- pick_col(cols_ind, c("ICRA","icra","kpi_icra"))
col_iatd  <- pick_col(cols_ind, c("IATD","iatd","kpi_iatd"))
col_score <- pick_col(cols_ind, c("scoreFinal","score_final","ScoreFinal","score","scorefinal"))
needed <- c(col_icra, col_iatd, col_score)
if (any(is.na(needed))) {
  out_png <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.png")
  miss <- c("ICRA"  = is.na(col_icra),
            "IATD"  = is.na(col_iatd),
            "Score" = is.na(col_score))
  msg <- paste("Colunas ausentes em 'indicadores':",
               paste(names(miss)[miss], collapse=", "))
  ggsave(out_png, fail_plot(msg), width=10, height=6, dpi=160)
  quit(save="no")
}

# Top10 por scoreFinal DESC (mesmo critério do Python), com mínimo de análises no período
sql_top10 <- "
SELECT p.nomePerito AS nomePerito
FROM indicadores i
JOIN peritos p ON i.perito = p.siapePerito
JOIN analises a ON a.siapePerito = i.perito
WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
GROUP BY p.nomePerito, i.%s
HAVING COUNT(a.protocolo) >= ?
ORDER BY i.%s DESC
LIMIT 10;
"
score_col_sql <- DBI::dbQuoteIdentifier(con, col_score)
top10 <- dbGetQuery(con, sprintf(sql_top10, col_score, col_score),
                    params = list(opt$start, opt$end, opt$`min-analises`))

if (nrow(top10) == 0) {
  out_png <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.png")
  ggsave(out_png, fail_plot("Nenhum Top 10 para o período/critério."), width=10, height=6, dpi=160)
  quit(save="no")
}
top10_set <- unique(top10$nomePerito)

# Peritos ativos no período (para formar o Resto coerente com o recorte temporal)
sql_ativos <- "
WITH ativos AS (
  SELECT DISTINCT a.siapePerito AS siape
  FROM analises a
  WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
)
SELECT p.nomePerito,
       i.%s AS icra, i.%s AS iatd, i.%s AS score
FROM indicadores i
JOIN peritos p ON i.perito = p.siapePerito
JOIN ativos  s ON s.siape = i.perito
"
df <- dbGetQuery(con, sprintf(sql_ativos, col_icra, col_iatd, col_score),
                 params = list(opt$start, opt$end)) %>%
  mutate(across(c(icra,iatd,score), as.numeric)) %>%
  mutate(grupo = if_else(nomePerito %in% top10_set, "Top10", "Resto"))

if (nrow(df) == 0) {
  out_png <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.png")
  ggsave(out_png, fail_plot("Sem peritos ativos no período."), width=10, height=6, dpi=160)
  quit(save="no")
}

# Long para boxplot + testes por KPI
long <- df %>%
  select(grupo, icra, iatd, score) %>%
  pivot_longer(cols = -grupo, names_to = "kpi", values_to = "valor") %>%
  mutate(kpi = recode(kpi, icra="ICRA", iatd="IATD", score="Score Final")) %>%
  filter(is.finite(valor))

# Testes (Mann-Whitney) por KPI
tests <- long %>%
  group_by(kpi) %>%
  summarise(
    p_wilcox = tryCatch({
      w <- wilcox.test(valor ~ grupo, exact = FALSE)
      unname(w$p.value)
    }, error = function(e) NA_real_),
    n_top10 = sum(grupo=="Top10"),
    n_resto = sum(grupo=="Resto"),
    med_top10 = median(valor[grupo=="Top10"], na.rm=TRUE),
    med_resto = median(valor[grupo=="Resto"], na.rm=TRUE),
    .groups = "drop"
  ) %>%
  mutate(lbl = sprintf("p=%.3g | med Top10=%.3f | med Resto=%.3f | n=(%d,%d)",
                       p_wilcox, med_top10, med_resto, n_top10, n_resto))

titulo <- sprintf("Top 10 — KPIs (ICRA, IATD, Score Final) vs. Brasil (resto)\n%s a %s",
                  opt$start, opt$end)

g <- ggplot(long, aes(x = grupo, y = valor, fill = grupo)) +
  geom_boxplot(outlier.shape = NA, width = 0.55) +
  geom_jitter(width = 0.12, alpha = 0.5, size = 1.8) +
  facet_wrap(~kpi, scales = "free_y", ncol = 1) +
  labs(title = titulo, x = NULL, y = NULL,
       subtitle = paste(tests$kpi, tests$lbl, collapse = "   |   ")) +
  theme_minimal(base_size = 11) +
  theme(panel.grid.minor = element_blank(),
        legend.position = "none")

out_png <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.png")
ggsave(out_png, g, width = 8.5, height = 10, dpi = 160)
message(sprintf("✅ Figura salva: %s", out_png))

# Markdown resumido
md <- c(
  "# Top 10 — KPIs (ICRA, IATD, Score Final) vs. Brasil (resto)",
  sprintf("- Período: %s a %s", opt$start, opt$end),
  sprintf("- Critério Top 10: %s (mín. análises = %d)", col_score, opt$`min-analises`),
  "- Teste: Mann-Whitney (não-paramétrico) por KPI",
  "",
  "| KPI | p (Wilcoxon) | Mediana Top10 | Mediana Resto | n Top10 | n Resto |",
  "|---:|---:|---:|---:|---:|---:|"
)
for (i in seq_len(nrow(tests))) {
  md <- c(md, sprintf("| %s | %.3g | %.3f | %.3f | %d | %d |",
                      tests$kpi[i], tests$p_wilcox[i], tests$med_top10[i], tests$med_resto[i],
                      tests$n_top10[i], tests$n_resto[i]))
}
out_md <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.md")
writeLines(md, out_md, useBytes = TRUE)
message(sprintf("📝 Markdown salvo: %s", out_md))

