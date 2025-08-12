#!/usr/bin/env Rscript

# -*- coding: utf-8 -*-
# Apêndice estatístico (R) — motivos NC: Top 10 (grupo) vs. Brasil (resto)
# Saída principal: rcheck_top10_motivos_chisq.png e .md no --out-dir
# Uso:
#   Rscript g05_top10_motivos_chisq.R --db /caminho/atestmed.db --start 2025-06-01 --end 2025-06-30 \
#           --min-analises 50 --out-dir /caminho/exports

suppressPackageStartupMessages({
  library(optparse)
  library(DBI)
  library(RSQLite)
  library(dplyr)
  library(ggplot2)
  library(stringr)
  library(forcats)
  library(tidyr) # NÃO usado em replace_na, mas para pivot_wider em fallback (não obrigatório)
})

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
option_list <- list(
  make_option("--db", type = "character", help = "Caminho do SQLite (.db)", metavar = "FILE"),
  make_option("--start", type = "character", help = "Data inicial YYYY-MM-DD"),
  make_option("--end",   type = "character", help = "Data final   YYYY-MM-DD"),
  make_option("--min-analises", type = "integer", default = 50L, help = "Elegibilidade Top 10 [default: %default]"),
  make_option("--out-dir", type = "character", default = ".", help = "Diretório de saída [default: %default]"),
  make_option("--min-count", type = "integer", default = 10L, help = "Agrupa motivos com contagem < min-count em 'OUTROS' [default: %default]"),
  make_option("--topn", type = "integer", default = 15L, help = "Quantidade de motivos por |diferença| no gráfico [default: %default]")
)
opt <- parse_args(OptionParser(option_list = option_list))

stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end))
if (!dir.exists(opt$`out-dir`)) dir.create(opt$`out-dir`, recursive = TRUE, showWarnings = FALSE)

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
safe_slug <- function(x) {
  x <- gsub("[^A-Za-z0-9\\-_]+", "_", x)
  x <- gsub("_+", "_", x)
  x <- gsub("^_|_$", "", x)
  ifelse(nchar(x) > 0, x, "output")
}

lump_rare <- function(tbl, min_count = 10L) {
  tbl %>%
    mutate(motivo = if_else(n < min_count, "OUTROS", motivo)) %>%
    group_by(motivo) %>%
    summarise(n = sum(n), .groups = "drop")
}

# ────────────────────────────────────────────────────────────────────────────────
# Conexão e dados básicos
# ────────────────────────────────────────────────────────────────────────────────
con <- dbConnect(RSQLite::SQLite(), opt$db)
on.exit(try(dbDisconnect(con), silent = TRUE))

# Top 10 piores por scoreFinal, com mínimo de análises (mesmo critério do Python)
sql_top10 <- "
SELECT p.nomePerito AS nomePerito
FROM indicadores i
JOIN peritos p ON i.perito = p.siapePerito
JOIN analises a ON a.siapePerito = i.perito
WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
GROUP BY p.nomePerito, i.scoreFinal
HAVING COUNT(a.protocolo) >= ?
ORDER BY i.scoreFinal DESC
LIMIT 10;
"
top10_df <- dbGetQuery(con, sql_top10, params = list(opt$start, opt$end, opt$`min-analises`))
if (nrow(top10_df) == 0) {
  message("Nenhum perito atende ao critério Top 10. Nada a fazer.")
  quit(save = "no", status = 0)
}
top10_set <- unique(top10_df$nomePerito)

# Todas NC no período (com texto de motivo)
sql_nc <- "
SELECT
  p.nomePerito AS perito,
  COALESCE(pr.motivo, 'Motivo_' || a.motivoNaoConformado) AS motivo_text
FROM analises a
JOIN peritos p   ON a.siapePerito = p.siapePerito
LEFT JOIN protocolos pr ON a.protocolo = pr.protocolo
WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
  AND a.motivoNaoConformado != 0
;"
all_nc <- dbGetQuery(con, sql_nc, params = list(opt$start, opt$end)) %>%
  mutate(motivo = if_else(is.na(motivo_text) | motivo_text == "", "MOTIVO_DESCONHECIDO", motivo_text)) %>%
  select(perito, motivo)

if (nrow(all_nc) == 0) {
  message("Nenhuma análise NC no período. Nada a fazer.")
  quit(save = "no", status = 0)
}

# ────────────────────────────────────────────────────────────────────────────────
# Grupo Top10 vs Resto
# ────────────────────────────────────────────────────────────────────────────────
tab_top10 <- all_nc %>%
  mutate(grupo = if_else(perito %in% top10_set, "Top10", "Resto")) %>%
  count(grupo, motivo, name = "n") %>%
  tidyr::pivot_wider(names_from = grupo, values_from = n) %>%
  # zera NAs com dplyr::coalesce via mutate/across (sem tidyr::replace_na)
  mutate(across(all_of(c("Top10","Resto")), ~ dplyr::coalesce(.x, 0L))) %>%
  arrange(desc(Top10 + Resto))

# Agrupa motivos raros
tab_top10 <- tab_top10 %>%
  rename(n = Top10) %>%
  select(motivo, n, Resto) %>%
  lump_rare(min_count = opt$`min-count`) %>%
  rename(Top10 = n) %>%
  left_join(tab_top10 %>% select(motivo, Resto), by = "motivo") %>%
  mutate(Resto = dplyr::coalesce(Resto, 0L)) %>%
  arrange(desc(Top10 + Resto))

total_top10 <- sum(tab_top10$Top10)
total_resto <- sum(tab_top10$Resto)

if (total_top10 == 0 || total_resto == 0) {
  message("Sem dados suficientes para qui-quadrado. Nada a fazer.")
  quit(save = "no", status = 0)
}

mat <- rbind(Top10 = tab_top10$Top10, Resto = tab_top10$Resto)
chs <- suppressWarnings(chisq.test(mat))
pval <- chs$p.value

resumo <- tab_top10 %>%
  mutate(
    prop_top10 = Top10 / total_top10,
    prop_resto = Resto / total_resto,
    diff       = prop_top10 - prop_resto
  ) %>%
  arrange(desc(abs(diff))) %>%
  slice_head(n = opt$topn) %>%
  mutate(motivo_plot = forcats::fct_reorder(motivo, diff))

# ────────────────────────────────────────────────────────────────────────────────
# Plot
# ────────────────────────────────────────────────────────────────────────────────
titulo <- sprintf("Motivos NC — Top 10 (grupo) vs. Brasil (resto)\n%s a %s  |  χ² p=%.3g",
                  opt$start, opt$end, pval)

g <- ggplot(resumo, aes(x = motivo_plot, y = diff)) +
  geom_hline(yintercept = 0, linewidth = 0.4) +
  geom_col() +
  coord_flip() +
  labs(
    title = titulo,
    x = NULL,
    y = "Diferença de proporções (Top10 − Resto)"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    panel.grid.minor = element_blank(),
    plot.title = element_text(face = "bold", hjust = 0)
  )

png_path <- file.path(opt$`out-dir`, "rcheck_top10_motivos_chisq.png")
ggsave(png_path, g, width = 10, height = 6, dpi = 160)
message(sprintf("✅ Figura salva: %s", png_path))

# ────────────────────────────────────────────────────────────────────────────────
# Markdown curto (opcional)
# ────────────────────────────────────────────────────────────────────────────────
md_path <- file.path(opt$`out-dir`, "rcheck_top10_motivos_chisq.md")
cat("# Motivos NC — Top 10 (grupo) vs. Brasil (resto)\n\n",
    sprintf("- Período: %s a %s\n", opt$start, opt$end),
    sprintf("- Critério Top 10: scoreFinal (min. análises = %d)\n", opt$`min-analises`),
    sprintf("- Teste qui-quadrado: p = %.3g\n", pval),
    "- Interpretação: barras positivas indicam motivos relativamente **mais frequentes** no grupo Top 10; negativas, **menos frequentes**.\n",
    file = md_path, sep = "")
message(sprintf("📝 Markdown salvo: %s", md_path))

