#!/usr/bin/env Rscript

# -*- coding: utf-8 -*-
# Apêndice estatístico (R) — motivos NC: Perito vs. Brasil (resto)
# Saída principal: rcheck_motivos_chisq_<safe_perito>.png e .md no --out-dir
# Uso:
#   Rscript 05_motivos_chisq.R --db /caminho/atestmed.db --start 2025-06-01 --end 2025-06-30 \
#           --perito "NOME DO PERITO" --out-dir /caminho/exports

suppressPackageStartupMessages({
  library(optparse)
  library(DBI)
  library(RSQLite)
  library(dplyr)
  library(ggplot2)
  library(stringr)
  library(forcats)
})

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
option_list <- list(
  make_option("--db", type = "character", help = "Caminho do SQLite (.db)", metavar = "FILE"),
  make_option("--start", type = "character", help = "Data inicial YYYY-MM-DD"),
  make_option("--end",   type = "character", help = "Data final   YYYY-MM-DD"),
  make_option("--perito", type = "character", help = "Nome do perito (obrigatório)"),
  make_option("--out-dir", type = "character", default = ".", help = "Diretório de saída [default: %default]"),
  make_option("--min-count", type = "integer", default = 5L, help = "Agrupa motivos com contagem < min-count em 'OUTROS' [default: %default]"),
  make_option("--topn", type = "integer", default = 12L, help = "Quantidade de motivos por |diferença| no gráfico [default: %default]")
)
opt <- parse_args(OptionParser(option_list = option_list))

stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))
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

# Lumping de motivos muito raros
lump_rare <- function(tbl, min_count = 5L) {
  tbl %>%
    mutate(motivo = if_else(n < min_count, "OUTROS", motivo)) %>%
    group_by(motivo) %>%
    summarise(n = sum(n), .groups = "drop")
}

# ────────────────────────────────────────────────────────────────────────────────
# Carrega dados
# ────────────────────────────────────────────────────────────────────────────────
con <- dbConnect(RSQLite::SQLite(), opt$db)
on.exit(try(dbDisconnect(con), silent = TRUE))

sql_base <- "
SELECT
  p.nomePerito AS perito,
  COALESCE(pr.motivo, 'Motivo_' || a.motivoNaoConformado) AS motivo_text
FROM analises a
JOIN peritos p   ON a.siapePerito = p.siapePerito
LEFT JOIN protocolos pr ON a.protocolo = pr.protocolo
WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
  AND a.motivoNaoConformado != 0
;"
all_nc <- dbGetQuery(con, sql_base, params = list(opt$start, opt$end)) %>%
  mutate(motivo = if_else(is.na(motivo_text) | motivo_text == "", "MOTIVO_DESCONHECIDO", motivo_text)) %>%
  select(perito, motivo)

if (nrow(all_nc) == 0) {
  message("Nenhuma análise NC no período. Nada a fazer.")
  quit(save = "no", status = 0)
}

# ────────────────────────────────────────────────────────────────────────────────
# Particiona: perito vs outros
# ────────────────────────────────────────────────────────────────────────────────
perito_alvo <- opt$perito

tab_perito <- all_nc %>%
  filter(perito == perito_alvo) %>%
  count(motivo, name = "n_p") %>%
  arrange(desc(n_p))

tab_outros <- all_nc %>%
  filter(perito != perito_alvo) %>%
  count(motivo, name = "n_o") %>%
  arrange(desc(n_o))

if (nrow(tab_perito) == 0) {
  message("Perito sem NC no período. Nada a fazer.")
  quit(save = "no", status = 0)
}

# junta e zera NAs (ajuste sem tidyr: coalesce)
base_join <- full_join(tab_perito, tab_outros, by = "motivo") %>%
  mutate(across(all_of(c("n_p","n_o")), ~ dplyr::coalesce(.x, 0L))) %>%
  arrange(desc(n_p + n_o))

# Agrupa raros para estabilidade do qui-quadrado
base_join <- base_join %>%
  rename(n = n_p) %>%
  select(motivo, n, n_o) %>%
  lump_rare(min_count = opt$`min-count`) %>%
  rename(n_p = n) %>%
  left_join(base_join %>% select(motivo, n_o), by = "motivo") %>%
  mutate(n_o = dplyr::coalesce(n_o, 0L)) %>%
  arrange(desc(n_p + n_o))

# ────────────────────────────────────────────────────────────────────────────────
# Teste qui-quadrado e diferenças de proporção
# ────────────────────────────────────────────────────────────────────────────────
total_p <- sum(base_join$n_p)
total_o <- sum(base_join$n_o)

# matriz 2 x K
mat <- rbind(Perito = base_join$n_p, Outros = base_join$n_o)

# Evita linha toda zero
if (total_p == 0 || total_o == 0) {
  message("Sem dados suficientes para qui-quadrado. Nada a fazer.")
  quit(save = "no", status = 0)
}

chs <- suppressWarnings(chisq.test(mat))
pval <- chs$p.value

resumo <- base_join %>%
  mutate(
    prop_p = n_p / total_p,
    prop_o = n_o / total_o,
    diff   = prop_p - prop_o
  ) %>%
  arrange(desc(abs(diff))) %>%
  slice_head(n = opt$topn) %>%
  mutate(motivo_plot = forcats::fct_reorder(motivo, diff))

# ────────────────────────────────────────────────────────────────────────────────
# Plot
# ────────────────────────────────────────────────────────────────────────────────
titulo <- sprintf("Motivos NC — %s vs. Brasil (resto)\n%s a %s  |  χ² p=%.3g",
                  perito_alvo, opt$start, opt$end, pval)

g <- ggplot(resumo, aes(x = motivo_plot, y = diff)) +
  geom_hline(yintercept = 0, linewidth = 0.4) +
  geom_col() +
  coord_flip() +
  labs(
    title = titulo,
    x = NULL,
    y = "Diferença de proporções (Perito − Outros)"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    panel.grid.minor = element_blank(),
    plot.title = element_text(face = "bold", hjust = 0)
  )

safe <- safe_slug(perito_alvo)
png_path <- file.path(opt$`out-dir`, sprintf("rcheck_motivos_chisq_%s.png", safe))
ggsave(png_path, g, width = 10, height = 6, dpi = 160)
message(sprintf("✅ Figura salva: %s", png_path))

# ────────────────────────────────────────────────────────────────────────────────
# Markdown curto (opcional)
# ────────────────────────────────────────────────────────────────────────────────
md_path <- file.path(opt$`out-dir`, sprintf("rcheck_motivos_chisq_%s.md", safe))
cat(sprintf("# Motivos NC — %s vs. Brasil (resto)\n\n", perito_alvo),
    sprintf("- Período: %s a %s\n", opt$start, opt$end),
    sprintf("- Teste qui-quadrado: p = %.3g\n", pval),
    "- Interpretação: barras positivas indicam motivos relativamente **mais frequentes** no perito; negativas, **menos frequentes**.\n",
    file = md_path, sep = "")
message(sprintf("📝 Markdown salvo: %s", md_path))

