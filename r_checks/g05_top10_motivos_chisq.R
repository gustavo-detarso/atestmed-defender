#!/usr/bin/env Rscript

# -*- coding: utf-8 -*-
# Apêndice estatístico (R) — motivos NC: Top 10 (grupo) vs. Brasil (resto)
# Saídas:
#   - rcheck_top10_motivos_chisq.png
#   - rcheck_top10_motivos_chisq.org            (imagem + comentário)
#   - rcheck_top10_motivos_chisq_comment.org    (apenas comentário)
#
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
  library(tidyr)
  library(scales)
})

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
option_list <- list(
  make_option("--db",            type="character", help="Caminho do SQLite (.db)", metavar="FILE"),
  make_option("--start",         type="character", help="Data inicial YYYY-MM-DD"),
  make_option("--end",           type="character", help="Data final   YYYY-MM-DD"),
  make_option("--min-analises",  type="integer",  default=50L, help="Elegibilidade Top 10 [default: %default]"),
  make_option("--out-dir",       type="character", default=".", help="Diretório de saída [default: %default]"),
  make_option("--min-count",     type="integer",  default=10L, help="Agrupa motivos com contagem < min-count em 'OUTROS' [default: %default]"),
  make_option("--topn",          type="integer",  default=15L, help="Quantidade de motivos por |diferença| no gráfico [default: %default]")
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
percent_s <- function(x, acc=.1) ifelse(is.finite(x), percent(x, accuracy=acc), "NA")

lump_rare <- function(tbl, min_count = 10L) {
  tbl %>%
    mutate(motivo = if_else(n < min_count, "OUTROS", motivo)) %>%
    group_by(motivo) %>%
    summarise(n = sum(n), .groups = "drop")
}

fail_plot <- function(msg) {
  ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()
}

table_exists <- function(con, name) {
  nrow(dbGetQuery(con,
                  "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
                  params = list(name))) > 0
}
detect_analises_table <- function(con) {
  for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t)
  stop("Não encontrei 'analises' nem 'analises_atestmed'.")
}

# ────────────────────────────────────────────────────────────────────────────────
# Conexão e caminhos de saída
# ────────────────────────────────────────────────────────────────────────────────
con <- dbConnect(RSQLite::SQLite(), opt$db)
on.exit(try(dbDisconnect(con), silent = TRUE))

png_path <- file.path(opt$`out-dir`, "rcheck_top10_motivos_chisq.png")
org_main <- file.path(opt$`out-dir`, "rcheck_top10_motivos_chisq.org")
org_comm <- file.path(opt$`out-dir`, "rcheck_top10_motivos_chisq_comment.org")

a_tbl <- detect_analises_table(con)
if (!table_exists(con, "indicadores")) {
  ggsave(png_path, fail_plot("Tabela 'indicadores' não encontrada — calcule indicadores antes de usar este script."), width=10, height=6, dpi=160)
  quit(save="no", status=0)
}

# ────────────────────────────────────────────────────────────────────────────────
# Top 10 piores por scoreFinal (mínimo de análises no período)
# ────────────────────────────────────────────────────────────────────────────────
sql_top10 <- sprintf("
SELECT p.nomePerito AS nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
FROM indicadores i
JOIN peritos p ON i.perito = p.siapePerito
JOIN %s a ON a.siapePerito = i.perito
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
GROUP BY p.nomePerito, i.scoreFinal
HAVING total_analises >= ?
ORDER BY i.scoreFinal DESC, total_analises DESC
LIMIT 10;
", a_tbl)

top10_df <- dbGetQuery(con, sql_top10, params = list(opt$start, opt$end, opt$`min-analises`))
if (nrow(top10_df) == 0) {
  ggsave(png_path, fail_plot("Nenhum perito atende ao critério Top 10 no período."), width=10, height=6, dpi=160)
  quit(save = "no", status = 0)
}
top10_set <- unique(top10_df$nomePerito)

# ────────────────────────────────────────────────────────────────────────────────
# NC robusto (preferindo protocolos.motivo quando houver)
# NC = conformado=0 OU (TRIM(motivoNaoConformado) <> '' E CAST(...) <> 0)
# ────────────────────────────────────────────────────────────────────────────────
sql_nc <- sprintf("
SELECT
  p.nomePerito AS perito,
  COALESCE(NULLIF(TRIM(pr.motivo), ''), CAST(IFNULL(a.motivoNaoConformado,'') AS TEXT)) AS motivo_text
FROM %s a
JOIN peritos p   ON a.siapePerito = p.siapePerito
LEFT JOIN protocolos pr ON a.protocolo = pr.protocolo
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
  AND (
        CAST(IFNULL(a.conformado,1) AS INTEGER)=0
        OR (
             TRIM(IFNULL(a.motivoNaoConformado,'')) <> ''
             AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0
           )
      );
", a_tbl)

all_nc <- dbGetQuery(con, sql_nc, params = list(opt$start, opt$end)) %>%
  mutate(
    motivo_text = as.character(motivo_text),
    motivo = if_else(is.na(motivo_text) | trimws(motivo_text) == "" | trimws(motivo_text) == "0",
                     "MOTIVO_DESCONHECIDO",
                     trimws(motivo_text))
  ) %>%
  select(perito, motivo)

if (nrow(all_nc) == 0) {
  ggsave(png_path, fail_plot("Nenhuma análise NC (robusto) no período."), width=10, height=6, dpi=160)
  quit(save = "no", status = 0)
}

# ────────────────────────────────────────────────────────────────────────────────
# Grupo Top10 vs Resto
# ────────────────────────────────────────────────────────────────────────────────
tab_top10 <- all_nc %>%
  mutate(grupo = if_else(perito %in% top10_set, "Top10", "Resto")) %>%
  count(grupo, motivo, name = "n") %>%
  tidyr::pivot_wider(names_from = grupo, values_from = n) %>%
  mutate(across(all_of(c("Top10","Resto")), ~ dplyr::coalesce(.x, 0L))) %>%
  arrange(desc(Top10 + Resto))

# Agrupa raros e recompõe
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
  ggsave(png_path, fail_plot("Sem dados suficientes para χ² (um dos grupos tem total 0)."), width=10, height=6, dpi=160)
  quit(save = "no", status = 0)
}

mat  <- rbind(Top10 = tab_top10$Top10, Resto = tab_top10$Resto)
chs  <- suppressWarnings(chisq.test(mat))
pval <- chs$p.value

resumo <- tab_top10 %>%
  mutate(
    prop_top10 = Top10 / total_top10,
    prop_resto = Resto / total_resto,
    diff       = prop_top10 - prop_resto
  ) %>%
  arrange(desc(abs(diff))) %>%
  slice_head(n = opt$topn) %>%
  mutate(
    diff = as.numeric(diff),
    motivo_plot = forcats::fct_reorder(motivo, diff)
  )

# ────────────────────────────────────────────────────────────────────────────────
# Plot
# ────────────────────────────────────────────────────────────────────────────────
titulo <- sprintf("Motivos NC (robusto) — Top 10 (grupo) vs. Brasil (resto)\n%s a %s  |  χ² p=%.3g",
                  opt$start, opt$end, pval)

g <- ggplot(resumo, aes(x = motivo_plot, y = diff)) +
  geom_hline(yintercept = 0, linewidth = 0.4) +
  geom_col() +
  coord_flip() +
  labs(
    title = titulo,
    x = NULL,
    y = "Diferença de proporções (Top10 − Resto)",
    caption = "NC robusto: conformado=0 OU (motivoNaoConformado ≠ '' E CAST(motivoNaoConformado) ≠ 0)."
  ) +
  theme_minimal(base_size = 11) +
  theme(
    panel.grid.minor = element_blank(),
    plot.title = element_text(face = "bold", hjust = 0)
  )

ggsave(png_path, g, width = 10, height = 6, dpi = 160)
message(sprintf("✅ Figura salva: %s", png_path))

# ────────────────────────────────────────────────────────────────────────────────
# Comentários (.org) — método + interpretação + destaques
# ────────────────────────────────────────────────────────────────────────────────
n_pos <- sum(resumo$diff > 0, na.rm = TRUE)
n_neg <- sum(resumo$diff < 0, na.rm = TRUE)

top_pos <- resumo %>%
  filter(diff > 0) %>%
  slice_max(order_by = diff, n = min(3L, n_pos)) %>%
  transmute(txt = sprintf("%s (+%s p.p.)", motivo, percent_s(100*diff/100, acc=.1)))

top_neg <- resumo %>%
  filter(diff < 0) %>%
  slice_min(order_by = diff, n = min(3L, n_neg)) %>%
  transmute(txt = sprintf("%s (%s p.p.)", motivo, percent_s(100*diff/100, acc=.1)))
# (percent_s recebe proporção; 100*diff/100 mantém rótulo em pontos percentuais)

metodo_txt <- paste0(
  "*Método.* Identificamos os *Top 10 piores* por *scoreFinal* (mín. ", opt$`min-analises`, " análises). ",
  "Construímos uma tabela motivo × grupo (*Top10* vs *Resto*), ",
  "agregando motivos raros (< ", opt$`min-count`, ") em 'OUTROS' para estabilidade. ",
  "Aplicamos o teste *qui-quadrado* global (χ²) e, por motivo, comparamos as *proporções* do Top10 (n=",
  total_top10, ") e do Resto (n=", total_resto, "). ",
  "No gráfico, exibimos os ", min(nrow(resumo), opt$topn),
  " motivos com maior |diferença| (Top10 − Resto)."
)

interpreta_txt <- {
  sig <- if (is.finite(pval) && pval < 0.05) "diferenças *estatisticamente significativas*" else "diferenças não significativas ao nível de 5%"
  pos_str <- if (nrow(top_pos)) paste("- Mais frequentes no Top10:", paste(top_pos$txt, collapse=", "), ".") else NULL
  neg_str <- if (nrow(top_neg)) paste("- Menos frequentes no Top10:", paste(top_neg$txt, collapse=", "), ".") else NULL
  paste0(
    "*Interpretação.* O teste global indica ", sig, " (p = ", formatC(pval, format="fg", digits=3), "). ",
    "Barras *positivas* indicam motivos relativamente mais comuns no Top10; *negativas*, menos comuns. ",
    "Use como *pistas* para auditoria qualitativa, considerando volume e contexto.\n",
    paste(na.omit(c(pos_str, neg_str)), collapse = "\n")
  )
}

# .org principal (imagem + texto)
org_main_txt <- paste(
  "#+CAPTION: Motivos de NC (robusto) — Diferença de proporções (Top10 − Resto)",
  sprintf("[[file:%s]]", basename(png_path)),
  "",
  metodo_txt, "",
  interpreta_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
message(sprintf("✅ Org salvo: %s", org_main))

# .org apenas com o comentário (para injeção no PDF)
org_comment_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comment_txt, org_comm)
message(sprintf("✅ Org(comment) salvo: %s", org_comm))

