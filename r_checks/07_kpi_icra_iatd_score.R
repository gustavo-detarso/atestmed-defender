#!/usr/bin/env Rscript

# -*- coding: utf-8 -*-
# KPIs (ICRA, IATD, ScoreFinal) — Perito vs. Brasil (resto)
# Saídas:
#   - rcheck_kpi_icra_iatd_score_<SAFE>.png
#   - rcheck_kpi_icra_iatd_score_<SAFE>.org            (imagem + comentário)
#   - rcheck_kpi_icra_iatd_score_<SAFE>_comment.org    (apenas o texto)
#   - rcheck_kpi_icra_iatd_score_<SAFE>.md             (retrocompat)
#
# Uso:
#   Rscript 07_kpi_icra_iatd_score.R --db /caminho/atestmed.db --start 2025-06-01 --end 2025-06-30 \
#           --perito "NOME DO PERITO" --out-dir /caminho/exports

suppressPackageStartupMessages({
  library(optparse)
  library(DBI); library(RSQLite)
  library(dplyr); library(tidyr); library(forcats)
  library(ggplot2); library(scales); library(stringr)
})

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
option_list <- list(
  make_option("--db",       type="character", help="Caminho do SQLite (.db)", metavar="FILE"),
  make_option("--start",    type="character", help="Data inicial YYYY-MM-DD"),
  make_option("--end",      type="character", help="Data final   YYYY-MM-DD"),
  make_option("--perito",   type="character", help="Nome do perito"),
  make_option("--out-dir",  type="character", default=NULL, help="Diretório de saída (PNG/ORG)")
)
opt <- parse_args(OptionParser(option_list = option_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
safe_slug <- function(x) {
  x <- gsub("[^A-Za-z0-9\\-_]+", "_", x)
  x <- gsub("_+", "_", x)
  x <- gsub("^_|_$", "", x)
  ifelse(nchar(x) > 0, x, "output")
}
percent_s <- function(x, acc=.1) ifelse(is.finite(x), percent(x, accuracy = acc), "NA")

# Deriva export_dir: usa --out-dir se fornecido; senão, <repo>/graphs_and_tables/exports
base_dir   <- normalizePath(file.path(dirname(opt$db), ".."))
export_dir <- if (!is.null(opt$`out-dir`)) normalizePath(opt$`out-dir`, mustWork = FALSE) else
              file.path(base_dir, "graphs_and_tables", "exports")
if (!dir.exists(export_dir)) dir.create(export_dir, recursive = TRUE, showWarnings = FALSE)

safe <- safe_slug(opt$perito)

fail_plot <- function(msg) {
  ggplot() + annotate("text", x = 0, y = 0, label = msg, size = 5) + theme_void()
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
png_base <- file.path(export_dir, sprintf("rcheck_kpi_icra_iatd_score_%s.png", safe))
org_main <- file.path(export_dir, sprintf("rcheck_kpi_icra_iatd_score_%s.org", safe))
org_comm <- file.path(export_dir, sprintf("rcheck_kpi_icra_iatd_score_%s_comment.org", safe))
md_out   <- file.path(export_dir, sprintf("rcheck_kpi_icra_iatd_score_%s.md", safe))

write_failure_orgs <- function(msg) {
  main_txt <- paste(
    "#+CAPTION: KPIs — ICRA, IATD e Score Final (distribuição) — mensagem do script",
    sprintf("[[file:%s]]", basename(png_base)),
    "",
    sprintf("*Método.* Este apêndice compara a posição do perito (%s) frente à distribuição nacional dos KPIs ICRA, IATD e Score Final, no período %s a %s. Por indisponibilidade de dados/colunas, apenas a mensagem de erro é exibida.",
            opt$perito, opt$start, opt$end),
    "",
    paste0("*Interpretação.* ", msg),
    "",
    sep = "\n"
  )
  writeLines(main_txt, org_main, useBytes = TRUE)
  writeLines(paste0(msg, "\n"), md_out, useBytes = TRUE)
  writeLines(paste(
    paste0("*Método.* Este apêndice compara a posição do perito (", opt$perito, ") frente à distribuição nacional dos KPIs ICRA, IATD e Score Final, no período ", opt$start, " a ", opt$end, "."),
    paste0("*Interpretação.* ", msg),
    "",
    sep = "\n"
  ), org_comm, useBytes = TRUE)
}

if (length(cols_ind) == 0) {
  ggsave(png_base, fail_plot("Tabela 'indicadores' não encontrada"), width=10, height=6, dpi=160)
  write_failure_orgs("Tabela 'indicadores' não encontrada no banco.")
  quit(save="no")
}

col_icra  <- pick_col(cols_ind, c("ICRA","icra","kpi_icra"))
col_iatd  <- pick_col(cols_ind, c("IATD","iatd","kpi_iatd"))
col_score <- pick_col(cols_ind, c("scoreFinal","score_final","ScoreFinal","score","scorefinal"))

needed <- c(col_icra, col_iatd, col_score)
if (any(is.na(needed))) {
  miss <- c("ICRA"  = is.na(col_icra),
            "IATD"  = is.na(col_iatd),
            "Score" = is.na(col_score))
  msg <- paste("Colunas ausentes em 'indicadores':", paste(names(miss)[miss], collapse=", "))
  ggsave(png_base, fail_plot(msg), width=10, height=6, dpi=160)
  write_failure_orgs(msg)
  quit(save="no")
}

# peritos ativos no período
sql_ativos <- "
WITH ativos AS (
  SELECT DISTINCT a.siapePerito AS siape
  FROM analises a
  WHERE date(a.dataHoraIniPericia) BETWEEN ? AND ?
)
SELECT p.siapePerito AS siape, p.nomePerito,
       i.%s AS icra, i.%s AS iatd, i.%s AS score
FROM indicadores i
JOIN peritos p ON i.perito = p.siapePerito
JOIN ativos  s ON s.siape = i.perito
"
resto_df <- dbGetQuery(con, sprintf(sql_ativos, col_icra, col_iatd, col_score),
                       params = list(opt$start, opt$end)) %>%
  mutate(across(c(icra,iatd,score), as.numeric))

if (nrow(resto_df) == 0) {
  ggsave(png_base, fail_plot("Sem peritos ativos no período."), width=10, height=6, dpi=160)
  write_failure_orgs("Sem peritos ativos no período para compor a distribuição nacional.")
  quit(save="no")
}

# separa perito alvo
this <- resto_df %>% filter(nomePerito == opt$perito) %>% slice_tail(n=1)
if (nrow(this) == 0) {
  ggsave(png_base, fail_plot("Perito sem indicador em 'indicadores'."), width=10, height=6, dpi=160)
  write_failure_orgs("Perito sem registro correspondente em 'indicadores' no período.")
  quit(save="no")
}

resto <- resto_df %>% filter(nomePerito != opt$perito)

# long para plot
dist <- resto %>%
  select(icra, iatd, score) %>%
  pivot_longer(everything(), names_to="kpi", values_to="valor") %>%
  mutate(kpi = recode(kpi, icra="ICRA", iatd="IATD", score="Score Final")) %>%
  filter(is.finite(valor))

# percentis do perito
calc_percentil <- function(x, v) {
  x <- x[is.finite(x)]
  if (!is.finite(v) || !length(x)) return(NA_real_)
  ecdf(x)(v) * 100
}
p_icra  <- calc_percentil(resto$icra,  this$icra)
p_iatd  <- calc_percentil(resto$iatd,  this$iatd)
p_score <- calc_percentil(resto$score, this$score)

# plot densidade + linha do perito
this_long <- tibble(
  kpi   = c("ICRA","IATD","Score Final"),
  valor = c(this$icra, this$iatd, this$score),
  pctl  = c(p_icra, p_iatd, p_score)
)

titulo <- sprintf("KPIs — ICRA, IATD e Score Final\n%s vs. Brasil (resto) | %s a %s",
                  opt$perito, opt$start, opt$end)

g <- ggplot(dist, aes(x = valor)) +
  geom_density(fill = "grey80", color = NA, alpha = 0.8, adjust = 1.2, na.rm = TRUE) +
  geom_vline(data=this_long, aes(xintercept = valor), linewidth=0.7) +
  geom_text(data=this_long,
            aes(x = valor, y = 0, label = sprintf("valor=%.3f\npctl=%.1f%%", valor, pctl)),
            vjust = -0.5, size = 3.3) +
  facet_wrap(~kpi, scales = "free", ncol = 1) +
  labs(title = titulo, x = NULL, y = "Densidade (Brasil)") +
  theme_minimal(base_size = 11) +
  theme(panel.grid.minor = element_blank())

ggsave(png_base, g, width = 9, height = 9, dpi = 160)
message(sprintf("✅ Figura salva: %s", png_base))

# ────────────────────────────────────────────────────────────────────────────────
# Comentários em .org (método + interpretação)
# ────────────────────────────────────────────────────────────────────────────────
icra_txt  <- ifelse(is.finite(this$icra),  sprintf("%.3f", this$icra), "NA")
iatd_txt  <- ifelse(is.finite(this$iatd),  sprintf("%.3f", this$iatd), "NA")
score_txt <- ifelse(is.finite(this$score), sprintf("%.3f", this$score), "NA")

p_icra_txt  <- ifelse(is.finite(p_icra),  sprintf("%.1f%%", p_icra),  "NA")
p_iatd_txt  <- ifelse(is.finite(p_iatd),  sprintf("%.1f%%", p_iatd),  "NA")
p_score_txt <- ifelse(is.finite(p_score), sprintf("%.1f%%", p_score), "NA")

metodo_txt <- paste0(
  "*Método.* Para o período ", opt$start, "–", opt$end,
  ", comparamos a posição do perito (", opt$perito, ") na distribuição nacional dos KPIs ",
  "ICRA, IATD e Score Final. Construímos curvas de densidade com todos os peritos ativos ",
  "no período (Brasil, exceto o perito-alvo), e marcamos o valor individual do perito com ",
  "uma linha vertical. Também calculamos o *percentil* do valor do perito em cada distribuição. ",
  "Os percentis indicam a fração de peritos com valores menores ou iguais ao observado; ",
  "interpretações de 'melhor/pior' dependem da convenção institucional de cada KPI."
)

interpreta_txt <- paste0(
  "*Interpretação.* Valores do perito: ICRA=", icra_txt, " (pctl=", p_icra_txt, "); ",
  "IATD=", iatd_txt, " (pctl=", p_iatd_txt, "); ",
  "Score Final=", score_txt, " (pctl=", p_score_txt, "). ",
  "Percentis mais altos significam que o valor do perito está mais à direita na distribuição nacional. ",
  "Use esta leitura em conjunto com o significado operacional de cada KPI para priorizar ações."
)

# .org principal (imagem + comentário)
org_main_txt <- paste(
  "#+CAPTION: KPIs — ICRA, IATD e Score Final (distribuição nacional e posição do perito)",
  sprintf("[[file:%s]]", basename(png_base)),  # make_report reescreve para ../imgs/
  "",
  metodo_txt, "",
  interpreta_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main, useBytes = TRUE)
message(sprintf("📝 Org salvo: %s", org_main))

# .org só com o comentário (para injeção no relatório)
org_comm_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comm_txt, org_comm, useBytes = TRUE)
message(sprintf("📝 Org (comentário) salvo: %s", org_comm))

# Retrocompat: resumo em Markdown
sum_md <- sprintf(
  paste0("*KPIs — %s (%s a %s)*\n",
         "- ICRA: %s  (percentil no Brasil: %s)\n",
         "- IATD: %s  (percentil no Brasil: %s)\n",
         "- Score Final: %s  (percentil no Brasil: %s)\n"),
  opt$perito, opt$start, opt$end,
  icra_txt,  p_icra_txt,
  iatd_txt,  p_iatd_txt,
  score_txt, p_score_txt
)
writeLines(sum_md, md_out, useBytes = TRUE)
message(sprintf("📝 Markdown salvo: %s", md_out))

