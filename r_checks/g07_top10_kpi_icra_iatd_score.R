#!/usr/bin/env Rscript

# -*- coding: utf-8 -*-
# KPIs (ICRA, IATD, ScoreFinal) — Top 10 (grupo) vs. Brasil (resto)
# Saídas:
#   - rcheck_top10_kpi_icra_iatd_score.png
#   - rcheck_top10_kpi_icra_iatd_score.org            (imagem + comentário)
#   - rcheck_top10_kpi_icra_iatd_score_comment.org    (apenas comentário)

suppressPackageStartupMessages({
  library(optparse)
  library(DBI); library(RSQLite)
  library(dplyr); library(tidyr); library(forcats)
  library(ggplot2); library(scales); library(stringr)
})

# ───────────────────────── CLI ─────────────────────────
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

# ───────────────────────── Helpers ─────────────────────────
fail_plot <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()

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

table_exists <- function(con, name) {
  nrow(dbGetQuery(con, "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
                  params=list(name))) > 0
}
detect_analises_table <- function(con) {
  for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t)
  stop("Não encontrei 'analises' nem 'analises_atestmed'.")
}

# ───────────────────────── Conexão/colunas ─────────────────────────
con <- dbConnect(RSQLite::SQLite(), opt$db)
on.exit(try(dbDisconnect(con), silent = TRUE))

if (!table_exists(con, "indicadores")) {
  out_png <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.png")
  ggsave(out_png, fail_plot("Tabela 'indicadores' não encontrada"), width=8.5, height=10, dpi=160)
  quit(save="no")
}

a_tbl <- detect_analises_table(con)

cols_ind <- dbGetQuery(con, "PRAGMA table_info(indicadores)")$name
col_icra  <- pick_col(cols_ind, c("ICRA","icra","kpi_icra"))
col_iatd  <- pick_col(cols_ind, c("IATD","iatd","kpi_iatd"))
col_score <- pick_col(cols_ind, c("scoreFinal","score_final","ScoreFinal","score","scorefinal"))

out_png  <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.png")
org_main <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score.org")
org_comm <- file.path(opt$`out-dir`, "rcheck_top10_kpi_icra_iatd_score_comment.org")

needed <- c(col_icra, col_iatd, col_score)
if (any(is.na(needed))) {
  miss <- c("ICRA"  = is.na(col_icra),
            "IATD"  = is.na(col_iatd),
            "Score" = is.na(col_score))
  msg <- paste("Colunas ausentes em 'indicadores':", paste(names(miss)[miss], collapse=", "))
  ggsave(out_png, fail_plot(msg), width=8.5, height=10, dpi=160)
  writeLines(paste0("*Erro.* ", msg), org_main)
  writeLines(paste0("*Erro.* ", msg), org_comm)
  quit(save="no")
}

# ───────────────────────── Top 10 por Score ─────────────────────────
sql_top10 <- sprintf("
SELECT p.nomePerito AS nomePerito, i.%s AS scoreFinal, COUNT(a.protocolo) AS total_analises
  FROM indicadores i
  JOIN peritos p ON i.perito = p.siapePerito
  JOIN %s a ON a.siapePerito = i.perito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
 GROUP BY p.nomePerito, i.%s
HAVING total_analises >= ?
 ORDER BY i.%s DESC, total_analises DESC
 LIMIT 10;
", col_score, a_tbl, col_score, col_score)

top10 <- dbGetQuery(con, sql_top10, params = list(opt$start, opt$end, opt$`min-analises`))
if (nrow(top10) == 0) {
  ggsave(out_png, fail_plot("Nenhum Top 10 para o período/critério."), width=8.5, height=10, dpi=160)
  writeLines("*Sem Top 10 no período/critério informado.*", org_main)
  writeLines("*Sem Top 10 no período/critério informado.*", org_comm)
  quit(save="no")
}
top10_set <- unique(top10$nomePerito)

# ───────────────────────── Coleta KPIs para ativos ─────────────────────────
sql_ativos <- sprintf("
WITH ativos AS (
  SELECT DISTINCT a.siapePerito AS siape
    FROM %s a
   WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
)
SELECT p.nomePerito,
       CAST(i.%s AS REAL) AS icra,
       CAST(i.%s AS REAL) AS iatd,
       CAST(i.%s AS REAL) AS score
  FROM indicadores i
  JOIN peritos p ON i.perito = p.siapePerito
  JOIN ativos  s ON s.siape   = i.perito
", a_tbl, col_icra, col_iatd, col_score)

df <- dbGetQuery(con, sql_ativos, params = list(opt$start, opt$end)) %>%
  mutate(grupo = if_else(nomePerito %in% top10_set, "Top10", "Resto"))

if (nrow(df) == 0) {
  ggsave(out_png, fail_plot("Sem peritos ativos no período."), width=8.5, height=10, dpi=160)
  writeLines("*Sem peritos ativos no período informado.*", org_main)
  writeLines("*Sem peritos ativos no período informado.*", org_comm)
  quit(save="no")
}

# ───────────────────────── Long + testes ─────────────────────────
long <- df %>%
  select(grupo, icra, iatd, score) %>%
  pivot_longer(cols = -grupo, names_to = "kpi", values_to = "valor") %>%
  mutate(kpi = recode(kpi, icra="ICRA", iatd="IATD", score="Score Final")) %>%
  filter(is.finite(valor))

if (nrow(long) == 0) {
  ggsave(out_png, fail_plot("Sem valores numéricos de KPIs para comparar."), width=8.5, height=10, dpi=160)
  writeLines("*Sem valores numéricos de KPIs para comparar.*", org_main)
  writeLines("*Sem valores numéricos de KPIs para comparar.*", org_comm)
  quit(save="no")
}

tests <- long %>%
  group_by(kpi) %>%
  summarise(
    p_wilcox = tryCatch({
      if (length(unique(grupo)) < 2) NA_real_ else
        unname(wilcox.test(valor ~ grupo, exact = FALSE)$p.value)
    }, error = function(e) NA_real_),
    n_top10   = sum(grupo=="Top10"),
    n_resto   = sum(grupo=="Resto"),
    med_top10 = median(valor[grupo=="Top10"], na.rm=TRUE),
    med_resto = median(valor[grupo=="Resto"], na.rm=TRUE),
    .groups = "drop"
  ) %>%
  mutate(lbl = sprintf("p=%.3g | med Top10=%.3f | med Resto=%.3f | n=(%d,%d)",
                       p_wilcox, med_top10, med_resto, n_top10, n_resto))

# ───────────────────────── Plot ─────────────────────────
titulo <- sprintf("Top 10 — KPIs (ICRA, IATD, Score Final) vs. Brasil (resto)\n%s a %s",
                  opt$start, opt$end)

g <- ggplot(long, aes(x = grupo, y = valor, fill = grupo)) +
  geom_boxplot(outlier.shape = NA, width = 0.55) +
  geom_jitter(width = 0.12, alpha = 0.5, size = 1.6) +
  facet_wrap(~kpi, scales = "free_y", ncol = 1) +
  labs(
    title = titulo, x = NULL, y = NULL,
    subtitle = paste(tests$kpi, tests$lbl, collapse = "   |   "),
    caption = sprintf("Top 10 por %s (desc), exigindo ao menos %d análises no período. Teste: Mann-Whitney por KPI.",
                      col_score, opt$`min-analises`)
  ) +
  theme_minimal(base_size = 11) +
  theme(panel.grid.minor = element_blank(),
        legend.position = "none")

ggsave(out_png, g, width = 8.5, height = 10, dpi = 160)
message(sprintf("✅ Figura salva: %s", out_png))

# ───────────────────────── Comentários (.org) ─────────────────────────
# Método
metodo_txt <- paste0(
  "*Método.* Selecionamos os *Top 10 piores* por *", col_score, "* em `indicadores`, ",
  "exigindo ao menos *", opt$`min-analises`, "* análises no período (", opt$start, " a ", opt$end, "). ",
  "Definimos o grupo *Resto* como peritos *ativos* no período (possuem alguma análise em ", shQuote(a_tbl), "). ",
  "Comparamos *ICRA*, *IATD* e *Score Final* entre *Top10* e *Resto* usando o teste *Mann-Whitney* (não-paramétrico)."
)

# Interpretação resumida por KPI
kpi_lines <- apply(tests, 1, function(r){
  sprintf("- %s: med Top10=%.3f vs med Resto=%.3f (p=%s; nTop10=%s, nResto=%s)",
          r[["kpi"]], as.numeric(r[["med_top10"]]), as.numeric(r[["med_resto"]]),
          formatC(as.numeric(r[["p_wilcox"]]), format="fg", digits=3),
          r[["n_top10"]], r[["n_resto"]])
})
interpreta_txt <- paste0(
  "*Interpretação.* As medianas e p-valores resumem diferenças entre grupos por KPI. ",
  "Leve em conta o tamanho amostral de cada grupo e a direção esperada de cada métrica no seu contexto.\n",
  paste(kpi_lines, collapse = "\n")
)

# .org principal (imagem + texto)
org_main_txt <- paste(
  "#+CAPTION: KPIs (ICRA, IATD, Score Final) — Top 10 vs. Brasil (resto)",
  sprintf("[[file:%s]]", basename(out_png)),
  "",
  metodo_txt, "",
  interpreta_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
message(sprintf("✅ Org salvo: %s", org_main))

# .org apenas comentário
org_comm_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comm_txt, org_comm)
message(sprintf("✅ Org(comment) salvo: %s", org_comm))

