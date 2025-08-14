#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI); library(RSQLite)
  library(ggplot2); library(dplyr); library(lubridate); library(scales); library(stringr)
})

`%||%` <- function(a,b) if (is.null(a)) b else a

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  kv <- list(); i <- 1
  while (i <= length(args)) {
    k <- args[[i]]
    if (startsWith(k, "--")) {
      v <- if (i + 1 <= length(args) && !startsWith(args[[i+1]], "--")) args[[i+1]] else TRUE
      kv[[substr(k, 3, nchar(k))]] <- v
      i <- i + if (isTRUE(v) || identical(v, TRUE)) 1 else 2
    } else i <- i + 1
  }
  kv
}

ensure_dir <- function(p) if (!dir.exists(p)) dir.create(p, recursive = TRUE, showWarnings = FALSE)
fail_plot <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()

safe_slug <- function(x){
  x <- gsub("[^A-Za-z0-9\\-_]+","_", x); x <- gsub("_+","_", x); x <- gsub("^_|_$","", x)
  ifelse(nchar(x)>0, x, "output")
}
percent_s <- function(x, acc=.1) ifelse(is.finite(x), percent(x, accuracy=acc), "NA")

# ───────────────────────── Args/paths ─────────────────────────
args    <- parse_args()
db_path <- args$db; start_d <- args$start; end_d <- args$end
min_n   <- as.integer(args[["min-analises"]] %||% "50")
thr     <- as.numeric(args[["threshold"]] %||% "15")
out_dir <- args[["out-dir"]]

if (is.null(db_path) || is.null(start_d) || is.null(end_d)) {
  stop("Uso: --db <path> --start YYYY-MM-DD --end YYYY-MM-DD [--min-analises 50] [--threshold 15] [--out-dir <dir>]")
}

base_dir   <- normalizePath(file.path(dirname(db_path), ".."))
export_dir <- if (!is.null(out_dir)) normalizePath(out_dir, mustWork = FALSE) else
  file.path(base_dir, "graphs_and_tables", "exports")
ensure_dir(export_dir)

png_file <- file.path(export_dir, "rcheck_top10_le15s.png")
org_main <- file.path(export_dir, "rcheck_top10_le15s.org")
org_comm <- file.path(export_dir, "rcheck_top10_le15s_comment.org")

# ───────────────────────── Conexão/schema ─────────────────────
con <- dbConnect(RSQLite::SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

table_exists <- function(con, name) {
  nrow(dbGetQuery(con,
                  "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
                  params=list(name))) > 0
}
detect_analises_table <- function(con) {
  for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t)
  stop("Não encontrei 'analises' nem 'analises_atestmed'.")
}

a_tbl <- detect_analises_table(con)
if (!table_exists(con, "indicadores")) {
  ggsave(png_file, fail_plot("Tabela 'indicadores' não encontrada"), width=9, height=5, dpi=150)
  quit(save="no")
}

# Top 10 (ScoreFinal DESC, mínimo de análises no período)
qry_top10 <- sprintf("
SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
  FROM indicadores i
  JOIN peritos   p ON i.perito = p.siapePerito
  JOIN %s  a ON a.siapePerito = i.perito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
 GROUP BY p.nomePerito, i.scoreFinal
HAVING total_analises >= ?
 ORDER BY i.scoreFinal DESC, total_analises DESC
 LIMIT 10
", a_tbl)

top10 <- dbGetQuery(con, qry_top10, params = list(start_d, end_d, min_n))
if (nrow(top10) == 0) {
  ggsave(png_file, fail_plot("Sem Top 10 para o período/critério"), width=9, height=5, dpi=150); quit(save="no")
}
peritos_in <- paste(sprintf("'%s'", gsub("'", "''", top10$nomePerito)), collapse=",")

# ───────────────────────── Duração ≤ threshold ─────────────────
# 1) tenta coluna de duração; 2) fallback: julianday(fim) - julianday(ini)
cols <- dbGetQuery(con, sprintf("PRAGMA table_info(%s)", a_tbl))$name
cand <- intersect(cols, c("tempoAnaliseSeg", "tempoAnalise", "duracaoSegundos", "duracao_seg", "tempo_seg"))
dur_col <- if (length(cand) > 0) cand[[1]] else NA_character_

if (is.na(dur_col)) {
  qry <- sprintf("
    SELECT p.nomePerito AS nomePerito,
           ((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400.0) AS dur
      FROM %s a
      JOIN peritos  p ON a.siapePerito = p.siapePerito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
       AND a.dataHoraIniPericia IS NOT NULL
       AND a.dataHoraFimPericia   IS NOT NULL
       AND p.nomePerito IN (%s)
  ", a_tbl, peritos_in)
  df <- dbGetQuery(con, qry, params = list(start_d, end_d))
} else {
  qry <- sprintf("
    SELECT p.nomePerito AS nomePerito, CAST(a.%s AS REAL) AS dur
      FROM %s a
      JOIN peritos  p ON a.siapePerito = p.siapePerito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
       AND p.nomePerito IN (%s)
  ", dur_col, a_tbl, peritos_in)
  df <- dbGetQuery(con, qry, params = list(start_d, end_d))
}

if (nrow(df) == 0) {
  ggsave(png_file, fail_plot("Sem dados de duração para o Top 10"), width=9, height=5, dpi=150); quit(save="no")
}

# Considera apenas durações válidas no denominador: 0 < dur ≤ 3600
df <- df %>%
  mutate(dur = suppressWarnings(as.numeric(dur))) %>%
  group_by(nomePerito) %>%
  summarise(
    n_valid = sum(is.finite(dur) & dur > 0 & dur <= 3600, na.rm = TRUE),
    n_le    = sum(is.finite(dur) & dur > 0 & dur <= 3600 & dur <= thr, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(prop = ifelse(n_valid > 0, n_le / n_valid, NA_real_),
         pct  = prop * 100) %>%
  arrange(desc(pct)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito))

if (all(is.na(df$prop))) {
  ggsave(png_file, fail_plot("Todas as durações inválidas (0<dur≤3600) ou ausentes."), width=9, height=5, dpi=150)
  quit(save="no")
}

# ───────────────────────── Plot ────────────────────────────────
p <- ggplot(df, aes(x = nomePerito, y = pct)) +
  geom_col(fill = "#1f77b4") +
  geom_text(aes(label = sprintf("%.1f%% (n=%d/%d)", pct, n_le, n_valid)),
            vjust = -0.3, size = 3) +
  labs(
    title    = sprintf("Top 10 — Perícias ≤ %.0fs (%%)", thr),
    subtitle = sprintf("%s a %s | mínimo de análises: %d", start_d, end_d, min_n),
    x = "Perito", y = sprintf("%% ≤ %.0fs (entre válidas)", thr),
    caption  = "Duração válida: 0 < dur ≤ 3600s. Quando não há coluna de duração, usa-se (julianday(fim)-julianday(ini))*86400."
  ) +
  theme_minimal(base_size = 11) +
  theme(axis.text.x = element_text(angle=45, hjust=1))

ymax <- suppressWarnings(max(df$pct, na.rm = TRUE)); if (!is.finite(ymax)) ymax <- 0
ggsave(png_file, p + coord_cartesian(ylim = c(0, ymax * 1.15)), width=9, height=5, dpi=150)
cat(sprintf("✅ Figura salva: %s\n", png_file))

# ───────────────────────── Comentários (.org) ─────────────────
ord  <- df %>% arrange(desc(prop))
top3 <- ord %>% slice_head(n = min(3, n())) %>%
  transmute(txt = sprintf("%s: %s (n=%d/%d)",
                          as.character(nomePerito),
                          percent_s(prop, .1), n_le, n_valid))
rng     <- range(df$prop, na.rm = TRUE)
media   <- mean(df$prop, na.rm = TRUE)
mediana <- median(df$prop, na.rm = TRUE)

metodo_txt <- paste0(
  "*Método.* Selecionamos os *10 piores* peritos no período (", start_d, " a ", end_d,
  ") pelo *ScoreFinal* em `indicadores`, exigindo ao menos *", min_n, "* análises. ",
  "Para cada um deles, calculamos a *proporção de perícias com duração ≤ ", thr, "s* ",
  "entre *durações válidas* (0<dur≤3600). ",
  "Quando não há coluna explícita de duração em ", shQuote(a_tbl), ", usa-se ",
  "(julianday(fim)−julianday(início))*86400."
)

interpreta_txt <- paste0(
  "*Interpretação.* Entre os Top 10, a fração ≤ ", thr, "s variou de ",
  percent_s(min(rng), .1), " a ", percent_s(max(rng), .1),
  "; média=", percent_s(media, .1), ", mediana=", percent_s(mediana, .1), ".\n",
  if (nrow(top3)) paste0("- Maiores %≤", thr, "s: ", paste(top3$txt, collapse = "; "), ".\n") else "",
  "Valores muito altos podem indicar *execuções excessivamente rápidas* e merecem auditoria; ",
  "sempre considere o volume (n) e o contexto das tarefas."
)

org_main_txt <- paste(
  sprintf("#+CAPTION: Top 10 — %% ≤ %.0fs (entre válidas)", thr),
  sprintf("[[file:%s]]", basename(png_file)),
  "",
  metodo_txt, "",
  interpreta_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
cat(sprintf("✅ Org salvo: %s\n", org_main))

org_comm_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comm_txt, org_comm)
cat(sprintf("✅ Org(comment) salvo: %s\n", org_comm))

