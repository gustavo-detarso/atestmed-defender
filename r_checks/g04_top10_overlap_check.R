#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI); library(RSQLite)
  library(ggplot2); library(dplyr); library(lubridate); library(purrr); library(stringr); library(scales)
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

# ───────────────── Args e paths ─────────────────
args    <- parse_args()
db_path <- args$db
start_d <- args$start
end_d   <- args$end
min_n   <- as.integer(args[["min-analises"]] %||% "50")
out_dir <- args[["out-dir"]]

if (is.null(db_path) || is.null(start_d) || is.null(end_d)) {
  stop("Uso: --db <path> --start YYYY-MM-DD --end YYYY-MM-DD [--min-analises 50] [--out-dir <dir>]")
}

base_dir   <- normalizePath(file.path(dirname(db_path), ".."))
export_dir <- if (!is.null(out_dir)) normalizePath(out_dir, mustWork = FALSE) else
  file.path(base_dir, "graphs_and_tables", "exports")
ensure_dir(export_dir)

png_file <- file.path(export_dir, "rcheck_top10_overlap.png")
org_main <- file.path(export_dir, "rcheck_top10_overlap.org")
org_comm <- file.path(export_dir, "rcheck_top10_overlap_comment.org")

# ─────────────── Conexão e schema ───────────────
con <- dbConnect(RSQLite::SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

table_exists <- function(con, name) {
  nrow(dbGetQuery(con, "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
                  params=list(name))) > 0
}
detect_analises_table <- function(con) {
  for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t)
  stop("Não encontrei 'analises' nem 'analises_atestmed'.")
}
a_tbl <- detect_analises_table(con)

# Detecta colunas da tabela de análises detectada
cols <- dbGetQuery(con, sprintf("PRAGMA table_info(%s)", a_tbl))$name
cand_dur <- intersect(cols, c("tempoAnaliseSeg","tempoAnalise","duracaoSegundos","duracao_seg","tempo_seg"))
dur_col  <- if (length(cand_dur)>0) cand_dur[[1]] else NA_character_
has_end  <- "dataHoraFimPericia" %in% cols
has_ini  <- "dataHoraIniPericia" %in% cols
if (!has_ini) {
  ggsave(png_file, fail_plot("Coluna 'dataHoraIniPericia' não encontrada"), width=9, height=5, dpi=150)
  quit(save="no")
}

# ─────────────── Seleção Top 10 pelo ScoreFinal ───────────────
if (!table_exists(con, "indicadores")) {
  ggsave(png_file, fail_plot("Tabela 'indicadores' não encontrada"), width=9, height=5, dpi=150)
  quit(save="no")
}
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
  ggsave(png_file, fail_plot("Sem Top 10 para o período/critério"), width=9, height=5, dpi=150)
  quit(save="no")
}
peritos_in <- paste(sprintf("'%s'", gsub("'", "''", top10$nomePerito)), collapse=",")

# ─────────────── Carrega janela temporal por perito ───────────────
# 1) Se existir 'fim': usar ini/fim.
# 2) Se não existir 'fim' e existir duração: fim = ini + duração (segundos).
sel_cols <- c("p.nomePerito AS nomePerito", "a.dataHoraIniPericia AS ini")
if (has_end) sel_cols <- c(sel_cols, "a.dataHoraFimPericia AS fim")
if (!has_end && !is.na(dur_col)) sel_cols <- c(sel_cols, sprintf("CAST(a.%s AS REAL) AS dur", dur_col))

qry <- sprintf("
SELECT %s
  FROM %s a
  JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
   AND p.nomePerito IN (%s)
", paste(sel_cols, collapse=", "), a_tbl, peritos_in)

df <- dbGetQuery(con, qry, params = list(start_d, end_d))
if (nrow(df) == 0) {
  ggsave(png_file, fail_plot("Sem timestamps para sobreposição"), width=9, height=5, dpi=150)
  quit(save="no")
}

# Parse e construção de 'fim' quando necessário
df$ini <- suppressWarnings(ymd_hms(df$ini, quiet=TRUE))
if ("fim" %in% names(df)) df$fim <- suppressWarnings(ymd_hms(df$fim, quiet=TRUE))
if (!("fim" %in% names(df)) && "dur" %in% names(df)) {
  df$fim <- df$ini + dseconds(suppressWarnings(as.numeric(df$dur)))
}

# Se não for possível calcular 'fim', aborta com plot informativo
if (!"fim" %in% names(df) || all(!is.finite(as.numeric(df$fim)))) {
  ggsave(png_file, fail_plot("Sem dado de fim/duração parsável para calcular sobreposição"), width=9, height=5, dpi=150)
  quit(save="no")
}

# Limpeza e filtro de intervalos válidos
df <- df %>%
  filter(!is.na(ini), !is.na(fim), fim >= ini) %>%
  mutate(dur_s = as.numeric(difftime(fim, ini, units = "secs"))) %>%
  filter(is.finite(dur_s), dur_s > 0, dur_s <= 3600)

if (nrow(df) == 0) {
  ggsave(png_file, fail_plot("Todas as janelas inválidas (necessário 0<dur≤3600s)."), width=9, height=5, dpi=150)
  quit(save="no")
}

# ─────────────── Cálculo de sobreposição (share de tarefas) ───────────────
overlap_share <- function(tb) {
  tb <- tb %>% arrange(ini, fim)
  n  <- nrow(tb)
  if (n <= 1) return(0)
  overl <- logical(n)
  last_end <- tb$fim[1]
  for (i in 2:n) {
    overl[i] <- tb$ini[i] < last_end
    last_end <- max(last_end, tb$fim[i], na.rm = TRUE)
  }
  mean(overl, na.rm = TRUE) * 100
}

res <- df %>%
  group_by(nomePerito) %>%
  group_modify(~tibble(pct_overlap = overlap_share(.x), total = nrow(.x))) %>%
  ungroup() %>%
  arrange(desc(pct_overlap)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito))

# ─────────────── Plot ───────────────
p <- ggplot(res, aes(x = nomePerito, y = pct_overlap)) +
  geom_col(fill = "#1f77b4") +
  geom_text(aes(label = sprintf("%.1f%% (n=%d)", pct_overlap, total)),
            vjust = -0.3, size = 3) +
  labs(
    title = "Top 10 — Tarefas sobrepostas (%)",
    subtitle = sprintf("%s a %s | mínimo de análises: %d", start_d, end_d, min_n),
    x = "Perito", y = "% sobrepostas",
    caption = "Janela válida por tarefa: 0 < (fim − ini) ≤ 3600s. Fim = coluna fim quando existe; caso contrário, ini + duração (s)."
  ) +
  theme_minimal(base_size = 11) +
  theme(axis.text.x = element_text(angle=45, hjust=1))

ymax <- suppressWarnings(max(res$pct_overlap, na.rm = TRUE)); if (!is.finite(ymax)) ymax <- 1
ggsave(png_file, p + coord_cartesian(ylim = c(0, ymax * 1.15)), width=9, height=5, dpi=150)
cat(sprintf("✅ Figura salva: %s\n", png_file))

# ─────────────── Comentários (.org) ───────────────
top3 <- res %>% slice_head(n = min(3, n())) %>%
  transmute(txt = sprintf("%s: %.1f%% (n=%d)", as.character(nomePerito), pct_overlap, total))

rng_pct <- range(res$pct_overlap, na.rm = TRUE)
media   <- mean(res$pct_overlap, na.rm = TRUE)
mediana <- median(res$pct_overlap, na.rm = TRUE)

metodo_txt <- paste0(
  "*Método.* Selecionamos os *10 piores* por *ScoreFinal* (", start_d, " a ", end_d,
  "), exigindo ao menos *", min_n, "* análises. ",
  "Construímos intervalos [ini, fim] por tarefa e calculamos o *percentual de tarefas sobrepostas* ",
  "como a fração de tarefas cujo início ocorre *antes* do fim da janela acumulada anterior ",
  "(dados ordenados por início). Consideramos *válidas* apenas janelas com *0 < (fim−ini) ≤ 3600s*. ",
  "Quando não há coluna de fim, usamos *ini + duração (s)*; caso contrário, a própria coluna de fim."
)

interpreta_txt <- paste0(
  "*Interpretação.* Entre os Top 10, a sobreposição variou de ",
  sprintf("%.1f%%", min(rng_pct)), " a ", sprintf("%.1f%%", max(rng_pct)),
  "; média=", sprintf("%.1f%%", media), ", mediana=", sprintf("%.1f%%", mediana), ".\n",
  if (nrow(top3)) paste0("- Maiores shares: ", paste(top3$txt, collapse = "; "), ".\n") else "",
  "Valores muito altos podem indicar *concorrência* ou *registro simultâneo*. ",
  "Use como *sinal de alerta* para auditoria qualitativa, considerando volume (n) e contexto."
)

org_main_txt <- paste(
  "#+CAPTION: Top 10 — Tarefas sobrepostas (%)",
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

