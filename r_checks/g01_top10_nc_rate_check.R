#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(ggplot2)
  library(dplyr)
  library(stringr)
  library(scales)
})

`%||%` <- function(a,b) if (is.null(a)) b else a

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  kv <- list()
  i <- 1
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

# ----------------------------------------------------------------------
# Args e paths
# ----------------------------------------------------------------------
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

png_file <- file.path(export_dir, "rcheck_top10_nc_rate.png")
org_main <- file.path(export_dir, "rcheck_top10_nc_rate.org")
org_comm <- file.path(export_dir, "rcheck_top10_nc_rate_comment.org")

# ----------------------------------------------------------------------
# Conexão e helpers de schema
# ----------------------------------------------------------------------
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

# ----------------------------------------------------------------------
# Top 10 pelo critério: scoreFinal DESC, mínimo de análises no período
# ----------------------------------------------------------------------
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

# ----------------------------------------------------------------------
# %NC (robusto) por perito — apenas para os Top 10
# ----------------------------------------------------------------------
peritos <- paste(sprintf("'%s'", gsub("'", "''", top10$nomePerito)), collapse=",")

nc_expr <- "
CASE
  WHEN CAST(IFNULL(a.conformado,1) AS INTEGER)=0 THEN 1
  WHEN TRIM(IFNULL(a.motivoNaoConformado,'')) <> ''
       AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0 THEN 1
  ELSE 0
END
"

qry_nc <- sprintf("
SELECT p.nomePerito,
       COUNT(*) AS total,
       SUM(%s) AS nc
  FROM %s a
  JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
   AND p.nomePerito IN (%s)
 GROUP BY p.nomePerito
", nc_expr, a_tbl, peritos)

df <- dbGetQuery(con, qry_nc, params = list(start_d, end_d))

if (nrow(df) == 0) {
  ggsave(png_file, fail_plot("Sem dados de NC para o Top 10"), width=9, height=5, dpi=150)
  quit(save="no")
}

df <- df %>%
  mutate(prop_nc = ifelse(total > 0, nc/total, NA_real_),
         pct_nc  = prop_nc * 100) %>%
  arrange(desc(pct_nc)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito))

# ----------------------------------------------------------------------
# Plot
# ----------------------------------------------------------------------
p <- ggplot(df, aes(x = nomePerito, y = pct_nc)) +
  geom_col(fill = "#d62728") +
  geom_text(aes(label = sprintf("%.1f%% (n=%d/%d)", pct_nc, nc, total)),
            vjust = -0.3, size = 3) +
  labs(
    title = "Top 10 — Taxa de Não Conformidade (NC robusto) [%]",
    subtitle = sprintf("%s a %s | mínimo de análises: %d", start_d, end_d, min_n),
    x = "Perito", y = "% NC (robusto)",
    caption = "NC robusto: conformado=0 OU (motivoNaoConformado ≠ '' E CAST(motivoNaoConformado) ≠ 0)."
  ) +
  theme_minimal(base_size = 11) +
  theme(axis.text.x = element_text(angle=45, hjust=1))

ymax <- suppressWarnings(max(df$pct_nc, na.rm = TRUE))
if (!is.finite(ymax)) ymax <- 0
ggsave(png_file, p + coord_cartesian(ylim = c(0, ymax * 1.15)),
       width=9, height=5, dpi=150)
cat(sprintf("✅ Figura salva: %s\n", png_file))

# ----------------------------------------------------------------------
# Comentário .org (método + interpretação)
# ----------------------------------------------------------------------
ord <- df %>% arrange(desc(prop_nc))
top3 <- ord %>% slice_head(n = min(3, n())) %>%
  transmute(txt = sprintf("%s: %s (n=%d/%d)",
                          as.character(nomePerito),
                          percent_s(prop_nc, .1), nc, total))
rng  <- range(df$prop_nc, na.rm = TRUE)
media <- mean(df$prop_nc, na.rm = TRUE)
mediana <- median(df$prop_nc, na.rm = TRUE)

metodo_txt <- paste0(
  "*Método.* Selecionamos os *10 piores* peritos no período (", start_d, " a ", end_d,
  ") pelo *ScoreFinal* em `indicadores`, exigindo ao menos *", min_n, "* análises. ",
  "Para cada um deles, calculamos a *taxa de NC robusto* (conformado=0 OU motivo≠'' e CAST(...)≠0) ",
  "com base em ", shQuote(a_tbl), ". O gráfico exibe o *ranking* por %NC entre esses 10."
)

interpreta_txt <- paste0(
  "*Interpretação.* Entre os Top 10, a %NC variou de ",
  percent_s(min(rng), .1), " a ", percent_s(max(rng), .1),
  "; média=", percent_s(media, .1), ", mediana=", percent_s(mediana, .1), ".\n",
  if (nrow(top3)) paste0("- Maiores %NC: ", paste(top3$txt, collapse = "; "), ".\n") else "",
  "Observação: 'Top 10' é definido pelo *ScoreFinal* (não necessariamente pelas maiores %NC). ",
  "Use estes resultados como *pistas* para auditoria, levando em conta o volume (n) e o contexto."
)

org_main_txt <- paste(
  "#+CAPTION: Top 10 — %NC (robusto) no período",
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

