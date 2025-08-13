#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite); library(dplyr); library(ggplot2); library(scales)
})

# ----------------------------- CLI --------------------------------------------
opt_list <- list(
  make_option("--db",          type="character"),
  make_option("--start",       type="character"),
  make_option("--end",         type="character"),
  make_option("--perito",      type="character"),
  make_option("--threshold",   type="double", default=50),
  make_option("--out-dir",     type="character", default=NULL, help="Diretório de saída (PNG/org)")
)
opt <- parse_args(OptionParser(option_list = opt_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))

# --------------------------- helpers -------------------------------------------
safe <- function(x) { gsub("(^_+|_+$)", "", gsub("[^A-Za-z0-9_-]+","_", x)) }
perito_safe <- safe(opt$perito)

base_dir   <- normalizePath(file.path(dirname(opt$db), ".."))
export_dir <- if (!is.null(opt$`out-dir`)) normalizePath(opt$`out-dir`, mustWork = FALSE) else
  file.path(base_dir, "graphs_and_tables", "exports")
dir.create(export_dir, showWarnings = FALSE, recursive = TRUE)

percent_s <- function(x) ifelse(is.finite(x), percent(x, accuracy = .1), "NA")
num_s     <- function(x, d=2) format(round(x, d), big.mark=".", decimal.mark=",", nsmall=d, trim=TRUE)

# ------------------------------ DB --------------------------------------------
con <- dbConnect(SQLite(), opt$db)
on.exit(dbDisconnect(con), add = TRUE)

sql <- sprintf("
SELECT p.nomePerito AS perito,
       COUNT(*) AS total,
       SUM((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400) AS segs
FROM peritos p
JOIN analises a ON p.siapePerito = a.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
GROUP BY p.nomePerito
", opt$start, opt$end)

df <- dbGetQuery(con, sql)

# --------------------------- cálculo ------------------------------------------
df <- df %>%
  mutate(prod = ifelse(is.na(segs) | segs <= 0, NA_real_, total / (segs/3600)))

if (!(opt$perito %in% df$perito)) {
  sim <- df %>% filter(grepl(opt$perito, perito, ignore.case = TRUE)) %>% pull(perito)
  msg <- if (length(sim)) paste0(" Peritos semelhantes: ", paste(sim, collapse=", "), ".") else ""
  stop(sprintf("Perito '%s' não encontrado no período.%s", opt$perito, msg))
}

p_val <- df %>% filter(perito==opt$perito) %>% pull(prod) %>% .[1]
vals  <- df %>% filter(is.finite(prod)) %>% pull(prod)
n_eff <- length(vals)

# percentil empírico do perito na distribuição
pctile <- if (is.finite(p_val) && n_eff > 0) mean(vals <= p_val) else NA_real_
share_above_thr <- if (n_eff > 0) mean(vals >= opt$threshold) else NA_real_

# ----------------------------- gráfico -----------------------------------------
gg <- ggplot(data.frame(prod = vals), aes(prod)) +
  geom_histogram(bins=40, fill="#1f77b4", alpha=.85) +
  geom_vline(xintercept = p_val,    color="#d62728", size=1, na.rm = TRUE) +
  geom_vline(xintercept = opt$threshold, color="#2ca02c", linetype="dashed") +
  labs(
    title    = "Produtividade (análises/h) — distribuição nacional",
    subtitle = sprintf("%s a %s | perito=%s (%s/h) | threshold=%.0f/h",
                       opt$start, opt$end, opt$perito,
                       ifelse(is.finite(p_val), num_s(p_val,2), "NA"), opt$threshold),
    x="análises/h", y="freq."
  ) +
  theme_minimal(base_size=11)

png_path <- file.path(export_dir, sprintf("rcheck_productivity_%s.png", perito_safe))
ggsave(png_path, gg, width=8, height=5, dpi=160)
cat(sprintf("✓ salvo: %s\n", png_path))

# ------------------------ comentários em .org ----------------------------------
metodo_txt <- sprintf(
  paste0(
    "*Método.* Para cada perito, calculamos *produtividade* = total de análises ÷ horas trabalhadas, ",
    "onde horas = (∑ duração das perícias) e a duração é (dataHoraFim − dataHoraIni) em segundos. ",
    "Construímos a *distribuição nacional* de produtividades no período %s a %s e destacamos o perito alvo ",
    "com uma linha vertical. Também marcamos o *limiar* de %.0f análises/h (linha tracejada). ",
    "Relatamos o *percentil empírico* do perito na distribuição e a fração de peritos com ",
    "produtividade ≥ limiar (proporção acima do threshold)."
  ),
  opt$start, opt$end, opt$threshold
)

interpret_txt <- {
  dir_txt <- if (is.finite(p_val)) {
    if      (p_val >  opt$threshold) "acima do limiar"
    else if (p_val <  opt$threshold) "abaixo do limiar"
    else                             "no limiar"
  } else "indeterminado (sem tempo total válido)"
  pct_txt  <- if (is.finite(pctile)) sprintf("percentil ≈ %s", percent_s(pctile)) else "percentil indisponível"
  share_txt<- if (is.finite(share_above_thr)) sprintf("%s dos peritos ≥ limiar", percent_s(share_above_thr)) else "proporção ≥ limiar indisponível"
  sprintf(
    "*Interpretação.* A produtividade do perito é %s (≈ %s/h). %s. %s.",
    dir_txt, ifelse(is.finite(p_val), num_s(p_val,2), "NA"), pct_txt, share_txt
  )
}

# 1) .org principal (imagem + texto; opcional)
org_main <- file.path(export_dir, sprintf("rcheck_productivity_%s.org", perito_safe))
org_main_txt <- paste(
  "#+CAPTION: Produtividade — distribuição nacional com destaque do perito",
  sprintf("[[file:%s]]", basename(png_path)),  # make_report ajusta para ../imgs/
  "",
  metodo_txt, "",
  interpret_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
cat(sprintf("✓ org: %s\n", org_main))

# 2) .org somente com o comentário (é este que o make_report injeta no PDF)
org_comment <- file.path(export_dir, sprintf("rcheck_productivity_%s_comment.org", perito_safe))
org_comment_txt <- paste(metodo_txt, "", interpret_txt, "", sep = "\n")
writeLines(org_comment_txt, org_comment)
cat(sprintf("✓ org(comment): %s\n", org_comment))

