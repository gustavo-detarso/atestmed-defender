#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite); library(dplyr); library(ggplot2); library(scales)
})

# ----------------------------- CLI --------------------------------------------
opt_list <- list(
  make_option("--db",        type="character", help="Caminho do SQLite"),
  make_option("--start",     type="character", help="AAAA-MM-DD"),
  make_option("--end",       type="character", help="AAAA-MM-DD"),
  make_option("--perito",    type="character", help="Nome do perito"),
  make_option("--out-dir",   type="character", default=NULL, help="Diretório de saída (PNG/org)")
)
opt <- parse_args(OptionParser(option_list = opt_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))

# --------------------------- helpers -------------------------------------------
safe <- function(x) { gsub("(^_+|_+$)", "", gsub("[^A-Za-z0-9_-]+","_", x)) }
perito_safe <- safe(opt$perito)

# Deriva EXPORT_DIR a partir do DB (fallback) OU usa --out-dir
base_dir   <- normalizePath(file.path(dirname(opt$db), ".."))
export_dir <- if (!is.null(opt$`out-dir`)) normalizePath(opt$`out-dir`, mustWork = FALSE) else
              file.path(base_dir, "graphs_and_tables", "exports")
dir.create(export_dir, showWarnings = FALSE, recursive = TRUE)

detect_analises_table <- function(con) {
  tabs <- DBI::dbGetQuery(con, "SELECT name FROM sqlite_master WHERE type IN ('table','view')")
  cand <- c("analises", "analises_atestmed")
  hit  <- intersect(cand, tabs$name)
  if (length(hit) == 0) stop("Não encontrei 'analises' nem 'analises_atestmed'.")
  hit[[1]]
}

nc_case_sql <- function(alias = "a") {
  sprintf("
    CASE
      WHEN CAST(IFNULL(%1$s.conformado,1) AS INTEGER)=0 THEN 1
      WHEN TRIM(IFNULL(%1$s.motivoNaoConformado,'')) <> ''
           AND CAST(IFNULL(%1$s.motivoNaoConformado,'0') AS INTEGER) <> 0 THEN 1
      ELSE 0
    END
  ", alias)
}

# ------------------------------ DB --------------------------------------------
con <- dbConnect(SQLite(), opt$db)
on.exit(dbDisconnect(con), add = TRUE)

a_tbl <- detect_analises_table(con)
nc_sql <- nc_case_sql("a")

sql <- sprintf("
SELECT
  p.nomePerito AS perito,
  SUM(%s) AS nc,
  COUNT(*) AS n
FROM %s a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
GROUP BY p.nomePerito
", nc_sql, DBI::dbQuoteIdentifier(con, a_tbl))

df <- dbGetQuery(con, sql, params = list(opt$start, opt$end))
stopifnot(nrow(df) > 0)

if (!(opt$perito %in% df$perito)) {
  sim <- df %>% filter(grepl(opt$perito, perito, ignore.case = TRUE)) %>% pull(perito)
  msg <- if (length(sim)) paste0(" Peritos semelhantes: ", paste(sim, collapse=", "), ".") else ""
  stop(sprintf("Perito '%s' não encontrado no período.%s", opt$perito, msg))
}

# --------------------------- cálculo ------------------------------------------
p_row <- df %>% filter(perito == opt$perito) %>% slice(1)
o_row <- df %>% filter(perito != opt$perito) %>% summarise(nc = sum(nc), n = sum(n))

p_pct <- ifelse(p_row$n > 0, p_row$nc / p_row$n, NA_real_)
o_pct <- ifelse(o_row$n > 0, o_row$nc / o_row$n, NA_real_)

# ICs (prop.test; aproximação normal com correção de continuidade)
p_ci <- if (is.finite(p_pct)) prop.test(p_row$nc, p_row$n)$conf.int else c(NA_real_, NA_real_)
o_ci <- if (is.finite(o_pct)) prop.test(o_row$nc, o_row$n)$conf.int else c(NA_real_, NA_real_)

# teste 2 proporções (quando ambos têm n > 0)
pval <- NA_real_
if (p_row$n > 0 && o_row$n > 0) {
  pval <- suppressWarnings(prop.test(c(p_row$nc, o_row$nc), c(p_row$n, o_row$n))$p.value)
}

plot_df <- tibble::tibble(
  Grupo = factor(c(opt$perito, "Demais (excl.)"), levels=c(opt$perito, "Demais (excl.)")),
  pct   = c(p_pct, o_pct),
  lo    = c(p_ci[1], o_ci[1]),
  hi    = c(p_ci[2], o_ci[2]),
  n     = c(p_row$n, o_row$n)
)

ylim_max <- max(c(plot_df$hi, 0), na.rm = TRUE)
if (!is.finite(ylim_max) || ylim_max <= 0) ylim_max <- max(c(plot_df$pct, 0.05), na.rm = TRUE)
ylim_max <- min(1, ylim_max * 1.15)

gg <- ggplot(plot_df, aes(Grupo, pct)) +
  geom_col(fill=c("#d62728","#1f77b4"), width=.6) +
  geom_errorbar(aes(ymin=lo, ymax=hi), width=.15, size=.4, na.rm = TRUE) +
  geom_text(aes(label=scales::percent(pct, accuracy=.1)), vjust=-.4, size=3.3) +
  scale_y_continuous(labels=percent_format(accuracy=1), limits=c(0, ylim_max)) +
  labs(
    title   = "Taxa de Não Conformidade (NC robusto) – Perito vs Demais (excl.)",
    subtitle= sprintf("Período: %s a %s  |  n=%d vs %d", opt$start, opt$end, p_row$n, o_row$n),
    y       = "Percentual", x = NULL,
    caption = "NC robusto: conformado=0 OU (motivoNaoConformado ≠ '' E CAST(motivoNaoConformado) ≠ 0)."
  ) +
  theme_minimal(base_size=11) +
  theme(panel.grid.major.x = element_blank())

png_path <- file.path(export_dir, sprintf("rcheck_nc_rate_%s.png", perito_safe))
ggsave(png_path, gg, width=8, height=5, dpi=160)
cat(sprintf("✓ salvo: %s\n", png_path))

# ------------------------ comentários em .org ----------------------------------
percent_s <- function(x) ifelse(is.finite(x), percent(x, accuracy = .1), "NA")
num_s     <- function(x) format(x, big.mark=".", decimal.mark=",", trim=TRUE)

metodo_txt <- sprintf(
  paste0(
    "*Método.* Comparamos a *taxa robusta de NC* do perito (n=%s; nc=%s; %s) ",
    "com o agregado dos *demais peritos* (n=%s; nc=%s; %s) no período %s a %s. ",
    "A taxa de NC é definida como: conformado=0 OU motivoNaoConformado não-vazio ",
    "e diferente de 0. Para cada proporção calculamos *IC 95%%* via `prop.test` ",
    "(aproximação normal com correção de continuidade). Também aplicamos um *teste ",
    "de duas proporções* (qui-quadrado) para avaliar diferença global."
  ),
  num_s(p_row$n), num_s(p_row$nc), percent_s(p_pct),
  num_s(o_row$n), num_s(o_row$nc), percent_s(o_pct),
  opt$start, opt$end
)

interpret_txt <- {
  dir_txt <- if (is.finite(p_pct) && is.finite(o_pct)) {
    if (p_pct > o_pct) "acima dos demais" else if (p_pct < o_pct) "abaixo dos demais" else "igual aos demais"
  } else "indeterminado"
  sig_txt <- if (is.finite(pval)) {
    if (pval < 0.001) "diferença estatisticamente significativa (p<0,001)"
    else if (pval < 0.01) "diferença estatisticamente significativa (p<0,01)"
    else if (pval < 0.05) "diferença estatisticamente significativa (p<0,05)"
    else "diferença *não* significativa (p≥0,05)"
  } else {
    "amostra insuficiente para inferência (algum grupo com n=0)"
  }
  sprintf("*Interpretação.* A taxa do perito está %s em relação ao grupo. Resultado: %s.", dir_txt, sig_txt)
}

# 1) .org "principal" (contém a imagem; opcional para consulta)
org_main <- file.path(export_dir, sprintf("rcheck_nc_rate_%s.org", perito_safe))
org_main_txt <- paste(
  "#+CAPTION: Taxa de NC — perito vs demais",
  sprintf("[[file:%s]]", basename(png_path)),  # será reescrita para ../imgs/ pelo make_report
  "",
  metodo_txt, "",
  interpret_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
cat(sprintf("✓ org: %s\n", org_main))

# 2) .org de comentário (é este que o make_report injeta no PDF final)
org_comment <- file.path(export_dir, sprintf("rcheck_nc_rate_%s_comment.org", perito_safe))
org_comment_txt <- paste(metodo_txt, "", interpret_txt, "", sep = "\n")
writeLines(org_comment_txt, org_comment)
cat(sprintf("✓ org(comment): %s\n", org_comment))

