#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse)
  library(DBI)
  library(RSQLite)
  library(dplyr)
  library(ggplot2)
  library(scales)
  library(stringr)
  library(tibble)
})

# ---------------- CLI ----------------
opt_list <- list(
  make_option("--db",        type="character", help="Caminho do SQLite (ex.: db/atestmed.db)"),
  make_option("--start",     type="character", help="AAAA-MM-DD"),
  make_option("--end",       type="character", help="AAAA-MM-DD"),
  make_option("--perito",    type="character", help="Nome exato do perito"),
  make_option("--top10",     action="store_true", default=FALSE,
              help="Ativa modo Top 10 (pelo scoreFinal)"),
  make_option("--min-analises", type="integer", default=50,
              help="Mínimo de análises para elegibilidade no Top 10 [default: %default]"),
  make_option("--measure",   type="character", default="nc",
              help="Métrica: 'nc' (não conformidade) ou 'le' (<= threshold s) [default: %default]"),
  make_option("--threshold", type="integer", default=15,
              help="Limite em segundos quando measure=le [default: %default]"),
  # compat com make_report.py:
  make_option("--out-dir",   type="character", help="Diretório de saída (opcional)"),
  make_option("--out",       type="character", help="Alias de --out-dir (opcional)")
)
opt <- parse_args(OptionParser(option_list = opt_list))

if (is.null(opt$db) || is.null(opt$start) || is.null(opt$end)) {
  stop("Parâmetros obrigatórios: --db, --start, --end", call. = FALSE)
}
if (!opt$top10 && is.null(opt$perito)) {
  stop("Informe --perito ou --top10.", call. = FALSE)
}
if (opt$top10 && !is.null(opt$perito)) {
  stop("Use OU --perito OU --top10, não ambos.", call. = FALSE)
}

safe <- function(x) { gsub("(^_+|_+$)", "", gsub("[^A-Za-z0-9_-]+","_", x)) }

# Deriva EXPORT_DIR a partir do DB (ou usa --out/--out-dir)
base_dir <- normalizePath(file.path(dirname(opt$db), ".."))
explicit_out <- if (!is.null(opt$`out-dir`)) opt$`out-dir` else opt$out
export_dir <- if (!is.null(explicit_out) && nzchar(explicit_out)) {
  normalizePath(explicit_out, mustWork = FALSE)
} else {
  file.path(base_dir, "graphs_and_tables", "exports")
}
dir.create(export_dir, showWarnings = FALSE, recursive = TRUE)

# ---------------- Helpers ----------------
wilson_ci <- function(x, n) {
  if (is.na(x) || is.na(n) || n <= 0) return(c(NA_real_, NA_real_))
  ci <- tryCatch(stats::prop.test(x, n)$conf.int,
                 error = function(e) c(NA_real_, NA_real_))
  as.numeric(ci)
}

z_test_2props <- function(x1,n1,x2,n2) {
  if (min(n1,n2) <= 0) return(list(z=NA_real_, p=NA_real_))
  p_pool <- (x1 + x2) / (n1 + n2)
  se     <- sqrt(p_pool*(1-p_pool)*(1/n1 + 1/n2))
  if (!is.finite(se) || se == 0) return(list(z=NA_real_, p=NA_real_))
  z <- (x1/n1 - x2/n2) / se
  p <- 2 * (1 - pnorm(abs(z)))
  list(z=z, p=p)
}

get_top10_names <- function(con, start_date, end_date, min_n=50) {
  q <- sprintf("
    SELECT p.nomePerito AS perito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
      FROM indicadores i
      JOIN peritos   p ON i.perito = p.siapePerito
      JOIN analises  a ON a.siapePerito = i.perito
     WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
     GROUP BY p.nomePerito, i.scoreFinal
    HAVING total_analises >= %d
     ORDER BY i.scoreFinal DESC, total_analises DESC
     LIMIT 10
  ", start_date, end_date, as.integer(min_n))
  df <- DBI::dbGetQuery(con, q)
  if (nrow(df) == 0) character(0) else df$perito
}

build_agg_query <- function(measure, threshold, start_date, end_date) {
  if (tolower(measure) == "nc") {
    title_txt <- "Meta-análise simples: Não Conformidade"
    ylab_txt  <- "Proporção de NC"
    meas_tag  <- "nc"
    sql <- sprintf("
      SELECT p.nomePerito AS perito,
             SUM(CASE WHEN a.motivoNaoConformado != 0 THEN 1 ELSE 0 END) AS x,
             COUNT(*) AS n
        FROM analises a
        JOIN peritos  p ON a.siapePerito = p.siapePerito
       WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
       GROUP BY p.nomePerito
    ", start_date, end_date)
  } else if (tolower(measure) == "le") {
    title_txt <- sprintf("Meta-análise simples: ≤ %ds", as.integer(threshold))
    ylab_txt  <- "Proporção (≤ threshold)"
    meas_tag  <- sprintf("le%ds", as.integer(threshold))
    sql <- sprintf("
      SELECT p.nomePerito AS perito,
             SUM( ((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400) <= %d ) AS x,
             COUNT(*) AS n
        FROM analises a
        JOIN peritos  p ON a.siapePerito = p.siapePerito
       WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
       GROUP BY p.nomePerito
    ", as.integer(threshold), start_date, end_date)
  } else {
    stop("Valor inválido para --measure. Use 'nc' ou 'le'.")
  }
  list(sql=sql, title=title_txt, ylab=ylab_txt, tag=meas_tag)
}

plot_two_groups <- function(df_plot, title_txt, subtitle_txt, ylab_txt, outfile) {
  # Limites do eixo Y
  yh <- suppressWarnings(max(df_plot$hi, na.rm=TRUE))
  if (!is.finite(yh)) yh <- suppressWarnings(max(df_plot$prop, na.rm=TRUE))
  if (!is.finite(yh)) yh <- 0.1
  yh <- min(1, yh * 1.12 + 0.02)

  df_plot <- df_plot %>%
    mutate(
      y_label = pmin(prop + 0.03, yh - 0.03),
      label_inside = prop * 1.05 > yh,
      label_text = sprintf("%s (n=%d)", scales::percent(prop, accuracy=.1), n),
      fill = c("#d62728", "#1f77b4")
    )

  gg <- ggplot(df_plot, aes(Grupo, prop)) +
    geom_col(aes(fill=Grupo), width=.55, alpha=.9, show.legend = FALSE) +
    scale_fill_manual(values = df_plot$fill) +
    geom_errorbar(aes(ymin=lo, ymax=hi), width=.15, size=.5, na.rm = TRUE) +
    geom_text(aes(y = ifelse(label_inside, prop - 0.02, y_label), label = label_text),
              color = ifelse(df_plot$prop > 0.85, "white", "black"),
              vjust = ifelse(df_plot$prop > 0.85, 1.2, -0.35),
              size = 3.3) +
    scale_y_continuous(labels=percent_format(accuracy=1), limits=c(0, yh)) +
    labs(title=title_txt, subtitle=subtitle_txt, x=NULL, y=ylab_txt) +
    theme_minimal(base_size=11) +
    theme(panel.grid.major.x = element_blank())

  ggsave(outfile, gg, width=8, height=5, dpi=160)
  cat(sprintf("✓ salvo: %s\n", outfile))
}

# ---------------- Execução ----------------
con <- dbConnect(SQLite(), opt$db)

# 1) agrega x/n por perito para a métrica desejada
spec <- build_agg_query(opt$measure, opt$threshold, opt$start, opt$end)
agg <- dbGetQuery(con, spec$sql)

if (nrow(agg) == 0) {
  dbDisconnect(con); stop("Nenhuma linha encontrada no período informado.")
}

if (!opt$top10) {
  # ---------------- modo perito ----------------
  if (!(opt$perito %in% agg$perito)) {
    dbDisconnect(con); stop("Perito informado não encontrado no período.")
  }
  p_row <- agg %>% filter(perito == opt$perito) %>% slice(1)
  o_row <- agg %>% filter(perito != opt$perito) %>% summarise(x = sum(x), n = sum(n), .groups="drop")

  p_hat <- ifelse(p_row$n > 0, p_row$x / p_row$n, NA_real_)
  o_hat <- ifelse(o_row$n > 0, o_row$x / o_row$n, NA_real_)
  p_ci  <- wilson_ci(p_row$x, p_row$n)
  o_ci  <- wilson_ci(o_row$x, o_row$n)
  zt    <- z_test_2props(p_row$x, p_row$n, o_row$x, o_row$n)

  plot_df <- tibble(
    Grupo = factor(c(opt$perito, "Demais (excl.)"), levels=c(opt$perito, "Demais (excl.)")),
    prop  = c(p_hat, o_hat),
    lo    = c(p_ci[1], o_ci[1]),
    hi    = c(p_ci[2], o_ci[2]),
    n     = c(p_row$n, o_row$n),
    x     = c(p_row$x, o_row$x)
  )

  subtitle_txt <- sprintf("Período: %s a %s | n=%d vs %d | z=%s, p=%s",
                          opt$start, opt$end, p_row$n, o_row$n,
                          ifelse(is.na(zt$z), "NA", sprintf("%.2f", zt$z)),
                          ifelse(is.na(zt$p), "NA", scales::pvalue(zt$p, accuracy = .001)))

  perito_safe <- safe(opt$perito)
  outfile <- file.path(export_dir, sprintf("rcheck_weighted_props_%s_%s.png", spec$tag, perito_safe))
  plot_two_groups(plot_df, spec$title, subtitle_txt, spec$ylab, outfile)

  cat(sprintf("\nResumo (perito): %s -> %d/%d (%.1f%%)\n", opt$perito, p_row$x, p_row$n, 100*p_hat))
  cat(sprintf("Resumo (demais): %d/%d (%.1f%%)\n\n", o_row$x, o_row$n, 100*o_hat))

} else {
  # ---------------- modo top10 ----------------
  top10_names <- get_top10_names(con, opt$start, opt$end, opt$min_analises)
  dbDisconnect(con)

  if (length(top10_names) == 0) {
    stop("Top 10: nenhum perito encontrado com os critérios no período.")
  }

  grp <- agg %>% filter(perito %in% top10_names) %>% summarise(x = sum(x), n = sum(n), .groups="drop")
  oth <- agg %>% filter(!(perito %in% top10_names)) %>% summarise(x = sum(x), n = sum(n), .groups="drop")

  g_hat <- ifelse(grp$n > 0, grp$x / grp$n, NA_real_)
  o_hat <- ifelse(oth$n > 0, oth$x / oth$n, NA_real_)
  g_ci  <- wilson_ci(grp$x, grp$n)
  o_ci  <- wilson_ci(oth$x, oth$n)
  zt    <- z_test_2props(grp$x, grp$n, oth$x, oth$n)

  plot_df <- tibble(
    Grupo = factor(c("Top 10 piores", "Brasil (excl.)"), levels=c("Top 10 piores", "Brasil (excl.)")),
    prop  = c(g_hat, o_hat),
    lo    = c(g_ci[1], o_ci[1]),
    hi    = c(g_ci[2], o_ci[2]),
    n     = c(grp$n, oth$n),
    x     = c(grp$x, oth$x)
  )

  subtitle_txt <- sprintf("Período: %s a %s | Top10 n=%d (%s) vs Brasil n=%d | z=%s, p=%s",
                          opt$start, opt$end, grp$n,
                          paste(head(top10_names, 5), collapse = ", "),
                          oth$n,
                          ifelse(is.na(zt$z), "NA", sprintf("%.2f", zt$z)),
                          ifelse(is.na(zt$p), "NA", scales::pvalue(zt$p, accuracy = .001)))

  # IMPORTANTE: usa prefixo rcheck_top10_* para acionar comentários de grupo
  outfile <- file.path(export_dir, sprintf("rcheck_top10_weighted_props_%s.png", spec$tag))
  plot_two_groups(plot_df, spec$title, subtitle_txt, spec$ylab, outfile)

  cat(sprintf("\nResumo (Top10): %d/%d (%.1f%%)\n", grp$x, grp$n, 100*g_hat))
  cat(sprintf("Resumo (Brasil excl.): %d/%d (%.1f%%)\n\n", oth$x, oth$n, 100*o_hat))
  cat(sprintf("Peritos Top10 (%d): %s\n", length(top10_names), paste(top10_names, collapse = "; ")))
}

