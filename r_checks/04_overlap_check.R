#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite)
  library(dplyr); library(ggplot2); library(scales); library(lubridate)
})

# ----------------------------- CLI --------------------------------------------
opt_list <- list(
  make_option("--db",        type="character"),
  make_option("--start",     type="character"),
  make_option("--end",       type="character"),
  make_option("--perito",    type="character"),
  make_option("--out-dir",   type="character", default=NULL, help="Diretório de saída (PNG/org)")
)
opt <- parse_args(OptionParser(option_list = opt_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))

# --------------------------- helpers -------------------------------------------
safe <- function(x) gsub("(^_+|_+$)", "", gsub("[^A-Za-z0-9_-]+","_", x))
perito_safe <- safe(opt$perito)

base_dir   <- normalizePath(file.path(dirname(opt$db), ".."))
export_dir <- if (!is.null(opt$`out-dir`)) normalizePath(opt$`out-dir`, mustWork = FALSE) else
  file.path(base_dir, "graphs_and_tables", "exports")
dir.create(export_dir, showWarnings = FALSE, recursive = TRUE)

percent_s <- function(x, acc = .1) ifelse(is.finite(x), percent(x, accuracy = acc), "NA")

# ------------------------------ DB --------------------------------------------
con <- dbConnect(SQLite(), opt$db)
on.exit(dbDisconnect(con), add = TRUE)

sql <- sprintf("
SELECT p.nomePerito AS perito, a.dataHoraIniPericia AS ini, a.dataHoraFimPericia AS fim
FROM analises a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
ORDER BY p.nomePerito, a.dataHoraIniPericia
", opt$start, opt$end)

df <- dbGetQuery(con, sql)

# --------------------------- preparação ----------------------------------------
df <- df %>%
  mutate(
    ini = ymd_hms(ini, quiet = TRUE),
    fim = ymd_hms(fim, quiet = TRUE)
  ) %>%
  filter(!is.na(ini), !is.na(fim), fim >= ini)

has_overlap <- function(dfp) {
  if (nrow(dfp) < 2) return(FALSE)
  dfp <- dfp[order(dfp$ini), , drop = FALSE]
  any(dfp$ini[-1] < dfp$fim[-nrow(dfp)])
}

flag_by_perito <- df %>%
  group_by(perito) %>%
  summarize(overlap = has_overlap(cur_data()), .groups = "drop")

if (!(opt$perito %in% flag_by_perito$perito)) {
  sim <- flag_by_perito %>% filter(grepl(opt$perito, perito, ignore.case = TRUE)) %>% pull(perito)
  msg <- if (length(sim)) paste0(" Peritos semelhantes: ", paste(sim, collapse = ", "), ".") else ""
  stop(sprintf("Perito '%s' não encontrado no período.%s", opt$perito, msg))
}

p_flag <- flag_by_perito %>% filter(perito == opt$perito) %>% pull(overlap) %>% .[1]

others <- flag_by_perito %>% filter(perito != opt$perito)
n_others <- nrow(others)
n_others_overlap <- sum(others$overlap, na.rm = TRUE)
o_rate <- if (n_others > 0) n_others_overlap / n_others else NA_real_

# ----------------------------- gráfico -----------------------------------------
ylim_max <- max(c(as.numeric(p_flag), o_rate), na.rm = TRUE)
if (!is.finite(ylim_max) || ylim_max <= 0) ylim_max <- 0.05
ylim_max <- min(1, ylim_max * 1.15)

plot_df <- tibble::tibble(
  Grupo = factor(c(opt$perito, "Demais (excl.)"), levels = c(opt$perito, "Demais (excl.)")),
  pct   = c(ifelse(p_flag, 1, 0), o_rate)
)

gg <- ggplot(plot_df, aes(Grupo, pct)) +
  geom_col(fill = c("#ff7f0e", "#1f77b4"), width = .6) +
  geom_text(aes(label = scales::percent(pct, accuracy = .1)), vjust = -.4, size = 3.3, na.rm = TRUE) +
  scale_y_continuous(labels = percent_format(accuracy = 1), limits = c(0, ylim_max)) +
  labs(
    title = "Sobreposição de tarefas — Perito (indicador de ocorrência) vs Demais",
    subtitle = sprintf("Período: %s a %s  |  n peritos (excl.) = %d", opt$start, opt$end, n_others),
    y = "Percentual de peritos com sobreposição", x = NULL,
    caption = "Indicador binário por perito: '1' se houve pelo menos uma interseção entre perícias no período."
  ) +
  theme_minimal(base_size = 11) +
  theme(panel.grid.major.x = element_blank())

png_path <- file.path(export_dir, sprintf("rcheck_overlap_%s.png", perito_safe))
ggsave(png_path, gg, width = 8, height = 5, dpi = 160)
cat(sprintf("✓ salvo: %s\n", png_path))

# ------------------------ comentários em .org ----------------------------------
metodo_txt <- paste0(
  "*Método.* Para cada perito, ordenamos as perícias por início e marcamos *sobreposição* ",
  "quando algum início ocorre antes do fim da perícia imediatamente anterior (interseção de intervalos). ",
  "Isso produz um *indicador binário* por perito (houve/não houve). ",
  "Em seguida, comparamos o perito-alvo aos *demais peritos (excl.)*, reportando a fração de ",
  "peritos com sobreposição entre os demais. O gráfico mostra as duas barras com rótulos em porcentagem."
)

interpret_txt <- {
  p_str <- if (isTRUE(p_flag)) "houve sobreposição" else "não houve sobreposição"
  o_str <- if (is.finite(o_rate)) sprintf("entre os demais, %s apresentam sobreposição", percent_s(o_rate)) else
    "a taxa entre os demais é indeterminada (amostra vazia)"
  paste0(
    "*Interpretação.* Para o perito analisado, ", p_str, ". ",
    o_str, ". Lembrando que este indicador capta *ocorrência* (>=1 evento) e ",
    "não mede *duração* ou *gravidade* da sobreposição."
  )
}

# 1) .org principal (imagem + texto)
org_main <- file.path(export_dir, sprintf("rcheck_overlap_%s.org", perito_safe))
org_main_txt <- paste(
  "#+CAPTION: Sobreposição de tarefas — indicador de ocorrência",
  sprintf("[[file:%s]]", basename(png_path)),  # make_report ajusta para ../imgs/
  "",
  metodo_txt, "",
  interpret_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
cat(sprintf("✓ org: %s\n", org_main))

# 2) .org somente com o comentário (é este que o make_report injeta no PDF)
org_comment <- file.path(export_dir, sprintf("rcheck_overlap_%s_comment.org", perito_safe))
org_comment_txt <- paste(metodo_txt, "", interpret_txt, "", sep = "\n")
writeLines(org_comment_txt, org_comment)
cat(sprintf("✓ org(comment): %s\n", org_comment))

