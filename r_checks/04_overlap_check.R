#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite)
  library(dplyr); library(ggplot2); library(scales); library(lubridate)
})

# ==== ATESTMED PROLOGO (INICIO) ====
local({
  .am_loaded <- FALSE
  for (pp in c("r_checks/_common.R","./_common.R","../r_checks/_common.R")) {
    if (file.exists(pp)) { source(pp, local=TRUE); .am_loaded <- TRUE; break }
  }
  if (!.am_loaded) message("[prolog] _common.R não encontrado — usando fallbacks internos.")

  `%||%` <- function(a,b) if (is.null(a)) b else a

  # ---- Fallbacks essenciais (se _common.R não definiu) ----
  if (!exists("am_normalize_cli", mode="function", inherits=TRUE)) {
    am_normalize_cli <<- function(x) as.character(x)
  }
  if (!exists("am_parse_args", mode="function", inherits=TRUE)) {
    am_parse_args <<- function() {
      a <- am_normalize_cli(commandArgs(trailingOnly=TRUE))
      kv <- list(); i <- 1L; n <- length(a)
      while (i <= n) {
        k <- a[[i]]
        if (startsWith(k, "--")) {
          v <- if (i+1L <= n && !startsWith(a[[i+1L]], "--")) a[[i+1L]] else TRUE
          kv[[sub("^--","",k)]] <- v
          i <- i + (if (identical(v, TRUE)) 1L else 2L)
        } else i <- i + 1L
      }
      kv
    }
  }
  if (!exists("am_open_db", mode="function", inherits=TRUE)) {
    am_open_db <<- function(path) {
      p <- normalizePath(path, mustWork=TRUE)
      DBI::dbConnect(RSQLite::SQLite(), dbname=p)
    }
  }
  if (!exists("am_resolve_export_dir", mode="function", inherits=TRUE)) {
    am_resolve_export_dir <<- function(out_dir=NULL) {
      if (!is.null(out_dir) && nzchar(out_dir)) {
        od <- normalizePath(out_dir, mustWork=FALSE)
      } else {
        dbp <- am_args[["db"]] %||% ""
        base_dir <- if (nzchar(dbp)) normalizePath(file.path(dirname(dbp), ".."), mustWork=FALSE) else getwd()
        od <- file.path(base_dir, "graphs_and_tables", "exports")
      }
      if (!dir.exists(od)) dir.create(od, recursive=TRUE, showWarnings=FALSE)
      od
    }
  }
  if (!exists("am_detect_analises_table", mode="function", inherits=TRUE)) {
    am_detect_analises_table <<- function(con) {
      has <- function(nm) {
        nrow(am_dbGetQuery(con,
          "SELECT 1 FROM sqlite_master WHERE type in ('table','view') AND name=? LIMIT 1",
          params=list(nm))) > 0
      }
      for (t in c("analises","analises_atestmed")) if (has(t)) return(t)
      stop("Não encontrei 'analises' nem 'analises_atestmed'.")
    }
  }
  if (!exists("am_detect_columns", mode="function", inherits=TRUE)) {
    am_detect_columns <<- function(con, tbl) {
      if (is.na(tbl) || !nzchar(tbl)) return(character(0))
      am_dbGetQuery(con, sprintf("PRAGMA table_info(%s)", tbl))$name
    }
  }

  # 1) args → lista nomeada (sem rebind de objetos bloqueados)
  .raw <- NULL
  if (exists("args", inherits=TRUE)) {
    .cand <- get("args", inherits=TRUE)
    if (!is.function(.cand)) .raw <- .cand
  }
  .kv <- tryCatch(am_parse_args(), error=function(e) list())
  if (is.character(.raw)) {
    .kv2 <- list(); i <- 1L; n <- length(.raw)
    while (i <= n) {
      k <- .raw[[i]]
      if (startsWith(k, "--")) {
        v <- if (i+1L <= n && !startsWith(.raw[[i+1L]], "--")) .raw[[i+1L]] else TRUE
        .kv2[[sub("^--","",k)]] <- v
        i <- i + (if (identical(v, TRUE)) 1L else 2L)
      } else i <- i + 1L
    }
    if (length(.kv2)) .kv <- utils::modifyList(.kv, .kv2)
  } else if (is.environment(.raw)) {
    .kv <- utils::modifyList(.kv, as.list(.raw))
  } else if (is.list(.raw)) {
    .kv <- utils::modifyList(.kv, .raw)
  }
  am_args <<- .kv

  # 2) Conexão ao DB
  db_path <- am_args[["db"]]
  if (is.null(db_path) || !nzchar(db_path)) stop("Faltou --db <path>")
  con <<- am_open_db(db_path)

  # Fecha TODAS as conexões SQLite ao sair (remove avisos)
  on.exit({
    try({
      if (exists("con", inherits=TRUE)) try(DBI::dbDisconnect(con), silent=TRUE)
      conns <- try(DBI::dbListConnections(RSQLite::SQLite()), silent=TRUE)
      if (!inherits(conns, "try-error")) for (cc in conns) try(DBI::dbDisconnect(cc), silent=TRUE)
    }, silent=TRUE)
  }, add=TRUE)

  # 3) Paths e schema
  export_dir <<- am_resolve_export_dir(am_args[["out-dir"]])
  a_tbl <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
  cols  <<- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))

  # 4) Args derivados
  start_d   <<- am_args[["start"]]
  end_d     <<- am_args[["end"]]
  min_n     <<- suppressWarnings(as.integer(am_args[["min-analises"]]))
  threshold <<- suppressWarnings(as.numeric(am_args[["threshold"]]))
  measure   <<- as.character(am_args[["measure"]] %||% NA_character_); if (!is.na(measure)) measure <<- measure[[1L]]
  top10     <<- isTRUE(am_args[["top10"]])
  perito    <<- as.character(am_args[["perito"]] %||% NA_character_); if (!is.na(perito)) perito <<- perito[[1L]]

  # 5) Wrapper seguro para consultas (evita "Expected string vector of length 1")
  am_dbGetQuery <<- (function(.f) {
    force(.f)
    function(con, statement, ...) {
      st <- statement
      if (length(st) != 1L) st <- paste(st, collapse=" ")
      .f(con, st, ...)
    }
  })(DBI::dbGetQuery)
})
# ==== ATESTMED PROLOGO (FIM) ====















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
# (patched) # (patched) on.exit(dbDisconnect(con), add = TRUE)

sql <- sprintf("
SELECT p.nomePerito AS perito, a.dataHoraIniPericia AS ini, a.dataHoraFimPericia AS fim
FROM analises a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
ORDER BY p.nomePerito, a.dataHoraIniPericia
", opt$start, opt$end)

df <- am_dbGetQuery(con, sql)

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
  summarize(overlap = has_overlap(pick(dplyr::everything())), .groups = "drop")

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
