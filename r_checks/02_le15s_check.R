#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite); library(dplyr); library(ggplot2); library(scales)
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
  make_option("--threshold", type="integer", default=15L),
  make_option("--out-dir",   type="character", default=NULL, help="Diretório de saída (PNG/org)")
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
num_s     <- function(x) format(x, big.mark=".", decimal.mark=",", trim=TRUE)

# ------------------------------ DB --------------------------------------------
con <- dbConnect(SQLite(), opt$db)
# (patched) # (patched) on.exit(dbDisconnect(con), add = TRUE)

sql <- sprintf("
SELECT p.nomePerito AS perito,
       SUM( (julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400 <= %d ) AS leT,
       COUNT(*) AS n
FROM analises a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
GROUP BY p.nomePerito
", opt$threshold, opt$start, opt$end)

df <- am_dbGetQuery(con, sql)

if (!(opt$perito %in% df$perito)) {
  sim <- df %>% filter(grepl(opt$perito, perito, ignore.case = TRUE)) %>% pull(perito)
  msg <- if (length(sim)) paste0(" Peritos semelhantes: ", paste(sim, collapse=", "), ".") else ""
  stop(sprintf("Perito '%s' não encontrado no período.%s", opt$perito, msg))
}

# --------------------------- cálculo ------------------------------------------
p_row <- df %>% filter(perito == opt$perito) %>% slice(1)
o_row <- df %>% filter(perito != opt$perito) %>% summarise(leT = sum(leT), n = sum(n))

p_pct <- ifelse(p_row$n > 0, p_row$leT / p_row$n, NA_real_)
o_pct <- ifelse(o_row$n > 0, o_row$leT / o_row$n, NA_real_)

# IC 95% (prop.test) só quando n>0
p_ci <- if (p_row$n > 0) prop.test(p_row$leT, p_row$n)$conf.int else c(NA_real_, NA_real_)
o_ci <- if (o_row$n > 0) prop.test(o_row$leT, o_row$n)$conf.int else c(NA_real_, NA_real_)

# teste 2 proporções (p-valor) quando ambos têm n>0
pval <- NA_real_
if (p_row$n > 0 && o_row$n > 0) {
  pval <- suppressWarnings(prop.test(c(p_row$leT, o_row$leT), c(p_row$n, o_row$n))$p.value)
}

plot_df <- tibble::tibble(
  Grupo = factor(c(opt$perito, "Brasil (excl.)"), levels=c(opt$perito, "Brasil (excl.)")),
  pct   = c(p_pct, o_pct),
  lo    = c(p_ci[1], o_ci[1]),
  hi    = c(p_ci[2], o_ci[2]),
  n     = c(p_row$n, o_row$n)
)

ylim_max <- max(c(plot_df$hi, 0), na.rm = TRUE)
if (!is.finite(ylim_max) || ylim_max <= 0) ylim_max <- max(c(plot_df$pct, 0.05), na.rm = TRUE)
ylim_max <- min(1, ylim_max * 1.15)

gg <- ggplot(plot_df, aes(Grupo, pct)) +
  geom_col(fill=c("#9467bd","#1f77b4"), width=.6) +
  geom_errorbar(aes(ymin=lo, ymax=hi), width=.15, linewidth=.4, na.rm = TRUE) +
  geom_text(aes(label=scales::percent(pct, accuracy=.1)), vjust=-.4, size=3.3) +
  scale_y_continuous(labels=percent_format(accuracy=1), limits=c(0, ylim_max)) +
  labs(
    title    = sprintf("Perícias ≤ %ds – Perito vs Brasil (excl.)", opt$threshold),
    subtitle = sprintf("Período: %s a %s  |  n=%d vs %d", opt$start, opt$end, p_row$n, o_row$n),
    y        = "Percentual", x = NULL
  ) +
  theme_minimal(base_size=11) +
  theme(panel.grid.major.x = element_blank())

png_path <- file.path(export_dir, sprintf("rcheck_le%ds_%s.png", opt$threshold, perito_safe))
ggsave(png_path, gg, width=8, height=5, dpi=160)
cat(sprintf("✓ salvo: %s\n", png_path))

# ------------------------ comentários em .org ----------------------------------
metodo_txt <- sprintf(
  paste0(
    "*Método.* Comparamos a *proporção de perícias com duração ≤ %ds* do perito ",
    "(n=%s; ≤T=%s; %s) com o agregado do *Brasil (excl.)* ",
    "(n=%s; ≤T=%s; %s) no período %s a %s. ",
    "A duração é calculada como (dataHoraFim − dataHoraIni) em segundos. ",
    "Para cada proporção, calculamos *IC 95%%* com `prop.test` ",
    "e avaliamos *diferença global* com teste de *duas proporções*."
  ),
  opt$threshold,
  num_s(p_row$n), num_s(p_row$leT), percent_s(p_pct),
  num_s(o_row$n), num_s(o_row$leT), percent_s(o_pct),
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
  sprintf("*Interpretação.* A proporção ≤ %ds do perito está %s em relação ao grupo. Resultado: %s.",
          opt$threshold, dir_txt, sig_txt)
}

# 1) .org "principal" (contém a imagem + texto; opcional)
org_main <- file.path(export_dir, sprintf("rcheck_le%ds_%s.org", opt$threshold, perito_safe))
org_main_txt <- paste(
  "#+CAPTION: Perícias ≤ limiar — perito vs Brasil (excl.)",
  sprintf("[[file:%s]]", basename(png_path)),  # make_report ajusta para ../imgs/
  "",
  metodo_txt, "",
  interpret_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
cat(sprintf("✓ org: %s\n", org_main))

# 2) .org de comentário (é este que o make_report injeta no PDF final)
org_comment <- file.path(export_dir, sprintf("rcheck_le%ds_%s_comment.org", opt$threshold, perito_safe))
org_comment_txt <- paste(metodo_txt, "", interpret_txt, "", sep = "\n")
writeLines(org_comment_txt, org_comment)
cat(sprintf("✓ org(comment): %s\n", org_comment))
