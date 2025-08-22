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
# (patched) # (patched) on.exit(dbDisconnect(con), add = TRUE)

sql <- sprintf("
SELECT p.nomePerito AS perito,
       COUNT(*) AS total,
       SUM((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400) AS segs
FROM peritos p
JOIN analises a ON p.siapePerito = a.siapePerito
WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
GROUP BY p.nomePerito
", opt$start, opt$end)

df <- am_dbGetQuery(con, sql)

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
  geom_vline(xintercept = p_val,          color="#d62728",  linewidth=1, na.rm = TRUE) + 
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
