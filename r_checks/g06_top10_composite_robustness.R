#!/usr/bin/env Rscript
# -*- coding: utf-8 -*-
# Top 10 — Robustez do Composto (z-score médio)
# ---- Auto-normalized header (injected by run_r_checks_autofix.py) ----
options(stringsAsFactors = FALSE)
options(encoding = "UTF-8")
options(warn = 1)
options(repos = c(CRAN = "https://cloud.r-project.org"))
Sys.setlocale(category = "LC_ALL", locale = "C.UTF-8")
# ----------------------------------------------------------------------

suppressPackageStartupMessages({

# --- hardening: garanta am_resolve_export_dir mesmo sem _common.R ---
if (!exists("am_resolve_export_dir", mode = "function", inherits = TRUE)

) {
  `%||%` <- function(a,b) if (is.null(a)) b else a
  am_resolve_export_dir <- function(out_dir = NULL) {
    od <- if (!is.null(out_dir) && nzchar(out_dir)) {
      normalizePath(out_dir, mustWork = FALSE)
    } else {
      dbp <- tryCatch(am_args[["db"]], error = function(e) NULL) %||% ""
      base_dir <- if (nzchar(dbp)) normalizePath(file.path(dirname(dbp), ".."), mustWork = FALSE) else getwd()
      file.path(base_dir, "graphs_and_tables", "exports")
    }
    if (!dir.exists(od)) dir.create(od, recursive = TRUE, showWarnings = FALSE)
    od
  }
}
  library(DBI); library(RSQLite); library(ggplot2); library(dplyr)
  library(lubridate); library(scales); library(stringr); library(readr); library(forcats)
})

# --- begin: am_db_reconnect_helpers ---
if (!exists("am_safe_disconnect", mode="function", inherits=TRUE)) {
  am_safe_disconnect <- function(con) {
    try({
      if (inherits(con, "DBIConnection") && DBI::dbIsValid(con)) DBI::dbDisconnect(con)
    }, silent=TRUE)
  }
}
if (!exists("am__db_path_guess", mode="function", inherits=TRUE)) {
  am__db_path_guess <- function() {
    tryCatch({
      if (exists("am_args", inherits=TRUE)) {
        p <- tryCatch(am_args[["db"]], error=function(e) NULL)
        if (!is.null(p) && nzchar(p)) return(p)
      }
      if (exists("opt", inherits=TRUE)) {
        p <- tryCatch(opt$db, error=function(e) NULL)
        if (!is.null(p) && nzchar(p)) return(p)
      }
      NULL
    }, error=function(e) NULL)
  }
}
if (!exists("am_ensure_con", mode="function", inherits=TRUE)) {
  am_ensure_con <- function(con) {
    if (inherits(con, "DBIConnection") && DBI::dbIsValid(con)) return(con)
    dbp <- am__db_path_guess()
    if (is.null(dbp) || !nzchar(dbp)) stop("am_ensure_con: caminho do DB desconhecido")
    DBI::dbConnect(RSQLite::SQLite(), dbname = dbp)
  }
}
if (!exists("am_dbGetQuery", mode="function", inherits=TRUE)) {
  am_dbGetQuery <- function(con, ...) { con <- am_ensure_con(con); DBI::dbGetQuery(con, ...) }
}
if (!exists("am_dbReadTable", mode="function", inherits=TRUE)) {
  am_dbReadTable <- function(con, ...) { con <- am_ensure_con(con); DBI::dbReadTable(con, ...) }
}
if (!exists("am_dbListFields", mode="function", inherits=TRUE)) {
  am_dbListFields <- function(con, ...) { con <- am_ensure_con(con); DBI::dbListFields(con, ...) }
}
# --- fix: am_dbQuoteIdentifier sem recursão ---
if (!exists("am_dbQuoteIdentifier", mode="function", inherits=TRUE)) {
  am_dbQuoteIdentifier <<- (function(.f){
    force(.f)
    function(con, ...) {
      con <- am_ensure_con(con)
      as.character(.f(con, ...))
    }
  })(DBI::dbQuoteIdentifier)
}
# --- fim do fix ---

# --- end: am_db_reconnect_helpers ---







# ==== ATESTMED PROLOGO (INICIO) ====
local({
  .am_loaded <- FALSE
  for (pp in c("r_checks/_common.R","./_common.R","../r_checks/_common.R")) {
    if (file.exists(pp)) { source(pp, local=TRUE); .am_loaded <- TRUE; break }
  }
  if (!.am_loaded) message("[prolog] _common.R não encontrado — usando fallbacks internos.")
  `%||%` <- function(a,b) if (is.null(a)) b else a
  if (!exists("am_normalize_cli", mode="function", inherits=TRUE)) am_normalize_cli <<- function(x) as.character(x)
  if (!exists("am_parse_args", mode="function", inherits=TRUE)) {
    am_parse_args <<- function() {
      a <- am_normalize_cli(base::commandArgs(TRUE))
      kv <- list(); i <- 1L; n <- length(a)
      while (i <= n) { k <- a[[i]]
        if (startsWith(k, "--")) {
          v <- if (i+1L <= n && !startsWith(a[[i+1L]], "--")) a[[i+1L]] else TRUE
          kv[[sub("^--","",k)]] <- v; i <- i + (if (identical(v, TRUE)) 1L else 2L)
        } else i <- i + 1L
      }; kv
    }
  }
  if (!exists("am_open_db", mode="function", inherits=TRUE)) {
    am_open_db <<- function(path) { p <- normalizePath(path, mustWork=TRUE); DBI::dbConnect(RSQLite::SQLite(), dbname=p) }
  }
  if (!exists("am_resolve_export_dir", mode="function", inherits=TRUE)) {
    am_resolve_export_dir <<- function(out_dir=NULL) {
      if (!is.null(out_dir) && nzchar(out_dir)) normalizePath(out_dir, mustWork=FALSE) else {
        dbp <- am_args[["db"]] %||% ""
        base_dir <- if (nzchar(dbp)) normalizePath(file.path(dirname(dbp), ".."), mustWork=FALSE) else getwd()
        od <- file.path(base_dir, "graphs_and_tables", "exports")
        if (!dir.exists(od)) dir.create(od, recursive=TRUE, showWarnings=FALSE); od
      }
    }
  }
  if (!exists("am_detect_analises_table", mode="function", inherits=TRUE)) {
    am_detect_analises_table <<- function(con) {
      has <- function(nm) nrow(am_dbGetQuery(con,"SELECT 1 FROM sqlite_master WHERE type in ('table','view') AND name=? LIMIT 1", params=list(nm)))>0
      for (t in c("analises","analises_atestmed")) if (has(t)) return(t)
      stop("Não encontrei 'analises' nem 'analises_atestmed'.")
    }
  }
  am_dbGetQuery <<- (function(.f){ force(.f); function(con, statement, ...){ st <- if (length(statement)==1L) statement else paste(statement, collapse=" "); .f(con, st, ...) } })(DBI::dbGetQuery)

  # args + conexão única
  am_args <<- tryCatch(am_parse_args(), error=function(e) list())
  db_path <- am_args[["db"]]; if (is.null(db_path) || !nzchar(db_path)) stop("Faltou --db <path>")
  con <<- am_open_db(db_path)
  # (patch) removido on.exit de desconexão precoce

  export_dir <<- am_resolve_export_dir(am_args[["out-dir"]])
  a_tbl     <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
})
# ==== ATESTMED PROLOGO (FIM) ====


`%||%` <- function(a,b) if (is.null(a)) b else a

# --------- parse_args local ---------
parse_args <- function() {
  a <- am_normalize_cli(base::commandArgs(TRUE))
  kv <- list(); i <- 1L; n <- length(a)
  while (i <= n) {
    k <- a[[i]]
    if (startsWith(k, "--")) {
      v <- if (i+1L <= n && !startsWith(a[[i+1L]], "--")) a[[i+1L]] else TRUE
      kv[[substr(k, 3, nchar(k))]] <- v
      i <- i + (if (identical(v, TRUE)) 1L else 2L)
    } else i <- i + 1L
  }
  kv
}

fail_plot <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()
safe_slug <- function(x){ x <- gsub("[^A-Za-z0-9\\-_]+","_", x); x <- gsub("_+","_", x); x <- gsub("^_|_$","", x); ifelse(nchar(x)>0, x, "output") }

# ───────────────────────── Args/paths ─────────────────────────
args <- parse_args()
db_path     <- args$db
start_d     <- args$start
end_d       <- args$end
min_n       <- as.integer(args[["min-analises"]] %||% "50")
le_thr      <- as.integer(args[["le-threshold"]] %||% "15")
out_dir     <- args[["out-dir"]]
peritos_csv <- args[["peritos-csv"]] %||% NULL
scope_csv   <- args[["scope-csv"]]   %||% NULL     # NOVO: ESCOPO/coorte
flow_opt    <- toupper(args[["flow"]] %||% "")
rank_by_opt <- tolower(args[["rank-by"]] %||% "")

if (is.null(db_path) || is.null(start_d) || is.null(end_d)) {
  stop("Uso: --db <path> --start YYYY-MM-DD --end YYYY-MM-DD [--min-analises 50] [--le-threshold 15] [--peritos-csv <csv>] [--scope-csv <csv>] [--flow A|B] [--rank-by scoreFinal|harm] [--out-dir <dir>]")
}

export_dir <- am_resolve_export_dir(out_dir)
png_file   <- file.path(export_dir, "rcheck_top10_composite_robustness.png")
org_main   <- file.path(export_dir, "rcheck_top10_composite_robustness.org")
org_comm   <- file.path(export_dir, "rcheck_top10_composite_robustness_comment.org")

# ───────────────────────── Conexão/schema ─────────────────────
table_exists <- function(con, name) nrow(am_dbGetQuery(con,"SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1", params=list(name)))>0
cols <- tryCatch(am_dbGetQuery(con, sprintf("PRAGMA table_info(%s)", a_tbl))$name, error=function(e) character(0))
has_end <- "dataHoraFimPericia" %in% cols
cand_dur_num <- intersect(cols, c("tempoAnaliseSeg","tempoAnalise","duracaoSegundos","duracao_seg","tempo_seg"))
dur_num_col  <- if (length(cand_dur_num)) cand_dur_num[[1]] else NA_character_
cand_dur_txt <- intersect(cols, c("duracaoPericia","duracao_txt","tempoFmt","tempo_formatado"))
dur_txt_col  <- if (length(cand_dur_txt)) cand_dur_txt[[1]] else NA_character_

# ───────────────────── Helpers de leitura ─────────────────────
load_names <- function(path){
  if (is.null(path) || !nzchar(path) || !file.exists(path)) return(NULL)
  df <- tryCatch(readr::read_csv(path, show_col_types=FALSE), error=function(e) NULL)
  if (is.null(df) || !nrow(df)) return(NULL)
  key <- if ("nomePerito" %in% names(df)) "nomePerito" else names(df)[1]
  u <- unique(trimws(as.character(df[[key]]))); u[nzchar(u)]
}
to_upper <- function(xs) unique(toupper(trimws(xs)))

# ───────────────────── ESCOPO opcional ─────────────────────
scope_names  <- to_upper(load_names(scope_csv))
scope_clause <- ""
scope_params <- list()
if (length(scope_names)) {
  placeholders <- paste(rep("?", length(scope_names)), collapse=",")
  scope_clause <- sprintf(" AND TRIM(UPPER(p.nomePerito)) IN (%s) ", placeholders)
  scope_params <- as.list(scope_names)
}

# ───────────────────── Seleção de peritos (Fluxo/Manifesto) ────────────────────
rank_by <- if (nzchar(rank_by_opt)) rank_by_opt else if (identical(flow_opt, "B")) "harm" else "scorefinal"

need_indicadores <- is.null(peritos_csv)
if (need_indicadores && !table_exists(con, "indicadores")) {
  ggsave(png_file, fail_plot("Tabela 'indicadores' não encontrada (e sem --peritos-csv)."), width=10, height=6, dpi=150); quit(save="no")
}
if (need_indicadores) {
  ind_cols <- am_dbGetQuery(con, "PRAGMA table_info(indicadores)")$name
  need_col <- if (rank_by=="harm") "harm" else "scoreFinal"
  if (!(need_col %in% ind_cols)) {
    ggsave(png_file, fail_plot(sprintf("Coluna '%s' ausente em 'indicadores'.", need_col)), width=10, height=6, dpi=150); quit(save="no")
  }
}

sel_caption <- NULL; sel_title <- NULL
names_manifest <- load_names(peritos_csv)

if (!is.null(names_manifest)) {
  # revalida n ≥ min_n preservando a ordem do manifesto (APLICA ESCOPO)
  in_list <- paste(sprintf("'%s'", gsub("'", "''", names_manifest)), collapse=",")
  sql_cnt <- sprintf("
    SELECT p.nomePerito AS nomePerito, COUNT(a.protocolo) AS total_analises
      FROM %s a
      JOIN peritos p ON a.siapePerito = p.siapePerito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
       AND p.nomePerito IN (%s) %s
     GROUP BY p.nomePerito
  ", am_dbQuoteIdentifier(con, a_tbl), in_list, scope_clause)
  cnt  <- do.call(am_dbGetQuery, c(list(con, sql_cnt), list(params=c(list(start_d, end_d), scope_params))))
  alvo <- cnt %>% filter(total_analises >= min_n) %>%
    arrange(match(nomePerito, names_manifest)) %>% slice_head(n=10) %>% select(nomePerito)
  if (!nrow(alvo)) { ggsave(png_file, fail_plot("Manifesto sem peritos elegíveis (min-analises/escopo)."), width=10, height=6, dpi=150); quit(save="no") }
  sel_caption <- sprintf("Seleção: manifesto (revalidado: n ≥ %d%s).", min_n, if (length(scope_names)) "; escopo aplicado" else "")
  sel_title   <- "Top 10 — (manifesto)"
} else {
  # seleção interna (APLICA ESCOPO)
  order_col <- if (rank_by=="harm") "i.harm" else "i.scoreFinal"
  sql_top10 <- sprintf("
    SELECT p.nomePerito AS nomePerito, %s AS rank_metric, COUNT(a.protocolo) AS total_analises
      FROM indicadores i
      JOIN peritos p ON i.perito = p.siapePerito
      JOIN %s a ON a.siapePerito = i.perito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ? %s
     GROUP BY p.nomePerito, rank_metric
    HAVING total_analises >= ?
     ORDER BY rank_metric DESC, total_analises DESC
     LIMIT 10
  ", order_col, am_dbQuoteIdentifier(con, a_tbl), scope_clause)
  params_top10 <- c(list(start_d, end_d), scope_params, list(min_n))
  alvo <- do.call(am_dbGetQuery, c(list(con, sql_top10), list(params=params_top10)))
  if (!nrow(alvo)) { ggsave(png_file, fail_plot("Sem Top 10 para o período/critério (após escopo)."), width=10, height=6, dpi=150); quit(save="no") }
  sel_caption <- sprintf("Seleção: %s (n ≥ %d%s).",
                         if (rank_by=="harm") "Top10 por harm (Fluxo B)" else "Top10 por scoreFinal (Fluxo A)",
                         min_n, if (length(scope_names)) "; escopo aplicado" else "")
  sel_title   <- "Top 10 — (ranking interno)"
}

peritos <- paste(sprintf("'%s'", gsub("'", "''", alvo$nomePerito)), collapse=",")

# ───────────────────────── base crua (APLICA ESCOPO) ─────────────────────────
sel_cols <- c("p.nomePerito AS nomePerito", "a.dataHoraIniPericia AS ini")
if (has_end)             sel_cols <- c(sel_cols, "a.dataHoraFimPericia AS fim")
if (!is.na(dur_num_col)) sel_cols <- c(sel_cols, sprintf("a.%s AS dur_num", dur_num_col))
if (!is.na(dur_txt_col)) sel_cols <- c(sel_cols, sprintf("a.%s AS dur_txt", dur_txt_col))
sel_cols <- unique(sel_cols)

qry_base <- sprintf("
SELECT %s, a.conformado AS conformado, a.motivoNaoConformado AS motivoNaoConformado
  FROM %s a
  JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
   AND p.nomePerito IN (%s) %s
", paste(sel_cols, collapse=", "), am_dbQuoteIdentifier(con, a_tbl), peritos, scope_clause)

base <- do.call(am_dbGetQuery, c(list(con, qry_base), list(params=c(list(start_d, end_d), scope_params))))
if (!nrow(base)) { ggsave(png_file, fail_plot("Sem dados no período para os selecionados (após escopo)"), width=10, height=6, dpi=150); quit(save="no") }

# ───────────────────────── duração robusta ─────────────────────────
parse_hms_one <- function(s) {
  s <- as.character(s %||% ""); s <- trimws(s)
  if (s == "" || s %in% c("0","00:00","00:00:00")) return(NA_real_)
  if (grepl(":", s, fixed = TRUE)) {
    parts <- strsplit(s, ":", fixed = TRUE)[[1]]
    if (length(parts) == 3) {
      suppressWarnings({ h <- as.numeric(parts[1]); m <- as.numeric(parts[2]); sec <- as.numeric(parts[3]) })
      if (any(is.na(c(h,m,sec)))) return(NA_real_) else return(h*3600 + m*60 + sec)
    }
    if (length(parts) == 2) {
      suppressWarnings({ m <- as.numeric(parts[1]); sec <- as.numeric(parts[2]) })
      if (any(is.na(c(m,sec)))) return(NA_real_) else return(m*60 + sec)
    }
    return(NA_real_)
  }
  suppressWarnings(x <- as.numeric(s)); ifelse(is.finite(x) && x > 0, x, NA_real_)
}

base <- base %>%
  mutate(
    ini_dt = ymd_hms(ini, quiet = TRUE),
    fim_dt = if ("fim" %in% names(base)) ymd_hms(fim, quiet = TRUE) else as.POSIXct(NA)
  )

dur_s <- as.numeric(difftime(base$fim_dt, base$ini_dt, units = "secs"))
dur_s[!is.finite(dur_s)] <- NA_real_
if ("dur_num" %in% names(base)) {
  dn <- suppressWarnings(as.numeric(base$dur_num))
  need <- is.na(dur_s) | dur_s <= 0
  dur_s[need] <- ifelse(is.finite(dn[need]) & dn[need] > 0, dn[need], dur_s[need])
}
if ("dur_txt" %in% names(base)) {
  need <- is.na(dur_s) | dur_s <= 0
  if (any(need, na.rm=TRUE)) {
    fb <- vapply(base$dur_txt[need], parse_hms_one, numeric(1))
    fb[!is.finite(fb)] <- NA_real_
    dur_s[need] <- fb
  }
}
if (!"fim" %in% names(base) || all(!is.finite(base$fim_dt))) {
  base$fim_dt <- base$ini_dt + dseconds(dur_s)
} else {
  need_fim <- !is.finite(base$fim_dt) & is.finite(base$ini_dt) & is.finite(dur_s)
  base$fim_dt[need_fim] <- base$ini_dt[need_fim] + dseconds(dur_s[need_fim])
}
base$dur_s <- as.numeric(dur_s)

base <- base %>% filter(is.finite(dur_s), dur_s > 0, dur_s <= 3600)
if (!nrow(base)) { ggsave(png_file, fail_plot("Sem análises válidas (duração) no período"), width=10, height=6, dpi=150); quit(save="no") }

# ───────────────────────── NC robusto ─────────────────────────
nc_flag <- function(conformado, motivo) {
  c0 <- suppressWarnings(as.integer(ifelse(is.na(conformado), 1L, conformado))) == 0L
  motivo_txt <- ifelse(is.na(motivo), "", trimws(as.character(motivo)))
  motivo_int <- suppressWarnings(as.integer(ifelse(motivo_txt == "", "0", motivo_txt)))
  m_ok <- (motivo_txt != "") & !is.na(motivo_int) & (motivo_int != 0L)
  c0 | m_ok
}
nc <- base %>%
  mutate(nc = nc_flag(conformado, motivoNaoConformado)) %>%
  group_by(nomePerito) %>%
  summarise(total = n(), nc = sum(nc, na.rm=TRUE), .groups="drop") %>%
  mutate(pct_nc = ifelse(total>0, 100*nc/total, NA_real_)) %>%
  select(nomePerito, pct_nc)

# ≤ le_thr (entre válidas)
le_df <- base %>%
  group_by(nomePerito) %>%
  summarise(total = n(),
            n_le  = sum(dur_s <= le_thr, na.rm=TRUE),
            pct_le = ifelse(total>0, 100*n_le/total, NA_real_),
            .groups="drop") %>%
  select(nomePerito, pct_le)

# produtividade (análises/h) entre válidas
prod <- base %>%
  group_by(nomePerito) %>%
  summarise(total = n(),
            sum_s = sum(dur_s, na.rm=TRUE),
            prod_h = ifelse(sum_s>0, total/(sum_s/3600), NA_real_),
            .groups="drop") %>%
  select(nomePerito, prod_h)

# overlap % (share de tarefas com início antes do fim acumulado)
overlap_share <- function(tb) {
  tb <- tb %>% arrange(ini_dt, fim_dt)
  n <- nrow(tb); if (n <= 1) return(0)
  overl <- logical(n)
  last_end <- tb$fim_dt[1]
  for (i in 2:n) {
    overl[i] <- tb$ini_dt[i] < last_end
    last_end <- max(last_end, tb$fim_dt[i], na.rm=TRUE)
  }
  mean(overl, na.rm=TRUE) * 100
}
ovm <- base %>%
  select(nomePerito, ini_dt, fim_dt) %>%
  group_by(nomePerito) %>%
  summarise(pct_overlap = overlap_share(cur_data()), .groups="drop")

# composição (z-score; produtividade invertida)
z <- function(x) if (all(is.na(x))) rep(NA_real_, length(x)) else as.numeric(scale(x))

full <- alvo %>%
  select(nomePerito) %>%
  left_join(nc,    by="nomePerito") %>%
  left_join(le_df, by="nomePerito") %>%
  left_join(ovm,   by="nomePerito") %>%
  left_join(prod,  by="nomePerito") %>%
  mutate(
    z_nc   = z(pct_nc),
    z_le   = z(pct_le),
    z_ov   = z(pct_overlap),
    z_prod = z(-prod_h)
  )

# média de z-scores; se todos NA na linha, devolve NA
full$composite <- apply(full[,c("z_nc","z_le","z_ov","z_prod")], 1, function(r){
  r <- as.numeric(r)
  if (all(is.na(r))) NA_real_ else mean(r, na.rm = TRUE)
})

full <- full %>%
  arrange(desc(composite)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito),
         lbl = sprintf("z_nc=%s, z_≤%ds=%s, z_ov=%s, z_prod=%s",
                       ifelse(is.finite(z_nc), sprintf("%.2f", z_nc), "NA"),
                       le_thr,
                       ifelse(is.finite(z_le), sprintf("%.2f", z_le), "NA"),
                       ifelse(is.finite(z_ov), sprintf("%.2f", z_ov), "NA"),
                       ifelse(is.finite(z_prod), sprintf("%.2f", z_prod), "NA")))

if (nrow(full) == 0 || all(is.na(full$composite))) {
  ggsave(png_file, fail_plot("Sem dados suficientes para compor o índice"), width=10, height=6, dpi=150); quit(save="no")
}

# ───────────────────────── Plot ─────────────────────────
p <- ggplot(full, aes(x=nomePerito, y=composite)) +
  geom_col() +
  geom_text(aes(label=lbl), vjust=-0.3, size=3) +
  labs(
    title    = "Top 10 — Robustez do Composto (z-score médio)",
    subtitle = sprintf("%s a %s | maior = pior (padronizado) | %s", start_d, end_d, sel_caption),
    x = "Perito", y = "z-score médio",
    caption = sprintf("Composto = média(z_nc, z_≤%ds, z_ov, z_prod_inv). Produtividade invertida (maior = pior). Duração válida: 0<dur≤3600s.", le_thr)
  ) +
  theme_minimal(base_size = 11) +
  theme(axis.text.x = element_text(angle=45, hjust=1))

ymin <- suppressWarnings(min(full$composite, na.rm = TRUE)); if (!is.finite(ymin)) ymin <- 0
ymax <- suppressWarnings(max(full$composite, na.rm = TRUE)); if (!is.finite(ymax)) ymax <- 0
padl <- if (ymin < 0) abs(ymin)*0.15 else 0.1
padu <- if (ymax > 0) ymax*0.15 else 0.1

ggsave(png_file, p + coord_cartesian(ylim = c(ymin - padl, ymax + padu)), width=10, height=6, dpi=150)
base::cat(sprintf("✅ Figura salva: %s\n", png_file))

# ───────────────────────── Comentários (.org) ─────────────────
top_worst <- full %>% dplyr::slice_head(n = 3)
top_best  <- full %>% arrange(composite) %>% dplyr::slice_head(n = 3)

escopo_txt <- if (length(scope_names)) {
  paste0("\n*Escopo.* Coorte com ", length(scope_names), " peritos; primeiros: ",
         paste(head(scope_names, 5), collapse = ", "),
         if (length(scope_names) > 5) ", …" else "", ".")
} else ""

metodo_txt <- paste0(
  "*Método.* Seleção de peritos: ",
  if (!is.null(names_manifest))
    sprintf("manifesto (revalidado: n ≥ %d). ", min_n)
  else if (rank_by=="harm")
    sprintf("Top 10 por *harm* (mín. %d análises). ", min_n)
  else
    sprintf("Top 10 por *scoreFinal* (mín. %d análises). ", min_n),
  "Indicadores por perito no período (", start_d, " a ", end_d, "): ",
  "%NC *robusto*, % ≤ ", le_thr, "s (entre válidas 0<dur≤3600), % de *sobreposição* e *produtividade* (análises/h). ",
  "Cada métrica foi padronizada via *z-score*; produtividade foi *invertida* (maior = pior). ",
  "O composto é a *média* dos z-scores.",
  escopo_txt
)

interp_lines <- c(
  "*Interpretação.* Barras mais altas indicam pior desempenho composto.",
  if (nrow(top_worst)) {
    paste0("- *Piores (top 3)*: ",
           paste0(sprintf("%s (comp=%.2f)", as.character(top_worst$nomePerito), top_worst$composite), collapse = "; "), ".")
  } else NULL,
  if (nrow(top_best)) {
    paste0("- *Melhores (top 3)*: ",
           paste0(sprintf("%s (comp=%.2f)", as.character(top_best$nomePerito), top_best$composite), collapse = "; "), ".")
  } else NULL,
  "- Leia os rótulos (z_nc, z_≤limiar, z_ov, z_prod) para pistas de quais dimensões puxam o composto."
)
interpreta_txt <- paste(interp_lines[!is.na(interp_lines)], collapse = "\n")

# .org principal (imagem + comentário)
org_main_txt <- paste(
  "#+CAPTION: Top 10 — Robustez do Composto (z-score médio)",
  sprintf("[[file:%s]]", basename(png_file)),
  "",
  metodo_txt, "",
  interpreta_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
base::cat(sprintf("✅ Org salvo: %s\n", org_main))

# .org apenas comentário
org_comm_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comm_txt, org_comm)
base::cat(sprintf("✅ Org(comment) salvo: %s\n", org_comm))
