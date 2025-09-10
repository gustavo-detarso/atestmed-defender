#!/usr/bin/env Rscript
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
  library(optparse); library(DBI); library(RSQLite)
  library(dplyr); library(ggplot2); library(scales); library(lubridate)
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
  if (!exists("am_parse_args",   mode="function", inherits=TRUE)) {
    am_parse_args <<- function() {
      a <- am_normalize_cli(base::commandArgs(TRUE))
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
    am_open_db <<- function(path) { p <- normalizePath(path, mustWork=TRUE); DBI::dbConnect(RSQLite::SQLite(), dbname=p) }
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
      has <- function(nm) nrow(am_dbGetQuery(con,
        "SELECT 1 FROM sqlite_master WHERE type in ('table','view') AND name=? LIMIT 1",
        params=list(nm))) > 0
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
  # am_dbGetQuery deve existir ANTES de qualquer uso
  if (!exists("am_dbGetQuery", mode="function", inherits=TRUE)) {
    am_dbGetQuery <<- (function(.f){ force(.f); function(con, statement, ...){
      st <- if (length(statement)==1L) statement else paste(statement, collapse=" ")
      .f(con, st, ...)
    } })(DBI::dbGetQuery)
  }

  # args + conexão única
  am_args <<- tryCatch(am_parse_args(), error=function(e) list())
  db_path <- am_args[["db"]]; if (is.null(db_path) || !nzchar(db_path)) stop("Faltou --db <path>")
  con <<- am_open_db(db_path)
  # (patch) removido on.exit de desconexão precoce

  export_dir <<- am_resolve_export_dir(am_args[["out-dir"]])
  a_tbl     <<- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
  cols <- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))
})
# ==== ATESTMED PROLOGO (FIM) ====


# ────────────────────────────────────────────────────────────────────────────────
# CLI (local)
opt_list <- list(
  make_option("--db",              type="character"),
  make_option("--start",           type="character"),
  make_option("--end",             type="character"),
  make_option("--perito",          type="character"),
  make_option("--prod-threshold",  type="double",  default=50),
  make_option("--le-threshold",    type="integer", default=15),
  make_option("--out-dir",         type="character", default=NULL)
)
opt <- optparse::parse_args(OptionParser(option_list = opt_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
safe  <- function(x) gsub("(^_+|_+$)", "", gsub("[^A-Za-z0-9_-]+","_", x))
percent_s <- function(x, acc=.1) ifelse(is.finite(x), percent(x, accuracy=acc), "NA")
normalize01 <- function(v){
  if (all(is.na(v))) return(v)
  mn <- suppressWarnings(min(v, na.rm=TRUE)); mx <- suppressWarnings(max(v, na.rm=TRUE))
  if (!is.finite(mn) || !is.finite(mx) || mx <= mn) return(ifelse(is.na(v), NA_real_, 0))
  (v - mn)/(mx - mn)
}
table_exists <- function(con, name) nrow(am_dbGetQuery(con,"SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1", params=list(name)))>0

perito_safe <- safe(opt$perito)
export_dir  <- am_resolve_export_dir(opt$`out-dir`)

# ────────────────────────────────────────────────────────────────────────────────
# Colunas disponíveis (para duração robusta)
cols <- tryCatch(am_dbGetQuery(con, sprintf("PRAGMA table_info(%s)", a_tbl))$name, error=function(e) character(0))
has_end     <- "dataHoraFimPericia" %in% cols
cand_durnum <- intersect(cols, c("tempoAnaliseSeg","tempoAnalise","duracaoSegundos","duracao_seg","tempo_seg"))
dur_num_col <- if (length(cand_durnum)) cand_durnum[[1]] else NA_character_
cand_durtxt <- intersect(cols, c("duracaoPericia","duracao_txt","tempoFmt","tempo_formatado"))
dur_txt_col <- if (length(cand_durtxt)) cand_durtxt[[1]] else NA_character_

parse_hms_one <- function(s) {
  s <- as.character(s %||% ""); s <- trimws(s)
  if (s=="" || s %in% c("0","00:00","00:00:00")) return(NA_real_)
  if (grepl(":", s, fixed=TRUE)) {
    parts <- strsplit(s, ":", fixed=TRUE)[[1]]
    if (length(parts)==3) {
      suppressWarnings({h<-as.numeric(parts[1]); m<-as.numeric(parts[2]); se<-as.numeric(parts[3])})
      if (any(is.na(c(h,m,se)))) return(NA_real_) else return(h*3600+m*60+se)
    }
    if (length(parts)==2) {
      suppressWarnings({m<-as.numeric(parts[1]); se<-as.numeric(parts[2])})
      if (any(is.na(c(m,se)))) return(NA_real_) else return(m*60+se)
    }
    return(NA_real_)
  }
  suppressWarnings(x <- as.numeric(s)); ifelse(is.finite(x) && x>0, x, NA_real_)
}

# ────────────────────────────────────────────────────────────────────────────────
# NC robusto (rate por perito)
nc_expr <- "
CASE
  WHEN CAST(IFNULL(a.conformado,1) AS INTEGER)=0 THEN 1
  WHEN TRIM(IFNULL(a.motivoNaoConformado,'')) <> '' AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0 THEN 1
  ELSE 0
END
"
sql_nc <- sprintf("
SELECT p.nomePerito AS perito, SUM(%s) AS nc, COUNT(*) AS n
FROM %s a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
GROUP BY p.nomePerito
", nc_expr, am_dbQuoteIdentifier(con, a_tbl))
df_nc <- am_dbGetQuery(con, sql_nc, params=list(opt$start, opt$end)) %>%
  mutate(nc_rate = ifelse(n>0, nc/n, NA_real_)) %>% select(perito, nc_rate)

# ────────────────────────────────────────────────────────────────────────────────
# Durações robustas (fim−ini; fallback numérico e HH:MM:SS), válidas 0<dur≤3600
sel_cols <- c("p.nomePerito AS perito", "a.dataHoraIniPericia AS ini")
if (has_end)                   sel_cols <- c(sel_cols, "a.dataHoraFimPericia AS fim")
if (!is.na(dur_num_col))       sel_cols <- c(sel_cols, sprintf("a.%s AS dur_num", dur_num_col))
if (!is.na(dur_txt_col))       sel_cols <- c(sel_cols, sprintf("a.%s AS dur_txt", dur_txt_col))
sel_cols <- unique(sel_cols)

sql_base <- sprintf("
SELECT %s
FROM %s a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
", paste(sel_cols, collapse=", "), am_dbQuoteIdentifier(con, a_tbl))
base <- am_dbGetQuery(con, sql_base, params=list(opt$start, opt$end))
if (!nrow(base)) {
  gg <- ggplot() + annotate("text", x=0, y=0, label="Sem dados no período", size=5) + theme_void()
  png_path <- file.path(export_dir, sprintf("rcheck_composite_%s.png", perito_safe))
  ggsave(png_path, gg, width=8.5, height=5, dpi=160); cat(sprintf("✓ salvo: %s\n", png_path)); quit(save="no")
}

base <- base %>%
  mutate(ini_dt = ymd_hms(ini, quiet=TRUE),
         fim_dt = if ("fim" %in% names(base)) ymd_hms(fim, quiet=TRUE) else as.POSIXct(NA))

dur_s <- as.numeric(difftime(base$fim_dt, base$ini_dt, units="secs"))
dur_s[!is.finite(dur_s)] <- NA_real_
if ("dur_num" %in% names(base)) {
  dn <- suppressWarnings(as.numeric(base$dur_num))
  need <- is.na(dur_s) | dur_s <= 0
  dur_s[need] <- ifelse(is.finite(dn[need]) & dn[need]>0, dn[need], dur_s[need])
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

df_valid <- base %>%
  filter(is.finite(ini_dt), is.finite(fim_dt), is.finite(dur_s), dur_s > 0, dur_s <= 3600) %>%
  select(perito, ini=ini_dt, fim=fim_dt, dur_s)

if (!nrow(df_valid)) {
  gg <- ggplot() + annotate("text", x=0, y=0, label="Sem análises válidas (0<dur≤3600s)", size=5) + theme_void()
  png_path <- file.path(export_dir, sprintf("rcheck_composite_%s.png", perito_safe))
  ggsave(png_path, gg, width=8.5, height=5, dpi=160); cat(sprintf("✓ salvo: %s\n", png_path)); quit(save="no")
}

# ≤ threshold (entre válidas)
df_le <- df_valid %>%
  group_by(perito) %>%
  summarise(le = sum(dur_s <= opt$`le-threshold`, na.rm=TRUE),
            n  = n(), .groups="drop") %>%
  mutate(le_rate = ifelse(n>0, le/n, NA_real_)) %>%
  select(perito, le_rate)

# Produtividade (entre válidas)
df_pd <- df_valid %>%
  group_by(perito) %>%
  summarise(total = n(), segs = sum(dur_s, na.rm=TRUE), .groups="drop") %>%
  mutate(prod = ifelse(segs>0, total/(segs/3600), NA_real_)) %>%
  select(perito, prod)

# Overlap (flag por perito, entre válidas)
overlap_flag <- function(ini, fim) { if (length(ini)<2) return(FALSE); any(ini[-1] < fim[-length(fim)]) }
df_ov <- df_valid %>%
  arrange(perito, ini) %>%
  group_by(perito) %>%
  summarise(overlap = overlap_flag(ini, fim), .groups="drop")

# Junta tudo
df <- df_nc %>%
  left_join(df_le, by="perito") %>%
  left_join(df_pd, by="perito") %>%
  left_join(df_ov, by="perito") %>%
  mutate(overlap = ifelse(is.na(overlap), FALSE, overlap))

if (!(opt$perito %in% df$perito)) {
  sim <- df %>% filter(grepl(opt$perito, perito, ignore.case=TRUE)) %>% pull(perito) %>% unique()
  msg <- if (length(sim)) paste0(" Sugeridos: ", paste(sim, collapse=", "), ".") else ""
  stop(sprintf("Perito '%s' não encontrado no período.%s", opt$perito, msg))
}

# Normalizações (quanto mais alto, "pior")
max_prod <- suppressWarnings(max(df$prod, na.rm=TRUE)); if (!is.finite(max_prod)) max_prod <- 0
df <- df %>%
  mutate(prod_inv = max_prod - prod,
         nc_rate_norm  = normalize01(nc_rate),
         le_rate_norm  = normalize01(le_rate),
         prod_inv_norm = normalize01(prod_inv),
         overlap_norm  = ifelse(overlap, 1, 0))

df$score <- rowMeans(df[,c("nc_rate_norm","le_rate_norm","prod_inv_norm","overlap_norm")], na.rm=TRUE)

p_row      <- df %>% filter(perito==opt$perito) %>% slice(1)
mean_score <- mean(df$score, na.rm=TRUE)

plot_df <- tibble::tibble(
  Indicador = c("NC rate (robusto)", sprintf("≤%ds", opt$`le-threshold`), "Prod (invertida)", "Overlap"),
  Valor     = c(p_row$nc_rate_norm,   p_row$le_rate_norm,                 p_row$prod_inv_norm, p_row$overlap_norm)
)

gg <- ggplot(plot_df, aes(Indicador, Valor)) +
  geom_col(fill="#d62728", width=.6) +
  geom_hline(yintercept = mean_score, linetype="dashed", color="#1f77b4", linewidth=.4) +
  coord_cartesian(ylim=c(0,1.05)) +
  labs(
    title    = "Robustez do Composto — posição do perito (normalizado 0–1)",
    subtitle = sprintf("%s a %s | score do perito = %.2f (média ref. tracejada) | prod≥%.0f/h, ≤%ds",
                       opt$start, opt$end, p_row$score, opt$`prod-threshold`, opt$`le-threshold`),
    y="Escala normalizada (0–1)", x=NULL,
    caption="NC robusto: conformado=0 OU (motivoNaoConformado≠'' e CAST(...)≠0). Durações válidas: 0<dur≤3600s."
  ) +
  theme_minimal(base_size=11) +
  theme(panel.grid.major.x = element_blank())

png_path <- file.path(export_dir, sprintf("rcheck_composite_%s.png", perito_safe))
ggsave(png_path, gg, width=8.5, height=5, dpi=160)
cat(sprintf("✓ salvo: %s\n", png_path))

# Comentários (.org)
nc_txt  <- percent_s(p_row$nc_rate, acc=.1)
le_txt  <- percent_s(p_row$le_rate, acc=.1)
prod_tx <- ifelse(is.finite(p_row$prod), sprintf("%.2f/h", p_row$prod), "NA")
ov_txt  <- ifelse(isTRUE(p_row$overlap), "Sim", "Não")
pos_txt <- ifelse(is.finite(p_row$score) & is.finite(mean_score) & p_row$score > mean_score,
                  "acima da média nacional (pior)", "abaixo/na média nacional (melhor)")

metodo_txt <- paste0(
  "*Método.* Combinamos quatro indicadores calculados no período (", opt$start, "–", opt$end, "): ",
  "(i) taxa de NC *robusto*; (ii) proporção de perícias ≤ ", opt$`le-threshold`, "s entre durações válidas; ",
  "(iii) produtividade (análises/h), invertida; (iv) ocorrência de *sobreposição* (flag 0/1). ",
  "Cada indicador é min–max para 0–1; o *score* é a média simples. ",
  "Durações válidas: 0<dur≤3600s. A linha tracejada é a *média nacional* do score."
)

interpreta_txt <- paste0(
  "*Interpretação.* Barras próximas de 1 sugerem pior posição relativa; próximas de 0, melhor. ",
  "No período: NC=", nc_txt, "; ≤", opt$`le-threshold`, "s=", le_txt, "; Prod=", prod_tx, "; Overlap=", ov_txt, ". ",
  "Score do perito = ", sprintf("%.2f", p_row$score), " (", pos_txt, "). ",
  "Priorize auditorias nas dimensões com barras mais altas."
)

org_main <- file.path(export_dir, sprintf("rcheck_composite_%s.org", perito_safe))
writeLines(paste(
  "#+CAPTION: Composto (normalizado 0–1) — NC, ≤limiar, Prod (invertida) e Overlap",
  sprintf("[[file:%s]]", basename(png_path)), "", metodo_txt, "", interpreta_txt, "", sep="\n"
), org_main)
