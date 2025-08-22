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















# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────
opt_list <- list(
  make_option("--db",              type="character"),
  make_option("--start",           type="character"),
  make_option("--end",             type="character"),
  make_option("--perito",          type="character"),
  make_option("--prod-threshold",  type="double",  default=50),
  make_option("--le-threshold",    type="integer", default=15),
  make_option("--out-dir",         type="character", default=NULL, help="Diretório de saída (PNG + ORG)")
)
opt <- parse_args(OptionParser(option_list = opt_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
safe <- function(x) { gsub("(^_+|_+$)", "", gsub("[^A-Za-z0-9_-]+","_", x)) }
percent_s <- function(x, acc=.1) ifelse(is.finite(x), percent(x, accuracy = acc), "NA")

perito_safe <- safe(opt$perito)

base_dir   <- normalizePath(file.path(dirname(opt$db), ".."))
export_dir <- if (!is.null(opt$`out-dir`)) normalizePath(opt$`out-dir`, mustWork = FALSE) else
              file.path(base_dir, "graphs_and_tables", "exports")
dir.create(export_dir, showWarnings = FALSE, recursive = TRUE)

table_exists <- function(con, name) {
  nrow(am_dbGetQuery(con, "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
                  params=list(name))) > 0
}
detect_analises_table <- function(con) {
  for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t)
  stop("Não encontrei 'analises' nem 'analises_atestmed'.")
}
normalize01 <- function(v) {
  if (all(is.na(v))) return(v)
  mn <- suppressWarnings(min(v, na.rm=TRUE)); mx <- suppressWarnings(max(v, na.rm=TRUE))
  if (!is.finite(mn) || !is.finite(mx) || mx <= mn) return(ifelse(is.na(v), NA_real_, 0))
  (v - mn) / (mx - mn)
}

# ────────────────────────────────────────────────────────────────────────────────
# Conexão
# ────────────────────────────────────────────────────────────────────────────────
con <- dbConnect(SQLite(), opt$db)
# (patched) # (patched) on.exit(try(dbDisconnect(con), silent=TRUE))

a_tbl <- detect_analises_table(con)

# ────────────────────────────────────────────────────────────────────────────────
# NC robusto (rate por perito)
# ────────────────────────────────────────────────────────────────────────────────
nc_expr <- "
CASE
  WHEN CAST(IFNULL(a.conformado,1) AS INTEGER)=0 THEN 1
  WHEN TRIM(IFNULL(a.motivoNaoConformado,'')) <> ''
       AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0 THEN 1
  ELSE 0
END
"

sql_nc <- sprintf("
SELECT p.nomePerito AS perito,
       SUM(%s) AS nc,
       COUNT(*) AS n
FROM %s a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN '%s' AND '%s'
GROUP BY p.nomePerito
", nc_expr, dbQuoteIdentifier(con, a_tbl), opt$start, opt$end)
df_nc <- am_dbGetQuery(con, sql_nc) %>% mutate(nc_rate = ifelse(n>0, nc/n, NA_real_))

# ────────────────────────────────────────────────────────────────────────────────
# Base de durações válidas (0 < dur ≤ 3600) — reutilizada em ≤threshold e produtividade
# ────────────────────────────────────────────────────────────────────────────────
sql_valid <- sprintf("
SELECT p.nomePerito AS perito,
       a.dataHoraIniPericia AS ini,
       a.dataHoraFimPericia AS fim,
       ((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400.0) AS dur_s
FROM %s a
JOIN peritos p ON a.siapePerito = p.siapePerito
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN '%s' AND '%s'
  AND a.dataHoraIniPericia IS NOT NULL
  AND a.dataHoraFimPericia IS NOT NULL
  AND ((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400.0) > 0
  AND ((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400.0) <= 3600
", dbQuoteIdentifier(con, a_tbl), opt$start, opt$end)
df_valid <- am_dbGetQuery(con, sql_valid)

# ≤ threshold (entre válidas)
df_le <- df_valid %>%
  group_by(perito) %>%
  summarise(
    le = sum(dur_s <= opt$`le-threshold`, na.rm=TRUE),
    n  = n(),
    .groups = "drop"
  ) %>%
  mutate(le_rate = ifelse(n>0, le/n, NA_real_))

# Produtividade (entre válidas)
df_pd <- df_valid %>%
  group_by(perito) %>%
  summarise(
    total = n(),
    segs  = sum(dur_s, na.rm=TRUE),
    .groups = "drop"
  ) %>%
  mutate(prod = ifelse(segs>0, total/(segs/3600), NA_real_))

# ────────────────────────────────────────────────────────────────────────────────
# Overlap (flag por perito, entre válidas)
# ────────────────────────────────────────────────────────────────────────────────
df_ov_raw <- df_valid %>% select(perito, ini, fim) %>%
  mutate(ini = as.POSIXct(ini, tz="UTC"),
         fim = as.POSIXct(fim, tz="UTC")) %>%
  filter(!is.na(ini), !is.na(fim), fim >= ini) %>%
  arrange(perito, ini)

overlap_flag <- function(ini, fim) {
  if(length(ini) < 2) return(FALSE)
  any(ini[-1] < fim[-length(fim)])
}
df_ov <- df_ov_raw %>%
  group_by(perito) %>%
  summarise(overlap = overlap_flag(ini, fim), .groups="drop")

# ────────────────────────────────────────────────────────────────────────────────
# Junta tudo
# ────────────────────────────────────────────────────────────────────────────────
df <- df_nc %>%
  select(perito, nc_rate) %>%
  left_join(df_le %>% select(perito, le_rate), by="perito") %>%
  left_join(df_pd %>% select(perito, prod), by="perito") %>%
  left_join(df_ov, by="perito") %>%
  mutate(overlap = ifelse(is.na(overlap), FALSE, overlap))

stopifnot(opt$perito %in% df$perito)

# ────────────────────────────────────────────────────────────────────────────────
# Normalizações (quanto mais alto, "pior")
# ────────────────────────────────────────────────────────────────────────────────
max_prod <- suppressWarnings(max(df$prod, na.rm=TRUE)); if (!is.finite(max_prod)) max_prod <- 0
df <- df %>%
  mutate(prod_inv = max_prod - prod) %>%
  mutate(
    nc_rate_norm  = normalize01(nc_rate),
    le_rate_norm  = normalize01(le_rate),
    prod_inv_norm = normalize01(prod_inv),
    overlap_norm  = ifelse(overlap, 1, 0)
  )

# Score simples (média das normalizadas)
df$score <- rowMeans(df[,c("nc_rate_norm","le_rate_norm","prod_inv_norm","overlap_norm")], na.rm=TRUE)

p_row <- df %>% filter(perito==opt$perito) %>% slice(1)
mean_score <- mean(df$score, na.rm=TRUE)

plot_df <- tibble::tibble(
  Indicador = c("NC rate (robusto)", sprintf("≤%ds", opt$`le-threshold`), "Prod (invertida)", "Overlap"),
  Valor     = c(p_row$nc_rate_norm, p_row$le_rate_norm, p_row$prod_inv_norm, p_row$overlap_norm)
)

gg <- ggplot(plot_df, aes(Indicador, Valor)) +
  geom_col(fill="#d62728", width=.6) +
  geom_hline(yintercept = mean_score, linetype="dashed", color="#1f77b4") +
  coord_cartesian(ylim=c(0,1.05)) +
  labs(title="Robustez do Composto — posição do perito (normalizado 0–1)",
       subtitle=sprintf("%s a %s | score do perito = %.2f (média ref. tracejada) | prod≥%.0f/h, ≤%ds",
                        opt$start, opt$end, p_row$score, opt$`prod-threshold`, opt$`le-threshold`),
       y="Escala normalizada (0–1)", x=NULL,
       caption="NC robusto: conformado=0 OU (motivoNaoConformado≠'' e CAST(...)≠0). Durações válidas: 0<dur≤3600s.") +
  theme_minimal(base_size=11) +
  theme(panel.grid.major.x = element_blank())

png_path <- file.path(export_dir, sprintf("rcheck_composite_%s.png", perito_safe))
ggsave(png_path, gg, width=8.5, height=5, dpi=160)
cat(sprintf("✓ salvo: %s\n", png_path))

# ────────────────────────────────────────────────────────────────────────────────
# Comentários em .org (método + interpretação)
# ────────────────────────────────────────────────────────────────────────────────
nc_txt  <- percent_s(p_row$nc_rate, acc = .1)
le_txt  <- percent_s(p_row$le_rate, acc = .1)
prod_tx <- ifelse(is.finite(p_row$prod), sprintf("%.2f/h", p_row$prod), "NA")
ov_txt  <- ifelse(isTRUE(p_row$overlap), "Sim", "Não")
pos_txt <- ifelse(is.finite(p_row$score) & is.finite(mean_score) & p_row$score > mean_score,
                  "acima da média nacional (pior)", "abaixo/na média nacional (melhor)")

metodo_txt <- paste0(
  "*Método.* Combinamos quatro indicadores calculados no período (", opt$start, "–", opt$end, "): ",
  "(i) taxa de NC *robusto*; (ii) proporção de perícias ≤ ", opt$`le-threshold`, "s entre durações válidas; ",
  "(iii) produtividade (análises/h), invertida para que valores maiores signifiquem pior desempenho; ",
  "(iv) ocorrência de *sobreposição* (flag 0/1). ",
  "Cada indicador é normalizado por min–max para a escala 0–1 e o *score* é a média simples das normalizadas. ",
  "As métricas dependentes de tempo usam apenas tarefas com duração válida (0<dur≤3600s). ",
  "A linha tracejada do gráfico representa a *média nacional* do score."
)

interpreta_txt <- paste0(
  "*Interpretação.* Barras mais próximas de 1 indicam pior posição relativa do perito frente aos pares; ",
  "próximas de 0, melhor. Para o período: ",
  "NC=", nc_txt, "; ≤", opt$`le-threshold`, "s=", le_txt, "; Prod=", prod_tx, "; Overlap=", ov_txt, ". ",
  "Score do perito = ", sprintf("%.2f", p_row$score), " (", pos_txt, "). ",
  "Use estes sinais para priorizar auditorias: métricas com barras altas são prováveis *drivers* do composto."
)

# arquivo .org principal (imagem + comentário)
org_main <- file.path(export_dir, sprintf("rcheck_composite_%s.org", perito_safe))
org_main_txt <- paste(
  "#+CAPTION: Composto (normalizado 0–1) — NC, ≤limiar, Prod (invertida) e Overlap",
  sprintf("[[file:%s]]", basename(png_path)),  # make_report reescreve para ../imgs/
  "",
  metodo_txt, "",
  interpreta_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
cat(sprintf("✓ salvo: %s\n", org_main))

# arquivo .org somente comentário (para injeção no PDF)
org_comment <- file.path(export_dir, sprintf("rcheck_composite_%s_comment.org", perito_safe))
org_comment_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comment_txt, org_comment)
cat(sprintf("✓ salvo: %s\n", org_comment))
