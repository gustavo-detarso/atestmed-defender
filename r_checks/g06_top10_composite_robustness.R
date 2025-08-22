#!/usr/bin/env Rscript

# -*- coding: utf-8 -*-
# Top 10 — Robustez do Composto (z-score médio)
# Saídas:
#   - rcheck_top10_composite_robustness.png
#   - rcheck_top10_composite_robustness.org
#   - rcheck_top10_composite_robustness_comment.org

suppressPackageStartupMessages({
  library(DBI); library(RSQLite); library(ggplot2); library(dplyr)
  library(lubridate); library(scales); library(stringr); library(purrr)
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















`%||%` <- function(a,b) if (is.null(a)) b else a

parse_args <- function() {
  args <- am_normalize_cli(commandArgs(trailingOnly=TRUE))
  kv <- list(); i <- 1
  while (i <= length(args)) {
    k <- args[[i]]
    if (startsWith(k, "--")) {
      v <- if (i + 1 <= length(args) && !startsWith(args[[i+1]], "--")) args[[i+1]] else TRUE
      kv[[substr(k, 3, nchar(k))]] <- v
      i <- i + if (isTRUE(v) || identical(v, TRUE)) 1 else 2
    } else i <- i + 1
  }
  kv
}

ensure_dir <- function(p) if (!dir.exists(p)) dir.create(p, recursive = TRUE, showWarnings = FALSE)
fail_plot <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()
safe_slug <- function(x) { x <- gsub("[^A-Za-z0-9\\-_]+","_", x); x <- gsub("_+","_", x); x <- gsub("^_|_$","", x); ifelse(nchar(x)>0, x, "output") }

# ───────────────────────── Args/paths ─────────────────────────
args <- parse_args()
db_path <- args$db; start_d <- args$start; end_d <- args$end
min_n   <- as.integer(args[["min-analises"]] %||% "50")
out_dir <- args[["out-dir"]]

if (is.null(db_path) || is.null(start_d) || is.null(end_d)) {
  stop("Uso: --db <path> --start YYYY-MM-DD --end YYYY-MM-DD [--min-analises 50] [--out-dir <dir>]")
}

base_dir   <- normalizePath(file.path(dirname(db_path), ".."))
export_dir <- if (!is.null(out_dir)) normalizePath(out_dir, mustWork = FALSE) else
  file.path(base_dir, "graphs_and_tables", "exports")
ensure_dir(export_dir)
png_file <- file.path(export_dir, "rcheck_top10_composite_robustness.png")
org_main <- file.path(export_dir, "rcheck_top10_composite_robustness.org")
org_comm <- file.path(export_dir, "rcheck_top10_composite_robustness_comment.org")

# ───────────────────────── Conexão/schema ─────────────────────
con <- dbConnect(RSQLite::SQLite(), db_path)
# (patched) # (patched) on.exit(dbDisconnect(con), add = TRUE)

table_exists <- function(con, name) {
  nrow(am_dbGetQuery(con,
                  "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
                  params = list(name))) > 0
}
detect_analises_table <- function(con) {
  for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t)
  stop("Não encontrei 'analises' nem 'analises_atestmed'.")
}

a_tbl <- detect_analises_table(con)
if (!table_exists(con, "indicadores")) {
  ggsave(png_file, fail_plot("Tabela 'indicadores' não encontrada"), width=10, height=6, dpi=150); quit(save="no")
}

# ─────────────────── detectar colunas úteis ───────────────────
cols <- am_dbGetQuery(con, sprintf("PRAGMA table_info(%s)", a_tbl))$name
has_end <- "dataHoraFimPericia" %in% cols
cand_dur_num <- intersect(cols, c("tempoAnaliseSeg","tempoAnalise","duracaoSegundos","duracao_seg","tempo_seg"))
dur_num_col  <- if (length(cand_dur_num)) cand_dur_num[[1]] else NA_character_
cand_dur_txt <- intersect(cols, c("duracaoPericia","duracao_txt","tempoFmt","tempo_formatado"))
dur_txt_col  <- if (length(cand_dur_txt)) cand_dur_txt[[1]] else NA_character_

# ─────────────── Top10 por scoreFinal (desempate por volume) ───────────────
qry_top10 <- sprintf("
SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
  FROM indicadores i
  JOIN peritos   p ON i.perito = p.siapePerito
  JOIN %s  a ON a.siapePerito = i.perito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
 GROUP BY p.nomePerito, i.scoreFinal
HAVING total_analises >= ?
 ORDER BY i.scoreFinal DESC, total_analises DESC
 LIMIT 10
", a_tbl)
top10 <- am_dbGetQuery(con, qry_top10, params = list(start_d, end_d, min_n))
if (nrow(top10) == 0) {
  ggsave(png_file, fail_plot("Sem Top 10 para o período/critério"), width=10, height=6, dpi=150); quit(save="no")
}
peritos <- paste(sprintf("'%s'", gsub("'", "''", top10$nomePerito)), collapse=",")

# ───────────────────────── base crua ─────────────────────────
sel_cols <- c("p.nomePerito AS nomePerito", "a.dataHoraIniPericia AS ini")
if (has_end) sel_cols <- c(sel_cols, "a.dataHoraFimPericia AS fim")
if (!is.na(dur_num_col)) sel_cols <- c(sel_cols, sprintf("a.%s AS dur_num", dur_num_col))
if (!is.na(dur_txt_col)) sel_cols <- c(sel_cols, sprintf("a.%s AS dur_txt", dur_txt_col))
sel_cols <- unique(sel_cols)

qry_base <- sprintf("
SELECT %s,
       a.conformado AS conformado,
       a.motivoNaoConformado AS motivoNaoConformado
  FROM %s a
  JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
   AND p.nomePerito IN (%s)
", paste(sel_cols, collapse=", "), a_tbl, peritos)

base <- am_dbGetQuery(con, qry_base, params = list(start_d, end_d))
if (nrow(base) == 0) {
  ggsave(png_file, fail_plot("Sem dados no período para os Top 10"), width=10, height=6, dpi=150); quit(save="no")
}

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
if (nrow(base) == 0) {
  ggsave(png_file, fail_plot("Sem análises válidas (duração) no período"), width=10, height=6, dpi=150); quit(save="no")
}

# ───────────────────────── NC robusto ─────────────────────────
nc_flag <- function(conformado, motivo) {
  c0 <- suppressWarnings(as.integer(ifelse(is.na(conformado), 1L, conformado))) == 0L
  motivo_txt <- ifelse(is.na(motivo), "", trimws(as.character(motivo)))
  motivo_int <- suppressWarnings(as.integer(ifelse(motivo_txt == "", "0", motivo_txt)))
  m_ok <- (motivo_txt != "") & !is.na(motivo_int) & (motivo_int != 0L)
  c0 | m_ok
}
base <- base %>% mutate(nc = nc_flag(conformado, motivoNaoConformado))

nc <- base %>%
  group_by(nomePerito) %>%
  summarise(total = n(), nc = sum(nc, na.rm=TRUE), .groups="drop") %>%
  mutate(pct_nc = ifelse(total>0, 100*nc/total, NA_real_)) %>%
  select(nomePerito, pct_nc)

# ≤15s
le15 <- base %>%
  group_by(nomePerito) %>%
  summarise(total = n(),
            n_le15 = sum(dur_s <= 15, na.rm=TRUE),
            pct_le15 = ifelse(total>0, 100*n_le15/total, NA_real_),
            .groups="drop") %>%
  select(nomePerito, pct_le15)

# produtividade (análises/h)
prod <- base %>%
  group_by(nomePerito) %>%
  summarise(total = n(),
            sum_s = sum(dur_s, na.rm=TRUE),
            prod_h = ifelse(sum_s>0, total/(sum_s/3600), NA_real_),
            .groups="drop") %>%
  select(nomePerito, prod_h)

# overlap %
ov_base <- base %>% select(nomePerito, ini_dt, fim_dt) %>% filter(is.finite(ini_dt), is.finite(fim_dt), fim_dt > ini_dt)
overlap_share <- function(tb) {
  tb <- tb %>% arrange(ini_dt, fim_dt)
  n <- nrow(tb); if (n <= 1) return(NA_real_)
  overl <- rep(FALSE, n)
  last_end <- tb$fim_dt[1]
  for (i in 2:n) {
    overl[i] <- tb$ini_dt[i] < last_end
    last_end <- max(last_end, tb$fim_dt[i], na.rm=TRUE)
  }
  mean(overl, na.rm=TRUE) * 100
}
ovm <- if (nrow(ov_base)>0) ov_base %>% group_by(nomePerito) %>%
  summarise(pct_overlap = overlap_share(pick(dplyr::everything())), .groups="drop") else
  data.frame(nomePerito = unique(base$nomePerito), pct_overlap = NA_real_)

# composição (z-score; produtividade invertida)
z <- function(x) if (all(is.na(x))) rep(NA_real_, length(x)) else as.numeric(scale(x))

full <- top10 %>%
  select(nomePerito) %>%
  left_join(nc,    by="nomePerito") %>%
  left_join(le15,  by="nomePerito") %>%
  left_join(ovm,   by="nomePerito") %>%
  left_join(prod,  by="nomePerito") %>%
  mutate(
    z_nc   = z(pct_nc),
    z_le15 = z(pct_le15),
    z_ov   = z(pct_overlap),
    z_prod = z(-prod_h)
  )

# média de z-scores; se todos NA na linha, devolve NA (evita NaN)
full$composite <- apply(full[,c("z_nc","z_le15","z_ov","z_prod")], 1, function(r){
  r <- as.numeric(r)
  if (all(is.na(r))) NA_real_ else mean(r, na.rm = TRUE)
})

full <- full %>%
  arrange(desc(composite)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito))

if (nrow(full) == 0 || all(is.na(full$composite))) {
  ggsave(png_file, fail_plot("Sem dados suficientes para compor o índice"), width=10, height=6, dpi=150); quit(save="no")
}

# rótulos seguros (evita warnings com NA em sprintf)
fmt2 <- function(x) ifelse(is.finite(x), sprintf("%.2f", x), "NA")
full <- full %>%
  mutate(lbl = paste0("z_nc=", fmt2(z_nc),
                      ", z_≤15s=", fmt2(z_le15),
                      ", z_ov=", fmt2(z_ov),
                      ", z_prod=", fmt2(z_prod)))

# ───────────────────────── Plot ─────────────────────────
p <- ggplot(full, aes(x=nomePerito, y=composite)) +
  geom_col() +
  geom_text(aes(label=lbl), vjust=-0.3, size=3) +
  labs(
    title    = "Top 10 — Robustez do Composto (z-score médio)",
    subtitle = sprintf("%s a %s | maior = pior (padronizado)", start_d, end_d),
    x = "Perito", y = "z-score médio",
    caption = "Composto = média(z_nc, z_≤15s, z_ov, z_prod_inv). Produtividade invertida (maior = pior). Duração válida: 0<dur≤3600s."
  ) +
  theme_minimal(base_size = 11) +
  theme(axis.text.x = element_text(angle=45, hjust=1))

ymin <- suppressWarnings(min(full$composite, na.rm = TRUE)); if (!is.finite(ymin)) ymin <- 0
ymax <- suppressWarnings(max(full$composite, na.rm = TRUE)); if (!is.finite(ymax)) ymax <- 0
padl <- if (ymin < 0) abs(ymin)*0.15 else 0.1
padu <- if (ymax > 0) ymax*0.15 else 0.1

ggsave(png_file, p + coord_cartesian(ylim = c(ymin - padl, ymax + padu)), width=10, height=6, dpi=150)
cat(sprintf("✅ Figura salva: %s\n", png_file))

# ───────────────────────── Comentários (.org) ─────────────────
top_worst <- full %>% dplyr::slice_head(n = 3)
top_best  <- full %>% arrange(composite) %>% dplyr::slice_head(n = 3)

metodo_txt <- paste0(
  "*Método.* Selecionamos os *10 piores* por *ScoreFinal* (mínimo de análises = ", min_n, "). ",
  "No período (", start_d, " a ", end_d, "), calculamos por perito: ",
  "%NC *robusto*, % de análises ≤ 15s, % de *sobreposição* e *produtividade* (análises/h). ",
  "Cada métrica foi padronizada via *z-score*; produtividade foi *invertida* (maior = pior). ",
  "O composto é a *média* dos z-scores (maior = pior). Durações foram calculadas de forma robusta ",
  "(fim−início; ou colunas numéricas/textuais HH:MM:SS; limite 0–3600s)."
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
  "- Leia os rótulos (z_nc, z_≤15s, z_ov, z_prod) para pistas de quais dimensões puxam o composto."
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
cat(sprintf("✅ Org salvo: %s\n", org_main))

# .org apenas comentário
org_comm_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comm_txt, org_comm)
cat(sprintf("✅ Org(comment) salvo: %s\n", org_comm))
