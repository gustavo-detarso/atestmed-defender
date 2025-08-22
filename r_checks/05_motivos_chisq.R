#!/usr/bin/env Rscript
# -*- coding: utf-8 -*-
# Apêndice estatístico (R) — motivos NC: Perito vs. Demais (excl.)
# Saídas:
#   - rcheck_motivos_chisq_<safe_perito>.png
#   - rcheck_motivos_chisq_<safe_perito>.org
#   - rcheck_motivos_chisq_<safe_perito>_comment.org

suppressPackageStartupMessages({
  library(optparse); library(DBI); library(RSQLite)
  library(dplyr); library(ggplot2); library(stringr); library(forcats); library(scales)
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

message("[05_motivos_chisq.R] versão 2025-08-13-b (fix con guard + linewidth)")

# ── CLI ────────────────────────────────────────────────────────────────────────
option_list <- list(
  make_option("--db",        type="character", help="Caminho do SQLite (.db)", metavar="FILE"),
  make_option("--start",     type="character", help="Data inicial YYYY-MM-DD"),
  make_option("--end",       type="character", help="Data final   YYYY-MM-DD"),
  make_option("--perito",    type="character", help="Nome do perito (obrigatório)"),
  make_option("--out-dir",   type="character", default=".", help="Diretório de saída [default: %default]"),
  make_option("--min-count", type="integer",  default=5L,  help="Agrupa motivos com contagem < min-count em 'OUTROS' [default: %default]"),
  make_option("--topn",      type="integer",  default=12L, help="Quantidade de motivos por |diferença| no gráfico [default: %default]")
)
opt <- parse_args(OptionParser(option_list = option_list))
stopifnot(!is.null(opt$db), !is.null(opt$start), !is.null(opt$end), !is.null(opt$perito))
if (!dir.exists(opt$`out-dir`)) dir.create(opt$`out-dir`, recursive = TRUE, showWarnings = FALSE)

# ── Garantia de conexão local válida ───────────────────────────────────────────
if (!(exists("con", inherits=TRUE) &&
      isTRUE(try(DBI::dbIsValid(con), silent=TRUE)))) {
  con <- DBI::dbConnect(RSQLite::SQLite(), opt$db)
  on.exit(try(DBI::dbDisconnect(con), silent=TRUE), add=TRUE)
}

# ── Helpers ────────────────────────────────────────────────────────────────────
safe_slug <- function(x) {
  x <- gsub("[^A-Za-z0-9\\-_]+", "_", x); x <- gsub("_+", "_", x); x <- gsub("^_|_$", "", x)
  ifelse(nchar(x) > 0, x, "output")
}
percent_s <- function(x, acc = .1) ifelse(is.finite(x), percent(x, accuracy = acc), "NA")

table_exists <- function(con, name) {
  out <- am_dbGetQuery(con, "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1", params=list(name))
  nrow(out) > 0
}
detect_analises_table <- function(con) {
  for (t in c("analises", "analises_atestmed")) if (table_exists(con, t)) return(t)
  stop("Não encontrei 'analises' nem 'analises_atestmed'.")
}
lump_rare <- function(tbl, min_count = 5L) {
  tbl %>% mutate(motivo = if_else(n < min_count, "OUTROS", motivo)) %>%
    group_by(motivo) %>% summarise(n = sum(n), .groups = "drop")
}

# ── Dados (NC robusto) ────────────────────────────────────────────────────────
a_tbl <- detect_analises_table(con); has_protocolos <- table_exists(con, "protocolos")

nc_expr <- "
CASE
  WHEN CAST(IFNULL(a.conformado,1) AS INTEGER)=0 THEN 1
  WHEN TRIM(IFNULL(a.motivoNaoConformado,'')) <> ''
       AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0 THEN 1
  ELSE 0
END
"
desc_expr <- if (has_protocolos) {
  "COALESCE(NULLIF(TRIM(pr.motivo), ''), CAST(IFNULL(a.motivoNaoConformado,'') AS TEXT)) AS motivo_text"
} else {
  "CAST(IFNULL(a.motivoNaoConformado,'') AS TEXT) AS motivo_text"
}
join_prot <- if (has_protocolos) "LEFT JOIN protocolos pr ON pr.protocolo = a.protocolo" else ""

sql_nc <- sprintf("
SELECT p.nomePerito AS perito, %s
FROM %s a
JOIN peritos p ON a.siapePerito = p.siapePerito
%s
WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
  AND (%s) = 1
;", desc_expr, dbQuoteIdentifier(con, a_tbl), join_prot, nc_expr)

all_nc <- am_dbGetQuery(con, sql_nc, params = list(opt$start, opt$end)) %>%
  mutate(motivo_text = as.character(motivo_text),
         motivo = if_else(is.na(motivo_text) | trimws(motivo_text)=="" | trimws(motivo_text)=="0",
                          "MOTIVO_DESCONHECIDO", trimws(motivo_text))) %>%
  select(perito, motivo)

if (nrow(all_nc) == 0) { message("Nenhuma análise NC (robusto) no período. Nada a fazer."); quit(save="no", status=0) }

perito_alvo <- opt$perito
if (!(perito_alvo %in% all_nc$perito)) {
  sim <- unique(all_nc$perito[grepl(perito_alvo, all_nc$perito, ignore.case = TRUE)])
  msg <- if (length(sim)) paste0(" Peritos semelhantes: ", paste(sim, collapse=", "), ".") else ""
  stop(sprintf("Perito '%s' sem NC no período (ou não encontrado).%s", perito_alvo, msg))
}

tab_perito <- all_nc %>% filter(perito == perito_alvo) %>% count(motivo, name="n_p") %>% arrange(desc(n_p))
tab_outros <- all_nc %>% filter(perito != perito_alvo) %>% count(motivo, name="n_o") %>% arrange(desc(n_o))
if (nrow(tab_perito) == 0) { message("Perito sem NC (robusto) no período. Nada a fazer."); quit(save="no", status=0) }

base_join <- full_join(tab_perito, tab_outros, by="motivo") %>%
  mutate(across(all_of(c("n_p","n_o")), ~ dplyr::coalesce(.x, 0L))) %>%
  arrange(desc(n_p + n_o))

base_join <- base_join %>%
  rename(n = n_p) %>% select(motivo, n, n_o) %>% lump_rare(min_count = opt$`min-count`) %>%
  rename(n_p = n) %>% left_join(base_join %>% select(motivo, n_o), by="motivo") %>%
  mutate(n_o = dplyr::coalesce(n_o, 0L)) %>% arrange(desc(n_p + n_o))

total_p <- sum(base_join$n_p); total_o <- sum(base_join$n_o)
if (total_p == 0 || total_o == 0) { message("Sem dados suficientes para qui-quadrado."); quit(save="no", status=0) }

mat <- rbind(Perito = base_join$n_p, Outros = base_join$n_o)
chs <- suppressWarnings(chisq.test(mat)); pval <- chs$p.value

resumo <- base_join %>%
  mutate(prop_p = n_p/total_p, prop_o = n_o/total_o, diff = prop_p - prop_o) %>%
  arrange(desc(abs(diff))) %>% slice_head(n = opt$topn) %>%
  mutate(motivo_plot = forcats::fct_reorder(motivo, diff))

# ── Plot ───────────────────────────────────────────────────────────────────────
titulo <- sprintf("Motivos NC (robusto) — %s vs. Demais (excl.)\n%s a %s  |  χ² p=%.3g",
                  perito_alvo, opt$start, opt$end, pval)

g <- ggplot(resumo, aes(x = motivo_plot, y = diff)) +
  geom_hline(yintercept = 0, linewidth = 0.4) +
  geom_col() + coord_flip() +
  labs(title = titulo, x = NULL, y = "Diferença de proporções (Perito − Demais)",
       caption = "NC robusto: conformado=0 OU (motivoNaoConformado ≠ '' E CAST(motivoNaoConformado) ≠ 0).") +
  theme_minimal(base_size = 11) +
  theme(panel.grid.minor = element_blank(), plot.title = element_text(face="bold", hjust=0))

perito_safe <- safe_slug(perito_alvo)
png_path <- file.path(opt$`out-dir`, sprintf("rcheck_motivos_chisq_%s.png", perito_safe))
ggsave(png_path, g, width=10, height=6, dpi=160)
message(sprintf("✅ Figura salva: %s", png_path))

# ── Comentários (.org) ─────────────────────────────────────────────────────────
pos_tbl <- resumo %>% filter(is.finite(diff), diff > 0) %>% arrange(desc(diff))
neg_tbl <- resumo %>% filter(is.finite(diff), diff < 0) %>% arrange(diff)

n_pos <- nrow(pos_tbl); n_neg <- nrow(neg_tbl)
top_pos <- pos_tbl %>% slice_head(n = min(3L, n_pos)) %>%
  transmute(txt = sprintf("%s (+%s p.p.)", motivo, percent_s(diff, acc=.1)))
top_neg <- neg_tbl %>% slice_head(n = min(3L, n_neg)) %>%
  transmute(txt = sprintf("%s (%s p.p.)", motivo, percent_s(diff, acc=.1)))

metodo_txt <- paste0(
  "*Método.* Construímos uma tabela de contingência motivo × grupo (Perito vs Demais), ",
  "após agrupar motivos raros (< ", opt$`min-count`, ") em 'OUTROS' para estabilidade. ",
  "Aplicamos o *teste qui-quadrado* global (χ²) ao total e, para cada motivo, ",
  "comparamos as *proporções* do perito (n=", total_p, ") e dos demais (n=", total_o, "). ",
  "No gráfico, exibimos os ", min(nrow(resumo), opt$topn),
  " motivos com maior |diferença| (Perito − Demais)."
)

interpreta_txt <- {
  sig <- if (is.finite(pval) && pval < 0.05) "diferenças *estatisticamente significativas*" else "diferenças não significativas ao nível 5%"
  pos_str <- if (nrow(top_pos)) paste("- Mais frequentes no perito:", paste(top_pos$txt, collapse=", "), ".") else NULL
  neg_str <- if (nrow(top_neg)) paste("- Menos frequentes no perito:", paste(top_neg$txt, collapse=", "), ".") else NULL
  paste0(
    "*Interpretação.* O teste global indica ", sig, " (p = ", formatC(pval, format="fg", digits=3), "). ",
    "Barras *positivas* significam motivos relativamente mais comuns no perito; *negativas*, menos comuns. ",
    "Use estes sinais como *pistas* para auditoria qualitativa, considerando volume e contexto.\n",
    paste(na.omit(c(pos_str, neg_str)), collapse = "\n")
  )
}

# 1) .org principal (imagem + texto)
org_main <- file.path(opt$`out-dir`, sprintf("rcheck_motivos_chisq_%s.org", perito_safe))
org_main_txt <- paste(
  "#+CAPTION: Motivos de NC (robusto) — Diferença de proporções (Perito − Demais)",
  sprintf("[[file:%s]]", basename(png_path)),
  "",
  metodo_txt, "",
  interpreta_txt, "",
  sep = "\n"
)
writeLines(org_main_txt, org_main)
message(sprintf("✅ Org salvo: %s", org_main))

# 2) .org apenas com o comentário
org_comment <- file.path(opt$`out-dir`, sprintf("rcheck_motivos_chisq_%s_comment.org", perito_safe))
org_comment_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
writeLines(org_comment_txt, org_comment)
message(sprintf("✅ Org(comment) salvo: %s", org_comment))

