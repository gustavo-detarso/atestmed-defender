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
  if (!exists("am_resolve_export_dir", mode = "function", inherits = TRUE)) {
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
  library(DBI); library(RSQLite)
  library(dplyr); library(ggplot2); library(scales); library(stringr); library(tibble); library(readr)
})

# ── Localizar e carregar _common.R de forma robusta (se existir) ───────────────
local({
  args_all   <- commandArgs(trailingOnly = FALSE)
  file_arg   <- sub("^--file=", "", args_all[grep("^--file=", args_all)])
  script_dir <- if (length(file_arg)) dirname(normalizePath(file_arg)) else getwd()
  common_candidates <- c(
    file.path(script_dir, "_common.R"),
    file.path(script_dir, "r_checks", "_common.R"),
    file.path(getwd(), "_common.R"),
    file.path(getwd(), "r_checks", "_common.R")
  )
  common_path <- common_candidates[file.exists(common_candidates)][1]
  if (!is.na(common_path)) {
    # Carrega no ambiente local (não polui global), mas define funções com <<- se necessário
    source(common_path, local = TRUE)
  }
})

# --- begin: am_db_reconnect_helpers (fallbacks, caso _common.R não tenha sido carregado) ---
`%||%` <- get("%||%", envir = .GlobalEnv, inherits = TRUE)
if (!is.function(`%||%`)) `%||%` <- function(a,b) if (is.null(a)) b else a

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
      Sys.getenv("KPI_DB", "")
    }, error=function(e) "")
  }
}
# >>> ADIÇÃO: conector único
if (!exists("am_db_connect", mode="function", inherits=TRUE)) {
  am_db_connect <- function(db_arg = NULL) {
    dbp <- db_arg %||% am__db_path_guess()
    if (!nzchar(dbp)) stop("Caminho do banco não informado. Use --db ou KPI_DB.", call. = FALSE)
    dbp <- tryCatch(normalizePath(dbp, mustWork = TRUE), error = function(e) dbp)
    DBI::dbConnect(RSQLite::SQLite(), dbname = dbp)
  }
}
if (!exists("am_ensure_con", mode="function", inherits=TRUE)) {
  am_ensure_con <- function(con, db_arg = NULL) {
    if (inherits(con, "DBIConnection") && DBI::dbIsValid(con)) return(con)
    am_db_connect(db_arg)
  }
}
# >>> ADIÇÃO: open_db fino
if (!exists("am_open_db", mode="function", inherits=TRUE)) {
  am_open_db <- function(db_path = NULL) {
    con <- am_db_connect(db_path)
    ok <- tryCatch(DBI::dbIsValid(con), error = function(e) FALSE)
    if (!isTRUE(ok)) stop("am_open_db: conexão inválida")
    tryCatch(DBI::dbGetQuery(con, "SELECT 1"),
             error = function(e) stop("am_open_db: SELECT 1 falhou: ", conditionMessage(e)))
    con
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
# --- end: am_db_reconnect_helpers ---

# ==== ATESTMED PROLOGO (INICIO) ====
local({
  # Prólogo específico para 08_weighted_props.R (sem abrir/fechar DB)

  # Wrapper robusto para consultas (aceita vetor de strings)
  if (!exists("am_dbGetQuery", mode="function", inherits=TRUE)) {
    am_dbGetQuery <<- (function(.f) {
      force(.f)
      function(con, statement, ...) {
        st <- statement
        if (length(st) != 1L) st <- paste(st, collapse=" ")
        .f(con, st, ...)
      }
    })(DBI::dbGetQuery)
  }

  # Fallbacks de detecção de tabela/colunas (não mexe em conexões)
  if (!exists("am_detect_analises_table", mode="function", inherits=TRUE)) {
    am_detect_analises_table <<- function(con) {
      has <- function(nm) {
        out <- tryCatch(am_dbGetQuery(con,
          "SELECT 1 FROM sqlite_master WHERE type in ('table','view') AND name=? LIMIT 1",
          params=list(nm)), error=function(e) NULL)
        !is.null(out) && is.data.frame(out) && nrow(out) > 0
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
})
# ==== ATESTMED PROLOGO (FIM) ====

# ---- CLI fallback (ATESTMED) ----
if (!exists("start_d", inherits=TRUE) ||
    !exists("end_d", inherits=TRUE) ||
    !exists("top10", inherits=TRUE) ||
    !exists("perito", inherits=TRUE) ||
    !exists("min_n", inherits=TRUE) ||
    !exists("threshold", inherits=TRUE) ||
    !exists("measure", inherits=TRUE) ||
    !exists("export_dir", inherits=TRUE) ||
    !exists("con", inherits=TRUE) ||
    !exists("a_tbl", inherits=TRUE) ||
    !exists("peritos_csv", inherits=TRUE)) {

  .args <- base::commandArgs(TRUE)
  .kv <- list(); i <- 1L; n <- length(.args)
  while (i <= n) {
    k <- .args[[i]]
    if (startsWith(k, "--")) {
      v <- if (i+1L <= n && !startsWith(.args[[i+1L]], "--")) .args[[i+1L]] else TRUE
      .kv[[sub("^--","",k)]] <- v
      i <- i + (if (identical(v, TRUE)) 1L else 2L)
    } else i <- i + 1L
  }

  `%||%` <- function(a,b) if (is.null(a)) b else a

  start_d     <- if (!exists("start_d", inherits=TRUE))     .kv[["start"]]   else start_d
  end_d       <- if (!exists("end_d", inherits=TRUE))       .kv[["end"]]     else end_d
  perito      <- if (!exists("perito", inherits=TRUE))      (.kv[["perito"]] %||% NULL) else perito
  top10       <- if (!exists("top10", inherits=TRUE))       isTRUE(.kv[["top10"]]) else top10
  min_n       <- if (!exists("min_n", inherits=TRUE))       suppressWarnings(as.integer(.kv[["min-analises"]] %||% .kv[["min_analises"]] %||% "50")) else min_n
  threshold   <- if (!exists("threshold", inherits=TRUE))   suppressWarnings(as.numeric(.kv[["threshold"]] %||% "15")) else threshold
  measure     <- if (!exists("measure", inherits=TRUE))     as.character((.kv[["measure"]] %||% "nc")) else measure
  if (is.character(measure)) measure <- measure[[1L]]
  peritos_csv <- if (!exists("peritos_csv", inherits=TRUE)) (.kv[["peritos-csv"]] %||% NULL) else peritos_csv

  if (!exists("export_dir", inherits=TRUE) || is.null(export_dir)) {
    db_path <- .kv[["db"]]
    if (is.null(db_path) || !nzchar(db_path)) stop("Faltou --db <path> (CLI fallback).")
    base_dir <- normalizePath(file.path(dirname(db_path), ".."), mustWork=FALSE)
    out_dir  <- .kv[["out-dir"]] %||% .kv[["out"]]
    export_dir <- if (!is.null(out_dir) && nzchar(out_dir)) normalizePath(out_dir, mustWork=FALSE) else file.path(base_dir, "graphs_and_tables","exports")
    if (!dir.exists(export_dir)) dir.create(export_dir, recursive=TRUE, showWarnings=FALSE)

    # >>> AJUSTE: abrir conexão via helper presente (do _common.R ou fallback)
    con <- am_open_db(db_path)
    on.exit(try(am_safe_disconnect(con), silent=TRUE), add=TRUE)

    if (is.null(con) || !DBI::dbIsValid(con)) stop("Conexão DB inválida (fallback).")

    if (!exists("am_detect_analises_table", mode="function", inherits=TRUE)) {
      am_detect_analises_table <<- function(con) {
        has <- function(nm) {
          out <- tryCatch(DBI::dbGetQuery((con <- am_ensure_con(con)),
            "SELECT 1 FROM sqlite_master WHERE type in ('table','view') AND name=? LIMIT 1",
            params=list(nm)), error=function(e) NULL)
          !is.null(out) && is.data.frame(out) && nrow(out) > 0
        }
        for (t in c("analises","analises_atestmed")) if (has(t)) return(t)
        stop("Não encontrei 'analises' nem 'analises_atestmed'.")
      }
    }
    if (!exists("am_detect_columns", mode="function", inherits=TRUE)) {
      am_detect_columns <<- function(con, tbl) {
        if (is.na(tbl) || !nzchar(tbl)) return(character(0))
        DBI::dbGetQuery((con <- am_ensure_con(con)), sprintf("PRAGMA table_info(%s)", tbl))$name
      }
    }

    a_tbl <- tryCatch(am_detect_analises_table(con), error=function(e) NA_character_)
    cols  <- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))
  }
}

# ---------------- Helpers locais ----------------
`%||%` <- function(a,b) if (is.null(a)) b else a
safe <- function(x) { gsub("(^_+|_+$)", "", gsub("[^A-Za-z0-9_-]+","_", x)) }
percent_s <- function(x, acc=.1) ifelse(is.finite(x), percent(x, accuracy = acc), "NA")

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
  p <- 2 * (1 - stats::pnorm(abs(z)))
  list(z=z, p=p)
}

fail_plot <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()

write_org_bundle <- function(png_base, caption, metodo_txt, interpreta_txt, org_main, org_comm, md_out=NULL) {
  main_txt <- paste(
    paste0("#+CAPTION: ", caption),
    sprintf("[[file:%s]]", basename(png_base)),
    "",
    metodo_txt, "",
    interpreta_txt, "",
    sep = "\n"
  )
  writeLines(main_txt, org_main, useBytes = TRUE)
  comm_txt <- paste(metodo_txt, "", interpreta_txt, "", sep = "\n")
  writeLines(comm_txt, org_comm, useBytes = TRUE)
  if (!is.null(md_out)) writeLines(comm_txt, md_out, useBytes = TRUE)
}

load_names_csv <- function(path) {
  if (is.null(path) || !nzchar(path) || !file.exists(path)) return(character(0))
  df <- tryCatch(readr::read_csv(path, show_col_types = FALSE), error=function(e) NULL)
  if (is.null(df) || !nrow(df)) return(character(0))
  key <- if ("nomePerito" %in% names(df)) "nomePerito" else names(df)[1]
  out <- unique(trimws(as.character(df[[key]]))); out[nzchar(out)]
}

get_top10_names_legacy <- function(con, start_date, end_date, min_n=50) {
  q <- sprintf("
    SELECT p.nomePerito AS perito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
      FROM indicadores i
      JOIN peritos   p ON i.perito = p.siapePerito
      JOIN %s        a ON a.siapePerito = i.perito
     WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN ? AND ?
     GROUP BY p.nomePerito, i.scoreFinal
    HAVING total_analises >= ?
     ORDER BY i.scoreFinal DESC, total_analises DESC
     LIMIT 10
  ", a_tbl)
  df <- am_dbGetQuery(con, q, params=list(start_date, end_date, as.integer(min_n)))
  if (nrow(df) == 0) character(0) else df$perito
}

detect_duration_expr <- function(con, a_tbl) {
  cols <- tryCatch(am_detect_columns(con, a_tbl), error=function(e) character(0))
  cand <- intersect(cols, c("tempoAnaliseSeg","tempoAnalise","duracaoSegundos","duracao_seg","tempo_seg"))
  if (length(cand)) return(sprintf("CAST(a.%s AS REAL)", cand[[1]]))
  if (all(c("dataHoraIniPericia","dataHoraFimPericia") %in% cols))
    return("((julianday(a.dataHoraFimPericia) - julianday(a.dataHoraIniPericia)) * 86400.0)")
  NULL
}

build_agg_query <- function(measure, threshold, start_date, end_date, con, a_tbl) {
  if (tolower(measure) == "nc") {
    title_txt <- "Meta-análise simples: Não Conformidade (NC robusto)"
    ylab_txt  <- "Proporção de NC"
    meas_tag  <- "nc"
    sql <- sprintf("
      SELECT p.nomePerito AS perito,
             SUM(
               CASE
                 WHEN CAST(IFNULL(a.conformado,1) AS INTEGER)=0 THEN 1
                 WHEN TRIM(IFNULL(a.motivoNaoConformado,'')) <> ''
                      AND CAST(IFNULL(a.motivoNaoConformado,'0') AS INTEGER) <> 0 THEN 1
                 ELSE 0
               END
             ) AS x,
             COUNT(*) AS n
        FROM %s a
        JOIN peritos  p ON a.siapePerito = p.siapePerito
       WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN '%s' AND '%s'
       GROUP BY p.nomePerito
    ", a_tbl, start_date, end_date)
  } else if (tolower(measure) == "le") {
    title_txt <- sprintf("Meta-análise simples: ≤ %ds", as.integer(threshold))
    ylab_txt  <- "Proporção (≤ threshold, entre válidas)"
    meas_tag  <- sprintf("le%ds", as.integer(threshold))
    dur_expr  <- detect_duration_expr(con, a_tbl)
    if (is.null(dur_expr)) stop("Sem coluna de duração nem (início/fim) para calcular 'LE'.")
    sql <- sprintf("
      SELECT p.nomePerito AS perito,
             SUM(CASE WHEN (%s) > 0 AND (%s) <= 3600 AND (%s) <= %d THEN 1 ELSE 0 END) AS x,
             SUM(CASE WHEN (%s) > 0 AND (%s) <= 3600 THEN 1 ELSE 0 END)                AS n
        FROM %s a
        JOIN peritos  p ON a.siapePerito = p.siapePerito
       WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN '%s' AND '%s'
       GROUP BY p.nomePerito
    ", dur_expr, dur_expr, dur_expr, as.integer(threshold),
       dur_expr, dur_expr,
       a_tbl, start_date, end_date)
  } else {
    stop("Valor inválido para --measure. Use 'nc' ou 'le'.")
  }
  list(sql=sql, title=title_txt, ylab=ylab_txt, tag=meas_tag)
}

plot_two_groups <- function(df_plot, title_txt, subtitle_txt, ylab_txt, outfile) {
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
    geom_errorbar(aes(ymin=lo, ymax=hi), width=.15, linewidth=.5, na.rm = TRUE) +
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

make_names_perito <- function(tag, perito_safe) {
  png  <- file.path(export_dir, sprintf("rcheck_weighted_props_%s_%s.png", tag, perito_safe))
  org  <- file.path(export_dir, sprintf("rcheck_weighted_props_%s_%s.org", tag, perito_safe))
  orgc <- file.path(export_dir, sprintf("rcheck_weighted_props_%s_%s_comment.org", tag, perito_safe))
  md   <- file.path(export_dir, sprintf("rcheck_weighted_props_%s_%s.md", tag, perito_safe))
  list(png=png, org=org, orgc=orgc, md=md)
}
make_names_top10 <- function(tag) {
  png  <- file.path(export_dir, sprintf("rcheck_top10_weighted_props_%s.png", tag))
  org  <- file.path(export_dir, sprintf("rcheck_top10_weighted_props_%s.org", tag))
  orgc <- file.path(export_dir, sprintf("rcheck_top10_weighted_props_%s_comment.org", tag))
  md   <- file.path(export_dir, sprintf("rcheck_top10_weighted_props_%s.md", tag))
  list(png=png, org=org, orgc=orgc, md=md)
}

# ---------------- Execução ----------------

# Checagens básicas
if (is.null(start_d) || is.null(end_d)) stop("Parâmetros obrigatórios: --start, --end")
if (!top10 && is.null(perito)) stop("Informe --perito ou --top10.")
if (top10 && !is.null(perito)) stop("Use OU --perito OU --top10, não ambos.")

# 1) agrega x/n por perito para a métrica desejada
spec <- build_agg_query(measure, threshold, start_d, end_d, con, a_tbl)
agg  <- am_dbGetQuery(con, spec$sql)

# Saída imediata se não houver dados
if (nrow(agg) == 0) {
  if (!top10) {
    per_safe <- safe(perito %||% "perito")
    nm <- make_names_perito(spec$tag, per_safe)
    ggsave(nm$png, fail_plot("Nenhuma linha encontrada no período informado."), width=8, height=5, dpi=160)
    metodo_txt <- paste0(
      "*Método.* Comparamos duas proporções (grupo do perito vs demais) para a métrica '", measure,
      "' no período ", start_d, "–", end_d,
      ", com IC de Wilson e teste z de duas proporções (pooled)."
    )
    interpreta_txt <- "Não há dados no período para estimar proporções."
    write_org_bundle(nm$png, spec$title, metodo_txt, interpreta_txt, nm$org, nm$orgc, nm$md)
  } else {
    nm <- make_names_top10(spec$tag)
    ggsave(nm$png, fail_plot("Top 10: sem dados no período."), width=8, height=5, dpi=160)
    metodo_txt <- paste0(
      "*Método.* Comparamos duas proporções (Top 10 vs Brasil (excl.)) ",
      "para a métrica '", measure, "' no período ", start_d, "–", end_d,
      ", com IC de Wilson e teste z de duas proporções (pooled)."
    )
    interpreta_txt <- "Não há dados no período para formar os grupos."
    write_org_bundle(nm$png, spec$title, metodo_txt, interpreta_txt, nm$org, nm$orgc, nm$md)
  }
  quit(save="no")
}

if (!top10) {
  # ---------------- modo perito ----------------
  if (!(perito %in% agg$perito)) {
    per_safe <- safe(perito %||% "perito")
    nm <- make_names_perito(spec$tag, per_safe)
    ggsave(nm$png, fail_plot("Perito informado não encontrado no período."), width=8, height=5, dpi=160)
    metodo_txt <- "*Método.* Duas proporções (perito vs demais) com IC de Wilson e teste z (pooled)."
    interpreta_txt <- "Perito não encontrado entre os registros do período."
    write_org_bundle(nm$png, spec$title, metodo_txt, interpreta_txt, nm$org, nm$orgc, nm$md)
    quit(save="no")
  }

  p_row <- agg %>% filter(perito == !!perito) %>% slice(1)
  o_row <- agg %>% filter(perito != !!perito) %>% summarise(x = sum(x), n = sum(n), .groups="drop")

  p_hat <- ifelse(p_row$n > 0, p_row$x / p_row$n, NA_real_)
  o_hat <- ifelse(o_row$n > 0, o_row$x / o_row$n, NA_real_)
  p_ci  <- wilson_ci(p_row$x, p_row$n)
  o_ci  <- wilson_ci(o_row$x, o_row$n)
  zt    <- z_test_2props(p_row$x, p_row$n, o_row$x, o_row$n)

  plot_df <- tibble(
    Grupo = factor(c(perito, "Demais (excl.)"), levels=c(perito, "Demais (excl.)")),
    prop  = c(p_hat, o_hat),
    lo    = c(p_ci[1], o_ci[1]),
    hi    = c(p_ci[2], o_ci[2]),
    n     = c(p_row$n, o_row$n),
    x     = c(p_row$x, o_row$x)
  )

  subtitle_txt <- sprintf("Período: %s a %s | n=%d vs %d | z=%s, p=%s",
                          start_d, end_d, p_row$n, o_row$n,
                          ifelse(is.na(zt$z), "NA", sprintf("%.2f", zt$z)),
                          ifelse(is.na(zt$p), "NA", ifelse(zt$p < 0.001, "<0.001", sprintf("%.3f", zt$p))))

  perito_safe <- safe(perito)
  nm <- make_names_perito(spec$tag, perito_safe)
  plot_two_groups(plot_df, spec$title, subtitle_txt, spec$ylab, nm$png)

  what_txt <- if (tolower(measure) == "nc") {
    "proporção de Não Conformidade (NC robusto)"
  } else {
    sprintf("proporção de perícias com duração ≤ %ds (entre válidas)", as.integer(threshold))
  }

  metodo_txt <- paste0(
    "*Método.* Comparamos a ", what_txt, " do perito (", perito, ") contra o grupo 'Demais (excl.)' ",
    "no período ", start_d, "–", end_d, ". Estimativas pontuais com IC 95% de Wilson; ",
    "teste z de duas proporções (pooled) no subtítulo."
  )

  interp_txt <- paste0(
    "*Interpretação.* Perito: ", percent_s(p_hat), " (", p_row$x, "/", p_row$n, "); ",
    "Demais (excl.): ", percent_s(o_hat), " (", o_row$x, "/", o_row$n, "). ",
    "Diferença ",
    if (is.finite(p_hat) && is.finite(o_hat)) sprintf("= %s", percent_s(p_hat - o_hat)) else "= NA",
    "; z=", ifelse(is.finite(zt$z), sprintf("%.2f", zt$z), "NA"),
    ", p=", ifelse(is.finite(zt$p), ifelse(zt$p < 0.001, "<0.001", sprintf("%.3f", zt$p)), "NA"), ". ",
    "Sinal e magnitude devem ser lidos conforme a natureza da métrica."
  )

  write_org_bundle(nm$png, spec$title, metodo_txt, interp_txt, nm$org, nm$orgc, nm$md)

} else {
  # ---------------- modo top10 (Fluxo/legado) ----------------
  nm <- make_names_top10(spec$tag)

  manifest_names <- load_names_csv(peritos_csv)
  if (length(manifest_names)) {
    top10_names <- unique(head(manifest_names, 10L))
    sel_caption <- "Seleção: lista externa (manifest) — alinhada ao Fluxo."
  } else {
    top10_names <- get_top10_names_legacy(con, start_d, end_d, min_n)
    sel_caption <- "Seleção: Top10 por scoreFinal (legado)."
  }

  if (length(top10_names) == 0) {
    ggsave(nm$png, fail_plot("Top 10: nenhum perito encontrado com os critérios no período."), width=8, height=5, dpi=160)
    metodo_txt <- paste0(
      "*Método.* Comparamos a métrica '", measure, "' entre o grupo Top 10 ",
      "e o Brasil (excl.), com IC de Wilson e teste z de duas proporções (pooled). ",
      sel_caption
    )
    interpreta_txt <- "Sem peritos elegíveis no período para compor o grupo-alvo."
    write_org_bundle(nm$png, spec$title, metodo_txt, interpreta_txt, nm$org, nm$orgc, nm$md)
    quit(save="no")
  }

  grp <- agg %>% filter(perito %in% top10_names) %>% summarise(x = sum(x), n = sum(n), .groups="drop")
  oth <- agg %>% filter(!(perito %in% top10_names)) %>% summarise(x = sum(x), n = sum(n), .groups="drop")

  g_hat <- ifelse(grp$n > 0, grp$x / grp$n, NA_real_)
  o_hat <- ifelse(oth$n > 0, oth$x / oth$n, NA_real_)
  g_ci  <- wilson_ci(grp$x, grp$n)
  o_ci  <- wilson_ci(oth$x, oth$n)
  zt    <- z_test_2props(grp$x, grp$n, oth$x, oth$n)

  plot_df <- tibble(
    Grupo = factor(c("Grupo-alvo", "Brasil (excl.)"), levels=c("Grupo-alvo", "Brasil (excl.)")),
    prop  = c(g_hat, o_hat),
    lo    = c(g_ci[1], o_ci[1]),
    hi    = c(g_ci[2], o_ci[2]),
    n     = c(grp$n, oth$n),
    x     = c(grp$x, oth$x)
  )

  subtitle_txt <- sprintf("Período: %s a %s | Alvo n=%d (%s) vs Brasil n=%d | z=%s, p=%s | %s",
                          start_d, end_d, grp$n,
                          paste(head(top10_names, 5), collapse = ", "),
                          oth$n,
                          ifelse(is.na(zt$z), "NA", sprintf("%.2f", zt$z)),
                          ifelse(is.na(zt$p), "NA", ifelse(zt$p < 0.001, "<0.001", sprintf("%.3f", zt$p))),
                          sel_caption)

  plot_two_groups(plot_df, spec$title, subtitle_txt, spec$ylab, nm$png)

  what_txt <- if (tolower(measure) == "nc") {
    "proporção de Não Conformidade (NC robusto)"
  } else {
    sprintf("proporção de perícias com duração ≤ %ds (entre válidas)", as.integer(threshold))
  }

  metodo_txt <- paste0(
    "*Método.* Comparamos a ", what_txt, " do *grupo-alvo* contra o Brasil (excl.) ",
    "no período ", start_d, "–", end_d,
    ". Estimativas com IC 95%% de Wilson; diferença testada via z de duas proporções (pooled). ",
    sel_caption
  )

  interp_txt <- paste0(
    "*Interpretação.* Grupo-alvo: ", percent_s(g_hat), " (", grp$x, "/", grp$n, "); ",
    "Brasil (excl.): ", percent_s(o_hat), " (", oth$x, "/", oth$n, "). ",
    "Diferença ",
    if (is.finite(g_hat) && is.finite(o_hat)) sprintf("= %s", percent_s(g_hat - o_hat)) else "= NA",
    "; z=", ifelse(is.finite(zt$z), sprintf("%.2f", zt$z), "NA"),
    ", p=", ifelse(is.finite(zt$p), ifelse(zt$p < 0.001, "<0.001", sprintf("%.3f", zt$p)), "NA"), ". ",
    "Sinal e magnitude devem ser lidos conforme a natureza da métrica."
  )

  write_org_bundle(nm$png, spec$title, metodo_txt, interp_txt, nm$org, nm$orgc, nm$md)
}

