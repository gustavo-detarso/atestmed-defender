#!/usr/bin/env Rscript

library(DBI); library(RSQLite); library(ggplot2); library(dplyr); library(lubridate); library(scales)

`%||%` <- function(a,b) if (is.null(a)) b else a

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  kv <- list(); i <- 1
  while (i <= length(args)) {
    k <- args[[i]]
    if (startsWith(k, "--")) {
      v <- if (i + 1 <= length(args) && !startsWith(args[[i+1]], "--")) args[[i+1]] else TRUE
      kv[[substr(k, 3, nchar(k))]] <- v; i <- i + if (isTRUE(v) || identical(v, TRUE)) 1 else 2
    } else i <- i + 1
  }
  kv
}

ensure_dir <- function(p) if (!dir.exists(p)) dir.create(p, recursive = TRUE, showWarnings = FALSE)
fail_plot <- function(msg) ggplot() + annotate("text", x=0, y=0, label=msg, size=5) + theme_void()

args <- parse_args()
db_path <- args$db; start_d <- args$start; end_d <- args$end
min_n   <- as.integer(args[["min-analises"]] %||% "50")
out_dir <- args[["out-dir"]]

if (is.null(db_path) || is.null(start_d) || is.null(end_d)) {
  stop("Uso: --db <path> --start YYYY-MM-DD --end YYYY-MM-DD [--min-analises 50] [--out-dir <dir>]")
}

base_dir <- normalizePath(file.path(dirname(db_path), ".."))
export_dir <- if (!is.null(out_dir)) normalizePath(out_dir, mustWork = FALSE) else
  file.path(base_dir, "graphs_and_tables", "exports")
ensure_dir(export_dir)
outfile <- file.path(export_dir, "rcheck_top10_composite_robustness.png")

con <- dbConnect(RSQLite::SQLite(), db_path); on.exit(dbDisconnect(con), add = TRUE)

qry_top10 <- sprintf("
SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
  FROM indicadores i
  JOIN peritos   p ON i.perito = p.siapePerito
  JOIN analises  a ON a.siapePerito = i.perito
 WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
 GROUP BY p.nomePerito, i.scoreFinal
HAVING total_analises >= %d
 ORDER BY i.scoreFinal DESC
 LIMIT 10
", start_d, end_d, min_n)
top10 <- dbGetQuery(con, qry_top10)
if (nrow(top10) == 0) {
  ggsave(outfile, fail_plot("Sem Top 10 para o período/critério"), width=9, height=5, dpi=150); quit(save="no")
}
peritos <- paste(sprintf("'%s'", gsub("'", "''", top10$nomePerito)), collapse=",")

# --- Métricas: %NC, %<=15s, %overlap, produtividade ---
# %NC
qry_nc <- sprintf("
SELECT p.nomePerito, COUNT(*) total,
       SUM(CASE WHEN a.motivoNaoConformado != 0 THEN 1 ELSE 0 END) nc
  FROM analises a JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
   AND p.nomePerito IN (%s)
 GROUP BY p.nomePerito
", start_d, end_d, peritos)
nc <- dbGetQuery(con, qry_nc) %>% mutate(pct_nc = ifelse(total>0, 100*nc/total, 0)) %>% select(nomePerito, pct_nc)

# <=15s (tentativa robusta)
cols <- dbGetQuery(con, "PRAGMA table_info(analises)")$name
cand_dur <- intersect(cols, c("tempoAnaliseSeg","tempoAnalise","duracaoSegundos","duracao_seg","tempo_seg"))
dur_col <- if (length(cand_dur)>0) cand_dur[[1]] else NA_character_
le15 <- if (!is.na(dur_col)) {
  qry <- sprintf("
  SELECT p.nomePerito, a.%s AS dur
    FROM analises a JOIN peritos p ON a.siapePerito = p.siapePerito
   WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
     AND p.nomePerito IN (%s)
  ", dur_col, start_d, end_d, peritos)
  tmp <- dbGetQuery(con, qry)
  if (nrow(tmp)>0) tmp %>% group_by(nomePerito) %>%
    summarise(total=n(), n_le15 = sum(as.numeric(dur) <= 15, na.rm=TRUE),
              pct_le15 = ifelse(total>0, 100*n_le15/total, 0)) %>% select(nomePerito, pct_le15)
  else data.frame(nomePerito=top10$nomePerito, pct_le15 = NA_real_)
} else data.frame(nomePerito=top10$nomePerito, pct_le15 = NA_real_)

# Produtividade
qry_ts <- sprintf("
SELECT p.nomePerito, a.dataHoraIniPericia AS ts
  FROM analises a JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
   AND p.nomePerito IN (%s)
", start_d, end_d, peritos)
ts <- dbGetQuery(con, qry_ts)
ts$ts <- lubridate::ymd_hms(ts$ts, quiet=TRUE)
prod <- if (nrow(ts)>0) ts %>% group_by(nomePerito) %>%
  summarise(total=n(),
            span_h = as.numeric(difftime(max(ts, na.rm=TRUE), min(ts, na.rm=TRUE), units="hours")),
            prod_h = ifelse(is.finite(span_h) & span_h>0, total/span_h, NA_real_)) %>%
  select(nomePerito, prod_h) else data.frame(nomePerito=top10$nomePerito, prod_h=NA_real_)

# Overlap (precisa ini e fim/duração)
has_end <- "dataHoraFimPericia" %in% cols
sel_cols <- c("p.nomePerito AS nomePerito", "a.dataHoraIniPericia AS ini")
if (has_end) sel_cols <- c(sel_cols, "a.dataHoraFimPericia AS fim")
if (!has_end && !is.na(dur_col)) sel_cols <- c(sel_cols, sprintf("a.%s AS dur", dur_col))
qry_ov <- sprintf("
SELECT %s
  FROM analises a JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE date(a.dataHoraIniPericia) BETWEEN '%s' AND '%s'
   AND p.nomePerito IN (%s)
", paste(sel_cols, collapse=", "), start_d, end_d, peritos)
ov <- dbGetQuery(con, qry_ov)
ov$ini <- lubridate::ymd_hms(ov$ini, quiet=TRUE)
if (has_end) ov$fim <- lubridate::ymd_hms(ov$fim, quiet=TRUE)
if (!has_end && "dur" %in% names(ov)) ov$fim <- ov$ini + lubridate::dseconds(as.numeric(ov$dur))
overlap_share <- function(tb) {
  tb <- tb %>% arrange(ini, fim)
  if (nrow(tb) <= 1) return(NA_real_)
  overl <- logical(nrow(tb)); last_end <- tb$fim[1]
  for (i in 2:nrow(tb)) { overl[i] <- tb$ini[i] < last_end; last_end <- max(last_end, tb$fim[i], na.rm=TRUE) }
  mean(overl, na.rm=TRUE) * 100
}
ovm <- if (nrow(ov)>0 && "fim" %in% names(ov)) ov %>% group_by(nomePerito) %>%
  summarise(pct_overlap = overlap_share(pick(everything()))) else data.frame(nomePerito=top10$nomePerito, pct_overlap=NA_real_)

# Junta e padroniza (z-score) — sinaliza “pior” como maior valor
full <- top10 %>% select(nomePerito) %>%
  left_join(nc, by="nomePerito") %>%
  left_join(le15, by="nomePerito") %>%
  left_join(ovm, by="nomePerito") %>%
  left_join(prod, by="nomePerito")

z <- function(x) if (all(is.na(x))) rep(NA_real_, length(x)) else as.numeric(scale(x))
full <- full %>%
  mutate(z_nc = z(pct_nc),
         z_le15 = z(pct_le15),
         z_ov = z(pct_overlap),
         z_prod = z(prod_h)) %>%
  mutate(composite = rowMeans(cbind(z_nc, z_le15, z_ov, z_prod), na.rm=TRUE)) %>%
  arrange(desc(composite)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito))

p <- ggplot(full, aes(x=nomePerito, y=composite)) +
  geom_col() +
  geom_text(aes(label=sprintf("z_nc=%.2f, z_≤15s=%.2f, z_ov=%.2f, z_prod=%.2f",
                              z_nc, z_le15, z_ov, z_prod)),
            vjust=-0.3, size=3) +
  labs(title="Top 10 — Robustez do Composto (z-score médio)",
       subtitle=sprintf("%s a %s | maior = pior (padronizado)", start_d, end_d),
       x="Perito", y="z-score médio") +
  theme_minimal() + theme(axis.text.x = element_text(angle=45, hjust=1))

ggsave(outfile, p, width=10, height=6, dpi=150)

