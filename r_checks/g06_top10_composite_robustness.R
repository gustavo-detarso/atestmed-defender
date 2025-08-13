#!/usr/bin/env Rscript

library(DBI); library(RSQLite); library(ggplot2); library(dplyr); library(lubridate); library(scales); library(stringr)

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

safe_slug <- function(x) {
  x <- gsub("[^A-Za-z0-9\\-_]+", "_", x)
  x <- gsub("_+", "_", x)
  x <- gsub("^_|_$", "", x)
  ifelse(nchar(x) > 0, x, "output")
}

# --- args & paths ---
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

# --- DB ---
con <- dbConnect(RSQLite::SQLite(), db_path); on.exit(dbDisconnect(con), add = TRUE)

table_exists <- function(con, name) {
  nrow(dbGetQuery(con,
                  "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1",
                  params = list(name))) > 0
}
detect_analises_table <- function(con) {
  for (t in c("analises","analises_atestmed")) if (table_exists(con, t)) return(t)
  stop("Não encontrei 'analises' nem 'analises_atestmed'.")
}
a_tbl <- detect_analises_table(con)

# --- Top10 por scoreFinal (desempate por volume no período) ---
qry_top10 <- sprintf("
SELECT p.nomePerito, i.scoreFinal, COUNT(a.protocolo) AS total_analises
  FROM indicadores i
  JOIN peritos   p ON i.perito = p.siapePerito
  JOIN %s  a ON a.siapePerito = i.perito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN '%s' AND '%s'
 GROUP BY p.nomePerito, i.scoreFinal
HAVING total_analises >= %d
 ORDER BY i.scoreFinal DESC, total_analises DESC
 LIMIT 10
", a_tbl, start_d, end_d, min_n)
top10 <- dbGetQuery(con, qry_top10)
if (nrow(top10) == 0) {
  ggsave(outfile, fail_plot("Sem Top 10 para o período/critério"), width=9, height=5, dpi=150); quit(save="no")
}
peritos <- paste(sprintf("'%s'", gsub("'", "''", top10$nomePerito)), collapse=",")

# --- Carregar base crua necessária (ini, fim, dur_txt) para calcular durações de forma robusta ---
qry_base <- sprintf("
SELECT p.nomePerito AS nomePerito,
       a.dataHoraIniPericia AS ini,
       a.dataHoraFimPericia AS fim,
       a.duracaoPericia     AS dur_txt,
       a.conformado         AS conformado,
       a.motivoNaoConformado AS motivoNaoConformado
  FROM %s a
  JOIN peritos p ON a.siapePerito = p.siapePerito
 WHERE substr(a.dataHoraIniPericia,1,10) BETWEEN '%s' AND '%s'
   AND p.nomePerito IN (%s)
", a_tbl, start_d, end_d, peritos)
base <- dbGetQuery(con, qry_base)

# --- Parse datas e duração (fim−início, fallback HH:MM:SS/MM:SS/numérico), filtra (0,3600] ---
parse_hms <- function(s) {
  s <- as.character(s %||% "")
  s <- trimws(s)
  if (s == "" || s %in% c("0","00:00","00:00:00")) return(NA_real_)
  if (grepl(":", s, fixed = TRUE)) {
    parts <- strsplit(s, ":", fixed = TRUE)[[1]]
    if (length(parts) == 3) {
      suppressWarnings({
        h <- as.numeric(parts[1]); m <- as.numeric(parts[2]); sec <- as.numeric(parts[3])
      })
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
    fim_dt = ymd_hms(fim, quiet = TRUE),
    dur_s  = as.numeric(difftime(fim_dt, ini_dt, units = "secs"))
  )

need_fb <- is.na(base$dur_s) | base$dur_s <= 0
if ("dur_txt" %in% names(base) && any(need_fb, na.rm=TRUE)) {
  fb <- vapply(base$dur_txt[need_fb], parse_hms, numeric(1))
  base$dur_s[need_fb] <- fb
}

base <- base %>%
  mutate(dur_s = as.numeric(dur_s)) %>%
  filter(!is.na(dur_s), dur_s > 0, dur_s <= 3600)

if (nrow(base) == 0) {
  ggsave(outfile, fail_plot("Sem análises válidas (duração) no período"), width=9, height=5, dpi=150); quit(save="no")
}

# --- %NC (NC robusto) ---
# NC = (conformado=0) OR (TRIM(motivoNaoConformado)<>'' AND CAST(motivoNaoConformado AS INTEGER)<>0)
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
  mutate(pct_nc = ifelse(total>0, 100*nc/total, 0)) %>%
  select(nomePerito, pct_nc)

# --- ≤15s usando dur_s calculado ---
le15 <- base %>%
  group_by(nomePerito) %>%
  summarise(total = n(),
            n_le15 = sum(dur_s <= 15, na.rm=TRUE),
            pct_le15 = ifelse(total>0, 100*n_le15/total, 0),
            .groups="drop") %>%
  select(nomePerito, pct_le15)

# --- Produtividade = total / (soma das durações / 3600) ---
prod <- base %>%
  group_by(nomePerito) %>%
  summarise(total = n(),
            sum_s = sum(dur_s, na.rm=TRUE),
            prod_h = ifelse(sum_s>0, total/(sum_s/3600), NA_real_),
            .groups="drop") %>%
  select(nomePerito, prod_h)

# --- Overlap: % de tarefas que participam de sobreposição (por perito) ---
# precisa de ini/fim; se fim faltou e veio via fallback, já filtramos linhas inválidas
ov_base <- base %>% select(nomePerito, ini_dt, fim_dt) %>% filter(!is.na(ini_dt), !is.na(fim_dt), fim_dt > ini_dt)
overlap_share <- function(tb) {
  tb <- tb %>% arrange(ini_dt, fim_dt)
  n <- nrow(tb); if (n <= 1) return(NA_real_)
  overl <- rep(FALSE, n)
  last_end <- tb$fim_dt[1]
  for (i in 2:n) {
    overl[i] <- tb$ini_dt[i] < last_end
    if (!is.na(tb$fim_dt[i])) last_end <- max(last_end, tb$fim_dt[i], na.rm=TRUE)
  }
  mean(overl, na.rm=TRUE) * 100
}
ovm <- if (nrow(ov_base)>0) ov_base %>% group_by(nomePerito) %>%
  summarise(pct_overlap = overlap_share(cur_data()), .groups="drop") else
  data.frame(nomePerito = top10$nomePerito, pct_overlap = NA_real_)

# --- Junta e padroniza (z-score). “Maior = pior”: produtividade invertida ---
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
    z_prod = z(-prod_h)   # invertido: mais produtivo → melhor (valor menor)
  ) %>%
  mutate(composite = rowMeans(cbind(z_nc, z_le15, z_ov, z_prod), na.rm=TRUE)) %>%
  arrange(desc(composite)) %>%
  mutate(nomePerito = factor(nomePerito, levels = nomePerito))

if (nrow(full) == 0) {
  ggsave(outfile, fail_plot("Sem dados suficientes para compor o índice"), width=10, height=6, dpi=150); quit(save="no")
}

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
cat(sprintf("✓ salvo: %s\n", outfile))

