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

message("[05_motivos_chisq.R] versão 2025-08-13-a (sem slice_max/min dinâmico)")

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

# ── Helpers ────────────────────────────────────────────────────────────────────
safe_slug <- function(x) {
  x <- gsub("[^A-Za-z0-9\\-_]+", "_", x); x <- gsub("_+", "_", x); x <- gsub("^_|_$", "", x)
  ifelse(nchar(x) > 0, x, "output")
}
percent_s <- function(x, acc = .1) ifelse(is.finite(x), percent(x, accuracy = acc), "NA")

table_exists <- function(con, name) {
  out <- dbGetQuery(con, "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1", params=list(name))
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
con <- dbConnect(RSQLite::SQLite(), opt$db); on.exit(try(dbDisconnect(con), silent = TRUE))
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

all_nc <- dbGetQuery(con, sql_nc, params = list(opt$start, opt$end)) %>%
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

