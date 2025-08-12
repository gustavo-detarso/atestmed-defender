#!/usr/bin/env Rscript

# _ensure_deps.R — instala e carrega todos os pacotes que podemos precisar
# Rode manualmente com: Rscript r_checks/_ensure_deps.R

options(warn = 1)

# garante CRAN
if (is.null(getOption("repos")) || is.na(getOption("repos")["CRAN"]) || getOption("repos")["CRAN"] == "") {
  options(repos = c(CRAN = Sys.getenv("CRAN_MIRROR", "https://cloud.r-project.org")))
}

pkgs <- c(
  # tidyverse básico
  "dplyr","tidyr","tibble","readr","purrr","stringr","forcats","ggplot2",
  # datas e utilidades
  "lubridate","janitor","scales","cli","glue",
  # gráficos/estética extra
  "ggtext","cowplot","patchwork","ggpubr","ggrepel",
  # CLI e banco
  "optparse","DBI","RSQLite",
  # estatística amigável (se usar)
  "broom","rstatix",
  # render robusto de PNGs (opcional, mas ajuda)
  "ragg","systemfonts"
)

to_install <- setdiff(pkgs, rownames(installed.packages()))
if (length(to_install)) {
  install.packages(to_install, repos = "https://cloud.r-project.org")
}
message("✓ Pacotes OK")

install_if_missing <- function(x) {
  miss <- x[!vapply(x, requireNamespace, logical(1), quietly = TRUE)]
  if (length(miss)) {
    message("[deps] Instalando: ", paste(miss, collapse = ", "))
    install.packages(miss, dependencies = TRUE)
  } else {
    message("[deps] Nenhum pacote pendente.")
  }
  invisible(lapply(x, function(p) {
    suppressPackageStartupMessages(require(p, character.only = TRUE))
    NULL
  }))
}

install_if_missing(pkgs)
message("[deps] OK")

