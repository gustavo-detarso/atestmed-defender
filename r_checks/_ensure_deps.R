# auto-gerado pelo make_report.py ─ não editar manualmente
options(warn = 1)
repos <- getOption("repos"); repos["CRAN"] <- "https://cloud.r-project.org"; options(repos = repos)

# Biblioteca do usuário primeiro, se existir
user_lib <- Sys.getenv("R_LIBS_USER")
if (nzchar(user_lib)) {
  dir.create(user_lib, showWarnings = FALSE, recursive = TRUE)
  .libPaths(unique(c(user_lib, .libPaths())))
}

message("[deps] .libPaths(): ", paste(.libPaths(), collapse = " | "))

# Conjunto de pacotes usados pelos r_checks/
need <- c(
  # tidyverse essenciais
  "dplyr","tidyr","readr","stringr","purrr","forcats","lubridate",
  # gráficos
  "ggplot2","scales","broom",
  # banco
  "DBI","RSQLite",
  # texto/gráficos avançados
  "ggtext","gridtext","ragg","textshaping",
  # utilitários que frequentemente são dependências
  "cli","glue","curl","httr"
)

have <- rownames(installed.packages())
to_install <- setdiff(need, have)

ncpus <- 1L
try({ ncpus <- max(1L, parallel::detectCores(logical = TRUE) - 1L) }, silent = TRUE)

if (length(to_install)) {
  message("[deps] Instalando: ", paste(to_install, collapse = ", "))
  tryCatch({
    install.packages(to_install, dependencies = TRUE, Ncpus = ncpus,
                     lib = if (nzchar(user_lib)) user_lib else .libPaths()[1])
  }, error = function(e) {
    message("[deps][ERRO] Falha ao instalar: ", conditionMessage(e))
    quit(status = 1L)
  })
} else {
  message("[deps] Todos os pacotes já presentes.")
}

# Smoke tests mínimos (carregamento)
ok <- TRUE
for (pkg in c("dplyr","ggplot2","DBI","RSQLite")) {
  ok <- ok && requireNamespace(pkg, quietly = TRUE)
}

# Teste de dispositivos/labels avançados
ok <- ok && requireNamespace("ggtext", quietly = TRUE)
ok <- ok && requireNamespace("ragg", quietly = TRUE)

if (!ok) {
  message("[deps][AVISO] Alguns pacotes não puderam ser carregados. Verifique dependências de sistema (ex.: libcurl, harfbuzz, fribidi, freetype).")
} else {
  message("[deps] OK")
}

# Sessão só para log
message(capture.output(sessionInfo()), sep = "\n")
