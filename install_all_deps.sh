#!/usr/bin/env bash
# Debian 12 – setup completo p/ ATESTMED
# - SO deps (build, fonts, pandoc, xelatex, sqlite, curl…)
# - R + pacotes CRAN no diretório do usuário (sem sudo em /usr/local/lib/R)
# - Python 3.12 via pyenv (se não existir no sistema)
# - pipx + pipenv e "pipenv sync --dev" (respeitando seu Pipfile.lock)
# - Cria estrutura de pastas padrão

set -euo pipefail

# ---------- opções ----------
USE_SUDO=true
if [[ "${1:-}" == "--no-sudo" ]]; then USE_SUDO=false; fi
maybe_sudo() { $USE_SUDO && command -v sudo >/dev/null && sudo "$@" || "$@"; }

export DEBIAN_FRONTEND=noninteractive

REPO_DIR="$(pwd)"
echo "[INFO] Repositório: $REPO_DIR"

# ---------- apt ----------
echo "[1/7] Apt update & pacotes base…"
maybe_sudo apt-get update
maybe_sudo apt-get install -y --no-install-recommends \
  ca-certificates curl git wget gnupg locales \
  build-essential pkg-config \
  python3-venv python3-pip python3-dev \
  libssl-dev libffi-dev zlib1g-dev libbz2-dev liblzma-dev \
  libreadline-dev libsqlite3-dev tk-dev xz-utils \
  sqlite3 \
  pandoc \
  texlive-xetex texlive-latex-recommended texlive-latex-extra lmodern \
  fonts-dejavu \
  r-base r-base-dev \
  # --- headers/bibs p/ R pacotes (curl, textshaping, systemfonts, ragg, ggtext) ---
  libcurl4-openssl-dev \
  libharfbuzz-dev libfribidi-dev \
  libfreetype6-dev libfontconfig1-dev \
  libpng-dev libjpeg-dev libtiff5-dev \
  libglib2.0-dev

# ---------- locales (pt_BR.UTF-8) ----------
echo "[2/7] Configurando locale pt_BR.UTF-8…"
if ! locale -a | grep -q 'pt_BR\.utf8'; then
  maybe_sudo sed -i 's/^# *pt_BR.UTF-8/pt_BR.UTF-8/' /etc/locale.gen
  maybe_sudo sed -i 's/^# *en_US.UTF-8/en_US.UTF-8/' /etc/locale.gen || true
  maybe_sudo locale-gen
fi
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

# ---------- pipx & pipenv ----------
echo "[3/7] pipx + pipenv…"
maybe_sudo apt-get install -y pipx || true
python3 -m pipx ensurepath || true
export PATH="$HOME/.local/bin:$PATH"
pipx install --include-deps pipenv || true

# ---------- Python 3.12 ----------
# Seu Pipfile.lock exige python_version = 3.12
echo "[4/7] Verificando Python 3.12…"
want_py="3.12"
have_py=""
if command -v python3.12 >/dev/null 2>&1; then
  have_py="$(python3.12 -V | awk '{print $2}')"
  echo "[INFO] Python3.12 já disponível: $have_py"
else
  echo "[INFO] Instalando Python $want_py via pyenv (usuário)…"
  if [[ ! -d "$HOME/.pyenv" ]]; then
    curl -fsSL https://pyenv.run | bash
  fi
  export PYENV_ROOT="$HOME/.pyenv"
  export PATH="$PYENV_ROOT/bin:$PATH"
  eval "$(pyenv init -)"
  latest_312="$(pyenv install -l | awk '{print $1}' | grep -E '^3\.12\.' | tail -1)"
  pyenv install -s "${latest_312}"
  pyenv shell "${latest_312}"
  have_py="$(python -V | awk '{print $2}')"
  echo "[INFO] Python via pyenv: $have_py"
fi

# define o binário 3.12 a usar no pipenv
if command -v python3.12 >/dev/null 2>&1; then
  PY312_BIN="$(command -v python3.12)"
else
  PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
  export PATH="$PYENV_ROOT/bin:$PATH"
  eval "$(pyenv init -)"
  PY312_BIN="$(pyenv which python)"
fi
echo "[INFO] Usando intérprete: $PY312_BIN"

# ---------- pipenv sync ----------
echo "[5/7] Instalando deps Python conforme Pipfile.lock…"
cd "$REPO_DIR"
if [[ -f Pipfile.lock ]]; then
  PIPENV_PYTHON="$PY312_BIN" pipenv --python "$PY312_BIN"
  PIPENV_PYTHON="$PY312_BIN" pipenv sync --dev
else
  echo "[AVISO] Pipfile.lock não encontrado; farei 'pipenv install --dev'."
  PIPENV_PYTHON="$PY312_BIN" pipenv --python "$PY312_BIN" install --dev
fi

# ---------- R pacotes (em diretório do usuário) ----------
echo "[6/7] Instalando pacotes R no user library…"
R_VER="$(Rscript -e 'cat(paste(R.version$major, R.version$minor, sep="."))')"
export R_LIBS_USER="$HOME/R/x86_64-pc-linux-gnu-library/${R_VER}"
mkdir -p "$R_LIBS_USER"

# garante que a lib do user entra primeiro no .libPaths() e define CRAN
RPROFILE="$HOME/.Rprofile"
if ! grep -q "R_LIBS_USER" "$RPROFILE" 2>/dev/null; then
  cat >> "$RPROFILE" <<'EOF'
## Prioriza biblioteca de usuário e fixa CRAN
if (nzchar(Sys.getenv("R_LIBS_USER"))) {
  .libPaths(unique(c(Sys.getenv("R_LIBS_USER"), .libPaths())))
}
options(repos = c(CRAN = "https://cloud.r-project.org"))
EOF
fi

# instala pacotes (se já instalados, ignora)
Rscript - <<'RSCRIPT'
repos <- getOption("repos"); repos["CRAN"] <- "https://cloud.r-project.org"; options(repos = repos)
lib <- Sys.getenv("R_LIBS_USER"); dir.create(lib, showWarnings = FALSE, recursive = TRUE)

pkgs <- c(
  # base usados pelos .R do projeto
  "optparse","ggplot2","dplyr","tidyr","readr","DBI","RSQLite",
  "stringr","broom","scales","forcats","lubridate",
  # HTTP/arquivos
  "curl","httr",
  # para gráficos ricos / render
  "systemfonts","textshaping","gridtext","ragg","ggtext"
)

need <- pkgs[!vapply(pkgs, requireNamespace, FUN.VALUE = logical(1), quietly = TRUE)]
if (length(need)) install.packages(need, lib = lib)
message("Pacotes instalados/ok: ", paste(pkgs, collapse=", "))

# smoke-test básico (falha cedo se algo der errado)
ok <- TRUE
for (nm in c("ggplot2","dplyr","ragg","ggtext","curl","gridtext","textshaping","systemfonts","forcats","lubridate")) {
  if (!requireNamespace(nm, quietly = TRUE)) { message("FALHA carregando: ", nm); ok <- FALSE }
}
if (!ok) q(status = 1) else message("[R] Dependências OK")
RSCRIPT

# ---------- estrutura de pastas ----------
echo "[7/7] Estruturando diretórios esperados…"
mkdir -p \
  "$REPO_DIR/graphs_and_tables/exports" \
  "$REPO_DIR/reports/outputs" \
  "$REPO_DIR/misc" \
  "$REPO_DIR/r_checks"

echo
echo "✅ Setup concluído!"
echo "➡️  Ative o ambiente e rode, por exemplo:"
echo "    pipenv run python reports/make_report.py --start 2025-07-01 --end 2025-07-31 --top10 --export-org --export-pdf --add-comments"
echo
echo "Observações:"
echo "- Se precisar expor OPENAI_API_KEY, crie um .env na raiz com a variável."
echo "- R pacotes foram instalados em: $R_LIBS_USER"

