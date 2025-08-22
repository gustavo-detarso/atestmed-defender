#!/usr/bin/env bash
# Debian 12/13 – setup completo p/ ATESTMED
# - SO deps (build, fonts, pandoc, xelatex, sqlite, curl…)
# - R + pacotes CRAN no diretório do usuário (opcional)
# - Python via pyenv conforme Pipfile.lock (ou override por flag)
# - pipx + pipenv e "pipenv sync --dev" (se houver Pipfile.lock)
# - Cria estrutura de pastas padrão

set -euo pipefail

# ─────────────────────────── Flags ───────────────────────────
USE_SUDO=true
INSTALL_R=true
USE_PYENV=true
OVERRIDE_PYVER=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-sudo)   USE_SUDO=false; shift;;
    --no-r)      INSTALL_R=false; shift;;
    --no-pyenv)  USE_PYENV=false; shift;;
    --pyver)     OVERRIDE_PYVER="${2:-}"; shift 2;;
    *) echo "Flag desconhecida: $1"; exit 2;;
  esac
done

maybe_sudo() { $USE_SUDO && command -v sudo >/dev/null && sudo "$@" || "$@"; }

export DEBIAN_FRONTEND=noninteractive

# ────────────────────── SO packages (Debian) ─────────────────
echo "[1/7] Pacotes de sistema…"
maybe_sudo apt-get update -y
maybe_sudo apt-get install -y \
  build-essential git curl wget ca-certificates locales \
  pkg-config software-properties-common unzip zip \
  sqlite3 libsqlite3-dev \
  libffi-dev zlib1g-dev libssl-dev libxml2-dev \
  libfreetype6-dev libpng-dev libjpeg-dev libtiff5-dev \
  libharfbuzz-dev libfribidi-dev libxt6 libxt-dev \
  fonts-dejavu fonts-liberation \
  pandoc \
  texlive-xetex texlive-latex-recommended texlive-latex-extra lmodern \
  graphviz

# (opcional para plantuml/diagramas)
maybe_sudo apt-get install -y default-jre || true

# ─────────────────────── Locale pt_BR ────────────────────────
echo "[2/7] Locale (pt_BR.UTF-8)…"
if $USE_SUDO; then
  maybe_sudo sed -i 's/^# *pt_BR.UTF-8/pt_BR.UTF-8/' /etc/locale.gen || true
  maybe_sudo sed -i 's/^# *en_US.UTF-8/en_US.UTF-8/' /etc/locale.gen || true
  maybe_sudo locale-gen
fi
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

# ─────────────────────── pipx + pipenv ───────────────────────
echo "[3/7] pipx + pipenv…"
maybe_sudo apt-get install -y pipx || true
python3 -m pipx ensurepath || true
export PATH="$HOME/.local/bin:$PATH"
pipx install --include-deps pipenv || true

# ─────────────── Detecta versão de Python alvo ───────────────
echo "[4/7] Detectando versão de Python alvo…"
REPO_DIR="$(pwd)"
LOCK_PATH="$REPO_DIR/Pipfile.lock"

if [[ -n "$OVERRIDE_PYVER" ]]; then
  WANT_PY="$OVERRIDE_PYVER"
elif [[ -f "$LOCK_PATH" ]]; then
  # Lê python_version do Pipfile.lock
  WANT_PY="$(python3 - <<'PY'
import json,sys
try:
    with open("Pipfile.lock","r") as f:
        j=json.load(f)
    print(j.get("_meta",{}).get("requires",{}).get("python_version","3.12"))
except Exception:
    print("3.12")
PY
)"
else
  # fallback quando não há lock
  WANT_PY="3.12"
fi
echo "[INFO] Python alvo: $WANT_PY"

# ───────────────────── Instala/seleciona Python ──────────────
PY_BIN=""
if command -v "python${WANT_PY}" >/dev/null 2>&1; then
  PY_BIN="$(command -v python${WANT_PY})"
elif $USE_PYENV; then
  echo "[5/7] Instalando Python $WANT_PY via pyenv (user)…"
  if [[ ! -d "$HOME/.pyenv" ]]; then
    curl -fsSL https://pyenv.run | bash
  fi
  export PYENV_ROOT="$HOME/.pyenv"
  export PATH="$PYENV_ROOT/bin:$PATH"
  eval "$(pyenv init -)"
  # Dependências comuns do pyenv já cobertas nos apt acima
  pyenv install -s "$WANT_PY"
  pyenv shell "$WANT_PY"
  PY_BIN="$(pyenv which python)"
else
  echo "[ERRO] Python $WANT_PY não encontrado e --no-pyenv está ativo."
  exit 1
fi
echo "[INFO] Python selecionado: $("$PY_BIN" -V)"

# ─────────────────────── Ambiente pipenv ─────────────────────
echo "[6/7] Ambiente Python (pipenv)…"
if [[ -f "$LOCK_PATH" ]]; then
  PIPENV_PYTHON="$PY_BIN" pipenv --python "$PY_BIN" sync --dev
else
  echo "[AVISO] Pipfile.lock não encontrado; executando 'pipenv install --dev'."
  PIPENV_PYTHON="$PY_BIN" pipenv --python "$PY_BIN" install --dev
fi

# ──────────────────────── R e pacotes ────────────────────────
if $INSTALL_R; then
  echo "[7/7] R + pacotes (user library)…"
  maybe_sudo apt-get install -y r-base r-base-dev
  R_VER="$(Rscript -e 'cat(paste(R.version$major, R.version$minor, sep="."))')"
  export R_LIBS_USER="$HOME/R/x86_64-pc-linux-gnu-library/${R_VER}"
  mkdir -p "$R_LIBS_USER"

  RPROFILE="$HOME/.Rprofile"
  if ! grep -q "R_LIBS_USER" "$RPROFILE" 2>/dev/null; then
    cat >> "$RPROFILE" <<'EOF'
## Prioriza biblioteca do usuário e fixa CRAN
if (nzchar(Sys.getenv("R_LIBS_USER"))) {
  .libPaths(unique(c(Sys.getenv("R_LIBS_USER"), .libPaths())))
}
options(repos = c(CRAN = "https://cloud.r-project.org"))
EOF
  fi

  Rscript - <<'RSCRIPT'
repos <- getOption("repos"); repos["CRAN"] <- "https://cloud.r-project.org"; options(repos = repos)
lib <- Sys.getenv("R_LIBS_USER"); dir.create(lib, showWarnings = FALSE, recursive = TRUE)
pkgs <- c(
  "optparse","ggplot2","dplyr","tidyr","readr","DBI","RSQLite",
  "forcats","purrr","stringr","cowplot","scales","patchwork",
  "ggpubr","Hmisc","broom","janitor","viridis","ggtext","rlang",
  "DescTools","car","coin","Exact","vcd","metafor"
)
need <- pkgs[!vapply(pkgs, requireNamespace, FUN.VALUE = logical(1), quietly = TRUE)]
if (length(need)) install.packages(need, lib = lib)
RSCRIPT
else
  echo "[7/7] (pulado) Instalação de R desativada por --no-r."
fi

# ─────────────────── Estrutura de diretórios ─────────────────
mkdir -p \
  "$REPO_DIR/graphs_and_tables/exports" \
  "$REPO_DIR/reports/outputs" \
  "$REPO_DIR/misc" \
  "$REPO_DIR/r_checks"

echo
echo "✅ Setup concluído!"
echo "➡️  Ative e rode, por exemplo:"
echo "    pipenv run python reports/make_report.py \\"
echo "      --start 2025-07-01 --end 2025-07-31 --top10 --export-org --export-pdf --add-comments"
echo
echo "Observações:"
echo "- OPENAI_API_KEY: se necessário, crie um .env na raiz."
if $INSTALL_R; then echo "- Pacotes R instalados em: \$R_LIBS_USER"; fi

