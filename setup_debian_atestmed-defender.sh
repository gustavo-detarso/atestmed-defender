#!/usr/bin/env bash
# Debian 12/13 – setup completo p/ ATESTMED (INTERATIVO)
# - SO deps (build, fonts, Chromium headless deps)
# - Python via pyenv conforme Pipfile.lock (ou override)
# - pipx + pipenv + pacotes do pipeline (playwright, jinja2, markdown, dotenv, tiktoken, openai…)
# - playwright install chromium (+ install-deps)
# - R opcional / LaTeX opcional
# - Estrutura de pastas (inclui docs/ e utils/)
# - Modo interativo por padrão; flags permitem automação

set -euo pipefail

# ============================ Helpers =================================
color() { local c="$1"; shift; printf "\033[%sm%s\033[0m" "$c" "$*"; }
info()  { echo "$(color '1;34' '[INFO]') $*"; }
ok()    { echo "$(color '1;32' '  OK ') $*"; }
warn()  { echo "$(color '1;33' '[AVISO]') $*"; }
err()   { echo "$(color '1;31' '[ERRO]') $*"; }
hr()    { printf '%*s\n' "${COLUMNS:-80}" '' | tr ' ' '─'; }

ask() {
  local prompt="$1"; shift || true
  local default="${1:-}"
  local val
  if [[ -n "$default" ]]; then
    read -r -p "$prompt [${default}]: " val || true
    echo "${val:-$default}"
  else
    read -r -p "$prompt: " val || true
    echo "$val"
  fi
}

confirm() {
  local prompt="$1"; shift || true
  local def="${1:-y}"
  local yn
  if [[ "${def,,}" == "y" ]]; then
    read -r -p "$prompt [Y/n]: " yn || true
    yn="${yn:-y}"
  else
    read -r -p "$prompt [y/N]: " yn || true
    yn="${yn:-n}"
  fi
  case "${yn,,}" in y|yes|s|sim) return 0 ;; *) return 1 ;; esac
}

choose() { # usage: choose "Pergunta" "opt1" "opt2" ...
  local prompt="$1"; shift
  local -a opts=("$@")
  echo "$prompt"
  local i=1
  for o in "${opts[@]}"; do echo "  $i) $o"; ((i++)); done
  local num; read -r -p "Opção [1-${#opts[@]}]: " num || true
  num="${num:-1}"
  echo "${opts[$((num-1))]}"
}

# ============================ Flags/Defaults ===========================
USE_SUDO=true
INSTALL_R=true
INSTALL_LATEX=false
USE_PYENV=true
OVERRIDE_PYVER=""
SKIP_PLAYWRIGHT_DEPS=false
ASSUME_YES=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-sudo)       USE_SUDO=false; shift ;;
    --no-r)          INSTALL_R=false; shift ;;
    --latex)         INSTALL_LATEX=true; shift ;;
    --no-pyenv)      USE_PYENV=false; shift ;;
    --pyver)         OVERRIDE_PYVER="${2:-}"; shift 2 ;;
    --skip-pw-deps)  SKIP_PLAYWRIGHT_DEPS=true; shift ;;
    --yes|-y)        ASSUME_YES=true; shift ;;
    *) err "Flag desconhecida: $1"; exit 2 ;;
  esac
done

maybe_sudo() { $USE_SUDO && command -v sudo >/dev/null && sudo "$@" || "$@"; }
export DEBIAN_FRONTEND=noninteractive

# ============================ Boas-vindas ==============================
hr
echo "Setup ATESTMED — Debian 12/13 (HTML+CSS → PDF com Playwright/Chromium)"
hr

if ! $ASSUME_YES; then
  # Perfil
  PERFIL="$(choose "Escolha um perfil:" \
    "Mínimo (sem R, sem LaTeX) – recomendado" \
    "Completo (com R e LaTeX)" \
    "Personalizado")"
  case "$PERFIL" in
    Mínimo*)   INSTALL_R=false; INSTALL_LATEX=false; USE_PYENV=true ;;
    Completo*) INSTALL_R=true;  INSTALL_LATEX=true;  USE_PYENV=true ;;
    Personalizado*) : ;;
  esac

  # Ajustes personalizados
  if [[ "$PERFIL" == "Personalizado" ]]; then
    $USE_SUDO || warn "Rodando sem sudo: algumas dependências de sistema podem falhar."
    $USE_PYENV && confirm "Usar pyenv para instalar Python (recomendado)?" "y" || USE_PYENV=false
    confirm "Instalar R + pacotes CRAN?" "n" && INSTALL_R=true || INSTALL_R=false
    confirm "Instalar LaTeX (XeLaTeX/pandoc)?" "n" && INSTALL_LATEX=true || INSTALL_LATEX=false
    confirm "Executar 'playwright install-deps' (requer sudo)?" "y" && SKIP_PLAYWRIGHT_DEPS=false || SKIP_PLAYWRIGHT_DEPS=true
  fi
else
  info "Modo não-interativo: usando flags passadas (ou defaults)."
fi

# Pergunta Python alvo se não houver override
REPO_DIR="$(pwd)"
LOCK_PATH="$REPO_DIR/Pipfile.lock"
if [[ -n "$OVERRIDE_PYVER" ]]; then
  WANT_PY="$OVERRIDE_PYVER"
else
  if [[ -f "$LOCK_PATH" ]]; then
    WANT_PY="$(python3 - <<'PY'
import json
try:
    with open("Pipfile.lock","r",encoding="utf-8") as f:
        j=json.load(f)
    print(j.get("_meta",{}).get("requires",{}).get("python_version","3.12"))
except Exception:
    print("3.12")
PY
)"
  else
    WANT_PY="3.12"
  fi
  $ASSUME_YES || WANT_PY="$(ask "Versão do Python alvo" "$WANT_PY")"
fi
info "Python alvo: $WANT_PY"

# ============================ Passo 1: APT ============================
info "[1/10] Pacotes de sistema…"
maybe_sudo apt-get update -y
maybe_sudo apt-get install -y \
  build-essential git curl wget ca-certificates locales \
  pkg-config software-properties-common unzip zip \
  sqlite3 libsqlite3-dev \
  libffi-dev zlib1g-dev libssl-dev libxml2-dev \
  libfreetype6-dev libpng-dev libjpeg-dev libtiff5-dev \
  libharfbuzz-dev libfribidi-dev libxt6 libxt-dev \
  fonts-dejavu fonts-liberation fonts-noto fonts-noto-mono \
  fonts-noto-color-emoji fonts-noto-cjk fonts-noto-extra \
  graphviz

# deps do Chromium headless (muitas já cobertas por playwright install-deps)
maybe_sudo apt-get install -y \
  libnss3 libnspr4 libx11-6 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 \
  libxext6 libxfixes3 libdrm2 libgbm1 libgtk-3-0 libatk1.0-0 libatk-bridge2.0-0 \
  libcairo2 libcups2 libasound2 libpangocairo-1.0-0 libpango-1.0-0 libatspi2.0-0 \
  libxkbcommon0 libglib2.0-0 libxcb1 libxrender1 libxi6 libxshmfence1 \
  libgdk-pixbuf-2.0-0 libpci3 || true

maybe_sudo apt-get install -y default-jre || true

if $INSTALL_LATEX; then
  info "[1b] Instalando LaTeX (XeLaTeX/pandoc)…"
  maybe_sudo apt-get install -y pandoc texlive-xetex texlive-latex-recommended texlive-latex-extra lmodern
else
  warn "LaTeX não será instalado (use --latex para habilitar)."
fi
ok "Pacotes base ok."

# ============================ Passo 2: Locale =========================
info "[2/10] Locale pt_BR.UTF-8…"
if $USE_SUDO; then
  maybe_sudo sed -i 's/^# *pt_BR.UTF-8/pt_BR.UTF-8/' /etc/locale.gen || true
  maybe_sudo sed -i 's/^# *en_US.UTF-8/en_US.UTF-8/' /etc/locale.gen || true
  maybe_sudo locale-gen
fi
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8
ok "Locale ok."

# ============================ Passo 3: pipx/pipenv ====================
info "[3/10] pipx + pipenv…"
maybe_sudo apt-get install -y pipx || true
python3 -m pipx ensurepath || true
export PATH="$HOME/.local/bin:$PATH"
pipx install --include-deps pipenv || true
ok "pipenv disponível."

# ============================ Passo 4: Python alvo ====================
info "[4/10] Preparando Python $WANT_PY…"
PY_BIN=""
if command -v "python${WANT_PY}" >/dev/null 2>&1; then
  PY_BIN="$(command -v python${WANT_PY})"
elif $USE_PYENV; then
  info "Instalando via pyenv (user)…"
  if [[ ! -d "$HOME/.pyenv" ]]; then
    curl -fsSL https://pyenv.run | bash
  fi
  export PYENV_ROOT="$HOME/.pyenv"
  export PATH="$PYENV_ROOT/bin:$PATH"
  eval "$(pyenv init -)"
  pyenv install -s "$WANT_PY"
  pyenv shell "$WANT_PY"
  PY_BIN="$(pyenv which python)"
else
  err "Python $WANT_PY não encontrado e --no-pyenv está ativo."
  exit 1
fi
ok "Python: $("$PY_BIN" -V)"

# ============================ Passo 5: pipenv venv ====================
info "[5/10] Ambiente pipenv…"
LOCK_PATH="$REPO_DIR/Pipfile.lock"
if [[ -f "$LOCK_PATH" ]]; then
  PIPENV_PYTHON="$PY_BIN" pipenv --python "$PY_BIN" sync --dev
else
  warn "Pipfile.lock não encontrado; executando 'pipenv install --dev'."
  PIPENV_PYTHON="$PY_BIN" pipenv --python "$PY_BIN" install --dev
fi
ok "Ambiente pipenv configurado."

# ============================ Passo 6: libs Python ====================
info "[6/10] Instalando bibliotecas do pipeline (no venv)…"
pipenv run python - <<'PY'
import sys, subprocess
pkgs = [
  "playwright>=1.44",
  "jinja2>=3.1",
  "markdown>=3.5",
  "python-dotenv>=1.0",
  "tiktoken>=0.7",
  "openai>=1.0",
  "pikepdf>=8.0",
  "pillow>=10.0"
]
subprocess.check_call([sys.executable,"-m","pip","install","-q","--upgrade"]+pkgs)
print("Pacotes instalados/atualizados.")
PY
ok "Bibliotecas instaladas."

# ============================ Passo 7: Playwright =====================
info "[7/10] Playwright (Chromium)…"
pipenv run playwright install chromium
if ! $SKIP_PLAYWRIGHT_DEPS; then
  if $USE_SUDO; then
    maybe_sudo env -C "$REPO_DIR" pipenv run playwright install-deps chromium || true
  else
    warn "--no-sudo: pulando 'playwright install-deps'."
  fi
else
  warn "Flag --skip-pw-deps: pulando 'playwright install-deps'."
fi
ok "Playwright ok."

# ============================ Passo 8: R opcional =====================
if $INSTALL_R; then
  info "[8/10] R + pacotes (user library)…"
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
  ok "R preparado em $R_LIBS_USER"
else
  warn "Instalação de R desativada (use --no-r para manter assim)."
fi

# ============================ Passo 9: Estrutura ======================
info "[9/10] Criando estrutura de diretórios…"
mkdir -p \
  "$REPO_DIR/graphs_and_tables/exports" \
  "$REPO_DIR/reports/outputs" \
  "$REPO_DIR/misc" \
  "$REPO_DIR/r_checks" \
  "$REPO_DIR/utils" \
  "$REPO_DIR/docs"
ok "Pastas criadas."

# ============================ Passo 10: Extras ========================
info "[10/10] Extras…"
# Oferece criar utils/html2pdf_chromium.py se não existir
if [[ ! -f "$REPO_DIR/utils/html2pdf_chromium.py" ]]; then
  if $ASSUME_YES || confirm "Criar utilitário utils/html2pdf_chromium.py (HTML→PDF)?" "y"; then
    cat > "$REPO_DIR/utils/html2pdf_chromium.py" <<'PY'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, pathlib
from playwright.sync_api import sync_playwright

def html_to_pdf(html_path: str):
    p = pathlib.Path(html_path).resolve()
    pdf = p.with_suffix(".pdf")
    with sync_playwright() as sp:
        browser = sp.chromium.launch()
        page = browser.new_page()
        page.goto(p.as_uri(), wait_until="networkidle")
        page.pdf(path=str(pdf), format="A4", print_background=True,
                 margin={"top":"16mm","bottom":"16mm","left":"14mm","right":"14mm"},
                 display_header_footer=True,
                 header_template="<div></div>",
                 footer_template=(
                     "<div style='width:100%;font-size:10px;color:#475569;"
                     "padding:6px 10px;text-align:right;'>"
                     "Página <span class='pageNumber'></span>/<span class='totalPages'></span></div>"
                 ))
        browser.close()
    print(f"PDF: {pdf}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Uso: html2pdf_chromium.py caminho/arquivo.html")
    html_to_pdf(sys.argv[1])
PY
    chmod +x "$REPO_DIR/utils/html2pdf_chromium.py"
    ok "Criado utils/html2pdf_chromium.py"
  fi
fi

# Oferece criar docs/relatorio.css base
if [[ ! -f "$REPO_DIR/docs/relatorio.css" ]]; then
  if $ASSUME_YES || confirm "Criar CSS base em docs/relatorio.css?" "y"; then
    cat > "$REPO_DIR/docs/relatorio.css" <<'CSS'
:root{ --fg:#0f172a; --muted:#475569; --border:#e5e7eb; --accent:#111827;
       --body-fs:12.5pt; --h1-fs:32px; --h2-fs:22px; --h3-fs:18px; }
@page{ size:A4; margin:24mm 22mm; }
html{ -webkit-text-size-adjust:100%; hyphens:auto; -webkit-hyphens:auto; -ms-hyphens:auto; }
html,body{ margin:0; padding:0; color:var(--fg);
  font-family:"Source Serif Pro","Noto Serif",Georgia,Cambria,"Times New Roman",Times,serif;
  line-height:1.6; -webkit-print-color-adjust:exact; print-color-adjust:exact; }
body{ font-size:var(--body-fs); counter-reset:fig tbl; }
p{ text-align:justify; text-justify:inter-word; margin:0 0 3.5mm; orphans:3; widows:3; }
h1{ font-size:var(--h1-fs); text-align:center; margin:8mm 0 4mm; }
h2{ font-size:var(--h2-fs); margin:12mm 0 4mm; position:relative; font-variant-caps:small-caps; letter-spacing:.04em; break-after:avoid; }
h2::after{ content:""; position:absolute; left:0; bottom:-6px; width:85px; height:2px; background:#d1d5db; }
h3{ font-size:var(--h3-fs); margin:8mm 0 3mm; break-after:avoid; }
table{ border-collapse:collapse; width:100%; margin:6mm 0; font-size:12pt; counter-increment:tbl; break-inside:avoid; }
th,td{ border:1px solid #e5e7eb; padding:6px 8px; text-align:left; vertical-align:top; }
th{ background:#f8fafc; }
caption{ caption-side:bottom; padding-top:2mm; font-size:11.5pt; color:#334155; text-align:center; }
caption::before{ content:"Tabela " counter(tbl) ": "; font-weight:600; color:#111827; }
figure, div.figure{ margin:6mm auto; counter-increment:fig; break-inside:avoid; text-align:center; }
figure img, div.figure > img, div.figure > a > img{ max-width:100%; height:auto; display:block; margin:0 auto; }
figure figcaption, div.figure > p.caption{ margin-top:2mm; font-size:11.5pt; color:#334155; }
figure figcaption::before, div.figure > p.caption::before{ content:"Figura " counter(fig) ": "; font-weight:600; color:#111827; }
blockquote{ margin:4mm 0; padding:3mm 5mm; border-left:3px solid #d1d5db; color:#334155; background:#f8fafc; break-inside:avoid; }
code{ font-family:ui-monospace,Menlo,Consolas,monospace; font-size:.95em; background:#f1f5f9; padding:0 .25em; border-radius:3px; border:1px solid #e2e8f0; }
pre{ background:#0b1220; color:#e5e7eb; padding:8px 10px; border-radius:6px; overflow:auto; border:1px solid #111827; break-inside:avoid; }
img, table, pre, blockquote, div.figure, figure{ page-break-inside:avoid; break-inside:avoid; }
.page-break{ break-before:page; page-break-before:always; }
CSS
    ok "Criado docs/relatorio.css"
  fi
fi

# Oferece criar .env com OPENAI_API_KEY
if [[ ! -f "$REPO_DIR/.env" ]]; then
  if $ASSUME_YES || confirm "Criar .env com OPENAI_API_KEY (vazio)?" "n"; then
    cat > "$REPO_DIR/.env" <<'ENV'
OPENAI_API_KEY=
ENV
    ok "Criado .env (preencha sua chave)."
  fi
fi

hr
ok "Setup concluído!"
echo
echo "Exemplo de uso (HTML→PDF com IA):"
echo "  pipenv run python gerar_documentation.py \\"
echo "    --logo-path misc/logolula.png \\"
echo "    --ent 'República Federativa do Brasil' \\"
echo "    --ent 'Ministério da Gestão e da Inovação em Serviços Públicos' \\"
echo "    --dept-text 'Coordenação-Geral de Assuntos Corporativos e Disseminação de Conhecimento' \\"
echo "    --person 'Gustavo Magalhães Mendes de Tarso' \\"
echo "    --ent-gap 0.5mm --ent-lh 1.12 --logo-h 160px --dept-fs 11pt --brand-gap 0mm --logo-mb=-6mm \\"
echo "    --toc --toc-pagebreak --theme clean"
echo
$INSTALL_LATEX || echo "LaTeX: não instalado (use --latex se realmente precisar)."
$INSTALL_R || echo "R: não instalado (passe sem --no-r para habilitar)."

