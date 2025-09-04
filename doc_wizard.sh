#!/usr/bin/env bash
set -euo pipefail

# =========================================================
# Doc Wizard (Bash) — ATESTMED
# Gera documentação via gerar_documentation.py (HTML+CSS → PDF com IA)
# Perfis: Rápido, Completo, Personalizado
# Saídas do Python vão para a pasta docs/
# =========================================================

# ------------------------- Helpers -------------------------
ask() {
  local prompt="$1"; shift || true
  local default="${1:-}"
  local var
  if [[ -n "${default}" ]]; then
    read -r -p "${prompt} [${default}]: " var || true
    echo "${var:-$default}"
  else
    read -r -p "${prompt}: " var || true
    echo "${var}"
  fi
}

confirm() {
  local prompt="$1"; shift || true
  local default="${1:-y}"
  local yn
  if [[ "${default,,}" == "y" ]]; then
    read -r -p "${prompt} [Y/n]: " yn || true
    yn="${yn:-y}"
  else
    read -r -p "${prompt} [y/N]: " yn || true
    yn="${yn:-n}"
  fi
  case "${yn,,}" in
    y|yes|s|sim) return 0 ;;
    *) return 1 ;;
  esac
}

has_cmd() { command -v "$1" >/dev/null 2>&1; }
print_hr() { printf '%*s\n' "${COLUMNS:-80}" '' | tr ' ' '='; }

ask_multilines() {
  # lê múltiplas linhas até vazio; imprime cada linha em stdout
  local label="$1"
  local lines=()
  local i=1
  echo "$label (ENTER em branco para finalizar)"
  while true; do
    read -r -p "  Linha ${i}: " _ln || true
    [[ -z "${_ln}" ]] && break
    lines+=("$_ln")
    ((i++))
  done
  printf '%s\n' "${lines[@]:-}"
}

# ---------------------- Defaults/State ---------------------
# Modelos / IA
DOC_MODEL="${DOC_MODEL:-gpt-4o}"
DOC_SUMMARY_MODEL="${DOC_SUMMARY_MODEL:-gpt-4o-mini}"
DOC_POLISH_MODEL="${DOC_POLISH_MODEL:-gpt-4o-mini}"

DOC_FILES_PER_BATCH="${DOC_FILES_PER_BATCH:-8}"
DOC_FILE_EXCERPT_CHARS="${DOC_FILE_EXCERPT_CHARS:-2000}"
DOC_BATCH_OUT_TOKENS="${DOC_BATCH_OUT_TOKENS:-1200}"
DOC_SEC_OUT_TOKENS="${DOC_SEC_OUT_TOKENS:-900}"

DOC_STREAM="${DOC_STREAM:-0}"
DOC_NO_PDF="${DOC_NO_PDF:-0}"           # PDF habilitado por padrão (Playwright/Chromium)
DOC_POST_POLISH="${DOC_POST_POLISH:-0}"
DOC_POLISH_APPLY_FILES="${DOC_POLISH_APPLY_FILES:-0}"

DOC_POLISH_STYLE="${DOC_POLISH_STYLE:-tom sóbrio, técnico, coesão alta, períodos médios}"

DOC_CTX="${DOC_CTX:-128000}"
DOC_CTX_BUFFER="${DOC_CTX_BUFFER:-2048}"

SCRIPT_PATH="${SCRIPT_PATH:-gerar_documentation.py}"

# Capa / identidade (flags do Python)
LOGO_PATH="${LOGO_PATH:-misc/logolula.png}"
DEPT_TEXT="${DEPT_TEXT:-Coordenação-Geral de Assuntos Corporativos e Disseminação de Conhecimento}"
PERSON_NAME="${PERSON_NAME:-Gustavo Magalhães Mendes de Tarso}"

CSS_LOGO_H="${CSS_LOGO_H:-96px}"
CSS_DEPT_FS="${CSS_DEPT_FS:-12pt}"
CSS_BRAND_GAP="${CSS_BRAND_GAP:-0mm}"          # gap vertical entre blocos da marca
CSS_LOGO_MB="${CSS_LOGO_MB:-0mm}"              # pode ser negativo (use '=' ao passar)

# Entidades acima da coordenação (flags novas)
CSS_ENT_FS="${CSS_ENT_FS:-11pt}"
CSS_ENT_GAP="${CSS_ENT_GAP:-0.5mm}"
CSS_ENT_LH="${CSS_ENT_LH:-1.15}"
ENT_FILE="${ENT_FILE:-}"                       # arquivo txt opcional
# ENT_LINES pode vir por env (separadas por | ou ;). Também perguntamos interativamente.

# Sumário / TOC
TOC="${TOC:-1}"
TOC_TITLE="${TOC_TITLE:-Sumário}"
TOC_PAGEBREAK="${TOC_PAGEBREAK:-1}"

# Tema / CSS
DOC_THEME="${DOC_THEME:-clean}"
DOC_CSS_FILE="${DOC_CSS_FILE:-}"
DOC_CSS_URL="${DOC_CSS_URL:-}"

# -------------------------- UI -----------------------------
banner() {
  print_hr
  echo "===== DOC WIZARD — Gerar Documentação (HTML+CSS → PDF com IA) ====="
  print_hr
  echo
}

choose_profile() {
  echo "Escolha um perfil:"
  echo "  1) Rápido    → sem PDF, sem pós-polimento"
  echo "  2) Completo  → com PDF + pós-polimento (seções e arquivos)"
  echo "  3) Personalizado"
  local choice
  choice="$(ask "Opção [1-3]" "2")"
  case "$choice" in
    1)
      DOC_NO_PDF=1
      DOC_POST_POLISH=0
      DOC_POLISH_APPLY_FILES=0
      DOC_STREAM=0
      ;;
    2)
      DOC_NO_PDF=0
      DOC_POST_POLISH=1
      DOC_POLISH_APPLY_FILES=1
      DOC_STREAM=0
      ;;
    *)
      personalize
      ;;
  esac
}

personalize() {
  DOC_MODEL="$(ask "Modelo principal" "$DOC_MODEL")"
  DOC_SUMMARY_MODEL="$(ask "Modelo p/ resumos (contexto compacto)" "$DOC_SUMMARY_MODEL")"
  DOC_POLISH_MODEL="$(ask "Modelo p/ pós-polimento" "$DOC_POLISH_MODEL")"

  DOC_FILES_PER_BATCH="$(ask "Arquivos por lote" "$DOC_FILES_PER_BATCH")"
  DOC_FILE_EXCERPT_CHARS="$(ask "Chars de excerto por arquivo" "$DOC_FILE_EXCERPT_CHARS")"
  DOC_BATCH_OUT_TOKENS="$(ask "Tokens de saída por LOTE" "$DOC_BATCH_OUT_TOKENS")"
  DOC_SEC_OUT_TOKENS="$(ask "Tokens de saída por SEÇÃO" "$DOC_SEC_OUT_TOKENS")"

  if confirm "Ativar streaming (pode reduzir folga de contexto)?" "n"; then DOC_STREAM=1; else DOC_STREAM=0; fi
  if confirm "Gerar PDF (Playwright/Chromium)?" "y"; then DOC_NO_PDF=0; else DOC_NO_PDF=1; fi

  if confirm "Ativar pós-polimento por seção?" "y"; then
    DOC_POST_POLISH=1
    if confirm "Polir também a seção de 'Arquivos...'?" "y"; then DOC_POLISH_APPLY_FILES=1; else DOC_POLISH_APPLY_FILES=0; fi
    DOC_POLISH_STYLE="$(ask "Instruções de estilo (texto livre)" "$DOC_POLISH_STYLE")"
    DOC_POLISH_MODEL="$(ask "Modelo do polimento" "$DOC_POLISH_MODEL")"
  else
    DOC_POST_POLISH=0
    DOC_POLISH_APPLY_FILES=0
  fi

  DOC_CTX="$(ask "Limite de contexto (DOC_CTX)" "$DOC_CTX")"
  DOC_CTX_BUFFER="$(ask "Folga de contexto (DOC_CTX_BUFFER)" "$DOC_CTX_BUFFER")"

  # Capa / identidade
  LOGO_PATH="$(ask "Logo (caminho/URL)" "$LOGO_PATH")"
  DEPT_TEXT="$(ask "Texto da Coordenação" "$DEPT_TEXT")"
  PERSON_NAME="$(ask "Nome (abaixo da Coordenação)" "$PERSON_NAME")"
  CSS_LOGO_H="$(ask "Altura da logo (ex.: 96px, 160px)" "$CSS_LOGO_H")"
  CSS_DEPT_FS="$(ask "Fonte da Coordenação (ex.: 11pt, 12pt)" "$CSS_DEPT_FS")"
  CSS_BRAND_GAP="$(ask "Gap vertical entre blocos da marca (ex.: 0mm, 2mm)" "$CSS_BRAND_GAP")"
  CSS_LOGO_MB="$(ask "Margem inferior da logo (aceita negativo, ex.: -4mm)" "$CSS_LOGO_MB")"

  # Entidades (acima da Coordenação)
  if confirm "Adicionar linhas de entidades (acima da Coordenação) agora?" "y"; then
    mapfile -t _ents < <(ask_multilines "Digite as entidades")
    if [[ "${#_ents[@]}" -gt 0 ]]; then
      ENT_LINES_ARR=("${_ents[@]}")
    fi
  fi
  ENT_FILE="$(ask "Arquivo .txt com entidades (opcional; uma por linha)" "$ENT_FILE")"
  CSS_ENT_FS="$(ask "Fonte das entidades (ex.: 11pt)" "$CSS_ENT_FS")"
  CSS_ENT_GAP="$(ask "Gap entre entidades (ex.: 0mm, 0.5mm, 1.5mm)" "$CSS_ENT_GAP")"
  CSS_ENT_LH="$(ask "Line-height das entidades (ex.: 1.10, 1.15)" "$CSS_ENT_LH")"

  # TOC
  if confirm "Incluir Sumário (TOC)?" "y"; then TOC=1; else TOC=0; fi
  TOC_TITLE="$(ask "Título do Sumário" "$TOC_TITLE")"
  if confirm "Quebrar página antes do Sumário?" "y"; then TOC_PAGEBREAK=1; else TOC_PAGEBREAK=0; fi

  # Tema / CSS
  DOC_THEME="$(ask "Tema embutido (default/clean/dark/serif)" "$DOC_THEME")"
  DOC_CSS_FILE="$(ask "CSS local (caminho .css) — sobrepõe tema (opcional)" "$DOC_CSS_FILE")"
  DOC_CSS_URL="$(ask "CSS remoto (URL) — sobrepõe tema se arquivo vazio (opcional)" "$DOC_CSS_URL")"
}

print_summary() {
  echo
  print_hr
  echo "===== RESUMO DA EXECUÇÃO ====="
  cat <<EOF
  [IA]
    DOC_MODEL=$DOC_MODEL
    DOC_SUMMARY_MODEL=$DOC_SUMMARY_MODEL
    DOC_POLISH_MODEL=$DOC_POLISH_MODEL
    DOC_FILES_PER_BATCH=$DOC_FILES_PER_BATCH
    DOC_FILE_EXCERPT_CHARS=$DOC_FILE_EXCERPT_CHARS
    DOC_BATCH_OUT_TOKENS=$DOC_BATCH_OUT_TOKENS
    DOC_SEC_OUT_TOKENS=$DOC_SEC_OUT_TOKENS
    DOC_STREAM=$DOC_STREAM
    DOC_NO_PDF=$DOC_NO_PDF
    DOC_POST_POLISH=$DOC_POST_POLISH
    DOC_POLISH_APPLY_FILES=$DOC_POLISH_APPLY_FILES
    DOC_POLISH_STYLE=$DOC_POLISH_STYLE
    DOC_CTX=$DOC_CTX
    DOC_CTX_BUFFER=$DOC_CTX_BUFFER

  [Capa/Identidade]
    LOGO_PATH=$LOGO_PATH
    DEPT_TEXT=$DEPT_TEXT
    PERSON_NAME=$PERSON_NAME
    CSS_LOGO_H=$CSS_LOGO_H
    CSS_DEPT_FS=$CSS_DEPT_FS
    CSS_BRAND_GAP=$CSS_BRAND_GAP
    CSS_LOGO_MB=$CSS_LOGO_MB

  [Entidades]
    CSS_ENT_FS=$CSS_ENT_FS
    CSS_ENT_GAP=$CSS_ENT_GAP
    CSS_ENT_LH=$CSS_ENT_LH
    ENT_FILE=${ENT_FILE:-}
    ENT_LINES (env)=${ENT_LINES:-}

  [Sumário/CSS]
    TOC=$TOC
    TOC_TITLE=$TOC_TITLE
    TOC_PAGEBREAK=$TOC_PAGEBREAK
    DOC_THEME=$DOC_THEME
    DOC_CSS_FILE=${DOC_CSS_FILE:-}
    DOC_CSS_URL=${DOC_CSS_URL:-}

  SCRIPT_PATH=$SCRIPT_PATH
EOF
  print_hr
  echo
}

build_and_run() {
  SCRIPT_PATH="$(ask "Caminho do gerar_documentation.py" "$SCRIPT_PATH")"
  if [[ ! -f "$SCRIPT_PATH" ]]; then
    echo "❌ Script não encontrado: $SCRIPT_PATH"
    exit 2
  fi

  if [[ -z "${OPENAI_API_KEY:-}" ]]; then
    echo "⚠️  OPENAI_API_KEY não encontrado no ambiente. Se você usa .env ao lado do script, tudo bem."
  fi

  if ! confirm "Executar agora?"; then
    echo "Operação cancelada."
    exit 0
  fi

  # Monta comando com flags (usando '=' para permitir valores negativos ex.: --logo-mb=-4mm)
  CMD_ARGS=()
  CMD_ARGS+=("--logo-path=${LOGO_PATH}")
  CMD_ARGS+=("--dept-text=${DEPT_TEXT}")
  CMD_ARGS+=("--person=${PERSON_NAME}")

  CMD_ARGS+=("--logo-h=${CSS_LOGO_H}")
  CMD_ARGS+=("--dept-fs=${CSS_DEPT_FS}")
  CMD_ARGS+=("--brand-gap=${CSS_BRAND_GAP}")
  CMD_ARGS+=("--logo-mb=${CSS_LOGO_MB}")

  # Entidades
  if [[ -n "${ENT_FILE:-}" ]]; then
    CMD_ARGS+=("--ent-file=${ENT_FILE}")
  fi
  if [[ -n "${ENT_LINES:-}" ]]; then
    # ENT_LINES via env: separadas por | ou ;
    IFS='|;' read -r -a _env_ents <<< "${ENT_LINES}"
    for e in "${_env_ents[@]}"; do
      e="${e#"${e%%[![:space:]]*}"}"; e="${e%"${e##*[![:space:]]}"}" # trim
      [[ -n "$e" ]] && CMD_ARGS+=("--ent=${e}")
    done
  fi
  if [[ "${#ENT_LINES_ARR[@]:-0}" -gt 0 ]]; then
    for e in "${ENT_LINES_ARR[@]}"; do
      CMD_ARGS+=("--ent=${e}")
    done
  fi
  CMD_ARGS+=("--ent-fs=${CSS_ENT_FS}")
  CMD_ARGS+=("--ent-gap=${CSS_ENT_GAP}")
  CMD_ARGS+=("--ent-lh=${CSS_ENT_LH}")

  # TOC
  [[ "${TOC}" == "1" ]] && CMD_ARGS+=("--toc")
  CMD_ARGS+=("--toc-title=${TOC_TITLE}")
  [[ "${TOC_PAGEBREAK}" == "1" ]] && CMD_ARGS+=("--toc-pagebreak")

  # Tema / CSS
  if [[ -n "${DOC_CSS_FILE:-}" ]]; then
    CMD_ARGS+=("--css-file=${DOC_CSS_FILE}")
  elif [[ -n "${DOC_CSS_URL:-}" ]]; then
    CMD_ARGS+=("--css-url=${DOC_CSS_URL}")
  else
    CMD_ARGS+=("--theme=${DOC_THEME}")
  fi

  print_hr
  echo "[ENV] Variáveis que serão usadas:"
  cat <<EOF
DOC_MODEL=$DOC_MODEL
DOC_SUMMARY_MODEL=$DOC_SUMMARY_MODEL
DOC_POLISH_MODEL=$DOC_POLISH_MODEL
DOC_FILES_PER_BATCH=$DOC_FILES_PER_BATCH
DOC_FILE_EXCERPT_CHARS=$DOC_FILE_EXCERPT_CHARS
DOC_BATCH_OUT_TOKENS=$DOC_BATCH_OUT_TOKENS
DOC_SEC_OUT_TOKENS=$DOC_SEC_OUT_TOKENS
DOC_STREAM=$DOC_STREAM
DOC_NO_PDF=$DOC_NO_PDF
DOC_POST_POLISH=$DOC_POST_POLISH
DOC_POLISH_APPLY_FILES=$DOC_POLISH_APPLY_FILES
DOC_POLISH_STYLE=$DOC_POLISH_STYLE
DOC_CTX=$DOC_CTX
DOC_CTX_BUFFER=$DOC_CTX_BUFFER
EOF
  print_hr
  echo "[CMD] python3 \"$SCRIPT_PATH\" ${CMD_ARGS[*]}"
  print_hr

  DOC_MODEL="$DOC_MODEL" \
  DOC_SUMMARY_MODEL="$DOC_SUMMARY_MODEL" \
  DOC_POLISH_MODEL="$DOC_POLISH_MODEL" \
  DOC_FILES_PER_BATCH="$DOC_FILES_PER_BATCH" \
  DOC_FILE_EXCERPT_CHARS="$DOC_FILE_EXCERPT_CHARS" \
  DOC_BATCH_OUT_TOKENS="$DOC_BATCH_OUT_TOKENS" \
  DOC_SEC_OUT_TOKENS="$DOC_SEC_OUT_TOKENS" \
  DOC_STREAM="$DOC_STREAM" \
  DOC_NO_PDF="$DOC_NO_PDF" \
  DOC_POST_POLISH="$DOC_POST_POLISH" \
  DOC_POLISH_APPLY_FILES="$DOC_POLISH_APPLY_FILES" \
  DOC_POLISH_STYLE="$DOC_POLISH_STYLE" \
  DOC_CTX="$DOC_CTX" \
  DOC_CTX_BUFFER="$DOC_CTX_BUFFER" \
  python3 "$SCRIPT_PATH" "${CMD_ARGS[@]}"
}

# -------------------------- Main ---------------------------
main() {
  banner
  choose_profile
  print_summary
  build_and_run
  echo "✅ Finalizado."
}

main "$@"

