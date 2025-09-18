#!/usr/bin/env bash
# KPI Wizard — Top10 (com pré-pass p/ CSVs) ou Individual
# - Faz pré-pass no modo TopK=10 Fluxo B para gerar:
#     reports/outputs/<PERIODO>/top10/topk_peritos.csv
#     reports/outputs/<PERIODO>/top10/scope_gate_b.csv
#   e então executa o run principal em modo Top10 com --peritos-csv/--scope-csv.
# - Requisitos opcionais: fzf (ou gum) para autocomplete
# - Dependências: sqlite3 (para listar peritos), python3

set -euo pipefail

# ─────────────────────────── Helpers de data ───────────────────────────
DBIN() { if command -v gdate >/dev/null 2>&1; then echo "gdate"; else echo "date"; fi; }
DATE_BIN="$(DBIN)"

# ─────────────────────────── Raiz do projeto ───────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Tenta git root; se falhar, usa o pai do script
if command -v git >/dev/null 2>&1 && git -C "$SCRIPT_DIR" rev-parse --show-toplevel >/dev/null 2>&1; then
  PROJ_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
else
  PROJ_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

DB_PATH="${PROJ_ROOT}/db/atestmed.db"

# ───────────────────────── localizar make_kpi_report.py ─────────────────────────
find_make_py() {
  local cands=(
    "${PROJ_ROOT}/reports/make_kpi_report.py"
    "${PROJ_ROOT}/graphs_and_tables/make_kpi_report.py"
    "${PROJ_ROOT}/make_kpi_report.py"
    "${SCRIPT_DIR}/make_kpi_report.py"
    "${SCRIPT_DIR}/../reports/make_kpi_report.py"
  )
  for p in "${cands[@]}"; do
    [[ -f "$p" ]] && { echo "$p"; return 0; }
  done
  # Fallback: varredura rasa (até 3 níveis) a partir do PROJ_ROOT
  local found
  found="$(find "$PROJ_ROOT" -maxdepth 3 -type f -name 'make_kpi_report.py' -print -quit 2>/dev/null || true)"
  if [[ -n "$found" && -f "$found" ]]; then
    echo "$found"
    return 0
  fi
  echo ""
}

MAKE_KPI_PY="$(find_make_py)"
if [[ -z "$MAKE_KPI_PY" ]]; then
  echo "⚠️  Não encontrei make_kpi_report.py automaticamente."
  read -rp "Informe o caminho completo para make_kpi_report.py: " MAKE_KPI_PY
fi
if [[ ! -f "$MAKE_KPI_PY" ]]; then
  echo "❌ make_kpi_report.py não encontrado em: $MAKE_KPI_PY"
  exit 2
fi
echo "✅ Usando: $MAKE_KPI_PY"

# ─────────────────────────── Bins básicos ───────────────────────────
PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "❌ python3 não encontrado no PATH."
  exit 2
fi

if [[ ! -f "$DB_PATH" ]]; then
  echo "⚠️  Banco não encontrado em: $DB_PATH"
  read -rp "Informe o caminho do SQLite do projeto (atestmed.db): " DB_PATH
fi

has_fzf() { command -v fzf >/dev/null 2>&1; }
has_gum() { command -v gum >/dev/null 2>&1; }

ask() { # ask "Pergunta" "default"
  local prompt="${1:-}"; shift
  local default="${1:-}"; shift || true
  local ans
  if [[ -n "$default" ]]; then
    read -rp "${prompt} [${default}]: " ans || true
    echo "${ans:-$default}"
  else
    read -rp "${prompt}: " ans || true
    echo "${ans}"
  fi
}

confirm() { # confirm "Pergunta" "Y/n"
  local q="${1:-Confirmar?}"; local def="${2:-Y/n}"
  local ans; read -rp "${q} [${def}]: " ans || true
  ans="${ans:-${def%%/*}}"
  case "${ans,,}" in
    y|yes|s|sim) return 0 ;;
    *) return 1 ;;
  esac
}

# ─────────────────────── Modo: Top10 x Individual ───────────────────────
echo "===== KPI Wizard ====="
echo "Selecione o modo:"
echo "  1) Top 10 (Fluxo B com pré-pass p/ CSVs)"
echo "  2) Individual (por perito)"
MODE="$(ask "Opção [1-2]" "1")"
case "$MODE" in
  1) KIND="top10" ;;
  2) KIND="perito" ;;
  *) echo "Opção inválida."; exit 2 ;;
esac

# ───────────────────── Seleção de perito (se Individual) ─────────────────────
PERITO_NAME=""
if [[ "$KIND" == "perito" ]]; then
  echo "Buscando peritos no banco..."
  if ! command -v sqlite3 >/dev/null 2>&1; then
    echo "⚠️  sqlite3 não encontrado. Entrada manual."
    PERITO_NAME="$(ask "Digite exatamente o nome do perito (como no DB)")"
  else
    mapfile -t PERITOS < <(sqlite3 -noheader -batch "$DB_PATH" "SELECT nomePerito FROM peritos ORDER BY nomePerito COLLATE NOCASE;")
    if ((${#PERITOS[@]}==0)); then
      echo "⚠️  Nenhum perito encontrado. Entrada manual."
      PERITO_NAME="$(ask "Digite exatamente o nome do perito (como no DB)")"
    else
      if has_fzf; then
        PERITO_NAME="$(printf '%s\n' "${PERITOS[@]}" | fzf --height=20 --reverse --prompt="Perito > " --border || true)"
      elif has_gum; then
        PERITO_NAME="$(printf '%s\n' "${PERITOS[@]}" | gum choose || true)"
      else
        printf '%s\n' "${PERITOS[@]}"
        PERITO_NAME="$(ask "Copie/cole o nome do perito exatamente como acima")"
      fi
    fi
  fi
  [[ -z "$PERITO_NAME" ]] && { echo "❌ Perito não selecionado."; exit 2; }
  echo "Perito selecionado: $PERITO_NAME"
fi

# ───────────────────────────── Escolha do período ─────────────────────────────
echo
echo "Escolha o período:"
echo "  1) Mês atual"
echo "  2) Mês anterior"
echo "  3) Escolher um dos últimos 6 meses"
echo "  4) Intervalo personalizado (YYYY-MM-DD a YYYY-MM-DD)"
OPT_PERIOD="$(ask "Opção [1-4]" "1")"

calc_month_bounds() {
  local ym="$1"  # YYYY-MM
  local y="${ym%-*}" m="${ym#*-}"
  local first="${y}-${m}-01"
  local last="$($DATE_BIN -d "${first} +1 month -1 day" +%Y-%m-%d)"
  echo "$first" "$last"
}

case "$OPT_PERIOD" in
  1) YM="$($DATE_BIN +%Y-%m)";              read START END < <(calc_month_bounds "$YM") ;;
  2) YM="$($DATE_BIN -d "-1 month" +%Y-%m)"; read START END < <(calc_month_bounds "$YM") ;;
  3)
     echo "Selecione o mês (últimos 6):"
     declare -a YMS=()
     for i in {0..5}; do YMS+=("$($DATE_BIN -d "-$i month" +%Y-%m)"); done
     SEL=""
     if has_fzf; then
       SEL="$(printf '%s\n' "${YMS[@]}" | fzf --height=10 --reverse --prompt="YYYY-MM > " --border || true)"
     elif has_gum; then
       SEL="$(printf '%s\n' "${YMS[@]}" | gum choose || true)"
     else
       printf '%s\n' "${YMS[@]}"
       SEL="$(ask "Digite YYYY-MM" "${YMS[0]}")"
     fi
     [[ -z "$SEL" ]] && { echo "❌ Mês não selecionado."; exit 2; }
     read START END < <(calc_month_bounds "$SEL")
     ;;
  4)
     START="$(ask "Data inicial (YYYY-MM-DD)")"
     END="$(ask "Data final   (YYYY-MM-DD)")"
     ;;
  *) echo "Opção inválida."; exit 2 ;;
esac
echo "Período escolhido: $START a $END"

# ───────────────────────── Flags padrão (Top10 e Individual) ─────────────────────────
DEFAULT_FLAGS=( --min-analises 50 --export-org --export-pdf --add-comments --r-appendix --include-high-nc --kpi-base full --save-manifests )

echo
if confirm "Usar o comando PADRÃO agora?" "Y/n"; then
  USE_DEFAULT=1
else
  USE_DEFAULT=0
fi

# ========== SELEÇÃO DE EXTRAS COM HELP ==========
declare -a EXTRA_CATALOG=(
  "--with-impact|Gera seção 'Impacto na Fila' (grupos e/ou individual)|"
  "--impact-all-tests|Roda todos os testes/figuras do módulo de impacto|"
  "--reuse-kpi|Reusa KPI/figuras já existentes (montagem apenas)|"
  "--rank-by=scoreFinal|Ranking por scoreFinal (quando aplicável)|VALUE_FIXED"
  "--rank-by=harm|Ranking por 'harm' (quando aplicável)|VALUE_FIXED"
  "--high-nc-threshold|Altera limiar de 'Alta NC' (padrão 90%)|ASK_NUMBER"
  "--high-nc-min-tasks|Mínimo de tarefas para 'Alta NC' (padrão 50)|ASK_INT"
  "--kpi-base=full|KPI completo (padrão)|VALUE_FIXED"
  "--kpi-base=nc-only|KPI calculado apenas por NC|VALUE_FIXED"
  "--save-manifests|Salvar CSVs com seleção/escopo usados|"
  "--plan-only|Mostra plano (dry-run) e sai|"
)

show_extra_help() {
  local key="$1"
  for entry in "${EXTRA_CATALOG[@]}"; do
    IFS='|' read -r flag desc kind <<<"$entry"
    if [[ "$key" == "$flag" ]]; then
      printf "%s\n\nEx.: %s\n" "$desc" "$flag"
      return
    fi
  done
  echo "Selecione para ativar."
}

pick_extras_gum() {
  local items=()
  for entry in "${EXTRA_CATALOG[@]}"; do
    IFS='|' read -r flag desc kind <<<"$entry"
    items+=("$flag  —  $desc")
  done
  gum choose --no-limit "${items[@]}" || true
}

pick_extras_fzf() {
  local list=""
  for entry in "${EXTRA_CATALOG[@]}"; do
    IFS='|' read -r flag desc kind <<<"$entry"
    list+="$flag  —  $desc"$'\n'
  done
  echo -n "$list" | \
    fzf --multi --height=18 --reverse --prompt="Extras > " \
        --preview='flag=$(echo {} | sed "s/  —.*//"); '"$(typeset -f show_extra_help)";' show_extra_help "$flag"' \
        --preview-window=right:60%:wrap || true
}

pick_extras_text() {
  echo "Extras disponíveis (digite números separados por espaço, Enter para nenhum):"
  local i=1
  for entry in "${EXTRA_CATALOG[@]}"; do
    IFS='|' read -r flag desc kind <<<"$entry"
    printf "  %2d) %-24s %s\n" "$i" "$flag" "$desc"
    ((i++))
  done
  read -rp "Escolhas: " CHOICES || true
  local out=()
  i=1
  while read -r n; do
    [[ "$n" =~ ^[0-9]+$ ]] || continue
    local idx=$((n-1))
    [[ $idx -ge 0 && $idx -lt ${#EXTRA_CATALOG[@]} ]] || continue
    IFS='|' read -r flag desc kind <<<"${EXTRA_CATALOG[$idx]}"
    out+=("$flag  —  $desc")
  done <<<"$(echo "$CHOICES")"
  printf '%s\n' "${out[@]}"
}

collect_extras() {
  local picks=()
  if has_gum; then
    mapfile -t picks < <(pick_extras_gum)
  elif has_fzf; then
    mapfile -t picks < <(pick_extras_fzf)
  else
    mapfile -t picks < <(pick_extras_text)
  fi

  local out=()
  for line in "${picks[@]}"; do
    local flag="${line%%  —*}"
    local kind=""
    for entry in "${EXTRA_CATALOG[@]}"; do
      IFS='|' read -r f d k <<<"$entry"
      [[ "$f" == "$flag" ]] && { kind="$k"; break; }
    done
    case "$kind" in
      VALUE_FIXED) out+=("$flag") ;;
      ASK_NUMBER)
        read -rp "Valor numérico para ${flag} (ex.: 90): " val
        [[ -n "$val" ]] && out+=("${flag}=${val}") || true
        ;;
      ASK_INT)
        read -rp "Valor inteiro para ${flag} (ex.: 50): " val
        [[ -n "$val" ]] && out+=("${flag}=${val}") || true
        ;;
      *) out+=("$flag") ;;
    esac
  done

  # normalizações exclusivas
  dedupe_last() {
    local key="$1"; shift
    local -a keep=(); local last=""
    for t in "$@"; do
      [[ "$t" == "$key="* ]] && last="$t" || keep+=("$t")
    done
    [[ -n "$last" ]] && keep+=("$last")
    printf '%s\n' "${keep[@]}"
  }

  local tmp=("${out[@]}")
  out=($(dedupe_last "--rank-by" "${tmp[@]}"))
  tmp=("${out[@]}")
  out=($(dedupe_last "--kpi-base" "${tmp[@]}"))

  printf '%s\n' "${out[@]}"
}

EXTRA_FLAGS=()
if (( USE_DEFAULT == 0 )); then
  echo
  echo "=== Extras (múltipla seleção) ==="
  mapfile -t EXTRA_FLAGS < <(collect_extras)
  if ((${#EXTRA_FLAGS[@]})); then
    echo "Extras selecionados: ${EXTRA_FLAGS[*]}"
  else
    echo "Nenhum extra selecionado."
  fi
fi

# ========== EDIÇÃO INTERATIVA DE FLAGS (seed com defaults SEMPRE) ==========
# Defaults
PRE_TOPK_DEFAULT="10"
PRE_FLUXO_DEFAULT="B"
PRE_MIN_DEFAULT="50"
PRE_PLAN_ONLY_DEFAULT="N"   # gerar CSVs por padrão

MAIN_FLUXO_DEFAULT="B"
MAIN_MIN_DEFAULT="50"

# Seed (evita 'unbound' com set -u)
PRE_TOPK="$PRE_TOPK_DEFAULT"
PRE_FLUXO="$PRE_FLUXO_DEFAULT"
PRE_MIN="$PRE_MIN_DEFAULT"
PRE_PLAN_ONLY="$PRE_PLAN_ONLY_DEFAULT"
MAIN_FLUXO="$MAIN_FLUXO_DEFAULT"
MAIN_MIN="$MAIN_MIN_DEFAULT"

# Quando não usar padrão, permitir editar
if (( USE_DEFAULT == 0 )); then
  echo
  echo "=== Edição de Flags (Pré-pass TopK) ==="
  PRE_TOPK="$(ask "TopK" "$PRE_TOPK_DEFAULT")"
  PRE_FLUXO="$(ask "Fluxo (A/B)" "$PRE_FLUXO_DEFAULT")"
  PRE_MIN="$(ask "Mínimo de análises (pré-pass)" "$PRE_MIN_DEFAULT")"
  PRE_PLAN_ONLY="$(ask "Pré-pass em modo plan-only? (Y/n)" "$PRE_PLAN_ONLY_DEFAULT")"

  echo
  echo "=== Edição de Flags (Run principal) ==="
  MAIN_FLUXO="$(ask "Fluxo (A/B)" "$MAIN_FLUXO_DEFAULT")"
  MAIN_MIN="$(ask "Mínimo de análises (run)" "$MAIN_MIN_DEFAULT")"

  echo
  echo "Selecione os recursos para o relatório:"
  OPT_ORG="$(ask "Exportar ORG? (Y/n)" "Y")"
  OPT_PDF="$(ask "Exportar PDF? (Y/n)" "Y")"
  OPT_COMMENTS="$(ask "Incluir comentários ChatGPT? (Y/n)" "Y")"
  OPT_RAPP="$(ask "Incluir apêndice R? (Y/n)" "Y")"
  OPT_HIGHNC="$(ask "Incluir seção High-NC? (Y/n)" "Y")"
  KPI_BASE="$(ask "Base KPI (full|nc-only)" "full")"

  echo
  echo "Você pode acrescentar flags extras (vazio para continuar)."
  echo "Exemplos: --with-impact  --impact-all-tests  --reuse-kpi  --rank-by scoreFinal"
  read -rp "Flags extras: " EXTRA_LINE || true
  EXTRA_LINE_ARR=( $EXTRA_LINE )
  EXTRA_FLAGS=( "${EXTRA_FLAGS[@]}" "${EXTRA_LINE_ARR[@]}" )

  EDITED_DEFAULTS=()
  [[ "${OPT_ORG^^}"      == "Y" ]] && EDITED_DEFAULTS+=( --export-org )
  [[ "${OPT_PDF^^}"      == "Y" ]] && EDITED_DEFAULTS+=( --export-pdf )
  [[ "${OPT_COMMENTS^^}" == "Y" ]] && EDITED_DEFAULTS+=( --add-comments )
  [[ "${OPT_RAPP^^}"     == "Y" ]] && EDITED_DEFAULTS+=( --r-appendix )
  [[ "${OPT_HIGHNC^^}"   == "Y" ]] && EDITED_DEFAULTS+=( --include-high-nc )
  EDITED_DEFAULTS+=( --kpi-base "$KPI_BASE" --save-manifests )

  DEFAULT_FLAGS=( --min-analises "$MAIN_MIN" "${EDITED_DEFAULTS[@]}" )
fi

# ───────────────────────────── Montagem do comando ─────────────────────────────
CMD=( "$PYTHON_BIN" "$MAKE_KPI_PY" --start "$START" --end "$END" )

# Diretórios conforme make_kpi_report.py (BASE_DIR = root do projeto; outputs em reports/outputs)
PERIODO_DIR="${PROJ_ROOT}/reports/outputs/${START}_a_${END}"
TOP_DIR="${PERIODO_DIR}/top10"
mkdir -p "$TOP_DIR"

if [[ "$KIND" == "top10" ]]; then
  echo
  echo "🔎 Pré-pass: gerando CSVs de seleção (TopK/Fluxo configuráveis)…"
  PREPASS_CMD=( "$PYTHON_BIN" "$MAKE_KPI_PY"
    --start "$START" --end "$END"
    --topk "$PRE_TOPK" --fluxo "$PRE_FLUXO"
    --min-analises "$PRE_MIN"
    --save-manifests
  )
  [[ "${PRE_PLAN_ONLY^^}" == "Y" ]] && PREPASS_CMD+=( --plan-only )

  printf ' [prepass] %q' "${PREPASS_CMD[@]}"; echo
  "${PREPASS_CMD[@]}"

  # Esperados: TOP_DIR/topk_peritos.csv e TOP_DIR/scope_gate_b.csv
  PERITOS_CSV="${TOP_DIR}/topk_peritos.csv"
  SCOPE_CSV="${TOP_DIR}/scope_gate_b.csv"

  if [[ ! -f "$PERITOS_CSV" ]]; then
    echo "❌ Pré-pass não gerou ${PERITOS_CSV}."
    echo "   Verifique se há peritos elegíveis (gate Fluxo B) no período informado."
    exit 3
  fi
  [[ ! -f "$SCOPE_CSV" ]] && { echo "⚠️  Pré-pass não gerou ${SCOPE_CSV}. Seguirei apenas com --peritos-csv."; SCOPE_CSV=""; }

  CMD+=( --top10 --fluxo "$MAIN_FLUXO" --peritos-csv "$PERITOS_CSV" )
  [[ -n "$SCOPE_CSV" ]] && CMD+=( --scope-csv "$SCOPE_CSV" )
else
  CMD+=( --perito "$PERITO_NAME" )
fi

# Acrescenta flags padrão (ou editadas) e extras
CMD+=( "${DEFAULT_FLAGS[@]}" )
if ((${#EXTRA_FLAGS[@]} > 0)); then
  CMD+=( "${EXTRA_FLAGS[@]}" )
fi

echo
echo "==========================================="
echo "Comando final:"
printf ' %q' "${CMD[@]}"; echo
echo "==========================================="
echo

# ───────────────────────────── Execução ─────────────────────────────
exec "${CMD[@]}"

