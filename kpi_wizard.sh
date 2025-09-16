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
      PERITO_NAME="$(ask "Digite o nome do perito")"
    else
      if has_fzf; then
        echo "Digite para filtrar e selecione com ↑/↓ (fzf):"
        PERITO_NAME="$(printf '%s\n' "${PERITOS[@]}" | fzf --height=20 --reverse --prompt="Perito > " --border --no-multi || true)"
      elif has_gum; then
        PERITO_NAME="$(printf '%s\n' "${PERITOS[@]}" | gum filter --placeholder "Busque o perito..." || true)"
      else
        printf '%s\n' "${PERITOS[@]:0:30}"
        echo "… (lista truncada; instale fzf/gum para UX melhor)"
        PERITO_NAME="$(ask "Digite exatamente o nome do perito")"
      fi
    fi
  fi
  if [[ -z "$PERITO_NAME" ]]; then
    echo "❌ Perito não selecionado."; exit 2
  fi
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
  1)
    YM="$($DATE_BIN +%Y-%m)"
    read START END < <(calc_month_bounds "$YM")
    ;;
  2)
    YM="$($DATE_BIN -d "-1 month" +%Y-%m)"
    read START END < <(calc_month_bounds "$YM")
    ;;
  3)
    echo "Selecione o mês (últimos 6):"
    declare -a YMS=()
    for i in {0..5}; do
      YMS+=("$($DATE_BIN -d "-$i month" +%Y-%m)")
    done
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
  *)
    echo "Opção inválida."; exit 2 ;;
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

EXTRA_FLAGS=()
if (( USE_DEFAULT == 0 )); then
  echo "Você pode acrescentar flags extras (vazio para continuar)."
  echo "Exemplos: --with-impact  --impact-all-tests  --reuse-kpi  --rank-by scoreFinal"
  read -rp "Flags extras: " EXTRA_LINE || true
  # shellcheck disable=SC2206
  EXTRA_FLAGS=( $EXTRA_LINE )
fi

# ───────────────────────────── Montagem do comando ─────────────────────────────
CMD=( "$PYTHON_BIN" "$MAKE_KPI_PY" --start "$START" --end "$END" )

# Diretórios conforme make_kpi_report.py (BASE_DIR = root do projeto; outputs em reports/outputs)
PERIODO_DIR="${PROJ_ROOT}/reports/outputs/${START}_a_${END}"
TOP_DIR="${PERIODO_DIR}/top10"
mkdir -p "$TOP_DIR"

if [[ "$KIND" == "top10" ]]; then
  # ───────────────────── Pré-pass: materializa seleção (TopK=10, Fluxo B) ─────────────────────
  echo
  echo "🔎 Pré-pass: gerando CSVs de seleção (TopK=10, Fluxo B)…"
  PREPASS_CMD=( "$PYTHON_BIN" "$MAKE_KPI_PY"
    --start "$START" --end "$END"
    --topk 10 --fluxo B
    --min-analises 50
    --save-manifests
    --plan-only
  )
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
  if [[ ! -f "$SCOPE_CSV" ]]; then
    echo "⚠️  Pré-pass não gerou ${SCOPE_CSV}. Seguirei apenas com --peritos-csv."
    SCOPE_CSV=""
  fi

  # Comando principal em Top10 (Fluxo B) com CSVs materializados
  CMD+=( --top10 --fluxo B --peritos-csv "$PERITOS_CSV" )
  [[ -n "$SCOPE_CSV" ]] && CMD+=( --scope-csv "$SCOPE_CSV" )
else
  # Individual
  CMD+=( --perito "$PERITO_NAME" )
fi

# Acrescenta flags padrão e extras
if (( USE_DEFAULT == 1 )); then
  CMD+=( "${DEFAULT_FLAGS[@]}" )
fi
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

