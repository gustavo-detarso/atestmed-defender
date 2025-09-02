#!/usr/bin/env bash
# kpi_wizard.sh — Wizard interativo para make_kpi_report.py
# Gera relatórios KPI (Top10 ou Individual) com todas as flags suportadas,
# em lógica similar ao impact_wizard.sh.
#
# Uso rápido (não interativo): você pode passar flags que serão anexadas ao comando final.
#   ./kpi_wizard.sh --add-comments --with-impact
#
# Requisitos: bash, python3, Rscript (opcional para apêndice R)
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────
# Descobrir Python (prioriza venv local)
# ──────────────────────────────────────────────────────────────────────
if [[ -x "./.venv/bin/python" ]]; then
  PYTHON="./.venv/bin/python"
elif compgen -G "${HOME}/.local/share/virtualenvs/atestmed-*/bin/python3" >/dev/null; then
  PYTHON="$(ls -1 ${HOME}/.local/share/virtualenvs/atestmed-*/bin/python3 | head -n1)"
else
  PYTHON="$(command -v python3 || command -v python || true)"
fi
if [[ -z "${PYTHON}" ]]; then
  echo "❌ Python não encontrado no PATH." >&2
  exit 1
fi

# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
date_bin() { if command -v gdate >/dev/null 2>&1; then echo "gdate"; else echo "date"; fi; }
DBIN="$(date_bin)"

is_ym()  { [[ "$1" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]]; }
is_ymd() { [[ "$1" =~ ^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$ ]]; }

ym_first_day() { echo "$1-01"; }
ym_last_day()  { $DBIN -d "$1-01 +1 month -1 day" +%Y-%m-%d; }

ask() { # ask "Pergunta" "valor_padrao"
  local prompt="${1:-}"; local default="${2:-}"; local ans=""
  if [[ -n "${default}" ]]; then
    read -rp "${prompt} [${default}]: " ans || true
    echo "${ans:-$default}"
  else
    read -rp "${prompt}: " ans || true
    echo "${ans}"
  fi
}

confirm() { # confirm "Pergunta" "y|n"
  local prompt="${1:-}"; local def="${2:-y}"; local ans=""
  read -rp "${prompt} [${def}]: " ans || true
  ans="${ans:-$def}"
  [[ "${ans,,}" =~ ^y(es)?$|^s(im)?$|^y$|^$ ]]
}

sanitize() {
  # substitui chars não alfanuméricos por _ (para nomes de logs)
  local s="${1:-}"
  s="${s//[^[:alnum:]-_]/_}"
  s="${s##_}"; s="${s%%_}"
  echo "${s:-output}"
}

# ──────────────────────────────────────────────────────────────────────
# Localiza make_kpi_report.py na raiz ou em reports/
# ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_ROOT="$(cd "$SCRIPT_DIR" && pwd)"
TARGET=""
for CAND in \
  "${PROJ_ROOT}/reports/make_report.py" \
  "${PROJ_ROOT}/reports/make_kpi_report.py" \
  "${PROJ_ROOT}/make_report.py" \
  "${PROJ_ROOT}/make_kpi_report.py"
do
  if [[ -f "${CAND}" ]]; then TARGET="${CAND}"; break; fi
done
if [[ -z "${TARGET}" ]]; then
  echo "❌ Não encontrei make_report.py/make_kpi_report.py em '${PROJ_ROOT}' ou em 'reports/'." >&2
  exit 1
fi

# Aviso se o alvo não estiver em reports/ (pode quebrar BASE_DIR)
if [[ "$(basename "$(dirname "$TARGET")")" != "reports" ]]; then
  echo "⚠️  Alvo encontrado fora de 'reports/': ${TARGET}"
  echo "   Dica: mantenha o script Python em 'reports/' para BASE_DIR correto."
fi

# ──────────────────────────────────────────────────────────────────────
# Coleta de parâmetros (assistente)
# ──────────────────────────────────────────────────────────────────────
echo "===== KPI Wizard (Top10 | Individual) ====="
echo
echo "Escolha o período:"
echo "  1) Mês atual"
echo "  2) Mês anterior"
echo "  3) Escolher um dos últimos 6 meses"
echo "  4) Intervalo personalizado (YYYY-MM-DD a YYYY-MM-DD)"
echo

read -rp "Opção [1-4]: " OPT || true
echo

START=""; END=""
case "${OPT:-}" in
  1)
    YM="$($DBIN +%Y-%m)"
    START="$(ym_first_day "$YM")"
    END="$(ym_last_day "$YM")"
    ;;
  2)
    YM="$($DBIN -d "$($DBIN +%Y-%m-01) -1 month" +%Y-%m)"
    START="$(ym_first_day "$YM")"
    END="$(ym_last_day "$YM")"
    ;;
  3)
    echo "Selecione o mês:"
    declare -a LIST=()
    for i in {0..5}; do
      LIST+=("$($DBIN -d "$($DBIN +%Y-%m-01) -${i} month" +%Y-%m)")
    done
    idx=1
    for ym in "${LIST[@]}"; do
      echo "  $idx) $ym"
      idx=$((idx+1))
    done
    echo
    read -rp "Mês [1-${#LIST[@]}]: " MIDX || true
    if ! [[ "$MIDX" =~ ^[1-9][0-9]*$ ]] || (( MIDX < 1 || MIDX > ${#LIST[@]} )); then
      echo "❌ Opção inválida." >&2; exit 1
    fi
    YM="${LIST[$((MIDX-1))]}"
    START="$(ym_first_day "$YM")"
    END="$(ym_last_day "$YM")"
    ;;
  4)
    read -rp "Data inicial (YYYY-MM-DD): " START || true
    read -rp "Data final   (YYYY-MM-DD): " END   || true
    ;;
  *)
    echo "❌ Opção inválida." >&2; exit 1
    ;;
esac

if ! is_ymd "$START" || ! is_ymd "$END"; then
  echo "❌ Datas inválidas. Use YYYY-MM-DD." >&2
  exit 1
fi

echo
echo "Período escolhido: ${START} a ${END}"
echo "Padrão: Top 10, --min-analises 50, --export-org --export-pdf --add-comments,"
echo "        --r-appendix (Rscript), --include-high-nc (thr=90, min=50),"
echo "        sem --with-impact, sem --reuse-kpi, sem --plan-only."
if confirm "Usar COMANDO PADRÃO agora?" "y"; then
  USE_DEFAULT="y"
else
  USE_DEFAULT="n"
fi

# ──────────────────────────────────────────────────────────────────────
# Coletas adicionais (somente se NÃO usar o padrão)
# ──────────────────────────────────────────────────────────────────────
MODE="top10"
PERITO_NAME=""
MIN_ANALISES="50"
EXPORT_ORG="y"; EXPORT_PDF="y"; ADD_COMMENTS="y"
R_APPENDIX="y"; R_BIN="Rscript"
INCLUDE_HIGH_NC="y"; HIGH_NC_THRESHOLD="90"; HIGH_NC_MIN_TASKS="50"
WITH_IMPACT="n"; IMPACT_ALL_TESTS="n"
REUSE_KPI="n"
PLAN_ONLY="n"

if [[ "${USE_DEFAULT}" == "n" ]]; then
  echo
  echo "Modo:"
  echo "  1) Top 10"
  echo "  2) Individual (um perito)"
  read -rp "Opção [1-2]: " MODO || true
  case "${MODO:-}" in
    1) MODE="top10" ;;
    2) MODE="perito"; PERITO_NAME="$(ask "Nome exato do perito" "")" ;;
    *) echo "❌ Opção inválida." >&2; exit 1 ;;
  esac
  if [[ "$MODE" == "perito" && -z "${PERITO_NAME}" ]]; then
    echo "❌ Informe o nome do perito." >&2; exit 1
  fi

  MIN_ANALISES="$(ask "--min-analises" "50")"

  # Exportações e comentários
  confirm "Exportar ORG?" "y" && EXPORT_ORG="y" || EXPORT_ORG="n"
  confirm "Exportar PDF?" "y" && EXPORT_PDF="y" || EXPORT_PDF="n"
  confirm "Incluir comentários GPT?" "y" && ADD_COMMENTS="y" || ADD_COMMENTS="n"

  # Apêndice R
  confirm "Incluir apêndice R?" "y" && R_APPENDIX="y" || R_APPENDIX="n"
  R_BIN="$(ask "--r-bin" "Rscript")"

  # High-NC extra
  confirm "Incluir seção de alto %NC?" "y" && INCLUDE_HIGH_NC="y" || INCLUDE_HIGH_NC="n"
  HIGH_NC_THRESHOLD="$(ask "--high-nc-threshold" "90")"
  HIGH_NC_MIN_TASKS="$(ask "--high-nc-min-tasks" "50")"

  # Impacto na fila
  confirm "Rodar com Impacto na Fila (--with-impact)?" "n" && WITH_IMPACT="y" || WITH_IMPACT="n"
  if [[ "${WITH_IMPACT}" == "y" ]]; then
    confirm "Usar --impact-all-tests?" "n" && IMPACT_ALL_TESTS="y" || IMPACT_ALL_TESTS="n"
  fi

  # Reuso/assemble
  confirm "Reutilizar saídas já geradas (--reuse-kpi / --assemble-only)?" "n" && REUSE_KPI="y" || REUSE_KPI="n"

  # Dry-run
  confirm "Apenas plano (dry-run) --plan-only?" "n" && PLAN_ONLY="y" || PLAN_ONLY="n"
fi

# ──────────────────────────────────────────────────────────────────────
# Resumo
# ──────────────────────────────────────────────────────────────────────
echo
echo "===== Resumo ====="
echo "Período: ${START} a ${END}"
if [[ "$MODE" == "top10" ]]; then
  echo "Modo: Top 10"
else
  echo "Modo: Individual — Perito='${PERITO_NAME}'"
fi
echo "min-analises: ${MIN_ANALISES}"
echo "Export ORG: ${EXPORT_ORG} / Export PDF: ${EXPORT_PDF} / Comentários: ${ADD_COMMENTS}"
echo "Apêndice R: ${R_APPENDIX} (R bin=${R_BIN})"
echo "High NC: ${INCLUDE_HIGH_NC} (thr=${HIGH_NC_THRESHOLD}, min-tasks=${HIGH_NC_MIN_TASKS})"
echo "With Impact: ${WITH_IMPACT} / Impact all-tests: ${IMPACT_ALL_TESTS}"
echo "Reutilizar KPI: ${REUSE_KPI}"
echo "Dry-run: ${PLAN_ONLY}"
echo

# ──────────────────────────────────────────────────────────────────────
# Montagem do comando (aceita flags extras via "$@")
# ──────────────────────────────────────────────────────────────────────
cmd=( "${PYTHON}" "${TARGET}" --start "${START}" --end "${END}" )
if [[ "$MODE" == "top10" ]]; then
  cmd+=( --top10 )
else
  cmd+=( --perito "${PERITO_NAME}" )
fi
cmd+=( --min-analises "${MIN_ANALISES}" )

# exportações
[[ "${EXPORT_ORG}" == "y" ]] && cmd+=( --export-org )
[[ "${EXPORT_PDF}" == "y" ]] && cmd+=( --export-pdf )
[[ "${ADD_COMMENTS}" == "y" ]] && cmd+=( --add-comments )

# apêndice R
if [[ "${R_APPENDIX}" == "y" ]]; then
  cmd+=( --r-appendix )
else
  cmd+=( --no-r-appendix )
fi
cmd+=( --r-bin "${R_BIN}" )

# high-nc
if [[ "${INCLUDE_HIGH_NC}" == "y" ]]; then
  cmd+=( --include-high-nc )
else
  cmd+=( --no-high-nc )
fi
cmd+=( --high-nc-threshold "${HIGH_NC_THRESHOLD}" --high-nc-min-tasks "${HIGH_NC_MIN_TASKS}" )

# impacto
[[ "${WITH_IMPACT}" == "y" ]] && cmd+=( --with-impact )
[[ "${IMPACT_ALL_TESTS}" == "y" ]] && cmd+=( --impact-all-tests )

# reuse/assemble
[[ "${REUSE_KPI}" == "y" ]] && cmd+=( --reuse-kpi )

# dry-run
[[ "${PLAN_ONLY}" == "y" ]] && cmd+=( --plan-only )

# flags extras passadas ao script (não interativas)
if (( "$#" > 0 )); then
  cmd+=( "$@" )
fi

echo "▶ Executando:"
printf ' %q' "${cmd[@]}"; echo; echo

# Se for --plan-only, salvar plano em log
if [[ "${PLAN_ONLY}" == "y" ]]; then
  safe="top10"
  if [[ "$MODE" == "perito" ]]; then safe="$(sanitize "${PERITO_NAME}")"; fi
  LOG="${PROJ_ROOT}/plan_kpi_${safe}_${START}_${END}.log"
  "${cmd[@]}" | tee "${LOG}"
  exit "${PIPESTATUS[0]}"
else
  exec "${cmd[@]}"
fi

