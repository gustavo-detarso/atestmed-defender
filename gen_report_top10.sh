#!/usr/bin/env bash
# run_make_report.sh — executa make_report.py para Top10 com opções padrão
set -euo pipefail

# ── Python (prioriza venv local) ──────────────────────────────────────────────
if [[ -x "./.venv/bin/python" ]]; then
  PYTHON="./.venv/bin/python"
elif compgen -G "${HOME}/.local/share/virtualenvs/atestmed-defender-*/bin/python3" >/dev/null; then
  PYTHON="$(ls -1 ${HOME}/.local/share/virtualenvs/atestmed-defender-*/bin/python3 | head -n1)"
else
  PYTHON="$(command -v python3 || command -v python || true)"
fi
if [[ -z "${PYTHON}" ]]; then
  echo "❌ Python não encontrado no PATH."
  exit 1
fi

# ── Caminhos ──────────────────────────────────────────────────────────────────
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# tenta na raiz e em reports/
TARGET=""
for CAND in "${ROOT}/make_report.py" "${ROOT}/reports/make_report.py"; do
  if [[ -f "${CAND}" ]]; then TARGET="${CAND}"; break; fi
done
if [[ -z "${TARGET}" ]]; then
  echo "❌ Não encontrei make_report.py na raiz nem em reports/."
  exit 1
fi

# ── Utilitário para ler datas ────────────────────────────────────────────────
read_date() {
  local __outvar="$1"
  local __prompt="$2"
  local __in="" __norm=""
  while true; do
    read -rp "${__prompt}" __in
    if [[ ! "${__in}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "Formato inválido. Use YYYY-MM-DD."
      continue
    fi
    if ! __norm="$(date -d "${__in}" +%F 2>/dev/null)"; then
      echo "Data inválida. Tente novamente."
      continue
    fi
    printf -v "${__outvar}" '%s' "${__norm}"
    break
  done
}

# ── Entrada do usuário ───────────────────────────────────────────────────────
START=""; END=""
read_date START "Informe --start (YYYY-MM-DD): "
read_date END   "Informe --end   (YYYY-MM-DD): "

# valida ordem
s_epoch="$(date -d "${START}" +%s)"
e_epoch="$(date -d "${END}" +%s)"
if (( s_epoch > e_epoch )); then
  echo "❌ --start (${START}) não pode ser depois de --end (${END})."
  exit 1
fi

# padrão para min_analises (permite override via env)
MIN_ANALISES="${MIN_ANALISES:-50}"

# ── Monta comando base (aceita flags extras via "$@") ────────────────────────
cmd=(
  "${PYTHON}" "${TARGET}"
  --top10
  --start "${START}" --end "${END}"
  --min-analises "${MIN_ANALISES}"
  --add-comments
  --export-org
  --export-pdf
  --r-appendix
  "$@"
)

echo "▶ Executando: ${cmd[*]}"

# Se foi pedido --plan-only, salvamos o plano em log
PLAN_ONLY=0
for arg in "$@"; do
  [[ "${arg}" == "--plan-only" ]] && PLAN_ONLY=1
done

if (( PLAN_ONLY == 1 )); then
  LOG="${ROOT}/plan_top10_${START}_${END}.log"
  # pipefail já está ativo; preserva exit code do make_report
  "${cmd[@]}" | tee "${LOG}"
  exit "${PIPESTATUS[0]}"
else
  exec "${cmd[@]}"
fi

