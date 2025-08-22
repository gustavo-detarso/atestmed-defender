#!/usr/bin/env bash
# run_make_report_perito.sh — executa make_report.py para um perito específico
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
TARGET=""
for CAND in "${ROOT}/make_report.py" "${ROOT}/reports/make_report.py"; do
  if [[ -f "${CAND}" ]]; then TARGET="${CAND}"; break; fi
done
if [[ -z "${TARGET}" ]]; then
  echo "❌ Não encontrei make_report.py na raiz nem em reports/."
  exit 1
fi

# ── Helpers ───────────────────────────────────────────────────────────────────
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

read_nonempty() {
  local __outvar="$1"
  local __prompt="$2"
  local __in=""
  while true; do
    read -r -p "${__prompt}" __in
    # trim
    __in="${__in#"${__in%%[![:space:]]*}"}"
    __in="${__in%"${__in##*[![:space:]]}"}"
    if [[ -z "${__in}" ]]; then
      echo "Valor obrigatório."
      continue
    fi
    printf -v "${__outvar}" '%s' "${__in}"
    break
  done
}

sanitize() {
  # troca espaços por _, remove acentos e filtra para A-Za-z0-9_-
  local s="$*"
  # troca espaços por underscores
  s="${s// /_}"
  # remove caracteres não-ASCII “simples”
  s="$(printf '%s' "$s" | iconv -f UTF-8 -t ASCII//TRANSLIT 2>/dev/null || printf '%s' "$s")"
  # mantém apenas A-Za-z0-9_- (troca o resto por _)
  s="$(printf '%s' "$s" | sed -E 's/[^A-Za-z0-9_-]+/_/g')"
  # remove underscores duplicados
  s="$(printf '%s' "$s" | sed -E 's/_+/_/g; s/^_+//; s/_+$//')"
  printf '%s' "$s"
}

# ── Entrada ───────────────────────────────────────────────────────────────────
START=""; END=""; PERITO=""
read_date START  "Informe --start (YYYY-MM-DD): "
read_date END    "Informe --end   (YYYY-MM-DD): "
read_nonempty PERITO 'Informe --perito (nome exato): '

# valida ordem
s_epoch="$(date -d "${START}" +%s)"
e_epoch="$(date -d "${END}" +%s)"
if (( s_epoch > e_epoch )); then
  echo "❌ --start (${START}) não pode ser depois de --end (${END})."
  exit 1
fi

# ── Monta comando base (aceita flags extras via "$@") ────────────────────────
cmd=(
  "${PYTHON}" "${TARGET}"
  --start "${START}" --end "${END}"
  --perito "${PERITO}"
  --add-comments
  --export-org
  --export-pdf
  --r-appendix
  "$@"
)

echo "▶ Executando: ${cmd[*]}"

# ── Suporte a --plan-only com log ────────────────────────────────────────────
PLAN_ONLY=0
for arg in "$@"; do
  [[ "${arg}" == "--plan-only" ]] && PLAN_ONLY=1
done

if (( PLAN_ONLY == 1 )); then
  SAFE_PERITO="$(sanitize "${PERITO}")"
  LOG="${ROOT}/plan_individual_${SAFE_PERITO}_${START}_${END}.log"
  "${cmd[@]}" | tee "${LOG}"
  exit "${PIPESTATUS[0]}"
else
  exec "${cmd[@]}"
fi

