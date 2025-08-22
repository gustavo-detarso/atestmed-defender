#!/usr/bin/env bash
# run_impacto_fila.sh — pede --start e --end (YYYY-MM-DD) e executa impacto_fila.py
set -euo pipefail

# Descobre Python
PYTHON="$(command -v python3 || command -v python || true)"
if [[ -z "${PYTHON}" ]]; then
  echo "❌ Python não encontrado no PATH."
  exit 1
fi

# Caminhos (assuma este script salvo na raiz do projeto)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="${ROOT}/graphs_and_tables/impacto_fila.py"
if [[ ! -f "${TARGET}" ]]; then
  echo "❌ Não encontrei ${TARGET}."
  exit 1
fi

read_date() {
  local __outvar="$1"
  local __prompt="$2"
  local __in=""
  local __norm=""
  while true; do
    read -rp "${__prompt}" __in
    # Validação de formato YYYY-MM-DD
    if [[ ! "${__in}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "Formato inválido. Use YYYY-MM-DD."
      continue
    fi
    # Validação de data existente
    if ! __norm="$(date -d "${__in}" +%F 2>/dev/null)"; then
      echo "Data inválida. Tente novamente."
      continue
    fi
    printf -v "${__outvar}" '%s' "${__norm}"
    break
  done
}

# Perguntas
START=""; END=""
read_date START "Informe --start (YYYY-MM-DD): "
read_date END   "Informe --end   (YYYY-MM-DD): "

# Ordem cronológica
s_epoch="$(date -d "${START}" +%s)"
e_epoch="$(date -d "${END}" +%s)"
if (( s_epoch > e_epoch )); then
  echo "❌ --start (${START}) não pode ser depois de --end (${END})."
  exit 1
fi

# Comando
cmd=(
  "${PYTHON}" "${TARGET}"
  --start "${START}" --end "${END}"
  --top10 --alpha 0.8 --pbr 0.175
  --all-tests --add-comments
  --export-png --export-org --final-org --export-pdf
  --ship-outputs
)

echo "▶ Executando: ${cmd[*]}"
exec "${cmd[@]}"

