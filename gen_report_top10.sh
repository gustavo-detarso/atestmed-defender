#!/usr/bin/env bash
# run_make_report.sh — Gerador (Top10 KPI) com layout do impacto
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────
# Python (prioriza venv local)
# ──────────────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────────────
# Helpers (date compat: gdate no macOS, date no Linux)
# ──────────────────────────────────────────────────────────────────────
date_bin() {
  if command -v gdate >/dev/null 2>&1; then echo "gdate"; else echo "date"; fi
}
DBIN="$(date_bin)"

is_ym()  { [[ "$1" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]]; }
is_ymd() { [[ "$1" =~ ^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$ ]]; }

ym_first_day() { echo "$1-01"; }
ym_last_day()  { $DBIN -d "$1-01 +1 month -1 day" +%Y-%m-%d; }

# ──────────────────────────────────────────────────────────────────────
# Raiz do projeto e alvo (make_kpi_report.py)
# ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_ROOT="$(cd "$SCRIPT_DIR/" && pwd)"

TARGET=""
for CAND in \
  "${PROJ_ROOT}/reports/make_kpi_report.py" \
  "${PROJ_ROOT}/make_kpi_report.py"
do
  if [[ -f "${CAND}" ]]; then TARGET="${CAND}"; break; fi
done
if [[ -z "${TARGET}" ]]; then
  echo "❌ Não encontrei make_kpi_report.py em '${PROJ_ROOT}' ou em 'reports/'."
  exit 1
fi

# ──────────────────────────────────────────────────────────────────────
# Menu de período (mesmo layout do impacto)
# ──────────────────────────────────────────────────────────────────────
echo "===== Relatório KPI — Gerador (Top10) ====="
echo
echo "Escolha o período:"
echo "  1) Mês atual"
echo "  2) Mês anterior"
echo "  3) Escolher um dos últimos 6 meses"
echo "  4) Intervalo personalizado (YYYY-MM-DD a YYYY-MM-DD)"
echo

read -rp "Opção [1-4]: " OPT
echo

START=""; END=""
case "${OPT:-}" in
  1)
    YM="$($DBIN +%Y-%m)"
    START="$(ym_first_day "$YM")"
    END="$($DBIN -d "$START +1 month -1 day" +%Y-%m-%d)"
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
    read -rp "Mês [1-${#LIST[@]}]: " MIDX
    if ! [[ "$MIDX" =~ ^[1-9][0-9]*$ ]] || (( MIDX < 1 || MIDX > ${#LIST[@]} )); then
      echo "❌ Opção inválida."; exit 1
    fi
    YM="${LIST[$((MIDX-1))]}"
    START="$(ym_first_day "$YM")"
    END="$(ym_last_day "$YM")"
    ;;
  4)
    read -rp "Data inicial (YYYY-MM-DD): " START
    read -rp "Data final   (YYYY-MM-DD): " END
    if ! is_ymd "$START" || ! is_ymd "$END"; then
      echo "❌ Datas inválidas. Use YYYY-MM-DD."; exit 1
    fi
    if [[ "$($DBIN -d "$START" +%s)" -gt "$($DBIN -d "$END" +%s)" ]]; then
      echo "❌ Data inicial > final."; exit 1
    fi
    ;;
  *)
    echo "❌ Opção inválida."; exit 1
    ;;
esac

PERIODO="${START}_a_${END}"
echo "Período selecionado: $START a $END"
echo

# ──────────────────────────────────────────────────────────────────────
# Parâmetros (com defaults e possibilidade de Enter para manter)
# ──────────────────────────────────────────────────────────────────────
DEF_TOPN="${TOPN:-10}"
DEF_MIN_ANALISES="${MIN_ANALIS_

