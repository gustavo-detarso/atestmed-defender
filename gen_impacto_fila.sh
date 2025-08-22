#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

# usa gdate no macOS, date no Linux
date_bin() {
  if command -v gdate >/dev/null 2>&1; then echo "gdate"; else echo "date"; fi
}
DBIN="$(date_bin)"

# valida YYYY-MM ou YYYY-MM-DD
is_ym()  { [[ "$1" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]]; }
is_ymd() { [[ "$1" =~ ^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$ ]]; }

ym_first_day() { echo "$1-01"; }
ym_last_day()  { $DBIN -d "$1-01 +1 month -1 day" +%Y-%m-%d; }

# resolve diretório do projeto (raiz do repo)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Caminho do script Python
PY="${PROJ_ROOT}/reports/make_impact_report.py"
if [[ ! -f "$PY" ]]; then
  echo "❌ Não encontrei: $PY"
  echo "   Ajuste a variável PY no script, se necessário."
  exit 1
fi

# ──────────────────────────────────────────────────────────────────────
# Menu de período
# ──────────────────────────────────────────────────────────────────────

echo "===== Impacto na Fila — Gerador (Top10) ====="
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
    # valida ordem
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
# Parâmetros adicionais
# ──────────────────────────────────────────────────────────────────────
# Você pode alterar o estrato padrão (cr/dr/uo) definindo a variável BY antes de chamar o script:
#   BY=dr ./gen_impacto_fila.sh
BY_DEFAULT="${BY:-cr}"

# Flags fixas do comando (iguais ao que você forneceu)
CMD=( python3 "$PY"
  --start "$START" --end "$END"
  --top10
  --export-png --export-org --add-comments --export-comment-org
  --all-tests --permute-stratify --by "$BY_DEFAULT"
  --final-org --export-pdf --ship-outputs
)

# Permite acrescentar flags livres após o script, ex:
#   ./gen_impacto_fila.sh --no-sstar --min-analises 30
EXTRA_ARGS=( "$@" )
if ((${#EXTRA_ARGS[@]})); then
  echo "Args extras detectados: ${EXTRA_ARGS[*]}"
fi

echo "Comando:"
echo "  ${CMD[*]} ${EXTRA_ARGS[*]}"
echo
read -rp "Prosseguir? [S/n] " OK
OK="${OK:-S}"
if [[ "$OK" =~ ^[Nn]$ ]]; then
  echo "Abortado."; exit 0
fi

# ──────────────────────────────────────────────────────────────────────
# Execução + log
# ──────────────────────────────────────────────────────────────────────
cd "$PROJ_ROOT"

LOG_DIR="${PROJ_ROOT}/reports/outputs/${PERIODO}/impacto_fila/logs"
mkdir -p "$LOG_DIR"
TS="$($DBIN +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/run_${TS}.log"

# roda e duplica saída pro log
set -x
"${CMD[@]}" "${EXTRA_ARGS[@]}" |& tee "$LOG_FILE"
set +x

echo
echo "📝 Log salvo em: $LOG_FILE"
echo "✅ Concluído."

