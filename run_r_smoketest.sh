#!/usr/bin/env bash
set -u
shopt -s nullglob

DB="${DB:-db/atestmed.db}"
START="${START:-2025-01-01}"
END="${END:-2025-01-21}"
OUT="${OUT:-graphs_and_tables/exports}"
PERITO="${PERITO:-ALEXANDRE FELIPE FRANCA}"
PERITOS_CSV="${PERITOS_CSV:-}"
LE_THRESH="${LE_THRESH:-15}"
PROD_THRESH="${PROD_THRESH:-50}"
RCHECK_DIR="${RCHECK_DIR:-r_checks}"

LOG_DIR="${LOG_DIR:-smoke_logs}"
mkdir -p "$OUT" "$LOG_DIR"

# profile temporário pra imprimir traceback e sair com status≠0
RPROF="$LOG_DIR/.rprofile_smoke.R"
cat > "$RPROF" <<'RPROF'
options(error=function(e){
  cat("\n--- TRACEBACK ---\n", file=stderr())
  traceback(2)
  q(status=10, save="no")
})
RPROF

pass=(); fail=()

run_one() {
  local f="$1"; shift
  local base="$(basename "$f")"
  local log="$LOG_DIR/${base%.R}.log"

  echo "[R] $base $*" | tee "$log"
  # 2>&1 pra capturar STDERR também
  R_PROFILE_USER="$RPROF" LC_ALL=C.UTF-8 Rscript "$f" "$@" >>"$log" 2>&1
  local rc=$?

  if [[ $rc -eq 0 ]]; then
    pass+=("$base")
  else
    fail+=("$base")
  fi
  return $rc
}

for f in "$RCHECK_DIR"/*.R; do
  base="$(basename "$f")"

  # pule arquivos utilitários que não são executáveis como CLI
  case "$base" in
    utils.R) echo "[skip] $base (utilitário, não-CLI)"; continue ;;
  esac

  args=( --db "$DB" --start "$START" --end "$END" --out-dir "$OUT" )
  case "$base" in
    01_nc_rate_check.R|g01_top10_nc_rate_check.R)           args+=( --perito "$PERITO" ) ;;
    02_le15s_check.R|g02_top10_le15s_check.R)               args+=( --perito "$PERITO" --threshold "$LE_THRESH" ) ;;
    03_productivity_check.R|g03_top10_productivity_check.R) args+=( --perito "$PERITO" --threshold "$PROD_THRESH" ) ;;
    04_overlap_check.R|g04_top10_overlap_check.R)           args+=( --perito "$PERITO" ); [ -n "$PERITOS_CSV" ] && args+=( --scope-csv "$PERITOS_CSV" ) ;;
    05_motivos_chisq.R|g05_top10_motivos_chisq.R)           args+=( --perito "$PERITO" ) ;;
    06_composite_robustness.R|g06_top10_composite_robustness.R) args+=( --perito "$PERITO" ) ;;
    07_kpi_icra_iatd_score.R|g07_top10_kpi_icra_iatd_score.R)   args+=( --perito "$PERITO" ) ;;
    08_weighted_props.R)
      run_one "$f" "${args[@]}" --perito "$PERITO" --measure nc  || true
      run_one "$f" "${args[@]}" --perito "$PERITO" --measure le --threshold "$LE_THRESH" || true
      continue
      ;;
    _common.R|_ensure_deps.R|test_common_db.R)
      # Podem rodar "em branco", mas deixamos passar sem ruído
      run_one "$f" || true
      continue
      ;;
    *)
      args+=( --perito "$PERITO" )
      ;;
  esac

  run_one "$f" "${args[@]}" || true
done

echo
echo "==================== RESUMO ===================="
echo "✓ PASSARAM (${#pass[@]}):"
for x in "${pass[@]}"; do echo "  - $x"; done
echo
echo "✗ FALHARAM (${#fail[@]}):"
for x in "${fail[@]}"; do
  echo "  - $x"
  log="$LOG_DIR/${x%.R}.log"
  if [[ -f "$log" ]]; then
    # primeira linha 'Error in' e o rodapé do log
    errline="$(grep -m1 -n '^Error in ' "$log" || true)"
    [[ -n "$errline" ]] && echo "    • Primeira falha: $(echo "$errline" | sed 's/:/ (linha ) → /')"
    echo "    • Fim do log:"
    tail -n 25 "$log" | sed 's/^/      /'
  else
    echo "    • (sem log?)"
  fi
done

# Retorna 1 se houver falhas
[[ ${#fail[@]} -eq 0 ]]

