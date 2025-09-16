#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# kpi_wizard.sh
# Launcher para reports/make_kpi_report_fluxo_b.py com presets e grupos.
# Preset "default" replica o comando completo solicitado.
# ------------------------------------------------------------------------------

# Raiz do projeto (um nível acima deste script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PY_SCRIPT="${KPI_SCRIPT:-$ROOT_DIR/reports/make_kpi_report_fluxo_b.py}"

# Defaults
DB_DEFAULT="${KPI_DB:-$ROOT_DIR/db/atestmed.db}"
START_DEFAULT=""
END_DEFAULT=""
DRY_RUN=0
EXTRA_ARGS=""
PDF_ENGINE="emacs"

# --------------------- Grupos (opções inteligentes) ---------------------------
GROUP_fluxo=(
  --fluxo-b --fluxo-b-all --mark-fluxo-b
)

GROUP_impacto=(
  --with-impact --impact-mode combined --with-impact-lorenz
)

GROUP_graficos=(
  --with-pareto --with-robust-stats --annotate-figures
)

GROUP_dists=(
  --with-dist-dr --with-dist-uf
)

GROUP_cenarios=(
  --scenarios-follow-melhorias --scenarios-topk 10
  --scenarios-reductions 0.5,0.7,1.0 --scenarios-labels A,B,C
)

# ATENÇÃO: este grupo NÃO ativa --embed-tables,
# apenas leva o tamanho de fonte/linhas e longtable.
GROUP_tabelas_noembed=(
  --embed-rows 60
  --table-use-longtable --table-font-size scriptsize
)

# Mantido para quem quiser tabelas embutidas explicitamente (preset custom).
GROUP_tabelas=(
  --embed-tables --embed-rows 60
  --table-use-longtable --table-font-size scriptsize
)

GROUP_landscape=(
  --landscape-main --landscape-plan --landscape-appendix
)

GROUP_export=(
  --export-org --export-pdf --pdf-engine "$PDF_ENGINE"
  --emit-classic-figs --with-files-section --zip-bundle
)

GROUP_fontes=(
  --prepend-org-file misc/melhorias.org
  --propostas-from-file misc/melhorias.org
)

GROUP_cache=(
  --use-cache --seed 42
)

GROUP_cr_analises=(
  --cr-mode analises
)

GROUP_debug=(
  --debug-ai
)

# --------------------------- Ajuda / uso --------------------------------------
usage() {
  cat <<'USAGE'
Uso:
  ./kpi_wizard.sh [preset] [opções]

Presets:
  default     -> comando padrão solicitado (fluxo, impacto combined + Lorenz, Pareto, robust-stats,
                 dist-DR/UF, cenários A/B/C, sem --embed-tables, longtable+scriptsize, arquivos/ZIP,
                 legendas anotadas, cr=analises, debug-ai)
  all         -> default + landscape nas tabelas (main/plan/appendix) e cache (seed=42)
  light       -> igual ao default (sem mudanças estruturais)
  custom      -> monte por grupos com --groups fluxo,impacto,graficos,dists,cenarios,
                 tabelas_noembed,tabelas,landscape,export,fontes,cache,cr_analises,debug

Opções principais:
  --db PATH                 Caminho do SQLite (default: db/atestmed.db)
  --start YYYY-MM-DD        Data inicial
  --end YYYY-MM-DD          Data final
  --semester YYYYH1|YYYYH2  Preenche start/end automaticamente para o semestre
  --pdf-engine emacs|pandoc Seleciona engine de PDF (default: emacs)
  --groups g1,g2,...        Ativa grupos (para preset custom)
  --extra "..."             Acrescenta flags extras ao final (ex.: "--no-graphs")
  --dry-run                 Apenas imprime o comando, não executa
  -h | --help               Mostra esta ajuda

Exemplos:
  ./kpi_wizard.sh --semester 2025H1
  ./kpi_wizard.sh all --start 2025-01-01 --end 2025-06-30
  ./kpi_wizard.sh custom --groups fluxo,impacto,graficos,export,fontes,cr_analises,debug --semester 2025H1
USAGE
}

# ------------------------ Utilitários de data ---------------------------------
semester_to_dates() {
  # $1 = YYYYH1 | YYYYH2
  local s="$1"
  if [[ ! "$s" =~ ^([0-9]{4})H([12])$ ]]; then
    echo "ERROR: --semester deve ser YYYYH1 ou YYYYH2" >&2
    return 1
  fi
  local year="${BASH_REMATCH[1]}"
  local half="${BASH_REMATCH[2]}"
  if [[ "$half" == "1" ]]; then
    echo "${year}-01-01|${year}-06-30"
  else
    echo "${year}-07-01|${year}-12-31"
  fi
}

# ----------------------------- Parse args -------------------------------------
PRESET="default"
GROUPS_CSV=""
DB="$DB_DEFAULT"
START="$START_DEFAULT"
END="$END_DEFAULT"
SEMESTER=""

while (( $# )); do
  case "${1:-}" in
    default|all|light|custom)
      PRESET="$1"; shift ;;
    --db) DB="$2"; shift 2 ;;
    --start) START="$2"; shift 2 ;;
    --end) END="$2"; shift 2 ;;
    --semester) SEMESTER="$2"; shift 2 ;;
    --pdf-engine) PDF_ENGINE="$2"; shift 2 ;;
    --groups) GROUPS_CSV="$2"; shift 2 ;;
    --extra) EXTRA_ARGS="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; EXTRA_ARGS="$*"; break ;;
    *) echo "Argumento desconhecido: $1" >&2; usage; exit 1 ;;
  esac
done

# Atualiza engine também dentro do grupo export (se mudaram via flag)
GROUP_export=( --export-org --export-pdf --pdf-engine "$PDF_ENGINE" --emit-classic-figs --with-files-section --zip-bundle )

# Datas via --semester (se fornecido)
if [[ -n "$SEMESTER" ]]; then
  se="$(semester_to_dates "$SEMESTER")" || exit 1
  START="${se%%|*}"; END="${se##*|}"
fi

# Validação básica
if [[ -z "$START" || -z "$END" ]]; then
  echo "ERRO: informe --start/--end ou --semester." >&2
  usage; exit 2
fi
if [[ ! -f "$PY_SCRIPT" ]]; then
  echo "ERRO: script Python não encontrado em: $PY_SCRIPT" >&2
  exit 3
fi
if [[ ! -f "$DB" ]]; then
  echo "AVISO: DB não encontrado em '$DB' (continuando mesmo assim…)" >&2
fi

# -------------------------- Monta o comando -----------------------------------
build_cmd() {
  local -a args_base=(
    "$PY_SCRIPT"
    --db "$DB"
    --start "$START" --end "$END"
  )

  local -a args_groups=()

  case "$PRESET" in
    default)
      # Exatamente o comando solicitado (datas vindas das flags)
      args_groups+=("${GROUP_fluxo[@]}")
      args_groups+=("${GROUP_impacto[@]}")
      args_groups+=("${GROUP_graficos[@]}")
      args_groups+=("${GROUP_dists[@]}")
      args_groups+=("${GROUP_cenarios[@]}")
      args_groups+=("${GROUP_tabelas_noembed[@]}")
      args_groups+=("${GROUP_export[@]}")
      args_groups+=("${GROUP_fontes[@]}")
      args_groups+=("${GROUP_cr_analises[@]}")
      args_groups+=("${GROUP_debug[@]}")
      ;;
    all)
      args_groups+=("${GROUP_fluxo[@]}")
      args_groups+=("${GROUP_impacto[@]}")
      args_groups+=("${GROUP_graficos[@]}")
      args_groups+=("${GROUP_dists[@]}")
      args_groups+=("${GROUP_cenarios[@]}")
      args_groups+=("${GROUP_tabelas_noembed[@]}")  # mantém SEM --embed-tables
      args_groups+=("${GROUP_landscape[@]}")        # extra no "all"
      args_groups+=("${GROUP_export[@]}")
      args_groups+=("${GROUP_fontes[@]}")
      args_groups+=("${GROUP_cache[@]}")
      args_groups+=("${GROUP_cr_analises[@]}")
      args_groups+=("${GROUP_debug[@]}")
      ;;
    light)
      args_groups+=("${GROUP_fluxo[@]}")
      args_groups+=("${GROUP_impacto[@]}")
      args_groups+=("${GROUP_graficos[@]}")
      args_groups+=("${GROUP_dists[@]}")
      args_groups+=("${GROUP_cenarios[@]}")
      args_groups+=("${GROUP_tabelas_noembed[@]}")  # sem --embed-tables
      args_groups+=("${GROUP_export[@]}")
      args_groups+=("${GROUP_fontes[@]}")
      args_groups+=("${GROUP_cr_analises[@]}")
      args_groups+=("${GROUP_debug[@]}")
      ;;
    custom)
      if [[ -z "$GROUPS_CSV" ]]; then
        echo "ERRO: preset custom requer --groups g1,g2,..." >&2
        exit 4
      fi
      IFS=',' read -r -a sel <<< "$GROUPS_CSV"
      for g in "${sel[@]}"; do
        case "$g" in
          fluxo)            args_groups+=("${GROUP_fluxo[@]}");;
          impacto)          args_groups+=("${GROUP_impacto[@]}");;
          graficos)         args_groups+=("${GROUP_graficos[@]}");;
          dists)            args_groups+=("${GROUP_dists[@]}");;
          cenarios)         args_groups+=("${GROUP_cenarios[@]}");;
          tabelas_noembed)  args_groups+=("${GROUP_tabelas_noembed[@]}");;
          tabelas)          args_groups+=("${GROUP_tabelas[@]}");;
          landscape)        args_groups+=("${GROUP_landscape[@]}");;
          export)           args_groups+=("${GROUP_export[@]}");;
          fontes)           args_groups+=("${GROUP_fontes[@]}");;
          cache)            args_groups+=("${GROUP_cache[@]}");;
          cr_analises)      args_groups+=("${GROUP_cr_analises[@]}");;
          debug)            args_groups+=("${GROUP_debug[@]}");;
          *) echo "Grupo desconhecido: $g" >&2; exit 5;;
        esac
      done
      ;;
    *)
      echo "Preset desconhecido: $PRESET" >&2; usage; exit 6 ;;
  esac

  # Comando final
  local -a CMD=( python3 "${args_base[@]}" "${args_groups[@]}" )
  if [[ -n "$EXTRA_ARGS" ]]; then
    # shellcheck disable=SC2206
    EXTRA_ARR=( $EXTRA_ARGS )
    CMD+=( "${EXTRA_ARR[@]}" )
  fi

  printf '%q ' "${CMD[@]}"
}

FINAL_CMD="$(build_cmd)"

# ------------------------------ Execução --------------------------------------
echo "⇒ Executando:"
echo "  $FINAL_CMD"
echo

if (( DRY_RUN )); then
  echo "(dry-run) Nada foi executado."
  exit 0
fi

# shellcheck disable=SC2090
eval "$FINAL_CMD"

