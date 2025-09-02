#!/usr/bin/env bash
set -euo pipefail

# ==========================================
# Helpers (ask, confirm, datas, meses)
# ==========================================
ask() {
  local prompt="$1"; shift || true
  local default="${1:-}"
  local var
  if [[ -n "${default}" ]]; then
    read -r -p "${prompt} [${default}]: " var || true
    echo "${var:-$default}"
  else
    read -r -p "${prompt}: " var || true
    echo "${var}"
  fi
}

confirm() {
  local prompt="$1"; shift || true
  local default="${1:-y}"
  local ans; ans=$(ask "${prompt}" "${default}")
  case "${ans,,}" in
    y|yes|s|sim|"") return 0 ;;
    *) return 1 ;;
  esac
}

ask_date() {
  local prompt="$1"; shift || true
  local default="${1:-}"
  local val norm
  while true; do
    if [[ -n "$default" ]]; then
      read -r -p "${prompt} [${default}]: " val || true
      val="${val:-$default}"
    else
      read -r -p "${prompt}: " val || true
    fi
    if [[ -z "$val" ]]; then
      echo "Campo obrigatório." >&2
      continue
    fi
    if ! norm=$(date -d "$val" +%F 2>/dev/null); then
      echo "Data inválida. Use o formato YYYY-MM-DD." >&2
      continue
    fi
    echo "$norm"
    return 0
  done
}

date_bin() { if command -v gdate >/dev/null 2>&1; then echo "gdate"; else echo "date"; fi; }
DBIN="$(date_bin)"
ym_first_day() { echo "$1-01"; }
ym_last_day()  { $DBIN -d "$1-01 +1 month -1 day" +%Y-%m-%d; }

# ==========================================
# Defaults (podem ser alterados na personalização)
# ==========================================
START=""; END=""
MODE="top10"                # top10 | perito
PERITO_NAME=""

MIN_ANALISES="50"
TOPN="10"
ALPHA="0.8"
PBR=""                      # vazio = auto

SELECT_SRC="both"           # impact | kpi | both
KPI_MIN="50"

# Outliers por %NC (apêndice e regras)
APPENDIX_NC_OUTLIERS="y"
APPENDIX_NC_EXPLAIN="y"
NC_OUTLIER_MODE="adaptive-fdr"     # off | fixed | adaptive-fdr
NC_OUTLIER_THRESH="0.90"           # usado apenas se fixed
NC_OUTLIER_MIN_N="50"
NC_OUTLIER_FDR="0.05"
NC_OUTLIER_GRID="0.60,0.70,0.80,0.85,0.90,0.95"
NC_OUTLIER_ADD_TO="both"           # quando NÃO houver apêndice

# Testes / Sensibilidade / Estratos
ALL_TESTS="y"
TEST_BINOMIAL=""
BETABIN=""
PERMUTE_WEIGHT="5000"
PERMUTE_STRATIFY="y"
CMH_BY="by=cr"
PSA="10000"
PSA_ALPHA_STRENGTH="50"
SENS_PLOT="y"
SENS_ALPHA_FRAC="0.10"
SENS_PBR_PP="0.01"
BY="cr"                     # cr | dr | vazio

# Exportação
EXPORT_PNG="y"
EXPORT_PDF="y"

# Header/front (org único)
HEADER_AND_TEXT="y"
HEADER_AND_TEXT_FILE="/home/gustavodetarso/Documentos/.share/header_mps_org/header_and_text.org"

# Alternativas de front
FRONT_PDF=()
FRONT_ORG_RENDER=()
HEADER_ORG=""
FRONT_ORG_TEXT=""

# Aparência de gráficos e PDF
FIG_SCALE="1.0"
FIG_DPI="300"
PDF_IMG_FRAC="0.75"

# Margens da página (cm)
PAGE_MARGIN=""
PAGE_MARGIN_LEFT=""
PAGE_MARGIN_RIGHT=""
PAGE_MARGIN_TOP=""
PAGE_MARGIN_BOTTOM=""

# Tabelas
TABLE_FONT_SIZE="7"
TABLE_HEADER_FONT_SIZE="7"

# TMEA / Capacidade (opcional)
TMEA_BR="60.0"
CAP_BR=""
ATT_BR=""

# GPT comments
GPT_COMMENTS="y"

# ==========================================
# Cabeçalho + Escolha do período (mesma sequência do KPI Wizard)
# ==========================================
echo "===== Impacto na Fila — Wizard (Top10 | Individual) ====="
echo
echo "Escolha o período:"
echo "  1) Mês atual"
echo "  2) Mês anterior"
echo "  3) Escolher um dos últimos 6 meses"
echo "  4) Intervalo personalizado (YYYY-MM-DD a YYYY-MM-DD)"
echo

read -r -p "Opção [1-4]: " OPT || true
echo

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
    read -r -p "Mês [1-${#LIST[@]}]: " MIDX || true
    if ! [[ "$MIDX" =~ ^[1-9][0-9]*$ ]] || (( MIDX < 1 || MIDX > ${#LIST[@]} )); then
      echo "❌ Opção inválida." >&2; exit 1
    fi
    YM="${LIST[$((MIDX-1))]}"
    START="$(ym_first_day "$YM")"
    END="$(ym_last_day "$YM")"
    ;;
  4)
    START=$(ask_date "Data inicial (YYYY-MM-DD)")
    END=$(ask_date   "Data final   (YYYY-MM-DD)")
    ;;
  *)
    echo "❌ Opção inválida." >&2; exit 1
    ;;
esac

# Garante END >= START
while true; do
  if (( $(date -d "$END" +%s) < $(date -d "$START" +%s) )); then
    echo "A data final deve ser maior ou igual à data inicial." >&2
    END=$(ask_date "Data final (YYYY-MM-DD)")
  else
    break
  fi
done

echo
echo "Período escolhido: ${START} a ${END}"
echo "Padrão: Top 10, --min-analises 50, --alpha 0.8,"
echo "        --select-src both (--kpi-min-analises 50),"
echo "        --appendix-nc-outliers --appendix-nc-explain,"
echo "        --nc-outlier-mode adaptive-fdr (grid e FDR padrão),"
echo "        --all-tests (inclui permute 5000, CMH by=cr, PSA 10000), --sens-plot,"
echo "        --by cr, --export-png --export-pdf,"
echo "        --header-and-text (arquivo padrão), --gpt-comments."
if confirm "Usar COMANDO PADRÃO agora?" "y"; then
  USE_DEFAULT="y"
else
  USE_DEFAULT="n"
fi

# ==========================================
# Personalização guiada (se escolhida)
# ==========================================
if [[ "$USE_DEFAULT" == "n" ]]; then
  # Você pode reescrever as datas
  START=$(ask "Data inicial --start" "$START")
  END=$(ask "Data final --end" "$END")
  while true; do
    if (( $(date -d "$END" +%s) < $(date -d "$START" +%s) )); then
      echo "A data final deve ser maior ou igual à data inicial." >&2
      END=$(ask "Data final --end" "$END")
    else
      break
    fi
  done

  # Modo
  if confirm "Modo TOP-10? (se 'não', será perito único)" "y"; then
    MODE="top10"
  else
    MODE="perito"
    PERITO_NAME="$(ask "Nome EXATO do perito (--perito)")"
  fi

  MIN_ANALISES=$(ask "--min-analises (corte de N)" "$MIN_ANALISES")
  TOPN=$(ask "--topn (quantos listar)" "$TOPN")
  ALPHA=$(ask "--alpha" "$ALPHA")
  PBR=$(ask "--pbr (vazio = auto)" "$PBR")

  # Fonte de seleção
  SELECT_SRC=$(ask "--select-src (impact|kpi|both)" "$SELECT_SRC")
  KPI_MIN=$(ask "--kpi-min-analises" "$KPI_MIN")

  # Estratos
  BY=$(ask "--by (cr|dr|vazio)" "$BY")

  # Sensibilidade
  if confirm "Fazer gráfico de sensibilidade? (--sens-plot)" "$SENS_PLOT"; then
    SENS_PLOT="y"
    SENS_ALPHA_FRAC=$(ask "--sens-alpha-frac" "$SENS_ALPHA_FRAC")
    SENS_PBR_PP=$(ask "--sens-pbr-pp" "$SENS_PBR_PP")
  else
    SENS_PLOT="n"
  fi

  # Testes
  if confirm "Atalho --all-tests (recomendado)?" "$ALL_TESTS"; then
    ALL_TESTS="y"
  else
    ALL_TESTS="n"
    if confirm "Incluir binomial?" "y"; then TEST_BINOMIAL="y"; fi
    if confirm "Incluir beta-binomial?" "y"; then BETABIN="y"; fi
    PERMUTE_WEIGHT=$(ask "--permute-weight (0 para desativar)" "$PERMUTE_WEIGHT")
    if confirm "Estratificar permutação? (--permute-stratify)" "$PERMUTE_STRATIFY"; then
      PERMUTE_STRATIFY="y"
    else
      PERMUTE_STRATIFY="n"
    fi
    CMH_BY=$(ask "--cmh (ex.: by=cr, vazio p/ desativar)" "$CMH_BY")
    PSA=$(ask "--psa (0 para desativar)" "$PSA")
    PSA_ALPHA_STRENGTH=$(ask "--psa-alpha-strength" "$PSA_ALPHA_STRENGTH")
  fi

  # Outliers por %NC / Apêndice
  if confirm "Gerar apêndice de outliers por %NC alto? (--appendix-nc-outliers)" "$APPENDIX_NC_OUTLIERS"; then
    APPENDIX_NC_OUTLIERS="y"
    if confirm "Incluir explicação GPT de cálculos/fórmulas? (--appendix-nc-explain)" "$APPENDIX_NC_EXPLAIN"; then
      APPENDIX_NC_EXPLAIN="y"
    else
      APPENDIX_NC_EXPLAIN="n"
    fi
  else
    APPENDIX_NC_OUTLIERS="n"
    NC_OUTLIER_ADD_TO=$(ask "--nc-outlier-add-to (kpi|impact|both)" "$NC_OUTLIER_ADD_TO")
  fi
  NC_OUTLIER_MODE=$(ask "--nc-outlier-mode (off|fixed|adaptive-fdr)" "$NC_OUTLIER_MODE")
  if [[ "$NC_OUTLIER_MODE" == "fixed" ]]; then
    NC_OUTLIER_THRESH=$(ask "--nc-outlier-thresh (ex.: 0.90)" "$NC_OUTLIER_THRESH")
  fi
  NC_OUTLIER_MIN_N=$(ask "--nc-outlier-min-n" "$NC_OUTLIER_MIN_N")
  if [[ "$NC_OUTLIER_MODE" == "adaptive-fdr" ]]; then
    NC_OUTLIER_FDR=$(ask "--nc-outlier-fdr" "$NC_OUTLIER_FDR")
    NC_OUTLIER_GRID=$(ask "--nc-outlier-grid" "$NC_OUTLIER_GRID")
  fi

  # Exportação
  EXPORT_PNG=$(ask "Exportar PNGs? (y/n)" "$EXPORT_PNG")
  EXPORT_PDF=$(ask "Exportar PDF? (y/n)" "$EXPORT_PDF")

  # Header/front
  if confirm "Usar .org ÚNICO (capa+texto) no início? (--header-and-text)" "$HEADER_AND_TEXT"; then
    HEADER_AND_TEXT="y"
    HEADER_AND_TEXT_FILE="$(ask "--header-and-text-file" "$HEADER_AND_TEXT_FILE")"
  else
    HEADER_AND_TEXT="n"
    if confirm "Adicionar PDFs prontos ao front? (--front-pdf)" "n"; then
      while true; do
        p="$(ask "Caminho PDF (ENTER termina)" "")"
        [[ -z "$p" ]] && break
        FRONT_PDF+=("$p")
      done
    fi
    if confirm "Renderizar .org para o front? (--front-org-render)" "n"; then
      while true; do
        o="$(ask "Caminho .org (ENTER termina)" "")"
        [[ -z "$o" ]] && break
        FRONT_ORG_RENDER+=("$o")
      done
    fi
    if confirm "Inserir header-org como texto (não PDF)?" "n"; then
      HEADER_ORG="$(ask "--header-org (arquivo .org)" "$HEADER_ORG")"
    fi
    if confirm "Inserir front-org como texto (não PDF)?" "n"; then
      FRONT_ORG_TEXT="$(ask "--front-org (arquivo .org)" "$FRONT_ORG_TEXT")"
    fi
  fi

  # Aparência
  FIG_SCALE=$(ask "--fig-scale" "$FIG_SCALE")
  FIG_DPI=$(ask "--fig-dpi" "$FIG_DPI")
  PDF_IMG_FRAC=$(ask "--pdf-img-frac" "$PDF_IMG_FRAC")

  # Margens (opcional)
  if confirm "Deseja ajustar margens da página?" "n"; then
    PAGE_MARGIN=$(ask "--page-margin (única, cm; vazio=ignorar)" "$PAGE_MARGIN")
    PAGE_MARGIN_LEFT=$(ask "--page-margin-left (cm; vazio=ignorar)" "$PAGE_MARGIN_LEFT")
    PAGE_MARGIN_RIGHT=$(ask "--page-margin-right (cm; vazio=ignorar)" "$PAGE_MARGIN_RIGHT")
    PAGE_MARGIN_TOP=$(ask "--page-margin-top (cm; vazio=ignorar)" "$PAGE_MARGIN_TOP")
    PAGE_MARGIN_BOTTOM=$(ask "--page-margin-bottom (cm; vazio=ignorar)" "$PAGE_MARGIN_BOTTOM")
  fi

  # Tabelas
  TABLE_FONT_SIZE=$(ask "--table-font-size" "$TABLE_FONT_SIZE")
  TABLE_HEADER_FONT_SIZE=$(ask "--table-header-font-size" "$TABLE_HEADER_FONT_SIZE")

  # TMEA / capacidade
  if confirm "Quer definir TMEA/Capacidade para ΔTMEA?" "n"; then
    TMEA_BR=$(ask "--tmea-br" "$TMEA_BR")
    CAP_BR=$(ask "--cap-br (vazio=ignorar)" "$CAP_BR")
    ATT_BR=$(ask "--att-br (vazio=ignorar)" "$ATT_BR")
  fi

  # GPT
  if confirm "Incluir comentários GPT no PDF? (--gpt-comments)" "$GPT_COMMENTS"; then
    GPT_COMMENTS="y"
  else
    GPT_COMMENTS="n"
  fi
fi

# ==========================================
# Montagem do comando
# ==========================================
CMD=( python3 -m reports.make_impact_report
  --start "$START" --end "$END"
)

# Modo
if [[ "$MODE" == "top10" ]]; then
  CMD+=( --top10 )
else
  CMD+=( --perito "$PERITO_NAME" )
fi

# Parâmetros principais
CMD+=( --min-analises "$MIN_ANALISES" --alpha "$ALPHA" )
[[ -n "$TOPN" ]] && CMD+=( --topn "$TOPN" )
[[ -n "$PBR" ]] && CMD+=( --pbr "$PBR" )

# Seleção
CMD+=( --select-src "$SELECT_SRC" --kpi-min-analises "$KPI_MIN" )

# Outliers %NC
CMD+=( --nc-outlier-mode "$NC_OUTLIER_MODE" )
[[ "$NC_OUTLIER_MODE" == "fixed" ]] && CMD+=( --nc-outlier-thresh "$NC_OUTLIER_THRESH" )
CMD+=( --nc-outlier-min-n "$NC_OUTLIER_MIN_N" )
CMD+=( --nc-outlier-fdr "$NC_OUTLIER_FDR" )
CMD+=( --nc-outlier-grid "$NC_OUTLIER_GRID" )
CMD+=( --nc-outlier-add-to "$NC_OUTLIER_ADD_TO" )

# Apêndice %NC
[[ "${APPENDIX_NC_OUTLIERS,,}" == "y" ]] && CMD+=( --appendix-nc-outliers )
[[ "${APPENDIX_NC_EXPLAIN,,}" == "y" ]] && CMD+=( --appendix-nc-explain )

# Export
[[ "${EXPORT_PNG,,}" == "y" ]] && CMD+=( --export-png )
[[ "${EXPORT_PDF,,}" == "y" ]] && CMD+=( --export-pdf )

# Sensibilidade / Estratos
[[ "${SENS_PLOT,,}" == "y" ]] && CMD+=( --sens-plot )
[[ -n "$SENS_ALPHA_FRAC" ]] && CMD+=( --sens-alpha-frac "$SENS_ALPHA_FRAC" )
[[ -n "$SENS_PBR_PP" ]] && CMD+=( --sens-pbr-pp "$SENS_PBR_PP" )
[[ -n "$BY" ]] && CMD+=( --by "$BY" )

# Testes
if [[ "${ALL_TESTS,,}" == "y" ]]; then
  CMD+=( --all-tests )
else
  [[ "${TEST_BINOMIAL,,}" == "y" ]] && CMD+=( --test-binomial )
  [[ "${BETABIN,,}" == "y" ]] && CMD+=( --betabin )
  [[ -n "$PERMUTE_WEIGHT" && "$PERMUTE_WEIGHT" != "0" ]] && CMD+=( --permute-weight "$PERMUTE_WEIGHT" )
  [[ "${PERMUTE_STRATIFY,,}" == "y" ]] && CMD+=( --permute-stratify )
  [[ -n "$CMH_BY" ]] && CMD+=( --cmh "$CMH_BY" )
  [[ -n "$PSA" && "$PSA" != "0" ]] && CMD+=( --psa "$PSA" )
  [[ -n "$PSA_ALPHA_STRENGTH" ]] && CMD+=( --psa-alpha-strength "$PSA_ALPHA_STRENGTH" )
fi

# Header/front
if [[ "${HEADER_AND_TEXT,,}" == "y" ]]; then
  CMD+=( --header-and-text --header-and-text-file "$HEADER_AND_TEXT_FILE" )
else
  for p in "${FRONT_PDF[@]}"; do CMD+=( --front-pdf "$p" ); done
  for o in "${FRONT_ORG_RENDER[@]}"; do CMD+=( --front-org-render "$o" ); done
  [[ -n "$HEADER_ORG" ]] && CMD+=( --header-org "$HEADER_ORG" )
  [[ -n "$FRONT_ORG_TEXT" ]] && CMD+=( --front-org "$FRONT_ORG_TEXT" )
fi

# Aparência
CMD+=( --fig-scale "$FIG_SCALE" --fig-dpi "$FIG_DPI" --pdf-img-frac "$PDF_IMG_FRAC" )

# Margens
[[ -n "$PAGE_MARGIN" ]] && CMD+=( --page-margin "$PAGE_MARGIN" )
[[ -n "$PAGE_MARGIN_LEFT" ]] && CMD+=( --page-margin-left "$PAGE_MARGIN_LEFT" )
[[ -n "$PAGE_MARGIN_RIGHT" ]] && CMD+=( --page-margin-right "$PAGE_MARGIN_RIGHT" )
[[ -n "$PAGE_MARGIN_TOP" ]] && CMD+=( --page-margin-top "$PAGE_MARGIN_TOP" )
[[ -n "$PAGE_MARGIN_BOTTOM" ]] && CMD+=( --page-margin-bottom "$PAGE_MARGIN_BOTTOM" )

# Tabelas
CMD+=( --table-header-font-size "$TABLE_HEADER_FONT_SIZE" --table-font-size "$TABLE_FONT_SIZE" )

# TMEA / Capacidade
[[ -n "$TMEA_BR" ]] && CMD+=( --tmea-br "$TMEA_BR" )
[[ -n "$CAP_BR" ]] && CMD+=( --cap-br "$CAP_BR" )
[[ -n "$ATT_BR" ]] && CMD+=( --att-br "$ATT_BR" )

# GPT
if [[ "${GPT_COMMENTS,,}" == "y" ]]; then
  CMD+=( --gpt-comments )
else
  CMD+=( --no-gpt-comments )
fi

# ==========================================
# Mostrar e executar
# ==========================================
echo
echo "Comando final:"
printf ' %q' "${CMD[@]}"; echo
echo

if confirm "Executar agora?" "y"; then
  "${CMD[@]}"
else
  echo "Ok! Copie e cole o comando acima quando quiser executar."
fi

