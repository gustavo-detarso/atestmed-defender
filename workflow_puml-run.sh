#!/usr/bin/env bash
# puml-run — Renderizador PlantUML via JAR (distribuível)
# -------------------------------------------------------------------
# Uso básico:
#   ./puml-run arquivo.puml                 # gera arquivo.svg
#   ./puml-run --png arquivo.puml           # gera arquivo.png
#   ./puml-run --recurse .                  # gera SVG p/ todos .puml recursivamente
#   ./puml-run --out build --recurse .      # saída em ./build
#   ./puml-run --jar /caminho/plantuml.jar arquivo.puml
#
# Opções:
#   --svg            Gerar SVG (padrão)
#   --png            Gerar PNG
#   --pdf            Gerar PDF
#   --recurse, -r    Processar diretórios recursivamente
#   --out DIR        Diretório de saída (equivalente a -o do PlantUML)
#   --jar PATH       Caminho do plantuml.jar a usar
#   --limit N        Define PLANTUML_LIMIT_SIZE (padrão: 8192)
#   --java-opts STR  Opções extras p/ a JVM (ex.: "--add-opens=...")  [aspas!]
#   -h, --help       Mostrar esta ajuda e sair
#
# Variáveis de ambiente (alternativas às opções):
#   PLANTUML_JAR           Caminho do JAR (prioridade menor que --jar)
#   PLANTUML_LIMIT_SIZE    Limite de pixels (padrão 8192)
#   JAVA_OPTS              Opções extras p/ a JVM
#
# Descoberta do JAR (ordem):
#   1) --jar PATH
#   2) $PLANTUML_JAR
#   3) plantuml.jar na mesma pasta deste script
#   4) plantuml-gplv2-1.2025.4.jar na mesma pasta deste script
#   5) ~/.emacs.d/misc/plantuml-gplv2-1.2025.4.jar
# -------------------------------------------------------------------

set -euo pipefail

print_help() { sed -n '2,45p' "$0"; exit 0; }  # imprime o cabeçalho acima

# --- Descobrir diretório do script ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Defaults ---
FMT="-tsvg"
RECURSE=0
OUT_DIR=""
JAR="${PLANTUML_JAR:-}"
LIMIT="${PLANTUML_LIMIT_SIZE:-8192}"
EXTRA_JAVA_OPTS="${JAVA_OPTS:-}"

# --- Parse de argumentos ---
PASS_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --svg) FMT="-tsvg"; shift ;;
    --png) FMT="-tpng"; shift ;;
    --pdf) FMT="-tpdf"; shift ;;
    --recurse|-r) RECURSE=1; shift ;;
    --out) OUT_DIR="${2:-}"; shift 2 ;;
    --jar) JAR="${2:-}"; shift 2 ;;
    --limit) LIMIT="${2:-8192}"; shift 2 ;;
    --java-opts) EXTRA_JAVA_OPTS="${EXTRA_JAVA_OPTS} ${2:-}"; shift 2 ;;
    -h|--help) print_help ;;
    *) PASS_ARGS+=("$1"); shift ;;
  esac
done

# --- Localizar JAR se não veio por --jar / env ---
if [[ -z "${JAR}" ]]; then
  for c in \
    "$SCRIPT_DIR/plantuml.jar" \
    "$SCRIPT_DIR/plantuml-gplv2-1.2025.4.jar" \
    "$HOME/.emacs.d/misc/plantuml-gplv2-1.2025.4.jar"
  do
    [[ -f "$c" ]] && JAR="$c" && break
  done
fi

# --- Validações ---
[[ -n "${JAR}" && -f "${JAR}" ]] || { echo "ERRO: plantuml.jar não encontrado. Use --jar PATH ou defina \$PLANTUML_JAR."; exit 2; }
[[ ${#PASS_ARGS[@]} -gt 0 ]] || { echo "ERRO: informe arquivo(s) .puml ou diretório. Use --help para ver exemplos."; exit 3; }

# --- Montar comando ---
JAVA_OPTS_ALL="-Djava.awt.headless=true -DPLANTUML_LIMIT_SIZE=${LIMIT} ${EXTRA_JAVA_OPTS}"
CMD=(java ${JAVA_OPTS_ALL} -jar "${JAR}" "${FMT}")
[[ ${RECURSE} -eq 1 ]] && CMD+=("-recurse")
[[ -n "${OUT_DIR}" ]] && CMD+=("-o" "${OUT_DIR}")
CMD+=("${PASS_ARGS[@]}")

# --- Executar ---
exec "${CMD[@]}"

