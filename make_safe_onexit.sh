#!/usr/bin/env bash
set -euo pipefail

# make_safe_onexit.sh
# Aplica dois patches em arquivos .R:
#  1) Insere on.exit(...) logo após "VAR <- am_open_db(...)" se não houver.
#  2) Insere sanity-check após source('_common.R') se não houver.
#
# Uso:
#   ./make_safe_onexit.sh                 # roda na pasta atual (recursivo)
#   ./make_safe_onexit.sh r_checks        # roda só em r_checks/
#   DRY_RUN=1 ./make_safe_onexit.sh       # mostra o que alteraria, sem escrever
#
# Requer: perl, find, xargs
# Cria backups .bak por arquivo alterado

ROOT="${1:-.}"
DRY="${DRY_RUN:-0}"

if ! command -v perl >/dev/null 2>&1; then
  echo "❌ Requer perl"; exit 1
fi

echo "▶ Alvo: ${ROOT}"
echo "▶ Dry-run: ${DRY}"

# Localiza arquivos .R (exclui .git)
mapfile -d '' FILES < <(find "$ROOT" -type f -name '*.R' -not -path '*/.git/*' -print0)

if (( ${#FILES[@]} == 0 )); then
  echo "ℹ️  Nenhum .R encontrado em ${ROOT}"
  exit 0
fi

# Função: aplica patch com perl em um arquivo
patch_file() {
  local f="$1"
  local tmp="$(mktemp)"
  cp "$f" "$tmp"

  # 1) Inserir on.exit após am_open_db(...) se não houver on.exit na próxima linha
  perl -0777 -pe '
    s{
      (                                   # \1: linha com abertura
        ([A-Za-z_][A-Za-z0-9_.]*)         # \2: nome da variável (ex.: con)
        \s*<-\s*am_open_db\([^\)]*\)\s*\n
      )
      (?![^\n]*on\.exit)                  # não insere se já houver on.exit na próxima
    }{$1 . "on.exit(try(am_safe_disconnect(".$2."), silent=TRUE), add=TRUE)\n"}egx
  ' "$tmp" > "$tmp.1"

  # 2) Inserir sanity check após source(..._common.R...) se ainda não existir no arquivo
  #    (não repete se já houver am_dbGetQuery(NULL, "SELECT 1"))
  if ! grep -q 'am_dbGetQuery(NULL, "SELECT 1")' "$tmp.1"; then
    perl -0777 -pe '
      s{
        (^\s*source\([^)]*_common\.R[^)]*\)\s*\n)   # \1: linha source(...)
      }{$1 . "invisible(am_dbGetQuery(NULL, \"SELECT 1\"))\n"}m
    ' "$tmp.1" > "$tmp.2"
  else
    cp "$tmp.1" "$tmp.2"
  fi

  if ! diff -q "$f" "$tmp.2" >/dev/null 2>&1; then
    echo "✏️  Alteraria: $f"
    if [[ "$DRY" != "1" ]]; then
      cp "$f" "$f.bak"
      cp "$tmp.2" "$f"
      echo "✅ Patched:  $f (backup em $f.bak)"
    fi
  fi
  rm -f "$tmp" "$tmp.1" "$tmp.2"
}

for f in "${FILES[@]}"; do
  patch_file "$f"
done

echo "✔️  Finalizado."
echo "Dica: reveja diffs com 'git diff -- . \":!*.bak\"' e depois limpe backups com:"
echo "      find ${ROOT@Q} -type f -name '*.bak' -not -path '*/.git/*' -delete"

