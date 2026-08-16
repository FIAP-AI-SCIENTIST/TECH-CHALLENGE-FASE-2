#!/usr/bin/env bash
set -e

# Empacota o código do Streaming Producer para deploy como Cloud Function (Gen2).
#
# O buildpack Python do GCP espera main.py na raiz do zip com a função de
# entrada, mais um requirements.txt — não entende o layout src/ do monorepo
# nem pyproject.toml. Este script monta um diretório de build "achatado"
# (pacotes copiados para a raiz, não sob src/) e gera o zip a partir dele.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="$(mktemp -d)"
OUTPUT_ZIP="${1:-$REPO_ROOT/infra/.build/producer.zip}"

mkdir -p "$(dirname "$OUTPUT_ZIP")"

echo "Montando diretório de build em $BUILD_DIR..."

# Copia só os pacotes que o Producer realmente importa (streaming, contracts,
# common, observability) — não o monorepo inteiro.
cp -r "$REPO_ROOT/src/streaming" "$BUILD_DIR/"
cp -r "$REPO_ROOT/src/contracts" "$BUILD_DIR/"
cp -r "$REPO_ROOT/src/common" "$BUILD_DIR/"
cp -r "$REPO_ROOT/src/observability" "$BUILD_DIR/"

# Remove __pycache__ copiados por engano
find "$BUILD_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

cat > "$BUILD_DIR/main.py" <<'EOF'
"""Ponto de entrada do Cloud Function — expõe o handler HTTP do Producer."""
from streaming.producer import cloud_function_entrypoint as handler
EOF

# Versões pinadas ao ambiente local no momento do build — evita que o buildpack do GCP resolva
# uma versão diferente da testada localmente (achado A7 da revisão de aderência). Cai para o
# floor solto só se o pacote não estiver instalado localmente (build "a frio", sem venv).
PYTHON_BIN="${PYTHON:-python3}"
PIN() {
  "$PYTHON_BIN" -c "
import importlib.metadata as m
try:
    print(f'$1=={m.version(\"$1\")}')
except m.PackageNotFoundError:
    print('$2')
"
}

{
  PIN pydantic "pydantic>=2.0"
  PIN google-cloud-pubsub "google-cloud-pubsub"
  PIN google-cloud-bigquery "google-cloud-bigquery"
} > "$BUILD_DIR/requirements.txt"

echo "Gerando zip em $OUTPUT_ZIP..."
rm -f "$OUTPUT_ZIP"
(cd "$BUILD_DIR" && zip -r -q "$OUTPUT_ZIP" .)

rm -rf "$BUILD_DIR"
echo "Empacotamento concluído: $OUTPUT_ZIP"
