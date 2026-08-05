#!/usr/bin/env bash
set -e

# Script de guarda para o `terraform apply` em grupo.
# Previne destruição acidental de módulos causadas por state divergente.

echo "Verificando permissões para 'terraform apply'..."

CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo "ERRO: 'terraform apply' só é permitido a partir da branch 'main'."
    echo "Feature branches só devem rodar 'terraform plan'. Realize um PR para main antes de aplicar."
    exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
    echo "ERRO: Working tree não está limpo. Há mudanças não commitadas."
    echo "Faça o commit das alterações antes de aplicar."
    exit 1
fi

echo "Buscando atualizações de origin/main..."
git fetch origin main >/dev/null 2>&1

LOCAL_COMMIT=$(git rev-parse main)
REMOTE_COMMIT=$(git rev-parse origin/main)

if [ "$LOCAL_COMMIT" != "$REMOTE_COMMIT" ]; then
    echo "ERRO: A branch 'main' local está desatualizada em relação a 'origin/main'."
    echo "Execute 'git pull origin main' e sincronize o repositório antes de aplicar."
    exit 1
fi

echo "Guard checks passed. Procedendo com 'terraform apply'..."
exit 0
