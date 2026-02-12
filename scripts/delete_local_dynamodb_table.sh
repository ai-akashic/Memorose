#!/bin/bash

# Delete a table from local DynamoDB (default endpoint: http://localhost:8000)

set -euo pipefail

usage() {
    echo "Usage: $0 <table-name> [endpoint-url] [-y|--yes]"
    echo "Env: DYNAMODB_ENDPOINT (default: http://localhost:8000), AWS_REGION (default: us-east-1)"
}

if [ $# -lt 1 ]; then
    usage
    exit 1
fi

TABLE_NAME=""
ENDPOINT_URL=""
CONFIRM=1

for arg in "$@"; do
    case "$arg" in
        -y|--yes)
            CONFIRM=0
            ;;
        http://*|https://*)
            ENDPOINT_URL="$arg"
            ;;
        *)
            if [ -z "$TABLE_NAME" ]; then
                TABLE_NAME="$arg"
            else
                echo "Unexpected argument: $arg"
                usage
                exit 1
            fi
            ;;
    esac
done

if [ -z "$TABLE_NAME" ]; then
    usage
    exit 1
fi

ENDPOINT_URL="${ENDPOINT_URL:-${DYNAMODB_ENDPOINT:-http://localhost:8000}}"
REGION="${AWS_REGION:-us-east-1}"

if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
    export AWS_ACCESS_KEY_ID="local"
fi
if [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
    export AWS_SECRET_ACCESS_KEY="local"
fi

if [ "$CONFIRM" -ne 0 ]; then
    read -r -p "Delete DynamoDB table '${TABLE_NAME}' at ${ENDPOINT_URL}? [y/N] " reply
    if [[ ! "$reply" =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
fi

echo "Deleting table '${TABLE_NAME}' from ${ENDPOINT_URL}..."
aws dynamodb delete-table --table-name "$TABLE_NAME" --endpoint-url "$ENDPOINT_URL" --region "$REGION"
echo "Delete request submitted."
