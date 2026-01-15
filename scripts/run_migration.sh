#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."
source venv/bin/activate
export PYTHONPATH=$(pwd):$PYTHONPATH
python src/main.py --config config/config.yaml "$@"
