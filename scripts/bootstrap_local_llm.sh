#!/usr/bin/env bash
set -euo pipefail

DEFAULT_HOME="${LLM_BOOTSTRAP_HOME:-${HOME:-}}"

if [ -z "${DEFAULT_HOME}" ]; then
    DEFAULT_HOME="${XDG_DATA_HOME:-}"
fi

if [ -z "${DEFAULT_HOME}" ]; then
    DEFAULT_HOME="/tmp"
fi

TARGET_DIR="${LLM_BOOTSTRAP_TARGET_DIR:-${DEFAULT_HOME}/gpw-llm}"
VENV_DIR="${TARGET_DIR}/venv"
MODEL_DIR="${TARGET_DIR}/models"
MODEL_NAME="zephyr-7b-beta.Q4_K_M.gguf"
MODEL_URL="https://huggingface.co/TheBloke/zephyr-7B-beta-GGUF/resolve/main/${MODEL_NAME}?download=1"

log() {
    printf '\n>>> %s\n' "$1"
}

log "Tworzenie katalogów w ${TARGET_DIR}"
mkdir -p "${MODEL_DIR}"

if ! command -v python3 >/dev/null 2>&1; then
    echo "Błąd: wymagany jest python3" >&2
    exit 1
fi

if [ ! -d "${VENV_DIR}" ]; then
    log "Tworzenie wirtualnego środowiska"
    python3 -m venv "${VENV_DIR}"
fi

log "Aktywowanie środowiska i instalacja zależności"
# shellcheck disable=SC1090
source "${VENV_DIR}/bin/activate"
python -m pip install --upgrade pip
python -m pip install "llama-cpp-python==0.2.78"

log "Pobieranie przykładowego modelu GGUF"
if command -v curl >/dev/null 2>&1; then
    DOWNLOADER=(curl -L "${MODEL_URL}" -o "${MODEL_DIR}/${MODEL_NAME}")
elif command -v wget >/dev/null 2>&1; then
    DOWNLOADER=(wget "${MODEL_URL}" -O "${MODEL_DIR}/${MODEL_NAME}")
else
    echo "Błąd: wymagany jest curl lub wget" >&2
    deactivate || true
    exit 1
fi

if [ ! -f "${MODEL_DIR}/${MODEL_NAME}" ]; then
    "${DOWNLOADER[@]}"
else
    log "Plik modelu już istnieje – pomijam pobieranie"
fi

GPU_LAYERS=0
if command -v nvidia-smi >/dev/null 2>&1; then
    GPU_LAYERS=20
elif command -v rocm-smi >/dev/null 2>&1; then
    GPU_LAYERS=16
fi

CONFIG_FILE="${TARGET_DIR}/config.json"
cat >"${CONFIG_FILE}" <<JSON
{
  "model_path": "${MODEL_DIR}/${MODEL_NAME}",
  "gpu_layers": ${GPU_LAYERS}
}
JSON

deactivate || true

log "Środowisko LLM przygotowane"
echo "MODEL_PATH=${MODEL_DIR}/${MODEL_NAME}"
echo "GPU_LAYERS=${GPU_LAYERS}"
echo "Konfiguracja zapisana w ${CONFIG_FILE}"
