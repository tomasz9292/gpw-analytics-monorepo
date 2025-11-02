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

ensure_downloader() {
    if command -v curl >/dev/null 2>&1; then
        DOWNLOADER=(curl -L "${MODEL_URL}" -o "${MODEL_DIR}/${MODEL_NAME}")
        return 0
    fi

    if command -v wget >/dev/null 2>&1; then
        DOWNLOADER=(wget "${MODEL_URL}" -O "${MODEL_DIR}/${MODEL_NAME}")
        return 0
    fi

    echo "Błąd: wymagany jest curl lub wget" >&2
    return 1
}

detect_python() {
    if [ -n "${LLM_BOOTSTRAP_PYTHON:-}" ] && command -v "${LLM_BOOTSTRAP_PYTHON}" >/dev/null 2>&1; then
        PYTHON_BIN="$(command -v "${LLM_BOOTSTRAP_PYTHON}")"
        return 0
    fi

    for candidate in python3 python3.12 python3.11 python3.10 python3.9 python; do
        if command -v "${candidate}" >/dev/null 2>&1; then
            PYTHON_BIN="$(command -v "${candidate}")"
            return 0
        fi
    done

    return 1
}

ensure_tar() {
    if command -v tar >/dev/null 2>&1; then
        return 0
    fi

    local busybox_bin=""

    if command -v busybox >/dev/null 2>&1; then
        busybox_bin="$(command -v busybox)"
    else
        busybox_bin="${TARGET_DIR}/bin/busybox"
        if [ ! -x "${busybox_bin}" ]; then
            log "Brak tar – pobieranie busybox"

            local arch="$(uname -m)"
            local -a busybox_urls
            case "${arch}" in
                x86_64|amd64)
                    busybox_urls=(
                        "https://busybox.net/downloads/binaries/1.36.1-defconfig-multiarch/busybox-x86_64"
                        "http://busybox.net/downloads/binaries/1.36.1-defconfig-multiarch/busybox-x86_64"
                        "https://frippery.org/files/busybox/busybox-x86_64"
                    )
                    ;;
                aarch64|arm64)
                    busybox_urls=(
                        "https://busybox.net/downloads/binaries/1.36.1-defconfig-multiarch/busybox-aarch64"
                        "http://busybox.net/downloads/binaries/1.36.1-defconfig-multiarch/busybox-aarch64"
                    )
                    ;;
                *)
                    echo "Błąd: brak wsparcia dla architektury ${arch} bez narzędzia tar" >&2
                    return 1
                    ;;
            esac

            local download_succeeded=0
            for busybox_url in "${busybox_urls[@]}"; do
                if command -v curl >/dev/null 2>&1; then
                    if curl -fsSL "${busybox_url}" -o "${busybox_bin}"; then
                        download_succeeded=1
                        break
                    fi
                elif command -v wget >/dev/null 2>&1; then
                    if wget -q -O "${busybox_bin}" "${busybox_url}"; then
                        download_succeeded=1
                        break
                    fi
                else
                    echo "Błąd: wymagany jest curl lub wget, aby pobrać busybox" >&2
                    return 1
                fi
            done

            if [ "${download_succeeded}" -ne 1 ]; then
                echo "Błąd: nie udało się pobrać busybox" >&2
                return 1
            fi

            chmod +x "${busybox_bin}" || {
                echo "Błąd: nie można ustawić uprawnień dla busybox" >&2
                return 1
            }
        fi
    fi

    if [ -z "${busybox_bin}" ]; then
        echo "Błąd: nie można przygotować zastępczego tar" >&2
        return 1
    fi

    if ! "${busybox_bin}" tar --help >/dev/null 2>&1; then
        echo "Błąd: busybox nie obsługuje polecenia tar" >&2
        return 1
    fi

    local tar_shim="${TARGET_DIR}/bin/tar"
    printf '#!/usr/bin/env sh\nexec "%s" tar "$@"\n' "${busybox_bin}" >"${tar_shim}" || {
        echo "Błąd: nie można utworzyć zastępczego tar" >&2
        return 1
    }
    chmod +x "${tar_shim}" || {
        echo "Błąd: nie można ustawić uprawnień dla tar" >&2
        return 1
    }

    PATH="${TARGET_DIR}/bin:${PATH}"
    export PATH
    return 0
}

ensure_uv() {
    if command -v uv >/dev/null 2>&1; then
        UV_BIN="$(command -v uv)"
        return 0
    fi

    local uv_dir="${TARGET_DIR}/bin"
    local uv_bin="${uv_dir}/uv"

    if [ -x "${uv_bin}" ]; then
        UV_BIN="${uv_bin}"
        return 0
    fi

    if ! ensure_tar; then
        return 1
    fi

    log "Brak lokalnego Python – pobieranie uv"
    mkdir -p "${uv_dir}"

    local installer=(sh)
    if command -v curl >/dev/null 2>&1; then
        installer=(sh -s -- --install-dir "${uv_dir}" --bin-dir "${uv_dir}" --quiet)
        if ! curl -fsSL https://astral.sh/uv/install.sh | "${installer[@]}"; then
            echo "Błąd: nie udało się pobrać uv" >&2
            return 1
        fi
    elif command -v wget >/dev/null 2>&1; then
        installer=(sh -s -- --install-dir "${uv_dir}" --bin-dir "${uv_dir}" --quiet)
        if ! wget -qO- https://astral.sh/uv/install.sh | "${installer[@]}"; then
            echo "Błąd: nie udało się pobrać uv" >&2
            return 1
        fi
    else
        echo "Błąd: wymagany jest curl lub wget, aby pobrać uv" >&2
        return 1
    fi

    if [ ! -x "${uv_bin}" ]; then
        echo "Błąd: instalacja uv nie powiodła się" >&2
        return 1
    fi

    UV_BIN="${uv_bin}"
    PATH="${uv_dir}:${PATH}"
    export PATH
    return 0
}

ensure_python() {
    if detect_python; then
        return 0
    fi

    if ! ensure_uv; then
        return 1
    fi

    if [ ! -d "${VENV_DIR}" ]; then
        log "Tworzenie wirtualnego środowiska za pomocą uv"
        "${UV_BIN}" venv "${VENV_DIR}" >/dev/null 2>&1 || {
            echo "Błąd: nie udało się utworzyć środowiska wirtualnego przez uv" >&2
            return 1
        }
    fi

    PYTHON_BIN="${VENV_DIR}/bin/python"
    if [ ! -x "${PYTHON_BIN}" ]; then
        echo "Błąd: uv nie dostarczył interpretera Python" >&2
        return 1
    fi

    return 0
}

log "Tworzenie katalogów w ${TARGET_DIR}"
mkdir -p "${MODEL_DIR}" "${TARGET_DIR}/bin"

if ! ensure_python; then
    echo "Błąd: nie można przygotować interpretera Python" >&2
    exit 1
fi

if [ ! -d "${VENV_DIR}" ]; then
    log "Tworzenie wirtualnego środowiska"
    "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

log "Aktywowanie środowiska i instalacja zależności"
# shellcheck disable=SC1090
source "${VENV_DIR}/bin/activate"
python -m pip install --upgrade pip
python -m pip install "llama-cpp-python==0.2.78"

log "Pobieranie przykładowego modelu GGUF"
if ! ensure_downloader; then
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
