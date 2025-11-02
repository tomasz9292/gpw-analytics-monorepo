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
            if [ -n "${LLM_BOOTSTRAP_BUSYBOX_PATH:-}" ] && [ -f "${LLM_BOOTSTRAP_BUSYBOX_PATH}" ]; then
                log "Brak tar – używam lokalnego busybox z ${LLM_BOOTSTRAP_BUSYBOX_PATH}"
                mkdir -p "${TARGET_DIR}/bin"
                cp "${LLM_BOOTSTRAP_BUSYBOX_PATH}" "${busybox_bin}" || {
                    echo "Błąd: nie można skopiować lokalnego busybox" >&2
                    return 1
                }
                chmod +x "${busybox_bin}" || {
                    echo "Błąd: nie można ustawić uprawnień dla busybox" >&2
                    return 1
                }
            else
                log "Brak tar – pobieranie busybox"

            local arch="$(uname -m)"
            local busybox_filename=""
            local busybox_arch_slug="${arch}"
            case "${arch}" in
                x86_64|amd64)
                    busybox_filename="busybox-x86_64"
                    busybox_arch_slug="x86_64"
                    ;;
                aarch64|arm64)
                    busybox_filename="busybox-aarch64"
                    busybox_arch_slug="aarch64"
                    ;;
                *)
                    echo "Błąd: brak wsparcia dla architektury ${arch} bez narzędzia tar" >&2
                    return 1
                    ;;
            esac

            local -a busybox_urls=()
            if [ -n "${LLM_BOOTSTRAP_BUSYBOX_URLS:-}" ]; then
                while IFS= read -r line; do
                    [ -n "${line}" ] && busybox_urls+=("${line}")
                done <<EOF
${LLM_BOOTSTRAP_BUSYBOX_URLS}
EOF
            fi

            if [ "${#busybox_urls[@]}" -eq 0 ]; then
                local -a busybox_versions=(
                    "1.36.1"
                    "1.36.0"
                    "1.35.0"
                    "1.34.1"
                )

                for version in "${busybox_versions[@]}"; do
                    busybox_urls+=(
                        "https://busybox.net/downloads/binaries/${version}-defconfig-multiarch/${busybox_filename}"
                        "http://busybox.net/downloads/binaries/${version}-defconfig-multiarch/${busybox_filename}"
                        "https://busybox.net/downloads/binaries/${version}/${busybox_filename}"
                        "http://busybox.net/downloads/binaries/${version}/${busybox_filename}"
                        "https://busybox.net/downloads/binaries/${version}-${busybox_arch_slug}-linux-musl/${busybox_filename}"
                        "http://busybox.net/downloads/binaries/${version}-${busybox_arch_slug}-linux-musl/${busybox_filename}"
                    )
                done

                busybox_urls+=(
                    "https://frippery.org/files/busybox/${busybox_filename}"
                    "https://raw.githubusercontent.com/andrew-d/static-binaries/master/${busybox_filename}"
                    "https://raw.githubusercontent.com/moparisthebest/static-binaries/master/${busybox_filename}"
                    "https://raw.githubusercontent.com/landley/busybox/master/${busybox_filename}"
                    "https://raw.githubusercontent.com/mirror/busybox/master/${busybox_filename}"
                )

                if [ "${arch}" = "x86_64" ] || [ "${arch}" = "amd64" ]; then
                    busybox_urls+=(
                        "https://raw.githubusercontent.com/termux/termux-packages/master/packages/busybox-static/${busybox_filename}"
                        "https://raw.githubusercontent.com/andrew-d/static-binaries/master/busybox-amd64"
                        "https://raw.githubusercontent.com/moparisthebest/static-binaries/master/busybox-amd64"
                    )
                fi

                if [ "${arch}" = "aarch64" ] || [ "${arch}" = "arm64" ]; then
                    busybox_urls+=(
                        "https://raw.githubusercontent.com/andrew-d/static-binaries/master/busybox-arm64"
                        "https://raw.githubusercontent.com/moparisthebest/static-binaries/master/busybox-arm64"
                    )
                fi
            fi

                local download_succeeded=0
                for busybox_url in "${busybox_urls[@]}"; do
                    log "Próba pobrania busybox z ${busybox_url}"
                    if command -v curl >/dev/null 2>&1; then
                        if curl -fsSL --retry 2 --retry-connrefused --retry-delay 1 "${busybox_url}" -o "${busybox_bin}"; then
                            download_succeeded=1
                            break
                        else
                            local curl_status=$?
                            log "Niepowodzenie pobrania busybox (curl exit ${curl_status})"
                        fi
                    elif command -v wget >/dev/null 2>&1; then
                        if wget -q -O "${busybox_bin}" "${busybox_url}"; then
                            download_succeeded=1
                            break
                        else
                            local wget_status=$?
                            log "Niepowodzenie pobrania busybox (wget exit ${wget_status})"
                        fi
                    else
                        echo "Błąd: wymagany jest curl lub wget, aby pobrać busybox" >&2
                        return 1
                    fi

                    rm -f "${busybox_bin}" >/dev/null 2>&1 || true
                done

                if [ "${download_succeeded}" -ne 1 ]; then
                    echo "Błąd: nie udało się pobrać busybox" >&2
                    echo "Ustaw LLM_BOOTSTRAP_BUSYBOX_PATH lub LLM_BOOTSTRAP_BUSYBOX_URLS, aby wskazać własne źródło" >&2
                    return 1
                fi

                chmod +x "${busybox_bin}" || {
                    echo "Błąd: nie można ustawić uprawnień dla busybox" >&2
                    return 1
                }
            fi
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
