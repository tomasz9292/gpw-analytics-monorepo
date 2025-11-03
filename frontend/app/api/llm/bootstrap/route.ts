import { NextResponse } from "next/server";
import path from "path";
import { access } from "fs/promises";
import { spawn } from "child_process";

type LlmBootstrapVariant = "bash" | "powershell";

const OK_REGEX = /^LLM_OK=1$/m;
const MODEL_PATH_REGEX = /^MODEL_PATH=(.+)$/m;
const GPU_LAYERS_REGEX = /^GPU_LAYERS=(.+)$/m;
const SCRIPTS_DIR = path.resolve(process.cwd(), "..", "scripts");
const BASH_SCRIPT = "bootstrap_local_llm.sh";
const POWERSHELL_SCRIPT = "bootstrap_local_llm.ps1";

export const runtime = "nodejs";

const detectVariant = (): LlmBootstrapVariant =>
    process.platform === "win32" ? "powershell" : "bash";

const extractValue = (pattern: RegExp, source: string): string | undefined => {
    const match = source.match(pattern);
    if (!match || match.length < 2) {
        return undefined;
    }
    return match[1].trim();
};

const resolveScriptPath = (variant: LlmBootstrapVariant): string =>
    path.join(SCRIPTS_DIR, variant === "powershell" ? POWERSHELL_SCRIPT : BASH_SCRIPT);

const createBootstrapCommand = (variant: LlmBootstrapVariant, scriptPath: string) => {
    if (variant === "powershell") {
        return {
            command: "powershell",
            args: [
                "-NoLogo",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                scriptPath,
            ],
        } as const;
    }

    return {
        command: "bash",
        args: [scriptPath],
    } as const;
};

const sanitizeLog = (value: string | undefined): string => {
    if (!value) {
        return "";
    }
    return value
        .split(/\r?\n/)
        .map((line) => line.replace(/\s+$/u, ""))
        .join("\n")
        .trim();
};

function classifyError(logs: string): {
    errorCode: string;
    errorSummary: string;
    probableCause: string;
    remediation: string[];
} | undefined {
    const L = logs || "";
    const has = (re: RegExp) => re.test(L);

    // 1) Polityka wykonywania skryptów PowerShell
    if (has(/running scripts is disabled|cannot be loaded because running scripts is disabled/i)) {
        return {
            errorCode: "E_EXEC_POLICY",
            errorSummary: "PowerShell blokuje wykonywanie skryptów.",
            probableCause: "Polityka ExecutionPolicy na Windows jest zbyt restrykcyjna.",
            remediation: [
                "Uruchom PowerShell (profil użytkownika) i wykonaj:",
                "Set-ExecutionPolicy -Scope CurrentUser RemoteSigned -Force",
                "Następnie ponów instalację lokalnego LLM.",
            ],
        };
    }

    // 2) Kompilacja/instalacja llama-cpp-python
    if (has(/llama-cpp-python/i) && has(/subprocess-exited-with-error|cmake|build|wheel/i)) {
        return {
            errorCode: "E_PIP_BUILD_LLAMA",
            errorSummary: "Instalacja pakietu llama-cpp-python nie powiodła się.",
            probableCause: "Środowisko próbuje budować pakiet ze źródeł lub brakuje narzędzi build.",
            remediation: [
                'W wirtualnym środowisku wykonaj:',
                'python -m pip install --only-binary=:all: "llama-cpp-python==0.2.78"',
                "Jeśli budowanie jest wymagane: doinstaluj CMake i MSVC Build Tools (Windows) albo trzymaj się CPU.",
            ],
        };
    }

    // 3) Brak curl/wget
    if (has(/wymagany jest curl lub wget|curl: not found|wget: not found/i)) {
        return {
            errorCode: "E_NO_CURL_WGET",
            errorSummary: "Brakuje narzędzi do pobierania (curl/wget).",
            probableCause: "System nie ma zainstalowanego curl ani wget.",
            remediation: [
                "Zainstaluj curl lub wget i ponów bootstrap.",
                "Alternatywnie pobierz model ręcznie i umieść w katalogu models.",
            ],
        };
    }

    // 4) Błąd SSL / proxy podczas pobierania
    if (has(/SSL: CERTIFICATE_VERIFY_FAILED|ssl certificate/i)) {
        return {
            errorCode: "E_SSL_CERT",
            errorSummary: "Pobieranie modelu nie powiodło się z powodu błędu SSL.",
            probableCause: "Zapora/proxy/AV przechwytuje ruch lub zła konfiguracja certyfikatów.",
            remediation: [
                "Spróbuj pobrać model ręcznie (zezwolenie w zaporze/AV).",
                "Skonfiguruj poprawnie zaufane certyfikaty lub ustaw proxy systemowe.",
            ],
        };
    }

    // 5) 403 przy Hugging Face
    if (has(/403/) && has(/huggingface/i)) {
        return {
            errorCode: "E_HF_403",
            errorSummary: "Odmowa dostępu przy pobieraniu modelu z Hugging Face (403).",
            probableCause: "Model wymaga uwierzytelnienia albo przekroczono limit.",
            remediation: [
                "Pobierz model ręcznie po zalogowaniu do Hugging Face.",
                "Skopiuj plik GGUF do katalogu models i wskaż ścieżkę w config.json.",
            ],
        };
    }

    // 6) Model nie znaleziony
    if (has(/No such file or directory.*\.gguf|FileNotFoundError.*\.gguf/i)) {
        return {
            errorCode: "E_MODEL_NOT_FOUND",
            errorSummary: "Plik modelu GGUF nie został znaleziony.",
            probableCause: "Nie pobrano modelu lub wskazano błędną ścieżkę.",
            remediation: [
                "Pobierz plik *.gguf do katalogu ~/gpw-llm/models/",
                "Upewnij się, że config.json zawiera poprawny model_path.",
            ],
        };
    }

    // 7) Brak miejsca na dysku
    if (has(/No space left on device|ENOSPC/i)) {
        return {
            errorCode: "E_DISK_FULL",
            errorSummary: "Brak miejsca na dysku podczas instalacji lub pobierania modelu.",
            probableCause: "Za mało wolnego miejsca na partycji docelowej.",
            remediation: [
                "Zwolnij miejsce na dysku (kilka GB) i ponów bootstrap.",
                "Ewentualnie zmień katalog docelowy (LLM_BOOTSTRAP_TARGET_DIR).",
            ],
        };
    }

    // 8) Akceleracja GPU niezgodna
    if (has(/CUDA error|no kernel image|metal|clblast/i)) {
        return {
            errorCode: "E_GPU_INCOMPAT",
            errorSummary: "Problem z akceleracją na GPU.",
            probableCause: "Sterowniki/biblioteki niewłaściwe lub zbyt nowe/stare.",
            remediation: [
                "Ustaw gpu_layers: 0 (CPU) i sprawdź, czy działa.",
                "Potem skonfiguruj właściwe sterowniki CUDA/ROCm/Metal.",
            ],
        };
    }

    // Domyślnie
    return {
        errorCode: "E_UNKNOWN",
        errorSummary: "Instalator zakończył się błędem.",
        probableCause: "Nieznana przyczyna – zobacz logi.",
        remediation: [
            "Przejrzyj pełne logi poniżej.",
            "Skopiuj pierwsze ~30 linii błędu i zgłoś je w projekcie.",
        ],
    };
}

export async function POST() {
    const variant = detectVariant();
    const scriptPath = resolveScriptPath(variant);

    try {
        await access(scriptPath);
    } catch (error) {
        const message =
            error && typeof error === "object" && "code" in error && (error as { code?: string }).code === "ENOENT"
                ? "Brak skryptu automatycznej instalacji (scripts/bootstrap_local_llm.*)."
                : "Nie udało się zweryfikować skryptu instalacyjnego.";
        return NextResponse.json(
            {
                variant,
                ok: false,
                exitCode: 1,
                errorSummary: message,
                error: message,
            },
            { status: 500 }
        );
    }

    const bootstrap = createBootstrapCommand(variant, scriptPath);
    const bootstrapHome =
        process.env.LLM_BOOTSTRAP_HOME ??
        process.env.HOME ??
        process.env.USERPROFILE ??
        "/tmp";

    const child = spawn(bootstrap.command, bootstrap.args, {
        cwd: SCRIPTS_DIR,
        env: {
            ...process.env,
            PYTHONUNBUFFERED: "1",
            LLM_BOOTSTRAP_HOME: bootstrapHome,
        },
    });

    const stdoutChunks: string[] = [];
    const stderrChunks: string[] = [];

    if (child.stdout) {
        child.stdout.setEncoding("utf-8");
        child.stdout.on("data", (chunk: string) => {
            stdoutChunks.push(chunk);
        });
    }

    if (child.stderr) {
        child.stderr.setEncoding("utf-8");
        child.stderr.on("data", (chunk: string) => {
            stderrChunks.push(chunk);
        });
    }

    let exitCode: number;
    try {
        exitCode = await new Promise<number>((resolve, reject) => {
            child.once("error", (error) => reject(error));
            child.once("close", (code) => resolve(code ?? 1));
        });
    } catch (error) {
        console.error("Bootstrap process failed to start", error);
        return NextResponse.json(
            {
                variant,
                ok: false,
                exitCode: 1,
                errorSummary: "Nie udało się uruchomić procesu instalacji.",
                error: "Nie udało się uruchomić procesu instalacji.",
            },
            { status: 500 }
        );
    }

    const stdoutRaw = stdoutChunks.join("");
    const stderrRaw = stderrChunks.join("");
    const combinedRaw = [stdoutRaw, stderrRaw].filter((value) => value.length > 0).join("\n");
    const logs = sanitizeLog(combinedRaw);
    const ok = OK_REGEX.test(combinedRaw) || exitCode === 0;
    const modelPath = extractValue(MODEL_PATH_REGEX, combinedRaw);
    const gpuLayersValue = extractValue(GPU_LAYERS_REGEX, combinedRaw);
    const gpuLayersNumeric = gpuLayersValue ? Number(gpuLayersValue) : undefined;
    const gpuLayers =
        typeof gpuLayersNumeric === "number" && Number.isFinite(gpuLayersNumeric)
            ? gpuLayersNumeric
            : undefined;

    if (!ok) {
        const classification = classifyError(combinedRaw);
        return NextResponse.json(
            {
                variant,
                modelPath,
                gpuLayers,
                ok: false,
                exitCode,
                logs,
                ...classification,
            },
            { status: 500 }
        );
    }

    return NextResponse.json({
        variant,
        modelPath,
        gpuLayers,
        ok: true,
        exitCode: 0,
        logs,
    });
}
