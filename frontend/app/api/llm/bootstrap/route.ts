import { NextResponse } from "next/server";
import path from "path";
import { access } from "fs/promises";
import { spawn, spawnSync } from "child_process";

type LlmBootstrapVariant = "bash" | "powershell";

type PythonCommand = {
    command: string;
    args: string[];
};

const MODEL_PATH_REGEX = /^MODEL_PATH=(.+)$/m;
const GPU_LAYERS_REGEX = /^GPU_LAYERS=(.+)$/m;
const SCRIPT_PATH = path.resolve(process.cwd(), "..", "scripts", "llm_auto_setup.py");

export const runtime = "nodejs";

const candidateCommands = (): PythonCommand[] => {
    const items: PythonCommand[] = [];
    const fromEnv = [
        process.env.LLM_BOOTSTRAP_PYTHON,
        process.env.PYTHON,
    ].filter((value): value is string => Boolean(value && value.trim()));

    for (const command of fromEnv) {
        items.push({ command, args: [] });
    }

    if (process.platform === "win32") {
        items.push({ command: "py", args: ["-3"] });
    }

    items.push({ command: "python3", args: [] }, { command: "python", args: [] });

    return items;
};

const detectVariant = (): LlmBootstrapVariant =>
    process.platform === "win32" ? "powershell" : "bash";

const extractValue = (pattern: RegExp, source: string): string | undefined => {
    const match = source.match(pattern);
    if (!match || match.length < 2) {
        return undefined;
    }
    return match[1].trim();
};

const findPythonCommand = (): PythonCommand => {
    for (const candidate of candidateCommands()) {
        try {
            const result = spawnSync(candidate.command, [...candidate.args, "--version"], {
                stdio: "ignore",
            });
            if (!result.error && result.status === 0) {
                return candidate;
            }
        } catch {
            // Ignore and try next candidate.
        }
    }

    throw new Error(
        "Nie znaleziono interpretera Python. Zainstaluj python3 lub ustaw zmienną LLM_BOOTSTRAP_PYTHON."
    );
};

export async function POST() {
    try {
        await access(SCRIPT_PATH);
    } catch (error) {
        const message =
            error && typeof error === "object" && "code" in error && (error as { code?: string }).code === "ENOENT"
                ? "Brak skryptu automatycznej instalacji (scripts/llm_auto_setup.py)."
                : "Nie udało się zweryfikować skryptu instalacyjnego.";
        return NextResponse.json({ error: message }, { status: 500 });
    }

    let python: PythonCommand;
    try {
        python = findPythonCommand();
    } catch (error) {
        const message =
            error instanceof Error && error.message
                ? error.message
                : "Nie znaleziono interpretera Python.";
        return NextResponse.json({ error: message }, { status: 500 });
    }

    const child = spawn(python.command, [...python.args, SCRIPT_PATH, "--yes"], {
        cwd: path.dirname(SCRIPT_PATH),
        env: {
            ...process.env,
            PYTHONUNBUFFERED: "1",
        },
    });

    let stdout = "";
    let stderr = "";

    if (child.stdout) {
        child.stdout.setEncoding("utf-8");
        child.stdout.on("data", (chunk: string) => {
            stdout += chunk;
        });
    }

    if (child.stderr) {
        child.stderr.setEncoding("utf-8");
        child.stderr.on("data", (chunk: string) => {
            stderr += chunk;
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
                error: "Nie udało się uruchomić procesu instalacji.",
            },
            { status: 500 }
        );
    }

    if (exitCode !== 0) {
        const message =
            stderr.trim() ||
            "Instalator lokalnego modelu LLM zakończył się błędem. Sprawdź logi serwera.";
        return NextResponse.json(
            {
                error: message,
                logs: stdout,
                details: stderr,
            },
            { status: 500 }
        );
    }

    const variant = detectVariant();
    const modelPath = extractValue(MODEL_PATH_REGEX, stdout);
    const gpuLayersRaw = extractValue(GPU_LAYERS_REGEX, stdout);
    const gpuLayers =
        gpuLayersRaw && !Number.isNaN(Number(gpuLayersRaw))
            ? Number(gpuLayersRaw)
            : undefined;

    return NextResponse.json({
        variant,
        modelPath,
        gpuLayers,
        logs: stdout,
    });
}
