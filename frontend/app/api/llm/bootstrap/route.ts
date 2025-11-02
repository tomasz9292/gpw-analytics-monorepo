import { NextResponse } from "next/server";
import path from "path";
import { access } from "fs/promises";
import { spawn } from "child_process";

type LlmBootstrapVariant = "bash" | "powershell";

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
        return NextResponse.json({ error: message }, { status: 500 });
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
