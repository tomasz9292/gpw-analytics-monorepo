import { rm, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
const cacheDir = path.join(projectRoot, ".next", "cache");

async function removeCacheDir() {
    let exists = true;
    try {
        await stat(cacheDir);
    } catch (error) {
        if (error?.code === "ENOENT") {
            exists = false;
        } else {
            throw error;
        }
    }

    if (!exists) {
        return;
    }

    try {
        await rm(cacheDir, { recursive: true, force: true });
        process.stdout.write(`Removed ${path.relative(projectRoot, cacheDir)} directory.\n`);
    } catch (error) {
        process.stderr.write(
            `Warning: failed to remove ${path.relative(projectRoot, cacheDir)} directory.\n`
        );
        throw error;
    }
}

removeCacheDir().catch((error) => {
    process.exitCode = 1;
    console.error(error);
});
