import crypto from "node:crypto";

export const SESSION_COOKIE_NAME = "gpw_session";
const SESSION_TTL_MS = 1000 * 60 * 60 * 24 * 7; // 7 dni

export type SessionPayload = {
    sub: string;
    email: string | null;
    name: string | null;
    picture: string | null;
    provider: "google";
    issuedAt: number;
    expiresAt: number;
};

const getSecret = () => {
    const secret = process.env.AUTH_SECRET || process.env.NEXT_PUBLIC_AUTH_SECRET;
    if (!secret || !secret.trim()) {
        // Fallback, ale zachowujemy kompatybilność w trybie developerskim
        return "development-secret";
    }
    return secret.trim();
};

const sign = (payload: string, secret: string) => {
    return crypto.createHmac("sha256", secret).update(payload).digest("base64url");
};

export const createSessionToken = ({
    sub,
    email,
    name,
    picture,
}: {
    sub: string;
    email: string | null;
    name: string | null;
    picture: string | null;
}) => {
    const secret = getSecret();
    const issuedAt = Date.now();
    const expiresAt = issuedAt + SESSION_TTL_MS;
    const payload: SessionPayload = {
        sub,
        email,
        name,
        picture,
        provider: "google",
        issuedAt,
        expiresAt,
    };
    const serialized = JSON.stringify(payload);
    const signature = sign(serialized, secret);
    const token = `${Buffer.from(serialized).toString("base64url")}.${signature}`;
    return { token, expires: new Date(expiresAt) };
};

export const parseSessionToken = (token: string | undefined | null): SessionPayload | null => {
    if (!token) return null;
    const [payloadPart, signature] = token.split(".");
    if (!payloadPart || !signature) return null;
    try {
        const serialized = Buffer.from(payloadPart, "base64url").toString("utf8");
        const secret = getSecret();
        const expected = sign(serialized, secret);
        const safeExpected = Buffer.from(expected);
        const safeActual = Buffer.from(signature);
        if (
            safeExpected.length !== safeActual.length ||
            !crypto.timingSafeEqual(safeExpected, safeActual)
        ) {
            return null;
        }
        const payload = JSON.parse(serialized) as SessionPayload;
        if (!payload || typeof payload !== "object") {
            return null;
        }
        if (!payload.sub || typeof payload.sub !== "string") {
            return null;
        }
        if (typeof payload.expiresAt !== "number" || Date.now() > payload.expiresAt) {
            return null;
        }
        return {
            sub: payload.sub,
            email: payload.email ?? null,
            name: payload.name ?? null,
            picture: payload.picture ?? null,
            provider: "google",
            issuedAt: payload.issuedAt,
            expiresAt: payload.expiresAt,
        };
    } catch {
        return null;
    }
};
