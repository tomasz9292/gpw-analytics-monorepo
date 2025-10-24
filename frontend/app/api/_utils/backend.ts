import { NextResponse } from "next/server";

const DEFAULT_BACKEND_BASE = "https://gpw-analytics-starter-backend-1.onrender.com";

const getBackendBase = () => {
    const base = process.env.NEXT_PUBLIC_API_BASE || DEFAULT_BACKEND_BASE;
    return base.endsWith("/") ? base.slice(0, -1) : base;
};

export const resolveBackendUrl = (path: string) => {
    if (!path) {
        return getBackendBase();
    }
    if (path.startsWith("http://") || path.startsWith("https://")) {
        return path;
    }
    const normalized = path.startsWith("/") ? path : `/${path}`;
    return `${getBackendBase()}${normalized}`;
};

export const relayBackendResponse = async (response: Response) => {
    const headers = new Headers(response.headers);
    headers.set("cache-control", "no-store");
    headers.delete("content-encoding");
    headers.delete("content-length");
    headers.delete("transfer-encoding");
    const body = await response.arrayBuffer();
    return new NextResponse(body, {
        status: response.status,
        headers,
    });
};

export const createProxyErrorResponse = (error: unknown, fallbackMessage: string) => {
    const detail = error instanceof Error && error.message ? error.message : null;
    const message = detail ? `${fallbackMessage}: ${detail}` : fallbackMessage;
    return NextResponse.json({ error: message }, { status: 502 });
};
