import { NextRequest, NextResponse } from "next/server";

import { ensureAdmin } from "@/lib/admin-auth";

const resolveBackendUrl = (path: string) => {
    const base =
        process.env.NEXT_PUBLIC_API_BASE ||
        "https://gpw-analytics-starter-backend-1.onrender.com";
    return path.startsWith("http") ? path : `${base}${path.startsWith("/") ? path : `/${path}`}`;
};

export async function GET(req: NextRequest) {
    const auth = await ensureAdmin(req);
    if ("response" in auth) {
        return auth.response;
    }

    const backendResponse = await fetch(resolveBackendUrl("/companies/sync/schedule"), {
        cache: "no-store",
    });

    try {
        const payload = await backendResponse.json();
        if (!backendResponse.ok) {
            const errorPayload = payload && typeof payload === "object" ? payload : { error: "Nie udało się pobrać harmonogramu synchronizacji" };
            return NextResponse.json(errorPayload, { status: backendResponse.status });
        }
        return NextResponse.json(payload);
    } catch {
        const text = await backendResponse.text().catch(() => "");
        return NextResponse.json(
            {
                error:
                    text?.trim() ||
                    `Nie udało się pobrać harmonogramu synchronizacji (status ${backendResponse.status})`,
            },
            { status: backendResponse.status }
        );
    }
}

export async function POST(req: NextRequest) {
    const auth = await ensureAdmin(req);
    if ("response" in auth) {
        return auth.response;
    }

    let payload: unknown;
    try {
        payload = await req.json();
    } catch {
        payload = {};
    }

    const backendResponse = await fetch(resolveBackendUrl("/companies/sync/schedule"), {
        method: "POST",
        cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload ?? {}),
    });

    try {
        const responsePayload = await backendResponse.json();
        if (!backendResponse.ok) {
            const errorPayload =
                responsePayload && typeof responsePayload === "object"
                    ? responsePayload
                    : { error: "Nie udało się zaktualizować harmonogramu" };
            return NextResponse.json(errorPayload, { status: backendResponse.status });
        }
        return NextResponse.json(responsePayload);
    } catch {
        const text = await backendResponse.text().catch(() => "");
        return NextResponse.json(
            {
                error:
                    text?.trim() ||
                    `Nie udało się zaktualizować harmonogramu (status ${backendResponse.status})`,
            },
            { status: backendResponse.status }
        );
    }
}
