import { NextRequest, NextResponse } from "next/server";

import { ensureAdmin } from "@/lib/admin-auth";

const resolveBackendUrl = (path: string) => {
    const base =
        process.env.NEXT_PUBLIC_API_BASE ||
        "https://gpw-analytics-starter-backend-1.onrender.com";
    return path.startsWith("http") ? path : `${base}${path.startsWith("/") ? path : `/${path}`}`;
};

export async function POST(req: NextRequest) {
    const auth = await ensureAdmin(req);
    if ("response" in auth) {
        return auth.response;
    }

    const limit = req.nextUrl.searchParams.get("limit");
    const search = limit ? `?limit=${encodeURIComponent(limit)}` : "";
    const backendResponse = await fetch(
        resolveBackendUrl(`/companies/sync/background${search}`),
        {
            method: "POST",
            cache: "no-store",
        }
    );

    try {
        const payload = await backendResponse.json();
        if (!backendResponse.ok) {
            const errorPayload = payload && typeof payload === "object" ? payload : { error: "Nie udało się uruchomić synchronizacji" };
            return NextResponse.json(errorPayload, { status: backendResponse.status });
        }
        return NextResponse.json(payload);
    } catch {
        const text = await backendResponse.text().catch(() => "");
        return NextResponse.json(
            {
                error:
                    text?.trim() ||
                    `Nie udało się uruchomić synchronizacji (status ${backendResponse.status})`,
            },
            { status: backendResponse.status }
        );
    }
}
