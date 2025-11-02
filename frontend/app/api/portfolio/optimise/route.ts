import { NextRequest, NextResponse } from "next/server";

import {
    createProxyErrorResponse,
    relayBackendResponse,
    resolveBackendUrl,
} from "@/app/api/_utils/backend";

export async function POST(req: NextRequest) {
    let payload: unknown;
    try {
        payload = await req.json();
    } catch {
        return NextResponse.json(
            { error: "Niepoprawny JSON w żądaniu" },
            { status: 400 }
        );
    }

    const backendUrl = resolveBackendUrl("/portfolio/optimise");

    try {
        const backendResponse = await fetch(backendUrl, {
            method: "POST",
            headers: {
                "content-type": "application/json",
                accept: req.headers.get("accept") ?? "application/json",
            },
            body: JSON.stringify(payload ?? {}),
        });
        return await relayBackendResponse(backendResponse);
    } catch (error: unknown) {
        return createProxyErrorResponse(
            error,
            "Nie udało się uruchomić optymalizacji portfela (POST /portfolio/optimise)"
        );
    }
}
