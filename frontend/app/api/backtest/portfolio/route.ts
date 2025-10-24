import { NextRequest, NextResponse } from "next/server";

import {
    createProxyErrorResponse,
    relayBackendResponse,
    resolveBackendUrl,
} from "@/app/api/_utils/backend";

export async function GET(req: NextRequest) {
    const backendUrl = resolveBackendUrl(`/backtest/portfolio${req.nextUrl.search}`);

    try {
        const backendResponse = await fetch(backendUrl, {
            headers: {
                accept: req.headers.get("accept") ?? "application/json",
            },
            cache: "no-store",
        });
        return await relayBackendResponse(backendResponse);
    } catch (error: unknown) {
        return createProxyErrorResponse(
            error,
            "Nie udało się pobrać danych backtestu (GET /backtest/portfolio)"
        );
    }
}

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

    const backendUrl = resolveBackendUrl("/backtest/portfolio");

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
            "Nie udało się uruchomić backtestu (POST /backtest/portfolio)"
        );
    }
}
