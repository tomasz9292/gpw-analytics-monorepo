import { NextRequest, NextResponse } from "next/server";

import { addAdmin, AdminAlreadyExistsError, getAdminList } from "@/lib/admin-storage";
import { ensureAdmin } from "@/lib/admin-auth";

export async function GET(req: NextRequest) {
    const auth = await ensureAdmin(req);
    if ("response" in auth) {
        return auth.response;
    }

    const admins = await getAdminList();
    return NextResponse.json({ admins });
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
    const email =
        payload && typeof payload === "object" && "email" in payload
            ? (payload as { email?: unknown }).email
            : undefined;
    if (typeof email !== "string" || !email.trim()) {
        return NextResponse.json(
            { error: "Podaj poprawny adres e-mail administratora." },
            { status: 400 }
        );
    }

    try {
        const admins = await addAdmin(email, auth.session.email ?? null);
        return NextResponse.json({ admins });
    } catch (error: unknown) {
        if (error instanceof AdminAlreadyExistsError) {
            return NextResponse.json({ error: error.message }, { status: 409 });
        }
        if (error instanceof Error) {
            return NextResponse.json({ error: error.message }, { status: 400 });
        }
        return NextResponse.json(
            { error: "Nie udało się dodać administratora" },
            { status: 500 }
        );
    }
}
