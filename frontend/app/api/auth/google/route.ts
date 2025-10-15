import { NextRequest, NextResponse } from "next/server";
import { authenticateWithGoogle } from "@/lib/google-auth";
import { SESSION_COOKIE_NAME } from "@/lib/session";

export async function POST(req: NextRequest) {
    try {
        const body = (await req.json()) as { credential?: unknown };
        const credential = typeof body.credential === "string" ? body.credential : "";
        if (!credential) {
            return NextResponse.json({ error: "Brak tokenu logowania" }, { status: 400 });
        }

        const { user, token, expires } = await authenticateWithGoogle(credential);

        const response = NextResponse.json({ user });
        response.cookies.set({
            name: SESSION_COOKIE_NAME,
            value: token,
            httpOnly: true,
            sameSite: "lax",
            secure: process.env.NODE_ENV === "production",
            expires,
            path: "/",
        });
        return response;
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : "Nieznany błąd logowania";
        return NextResponse.json({ error: message }, { status: 401 });
    }
}
