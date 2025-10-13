import { NextRequest, NextResponse } from "next/server";
import { createSessionToken, SESSION_COOKIE_NAME } from "@/lib/session";
import { getOrCreateUserProfile } from "@/lib/user-storage";

const GOOGLE_TOKENINFO_URL = "https://oauth2.googleapis.com/tokeninfo";

const fetchGoogleProfile = async (credential: string) => {
    const url = `${GOOGLE_TOKENINFO_URL}?id_token=${encodeURIComponent(credential)}`;
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
        throw new Error("Nie udało się zweryfikować tokenu Google");
    }
    const payload = (await response.json()) as Record<string, unknown>;
    const aud = typeof payload.aud === "string" ? payload.aud : "";
    const clientId = process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID || process.env.GOOGLE_CLIENT_ID || "";
    if (!clientId || aud !== clientId) {
        throw new Error("Nieprawidłowy identyfikator klienta Google");
    }
    const sub = typeof payload.sub === "string" ? payload.sub : "";
    if (!sub) {
        throw new Error("Brak identyfikatora użytkownika Google");
    }
    return {
        sub,
        email: typeof payload.email === "string" ? payload.email : null,
        name:
            typeof payload.name === "string"
                ? payload.name
                : typeof payload.given_name === "string"
                ? payload.given_name
                : null,
        picture: typeof payload.picture === "string" ? payload.picture : null,
    };
};

export async function POST(req: NextRequest) {
    try {
        const body = (await req.json()) as { credential?: unknown };
        const credential = typeof body.credential === "string" ? body.credential : "";
        if (!credential) {
            return NextResponse.json({ error: "Brak tokenu logowania" }, { status: 400 });
        }

        const profile = await fetchGoogleProfile(credential);
        const { user } = await getOrCreateUserProfile({
            id: profile.sub,
            email: profile.email,
            name: profile.name,
            picture: profile.picture,
        });

        const { token, expires } = createSessionToken({
            sub: user.id,
            email: user.email,
            name: user.name,
            picture: user.picture,
        });

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
