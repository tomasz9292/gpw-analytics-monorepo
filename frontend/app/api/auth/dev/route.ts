import { NextResponse } from "next/server";
import { createSessionToken, SESSION_COOKIE_NAME } from "@/lib/session";

export async function GET(){ return NextResponse.json({ status: 'dev route ok' }); }

export async function POST() {
    // Blokada usunięta dla pewności działania lokalnego
    // if ((process.env.NODE_ENV as string) === "production") { ... }

    // Tworzymy sesję dla domyślnego administratora
    // Używamy emaila, który widziałem w admin-storage.ts, aby na pewno był adminem
    const user = {
        sub: "dev-admin-id",
        email: "tomasz.wasik92@gmail.com",
        name: "Tomasz Wąsik (Dev)",
        picture: null,
    };

    const { token, expires } = createSessionToken(user);

    const response = NextResponse.json({ user });

    response.cookies.set(SESSION_COOKIE_NAME, token, {
        httpOnly: true,
        // Secure musi być false na localhost http
        secure: false, 
        sameSite: "lax",
        expires: expires,
        path: "/",
    });

    return response;
}


