import { NextRequest, NextResponse } from "next/server";
import { authenticateWithGoogle } from "@/lib/google-auth";
import { SESSION_COOKIE_NAME } from "@/lib/session";

const buildRedirectUrl = (req: NextRequest) => {
    const url = new URL(req.nextUrl.origin);
    url.pathname = "/";
    return url;
};

export async function POST(req: NextRequest) {
    try {
        const formData = await req.formData();
        const credential = formData.get("credential");
        const csrfBody = formData.get("g_csrf_token");
        const csrfCookie = req.cookies.get("g_csrf_token")?.value;

        if (typeof csrfBody !== "string" || typeof csrfCookie !== "string" || csrfBody !== csrfCookie) {
            throw new Error("Nieprawidłowy token CSRF logowania Google");
        }

        if (typeof credential !== "string" || !credential) {
            throw new Error("Brak tokenu logowania");
        }

        const { token, expires } = await authenticateWithGoogle(credential);
        const redirectUrl = buildRedirectUrl(req);
        redirectUrl.searchParams.set("auth", "google_success");

        const response = NextResponse.redirect(redirectUrl, { status: 303 });
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
        const message =
            error instanceof Error ? error.message : "Logowanie przez Google nie powiodło się.";
        const redirectUrl = buildRedirectUrl(req);
        redirectUrl.searchParams.set("auth_error", message);

        const response = NextResponse.redirect(redirectUrl, { status: 303 });
        response.cookies.set({
            name: SESSION_COOKIE_NAME,
            value: "",
            expires: new Date(0),
            path: "/",
        });
        return response;
    }
}
