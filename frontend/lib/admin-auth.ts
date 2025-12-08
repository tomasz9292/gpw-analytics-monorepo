import { NextRequest, NextResponse } from "next/server";
import { SESSION_COOKIE_NAME, parseSessionToken, SessionPayload } from "@/lib/session";
import { isAdminEmail } from "@/lib/admin-storage";

export type AdminAuthResult =
    | { session: SessionPayload }
    | { response: NextResponse<{ error: string }> };

export const ensureAdmin = async (req: NextRequest): Promise<AdminAuthResult> => {
    // --- DEVELOPMENT OVERRIDE ---
    if (process.env.NODE_ENV !== "production") {
        return {
            session: {
                sub: "dev-admin-id",
                email: "dev@localhost",
                name: "Development Admin",
                picture: null,
                provider: "google",
                issuedAt: Date.now(),
                expiresAt: Date.now() + 1000 * 60 * 60 * 24,
            },
        };
    }
    // ----------------------------

    const token = req.cookies.get(SESSION_COOKIE_NAME)?.value;
    const session = parseSessionToken(token);
    if (!session) {
        return { response: NextResponse.json({ error: "Brak autoryzacji" }, { status: 401 }) };
    }
    const email = session.email ? session.email.trim().toLowerCase() : null;
    if (!email) {
        return { response: NextResponse.json({ error: "Brak uprawnień administratora" }, { status: 403 }) };
    }
    const allowed = await isAdminEmail(email);
    if (!allowed) {
        return { response: NextResponse.json({ error: "Brak uprawnień administratora" }, { status: 403 }) };
    }
    return { session };
};
