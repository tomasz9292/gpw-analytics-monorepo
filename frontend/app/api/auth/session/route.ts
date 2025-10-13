import { NextRequest, NextResponse } from "next/server";
import { parseSessionToken, SESSION_COOKIE_NAME } from "@/lib/session";
import { getOrCreateUserProfile } from "@/lib/user-storage";

export async function GET(req: NextRequest) {
    const token = req.cookies.get(SESSION_COOKIE_NAME)?.value;
    const session = parseSessionToken(token);
    if (!session) {
        return NextResponse.json({ authenticated: false }, { status: 401 });
    }

    const profile = await getOrCreateUserProfile({
        id: session.sub,
        email: session.email,
        name: session.name,
        picture: session.picture,
    });

    return NextResponse.json(profile);
}
