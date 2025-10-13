import { NextRequest, NextResponse } from "next/server";
import { parseSessionToken, SESSION_COOKIE_NAME } from "@/lib/session";
import { getOrCreateUserProfile, updateUserProfile } from "@/lib/user-storage";

const unauthorized = () =>
    NextResponse.json({ error: "Brak autoryzacji" }, { status: 401 });

export async function GET(req: NextRequest) {
    const token = req.cookies.get(SESSION_COOKIE_NAME)?.value;
    const session = parseSessionToken(token);
    if (!session) {
        return unauthorized();
    }

    const profile = await getOrCreateUserProfile({
        id: session.sub,
        email: session.email,
        name: session.name,
        picture: session.picture,
    });

    return NextResponse.json(profile);
}

export async function PUT(req: NextRequest) {
    const token = req.cookies.get(SESSION_COOKIE_NAME)?.value;
    const session = parseSessionToken(token);
    if (!session) {
        return unauthorized();
    }

    let payload: unknown;
    try {
        payload = await req.json();
    } catch {
        payload = {};
    }

    const profile = await updateUserProfile(
        session.sub,
        {
            email: session.email,
            name: session.name,
            picture: session.picture,
        },
        payload
    );

    return NextResponse.json(profile);
}
