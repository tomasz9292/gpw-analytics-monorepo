import { createSessionToken } from "@/lib/session";
import { getOrCreateUserProfile } from "@/lib/user-storage";

const GOOGLE_TOKENINFO_URL = "https://oauth2.googleapis.com/tokeninfo";

type GoogleProfile = {
    sub: string;
    email: string | null;
    name: string | null;
    picture: string | null;
};

export const verifyGoogleCredential = async (credential: string): Promise<GoogleProfile> => {
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

export const authenticateWithGoogle = async (credential: string) => {
    const profile = await verifyGoogleCredential(credential);
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

    return { user, token, expires };
};
