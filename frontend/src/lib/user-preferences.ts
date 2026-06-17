import { headers } from "next/headers";
import type { Preference } from "@/app/contexts/PreferenceContext";

interface ApiResponse {
  success: boolean;
  preference: Preference;
}

export async function getPreference(): Promise<Preference | null> {
  try {
    const requestHeaders = await headers();
    const baseUrl = process.env.NEXT_INTERNAL_BASE_URL || "http://127.0.0.1:3000";

    const cookie = requestHeaders.get("cookie");
    const res = await fetch(`${baseUrl}/api/account/preferences`, {
      cache: "no-store",
      headers: cookie ? { cookie } : undefined,
    });

    if (!res.ok) {
      return null;
    }

    const data: ApiResponse = await res.json();
    return data.success ? data.preference : null;
  } catch {
    return null;
  }
}
