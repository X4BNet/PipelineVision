import { betterAuth } from "better-auth";
import { drizzleAdapter } from "better-auth/adapters/drizzle";
import { db } from "./db";
import * as schema from "./schema";

const trustedOrigins = [
  ...(process.env.VERCEL_URL ? [`https://${process.env.VERCEL_URL}`] : []),
  ...(process.env.BETTER_AUTH_URL ? [process.env.BETTER_AUTH_URL] : []),
  ...(process.env.TRUSTED_ORIGINS
    ? process.env.TRUSTED_ORIGINS.split(",").map((o) => o.trim())
    : []),
  "http://localhost:3000",
  "https://pipelinevision.app",
  "https://www.pipelinevision.app",
];

const uniqueTrustedOrigins = [...new Set(trustedOrigins.filter(Boolean))];

type GitHubProfile = {
  id: number | string;
  login: string;
  name?: string | null;
  email?: string | null;
  avatar_url?: string | null;
};

type GitHubEmail = {
  email: string;
  primary: boolean;
  verified: boolean;
};

async function fetchGitHubJson<T>(url: string, accessToken: string): Promise<T> {
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    },
  });

  if (!response.ok) {
    throw new Error(`GitHub API request failed: ${response.status}`);
  }

  return response.json() as Promise<T>;
}

async function fetchGitHubEmails(accessToken: string): Promise<GitHubEmail[]> {
  try {
    return await fetchGitHubJson<GitHubEmail[]>(
      "https://api.github.com/user/emails",
      accessToken,
    );
  } catch (error) {
    console.warn("Unable to fetch GitHub emails; using placeholder fallback", error);
    return [];
  }
}

async function getGitHubUserInfo(token: { accessToken?: string }) {
  if (!token.accessToken) {
    throw new Error("Missing GitHub access token");
  }

  const profile = await fetchGitHubJson<GitHubProfile>(
    "https://api.github.com/user",
    token.accessToken,
  );

  let email = profile.email ?? null;
  let emailVerified = Boolean(email);

  if (!email) {
    const emails = await fetchGitHubEmails(token.accessToken);
    const preferredEmail =
      emails.find((item) => item.primary && item.verified) ??
      emails.find((item) => item.verified);

    if (preferredEmail) {
      email = preferredEmail.email;
      emailVerified = preferredEmail.verified;
    }
  }

  email = email ?? `github-${profile.id}@pipelinevision.invalid`;

  return {
    user: {
      id: String(profile.id),
      name: profile.name || profile.login,
      email,
      image: profile.avatar_url ?? undefined,
      emailVerified,
    },
    data: profile,
  };
}

export const auth = betterAuth({
  database: drizzleAdapter(db, {
    provider: "pg",
    schema: {
      user: schema.authUser,
      session: schema.authSession,
      account: schema.authAccount,
      verification: schema.authVerification,
    },
  }),
  emailAndPassword: {
    enabled: true,
  },
  socialProviders: {
    github: {
      clientId: process.env.GITHUB_CLIENT_ID as string,
      clientSecret: process.env.GITHUB_CLIENT_SECRET as string,
      scope: ["read:org", "user:email"],
      getUserInfo: getGitHubUserInfo,
    },
  },
  session: {
    expiresIn: 60 * 60 * 24 * 7,
    cookieCache: {
      enabled: true,
      maxAge: 60 * 60 * 24 * 7,
    },
  },
  cookies: {
    sessionToken: {
      name: "better-auth.session_token",
      options: {
        httpOnly: true,
        sameSite: "lax",
        secure: process.env.NODE_ENV === "production",
        path: "/",
      },
    },
  },
  secret: process.env.BETTER_AUTH_SECRET as string,
  baseURL:
    process.env.BETTER_AUTH_URL ||
    (process.env.VERCEL_URL
      ? `https://${process.env.VERCEL_URL}`
      : "http://localhost:3000"),
  trustedOrigins: uniqueTrustedOrigins,
  callbacks: {
    after: [
      {
        matcher: (ctx: { path: string }) =>
          ctx.path === "/api/auth/callback/github",
        handler: async (ctx: { baseURL: unknown }) => {
          return Response.redirect(`${ctx.baseURL}/auth/callback`);
        },
      },
    ],
  },
});
