const TOKEN_KEY = "memorose_dashboard_token";
const MUST_CHANGE_KEY = "memorose_must_change_password";

export function getToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token: string) {
  localStorage.setItem(TOKEN_KEY, token);
}

export function clearToken() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(MUST_CHANGE_KEY);
}

export function setMustChangePassword(value: boolean) {
  if (typeof window !== "undefined") {
    localStorage.setItem(MUST_CHANGE_KEY, value ? "true" : "false");
  }
}

export function getMustChangePassword(): boolean {
  if (typeof window === "undefined") return false;
  return localStorage.getItem(MUST_CHANGE_KEY) === "true";
}

export function isTokenExpired(): boolean {
  const token = getToken();
  if (!token) return true;
  try {
    const payload = JSON.parse(atob(token.split(".")[1]));
    return payload.exp * 1000 < Date.now();
  } catch {
    return true;
  }
}

export function isAuthenticated(): boolean {
  return !!getToken() && !isTokenExpired();
}
