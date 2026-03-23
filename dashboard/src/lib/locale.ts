export const DASHBOARD_LOCALES = ["en", "zh"] as const;

export type DashboardLocale = (typeof DASHBOARD_LOCALES)[number];

export const DASHBOARD_LOCALE_STORAGE_KEY = "memorose-dashboard-locale";

export function normalizeDashboardLocale(value: string | null | undefined): DashboardLocale {
  return value === "zh" ? "zh" : "en";
}
