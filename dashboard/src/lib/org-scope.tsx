"use client";

import { createContext, useContext, useEffect, useState } from "react";

const ORG_SCOPE_STORAGE_KEY = "memorose-dashboard-org-scope";
export const DEFAULT_ORG_ID = "default";

const OrgScopeContext = createContext<{
  orgId: string;
  setOrgId: (value: string) => void;
}>({
  orgId: DEFAULT_ORG_ID,
  setOrgId: () => {},
});

export function OrgScopeProvider({ children }: { children: React.ReactNode }) {
  const [orgId, setOrgId] = useState(() => {
    if (typeof window === "undefined") {
      return DEFAULT_ORG_ID;
    }
    return window.localStorage.getItem(ORG_SCOPE_STORAGE_KEY) ?? DEFAULT_ORG_ID;
  });

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const normalized = orgId.trim();
    window.localStorage.setItem(ORG_SCOPE_STORAGE_KEY, normalized || DEFAULT_ORG_ID);
  }, [orgId]);

  return (
    <OrgScopeContext.Provider value={{ orgId: orgId.trim() || DEFAULT_ORG_ID, setOrgId }}>
      {children}
    </OrgScopeContext.Provider>
  );
}

export function useOrgScope() {
  return useContext(OrgScopeContext);
}
