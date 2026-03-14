"use client";

import { NextIntlClientProvider } from 'next-intl';
import { useSyncExternalStore } from 'react';
import enMessages from '../../messages/en.json';
import zhMessages from '../../messages/zh.json';

export function ClientIntlProvider({ children }: { children: React.ReactNode }) {
  const mounted = useSyncExternalStore(
    () => () => {},
    () => true,
    () => false
  );

  if (!mounted) {
    return <div className="h-screen w-screen bg-background"></div>; // Prevents hydration mismatch
  }

  const locale = localStorage.getItem('locale') || 'en';
  const messages = locale === 'zh' ? zhMessages : enMessages;

  return (
    <NextIntlClientProvider locale={locale} messages={messages}>
      {children}
    </NextIntlClientProvider>
  );
}
