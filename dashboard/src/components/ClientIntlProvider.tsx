"use client";

import { NextIntlClientProvider } from 'next-intl';
import { useEffect, useState } from 'react';
import enMessages from '../../messages/en.json';
import zhMessages from '../../messages/zh.json';

export function ClientIntlProvider({ children }: { children: React.ReactNode }) {
  const [locale, setLocale] = useState('en');
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    const saved = localStorage.getItem('locale') || 'en';
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setLocale(saved);
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setMounted(true);
  }, []);

  if (!mounted) {
    return <div className="h-screen w-screen bg-background"></div>; // Prevents hydration mismatch
  }

  const messages = locale === 'zh' ? zhMessages : enMessages;

  return (
    <NextIntlClientProvider locale={locale} messages={messages}>
      {children}
    </NextIntlClientProvider>
  );
}
