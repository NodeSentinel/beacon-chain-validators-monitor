'use client';

import { backButton } from '@tma.js/sdk-react';
import { useRouter, usePathname } from 'next/navigation';
import { useEffect } from 'react';

/**
 * BackButton integration with Next.js router
 * Shows/hides Telegram's back button and handles navigation
 *
 * Usage: Place this component in your layout or page
 */
export function BackButtonBinder() {
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    // Hide back button on home page, show on other pages
    const isHomePage = pathname === '/';

    if (isHomePage) {
      backButton.hide();
    } else {
      backButton.show();
    }
  }, [pathname]);

  useEffect(() => {
    // Handle back button clicks
    const handleClick = () => {
      router.back();
    };

    const unsubscribe = backButton.onClick(handleClick);

    return () => {
      unsubscribe();
    };
  }, [router]);

  return null;
}
