"use client";
import { useEffect } from 'react';

export default function DevAutoLogin(){
  useEffect(() => {
    if (typeof window === 'undefined') return;
    const host = window.location.hostname;
    if (host !== 'localhost' && host !== '127.0.0.1') return;
    (async () => {
      try {
        const sess = await fetch('/api/auth/session');
        if (sess.ok) {
          const data = await sess.json();
          // Naprawiona struktura: email jest w data.user.email
          if (data?.user?.email) return; 
        }
        const res = await fetch('/api/auth/dev', { method: 'POST' });
        if (res.ok) {
          window.location.reload();
        }
      } catch {
        // ignore
      }
    })();
  }, []);
  return null;
}
