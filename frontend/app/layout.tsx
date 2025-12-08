import type { Metadata } from "next";
import { ThemeProvider } from "@/components/theme-provider";
import "./globals.css";
import DevAutoLogin from "./components/DevAutoLogin";

export const metadata: Metadata = {
  title: "GPW Analytics",
  description:
    "Panel analityczny GPW z integracją notowań, rankingów score oraz simulatorem portfela.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="pl">
      <body className="antialiased">
        <DevAutoLogin />
        <ThemeProvider>{children}</ThemeProvider>
      </body>
    </html>
  );
}
