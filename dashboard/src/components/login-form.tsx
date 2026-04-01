"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { api } from "@/lib/api";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Loader2, AlertCircle, ArrowRight } from "lucide-react";
import { setToken } from "@/lib/auth";
import { motion } from "framer-motion";

export function LoginForm() {
    const router = useRouter();
    const [username, setUsername] = useState("admin");
    const [password, setPassword] = useState("");
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const handleLogin = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);
        setError(null);

        try {
            const res = await api.login(username, password);
            setToken(res.token);
            router.push("/cluster/");
        } catch (err) {
            setError(err instanceof Error ? err.message : "Failed to login");
        } finally {
            setLoading(false);
        }
    };

    return (
        <Card className="glass-card group relative overflow-hidden border-white/10 bg-black/20 shadow-[0_24px_70px_rgba(0,0,0,0.32)]">
            <div className="absolute inset-x-6 top-0 h-px bg-gradient-to-r from-transparent via-white/40 to-transparent pointer-events-none" />
            <CardContent className="relative z-10 px-8 pb-8 pt-8">
                <form onSubmit={handleLogin}>
                    <div className="grid gap-6">
                        <div className="space-y-2">
                            <p className="text-[11px] font-medium uppercase tracking-[0.24em] text-muted-foreground">
                                Secure sign-in
                            </p>
                            <p className="text-sm leading-6 text-foreground/75">
                                Authenticate into the dashboard to inspect cluster health, knowledge flow, and operator settings.
                            </p>
                        </div>
                        <div className="grid gap-2">
                            <Label htmlFor="username" className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">
                                System ID
                            </Label>
                            <Input
                                id="username"
                                type="text"
                                placeholder="admin"
                                value={username}
                                onChange={(e) => setUsername(e.target.value)}
                                disabled={loading}
                                className="h-12 bg-black/20 font-mono text-sm"
                            />
                        </div>
                        <div className="grid gap-2">
                            <Label htmlFor="password" className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">
                                Access Token
                            </Label>
                            <Input
                                id="password"
                                type="password"
                                placeholder="••••••••"
                                value={password}
                                onChange={(e) => setPassword(e.target.value)}
                                disabled={loading}
                                className="h-12 bg-black/20 font-mono text-sm"
                            />
                        </div>

                        {error && (
                            <motion.div 
                                initial={{ opacity: 0, height: 0 }} 
                                animate={{ opacity: 1, height: "auto" }}
                                className="flex items-center gap-2 rounded-xl border border-destructive/20 bg-destructive/10 px-4 py-3 text-sm text-destructive shadow-[0_0_15px_rgba(220,38,38,0.1)]"
                            >
                                <AlertCircle className="h-4 w-4 shrink-0" />
                                <span>{error}</span>
                            </motion.div>
                        )}

                        <Button
                            type="submit"
                            className="mt-2 h-12 w-full font-medium"
                            disabled={loading}
                        >
                            <span className="relative z-10 flex items-center gap-2">
                                {loading ? (
                                    <>
                                        <Loader2 className="h-4 w-4 animate-spin" />
                                        Authenticating...
                                    </>
                                ) : (
                                    <>
                                        Initialize Link
                                        <ArrowRight className="h-4 w-4 group-hover/btn:translate-x-1 transition-transform" />
                                    </>
                                )}
                            </span>
                        </Button>
                    </div>
                </form>
            </CardContent>
        </Card>
    );
}
