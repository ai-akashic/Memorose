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
        <Card className="glass-card bg-black/40 border-white/10 shadow-2xl relative overflow-hidden group">
            <div className="absolute inset-0 bg-gradient-to-br from-white/[0.05] to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-500 pointer-events-none" />
            <CardContent className="pt-8 pb-8 px-8 relative z-10">
                <form onSubmit={handleLogin}>
                    <div className="grid gap-6">
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
                                className="h-12 bg-black/20 border-white/10 focus-visible:ring-1 focus-visible:ring-primary/50 focus-visible:border-primary/50 transition-all font-mono text-sm shadow-inner"
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
                                className="h-12 bg-black/20 border-white/10 focus-visible:ring-1 focus-visible:ring-primary/50 focus-visible:border-primary/50 transition-all font-mono text-sm shadow-inner"
                            />
                        </div>

                        {error && (
                            <motion.div 
                                initial={{ opacity: 0, height: 0 }} 
                                animate={{ opacity: 1, height: "auto" }}
                                className="flex items-center gap-2 rounded-lg bg-destructive/10 px-4 py-3 text-sm text-destructive border border-destructive/20 shadow-[0_0_15px_rgba(220,38,38,0.1)]"
                            >
                                <AlertCircle className="h-4 w-4 shrink-0" />
                                <span>{error}</span>
                            </motion.div>
                        )}

                        <Button
                            type="submit"
                            className="w-full h-12 font-medium bg-primary hover:bg-primary/90 text-primary-foreground shadow-[0_0_20px_rgba(255,255,255,0.2)] hover:shadow-[0_0_30px_rgba(255,255,255,0.3)] transition-all duration-300 relative overflow-hidden group/btn mt-2"
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
