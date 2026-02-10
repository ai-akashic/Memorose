"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { api } from "@/lib/api";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Loader2, AlertCircle } from "lucide-react";
import { setToken } from "@/lib/auth";

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
        <Card className="border-primary/20 backdrop-blur-sm shadow-2xl bg-card/95">
            <CardContent className="pt-6 pb-6">
                <form onSubmit={handleLogin}>
                    <div className="grid gap-5">
                        <div className="grid gap-2">
                            <Label htmlFor="username" className="text-sm">
                                Username
                            </Label>
                            <Input
                                id="username"
                                type="text"
                                placeholder="admin"
                                value={username}
                                onChange={(e) => setUsername(e.target.value)}
                                disabled={loading}
                                className="h-11 bg-background/50"
                            />
                        </div>
                        <div className="grid gap-2">
                            <Label htmlFor="password" className="text-sm">
                                Password
                            </Label>
                            <Input
                                id="password"
                                type="password"
                                placeholder="••••••••"
                                value={password}
                                onChange={(e) => setPassword(e.target.value)}
                                disabled={loading}
                                className="h-11 bg-background/50"
                            />
                        </div>

                        {error && (
                            <div className="flex items-center gap-2 rounded-lg bg-destructive/10 px-3 py-2.5 text-sm text-destructive border border-destructive/20">
                                <AlertCircle className="h-4 w-4 shrink-0" />
                                <span>{error}</span>
                            </div>
                        )}

                        <Button
                            type="submit"
                            className="w-full h-11 font-medium bg-primary hover:bg-primary/90"
                            disabled={loading}
                        >
                            {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                            {loading ? "Signing in..." : "Sign in"}
                        </Button>
                    </div>
                </form>
            </CardContent>
        </Card>
    );
}
