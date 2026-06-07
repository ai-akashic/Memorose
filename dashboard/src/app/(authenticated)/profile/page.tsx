"use client";

import { useCallback, useEffect, useState } from "react";
import { api } from "@/lib/api";
import type {
  ProfileSlot,
  ProfileSlotValue,
  ProfileReviewRecord,
  ProfileValueStatus,
} from "@/lib/types";
import { EmptyState } from "@/components/empty-state";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  UserCircle,
  Search,
  Pin,
  PinOff,
  Archive,
  ChevronDown,
  ChevronRight,
  Loader2,
} from "lucide-react";

function statusVariant(status: ProfileValueStatus): "default" | "secondary" | "destructive" | "outline" {
  switch (status) {
    case "active":
      return "default";
    case "negated":
      return "destructive";
    case "obsoleted":
    case "historical":
    default:
      return "outline";
  }
}

function ConfidenceBar({ value }: { value: number }) {
  const pct = Math.round(Math.min(1, Math.max(0, value)) * 100);
  return (
    <div className="flex items-center gap-2">
      <div className="h-1.5 w-20 overflow-hidden rounded-full bg-white/10">
        <div className="h-full rounded-full bg-primary" style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-muted-foreground">{pct}%</span>
    </div>
  );
}

function SlotRow({
  slot,
  userId,
  onChanged,
}: {
  slot: ProfileSlot;
  userId: string;
  onChanged: () => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const [busy, setBusy] = useState(false);
  const pinned = slot.state === "manually_pinned";

  const act = useCallback(
    async (op: string, canonical_value?: string) => {
      setBusy(true);
      try {
        await api.patchProfile(userId, { slot_key: slot.slot_key, op, canonical_value });
        onChanged();
      } catch (e) {
        console.error(e);
      } finally {
        setBusy(false);
      }
    },
    [userId, slot.slot_key, onChanged]
  );

  const activeValues = slot.values.filter((v) => v.status === "active");
  const summary = activeValues.slice(0, 3).map((v) => v.value).join("; ") || "—";

  return (
    <Card className="overflow-hidden">
      <CardHeader
        className="cursor-pointer flex-row items-center justify-between gap-3 py-3"
        onClick={() => setExpanded((v) => !v)}
      >
        <div className="flex items-center gap-2 min-w-0">
          {expanded ? (
            <ChevronDown className="size-4 shrink-0 text-muted-foreground" />
          ) : (
            <ChevronRight className="size-4 shrink-0 text-muted-foreground" />
          )}
          <div className="min-w-0">
            <CardTitle className="text-sm font-medium truncate">{slot.attribute}</CardTitle>
            <p className="text-xs text-muted-foreground truncate">{summary}</p>
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          {pinned && (
            <Badge variant="secondary" className="gap-1">
              <Pin className="size-3" /> pinned
            </Badge>
          )}
          {slot.state === "pending_review" && <Badge variant="outline">review</Badge>}
          <Badge variant="outline">{activeValues.length} active</Badge>
        </div>
      </CardHeader>
      {expanded && (
        <CardContent className="border-t border-white/5 pt-3 space-y-2">
          {slot.values.map((value: ProfileSlotValue) => (
            <div
              key={`${value.canonical_value}-${value.status}`}
              className="flex items-center justify-between gap-3 rounded-md bg-white/[0.02] px-3 py-2"
            >
              <div className="min-w-0 flex items-center gap-2">
                <Badge variant={statusVariant(value.status)}>{value.status}</Badge>
                <span className="text-sm truncate">{value.value}</span>
                <span className="text-xs text-muted-foreground">×{value.support_count}</span>
              </div>
              <div className="flex items-center gap-3 shrink-0">
                <ConfidenceBar value={value.confidence} />
                {value.status === "active" && (
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={busy}
                    onClick={() => act("obsolete_value", value.canonical_value)}
                    title="Obsolete this value"
                  >
                    <Archive className="size-3.5" />
                  </Button>
                )}
              </div>
            </div>
          ))}
          <div className="flex items-center gap-2 pt-1">
            <Button
              size="sm"
              variant="outline"
              disabled={busy}
              onClick={() => act(pinned ? "unpin" : "pin")}
            >
              {pinned ? <PinOff className="size-3.5 mr-1" /> : <Pin className="size-3.5 mr-1" />}
              {pinned ? "Unpin" : "Pin"}
            </Button>
            <span className="text-xs text-muted-foreground">
              updated {new Date(slot.updated_at).toLocaleString()}
            </span>
          </div>
        </CardContent>
      )}
    </Card>
  );
}

function ReviewsPanel({ userId }: { userId: string }) {
  const [reviews, setReviews] = useState<ProfileReviewRecord[] | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!userId) return;
    try {
      const res = await api.listProfileReviews(userId, { status: "pending" });
      setReviews(res.reviews);
    } catch (e) {
      console.error(e);
      setReviews([]);
    }
  }, [userId]);

  useEffect(() => {
    load();
  }, [load]);

  const resolve = useCallback(
    async (review_id: string, approve: boolean) => {
      setBusy(review_id);
      try {
        if (approve) await api.approveProfileReview(userId, review_id);
        else await api.rejectProfileReview(userId, review_id);
        await load();
      } catch (e) {
        console.error(e);
      } finally {
        setBusy(null);
      }
    },
    [userId, load]
  );

  if (reviews === null) {
    return <Skeleton className="h-24 w-full" />;
  }
  if (reviews.length === 0) {
    return <EmptyState icon={UserCircle} title="No pending reviews" description="Low-confidence or conflicting promotions will appear here for approval." />;
  }
  return (
    <div className="space-y-2">
      {reviews.map((review) => (
        <Card key={review.review_id}>
          <CardContent className="flex items-center justify-between gap-3 py-3">
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <Badge variant="outline">{review.attribute}</Badge>
                <span className="text-sm truncate">{review.proposed_value}</span>
                <span className="text-xs text-muted-foreground">
                  {Math.round(review.proposed_confidence * 100)}%
                </span>
              </div>
              <p className="text-xs text-muted-foreground truncate">{review.reason}</p>
            </div>
            <div className="flex items-center gap-2 shrink-0">
              <Button
                size="sm"
                variant="outline"
                disabled={busy === review.review_id}
                onClick={() => resolve(review.review_id, false)}
              >
                Reject
              </Button>
              <Button
                size="sm"
                disabled={busy === review.review_id}
                onClick={() => resolve(review.review_id, true)}
              >
                {busy === review.review_id ? <Loader2 className="size-3.5 animate-spin" /> : "Approve"}
              </Button>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

export default function ProfilePage() {
  const [userInput, setUserInput] = useState("");
  const [userId, setUserId] = useState("");
  const [slots, setSlots] = useState<ProfileSlot[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [includeInactive, setIncludeInactive] = useState(false);

  const load = useCallback(async () => {
    if (!userId) return;
    setLoading(true);
    try {
      const res = await api.getProfile(userId, { include_inactive: includeInactive });
      setSlots(res.slots);
    } catch (e) {
      console.error(e);
      setSlots([]);
    } finally {
      setLoading(false);
    }
  }, [userId, includeInactive]);

  useEffect(() => {
    load();
  }, [load]);

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center gap-2">
        <UserCircle className="size-5" />
        <h1 className="text-lg font-semibold">User Profile</h1>
      </div>

      <form
        className="flex items-center gap-2 max-w-md"
        onSubmit={(e) => {
          e.preventDefault();
          setUserId(userInput.trim());
        }}
      >
        <Input
          placeholder="Enter user id…"
          value={userInput}
          onChange={(e) => setUserInput(e.target.value)}
        />
        <Button type="submit" disabled={!userInput.trim()}>
          <Search className="size-4 mr-1" /> Load
        </Button>
      </form>

      {!userId ? (
        <EmptyState
          icon={UserCircle}
          title="Inspect a user profile"
          description="Enter a user id to view promoted profile slots, edit values, and review pending promotions."
        />
      ) : (
        <Tabs defaultValue="slots">
          <TabsList>
            <TabsTrigger value="slots">Slots</TabsTrigger>
            <TabsTrigger value="reviews">Reviews</TabsTrigger>
          </TabsList>
          <TabsContent value="slots" className="space-y-3 pt-3">
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              <input
                type="checkbox"
                checked={includeInactive}
                onChange={(e) => setIncludeInactive(e.target.checked)}
              />
              Show inactive values
            </label>
            {loading || slots === null ? (
              <Skeleton className="h-32 w-full" />
            ) : slots.length === 0 ? (
              <EmptyState
                icon={UserCircle}
                title="No profile slots"
                description="This user has no promoted profile facts yet."
              />
            ) : (
              <div className="space-y-2">
                {slots.map((slot) => (
                  <SlotRow key={slot.slot_key} slot={slot} userId={userId} onChanged={load} />
                ))}
              </div>
            )}
          </TabsContent>
          <TabsContent value="reviews" className="pt-3">
            <ReviewsPanel userId={userId} />
          </TabsContent>
        </Tabs>
      )}
    </div>
  );
}
