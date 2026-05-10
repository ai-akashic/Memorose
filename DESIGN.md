---
name: Memorose Dashboard
version: 0.1.0
scope: dashboard
status: draft
updated: 2026-05-10
owners:
  - dashboard
---

# Memorose Dashboard Design

Scope: `dashboard/` only.

## Purpose
This dashboard is an operational control plane for Memorose. It is for admins and operators who need to inspect cluster state, memories, tasks, organizations, metrics, settings, and recovery flows quickly.

The interface should feel dense, quiet, and reliable. It should not feel like a marketing site or a consumer app.

## Visual Direction
- Dark-first, warm-tinted utility UI.
- Use the current amber/orange accent family as the primary signal.
- Keep surfaces layered but restrained: subtle borders, low-contrast fills, soft blur only where it helps separation.
- Prefer clarity over decoration. Any visual flourish must support scanning or state recognition.

## Core Design Principles
- Information density is a feature.
- Every screen should answer "what is happening" and "what can I do next".
- Primary actions should be obvious, secondary actions should recede.
- Avoid unnecessary motion, ornament, and large empty hero areas.
- Components should be reusable across metrics, settings, memory browsing, and operational workflows.

## Typography
- Use Geist for both sans and mono.
- Base text should stay compact and legible.
- Headings should be restrained, not oversized.
- Use mono only for identifiers, config values, timestamps, IDs, and structured data.

## Color Tokens
Keep the existing palette direction unless a future redesign explicitly changes it.

- Background: near-black with a warm brown tint.
- Foreground: warm off-white, not pure white.
- Card / popover / sidebar: slightly lighter than the background, with subtle alpha.
- Primary: amber-orange.
- Accent: lighter amber-gold.
- Success: muted green.
- Warning: warm yellow.
- Destructive: muted red.

Rules:
- Do not introduce neon blue or purple as dominant colors.
- Do not use bright gradients as decoration.
- Use color to encode state, not to fill space.

## Layout
- Prefer a compact control-plane layout with a clear left-to-right scan path.
- Keep page chrome lightweight.
- Use full-width sections for data-heavy views.
- Use cards for discrete units only; do not nest cards inside cards.
- Keep spacing consistent and tight enough for dense operational data.
- Preserve readable line lengths for descriptive text.

## Components

### Cards
- Use rounded corners around `0.75rem` to `1rem`.
- Keep borders subtle and backgrounds translucent.
- Card headers should be compact and functional.
- Avoid large empty cards.

### Buttons
- Use icon + text for actions when the intent may be ambiguous.
- Use icon-only buttons only for common, recognizable controls.
- Primary buttons should be visually clear but not loud.
- Destructive actions must be visually explicit.

### Tables
- Use tables for structured operational lists.
- Headers should stay sticky only when useful.
- Rows must be scannable and support hover state.
- Numeric and identifier fields should align predictably.

### Charts
- Charts are for operational insight, not decoration.
- Use restrained tooltips with dark backgrounds and clear labels.
- Keep axes and labels compact.
- Prefer charts that can be read at a glance.

### Forms
- Form fields should be short, stable, and easy to parse.
- Group related fields visually.
- Show validation inline and immediately.
- Keep destructive or irreversible actions separated from routine settings.

### Dialogs and Sheets
- Use dialogs for small, focused tasks.
- Use sheets for multi-step or contextual editing.
- Avoid deep modal stacks.
- Keep escape and dismiss behavior predictable.

### Command Palette
- Treat the command palette as a primary power tool.
- It should remain fast, focused, and keyboard-friendly.
- Do not overload it with low-value commands.

## Motion
- Use motion sparingly.
- Motion should clarify entry, exit, loading, and state change.
- Favor short ease-out transitions and simple vertical or opacity movement.
- Do not animate layout-heavy properties unless there is no better option.
- Respect reduced-motion preferences.

## State Handling
- Loading, empty, degraded, and error states must be designed intentionally.
- Empty states should explain the next useful action.
- Error states should be direct and operational, not emotional.
- Degraded states should preserve as much utility as possible.

## Accessibility
- Maintain strong contrast for text and controls.
- Never rely on color alone to communicate state.
- Keep focus states visible.
- Ensure touch targets and keyboard targets are usable.
- Text must not overflow its container at common dashboard widths.

## Content Style
- Copy should be direct and functional.
- Prefer short labels over explanatory paragraphs.
- Use technical terms only where operators expect them.
- Avoid promotional language.

## Implementation Notes
- Existing dashboard primitives are the source of truth: `shadcn/ui`, Lucide icons, Framer Motion, Recharts, and the current glass-card treatment.
- New UI work should fit this system instead of introducing a second style language.
- When a new component is added, it should be usable in at least one of these contexts: metrics, settings, memory browsing, organization management, or operational diagnostics.
