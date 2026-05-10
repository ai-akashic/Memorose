# Memorose Dashboard UI Design Spec

Date: 2026-05-10

## Scope
This spec defines the UI design system for `Memorose/dashboard` only. It does not change product branding for the public website or the Rust backend.

## Context
The dashboard is an operator-facing control plane for Memorose. Current implementation already establishes the core visual language:

- Next.js App Router
- Geist sans and mono
- dark-first theme
- amber/orange primary accents
- `shadcn/ui`-style primitives
- Lucide icons
- Framer Motion for restrained transitions
- Recharts for operational charts

The design spec should formalize what already works and prevent the UI from drifting into a generic admin template.

## Goals
1. Keep the dashboard dense, legible, and operational.
2. Preserve the current warm dark theme.
3. Make state, hierarchy, and actionability obvious.
4. Keep the component set consistent across metrics, settings, memory browsing, organization views, and playground tools.
5. Support fast scanning by operators.

## Non-Goals
1. Do not redesign the public marketing site.
2. Do not rebrand Memorose.
3. Do not introduce a second visual language.
4. Do not optimize for decorative effect over utility.

## Design Principles
- Control plane first.
- Density without clutter.
- Warm dark surfaces with restrained contrast.
- Clear hierarchy for primary, secondary, and destructive actions.
- Motion only when it improves comprehension.

## Visual Tokens
- Font family: Geist sans for UI, Geist mono for IDs, config, and machine-readable data.
- Radius: compact rounded corners, roughly `0.75rem` to `1rem`.
- Background: near-black with a warm tint.
- Foreground: warm off-white.
- Primary: amber-orange.
- Accent: lighter amber-gold.
- Success: muted green.
- Warning: warm yellow.
- Destructive: muted red.

Rules:
- Avoid blue/purple dominant palettes.
- Avoid pure black and pure white.
- Avoid decorative gradients unless they carry meaning.

## Layout Model
The dashboard should use a predictable structure:

1. Compact page chrome.
2. Small hero/header for page identity and primary actions.
3. Dense content area with cards, tables, charts, or forms.
4. Contextual empty or degraded states.

Layout rules:
- Use full-width bands for data-heavy views.
- Use cards only for discrete units.
- Do not nest cards inside cards.
- Prefer scanning horizontally across rows or panels over decorative composition.

## Component Rules

### Card
- Cards are for contained units of information.
- Keep borders subtle and backgrounds translucent.
- Keep headers compact.
- Use cards for metrics, config blocks, summaries, and focused panels.

### Button
- Use icon + text for most actions.
- Use icon-only buttons only when the icon is obvious.
- Primary actions should be visually clear but not loud.
- Destructive actions should be explicit and isolated.

### Table
- Tables are the default for lists of records, keys, memories, and operations.
- Keep headers readable and rows scannable.
- Prefer compact density with hover feedback.
- Use stable alignment for IDs, dates, counts, and numeric values.

### Tabs
- Use tabs to switch between stable views of the same domain.
- Keep tab labels short.
- Do not overload tabs with nested controls.

### Dialog and Sheet
- Dialogs are for short, focused tasks.
- Sheets are for contextual or multi-step tasks.
- Do not stack multiple modal layers unless unavoidable.

### Charts
- Charts should support decision-making, not ornament.
- Tooltips must be dark, compact, and readable.
- Keep axes and labels restrained.
- Prefer small multiples or focused plots over crowded composite charts.

### Command Palette
- Treat the command palette as a power-user surface.
- Keep it fast, keyboard-first, and sparse.
- Do not fill it with low-value actions.

## Motion
- Motion should communicate entry, loading, and state change.
- Use short ease-out transitions.
- Prefer opacity and small translate changes.
- Avoid bounce, flourish, and decorative animation.
- Respect reduced-motion settings.

## State Design
- Loading states should preserve layout and avoid jumps.
- Empty states should point to the next useful action.
- Error states should be direct and operational.
- Degraded states should still allow the dashboard to function where possible.

## Accessibility
- Preserve strong contrast for text and controls.
- Never rely on color alone to communicate meaning.
- Keep focus styles visible.
- Keep touch and keyboard targets usable.
- Prevent text overflow in dense panels.

## Content Style
- Copy should be short and functional.
- Labels should be specific.
- Use technical vocabulary when it helps operators.
- Avoid marketing language and filler.

## Implementation Constraints
- New work should use the existing component stack before introducing new abstractions.
- New components must align with the current dashboard chrome, cards, buttons, and spacing.
- Any new screen should fit the dashboard's operational tone within one viewport.

## Review Notes
This spec intentionally matches the dashboard as currently implemented:

- warm amber accent system
- subtle glass-card surfaces
- compact top-level headings
- dense operational panels
- restrained Framer Motion
- Recharts with dark tooltips

That makes the spec suitable as a living design contract for future UI edits.
