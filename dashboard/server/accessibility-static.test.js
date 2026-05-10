const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const root = path.resolve(__dirname, "..");

function source(relativePath) {
  return fs.readFileSync(path.join(root, relativePath), "utf8");
}

test("command palette exposes modal dialog semantics", () => {
  const code = source("src/components/CommandPalette.tsx");

  assert.match(code, /role="dialog"/);
  assert.match(code, /aria-modal="true"/);
  assert.match(code, /aria-label=/);
});

test("icon-only dashboard controls have accessible names", () => {
  const layout = source("src/app/(authenticated)/layout.tsx");
  const localeSwitcher = source("src/components/LocaleSwitcher.tsx");
  const memories = source("src/app/(authenticated)/memories/page.tsx");

  assert.match(layout, /aria-label=\{collapsed \? tLayout\("expandSidebar"\) : tLayout\("collapseSidebar"\)\}/);
  assert.match(localeSwitcher, /aria-label=\{t\("ariaLabel"\)\}/);
  assert.match(memories, /aria-label=\{t\("search.submit"\)\}/);
  assert.match(memories, /aria-label=\{t\("pagination.previous"\)\}/);
  assert.match(memories, /aria-label=\{t\("pagination.next"\)\}/);
});

test("interactive memory rows are keyboard reachable", () => {
  const memories = source("src/app/(authenticated)/memories/page.tsx");

  assert.match(memories, /role=\{canOpenDetail \? "button" : undefined\}/);
  assert.match(memories, /tabIndex=\{canOpenDetail \? 0 : undefined\}/);
  assert.match(memories, /event\.key === "Enter" \|\| event\.key === " "/);
});

test("form fields provide browser autocomplete semantics", () => {
  const loginForm = source("src/components/login-form.tsx");
  const settings = source("src/app/(authenticated)/settings/page.tsx");

  assert.match(loginForm, /name="username"/);
  assert.match(loginForm, /autoComplete="username"/);
  assert.match(loginForm, /name="password"/);
  assert.match(loginForm, /autoComplete="current-password"/);
  assert.match(settings, /autoComplete=\{autoComplete\}/);
});

test("transient status messages are announced", () => {
  const loginForm = source("src/components/login-form.tsx");
  const settings = source("src/app/(authenticated)/settings/page.tsx");

  assert.match(loginForm, /role="alert"/);
  assert.match(settings, /role="status"/);
  assert.match(settings, /aria-live="polite"/);
});

test("stat card number animation respects reduced motion and cleans up frames", () => {
  const statCard = source("src/components/stat-card.tsx");

  assert.match(statCard, /useReducedMotion/);
  assert.match(statCard, /window\.cancelAnimationFrame/);
});
