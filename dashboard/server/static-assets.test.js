const assert = require("node:assert/strict");
const path = require("node:path");
const test = require("node:test");

const { resolveStaticAssetPath } = require("./static-assets");

test("maps basePath static asset URLs to the standalone .next/static directory", () => {
  const staticRoot = path.resolve("/app/dashboard/.next/static");

  assert.equal(
    resolveStaticAssetPath("/dashboard/_next/static/chunks/app.js", { staticRoot }),
    path.join(staticRoot, "chunks/app.js"),
  );
});

test("maps root static asset URLs to the standalone .next/static directory", () => {
  const staticRoot = path.resolve("/app/dashboard/.next/static");

  assert.equal(
    resolveStaticAssetPath("/_next/static/media/font.woff2", { staticRoot }),
    path.join(staticRoot, "media/font.woff2"),
  );
});

test("rejects paths outside the static asset namespace", () => {
  const staticRoot = path.resolve("/app/dashboard/.next/static");

  assert.equal(resolveStaticAssetPath("/dashboard/login/", { staticRoot }), null);
});

test("rejects static asset path traversal", () => {
  const staticRoot = path.resolve("/app/dashboard/.next/static");

  assert.equal(
    resolveStaticAssetPath("/dashboard/_next/static/%2e%2e/server.js", { staticRoot }),
    null,
  );
});
