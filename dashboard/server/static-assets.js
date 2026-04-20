const fs = require("node:fs");
const path = require("node:path");

const STATIC_PREFIXES = ["/dashboard/_next/static/", "/_next/static/"];

function decodeAssetPath(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return null;
  }
}

function resolveStaticAssetPath(urlPath, options = {}) {
  const staticRoot = path.resolve(options.staticRoot || path.join(__dirname, "..", ".next", "static"));
  const prefix = STATIC_PREFIXES.find((candidate) => urlPath.startsWith(candidate));

  if (!prefix) {
    return null;
  }

  const relativePath = decodeAssetPath(urlPath.slice(prefix.length));
  if (!relativePath || relativePath.includes("\0")) {
    return null;
  }

  const resolvedPath = path.resolve(staticRoot, relativePath);
  const rootWithSeparator = staticRoot.endsWith(path.sep) ? staticRoot : `${staticRoot}${path.sep}`;

  if (resolvedPath !== staticRoot && !resolvedPath.startsWith(rootWithSeparator)) {
    return null;
  }

  return resolvedPath;
}

function contentTypeFor(filePath) {
  switch (path.extname(filePath).toLowerCase()) {
    case ".css":
      return "text/css; charset=utf-8";
    case ".js":
    case ".mjs":
      return "application/javascript; charset=utf-8";
    case ".json":
      return "application/json; charset=utf-8";
    case ".map":
      return "application/json; charset=utf-8";
    case ".svg":
      return "image/svg+xml";
    case ".woff":
      return "font/woff";
    case ".woff2":
      return "font/woff2";
    case ".ico":
      return "image/x-icon";
    case ".png":
      return "image/png";
    case ".jpg":
    case ".jpeg":
      return "image/jpeg";
    case ".webp":
      return "image/webp";
    default:
      return "application/octet-stream";
  }
}

function sendStaticAsset(req, res, options = {}) {
  const pathname = new URL(req.url || "/", "http://localhost").pathname;
  const assetPath = resolveStaticAssetPath(pathname, options);

  if (!assetPath) {
    return false;
  }

  let stat;
  try {
    stat = fs.statSync(assetPath);
  } catch {
    res.statusCode = 404;
    res.end("Not Found");
    return true;
  }

  if (!stat.isFile()) {
    res.statusCode = 404;
    res.end("Not Found");
    return true;
  }

  res.statusCode = 200;
  res.setHeader("Content-Type", contentTypeFor(assetPath));
  res.setHeader("Content-Length", stat.size);
  res.setHeader("Cache-Control", "public, max-age=31536000, immutable");

  if (req.method === "HEAD") {
    res.end();
    return true;
  }

  fs.createReadStream(assetPath).pipe(res);
  return true;
}

module.exports = {
  contentTypeFor,
  resolveStaticAssetPath,
  sendStaticAsset,
};
