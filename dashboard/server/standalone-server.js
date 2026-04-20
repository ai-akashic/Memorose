const http = require("node:http");
const path = require("node:path");

const { sendStaticAsset } = require("./static-assets");

const dashboardDir = path.resolve(__dirname, "..");
const nextServerPath =
  process.env.DASHBOARD_NEXT_SERVER ||
  [path.join(dashboardDir, ".next", "standalone", "server.js"), path.join(dashboardDir, "server.js")].find((candidate) =>
    require("node:fs").existsSync(candidate),
  );
const staticRoot = path.join(dashboardDir, ".next", "static");
const requestedPort = Number.parseInt(process.env.PORT || "3100", 10);
const proxyPort = Number.parseInt(process.env.DASHBOARD_NEXT_PORT || String(requestedPort + 1), 10);
const hostname = process.env.HOSTNAME || "0.0.0.0";

if (!nextServerPath) {
  throw new Error("Next standalone server entry not found");
}

process.env.PORT = String(proxyPort);

require(nextServerPath);

const proxy = http.createServer((req, res) => {
  if (sendStaticAsset(req, res, { staticRoot })) {
    return;
  }

  const upstreamReq = http.request(
    {
      hostname: "127.0.0.1",
      port: proxyPort,
      method: req.method,
      path: req.url,
      headers: req.headers,
    },
    (upstreamRes) => {
      res.writeHead(upstreamRes.statusCode || 500, upstreamRes.headers);
      upstreamRes.pipe(res);
    },
  );

  upstreamReq.on("error", (error) => {
    res.statusCode = 502;
    res.end(`Dashboard upstream unavailable: ${error.message}`);
  });

  req.pipe(upstreamReq);
});

proxy.listen(requestedPort, hostname, () => {
  console.log(`Dashboard proxy listening on http://${hostname}:${requestedPort}`);
});
