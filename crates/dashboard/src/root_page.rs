pub const ROOT_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>swactor dashboard</title>
  <style>
    :root { color-scheme: dark; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #111827; color: #e5e7eb; }
    body { margin: 0; padding: 32px; }
    a { color: #93c5fd; }
    code { color: #fbbf24; }
    .card { max-width: 760px; background: #1f2937; border: 1px solid #374151; border-radius: 16px; padding: 24px; }
    li { margin: 10px 0; }
  </style>
</head>
<body>
  <main class="card">
    <h1>swactor dashboard</h1>
    <p>Read-only views over live telemetry frames.</p>
    <ul>
      <li><a href="/view/swactor/workers">Swactor workers</a></li>
      <li><a href="/api/views">Registered views JSON</a></li>
      <li><code>/events</code> streams raw incoming frames as SSE.</li>
      <li><code>/api/frames</code> returns the bounded recent raw frame window.</li>
    </ul>
  </main>
</body>
</html>"#;
