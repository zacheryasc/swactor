#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCS_DIR="$SCRIPT_DIR"
TOOLS_DIR="$ROOT_DIR/tools"

# --- Phase 1: Generate architecture.dot from source AST ---

echo "==> Generating architecture.dot from source..."
cargo run --manifest-path "$TOOLS_DIR/depgraph/Cargo.toml" \
  -- --src-dir "$ROOT_DIR/src/" --output architecture --output-dir "$DOCS_DIR/"

# --- Phase 2: Render all .dot files to .svg ---

dots=("$DOCS_DIR"/*.dot)
if [ ${#dots[@]} -eq 0 ]; then
  echo "No .dot files found in $DOCS_DIR"
  exit 0
fi

render_with_dot() {
  for src in "${dots[@]}"; do
    name="$(basename "$src" .dot)"
    echo "  dot: $name.dot -> $name.svg"
    dot -Tsvg "$src" -o "$DOCS_DIR/$name.svg"
  done
}

render_with_vizjs() {
  local tmpfile
  tmpfile="$(mktemp "${TMPDIR:-/tmp}/render_docs.XXXXXX.mjs")"
  trap 'rm -f "$tmpfile"' RETURN

  cat > "$tmpfile" <<NODEJS
import { createRequire } from "module";
import { readFileSync, writeFileSync, readdirSync } from "fs";
import { join, basename } from "path";

const require = createRequire("$TOOLS_DIR/package.json");
const { instance } = require("@viz-js/viz");

const docsDir = "$DOCS_DIR";

const viz = await instance();
const dots = readdirSync(docsDir).filter(f => f.endsWith(".dot"));

for (const file of dots) {
  const src  = readFileSync(join(docsDir, file), "utf-8");
  const name = basename(file, ".dot");
  const svg  = viz.renderString(src, { format: "svg" });
  writeFileSync(join(docsDir, \`\${name}.svg\`), svg);
  console.log(\`  viz-js: \${file} -> \${name}.svg\`);
}
NODEJS

  node "$tmpfile"
}

echo "==> Rendering DOT -> SVG..."

if command -v dot &>/dev/null; then
  render_with_dot
elif command -v node &>/dev/null; then
  if [ -f "$TOOLS_DIR/package.json" ]; then
    if ! [ -d "$TOOLS_DIR/node_modules/@viz-js/viz" ]; then
      echo "Installing @viz-js/viz..."
      (cd "$TOOLS_DIR" && npm install --silent)
    fi
  else
    echo "Error: tools/package.json not found at $TOOLS_DIR" >&2
    exit 1
  fi
  render_with_vizjs
else
  echo "Error: No renderer available." >&2
  echo "Install graphviz (apt install graphviz) or Node.js." >&2
  exit 1
fi

echo "==> Done. SVGs in $DOCS_DIR/"
