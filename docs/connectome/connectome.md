# Connectome Analysis

The connectome analysis applies spectral graph theory to the codebase's internal dependency DAG, producing quantitative coupling metrics and visual dashboards.

## What it measures

The tool parses `deps.dot` (a GraphViz DOT file describing struct/trait dependencies between modules) and computes:

- **Laplacian eigenvalue spectrum** -- encodes the graph's overall connectivity structure
- **Fiedler vector** -- the optimal spectral bisection of the dependency graph, revealing natural module clusters
- **Module coupling matrix** -- directed edge counts between every pair of modules
- **Connectome Complexity Index (CCI)** -- a single 0-1 score combining five sub-metrics:

| Sub-metric | Weight | What it captures |
|---|---|---|
| Algebraic connectivity (lambda_2/n) | 25% | How tightly connected the graph is |
| Spectral entropy (H/log2(k)) | 25% | How uniformly distributed coupling is across eigenvalues |
| Edge density (\|E\|/n(n-1)) | 15% | Raw ratio of edges to possible edges |
| Cross-module coupling ratio | 20% | Fraction of edges that cross module boundaries |
| Spectral radius (rho/(n-1)) | 15% | Maximum hub concentration |

### Interpreting CCI

| CCI range | Label | Meaning |
|---|---|---|
| < 0.30 | LOW | Well-decomposed architecture |
| 0.30 - 0.60 | MODERATE | Typical well-structured codebase |
| > 0.60 | HIGH | Consider reviewing module boundaries |

## Running

From the project root:

```sh
# Default: outputs to docs/connectome/
python tools/spectral/spectral_analysis.py deps.dot

# Custom output directory
python tools/spectral/spectral_analysis.py deps.dot -o path/to/output

# Also emit JSON metrics
python tools/spectral/spectral_analysis.py deps.dot --json

# Text report only (skip matplotlib PNG)
python tools/spectral/spectral_analysis.py deps.dot --no-plots
```

### Prerequisites

The script requires numpy, scipy, and matplotlib (for the PNG dashboard). These are available in the project's `.venv`:

```sh
source .venv/bin/activate
python tools/spectral/spectral_analysis.py deps.dot
```

## Output files

All output goes to `docs/connectome/` by default:

| File | Description |
|---|---|
| `connectome_report.txt` | Full text report with eigenvalues, Fiedler bisection, coupling matrix, and CCI breakdown |
| `connectome_dashboard.html` | Interactive HTML dashboard with zoomable DAG, eigenvalue plot, Fiedler bar chart, and coupling heatmap |
| `connectome_dashboard.png` | Static PNG snapshot of the spectral dashboard (dark theme, 16x12 @ 150 DPI) |
| `connectome_metrics.json` | Machine-readable metrics (only with `--json` flag) |

## Regenerating deps.dot

The DOT file is the input to the spectral analysis. To regenerate it from source:

```sh
cargo run --manifest-path tools/depgraph/Cargo.toml -- --src-dir src/ --output deps
```

Then re-run the spectral analysis to update the connectome report.
