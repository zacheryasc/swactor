#!/usr/bin/env python3
"""Analyze per-function complexity metrics for Rust source files.

Reports: function name, line count, max nesting depth, and file location.
Sorted by line count (descending) to surface the largest functions first.

Usage:
    python3 tools/fn_complexity.py src/worker.rs
    python3 tools/fn_complexity.py src/           # recurse into directory
    python3 tools/fn_complexity.py src/ --json     # JSON output
    python3 tools/fn_complexity.py src/ --min-lines 20  # filter small fns
"""

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class FnMetric:
    file: str
    name: str
    start_line: int
    end_line: int
    lines: int
    max_depth: int
    has_unsafe: bool

    @property
    def location(self) -> str:
        return f"{self.file}:{self.start_line}"


# Matches fn declarations (free functions, methods, trait impls)
FN_PATTERN = re.compile(
    r'^\s*(?:pub(?:\(crate\))?\s+)?(?:async\s+)?fn\s+(\w+)'
)

# Matches impl blocks to qualify method names
IMPL_PATTERN = re.compile(
    r'^\s*impl(?:<[^>]*>)?\s+(?:(\w+(?:<[^>]*>)?)\s+for\s+)?(\w+)'
)


def analyze_file(path: str) -> list[FnMetric]:
    """Parse a single Rust file and extract function metrics."""
    with open(path) as f:
        lines = f.readlines()

    metrics = []
    current_impl = None
    brace_depth = 0
    fn_stack: list[tuple[str, int, int, bool]] = []  # (name, start_line, start_depth, has_unsafe)

    for i, line in enumerate(lines, 1):
        stripped = line.rstrip()

        # Track impl blocks for method qualification
        impl_match = IMPL_PATTERN.match(stripped)
        if impl_match and '{' in stripped:
            trait_name = impl_match.group(1)
            type_name = impl_match.group(2)
            if trait_name:
                current_impl = f"{trait_name} for {type_name}"
            else:
                current_impl = type_name

        # Detect function start
        fn_match = FN_PATTERN.match(stripped)
        if fn_match and '{' in stripped:
            fn_name = fn_match.group(1)
            if current_impl:
                fn_name = f"{current_impl}::{fn_name}"
            has_unsafe = 'unsafe' in stripped
            fn_stack.append((fn_name, i, brace_depth, has_unsafe))

        # Track brace depth
        # Simple brace counting (ignores braces in strings/comments, good enough)
        opens = stripped.count('{')
        closes = stripped.count('}')
        brace_depth += opens - closes

        # Check for unsafe blocks within functions
        if fn_stack and 'unsafe' in stripped and fn_match is None:
            name, start, depth, _ = fn_stack[-1]
            fn_stack[-1] = (name, start, depth, True)

        # When a function's brace depth returns to entry level, it's done
        while fn_stack and brace_depth <= fn_stack[-1][2]:
            fn_name, start_line, _, has_unsafe = fn_stack.pop()
            end_line = i
            fn_lines = end_line - start_line + 1

            # Calculate max nesting depth within this function
            max_depth = 0
            local_depth = 0
            for j in range(start_line - 1, end_line):
                if j < len(lines):
                    local_depth += lines[j].count('{') - lines[j].count('}')
                    max_depth = max(max_depth, local_depth)

            # Reset impl context if we've left the impl block
            if brace_depth == 0:
                current_impl = None

            metrics.append(FnMetric(
                file=path,
                name=fn_name,
                start_line=start_line,
                end_line=end_line,
                lines=fn_lines,
                max_depth=max_depth,
                has_unsafe=has_unsafe,
            ))

    return metrics


def collect_files(path: str) -> list[str]:
    """Collect .rs files from a path (file or directory)."""
    p = Path(path)
    if p.is_file():
        return [str(p)]
    elif p.is_dir():
        return sorted(str(f) for f in p.rglob('*.rs'))
    else:
        print(f"Error: {path} is not a file or directory", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Rust function complexity analyzer')
    parser.add_argument('paths', nargs='+', help='Rust source files or directories')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--min-lines', type=int, default=0,
                       help='Only show functions with at least N lines')
    parser.add_argument('--top', type=int, default=0,
                       help='Show only the top N largest functions')
    args = parser.parse_args()

    all_metrics: list[FnMetric] = []
    for path in args.paths:
        for file in collect_files(path):
            all_metrics.extend(analyze_file(file))

    # Filter and sort
    if args.min_lines:
        all_metrics = [m for m in all_metrics if m.lines >= args.min_lines]
    all_metrics.sort(key=lambda m: m.lines, reverse=True)
    if args.top:
        all_metrics = all_metrics[:args.top]

    if args.json:
        output = [asdict(m) for m in all_metrics]
        print(json.dumps(output, indent=2))
    else:
        # Summary stats
        if all_metrics:
            total_fns = len(all_metrics)
            avg_lines = sum(m.lines for m in all_metrics) / total_fns
            max_fn = all_metrics[0]

            print(f"Functions: {total_fns}, avg lines: {avg_lines:.1f}, "
                  f"largest: {max_fn.name} ({max_fn.lines} lines)")
            print()

        # Table output
        print(f"{'Lines':>5}  {'Depth':>5}  {'Location':<45}  {'Function'}")
        print(f"{'─'*5}  {'─'*5}  {'─'*45}  {'─'*40}")
        for m in all_metrics:
            loc = f"{m.file}:{m.start_line}"
            unsafe_marker = " [unsafe]" if m.has_unsafe else ""
            print(f"{m.lines:>5}  {m.max_depth:>5}  {loc:<45}  {m.name}{unsafe_marker}")


if __name__ == '__main__':
    main()
