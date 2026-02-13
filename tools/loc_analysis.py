#!/usr/bin/env python3
"""Analyze line-of-code breakdown for Rust source files.

Reports: logic, comments, blank, and string-literal lines per file.
Helps track code reduction progress and identify embedded content.

Usage:
    python3 tools/loc_analysis.py src/
    python3 tools/loc_analysis.py src/ crates/std/src/ --json
    python3 tools/loc_analysis.py src/ --sort-by logic
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class FileMetrics:
    file: str
    total: int
    logic: int
    comment: int
    blank: int
    string_literal: int

    @property
    def logic_pct(self) -> float:
        return (self.logic / self.total * 100) if self.total else 0.0


def analyze_file(path: str) -> FileMetrics:
    """Count line types in a Rust source file."""
    with open(path) as f:
        lines = f.readlines()

    total = len(lines)
    blank = 0
    comment = 0
    string_lit = 0
    logic = 0

    in_block_comment = False
    in_raw_string = False

    for line in lines:
        stripped = line.strip()

        if not stripped:
            blank += 1
            continue

        # Track block comments
        if in_block_comment:
            comment += 1
            if '*/' in stripped:
                in_block_comment = False
            continue

        if stripped.startswith('/*'):
            comment += 1
            if '*/' not in stripped:
                in_block_comment = True
            continue

        # Line comments
        if stripped.startswith('//'):
            comment += 1
            continue

        # Raw string literals (r#"..."#, r##"..."##, etc.) and regular strings
        # Heuristic: line is predominantly a string if it's inside a raw string
        # or contains a long string literal (>60 chars of quoted content)
        if in_raw_string:
            string_lit += 1
            if '"#' in stripped or '"##' in stripped:
                in_raw_string = False
            continue

        if 'r#"' in stripped or 'r##"' in stripped:
            if '"#' not in stripped.split('r#"', 1)[-1] if 'r#"' in stripped else True:
                in_raw_string = True
                string_lit += 1
                continue

        # Heuristic: if the line has a long string literal, count it
        string_content = re.findall(r'"([^"]*)"', stripped)
        total_string_chars = sum(len(s) for s in string_content)
        if total_string_chars > 60:
            string_lit += 1
        else:
            logic += 1

    return FileMetrics(
        file=path,
        total=total,
        logic=logic,
        comment=comment,
        blank=blank,
        string_literal=string_lit,
    )


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
    parser = argparse.ArgumentParser(description='Rust LOC breakdown analyzer')
    parser.add_argument('paths', nargs='+', help='Rust source files or directories')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--sort-by', choices=['total', 'logic', 'comment', 'string_literal'],
                       default='total', help='Sort column')
    args = parser.parse_args()

    all_metrics: list[FileMetrics] = []
    for path in args.paths:
        for file in collect_files(path):
            all_metrics.extend([analyze_file(file)])

    all_metrics.sort(key=lambda m: getattr(m, args.sort_by), reverse=True)

    if args.json:
        print(json.dumps([asdict(m) for m in all_metrics], indent=2))
    else:
        # Summary
        totals = FileMetrics(
            file="TOTAL",
            total=sum(m.total for m in all_metrics),
            logic=sum(m.logic for m in all_metrics),
            comment=sum(m.comment for m in all_metrics),
            blank=sum(m.blank for m in all_metrics),
            string_literal=sum(m.string_literal for m in all_metrics),
        )

        print(f"Files: {len(all_metrics)}, Total: {totals.total}, "
              f"Logic: {totals.logic} ({totals.logic_pct:.1f}%), "
              f"Comment: {totals.comment}, Blank: {totals.blank}, "
              f"Strings: {totals.string_literal}")
        print()

        print(f"{'Total':>6}  {'Logic':>6}  {'Cmt':>5}  {'Blank':>5}  {'Str':>5}  {'%Logic':>6}  {'File'}")
        print(f"{'─'*6}  {'─'*6}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*6}  {'─'*45}")
        for m in all_metrics:
            print(f"{m.total:>6}  {m.logic:>6}  {m.comment:>5}  {m.blank:>5}  "
                  f"{m.string_literal:>5}  {m.logic_pct:>5.1f}%  {m.file}")
        print(f"{'─'*6}  {'─'*6}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*6}  {'─'*45}")
        print(f"{totals.total:>6}  {totals.logic:>6}  {totals.comment:>5}  {totals.blank:>5}  "
              f"{totals.string_literal:>5}  {totals.logic_pct:>5.1f}%  TOTAL")


if __name__ == '__main__':
    main()
