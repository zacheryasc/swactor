"""pytest configuration for the pipeline-parallel-inference example.

Adds the example directory to ``sys.path`` so test modules can
``import pp_tinygrad_worker`` to exercise its pure helpers directly.
Tests that spawn the worker as a subprocess invoke it by path.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
