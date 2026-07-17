"""Back-compat shim.

``fred_pipeline.gold_polars`` moved to :mod:`fred_pipeline.writer.gold_polars` as part of
reorganizing the package into subpackages by responsibility. This aliases the
old import path directly onto the new module object (not a re-export copy),
so every attribute access, ``from fred_pipeline.gold_polars import X``, and any
test monkeypatching by the old string path all operate on the same
underlying module. New code should still import from
:mod:`fred_pipeline.writer.gold_polars` directly.
"""

import sys as _sys

from fred_pipeline.writer import gold_polars as _real_module

_sys.modules[__name__] = _real_module
