"""Back-compat shim.

``fred_pipeline.regime_stats`` moved to :mod:`fred_pipeline.writer.regime_stats` as part of
reorganizing the package into subpackages by responsibility. This aliases the
old import path directly onto the new module object (not a re-export copy),
so every attribute access, ``from fred_pipeline.regime_stats import X``, and any
test monkeypatching by the old string path all operate on the same
underlying module. New code should still import from
:mod:`fred_pipeline.writer.regime_stats` directly.
"""

import sys as _sys

from fred_pipeline.writer import regime_stats as _real_module

_sys.modules[__name__] = _real_module
