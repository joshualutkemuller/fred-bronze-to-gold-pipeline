"""Back-compat shim.

``fred_pipeline.spark_io`` moved to :mod:`fred_pipeline.io.spark_io` as part of
reorganizing the package into subpackages by responsibility. This aliases the
old import path directly onto the new module object (not a re-export copy),
so every attribute access, ``from fred_pipeline.spark_io import X``, and any
test monkeypatching by the old string path all operate on the same
underlying module. New code should still import from
:mod:`fred_pipeline.io.spark_io` directly.
"""

import sys as _sys

from fred_pipeline.io import spark_io as _real_module

_sys.modules[__name__] = _real_module
