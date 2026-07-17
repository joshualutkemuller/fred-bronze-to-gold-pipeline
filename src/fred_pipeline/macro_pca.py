"""Back-compat shim.

``fred_pipeline.macro_pca`` moved to :mod:`fred_pipeline.ml.macro_pca` as part of
reorganizing the package into subpackages by responsibility. This aliases the
old import path directly onto the new module object (not a re-export copy),
so every attribute access, ``from fred_pipeline.macro_pca import X``, and any
test monkeypatching by the old string path all operate on the same
underlying module. New code should still import from
:mod:`fred_pipeline.ml.macro_pca` directly.
"""

import sys as _sys

from fred_pipeline.ml import macro_pca as _real_module

_sys.modules[__name__] = _real_module
