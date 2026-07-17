"""Back-compat shim.

``fred_pipeline.reconcile`` moved to :mod:`fred_pipeline.governance.reconcile` as part of
reorganizing the package into subpackages by responsibility. This aliases the
old import path directly onto the new module object (not a re-export copy),
so every attribute access, ``from fred_pipeline.reconcile import X``, and any
test monkeypatching by the old string path all operate on the same
underlying module. New code should still import from
:mod:`fred_pipeline.governance.reconcile` directly.
"""

import sys as _sys

from fred_pipeline.governance import reconcile as _real_module

_sys.modules[__name__] = _real_module
