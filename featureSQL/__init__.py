"""Top-level package for featureSQL.

This file exposes the public API and maintains the version.
"""

__version__ = "0.1.0"

from .cli import Run  # expose for convenience
from .dump_bin import DumpDataAll, DumpDataUpdate
from .yahoo import (
    get_calendar_list,
    get_default_symbols,
    get_us_stock_symbols,
    get_hs_stock_symbols,
    YahooCollectorUS,
    YahooCollectorFX,
    YahooCollectorIR,
    YahooCollectorVol,
    YahooCorrelationCollector,
    YahooOptionChainCollector,
    YahooNormalize,
)
from .ir import boost_ir_curve
from .volsurface import calibrate_vol_surface
from .utils import deco_retry

__all__ = [
    "Run",
    "DumpDataAll",
    "DumpDataUpdate",
    "get_calendar_list",
    "get_default_symbols",
    "get_us_stock_symbols",
    "get_hs_stock_symbols",
    "YahooCollectorUS",
    "YahooCollectorFX",
    "YahooCollectorIR",
    "YahooCollectorVol",
    "YahooCorrelationCollector",
    "YahooOptionChainCollector",
    "YahooNormalize",
    "boost_ir_curve",
    "calibrate_vol_surface",
    "deco_retry",
    "__version__",
]
