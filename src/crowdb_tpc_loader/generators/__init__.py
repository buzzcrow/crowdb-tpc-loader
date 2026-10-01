"""Native, versioned generator adapters. No synthetic fallback data."""

from __future__ import annotations

from typing import Callable

from ..models import Options
from ..security import Redactor


def make_generator(options: Options, emit: Callable[[str], None], redactor: Redactor):
    if options.benchmark == "tpch":
        from .tpch import TPCHGenerator

        return TPCHGenerator(options, emit, redactor)
    from .tpcds import TPCDSGenerator

    return TPCDSGenerator(options, emit, redactor)
