"""Glue job argument resolution, with support for optional parameters.

awsglue.utils.getResolvedOptions() errors if you ask it for a parameter that
wasn't passed, which makes it awkward to express "these parameters are
optional, fall back to a computed default." resolve_args() works around that
by only resolving the optional names that are actually present in sys.argv.
"""

import sys

from awsglue.utils import getResolvedOptions


def resolve_args(required: list, optional: list) -> dict:
    args = getResolvedOptions(sys.argv, required)
    present = [name for name in optional if f"--{name}" in sys.argv]
    if present:
        args.update(getResolvedOptions(sys.argv, present))
    return args
