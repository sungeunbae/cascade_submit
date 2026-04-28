#!/usr/bin/env python3
"""
Generate a .stoch file from an existing .srf file.
Replicates the exact logic used by realisation_to_srf.py:create_stoch
(for both single-segment and multi-segment / multi-plane SRFs).
"""

import argparse
import os
import subprocess
from typing import Optional

from qcore import binary_version, srf
from pathlib import Path

SRF2STOCH = "srf2stoch"


def create_stoch(
    stoch_file: str,
    srf_file: str,
    single_segment: bool = False,
) -> None:
    """
    Create a stoch file from a SRF file (exact copy of the original logic).

    Parameters
    ----------
    stoch_file : str
        Output .stoch path.
    srf_file : str
        Input .srf path.
    single_segment : bool
        True  → use target_dx / target_dy  (type 1, type 2, or type 4 with 1 plane)
        False → use dx / dy               (type 4 with >1 plane, i.e. your case)
    """
    out_dir = os.path.abspath(os.path.dirname(stoch_file))
    os.makedirs(out_dir, exist_ok=True)

    dx, dy = 2.0, 2.0
    if not srf.is_ff(srf_file):
        dx, dy = srf.srf_dxy(srf_file)

#    srf2stoch = str(Path(__file__).parent.resolve() / SRF2STOCH)
#    srf2stoch = binary_version.get_unversioned_bin(SRF2STOCH)
#    srf2stoch = f"/uoc/project/uoc40001/EMOD3D/tools/{SRF2STOCH}"
    srf2stoch = f"/uoc/project/uoc40001/scratch/baes/EMOD3D/tools/{SRF2STOCH}"

    if single_segment:
        cmd = [srf2stoch, f"target_dx={dx}", f"target_dy={dy}"]
    else:
        cmd = [srf2stoch, f"dx={dx}", f"dy={dy}"]

    cmd.extend([f"infile={srf_file}", f"outfile={stoch_file}"])

    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print(f"Created: {stoch_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate .stoch from .srf (multi-plane safe)"
    )
    parser.add_argument("srf_file", help="Path to the .srf file")
    parser.add_argument(
        "--single-segment",
        action="store_true",
        help="Use target_dx/target_dy (only for single-plane SRFs)",
    )
    args = parser.parse_args()

    stoch_file = args.srf_file.replace(".srf", ".stoch")
    create_stoch(stoch_file, args.srf_file, single_segment=args.single_segment)

