#!/usr/bin/env python3
"""
Generate HF simulation command from YAML config files.

Usage:
    python run_hf_command.py <rel_dir> [--bin-dir <path>]
"""

import argparse
import os
import sys
from pathlib import Path

from qcore import utils

# Default binary directory (can be overridden with --bin-dir)
BIN_DIR = "/uoc/project/uoc40001/scratch/baes/tools"


def load_config_hierarchy(rel_dir):
    """Load all config files in hierarchy."""
    rel_path = Path(rel_dir)

    # sim_params.yaml (realisation level)
    sim_params_path = rel_path / "sim_params.yaml"
    if not sim_params_path.exists():
        raise FileNotFoundError(f"sim_params.yaml not found at {sim_params_path}")

    sim_params = utils.load_yaml(str(sim_params_path))

    # fault_params.yaml (event level)
    fault_params_path = Path(sim_params["fault_yaml_path"])
    if not fault_params_path.exists():
        raise FileNotFoundError(f"fault_params.yaml not found at {fault_params_path}")

    fault_params = utils.load_yaml(str(fault_params_path))

    # root_params.yaml (run level)
    root_params_path = Path(fault_params["root_yaml_path"])
    if not root_params_path.exists():
        raise FileNotFoundError(f"root_params.yaml not found at {root_params_path}")

    root_params = utils.load_yaml(str(root_params_path))

    # vm_params.yaml (velocity model parameters)
    vel_mod_dir = fault_params.get("vel_mod_dir")
    vm_params = None
    if vel_mod_dir:
        vm_params_path = Path(vel_mod_dir) / "vm_params.yaml"
        if vm_params_path.exists():
            vm_params = utils.load_yaml(str(vm_params_path))
            print(f"# Loaded vm_params.yaml from: {vm_params_path}", file=sys.stderr)

    return sim_params, fault_params, root_params, vm_params


def get_duration(sim_params, hf_params, vm_params):
    """Get simulation duration from configs. REQUIRED - no fallback."""
    # Priority: sim_params > hf_params > vm_params
    if "hf" in sim_params and "duration" in sim_params["hf"]:
        return sim_params["hf"]["duration"]

    if "duration" in hf_params:
        return hf_params["duration"]

    if vm_params and "sim_duration" in vm_params:
        return vm_params["sim_duration"]

    # FAIL instead of using fallback
    raise ValueError(
        "Duration not found in configs. Expected one of:\n"
        "  - sim_params['hf']['duration']\n"
        "  - root_params['hf']['duration']\n"
        "  - vm_params['sim_duration']"
    )


def get_dt(hf_params):
    """Get timestep from config. REQUIRED - no fallback."""
    if "dt" not in hf_params:
        raise ValueError(
            "dt not found in hf config. Expected:\n"
            "  - root_params['hf']['dt']"
        )

    return hf_params["dt"]


def get_seed(sim_params, hf_params):
    """Get seed value, calculating event-specific seed if needed. REQUIRED - no fallback."""
    # Check for explicit seed in sim_params (realisation-specific)
    if "hf" in sim_params and "seed" in sim_params["hf"]:
        return sim_params["hf"]["seed"]

    # Check for seed in root params
    if "seed" not in hf_params:
        raise ValueError(
            "seed not found in hf config. Expected:\n"
            "  - root_params['hf']['seed']"
        )

    seed = hf_params["seed"]

    if seed == 0:
        # seed=0 means use event-specific seed
        # Generate from event name/srf path
        import hashlib

        srf_file = sim_params.get("srf_file", "")
        # Use hash of srf filename to generate reproducible seed
        seed_str = os.path.basename(srf_file)
        seed_hash = int(hashlib.md5(seed_str.encode()).hexdigest()[:8], 16)
        print(
            f"# Generated event-specific seed: {seed_hash} (from {seed_str})",
            file=sys.stderr,
        )
        return seed_hash

    return seed


def get_sim_bin(hf_params, bin_dir=None):
    """Get HF binary path."""
    # Check if sim_bin is explicitly specified in config
    if "sim_bin" in hf_params and hf_params["sim_bin"]:
        return hf_params["sim_bin"]

    # Use provided bin_dir or default
    if bin_dir is None:
        bin_dir = BIN_DIR

    # Get version (required)
    if "version" not in hf_params:
        print("# Warning: version not found in hf config, cannot locate binary", file=sys.stderr)
        return None

    version = hf_params["version"]

    # Map version to binary name
    if version.startswith("6.0.3"):
        binary = f"{bin_dir}/hb_high_binmod_v{version}"
    elif version.startswith("5.4"):
        binary = f"{bin_dir}/hb_high_v{version}"
    else:
        binary = f"{bin_dir}/hb_high_binmod_v{version}"

    # Check if binary exists
    if Path(binary).exists():
        return binary

    # If not found, return None and let version flag handle it
    print(f"# Warning: Binary not found at {binary}, using --version flag", file=sys.stderr)
    return None


def build_hf_command(rel_dir, sim_params, fault_params, root_params, vm_params, bin_dir=None):
    """Build hf_sim.py command from config hierarchy."""
    rel_path = Path(rel_dir)

    # Get HF parameters from configs
    hf_params = root_params.get("hf", {})
    sim_hf_params = sim_params.get("hf", {})

    # Required paths
    stat_file = fault_params["FD_STATLIST"]
    out_file = rel_path / "HF" / "Acc" / "HF.bin"

    srf_file = sim_params["srf_file"]
    slip_file = sim_hf_params.get("slip", srf_file)

    # Validate required files exist
    if not Path(srf_file).exists():
        raise FileNotFoundError(f"SRF file not found: {srf_file}")
    if not Path(stat_file).exists():
        raise FileNotFoundError(f"Station file not found: {stat_file}")
    if not Path(slip_file).exists():
        raise FileNotFoundError(f"Slip file not found: {slip_file}")

    # Build base command (no srun on Cascade)
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    hf_sim_script = os.path.join(scripts_dir, "hf_sim.py")
    command = f"python {hf_sim_script} {stat_file} {out_file}"

    # Get REQUIRED parameters (will raise if not found)
    duration = get_duration(sim_params, hf_params, vm_params)
    dt = get_dt(hf_params)
    seed = get_seed(sim_params, hf_params)

    command += f" --duration {duration}"
    command += f" --dt {dt}"

    # Add sim_bin if available
    sim_bin = get_sim_bin(hf_params, bin_dir)
    if sim_bin:
        command += f" --sim_bin {sim_bin}"

    command += f" --seed {seed}"

    # Add version
    if "version" in hf_params:
        command += f" --version {hf_params['version']}"

    # Add slip parameter
    command += f" --slip {slip_file}"

    # --sdrop (stress drop) - can be overridden per realisation
    if "sdrop" in sim_hf_params:
        command += f" --sdrop {sim_hf_params['sdrop']}"
    elif "sdrop" in hf_params:
        command += f" --sdrop {hf_params['sdrop']}"

    # --kappa
    if "kappa" in sim_hf_params:
        command += f" --kappa {sim_hf_params['kappa']}"
    elif "kappa" in hf_params:
        command += f" --kappa {hf_params['kappa']}"

    # --rvfac - can be overridden per realisation
    if "rvfac" in sim_hf_params:
        command += f" --rvfac {sim_hf_params['rvfac']}"
    elif "rvfac" in hf_params:
        command += f" --rvfac {hf_params['rvfac']}"

    # --rvfac_shal
    if "rvfac_shal" in hf_params:
        command += f" --rvfac_shal {hf_params['rvfac_shal']}"

    # --rvfac_deep
    if "rvfac_deep" in hf_params:
        command += f" --rvfac_deep {hf_params['rvfac_deep']}"

    # --czero
    if "czero" in hf_params:
        command += f" --czero {hf_params['czero']}"

    # --rayset
    if "rayset" in hf_params:
        rayset = hf_params["rayset"]
        if isinstance(rayset, list):
            command += f" --rayset {' '.join(map(str, rayset))}"
        else:
            command += f" --rayset {rayset}"

    # --path_dur
    if "path_dur" in hf_params:
        command += f" --path_dur {hf_params['path_dur']}"

    # --hf_vel_mod_1d
    if "hf_vel_mod_1d" in hf_params:
        vel_mod_1d = hf_params["hf_vel_mod_1d"]
        if Path(vel_mod_1d).exists():
            command += f" --hf_vel_mod_1d {vel_mod_1d}"

    # --vs-moho
    if "vs_moho" in hf_params:
        command += f" --vs-moho {hf_params['vs_moho']}"

    # Site-specific options
    if "site_specific" in hf_params and hf_params["site_specific"]:
        command += " --site_specific"
        if "site_v1d_dir" in hf_params:
            command += f" --site_v1d_dir {hf_params['site_v1d_dir']}"

    return command


def main():
    parser = argparse.ArgumentParser(
        description="Generate HF simulation command from YAML config files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default binary directory
  %(prog)s /path/to/realisation

  # Override binary directory
  %(prog)s /path/to/realisation --bin-dir /custom/path/to/binaries

  # Save command to file
  %(prog)s /path/to/realisation --save-to hf_command.sh

Default binary directory: """
        + BIN_DIR
        + """
        """,
    )

    parser.add_argument("rel_dir", help="Path to realisation directory")

    parser.add_argument(
        "--bin-dir",
        help=f"Directory containing HF binaries (default: {BIN_DIR})",
        default=None,
    )

    parser.add_argument("--save-to", help="Save command to specified file (optional)")

    args = parser.parse_args()

    if not os.path.exists(args.rel_dir):
        print(f"Error: Directory not found: {args.rel_dir}", file=sys.stderr)
        sys.exit(1)

    if args.bin_dir:
        print(f"# Using custom binary directory: {args.bin_dir}", file=sys.stderr)
    else:
        print(f"# Using default binary directory: {BIN_DIR}", file=sys.stderr)

    print(f"# Reading config hierarchy from: {args.rel_dir}", file=sys.stderr)

    try:
        sim_params, fault_params, root_params, vm_params = load_config_hierarchy(args.rel_dir)

        print("# Loaded sim_params.yaml", file=sys.stderr)
        print(f"# Loaded fault_params.yaml from: {sim_params['fault_yaml_path']}", file=sys.stderr)
        print(f"# Loaded root_params.yaml from: {fault_params['root_yaml_path']}", file=sys.stderr)

        command = build_hf_command(
            args.rel_dir,
            sim_params,
            fault_params,
            root_params,
            vm_params,
            bin_dir=args.bin_dir,
        )

        # Save to file if requested
        if args.save_to:
            from datetime import datetime

            with open(args.save_to, "w", encoding="utf-8") as f:
                f.write("#!/bin/bash\n")
                f.write("# HF simulation command\n")
                f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# REL_DIR: {args.rel_dir}\n")
                if args.bin_dir:
                    f.write(f"# BIN_DIR: {args.bin_dir}\n")
                else:
                    f.write(f"# BIN_DIR: {BIN_DIR} (default)\n")
                f.write("\n")
                f.write(command + "\n")
            os.chmod(args.save_to, 0o755)
            print(f"# Command saved to: {args.save_to}", file=sys.stderr)

        # Print command to stdout
        print(command)

    except Exception as exc:
        print(f"Error generating HF command: {exc}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
