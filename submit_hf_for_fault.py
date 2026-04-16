#!/usr/bin/env python3
"""
Submit HF jobs for a fault on Cascade.

Usage:
    python submit_hf_for_fault.py <fault_name> [MEDIAN|ALL] [--force] [--walltime HH:MM:SS] [--ncpus N] [--mem MEM]
"""

import argparse
import glob
import os
import subprocess
import sys

# --- CONFIGURATION ---
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
SUBMIT_BASH_SCRIPT = os.path.join(SCRIPTS_DIR, "submit_hf_pbs.sh")
MASTER_PBS_SCRIPT = os.path.join(SCRIPTS_DIR, "run_hf.pbs")
DEFAULT_GMSIM_ENV = "/uoc/project/uoc40001/Environments/mrd87_4"
DEFAULT_HF_BIN_DIR = "/uoc/project/uoc40001/scratch/baes/tools"

DEFAULT_NCPUS = 4
DEFAULT_MEM = "84GB"
DEFAULT_WALLTIME = "00:30:00"


def resolve_paths(fault_name):
    cwd = os.getcwd()
    runs_root = None
    if "Runs" in cwd:
        parts = cwd.split("Runs")
        runs_root = parts[0] + "Runs"
    elif os.path.isdir(os.path.join(cwd, "Runs")):
        runs_root = os.path.join(cwd, "Runs")
    elif os.path.isdir(os.path.join(cwd, fault_name)):
        runs_root = cwd
    else:
        print(f"Error: Could not determine 'Runs' root directory from {cwd}")
        sys.exit(1)

    fault_dir = os.path.join(runs_root, fault_name)
    if not os.path.isdir(fault_dir):
        print(f"Error: Could not locate fault directory: {fault_dir}")
        sys.exit(1)

    return fault_dir, runs_root


def get_hf_status(rel_dir):
    hf_bin = os.path.join(rel_dir, "HF", "Acc", "HF.bin")
    return "COMPLETED" if os.path.exists(hf_bin) and os.path.getsize(hf_bin) > 0 else "NEW"


def parse_walltime_seconds(walltime):
    parts = walltime.split(":")
    if len(parts) != 3:
        raise ValueError(f"Invalid walltime format: {walltime}. Expected HH:MM:SS")
    hh, mm, ss = (int(parts[0]), int(parts[1]), int(parts[2]))
    return hh * 3600 + mm * 60 + ss


def choose_queue(walltime):
    return "shortq" if parse_walltime_seconds(walltime) <= 48 * 3600 else "longq"


def confirm_or_exit(force, prompt):
    if force:
        return
    try:
        answer = input(f"{prompt} (y/N): ").strip().lower()
    except EOFError:
        answer = ""
    if answer != "y":
        print("Submission cancelled.")
        sys.exit(0)


def submit_median(rel_dir, ncpus, mem, walltime, gmsim_env):
    cmd = [
        SUBMIT_BASH_SCRIPT,
        rel_dir,
        str(ncpus),
        mem,
        walltime,
        gmsim_env,
    ]
    subprocess.run(cmd, check=True)


def submit_array(fault_dir, fault_name, rel_dirs, ncpus, mem, walltime, gmsim_env, hf_bin_dir):
    logs_dir = os.path.join(fault_dir, "Logs_Submission")
    os.makedirs(logs_dir, exist_ok=True)
    map_file = os.path.join(logs_dir, f"{fault_name}_hf_realisations.map")

    with open(map_file, "w", encoding="utf-8") as f:
        for rel_dir in rel_dirs:
            f.write(f"{rel_dir}\n")

    queue = choose_queue(walltime)
    resource_list = f"select=1:ncpus={ncpus}:mem={mem}"
    env_vars = [
        f"ARRAY_MAP_FILE={map_file}",
        f"GMSIM_ENV={gmsim_env}",
        f"HF_BIN_DIR={hf_bin_dir}",
        f"SCRIPTS_DIR={SCRIPTS_DIR}",
        f"JOBNAME={fault_name}",
    ]

    qsub_cmd = [
        "qsub",
        "-N",
        f"hf.{fault_name}",
        "-q",
        queue,
        "-l",
        resource_list,
        "-l",
        f"walltime={walltime}",
        "-J",
        f"1-{len(rel_dirs)}",
        "-v",
        ",".join(env_vars),
        MASTER_PBS_SCRIPT,
    ]
    subprocess.run(qsub_cmd, check=True)


def main():
    parser = argparse.ArgumentParser(description="Submit HF jobs for a fault on Cascade.")
    parser.add_argument("fault_name", help="Name of the fault")
    parser.add_argument("mode", nargs="?", default="MEDIAN", choices=["MEDIAN", "ALL"])
    parser.add_argument("--force", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--walltime", default=DEFAULT_WALLTIME, help=f"Walltime (default: {DEFAULT_WALLTIME})")
    parser.add_argument("--ncpus", type=int, default=DEFAULT_NCPUS, help=f"CPUs per job (default: {DEFAULT_NCPUS})")
    parser.add_argument("--mem", default=DEFAULT_MEM, help=f"Memory per node (default: {DEFAULT_MEM})")
    parser.add_argument("--gmsim-env", default=DEFAULT_GMSIM_ENV, help=f"GMSIM env root (default: {DEFAULT_GMSIM_ENV})")
    parser.add_argument("--hf-bin-dir", default=DEFAULT_HF_BIN_DIR, help=f"HF bin directory (default: {DEFAULT_HF_BIN_DIR})")
    args = parser.parse_args()

    if args.ncpus < 1:
        print("Error: --ncpus must be >= 1")
        sys.exit(1)

    fault_dir, _ = resolve_paths(args.fault_name)

    if args.mode == "MEDIAN":
        rel_dir = os.path.join(fault_dir, args.fault_name)
        if not os.path.isdir(rel_dir):
            print(f"Error: Median realisation not found: {rel_dir}")
            sys.exit(1)
        status = get_hf_status(rel_dir)
        if status == "COMPLETED":
            print("Median HF already completed. Nothing to submit.")
            return

        print("HF MEDIAN target:")
        print(f"  - {rel_dir}")
        print(f"Queue: {choose_queue(args.walltime)}")
        print(f"Resources: select=1:ncpus={args.ncpus}:mem={args.mem}, walltime={args.walltime}")
        confirm_or_exit(args.force, "Submit median HF job?")
        submit_median(rel_dir, args.ncpus, args.mem, args.walltime, args.gmsim_env)
        return

    all_dirs = sorted(glob.glob(os.path.join(fault_dir, f"{args.fault_name}_REL*")))
    targets = [d for d in all_dirs if os.path.isdir(d) and get_hf_status(d) != "COMPLETED"]

    if not targets:
        print("All realisations are already completed. Nothing to submit.")
        return

    print(f"HF ALL mode targets ({len(targets)}):")
    for rel_dir in targets:
        print(f"  - {os.path.basename(rel_dir)}")
    print(f"Queue: {choose_queue(args.walltime)}")
    print(f"Resources: select=1:ncpus={args.ncpus}:mem={args.mem}, walltime={args.walltime}")
    confirm_or_exit(args.force, f"Submit HF array job for {len(targets)} realisations?")

    submit_array(
        fault_dir=fault_dir,
        fault_name=args.fault_name,
        rel_dirs=targets,
        ncpus=args.ncpus,
        mem=args.mem,
        walltime=args.walltime,
        gmsim_env=args.gmsim_env,
        hf_bin_dir=args.hf_bin_dir,
    )


if __name__ == "__main__":
    main()
