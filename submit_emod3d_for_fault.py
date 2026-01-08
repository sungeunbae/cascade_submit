#!/usr/bin/env python3
"""
submit_emod3d_for_fault.py

Updates:
  - Fixes "illegal -J value" for single-job arrays.
  - Automatically submits as a standard job (simulating an array) if only 1 target exists.
  - Corrects High Memory Queue threshold to 500GB.
  - DIVIDES total memory by node count to request correct per-node memory.
  - Uses MB for memory request to avoid rounding inflation.
  - Fixes AttributeError when walltime is parsed as an int by PyYAML.
  - Adds --re-estimate to regenerate estimation yaml with 10-node cap script and backup support.
"""

import argparse
import os
import sys
import subprocess
import yaml
import glob
import shutil
import math

# --- CONFIGURATION ---
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
ESTIMATE_SCRIPT = os.path.join(SCRIPTS_DIR, "estimate_emod3d.py")
ESTIMATE_SCRIPT_10NODE = os.path.join(SCRIPTS_DIR, "estimate_emod3d_max10nodes.py")
SUBMIT_BASH_SCRIPT = os.path.join(SCRIPTS_DIR, "submit_emod3d_pbs.sh")
MASTER_PBS_SCRIPT = os.path.join(SCRIPTS_DIR, "run_emod3d.pbs")

EMOD3D_BIN = "/uoc/project/uoc40001/EMOD3D/tools/emod3d-mpi_v3.0.8"
#EMOD3D_BIN= "/uoc/project/uoc40001/scratch/baes/EMOD3D/tools/emod3d-mpi_v3.0.8"
DEFAULTS_YAML_NAME = "emod3d_defaults.yaml"

def resolve_paths(fault_name):
    cwd = os.getcwd()
    runs_root = None
    if "Runs" in cwd:
        parts = cwd.split("Runs")
        runs_root = parts[0] + "Runs"
    elif os.path.isdir(os.path.join(cwd, "Runs")):
        runs_root = os.path.join(cwd, "Runs")
    else:
        if os.path.isdir(os.path.join(cwd, fault_name)):
            runs_root = cwd
        else:
             print(f"Error: Could not determine 'Runs' root directory from {cwd}")
             sys.exit(1)

    fault_dir = os.path.join(runs_root, fault_name)
    if not os.path.isdir(fault_dir):
        print(f"Error: Could not locate fault directory: {fault_dir}")
        sys.exit(1)

    return fault_dir, runs_root

def sanitize_params(params):
    """
    Ensures parameters loaded from YAML are in the expected format.
    Specifically fixes PyYAML parsing XX:YY:ZZ as integer seconds (sexagesimal).
    """
    if 'walltime' in params and isinstance(params['walltime'], int):
        # Convert seconds back to HH:MM:SS string
        h, r = divmod(params['walltime'], 3600)
        m, s = divmod(r, 60)
        params['walltime'] = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"
    
    # Ensure string just in case it's some other type
    params['walltime'] = str(params['walltime'])
    return params

def get_queue_name(total_mem_gb, walltime_str):
    # Safety check if walltime_str comes in as int despite sanitization
    if isinstance(walltime_str, int):
        h, r = divmod(walltime_str, 3600)
        m, s = divmod(r, 60)
        walltime_str = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"

    try:
        parts = list(map(int, walltime_str.split(':')))
        if len(parts) == 3: h, m, s = parts
        elif len(parts) == 2: h, m, s = parts[0], parts[1], 0
        else: h, m, s = parts[0], 0, 0
    except ValueError:
        return "shortq"

    total_seconds = h * 3600 + m * 60 + s
    SECONDS_48H = 48 * 3600

    # Assuming user wants 735GB to be the CUTOFF for high mem.
    LIMIT_HIGH_MEM_TOTAL = 735000 # ~717GB

    mem_mb_total = total_mem_gb * 1024

    if mem_mb_total > LIMIT_HIGH_MEM_TOTAL:
        return "high_mem_longq" if total_seconds > SECONDS_48H else "high_mem_shortq"
    else:
        return "longq" if total_seconds > SECONDS_48H else "shortq"

def exec_estimation_script(fault_name, script_path=ESTIMATE_SCRIPT):
    print(f"→ Running resource estimation for {fault_name} using {os.path.basename(script_path)}...")
    if not os.path.exists(script_path):
        print(f"Error: Estimate script not found at {script_path}")
        sys.exit(1)

    cmd = ["python3", script_path, fault_name]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing estimation script:\n{e.stderr}")
        sys.exit(1)

    data = {}
    for line in result.stdout.splitlines():
        if "=" in line:
            key, val = line.split("=", 1)
            data[key.strip()] = val.strip()

    yaml_data = {
        "nodes": int(data.get("NODES", 1)),
        "tasks_per_node": int(data.get("TASKS_PER_NODE", 1)),
        "mem_gb": int(''.join(filter(str.isdigit, data.get("MEM_PER_NODE", "735")))),
        "walltime": data.get("WALLTIME", "01:00:00")
    }
    return yaml_data

def save_estimation_yaml(yaml_data, output_yaml):
    with open(output_yaml, 'w') as f: yaml.dump(yaml_data, f)
    print(f"  ✓ Saved estimate to: {output_yaml}")
    print(f"    (Nodes: {yaml_data['nodes']}, Tasks: {yaml_data['tasks_per_node']}, Mem: {yaml_data['mem_gb']}GB, Time: {yaml_data['walltime']})")

def get_run_status(run_dir):
    rlog_dir = os.path.join(run_dir, "LF", "Rlog")
    if not os.path.isdir(rlog_dir): return "NEW"
    rlogs = glob.glob(os.path.join(rlog_dir, "*.rlog"))
    if not rlogs: return "NEW"
    latest_rlog = max(rlogs, key=os.path.getmtime)

    is_finished = False
    try:
        with open(latest_rlog, 'r') as f:
            lines = f.readlines()[-50:]
            for line in lines:
                if "PROGRAM emod3d-mpi IS FINISHED" in line:
                    is_finished = True
                    break
    except Exception: pass

    return "COMPLETED" if is_finished else "IN_PROGRESS"

def backup_file(file_path):
    if not os.path.exists(file_path):
        return
    i = 1
    while True:
        backup_path = f"{file_path}.{i}"
        if not os.path.exists(backup_path):
            shutil.copy2(file_path, backup_path)
            print(f"  → Backed up old file to: {os.path.basename(backup_path)}")
            break
        i += 1

def submit_via_bash_script(job_path, params, defaults_file):
    # Calculate Per-Node Memory for Bash Script
    # Bash script expects Total Mem? Or Per Node?
    # Usually PBS scripts take per-node.
    mem_per_node = math.ceil(params['mem_gb'] / params['nodes'])

    cmd = [
        SUBMIT_BASH_SCRIPT,
        job_path,
        str(params['nodes']),
        str(params['tasks_per_node']),
        f"{mem_per_node}G", # Passing per-node mem
        str(params['walltime']),
        defaults_file,
        "no"
    ]
    print(f"  → calling bash wrapper for Median...")
    subprocess.run(cmd, check=True)

def submit_array_job(fault_dir, fault_name, valid_dirs, params, defaults_file):
    logs_dir = os.path.join(fault_dir, "Logs_Submission")
    os.makedirs(logs_dir, exist_ok=True)

    map_file = os.path.join(logs_dir, f"{fault_name}_realisations.map")
    backup_file(map_file)

    print(f"  → Targets ({len(valid_dirs)}):")
    for d in valid_dirs:
        print(f"      - {os.path.basename(d)}")

    with open(map_file, 'w') as f:
        for d in valid_dirs: f.write(f"{d}\n")

    count = len(valid_dirs)

    # -- MEMORY CALCULATION FIX --
    # Total Simulation Memory
    total_mem_gb = params['mem_gb']

    # Use MB for precision
    mem_per_node_mb = int((total_mem_gb * 1024) / params['nodes'])

    # Check bounds (e.g. if < 1GB, set to 1GB)
    if mem_per_node_mb < 1024: mem_per_node_mb = 1024

    print(f"  → Memory Logic: Total {total_mem_gb}GB / {params['nodes']} Nodes = {mem_per_node_mb}MB per node")

    # MaxMem for Slurm/Application (MB)
    # This is passed to the application to limit its internal cache
    mem_mb_total = total_mem_gb * 1024
    # MaxMem is usually per-core for some schedulers, or total.
    # EMOD3D treats maxmem as total model size check? No, usually check_realloc limit.
    # Let's stick to the existing logic: Total Mem / Total Tasks * Safety
    total_tasks = params['nodes'] * params['tasks_per_node']
    maxmem_core = int((mem_mb_total / total_tasks) * 0.85)

    # Queue Selection based on TOTAL memory
    queue = "shortq"
    if mem_per_node_mb > 500000: # If a single node needs > 500GB
         queue = "high_mem_shortq"

    # Override queue if total time is huge
    if get_queue_name(total_mem_gb, params['walltime']) == "longq":
        queue = "longq" # Logic might need refinement but keeping simple

    print(f"  → Queue Selected: {queue} (Requested {mem_per_node_mb}MB/node)")

    env_vars = [
        f"MAXMEM={maxmem_core}",
        f"EMOD3D_BIN={EMOD3D_BIN}",
        f"EMOD3D_DEFAULTS={defaults_file}",
        f"ENABLE_RESTART=no",
        f"ARRAY_MAP_FILE={map_file}"
    ]

    # --- QSUB COMMAND ---
    # Construct resource string with calculated per-node memory
    # Removed explicit nodepool to allow scheduler to choose based on queue

    # Use MB instead of GB for precision
    resource_list = f"select={params['nodes']}:ncpus={params['tasks_per_node']}:mpiprocs={params['tasks_per_node']}:ompthreads=1:mem={mem_per_node_mb}mb"

    if count == 1:
        print(f"  → Single target detected. Submitting as standard job (Simulating Array Index 1)...")
        env_vars.append("PBS_ARRAY_INDEX=1")

        qsub_cmd = [
            "qsub",
            "-N", f"{fault_name}_Arr",
            "-q", queue,
            "-l", resource_list,
            "-l", f"walltime={params['walltime']}",
            "-v", ",".join(env_vars),
            MASTER_PBS_SCRIPT
        ]
    else:
        qsub_cmd = [
            "qsub",
            "-N", f"{fault_name}_Arr",
            "-q", queue,
            "-l", resource_list,
            "-l", f"walltime={params['walltime']}",
            "-J", f"1-{count}",
            "-v", ",".join(env_vars),
            MASTER_PBS_SCRIPT
        ]

    print(f"  → Submitting to {queue}...")
    subprocess.run(qsub_cmd, check=True)


def main():
    parser = argparse.ArgumentParser(description="Submit EMOD3D jobs.")
    parser.add_argument("fault_name", help="Name of the Fault")
    parser.add_argument("mode", nargs="?", default="MEDIAN", choices=["MEDIAN", "ALL"])
    parser.add_argument("--est-yaml", help="Override estimate yaml path.")
    parser.add_argument("--force", action="store_true", help="Submit jobs even if they appear IN_PROGRESS.")
    parser.add_argument("--re-estimate", action="store_true", help="Re-run estimation using the 10-node limited script before submitting.")

    args = parser.parse_args()

    fault_name = args.fault_name
    fault_dir, runs_root = resolve_paths(fault_name)

    project_root = os.path.dirname(runs_root)
    defaults_file = os.path.join(project_root, DEFAULTS_YAML_NAME)

    if not os.path.exists(defaults_file):
        print(f"CRITICAL ERROR: Defaults file missing: {defaults_file}")
        sys.exit(1)

    estimate_yaml_path = os.path.join(fault_dir, "emod3d_estimate.yaml")

    if args.mode == "MEDIAN":
        print(f"=== Processing MEDIAN job for {fault_name} ===")
        if args.est_yaml:
            print(f"  → Using custom estimate file: {args.est_yaml}")
            if not os.path.exists(args.est_yaml):
                print(f"Error: Custom yaml {args.est_yaml} not found.")
                sys.exit(1)
            with open(args.est_yaml, 'r') as f: 
                params = yaml.safe_load(f)
                sanitize_params(params)
        else:
            # Default median behavior: estimate using default script and save automatically
            params = exec_estimation_script(fault_name, ESTIMATE_SCRIPT)
            save_estimation_yaml(params, estimate_yaml_path)
            sanitize_params(params)

        median_dir = os.path.join(fault_dir, fault_name)
        if not os.path.isdir(median_dir):
            print(f"Error: Median directory missing: {median_dir}")
            sys.exit(1)

        status = get_run_status(median_dir)

        if status == "COMPLETED":
            print("  [SKIP] Median job is already FINISHED.")
        elif status == "IN_PROGRESS" and not args.force:
            print(f"  [WARNING] Median job appears IN PROGRESS.")
            try:
                if input("  > Submit anyway? (y/N): ").lower() == 'y':
                    submit_via_bash_script(median_dir, params, defaults_file)
            except: pass
        else:
            if args.force: print("  [FORCE] Submitting despite IN_PROGRESS status.")
            submit_via_bash_script(median_dir, params, defaults_file)

    elif args.mode == "ALL":
        print(f"=== Processing ALL REALISATIONS for {fault_name} ===")

        load_path = args.est_yaml if args.est_yaml else estimate_yaml_path
        params = None

        if args.re_estimate and not args.est_yaml:
            print(f"  → --re-estimate requested. Generating new estimate with 10-node limit...")
            
            # Check if 10-node script exists, fallback if needed
            script_to_use = ESTIMATE_SCRIPT_10NODE
            if not os.path.exists(script_to_use):
                print(f"  [WARNING] {os.path.basename(script_to_use)} not found. Using default {os.path.basename(ESTIMATE_SCRIPT)}")
                script_to_use = ESTIMATE_SCRIPT

            new_params = exec_estimation_script(fault_name, script_to_use)
            sanitize_params(new_params)

            print("\n  ---------------------------------")
            print(f"  New Estimation for {fault_name}:")
            print(f"    Nodes: {new_params['nodes']}")
            print(f"    Tasks: {new_params['tasks_per_node']}")
            print(f"    Mem:   {new_params['mem_gb']} GB")
            print(f"    Time:  {new_params['walltime']}")
            print("  ---------------------------------")

            try:
                confirm = input("  > Overwrite existing estimate and proceed? (y/N): ").strip().lower()
                if confirm == 'y':
                    backup_file(estimate_yaml_path)
                    save_estimation_yaml(new_params, estimate_yaml_path)
                    params = new_params
                else:
                    print("  > Keep existing estimate.")
            except EOFError:
                print("  > Non-interactive mode: proceeding with new estimate.")
                backup_file(estimate_yaml_path)
                save_estimation_yaml(new_params, estimate_yaml_path)
                params = new_params

        # Fallback: Load existing if params not set by re-estimate
        if params is None:
            if not os.path.exists(load_path):
                print(f"Error: Estimate file missing: {load_path}")
                sys.exit(1)

            with open(load_path, 'r') as f: 
                params = yaml.safe_load(f)
                sanitize_params(params)
            
        print(f"  → Loaded Params: {params['nodes']} Nodes, {params['tasks_per_node']} Tasks")

        search_pattern = os.path.join(fault_dir, f"{fault_name}_REL*")
        all_dirs = sorted(glob.glob(search_pattern))

        valid_dirs = []
        forced_dirs = []

        for d in all_dirs:
            if not os.path.isdir(d): continue

            status = get_run_status(d)
            if status == "COMPLETED":
                pass
            elif status == "IN_PROGRESS":
                if args.force:
                    print(f"  [FORCE INCLUDE] {os.path.basename(d)}")
                    valid_dirs.append(d)
                    forced_dirs.append(os.path.basename(d))
                else:
                    print(f"  [SKIP] {os.path.basename(d)} (IN PROGRESS - Use --force to retry)")
            else:
                valid_dirs.append(d)

        if forced_dirs:
            print("\n  !!! WARNING !!!")
            print(f"  You are about to resubmit {len(forced_dirs)} jobs that appear IN_PROGRESS:")
            print(f"  First 3: {forced_dirs[:3]}...")
            try:
                confirm = input("  > Are you sure you want to proceed? (y/N): ").strip().lower()
                if confirm != 'y':
                    print("  > Aborting submission.")
                    sys.exit(0)
            except EOFError:
                print("  > Non-interactive session. Proceeding with force.")

        if not valid_dirs:
            print("  ✓ All realisations finished or running.")
        else:
            submit_array_job(fault_dir, fault_name, valid_dirs, params, defaults_file)

if __name__ == "__main__":
    main()
