#!/usr/bin/env python3
"""
submit_emod3d_for_fault.py

Updates:
  - Fixes "illegal -J value" for single-job arrays.
  - Automatically submits as a standard job (simulating an array) if only 1 target exists.
"""

import argparse
import os
import sys
import subprocess
import yaml
import glob
import shutil

# --- CONFIGURATION ---
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
ESTIMATE_SCRIPT = os.path.join(SCRIPTS_DIR, "estimate_emod3d.py")
SUBMIT_BASH_SCRIPT = os.path.join(SCRIPTS_DIR, "submit_emod3d_pbs.sh")
MASTER_PBS_SCRIPT = os.path.join(SCRIPTS_DIR, "run_emod3d.pbs") 

EMOD3D_BIN = "/uoc/project/uoc40001/EMOD3D/tools/emod3d-mpi_v3.0.8"
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

def get_queue_name(mem_gb, walltime_str):
    try:
        parts = list(map(int, walltime_str.split(':')))
        if len(parts) == 3: h, m, s = parts
        elif len(parts) == 2: h, m, s = parts[0], parts[1], 0
        else: h, m, s = parts[0], 0, 0
    except ValueError:
        return "shortq"

    total_seconds = h * 3600 + m * 60 + s
    SECONDS_48H = 48 * 3600
    mem_mb = mem_gb * 1024
    LIMIT_HIGH_MEM = 770000

    if mem_mb > LIMIT_HIGH_MEM:
        return "high_mem_longq" if total_seconds > SECONDS_48H else "high_mem_shortq"
    else:
        return "longq" if total_seconds > SECONDS_48H else "shortq"

def run_estimation(fault_name, fault_dir, output_yaml):
    print(f"→ Running resource estimation for {fault_name}...")
    if not os.path.exists(ESTIMATE_SCRIPT):
        print(f"Error: Estimate script not found at {ESTIMATE_SCRIPT}")
        sys.exit(1)

    cmd = ["python3", ESTIMATE_SCRIPT, fault_name]
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

    with open(output_yaml, 'w') as f: yaml.dump(yaml_data, f)
    print(f"  ✓ Saved estimate to: {output_yaml}")
    print(f"    (Nodes: {yaml_data['nodes']}, Tasks: {yaml_data['tasks_per_node']}, Mem: {yaml_data['mem_gb']}GB, Time: {yaml_data['walltime']})")
    return yaml_data

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

def backup_map_file(map_file):
    if not os.path.exists(map_file):
        return
    i = 1
    while True:
        backup_path = f"{map_file}.{i}"
        if not os.path.exists(backup_path):
            shutil.copy2(map_file, backup_path)
            print(f"  → Backed up old map file to: {os.path.basename(backup_path)}")
            break
        i += 1

def submit_via_bash_script(job_path, params, defaults_file):
    cmd = [
        SUBMIT_BASH_SCRIPT,
        job_path,
        str(params['nodes']),
        str(params['tasks_per_node']),
        f"{params['mem_gb']}G",
        params['walltime'],
        defaults_file,
        "no"
    ]
    print(f"  → calling bash wrapper for Median...")
    subprocess.run(cmd, check=True)

def submit_array_job(fault_dir, fault_name, valid_dirs, params, defaults_file):
    logs_dir = os.path.join(fault_dir, "Logs_Submission")
    os.makedirs(logs_dir, exist_ok=True)
    
    map_file = os.path.join(logs_dir, f"{fault_name}_realisations.map")
    
    # BACKUP OLD MAP
    backup_map_file(map_file)
    
    # VISUALIZATION
    print(f"  → Targets ({len(valid_dirs)}):")
    for d in valid_dirs:
        print(f"      - {os.path.basename(d)}")

    with open(map_file, 'w') as f:
        for d in valid_dirs: f.write(f"{d}\n")
    
    count = len(valid_dirs)
    
    mem_mb_total = params['mem_gb'] * 1024
    maxmem_core = int((mem_mb_total / params['tasks_per_node']) * 0.85)
    queue = get_queue_name(params['mem_gb'], params['walltime'])

    env_vars = [
        f"MAXMEM={maxmem_core}",
        f"EMOD3D_BIN={EMOD3D_BIN}",
        f"EMOD3D_DEFAULTS={defaults_file}",
        f"ENABLE_RESTART=no",
        f"ARRAY_MAP_FILE={map_file}"
    ]

    # --- SINGLE JOB VS ARRAY LOGIC ---
    if count == 1:
        # PBS rejects -J 1-1. We submit as a standard job but INJECT the array index manually.
        print(f"  → Single target detected. Submitting as standard job (Simulating Array Index 1)...")
        # We inject PBS_ARRAY_INDEX=1 so the PBS script logic (which reads the map file) still works!
        env_vars.append("PBS_ARRAY_INDEX=1")
        
        qsub_cmd = [
            "qsub",
            "-N", f"{fault_name}_Arr",
            "-q", queue,
            "-l", f"select={params['nodes']}:ncpus={params['tasks_per_node']}:mpiprocs={params['tasks_per_node']}:ompthreads=1:mem={params['mem_gb']}gb",
            "-l", f"walltime={params['walltime']}",
            "-v", ",".join(env_vars),
            MASTER_PBS_SCRIPT 
        ]
    else:
        # Standard Job Array
        qsub_cmd = [
            "qsub",
            "-N", f"{fault_name}_Arr",
            "-q", queue,
            "-l", f"select={params['nodes']}:ncpus={params['tasks_per_node']}:mpiprocs={params['tasks_per_node']}:ompthreads=1:mem={params['mem_gb']}gb",
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
            with open(args.est_yaml, 'r') as f: params = yaml.safe_load(f)
        else:
            params = run_estimation(fault_name, fault_dir, estimate_yaml_path)

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
        if not os.path.exists(load_path):
            print(f"Error: Estimate file missing: {load_path}")
            sys.exit(1)
            
        with open(load_path, 'r') as f: params = yaml.safe_load(f)
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
