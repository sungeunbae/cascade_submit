#!/usr/bin/env python3
"""
submit_emod3d_for_fault.py

Updates:
  - Uses the unified, intelligent estimator (estimate_emod3d.py).
  - Uses central EMOD3D binary.
  - Ensures memory is calculated correctly for array jobs.
  - Lists targets explicitly.
  - Queue threshold restored to 700GB (matching 750GB hardware).
  - Updates logic for --force to skip COMPLETED jobs and ask for confirmation on IN_PROGRESS jobs.
  - Displays 'nt' from rlog and allows updating walltime interactively.
"""

import argparse
import os
import sys
import subprocess
import yaml
import glob
import shutil
import math
import datetime

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

def sanitize_params(params):
    if 'walltime' in params and isinstance(params['walltime'], int):
        h, r = divmod(params['walltime'], 3600)
        m, s = divmod(r, 60)
        params['walltime'] = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"
    
    params['walltime'] = str(params['walltime'])
    return params

def get_queue_name(total_mem_gb, walltime_str, nodes):
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

    mem_per_node_mb = (total_mem_gb * 1024) / nodes
    
    # 700GB Threshold (Assuming 750GB Nodes)
    LIMIT_HIGH_MEM_PER_NODE = 700000 

    if mem_per_node_mb > LIMIT_HIGH_MEM_PER_NODE:
        return "high_mem_longq" if total_seconds > SECONDS_48H else "high_mem_shortq"
    else:
        return "longq" if total_seconds > SECONDS_48H else "shortq"

def exec_estimation_script(fault_name):
    script_path = ESTIMATE_SCRIPT
    if not os.path.exists(script_path):
        print(f"  [ERROR] Estimator script not found at {script_path}")
        sys.exit(1)

    print(f"→ Running intelligent resource estimation for {fault_name}...")
    
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
    print(f"    (Nodes: {yaml_data['nodes']}, Tasks: {yaml_data['tasks_per_node']}, Total Mem: {yaml_data['mem_gb']}GB, Time: {yaml_data['walltime']})")

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

def get_rlog_details(run_dir):
    """
    Returns tuple (filepath, formatted_timestamp, last_5_lines_list, nt_value)
    or (None, None, [], None) if no rlog found.
    """
    rlog_dir = os.path.join(run_dir, "LF", "Rlog")
    if not os.path.isdir(rlog_dir): return None, None, [], None
    rlogs = glob.glob(os.path.join(rlog_dir, "*.rlog"))
    if not rlogs: return None, None, [], None
    
    latest_rlog = max(rlogs, key=os.path.getmtime)
    nt_val = None

    try:
        mtime = os.path.getmtime(latest_rlog)
        ts_str = datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
        
        with open(latest_rlog, 'r', errors='replace') as f:
            lines = f.readlines()
            tail = [l.rstrip() for l in lines[-5:]]
            
            # Scan all lines for 'nt='
            for line in lines:
                if "nt=" in line:
                    # Example line: "                    nt= 21639"
                    parts = line.split("nt=")
                    if len(parts) > 1:
                        val = parts[1].strip().split()[0]
                        if val.isdigit():
                            nt_val = val
                            # Usually appears early, but we break on first find
                            break
            
        return latest_rlog, ts_str, tail, nt_val
    except Exception as e:
        return latest_rlog, "Unknown", [f"Error reading file: {e}"], None

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
    mem_per_node_mb = int((params['mem_gb'] * 1024) / params['nodes'])
    if mem_per_node_mb < 1024: mem_per_node_mb = 1024

    cmd = [
        SUBMIT_BASH_SCRIPT,
        job_path,
        str(params['nodes']),
        str(params['tasks_per_node']),
        f"{mem_per_node_mb}M", 
        str(params['walltime']),
        defaults_file,
        "no"
    ]
    print(f"  → calling bash wrapper for Median...")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        print("  [ERROR] Bash script submission failed.")
        sys.exit(1)

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

    total_mem_gb = params['mem_gb']
    mem_per_node_mb = int((total_mem_gb * 1024) / params['nodes'])
    if mem_per_node_mb < 1024: mem_per_node_mb = 1024

    print(f"  → Memory Logic: Total {total_mem_gb}GB / {params['nodes']} Nodes = {mem_per_node_mb}MB per node")

    mem_mb_total = total_mem_gb * 1024
    total_tasks = params['nodes'] * params['tasks_per_node']
    maxmem_core = int((mem_mb_total / total_tasks) * 0.85)

    queue = get_queue_name(total_mem_gb, params['walltime'], params['nodes'])
    print(f"  → Queue Selected: {queue}")

    env_vars = [
        f"MAXMEM={maxmem_core}",
        f"EMOD3D_BIN={EMOD3D_BIN}",
        f"EMOD3D_DEFAULTS={defaults_file}",
        f"ENABLE_RESTART=no",
        f"ARRAY_MAP_FILE={map_file}"
    ]

    resource_list = f"select={params['nodes']}:ncpus={params['tasks_per_node']}:mpiprocs={params['tasks_per_node']}:ompthreads=1:mem={mem_per_node_mb}mb"

    if count == 1:
        print(f"  → Single target detected. Submitting as standard job...")
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

    print(f"  → Submitting...")
    subprocess.run(qsub_cmd, check=True)


def main():
    parser = argparse.ArgumentParser(description="Submit EMOD3D jobs.")
    parser.add_argument("fault_name", help="Name of the Fault")
    parser.add_argument("mode", nargs="?", default="MEDIAN", choices=["MEDIAN", "ALL"])
    parser.add_argument("--est-yaml", help="Override estimate yaml path.")
    parser.add_argument("--force", action="store_true", help="Submit jobs even if they appear IN_PROGRESS. Will NOT submit COMPLETED jobs.")
    parser.add_argument("--re-estimate", action="store_true", help="Re-run estimation.")

    args = parser.parse_args()

    fault_name = args.fault_name
    fault_dir, runs_root = resolve_paths(fault_name)

    project_root = os.path.dirname(runs_root)
    defaults_file = os.path.join(project_root, DEFAULTS_YAML_NAME)

    if not os.path.exists(defaults_file):
        print(f"CRITICAL ERROR: Defaults file missing: {defaults_file}")
        sys.exit(1)

    estimate_yaml_path = os.path.join(fault_dir, "emod3d_estimate.yaml")
    params = None

    if args.est_yaml:
        with open(args.est_yaml, 'r') as f: 
            params = yaml.safe_load(f)
            sanitize_params(params)
    
    elif args.re_estimate or not os.path.exists(estimate_yaml_path):
        new_params = exec_estimation_script(fault_name)
        sanitize_params(new_params)

        print("\n  ---------------------------------")
        print(f"  New Estimation for {fault_name}:")
        print(f"    Nodes: {new_params['nodes']}")
        print(f"    Tasks: {new_params['tasks_per_node']}")
        print(f"    Total Mem:   {new_params['mem_gb']} GB")
        print(f"    Time:  {new_params['walltime']}")
        print("  ---------------------------------")

        if os.path.exists(estimate_yaml_path) and args.re_estimate:
            backup_file(estimate_yaml_path)
        
        save_estimation_yaml(new_params, estimate_yaml_path)
        params = new_params

    else:
        print(f"  → Loading existing estimate: {estimate_yaml_path}")
        with open(estimate_yaml_path, 'r') as f: 
            params = yaml.safe_load(f)
            sanitize_params(params)

    if args.mode == "MEDIAN":
        median_dir = os.path.join(fault_dir, fault_name)
        status = get_run_status(median_dir)
        
        if status == "COMPLETED":
            print(f"  [SKIP] Median job is COMPLETED.")
        elif status == "NEW":
            submit_via_bash_script(median_dir, params, defaults_file)
        elif status == "IN_PROGRESS":
            if args.force:
                print(f"  [FORCE] Resubmitting IN_PROGRESS Median job.")
                submit_via_bash_script(median_dir, params, defaults_file)
            else:
                print(f"  [SKIP] Median job status: {status}")

    elif args.mode == "ALL":
        search_pattern = os.path.join(fault_dir, f"{fault_name}_REL*")
        all_dirs = sorted(glob.glob(search_pattern))
        valid_dirs = []

        print(f"Scanning {len(all_dirs)} realisations...")

        for d in all_dirs:
            if not os.path.isdir(d): continue
            status = get_run_status(d)
            rel_name = os.path.basename(d)

            if status == "COMPLETED":
                # Requirement: Skip completed even if forced
                # print(f"  - {rel_name}: COMPLETED")
                continue

            if status == "NEW":
                # Always submit NEW jobs
                valid_dirs.append(d)
                continue

            # Handle IN_PROGRESS
            if status == "IN_PROGRESS":
                if args.force:
                    rlog, rtime, rlines, nt_val = get_rlog_details(d)
                    print(f"\n  [WARN] Job {rel_name} is IN_PROGRESS but --force is active.")
                    if rlog:
                        print(f"         Rlog: {os.path.basename(rlog)}")
                        print(f"         Modified: {rtime}")
                        if nt_val:
                            print(f"         Simulation steps (nt): {nt_val}")
                        print(f"         Last lines:")
                        for l in rlines:
                            print(f"           | {l}")
                    else:
                        print("         (Status IN_PROGRESS but valid rlog not found?)")
                    
                    # Interactive confirmation
                    try:
                        user_input = input(f"         > Resubmit {rel_name}? (y/N): ")
                        if user_input.lower().strip() == 'y':
                            valid_dirs.append(d)
                            print("         > Marked for resubmission.")
                            
                            # Ask for walltime update
                            current_wall = params.get('walltime', 'Unknown')
                            print(f"         > Current walltime estimate is: {current_wall}")
                            wt_input = input(f"         > Do you want to increase the walltime? (Enter new HH:MM:SS or Press Enter to keep): ")
                            if wt_input.strip():
                                params['walltime'] = wt_input.strip()
                                sanitize_params(params)
                                # Update yaml
                                params['comment'] = f"Walltime updated manually by user on {datetime.datetime.now()}"
                                save_estimation_yaml(params, estimate_yaml_path)
                                print(f"         > Updated {os.path.basename(estimate_yaml_path)} with new walltime: {params['walltime']}")
                        else:
                            print("         > Skipped.")
                    except EOFError:
                        print("         > Non-interactive input detected. Skipping.")
                else:
                    # print(f"  - {rel_name}: IN_PROGRESS (Skip)")
                    pass

        if not valid_dirs:
            print("  ✓ All realisations finished or running (and none forced to resubmit).")
        else:
            submit_array_job(fault_dir, fault_name, valid_dirs, params, defaults_file)

if __name__ == "__main__":
    main()
