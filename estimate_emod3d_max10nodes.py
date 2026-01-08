#!/usr/bin/env python3
"""
Resource Estimator for EMOD3D (Cascade/ESNZ) - Cybershake Optimized
Logic:
  - Memory: Always ensures enough nodes to hold data.
  - Compute: Throughput-Optimized Scaling.
      - Objective: Minimize Node-Hours and scheduling friction (prefer fewer nodes).
      - Constraint 1: Max Nodes = 10 (File I/O scalability limit on Cascade).
      - Constraint 2: Max Walltime = 24 hours (or user specified).
      - Strategy: Start at min_nodes for memory. Only add nodes if simulated time > Max Walltime.
"""
import sys
import os
import yaml
import math

# --- HARDWARE & POLICY ---
PHYSICAL_CORES_PER_NODE = 192  # Cascade (Intel Sapphire Rapids)
RAM_PER_NODE_GB = 755.0        # Cascade High Mem
REQUEST_RAM_GB = 735.0         # Safe margin for OS/Filesystem cache
BASH_SCRIPT_OVERHEAD = 0.85    # Python/MPI overhead safety factor
MAX_NODES_CAP = 10             # Strict cap due to Lustre I/O contention

# Policy: Max simulation walltime allowed before we force scaling up.
# For Cybershake, we prefer letting it run longer on fewer nodes to reduce 
# queue wait time and total node-hours.
DEFAULT_MAX_WALLTIME_HOURS = 24.0

# Density Safety: If memory usage is very high, reduce tasks per node 
# to ensure we don't OOM due to MPI buffers.
HIGH_DENSITY_THRESHOLD_GB = RAM_PER_NODE_GB * 0.85
MIN_GB_PER_CORE_IF_FULL = 2.0

def find_project_root(start_path):
    """Recursively find the project root containing 'Data' or 'Runs'."""
    curr = os.path.abspath(start_path)
    while True:
        if os.path.isdir(os.path.join(curr, "Data")) or os.path.isdir(os.path.join(curr, "Runs")):
            return curr
        parent = os.path.dirname(curr)
        if parent == curr: return None
        curr = parent

def resolve_paths(input_arg):
    """
    Locates vm_params.yaml and root_params.yaml based on fault name or path.
    """
    vm_file = None
    root_file = None
    fault_name = None

    # Check if direct file path
    if os.path.exists(input_arg): start_search = input_arg
    else: start_search = os.getcwd()

    if os.path.isfile(input_arg) and input_arg.endswith("yaml"):
        vm_file = os.path.abspath(input_arg)
        project_root = find_project_root(os.path.dirname(vm_file))
        if project_root: root_file = os.path.join(project_root, "Runs", "root_params.yaml")
        return vm_file, root_file

    project_root = find_project_root(start_search)

    # Check if fault name directory
    if os.path.exists(input_arg):
        abspath = os.path.abspath(input_arg)
        parts = abspath.split(os.sep)
        if "Runs" in parts:
            idx = parts.index("Runs")
            if idx + 1 < len(parts): fault_name = parts[idx+1]
        if not fault_name:
             fname_candidate = os.path.basename(abspath)
             if project_root and os.path.exists(os.path.join(project_root, "Data", "VMs", fname_candidate)):
                 fault_name = fname_candidate
    else:
        fault_name = input_arg

    if project_root:
        if fault_name:
            candidate = os.path.join(project_root, "Data", "VMs", fault_name, "vm_params.yaml")
            if os.path.exists(candidate): vm_file = candidate
        candidate_root = os.path.join(project_root, "Runs", "root_params.yaml")
        if os.path.exists(candidate_root): root_file = candidate_root

    if not vm_file and os.path.isdir(input_arg):
        local_vm = os.path.join(input_arg, "vm_params.yaml")
        if os.path.exists(local_vm): vm_file = local_vm

    return vm_file, root_file

def estimate_resources(vm_file, root_file, max_walltime_hours=DEFAULT_MAX_WALLTIME_HOURS):
    if not vm_file or not os.path.exists(vm_file):
        return None, f"vm_params.yaml not found."

    with open(vm_file, 'r') as f: vm_data = yaml.safe_load(f)

    # Default dt for EMOD3D
    dt = 0.005
    if root_file and os.path.exists(root_file):
        with open(root_file, 'r') as f:
            root_data = yaml.safe_load(f)
            if 'dt' in root_data: dt = float(root_data['dt'])
    elif 'dt' in vm_data:
        dt = float(vm_data['dt'])

    try:
        nx = int(vm_data['nx'])
        ny = int(vm_data['ny'])
        nz = int(vm_data['nz'])
        if 'nt' in vm_data: 
            nt = int(vm_data['nt'])
        elif 'sim_duration' in vm_data: 
            nt = int(float(vm_data['sim_duration']) / dt)
        else: 
            return None, "Missing nt or sim_duration"
    except KeyError as e: 
        return None, f"Missing param: {e}"

    # --- CONSTANTS & CALCS ---
    # Approximate memory footprint for EMOD3D
    surface_term = max(nx*ny, ny*nz, nx*nz)
    M_bytes = 4 * (31 * nx * ny * nz + 56 * surface_term + 6 * (nx + nz))
    M_gb_needed = M_bytes / (1024**3)
    
    # Computational Volume and Slope (Performance coefficient)
    V = M_bytes * nt
    # Tuned slope for Cascade (Intel Sapphire Rapids)
    slope = 1.9e-9 
    
    max_sec = max_walltime_hours * 3600

    # --- 1. MINIMUM NODES (Memory Constraint) ---
    usable_ram_per_node = REQUEST_RAM_GB * BASH_SCRIPT_OVERHEAD
    min_nodes_mem = math.ceil(M_gb_needed / usable_ram_per_node)

    if min_nodes_mem > MAX_NODES_CAP:
        # We cap at MAX_NODES_CAP even if memory is insufficient, 
        # but we warn loudly. This prevents the estimator from returning > 10.
        print(f"WARNING: Job technically needs {min_nodes_mem} nodes for RAM ({M_gb_needed:.2f}GB), but capped at {MAX_NODES_CAP}. Job may OOM.")
        min_nodes_mem = MAX_NODES_CAP

    if min_nodes_mem < 1: min_nodes_mem = 1

    # --- 2. THROUGHPUT SOLVER (Time Constraint) ---
    # Start checking from the minimum node count required for RAM.
    # Only increase node count if the walltime exceeds our allowed limit.
    best_config = None

    for n in range(min_nodes_mem, MAX_NODES_CAP + 1):
        mem_per_node = M_gb_needed / n

        # Density Check: If node is very full, reduce tasks per node slightly to leave breathing room
        tasks_per_node = PHYSICAL_CORES_PER_NODE
        if mem_per_node > HIGH_DENSITY_THRESHOLD_GB:
             max_safe_tasks = int(mem_per_node / MIN_GB_PER_CORE_IF_FULL)
             tasks_per_node = min(PHYSICAL_CORES_PER_NODE, max_safe_tasks)
             if tasks_per_node < 1: tasks_per_node = 1

        total_cores = n * tasks_per_node
        pred_sec = (slope * V) / total_cores

        current_config = {
            "nodes": n,
            "tasks": tasks_per_node,
            "time": pred_sec,
            "cores": total_cores
        }

        if best_config is None: best_config = current_config

        # Logic: If this config runs within max_walltime, we take it.
        # Since we start from min_nodes, this ensures we pick the smallest 
        # node count that satisfies the time constraint.
        if pred_sec <= max_sec:
            best_config = current_config
            break
        
        # If we hit the cap, we must accept the best we can do at 10 nodes
        if n == MAX_NODES_CAP:
            best_config = current_config

    # --- 3. OUTPUT ---
    req_mem_gb = int(REQUEST_RAM_GB)

    # Pad walltime: 20% + 10 minutes safety
    req_walltime_sec = int(best_config['time'] * 1.2 + 600)
    
    # Format HH:MM:SS
    h, r = divmod(req_walltime_sec, 3600)
    m, s = divmod(r, 60)
    wall_str = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"

    return {
        "nodes": best_config['nodes'],
        "tasks_per_node": best_config['tasks'],
        "mem_gb": req_mem_gb,
        "walltime": wall_str,
        "est_hours": best_config['time'] / 3600.0,
        "mem_needed_total": M_gb_needed
    }, None

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 estimate_emod3d.py <FaultName_or_Path> [max_walltime_hours]")
        sys.exit(1)

    input_arg = sys.argv[1]
    # Allow overriding default 24h limit via command line
    target_val = float(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_MAX_WALLTIME_HOURS

    vm_file, root_file = resolve_paths(input_arg)
    res, err = estimate_resources(vm_file, root_file, target_val)

    if err:
        print(f"Error: {err}")
        sys.exit(1)

    # Printing shell-sourceable output
    print(f"NODES={res['nodes']}")
    print(f"TASKS_PER_NODE={res['tasks_per_node']}")
    print(f"MEM_PER_NODE={res['mem_gb']}gb")
    print(f"WALLTIME={res['walltime']}")
    # Debug info to stderr so it doesn't break shell sourcing
    print(f"DEBUG: Total Mem Needed: {res['mem_needed_total']:.2f} GB", file=sys.stderr)
    print(f"DEBUG: Est. Runtime: {res['est_hours']:.2f} h", file=sys.stderr)

if __name__ == "__main__":
    main()
