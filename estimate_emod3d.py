#!/usr/bin/env python3
"""
Resource Estimator for EMOD3D (Cascade/ESNZ) - Cybershake Optimized
Logic:
  - Memory: Adds MPI overhead (1.5GB/task) to Grid Memory.
  - Queue Strategy: 
      - Tries to fit job into < 350GB per node (Safe Zone).
      - Increases node count until this condition is met.
"""
import sys
import os
import yaml
import math
import subprocess

# --- HARDWARE CONSTANTS ---
PHYSICAL_CORES_PER_NODE = 192  
MAX_NODES_CAP = 9  # Reduced to 9 to avoid metadata storm issues (previously 10/12)           

# --- MEMORY CONSTANTS ---
# Heuristic: 31 * float4 * grid_points
BYTES_PER_GRID_POINT = 4 * 31 
# Heuristic: MPI/OS Overhead per rank
OVERHEAD_PER_TASK_GB = 1.5     

# Scheduling Thresholds
# We lower this to 350GB. Even though nodes have 750GB, 
# the standard queue seems to reject/kill jobs > 450GB.
SAFE_STANDARD_MEM_GB = 350.0
# Hard limit for High Mem nodes
SAFE_HIGH_MEM_GB = 730.0

DEFAULT_MAX_WALLTIME_HOURS = 24.0

def find_project_root(start_path):
    curr = os.path.abspath(start_path)
    while True:
        if os.path.isdir(os.path.join(curr, "Data")) or os.path.isdir(os.path.join(curr, "Runs")):
            return curr
        parent = os.path.dirname(curr)
        if parent == curr: return None
        curr = parent

def resolve_paths(input_arg):
    vm_file = None
    root_file = None
    fault_name = None

    if os.path.exists(input_arg) and os.path.isfile(input_arg) and input_arg.endswith("yaml"):
        vm_file = os.path.abspath(input_arg)
        project_root = find_project_root(os.path.dirname(vm_file))
        if project_root: root_file = os.path.join(project_root, "Runs", "root_params.yaml")
        return vm_file, root_file

    project_root = find_project_root(input_arg if os.path.exists(input_arg) else os.getcwd())
    
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

    return vm_file, root_file

def estimate_resources(vm_file, root_file, max_walltime_hours=DEFAULT_MAX_WALLTIME_HOURS):
    if not vm_file or not os.path.exists(vm_file):
        return None, f"vm_params.yaml not found."

    with open(vm_file, 'r') as f: vm_data = yaml.safe_load(f)

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

    # --- MEMORY CALCULATION ---
    surface_term = max(nx*ny, ny*nz, nx*nz)
    grid_bytes = 4 * (31 * nx * ny * nz + 56 * surface_term + 6 * (nx + nz))
    grid_gb = grid_bytes / (1024**3)

    V = grid_bytes * nt
    slope = 1.9e-9 
    max_sec = max_walltime_hours * 3600

    candidates = []

    # Iterate from 1 to Max Nodes to find best fit
    for n in range(1, MAX_NODES_CAP + 1):
        tasks_per_node = PHYSICAL_CORES_PER_NODE
        
        # Total Memory for this config = Grid + (Tasks * Overhead)
        total_mem_gb = grid_gb + (n * tasks_per_node * OVERHEAD_PER_TASK_GB)
        mem_per_node = total_mem_gb / n

        # Check Time Constraint
        total_cores = n * tasks_per_node
        pred_sec = (slope * V) / total_cores
        
        if pred_sec > max_sec and n < MAX_NODES_CAP:
            continue 

        # Classify Queue Suitability
        # STRICTER: Only accept if < 350GB per node to ensure Standard Queue acceptance
        if mem_per_node < SAFE_STANDARD_MEM_GB:
            queue_tier = "standard"
            score = n 
        elif mem_per_node < SAFE_HIGH_MEM_GB:
            queue_tier = "high_mem"
            score = 100 + n # Heavy penalty, try to split instead
        else:
            continue 

        candidates.append({
            "nodes": n,
            "tasks": tasks_per_node,
            "time": pred_sec,
            "mem_per_node": mem_per_node,
            "queue": queue_tier,
            "score": score
        })

    if not candidates:
        return None, "No valid configuration found (Job too large for available nodes)."

    candidates.sort(key=lambda x: x['score'])
    best_config = candidates[0]

    # --- FINAL REQUEST CALCULATION ---
    # Add 10% buffer
    req_mem_per_node = best_config['mem_per_node'] * 1.10
    
    if best_config['queue'] == 'standard':
        # Ensure we request at least what we calculated, but don't ask for the full node (700)
        # if we only need 350. Asking for 700 might trigger the hidden queue rejection.
        # We cap the request at 450GB for standard queue.
        req_mem_per_node = max(req_mem_per_node, 100.0) # Min 100GB
        req_mem_per_node = min(req_mem_per_node, 450.0)
    else:
        # High Mem: Cap at 730GB
        req_mem_per_node = min(req_mem_per_node, SAFE_HIGH_MEM_GB)
    
    total_req_gb = int(req_mem_per_node * best_config['nodes'])

    # Time Buffer
    req_walltime_sec = int(best_config['time'] * 1.2 + 900)
    h, r = divmod(req_walltime_sec, 3600)
    m, s = divmod(r, 60)
    wall_str = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"

    return {
        "nodes": best_config['nodes'],
        "tasks_per_node": best_config['tasks'],
        "mem_gb": total_req_gb,
        "walltime": wall_str,
        "est_hours": best_config['time'] / 3600.0,
        "mem_model_grid": grid_gb,
        "mem_model_overhead": (best_config['nodes'] * best_config['tasks'] * OVERHEAD_PER_TASK_GB),
        "queue_type": best_config['queue']
    }, None

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 estimate_emod3d.py <FaultName_or_Path> [max_walltime_hours]")
        sys.exit(1)

    input_arg = sys.argv[1]
    target_val = float(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_MAX_WALLTIME_HOURS

    vm_file, root_file = resolve_paths(input_arg)
    res, err = estimate_resources(vm_file, root_file, target_val)

    if err:
        print(f"Error: {err}")
        sys.exit(1)

    print(f"NODES={res['nodes']}")
    print(f"TASKS_PER_NODE={res['tasks_per_node']}")
    print(f"MEM_PER_NODE={res['mem_gb']}gb")
    print(f"WALLTIME={res['walltime']}")
    
    print(f"DEBUG: Grid: {res['mem_model_grid']:.1f}GB + Overhead: {res['mem_model_overhead']:.1f}GB", file=sys.stderr)
    print(f"DEBUG: Est. Per Node: {res['mem_gb']/res['nodes']:.1f} GB (Target: {res['queue_type']})", file=sys.stderr)

if __name__ == "__main__":
    main()
