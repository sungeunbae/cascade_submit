#!/usr/bin/env python3
"""
Resource Estimator for EMOD3D (Cascade/ESNZ)
Logic:
  - Memory: Always ensures enough nodes to hold data.
  - Compute: Smart Scaling.
     - If job fits in 1 Node and runs < 5 hours, stick to 1 Node (Efficiency).
     - Otherwise, add nodes to meet the 'target_hours' (Speed).
  - Density: Throttles tasks only if node is full (>85% utilized).
"""
import sys
import os
import yaml
import math

# --- HARDWARE & POLICY ---
PHYSICAL_CORES_PER_NODE = 192
RAM_PER_NODE_GB = 755.0

REQUEST_RAM_GB = 735.0 
BASH_SCRIPT_OVERHEAD = 0.85
MAX_NODES_CAP = 10

# Efficiency Threshold: 
# If a job fits in 1 node and takes less than this many hours, 
# don't add more nodes just to make it faster.
SMALL_JOB_THRESHOLD_HOURS = 5.0

# Density Safety
HIGH_DENSITY_THRESHOLD_GB = RAM_PER_NODE_GB * 0.85 
MIN_GB_PER_CORE_IF_FULL = 2.0 

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
    
    if os.path.exists(input_arg): start_search = input_arg
    else: start_search = os.getcwd()

    if os.path.isfile(input_arg) and input_arg.endswith("yaml"):
        vm_file = os.path.abspath(input_arg)
        project_root = find_project_root(os.path.dirname(vm_file))
        if project_root: root_file = os.path.join(project_root, "Runs", "root_params.yaml")
        return vm_file, root_file

    project_root = find_project_root(start_search)
    
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

def estimate_resources(vm_file, root_file, target_hours=1.0):
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
        if 'nt' in vm_data: nt = int(vm_data['nt'])
        elif 'sim_duration' in vm_data: nt = int(float(vm_data['sim_duration']) / dt)
        else: return None, "Missing nt or sim_duration"
    except KeyError as e: return None, f"Missing param: {e}"

    # --- CONSTANTS ---
    surface_term = max(nx*ny, ny*nz, nx*nz)
    M_bytes = 4 * (31 * nx * ny * nz + 56 * surface_term + 6 * (nx + nz))
    M_gb_needed = M_bytes / (1024**3)
    V = M_bytes * nt
    slope = 1.9e-9
    target_sec = target_hours * 3600

    # --- 1. MINIMUM NODES (Capacity) ---
    usable_ram_per_node = REQUEST_RAM_GB * BASH_SCRIPT_OVERHEAD
    min_nodes_mem = math.ceil(M_gb_needed / usable_ram_per_node)
    
    if min_nodes_mem > MAX_NODES_CAP:
        return None, f"Job needs {min_nodes_mem} nodes for Memory, exceeding cap."

    # --- 2. ITERATIVE SOLVER ---
    best_config = None
    
    for n in range(min_nodes_mem, MAX_NODES_CAP + 1):
        mem_per_node = M_gb_needed / n
        
        # Density Check
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

        # SMART SCALING LOGIC:
        # If we are at the minimum nodes for memory (e.g. 1 node),
        # AND the time is reasonable (< 5h), stop here. Don't add nodes just for speed.
        if n == min_nodes_mem and pred_sec < (SMALL_JOB_THRESHOLD_HOURS * 3600):
            best_config = current_config
            break

        # Standard Logic: Stop if we hit user's target time
        if pred_sec <= target_sec:
            best_config = current_config
            break
            
        # Or if adding nodes helps speed
        if total_cores > best_config['cores']:
             best_config = current_config
        
    # --- 3. OUTPUT ---
    req_mem_gb = int(REQUEST_RAM_GB)
    
    req_walltime_sec = int(best_config['time'] * 1.2 + 300)
    h, r = divmod(req_walltime_sec, 3600)
    m, s = divmod(r, 60)
    wall_str = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"

    return {
        "nodes": best_config['nodes'],
        "tasks_per_node": best_config['tasks'],
        "mem_gb": req_mem_gb,
        "walltime": wall_str
    }, None

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 estimate_emod3d.py <FaultName_or_Path> [target_hours]")
        sys.exit(1)

    input_arg = sys.argv[1]
    target_hours = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

    vm_file, root_file = resolve_paths(input_arg)
    res, err = estimate_resources(vm_file, root_file, target_hours)
    
    if err:
        print(f"Error: {err}")
        sys.exit(1)

    print(f"NODES={res['nodes']}")
    print(f"TASKS_PER_NODE={res['tasks_per_node']}")
    print(f"MEM_PER_NODE={res['mem_gb']}gb")
    print(f"WALLTIME={res['walltime']}")

if __name__ == "__main__":
    main()
