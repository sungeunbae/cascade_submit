#!/usr/bin/env python3
"""
Resource Estimator for EMOD3D (Cascade/ESNZ)
Usage: 
  python3 estimate_emod3d.py <FaultName_or_Path> [target_hours]
"""
import sys
import os
import yaml
import math

# --- Cascade Hardware Constants (esi_researchp) ---
CORES_PER_NODE = 384
RAM_PER_NODE_GB = 755.0  
SAFE_RAM_GB = RAM_PER_NODE_GB * 0.90 

# --- Policies ---
MAX_NODES_CAP = 10  # User hard limit

# --- Path Resolution ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RUN_ROOT = SCRIPT_DIR

def resolve_paths(input_arg):
    vm_file = None
    root_file = None
    
    # 1. Check if input is a simple Fault Name (e.g. AlpineF2K)
    candidate_vm = os.path.join(RUN_ROOT, "Data", "VMs", input_arg, "vm_params.yaml")
    
    if os.path.exists(candidate_vm):
        vm_file = candidate_vm
        candidate_root = os.path.join(RUN_ROOT, "Runs", "root_params.yaml")
        if os.path.exists(candidate_root):
            root_file = candidate_root
            
    # 2. If not found, assume input is a direct path
    elif os.path.exists(input_arg):
        if os.path.isdir(input_arg):
             if os.path.exists(os.path.join(input_arg, "vm_params.yaml")):
                 vm_file = os.path.join(input_arg, "vm_params.yaml")
        elif os.path.isfile(input_arg):
             vm_file = input_arg
             
        # Fallback: Try to extract fault name from path to find Data/VMs
        if not vm_file:
            parts = os.path.abspath(input_arg).split(os.sep)
            if "Runs" in parts:
                idx = parts.index("Runs")
                if idx + 1 < len(parts):
                    fault = parts[idx+1]
                    vm_file = os.path.join(RUN_ROOT, "Data", "VMs", fault, "vm_params.yaml")

        # Hunt for root_params upwards
        curr = os.path.abspath(input_arg)
        for _ in range(4):
            check = os.path.join(curr, "root_params.yaml")
            if os.path.exists(check):
                root_file = check
                break
            curr = os.path.dirname(curr)
            if curr == "/": break
            
    return vm_file, root_file

def estimate_resources(vm_file, root_file, target_hours=1.0):
    if not vm_file or not os.path.exists(vm_file):
        return None, f"vm_params.yaml not found. (Checked: {vm_file})"

    # --- Load Data ---
    with open(vm_file, 'r') as f:
        vm_data = yaml.safe_load(f)
    
    dt = 0.005 # Default
    if root_file and os.path.exists(root_file):
        with open(root_file, 'r') as f:
            root_data = yaml.safe_load(f)
            if 'dt' in root_data:
                dt = float(root_data['dt'])
    elif 'dt' in vm_data:
        dt = float(vm_data['dt'])

    # --- Extract Dimensions ---
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

    # --- Calculation ---
    surface_term = max(nx*ny, ny*nz, nx*nz)
    M_bytes = 4 * (31 * nx * ny * nz + 56 * surface_term + 6 * (nx + nz))
    M_gb = M_bytes / (1024**3)
    V = M_bytes * nt

    target_sec = target_hours * 3600
    slope = 1.9e-9
    
    # 1. Constraints
    min_nodes_mem = math.ceil(M_gb / SAFE_RAM_GB)
    req_cores = (slope * V) / target_sec
    min_nodes_compute = math.ceil(req_cores / CORES_PER_NODE)

    # 2. Determine Nodes
    nodes = max(min_nodes_mem, min_nodes_compute)

    # 3. Apply Policy Cap
    is_capped = False
    if nodes > MAX_NODES_CAP:
        # Check if we are physically memory bound beyond the cap
        if min_nodes_mem > MAX_NODES_CAP:
            return None, f"Job requires {min_nodes_mem} nodes for memory ({M_gb:.1f}GB), exceeding cap of {MAX_NODES_CAP}."
        
        nodes = MAX_NODES_CAP
        is_capped = True

    total_cores = nodes * CORES_PER_NODE
    
    # 4. Re-Calculate Prediction based on FINAL node count
    # If we capped the nodes, total_cores is lower, so walltime goes UP.
    pred_walltime_sec = (slope * V) / total_cores
    
    # Formatting
    req_mem_mb = int((M_gb / nodes) * 1.05 * 1024)
    if req_mem_mb > (RAM_PER_NODE_GB * 1024): 
        req_mem_mb = int(RAM_PER_NODE_GB * 1024)

    req_walltime_sec = int(pred_walltime_sec * 1.2 + 300)
    h, r = divmod(req_walltime_sec, 3600)
    m, s = divmod(r, 60)
    wall_str = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"

    return {
        "nodes": nodes,
        "mem_mb": req_mem_mb,
        "walltime": wall_str,
        "capped": is_capped
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

    # Output specifically formatted for the shell script to eval
    mem_mb = res['mem_mb']
    mem_gb = int(mem_mb/1024. + 0.5)
    print(f"NODES={res['nodes']}")
    print(f"NTASKS={CORES_PER_NODE}")
    print(f"MEM={mem_mb}M = {mem_gb}G")
    print(f"WALLTIME={res['walltime']}")

if __name__ == "__main__":
    main()
