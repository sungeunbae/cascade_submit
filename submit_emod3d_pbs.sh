#!/bin/bash
# PBS wrapper for EMOD3D on Cascade (ESNZ)
#
# Usage:
#   submit_emod3d_pbs.sh <job_directory> <nodes> <ntasks_per_node> <mem> <time>
#
# Features:
#   - Auto-calculates MAXMEM based on memory/task (like NeSI)
#   - Auto-selects queue based on memory request

set -euo pipefail

# --- Configuration ---
PBS_SCRIPT="/uoc/project/uoc40001/scratch/baes/scripts/run_emod3d.pbs"
SAFETY_FACTOR=0.85  # Use 85% of available per-core memory for MAXMEM

# --- Functions ---

# Convert memory string to MB
mem_to_mb() {
    local mem_str=$1
    # Remove 'b' or 'B' (e.g., 750gb -> 750g)
    mem_str=${mem_str%[bB]} 
    
    case ${mem_str: -1} in
        G|g) echo $((${mem_str%?} * 1024)) ;;
        M|m) echo ${mem_str%?} ;;
        T|t) echo $((${mem_str%?} * 1024 * 1024)) ;;
        *)   echo "Error: Invalid memory format '$mem_str'. Use G, M, or T suffix." >&2; exit 1 ;;
    esac
}

# Calculate MAXMEM (MB per core)
calculate_maxmem() {
    local mem_mb=$1
    local ntasks=$2
    local factor=$3
    
    # Calculate and return integer
    awk "BEGIN {printf \"%.0f\", ($mem_mb / $ntasks) * $factor}"
}

# Automatic queue selection based on Cascade Spec
choose_queue() {
  local mem_val time="$WALLTIME"
  # Convert requested mem to MB for comparison
  mem_val=$(mem_to_mb "$MEM_PER_NODE")
  
  # Parse time to seconds
  IFS=: read -r hh mm ss <<<"$time"; local tsec=$((10#$hh*3600 + 10#$mm*60 + 10#$ss))

  # --- LOGIC UPDATE ---
  # Standard nodes (esi_researchp) have ~755GB (approx 773,000 MB).
  # We only want the High Mem queue if we EXCEED the standard node size.
  # Threshold set to 770,000 MB (~751 GB).
  
  if (( mem_val > 770000 )); then
    # > 48 hours goes to longq
    (( tsec > 48*3600 )) && echo "high_mem_longq" || echo "high_mem_shortq"
  else
    (( tsec > 48*3600 )) && echo "longq" || echo "shortq"
  fi

}

# --- Main Script ---

if [ "$#" -ne 5 ]; then
  echo "Usage: $0 <job_directory> <nodes> <ntasks_per_node> <mem> <time>"
  echo "Example: $0 \$(pwd) 8 84 750gb 24:00:00"
  exit 1
fi

JOB_DIR=$1
NODES=$2
NTASKS_PER_NODE=$3
MEM_PER_NODE=$4      
WALLTIME=$5          

echo "→ Changing directory to: $JOB_DIR"
cd "$JOB_DIR" || { echo "Failed to change directory to $JOB_DIR"; exit 1; }

# --- Calculations ---
MEM_MB=$(mem_to_mb "$MEM_PER_NODE")
MAXMEM_MB=$(calculate_maxmem "$MEM_MB" "$NTASKS_PER_NODE" "$SAFETY_FACTOR")

# --- Environment Exports ---
export JOBNAME=$(basename "$PWD")
export MAXMEM="$MAXMEM_MB"
export EMOD3D_BIN="/uoc/project/uoc40001/EMOD3D/tools/emod3d-mpi_v3.0.8"
export CREATE_E3D_SH="/uoc/project/uoc40001/scratch/baes/scripts/create_e3d.sh"
export FIX_OLD_PATH_SH="/uoc/project/uoc40001/scratch/baes/scripts/fix_nesi_path.sh"

# Determine Queue
QUEUE=${QUEUE:-$(choose_queue)}

# --- Reporting ---
echo "═══════════════════════════════════════════════════════════"
echo "Job Configuration (Cascade):"
echo "  Job Name:        $JOBNAME"
echo "  Nodes:           $NODES"
echo "  Tasks per Node:  $NTASKS_PER_NODE"
echo "  Total Cores:     $((NODES * NTASKS_PER_NODE))"
echo "  Memory per Node: $MEM_PER_NODE ($MEM_MB MB)"
echo "  Queue:           $QUEUE"
echo "  MAXMEM (Calc):   $MAXMEM_MB MB/core (Safety: $SAFETY_FACTOR)"
echo "═══════════════════════════════════════════════════════════"

# --- Submit ---
# Note: ompthreads=1 is standard for pure MPI EMOD3D runs
SELECT_STR="select=${NODES}:ncpus=${NTASKS_PER_NODE}:mpiprocs=${NTASKS_PER_NODE}:ompthreads=1:mem=${MEM_PER_NODE}"

QSUB_CMD=( qsub
  -q "$QUEUE"
  -N "lf.${JOBNAME}"
  -l "$SELECT_STR"
  -l "walltime=${WALLTIME}"
  -v MAXMEM="$MAXMEM",JOBNAME="$JOBNAME",EMOD3D_BIN="$EMOD3D_BIN",CREATE_E3D_SH="$CREATE_E3D_SH",FIX_OLD_PATH_SH="$FIX_OLD_PATH_SH"
  "$PBS_SCRIPT"
)

echo "Executing: ${QSUB_CMD[*]}"
"${QSUB_CMD[@]}"
