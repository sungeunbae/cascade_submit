#!/bin/bash
# PBS wrapper for EMOD3D on Cascade (ESNZ)
# Usage:
#   submit_emod3d_pbs.sh <job_dir> <nodes> <ntasks> <mem> <time> <defaults_yaml>

set -euo pipefail

# --- Configuration ---
PBS_SCRIPT="/uoc/project/uoc40001/scratch/baes/scripts/run_emod3d.pbs"
SAFETY_FACTOR=0.85

# --- Functions (Mem Calc & Queue Selection) ---
mem_to_mb() {
    local mem_str=$1
    mem_str=${mem_str%[bB]} 
    case ${mem_str: -1} in
        G|g) echo $((${mem_str%?} * 1024)) ;;
        M|m) echo ${mem_str%?} ;;
        T|t) echo $((${mem_str%?} * 1024 * 1024)) ;;
        *)   echo "Error: Invalid memory format '$mem_str'" >&2; exit 1 ;;
    esac
}

calculate_maxmem() {
    awk "BEGIN {printf \"%.0f\", ($1 / $2) * $3}"
}

choose_queue() {
  local mem_val time="$WALLTIME" q_name
  mem_val=$(mem_to_mb "$MEM_PER_NODE")
  IFS=: read -r hh mm ss <<<"$time"; local tsec=$((10#$hh*3600 + 10#$mm*60 + 10#$ss))
  
  if (( mem_val > 770000 )); then
    if (( tsec > 48*3600 )); then q_name="high_mem_longq"; else q_name="high_mem_shortq"; fi
  else
    if (( tsec > 48*3600 )); then q_name="longq"; else q_name="shortq"; fi
  fi
  echo "$q_name"
}

# --- Main Script ---

if [ "$#" -ne 6 ]; then
  echo "Usage: $0 <job_directory> <nodes> <ntasks_per_node> <mem> <time> <defaults_yaml>"
  echo "Example: $0 \$(pwd) 6 84 735gb 24:00:00 /path/to/emod3d_defaults.yaml"
  exit 1
fi

JOB_DIR=$1
NODES=$2
NTASKS_PER_NODE=$3
MEM_PER_NODE=$4      
WALLTIME=$5
DEFAULTS_ARG=$6

# Validate and absolute-path the defaults file
if [[ ! -f "$DEFAULTS_ARG" ]]; then
    echo "Error: Defaults file '$DEFAULTS_ARG' not found."
    exit 1
fi
export EMOD3D_DEFAULTS=$(realpath "$DEFAULTS_ARG")

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

QUEUE=${QUEUE:-$(choose_queue)}
QUEUE=$(echo "$QUEUE" | tr -d '\n')

# --- Reporting ---
echo "═══════════════════════════════════════════════════════════"
echo "Job Configuration (Cascade):"
echo "  Job Name:        $JOBNAME"
echo "  Nodes:           $NODES"
echo "  Tasks per Node:  $NTASKS_PER_NODE"
echo "  Total Cores:     $((NODES * NTASKS_PER_NODE))"
echo "  Queue:           $QUEUE"
echo "  MAXMEM:          $MAXMEM_MB MB/core"
echo "  Defaults File:   $EMOD3D_DEFAULTS"
echo "═══════════════════════════════════════════════════════════"

# --- Submit ---
SELECT_STR="select=${NODES}:ncpus=${NTASKS_PER_NODE}:mpiprocs=${NTASKS_PER_NODE}:ompthreads=1:mem=${MEM_PER_NODE}"

QSUB_CMD=( qsub
  -q "$QUEUE"
  -N "lf.${JOBNAME}"
  -l "$SELECT_STR"
  -l "walltime=${WALLTIME}"
  # Pass EMOD3D_DEFAULTS to the job environment
  -v MAXMEM="$MAXMEM",JOBNAME="$JOBNAME",EMOD3D_BIN="$EMOD3D_BIN",CREATE_E3D_SH="$CREATE_E3D_SH",FIX_OLD_PATH_SH="$FIX_OLD_PATH_SH",EMOD3D_DEFAULTS="$EMOD3D_DEFAULTS"
  "$PBS_SCRIPT"
)

echo "Executing: ${QSUB_CMD[*]}"
"${QSUB_CMD[@]}"

