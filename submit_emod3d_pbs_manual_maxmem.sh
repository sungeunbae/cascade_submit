#!/bin/bash
# PBS wrapper for EMOD3D on Cascade (ESNZ)
# Usage:
#   submit_emod3d_pbs.sh <job_directory> <maxmem_MB> <nodes> <ntasks_per_node> <mem> <time>
# Example:
#   ./submit_emod3d_pbs.sh /uoc/project/uoc40001/scratch/baes/runs/run01 2500 2 64 500gb 10:00:00

set -euo pipefail

# Path to the PBS batch script
PBS_SCRIPT=/uoc/project/uoc40001/scratch/baes/scripts/run_emod3d.pbs

if [ "$#" -ne 6 ]; then
  echo "Usage: $0 <job_directory> <maxmem_MB> <nodes> <ntasks_per_node> <mem> <time>"
  exit 1
fi

JOB_DIR=$1
MAXMEM_MB=$2
NODES=$3
NTASKS_PER_NODE=$4
MEM_PER_NODE=$5      # e.g. 420gb, 500gb, 755gb
WALLTIME=$6          # e.g. 10:00:00

echo "Changing directory to: $JOB_DIR"
cd "$JOB_DIR" || { echo "Failed to change directory to $JOB_DIR"; exit 1; }

export JOBNAME=$(basename "$PWD")
export MAXMEM="$MAXMEM_MB"
export EMOD3D_BIN="/uoc/project/uoc40001/EMOD3D/tools/emod3d-mpi_v3.0.13"
export CREATE_E3D_SH="/uoc/project/uoc40001/scratch/baes/scripts/create_e3d.sh"
export FIX_OLD_PATH_SH="/uoc/project/uoc40001/scratch/baes/scripts/fix_old_nesi_path.sh"

# --- Automatic queue selection ---
choose_queue() {
  local mem_lc time="$WALLTIME"
  mem_lc=$(echo "$MEM_PER_NODE" | tr '[:upper:]' '[:lower:]')
  IFS=: read -r hh mm ss <<<"$time"; local tsec=$((10#$hh*3600 + 10#$mm*60 + 10#$ss))

  if [[ "$mem_lc" =~ ^([5-9][0-9]{2,}|[1-9][0-9]{3,})gb$ ]]; then
    (( tsec >= 48*3600 )) && echo "high_mem_longq" || echo "high_mem_shortq"
  else
    (( tsec >= 48*3600 )) && echo "longq" || echo "shortq"
  fi
}

QUEUE=${QUEUE:-$(choose_queue)}
echo "Using queue: $QUEUE"

# --- Construct qsub command ---
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

