#!/bin/bash
# PBS wrapper for HF on Cascade
# Usage:
#   submit_hf_pbs.sh <rel_dir> <ncpus> <mem_per_node> <walltime> [gmsim_env]

set -euo pipefail

DEFAULT_GMSIM_ENV="/uoc/project/uoc40001/Environments/mrd87_4"
DEFAULT_HF_BIN_DIR="/uoc/project/uoc40001/scratch/baes/tools"
DEFAULT_SCRIPTS_DIR="/uoc/project/uoc40001/scratch/baes/scripts"

if [[ "$#" -lt 4 || "$#" -gt 5 ]]; then
    echo "Usage: $0 <rel_dir> <ncpus> <mem_per_node> <walltime> [gmsim_env]"
    echo "Example: $0 /path/to/Runs/FaultX/FaultX_REL001 4 84GB 00:30:00"
    exit 1
fi

choose_queue() {
    local time="$1"
    IFS=: read -r hh mm ss <<<"$time"
    local tsec=$((10#$hh * 3600 + 10#$mm * 60 + 10#$ss))
    if (( tsec > 48 * 3600 )); then
        echo "longq"
    else
        echo "shortq"
    fi
}

REL_DIR_INPUT="$1"
NCPUS="$2"
MEM_PER_NODE="$3"
WALLTIME="$4"
GMSIM_ENV="${5:-${GMSIM_ENV:-$DEFAULT_GMSIM_ENV}}"
HF_BIN_DIR="${HF_BIN_DIR:-$DEFAULT_HF_BIN_DIR}"
SCRIPTS_DIR="${SCRIPTS_DIR:-$DEFAULT_SCRIPTS_DIR}"
PBS_SCRIPT="${SCRIPTS_DIR}/run_hf.pbs"

if [[ ! -d "$REL_DIR_INPUT" ]]; then
    echo "Error: rel_dir not found: $REL_DIR_INPUT"
    exit 1
fi
REL_DIR=$(realpath "$REL_DIR_INPUT")

if [[ ! "$NCPUS" =~ ^[0-9]+$ ]] || [[ "$NCPUS" -lt 1 ]]; then
    echo "Error: ncpus must be a positive integer"
    exit 1
fi

if [[ ! -f "$PBS_SCRIPT" ]]; then
    echo "Error: PBS script not found: $PBS_SCRIPT"
    exit 1
fi

cd "$REL_DIR"
JOBNAME=$(basename "$REL_DIR")
QUEUE=${QUEUE:-$(choose_queue "$WALLTIME")}

SELECT_STR="select=1:ncpus=${NCPUS}:mem=${MEM_PER_NODE}"
QSUB_CMD=(
    qsub
    -N "hf.${JOBNAME}"
    -q "$QUEUE"
    -l "$SELECT_STR"
    -l "walltime=${WALLTIME}"
    -v "GMSIM_ENV=${GMSIM_ENV},HF_BIN_DIR=${HF_BIN_DIR},SCRIPTS_DIR=${SCRIPTS_DIR},JOBNAME=${JOBNAME}"
    "$PBS_SCRIPT"
)

echo "Submitting HF job:"
echo "  REL_DIR: $REL_DIR"
echo "  Queue: $QUEUE"
echo "  Resources: $SELECT_STR, walltime=$WALLTIME"
echo "Executing: ${QSUB_CMD[*]}"
"${QSUB_CMD[@]}"
