#!/bin/bash
# submit_nzcvm.sh  –  Submit a velocity model generation job on Cascade (PBS)
#
# Always generates in HDF5 format (parallel), then converts to EMOD3D binaries.
#
# Usage:
#   submit_nzcvm.sh <nzcvm_cfg> [nodes] [ncpus_per_node] [mem_per_node] [walltime] [out_dir] [np_workers]
#
# Arguments:
#   nzcvm_cfg        Path to the nzcvm.cfg file (required)
#   nodes            Number of nodes            (default: 1)
#   ncpus_per_node   CPUs per node              (default: 192, full node)
#   mem_per_node     Memory per node, e.g. 120G (default: 120G)
#   walltime         HH:MM:SS                   (default: 04:00:00)
#   out_dir          Override OUTPUT_DIR in cfg  (default: from cfg)
#   np_workers       Python parallel workers     (default: 32)
#
# NOTE: np_workers is intentionally decoupled from ncpus_per_node.
#       Requesting a full node (192 CPUs) gives memory headroom,
#       but forking 192 Python workers causes OOM due to CoW overhead
#       from large shared data (basin membership, tomography, mesh).
#       32 workers is a safe starting point; increase with caution.
#
# Examples:
#   submit_nzcvm.sh /path/to/nzvm.cfg
#   submit_nzcvm.sh /path/to/nzvm.cfg 1 192 120G 04:00:00 "" 32
#   submit_nzcvm.sh /path/to/nzvm.cfg 1 192 200G 12:00:00 "" 64

set -euo pipefail

# ---- Paths ----------------------------------------------------------------
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PBS_SCRIPT="${SCRIPTS_DIR}/run_nzcvm.pbs"

# ---- Parse Arguments ------------------------------------------------------
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <nzcvm_cfg> [nodes] [ncpus_per_node] [mem_per_node] [walltime] [out_dir] [np_workers]"
    exit 1
fi

NZCVM_CFG=$(realpath "$1")
NODES="${2:-1}"
NCPUS_PER_NODE="${3:-192}"
MEM_PER_NODE="${4:-120G}"
WALLTIME="${5:-04:00:00}"
OUT_DIR="${6:-}"
NP_WORKERS="${7:-32}"

if [[ ! -f "${NZCVM_CFG}" ]]; then
    echo "Error: Config file not found: ${NZCVM_CFG}"
    exit 1
fi

if [[ ! -f "${PBS_SCRIPT}" ]]; then
    echo "Error: PBS script not found: ${PBS_SCRIPT}"
    exit 1
fi

# ---- Derived Values -------------------------------------------------------
JOBNAME="nzcvm.$(basename "$(dirname "${NZCVM_CFG}")")"

# ---- Queue Selection ------------------------------------------------------
mem_to_mb() {
    local s="${1%[bB]}"
    case "${s: -1}" in
        G|g) echo $(( ${s%?} * 1024 )) ;;
        M|m) echo "${s%?}" ;;
        T|t) echo $(( ${s%?} * 1024 * 1024 )) ;;
        *)   echo "${s}" ;;
    esac
}

choose_queue() {
    local mem_mb wall_sec hh mm ss
    mem_mb=$(mem_to_mb "${MEM_PER_NODE}")
    IFS=: read -r hh mm ss <<<"${WALLTIME}"
    wall_sec=$(( 10#${hh}*3600 + 10#${mm}*60 + 10#${ss} ))
    if (( mem_mb > 700000 )); then
        (( wall_sec > 48*3600 )) && echo "high_mem_longq" || echo "high_mem_shortq"
    else
        (( wall_sec > 48*3600 )) && echo "longq" || echo "shortq"
    fi
}

QUEUE=$(choose_queue)
SELECT_STR="select=${NODES}:ncpus=${NCPUS_PER_NODE}:mpiprocs=1:ompthreads=${NCPUS_PER_NODE}:mem=${MEM_PER_NODE}"

# ---- Report ---------------------------------------------------------------
echo "═══════════════════════════════════════════════════════════"
echo "Submitting NZCVM Velocity Model Job"
echo "  Config:          ${NZCVM_CFG}"
echo "  Job Name:        ${JOBNAME}"
echo "  Nodes:           ${NODES}"
echo "  CPUs/Node:       ${NCPUS_PER_NODE}"
echo "  np_workers:      ${NP_WORKERS}  (Python workers, not == CPUs)"
echo "  Mem/Node:        ${MEM_PER_NODE}"
echo "  Walltime:        ${WALLTIME}"
echo "  Queue:           ${QUEUE}"
echo "  Output Format:   HDF5 → EMOD3D"
if [[ -n "${OUT_DIR}" ]]; then
echo "  Out Dir:         ${OUT_DIR}"
fi
echo "═══════════════════════════════════════════════════════════"

# ---- Build -v string ------------------------------------------------------
V_VARS="NZCVM_CFG=${NZCVM_CFG},JOBNAME=${JOBNAME},NP_WORKERS=${NP_WORKERS}"
if [[ -n "${OUT_DIR}" ]]; then
    V_VARS+=",OUT_DIR=${OUT_DIR}"
fi

# ---- Submit ---------------------------------------------------------------
QSUB_CMD=(
    qsub
    -N  "${JOBNAME}"
    -q  "${QUEUE}"
    -l  "${SELECT_STR}"
    -l  "walltime=${WALLTIME}"
    -j  oe
    -o  "${NZCVM_CFG%/*}/${JOBNAME}.pbs.log"
    -v  "${V_VARS}"
    "${PBS_SCRIPT}"
)

echo "Executing: ${QSUB_CMD[*]}"
"${QSUB_CMD[@]}"

