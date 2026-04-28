#!/bin/bash
#
# submit_srf2stoch.sh
# Submit a PBS job to run srf2stoch on a (possibly very large multi-plane) SRF file.
#
# Usage:
#   submit_srf2stoch.sh <input.srf> [output_directory]
#
#   - <input.srf>          : Path to the .srf file (required)
#   - [output_directory]   : Where to write the .stoch file (optional)
#                            Defaults to the same directory as the .srf file
#

set -euo pipefail

# ========================================================
# ARGUMENT PARSING
# ========================================================
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <path/to/file.srf> [output_directory]"
    echo ""
    echo "Examples:"
    echo "  $0 FiordSZ09_REL27.srf"
    echo "  $0 /path/to/FiordSZ09_REL27.srf /path/to/output"
    exit 1
fi

SRF_FILE="$1"
OUT_DIR="${2:-$(dirname "$SRF_FILE")}"

# Make paths absolute
SRF_FILE=$(realpath "$SRF_FILE")
OUT_DIR=$(realpath "$OUT_DIR")

STOCH_FILE="${OUT_DIR}/$(basename "${SRF_FILE%.srf}.stoch")"

echo "═══════════════════════════════════════════════════════════"
echo "Preparing srf2stoch PBS job"
echo "  SRF File:     ${SRF_FILE}"
echo "  Output Dir:   ${OUT_DIR}"
echo "  Stoch File:   ${STOCH_FILE}"
echo "═══════════════════════════════════════════════════════════"

# ========================================================
# SUBMIT THE JOB
# ========================================================
PBS_SCRIPT="/uoc/project/uoc40001/scratch/baes/cascade_submit/srf2stoch_job.pbs"

qsub -v "SRF_FILE=${SRF_FILE},OUT_DIR=${OUT_DIR},STOCH_FILE=${STOCH_FILE}" \
     "${PBS_SCRIPT}"

echo ""
echo "Job submitted successfully."
echo "Check status with: qstat -u $USER"
echo "Log will be written to: ${OUT_DIR}/srf2stoch.<jobid>.log"

