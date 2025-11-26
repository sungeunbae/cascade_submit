#!/usr/bin/env bash
# create_e3d.sh
# Usage: create_e3d.sh <sim_dir> <output_e3d.par> <defaults_yaml>

set -euo pipefail

[[ $# -eq 3 ]] || { echo "Usage: $0 <sim_dir> <output_e3d.par> <defaults_yaml>"; exit 1; }
SIM_DIR=$(realpath "$1")
OUT_PAR="$2"
DEFAULTS_YAML="$3"

[[ -f "$DEFAULTS_YAML" ]] || { echo "Error: Defaults file $DEFAULTS_YAML not found"; exit 1; }

mkdir -p "$(dirname "$OUT_PAR")"

# --- Helper: YAML Parser ---
yaml_get(){
  grep -E "^[[:space:]]*${1##*.}:" "$2" | head -1 | sed 's/^[^:]*:[[:space:]]*//' | tr -d '"' | tr -d "'"
}
die(){ echo "Error: $*" >&2; exit 1; }

# --- 1. Locate Params Files ---
SIM_YAML="$SIM_DIR/sim_params.yaml"
[[ -f "$SIM_YAML" ]] || die "Missing $SIM_YAML"

FAULT_YAML=$(yaml_get 'fault_yaml_path' "$SIM_YAML")
[[ -z "$FAULT_YAML" ]] && FAULT_YAML="$(dirname "$SIM_DIR")/fault_params.yaml"

ROOT_YAML=$(yaml_get 'root_yaml_path' "$FAULT_YAML")
if [[ -z "$ROOT_YAML" ]]; then
   CUR="$SIM_DIR"
   while [[ "$CUR" != "/" ]]; do
     if [[ -f "$CUR/../root_params.yaml" ]]; then ROOT_YAML="$CUR/../root_params.yaml"; break; fi
     CUR="$(dirname "$CUR")"
   done
fi

VM_YAML=$(yaml_get 'vm_params' "$SIM_YAML")
if [[ -z "$VM_YAML" ]]; then 
    VDIR=$(yaml_get 'vel_mod_dir' "$FAULT_YAML")
    VM_YAML="$VDIR/vm_params.yaml"
fi

# --- 2. Extract Variables ---
NX=$(yaml_get 'nx' "$VM_YAML")
NY=$(yaml_get 'ny' "$VM_YAML")
NZ=$(yaml_get 'nz' "$VM_YAML")
H=$(yaml_get 'hh' "$VM_YAML")
SIM_DUR=$(yaml_get 'sim_duration' "$VM_YAML")
MODEL_LON=$(yaml_get 'MODEL_LON' "$VM_YAML")
MODEL_LAT=$(yaml_get 'MODEL_LAT' "$VM_YAML")
MODEL_ROT=$(yaml_get 'MODEL_ROT' "$VM_YAML")
GRIDFILE=$(yaml_get 'GRIDFILE' "$VM_YAML")
MODEL_PARAMS=$(yaml_get 'MODEL_PARAMS' "$VM_YAML")

DT=$(yaml_get 'dt' "$ROOT_YAML"); DT=${DT:-0.005}
FLO=$(yaml_get 'flo' "$ROOT_YAML"); FLO=${FLO:-1.0}
STAT_FILE=$(yaml_get 'stat_file' "$ROOT_YAML")
EMOD3D_VER=$(yaml_get 'emod3d_version' "$ROOT_YAML"); EMOD3D_VER=${EMOD3D_VER:-"3.0.8"}

RUN_NAME=$(yaml_get 'run_name' "$SIM_YAML")
SRF_FILE=$(yaml_get 'srf_file' "$SIM_YAML")
STAT_COORDS=$(yaml_get 'stat_coords' "$FAULT_YAML")
VMOD_DIR=$(yaml_get 'vel_mod_dir' "$FAULT_YAML")

# --- 3. Calculations (Python Logic Replication) ---
# Time shift extension logic
read NT TS_TOTAL <<< $(awk -v dur="$SIM_DUR" -v flo="$FLO" -v dt="$DT" '
BEGIN {
    ext = 3.0 / flo
    total_dur = dur + ext
    nt = int((total_dur / dt) + 0.5)
    ts_total = int(total_dur / (dt * 20)) # dtts=20
    print nt, ts_total
}')
DUMP_ITINC=$NT

# --- 4. Prepare Paths ---
LF_DIR="$SIM_DIR/LF"
SEIS_DIR="$LF_DIR/SeismoBin"
RESTART_DIR="$LF_DIR/Restart"
LOG_DIR="$LF_DIR/Rlog"
TS_OUT_DIR="$LF_DIR/TSlice/TSFiles"
MAIN_DUMP_DIR="$LF_DIR/OutBin"
SLIPOUT="$LF_DIR/SlipOut/slipout-k2"
TS_FILE="$MAIN_DUMP_DIR/${RUN_NAME}_xyts.e3d"
EMOD3D_BIN="/uoc/project/uoc40001/EMOD3D/tools/emod3d-mpi_v${EMOD3D_VER}"

mkdir -p "$SEIS_DIR" "$RESTART_DIR" "$LOG_DIR" "$TS_OUT_DIR" "$MAIN_DUMP_DIR" "$(dirname "$SLIPOUT")"

# --- 5. Generate Output ---

# A. Parse Defaults YAML and write to output
echo "# --- Defaults from emod3d_defaults.yaml ---" > "$OUT_PAR"
awk -F: '/^[a-zA-Z0-9_]+:/ {
    key=$1; $1=""; val=$0;
    gsub(/^[ \t]+|[ \t]+$/, "", key);
    gsub(/^[ \t]+|[ \t]+$/, "", val);
    # Add quotes if value contains chars and isn not strictly numeric
    if (val !~ /^[0-9.-]+$/) val = "\"" val "\""
    printf "%s=%s\n", key, val
}' "$DEFAULTS_YAML" >> "$OUT_PAR"

# B. Append Run-Specific Overrides
cat >> "$OUT_PAR" <<EOF

# --- Run-Specific Overrides ---
version="${EMOD3D_VER}-mpi"
name="${RUN_NAME}"
n_proc=512

nx=$NX
ny=$NY
nz=$NZ
h=$H
dt=$DT
nt=$NT
flo=$FLO

dump_itinc=$DUMP_ITINC
ts_total=$TS_TOTAL

faultfile="${SRF_FILE}"
vmoddir="${VMOD_DIR}"

modellon=$MODEL_LON
modellat=$MODEL_LAT
modelrot=$MODEL_ROT

main_dump_dir="${MAIN_DUMP_DIR}"
seiscords="${STAT_COORDS}"
user_scratch="${SIM_DIR}/.."
seisdir="${SEIS_DIR}"
ts_file="${TS_FILE}"
ts_out_dir="${TS_OUT_DIR}"
restartdir="${RESTART_DIR}"
restartname="${RUN_NAME}"
logdir="${LOG_DIR}"
slipout="${SLIPOUT}"

wcc_prog_dir="${EMOD3D_BIN}"
vel_mod_params_dir="${VMOD_DIR}"
sim_dir="${SIM_DIR}"
stat_file="${STAT_FILE}"
grid_file="${GRIDFILE}"
model_params="${MODEL_PARAMS}"
EOF

echo "✅ Generated $OUT_PAR using defaults from $DEFAULTS_YAML"
