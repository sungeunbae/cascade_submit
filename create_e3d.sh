#!/usr/bin/env bash
# Auto-generate e3d.par from YAMLs.
# Usage: create_e3d_from_yaml.sh <sim_dir> <output_e3d.par>
set -euo pipefail

[[ $# -eq 2 ]] || { echo "Usage: $0 <sim_dir> <output_e3d.par>"; exit 1; }
SIM_DIR=$(realpath "$1")
OUT_PAR="$2"  # can be relative; we will mkdir for it
OUT_PAR_DIR=$(dirname "$OUT_PAR")
mkdir -p "$OUT_PAR_DIR"

# ---------- helpers ----------
have(){ command -v "$1" >/dev/null 2>&1; }
yaml_get_simple(){  # yaml_get_simple KEY FILE  (flat: key: value)
  awk -v k="$1" '
    $0 ~ "^[[:space:]]*#"{next}
    match($0, /^[[:space:]]*([A-Za-z0-9_\.]+)[[:space:]]*:[[:space:]]*(.*)$/, m){
      key=m[1]; val=m[2];
      gsub(/[[:space:]]+$/,"",val)
      if (key==k){ print val; exit }
    }' "$2"
}

yaml_get(){
  local key="$1" file="$2"
  local out=""
  if have yq; then
    # try both new and old yq syntaxes
    out=$(yq --no-jq-eval -r "$key" "$file" 2>/dev/null || yq r "$file" "$key" 2>/dev/null || true)
  fi
  # fallback to simple grep if yq produced nothing or null
  if [[ -z "$out" || "$out" == "null" ]]; then
    out=$(grep -E "^[[:space:]]*${key##*.}:" "$file" | head -1 | sed 's/^[^:]*:[[:space:]]*//')
  fi
  echo "$out"
}

die(){ echo "Error: $*" >&2; exit 1; }

# ---------- locate YAMLs ----------
SIM_YAML="$SIM_DIR/sim_params.yaml"
[[ -f "$SIM_YAML" ]] || die "Missing $SIM_YAML"

# fault_params.yaml path from sim_params.yaml if present; else guess in parent
FAULT_YAML=$(yaml_get '.fault_yaml_path' "$SIM_YAML" 2>/dev/null || true)
if [[ -z "${FAULT_YAML:-}" || "$FAULT_YAML" == "null" ]]; then
  # typical layout: SIM_DIR/../fault_params.yaml
  CANDIDATE="$(dirname "$SIM_DIR")/fault_params.yaml"
  [[ -f "$CANDIDATE" ]] || die "Missing fault_params.yaml (looked at $CANDIDATE); add fault_yaml_path in sim_params.yaml or place the file in the parent dir."
  FAULT_YAML="$CANDIDATE"
fi

# root_params.yaml: prefer fault_yaml_path->root_yaml_path, else walk up for Runs/root_params.yaml
ROOT_YAML=$(yaml_get '.root_yaml_path' "$FAULT_YAML" 2>/dev/null || true)
if [[ -z "${ROOT_YAML:-}" || "$ROOT_YAML" == "null" ]]; then
  # ascend until filesystem root; pick first Runs/root_params.yaml
  CUR="$SIM_DIR"
  FOUND=""
  while :; do
    CAND="$CUR/../root_params.yaml"; [[ -f "$CAND" ]] && { FOUND="$CAND"; break; }
    CAND="$CUR/../../root_params.yaml"; [[ -f "$CAND" ]] && { FOUND="$CAND"; break; }
    CAND="$CUR/../Runs/root_params.yaml"; [[ -f "$CAND" ]] && { FOUND="$CAND"; break; }
    CAND="$CUR/../../Runs/root_params.yaml"; [[ -f "$CAND" ]] && { FOUND="$CAND"; break; }
    [[ "$CUR" == "/" ]] && break
    CUR="$(dirname "$CUR")"
  done
  [[ -n "$FOUND" ]] || die "Missing root_params.yaml (tried common ancestors incl. Runs/root_params.yaml)."
  ROOT_YAML="$FOUND"
fi

# vm_params.yaml: try sim_params.yaml.vm_params, else fault_params.vel_mod_dir/vm_params.yaml
VM_YAML=$(yaml_get '.vm_params' "$SIM_YAML" 2>/dev/null || true)
if [[ -z "${VM_YAML:-}" || "$VM_YAML" == "null" || ! -f "$VM_YAML" ]]; then
  VDIR=$(yaml_get '.vel_mod_dir' "$FAULT_YAML" 2>/dev/null || true)
  # Fallback: manual grep if yq returned empty or null
  if [[ -z "${VDIR:-}" || "$VDIR" == "null" ]]; then
    VDIR=$(grep -E '^[[:space:]]*vel_mod_dir:' "$FAULT_YAML" | head -1 | sed 's/^[^:]*:[[:space:]]*//')
  fi
  [[ -z "${VDIR:-}" || "$VDIR" == "null" ]] && die "Cannot locate vel_mod_dir in $FAULT_YAML"
  VM_YAML="$VDIR/vm_params.yaml"
fi
[[ -f "$VM_YAML" ]] || die "Missing vm_params.yaml at $VM_YAML"

# ---------- read values ----------
# From VM
NX=$(yaml_get '.nx' "$VM_YAML"); NY=$(yaml_get '.ny' "$VM_YAML"); NZ=$(yaml_get '.nz' "$VM_YAML")
H=$(yaml_get '.hh' "$VM_YAML")
SIM_DURATION=$(yaml_get '.sim_duration' "$VM_YAML")
MODELLON=$(yaml_get '.MODEL_LON' "$VM_YAML"); MODELLAT=$(yaml_get '.MODEL_LAT' "$VM_YAML"); MODELROT=$(yaml_get '.MODEL_ROT' "$VM_YAML")
GRIDFILE=$(yaml_get '.GRIDFILE' "$VM_YAML"); MODEL_PARAMS=$(yaml_get '.MODEL_PARAMS' "$VM_YAML")

# From root
DT=$(yaml_get '.dt' "$ROOT_YAML"); [[ "$DT" == "null" || -z "$DT" ]] && DT=0.005
FLO=$(yaml_get '.flo' "$ROOT_YAML"); [[ "$FLO" == "null" || -z "$FLO" ]] && FLO=1.0
STAT_FILE=$(yaml_get '.stat_file' "$ROOT_YAML")

# From sim
RUN_NAME=$(yaml_get '.run_name' "$SIM_YAML")
SRF_FILE=$(yaml_get '.srf_file' "$SIM_YAML")

# From fault
STAT_COORDS=$(yaml_get '.stat_coords' "$FAULT_YAML")
VMOD_DIR=$(yaml_get '.vel_mod_dir' "$FAULT_YAML")

[[ -n "$RUN_NAME" && "$RUN_NAME" != "null" ]] || die "run_name missing in $SIM_YAML"
[[ -f "$SRF_FILE" ]] || die "srf_file not found: $SRF_FILE"
[[ -f "$STAT_COORDS" ]] || die "stat_coords not found: $STAT_COORDS"
[[ -d "$VMOD_DIR" ]] || die "vel_mod_dir not found: $VMOD_DIR"

# EMOD3D version
EMOD3D_VER=$(yaml_get '.emod3d.emod3d_version' "$ROOT_YAML" 2>/dev/null || true)
[[ -z "${EMOD3D_VER:-}" || "$EMOD3D_VER" == "null" ]] && EMOD3D_VER="3.0.8"
VERSION="${EMOD3D_VER}-mpi"
EMOD3D_BIN="/uoc/project/uoc40001/EMOD3D/tools/emod3d-mpi_v${EMOD3D_VER}"

# ---------- output dirs ----------
LF_DIR="$SIM_DIR/LF"
SEIS_DIR="$LF_DIR/SeismoBin"
RESTART_DIR="$LF_DIR/Restart"
LOG_DIR="$LF_DIR/Rlog"
TS_OUT_DIR="$LF_DIR/TSlice/TSFiles"
MAIN_DUMP_DIR="$LF_DIR/OutBin"
SLIPOUT="$LF_DIR/SlipOut/slipout-k2"
mkdir -p "$SEIS_DIR" "$RESTART_DIR" "$LOG_DIR" "$TS_OUT_DIR" "$MAIN_DUMP_DIR" "$(dirname "$SLIPOUT")"

TS_FILE="$MAIN_DUMP_DIR/${RUN_NAME}_xyts.e3d"

# ---------- derived values ----------
# version-based timeshift: <=3.0.4 → 1/flo ; >3.0.4 → 3/flo
ts_mult=1
IFS=. read -r a b c <<<"${EMOD3D_VER//[^0-9.]/}"
if (( a>3 || (a==3 && (b>0 || (b==0 && c>4))) )); then ts_mult=3; fi

timeshift=$(awk -v flo="$FLO" -v ts="$ts_mult" 'BEGIN{print ts/flo}')
ext_dur=$(awk -v dur="$SIM_DURATION" -v t="$timeshift" 'BEGIN{print dur+t}')
NT=$(awk -v d="$ext_dur" -v dt="$DT" 'BEGIN{printf "%.0f", d/dt}')
DUMP_ITINC="$NT"
DTTS=20
TS_TOTAL=$(awk -v d="$ext_dur" -v dt="$DT" -v dtts="$DTTS" 'BEGIN{printf "%.0f", d/(dt*dtts)}')

# ---------- write e3d.par ----------
cat > "$OUT_PAR" <<EOF
all_in_one=1
enable_output_dump=1
enable_restart=1
freesurf=1
geoproj=1
order=4
swap_bytes=0
vmodel_swapb=0
lonlat_out=1
report=100
n_proc=512
nx=$NX
ny=$NY
nz=$NZ
h=$H
dt=$DT
nt=$NT
flo=$FLO
dtts=$DTTS
dump_itinc=$DUMP_ITINC
ts_total=$TS_TOTAL
version="$VERSION"
name="$RUN_NAME"
restartname="$RUN_NAME"
maxmem=2500
faultfile="$SRF_FILE"
vmoddir="$VMOD_DIR"
modellon=$MODELLON
modellat=$MODELLAT
modelrot=$MODELROT
main_dump_dir="$MAIN_DUMP_DIR"
restartdir="$RESTART_DIR"
seisdir="$SEIS_DIR"
logdir="$LOG_DIR"
slipout="$SLIPOUT"
ts_out_dir="$TS_OUT_DIR"
ts_file="$TS_FILE"
seiscords="$STAT_COORDS"
grid_file="$GRIDFILE"
model_params="$MODEL_PARAMS"
stat_file="$STAT_FILE"
sim_dir="$SIM_DIR"
wcc_prog_dir="$EMOD3D_BIN"
vel_mod_params_dir="$VMOD_DIR"
EOF

echo "✅ Wrote $OUT_PAR"

