#!/bin/bash

# --- Configuration ---
SUBMIT_SCRIPT="../scripts/submit_emod3d_pbs.sh"
# Ensure DEFAULTS_FILE is set, otherwise default to a standard location or error
DEFAULTS_FILE="${DEFAULTS_FILE:-$SCRATCH/mike/emod3d_defaults.yaml}"

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <EVENT> [NODES] [TASKS] [MEM] [TIME]"
  echo "Example: $0 2012p783751 6 84 735gb 72:00:00"
  exit 1
fi

EVENT=$1
# Default values if not provided arguments
NODES=${2:-6}
TASKS=${3:-84}
MEM=${4:-735gb}
TIME=${5:-72:00:00}

BATCHES="20250502_01_msr_w_c 20250502_01_msr_w_sdvr 20250502_01_msr_w_sdvrqa_basr 20250502_01_msr_w_sdvrqa_basr_POINTSOURCE"

echo "=========================================================="
echo "Preparing batch submission for Event: $EVENT"
echo "Resources: $NODES Nodes | $TASKS Tasks | $MEM | $TIME"
echo "Defaults:  $DEFAULTS_FILE"
echo "=========================================================="

for B in $BATCHES; do
  
  RUN_DIR="$(pwd)/$B/Runs/$EVENT/$EVENT"
  
  echo ""
  echo "----------------------------------------------------------"
  echo "Batch: $B"
  echo "Dir:   $RUN_DIR"

  # --- Check if finished ---
  IS_FINISHED=0
  RLOG="$RUN_DIR/LF/Rlog/${EVENT}-00000.rlog"

  if [[ -f "$RLOG" ]]; then
    # Check last 10 lines for the success message
    if tail -n 10 "$RLOG" | grep -q "PROGRAM emod3d-mpi IS FINISHED"; then
        IS_FINISHED=1
        echo -e "\033[0;32m[STATUS] Run appears COMPLETED in rlog.\033[0m"
    else
        echo -e "\033[0;33m[STATUS] rlog exists but run not marked finished.\033[0m"
    fi
  else
    echo "[STATUS] No rlog found (Fresh run)."
  fi

  # --- First Prompt ---
  read -p "Submit job for $B? (y/n): " CONFIRM
  CONFIRM=${CONFIRM,,} # tolower

  if [[ "$CONFIRM" == "y" ]]; then
      
      # --- Safety Check: If finished, ask again ---
      if [[ "$IS_FINISHED" -eq 1 ]]; then
          echo -e "\033[0;31mWARNING: This job seems to be finished already!\033[0m"
          read -p "Are you SURE you want to resubmit/overwrite? (y/n): " SURE
          SURE=${SURE,,}
          if [[ "$SURE" != "y" ]]; then
              echo "Skipping $B."
              continue
          fi
      fi

      # --- Execute ---
      "$SUBMIT_SCRIPT" "$RUN_DIR" "$NODES" "$TASKS" "$MEM" "$TIME" "$DEFAULTS_FILE" "no"
  
  else
      echo "Skipping $B."
  fi

done

