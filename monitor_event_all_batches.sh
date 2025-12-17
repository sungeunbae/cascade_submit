#!/bin/bash

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <EVENT>"
  echo "Example: $0 2012p783751"
  exit 1
fi

EVENT=$1
BATCHES="20250502_01_msr_w_c 20250502_01_msr_w_sdvr 20250502_01_msr_w_sdvrqa_basr 20250502_01_msr_w_sdvrqa_basr_POINTSOURCE"

echo "=========================================================="
echo "Monitoring Event: $EVENT"
echo "Date: $(date)"
echo "=========================================================="

for B in $BATCHES; do
  
  # Construct the expected Rlog path
  # Note: The log usually ends in -00000.rlog for the main MPI rank
  RLOG_DIR="$(pwd)/$B/Runs/$EVENT/$EVENT/LF/Rlog"
  RLOG_FILE="$RLOG_DIR/${EVENT}-00000.rlog"
  
  echo ""
  echo "----------------------------------------------------------"
  echo "Batch: $B: $(pwd)/$B/Runs/$EVENT/$EVENT/"

  if [[ -f "$RLOG_FILE" ]]; then
      echo "Log:   LF/Rlog/${EVENT}-00000.rlog"
      echo -e "\033[0;36m--- Last 10 lines ---\033[0m"
      tail -n 10 "$RLOG_FILE"
      echo -e "\033[0;36m---------------------\033[0m"
      
      # Quick status check using the completion message
      if tail -n 5 "$RLOG_FILE" | grep -q "PROGRAM emod3d-mpi IS FINISHED"; then
         echo -e "\033[0;32m[STATUS] COMPLETED\033[0m"
      else
         echo -e "\033[0;33m[STATUS] RUNNING / IN PROGRESS\033[0m"
      fi

  else
      # Check if directory exists at least
      if [[ -d "$RLOG_DIR" ]]; then
          echo -e "\033[0;31m[STATUS] Log file not found yet (Job queued or starting?)\033[0m"
          ls -l "$RLOG_DIR" 2>/dev/null
      else
          echo -e "\033[0;31m[STATUS] Directory not found (Job not submitted or path error)\033[0m"
          echo "Expected: $RLOG_DIR"
      fi
  fi

done
echo ""
echo "=========================================================="
