#!/bin/bash

# ==============================================================================
# Script: pack_and_upload_lf.sh
# Author: Earthquake Research Software Engineer
# Description:
#   1. Checks EMOD3D completion status (LF/Rlog/*.rlog contains "FINISHED" in last line).
#   2. Tars LF/OutBin, e3d.par, and latest Rlog file for valid realisations into staging.
#   3. Optionally uploads to Dropbox via rclone if --upload-path is provided.
#   4. Optionally cleans up local OutBin files to save space.
#
# Usage:
#   ./pack_and_upload_lf.sh [-f] [--cleanup] [--upload-path <Dropbox_Remote_Path>] <Path_To_Fault_Dir>
#
# Flags:
#   -f            : Force mode. Suppress confirmation prompts for errors or cleanup.
#   --cleanup     : Delete local LF/OutBin directories after successful packing/uploading.
#   --upload-path : Destination for rclone (e.g., dropbox:/Path). If omitted, skips upload.
#
# Example:
#   ./pack_and_upload_lf.sh --cleanup --upload-path dropbox:/QuakeCoRE/Runs/AlpineF2K /gpfs/scratch/cant1/Runs/AlpineF2K
# ==============================================================================

set -u

# --- Default Values ---
FORCE_MODE=false
CLEANUP_MODE=false
FAULT_DIR_PATH=""
DB_PATH=""

# --- Function: Usage ---
usage() {
    echo "Usage: $0 [-f] [--cleanup] [--upload-path <Dropbox_Remote_Path>] <Path_To_Fault_Dir>"
    echo "  -f            : Force mode (no confirmation prompts)"
    echo "  --cleanup     : Delete local LF/OutBin after packing (and uploading if specified)"
    echo "  --upload-path : Rclone remote path (optional)"
    exit 1
}

# --- Argument Parsing ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        -f)
            FORCE_MODE=true
            shift
            ;;
        --cleanup)
            CLEANUP_MODE=true
            shift
            ;;
        --upload-path)
            if [[ -n "${2:-}" ]] && [[ "$2" != -* ]]; then
                DB_PATH="${2%/}" # Remove trailing slash
                shift 2
            else
                echo "Error: --upload-path requires an argument."
                usage
            fi
            ;;
        -*)
            echo "Unknown option: $1"
            usage
            ;;
        *)
            if [[ -z "$FAULT_DIR_PATH" ]]; then
                FAULT_DIR_PATH=$(realpath "$1")
            else
                echo "Error: Too many arguments or invalid argument order."
                usage
            fi
            shift
            ;;
    esac
done

# --- Validation ---
if [[ -z "$FAULT_DIR_PATH" ]]; then
    echo "Error: Missing Fault Directory argument."
    usage
fi

if [ ! -d "$FAULT_DIR_PATH" ]; then
    echo "Error: Directory $FAULT_DIR_PATH does not exist."
    exit 1
fi

if [[ -n "$DB_PATH" ]]; then
    if ! command -v rclone &> /dev/null; then
        echo "Error: rclone is not loaded. Please run 'module load rclone' or add to PATH."
        exit 1
    fi
fi

FAULT_NAME=$(basename "$FAULT_DIR_PATH")
ORIGINAL_DIR="$(pwd)"
STAGING_DIR="${ORIGINAL_DIR}/upload_staging"

# --- State Tracking ---
declare -a SUCCESS_LIST=()
declare -a FAIL_LIST=()
declare -a SKIP_LIST=()

echo "=========================================================="
echo "Workflow: Pack (and optionally Upload) LF Data"
echo "Fault:       $FAULT_NAME"
echo "Source:      $FAULT_DIR_PATH"
echo "Upload To:   ${DB_PATH:-[Skipped]}"
echo "Force:       $FORCE_MODE"
echo "Cleanup:     $CLEANUP_MODE"
echo "=========================================================="

# 1. Setup Staging
mkdir -p "$STAGING_DIR"

# 2. Build List of Directories to Process
cd "$FAULT_DIR_PATH" || exit 1

# Find Median (Exact match to Fault Name)
MEDIAN_DIR=$(find . -maxdepth 1 -type d -name "$FAULT_NAME")
# Find Realisations (Fault Name + _REL + digits)
REL_DIRS=$(find . -maxdepth 1 -type d -name "${FAULT_NAME}_REL*" | sort)

# Combine: Median first, then sorted Realisations
ALL_DIRS="$MEDIAN_DIR $REL_DIRS"

echo "Phase 1: Verification and Packing"

for rel_dir in $ALL_DIRS; do
    # Handle empty find results
    [ -z "$rel_dir" ] && continue

    clean_rel_name=$(basename "$rel_dir")

    # Paths
    rlog_dir="${clean_rel_name}/LF/Rlog"
    outbin_dir="${clean_rel_name}/LF/OutBin"
    e3d_par_file="${clean_rel_name}/LF/e3d.par"
    tar_filename="${STAGING_DIR}/${clean_rel_name}_LF_Data.tar"

    echo "----------------------------------------------------------"
    echo "Checking: $clean_rel_name"

    # Step 1: Check Rlog for "FINISHED" in last line
    # Find the latest rlog file
    rlog_file=$(find "$rlog_dir" -name "*.rlog" -print0 | xargs -0 ls -t 2>/dev/null | head -n 1)

    if [[ -z "$rlog_file" ]]; then
        echo "  [ERROR] No .rlog file found in $rlog_dir"
        FAIL_LIST+=("$clean_rel_name (No Rlog)")
        continue
    fi

    # Check if last line contains "FINISHED"
    last_line=$(tail -n 1 "$rlog_file")
    if [[ "$last_line" != *"FINISHED"* ]]; then
        echo "  [ERROR] EMOD3D not finished. 'FINISHED' not found in last line of $rlog_file"
        echo "  Last line was: $last_line"
        FAIL_LIST+=("$clean_rel_name (Not Finished)")
        continue
    else
        echo "  [OK] Verification passed - FINISHED found in last line."
    fi

    # Step 2: Check if OutBin exists
    if [ ! -d "$outbin_dir" ]; then
        echo "  [ERROR] $outbin_dir does not exist."
        FAIL_LIST+=("$clean_rel_name (No OutBin)")
        continue
    fi

    # Step 3: Check if Tar already exists in Staging
    if [ -f "$tar_filename" ]; then
        echo "  [WARNING] Tar file already exists: $tar_filename"
        
        if [ "$FORCE_MODE" = false ]; then
            read -p "  Overwrite existing tar file? (y/n) " -n 1 -r
            echo ""
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                echo "  [SKIP] Keeping existing tar file."
                SUCCESS_LIST+=("$clean_rel_name")
                continue
            else
                echo "  -> Will overwrite existing tar file."
                rm -f "$tar_filename"
            fi
        else
            echo "  -> Force mode: overwriting existing tar file."
            rm -f "$tar_filename"
        fi
    fi

    # Step 4: Create Tar with OutBin, e3d.par, and latest rlog file
    echo "  -> Packing $outbin_dir, e3d.par, and latest rlog file..."
   
    # Build tar command with files to include
    tar_items=()
    tar_items+=("$outbin_dir")

    if [ -f "$e3d_par_file" ]; then
        tar_items+=("$e3d_par_file")
        echo "     Including e3d.par"
    else
        echo "  [WARNING] e3d.par not found at $e3d_par_file"
    fi

    if [ -f "$rlog_file" ]; then
        tar_items+=("$rlog_file")
        echo "     Including $(basename "$rlog_file")"
    fi

    # Create the tar file
    tar -cf "$tar_filename" "${tar_items[@]}"
    tar_exit_code=$?

    if [ $tar_exit_code -eq 0 ]; then
        echo "  -> Packed successfully."
        SUCCESS_LIST+=("$clean_rel_name")
    else
        echo "  [ERROR] Failed to create tarball (exit code: $tar_exit_code)."
        FAIL_LIST+=("$clean_rel_name (Tar Failed)")
        rm -f "$tar_filename"
    fi
 
    
done

# --- Summary of Phase 1 ---
NUM_FAIL=${#FAIL_LIST[@]}
NUM_SUCCESS=${#SUCCESS_LIST[@]}

echo "----------------------------------------------------------"
echo "Phase 1 Summary:"
echo "  Successful Packs: $NUM_SUCCESS"
echo "  Failed/Skipped:   $NUM_FAIL"

if [ $NUM_FAIL -gt 0 ]; then
    echo "  Failures:"
    printf "   - %s\n" "${FAIL_LIST[@]}"

    if [ "$FORCE_MODE" = false ]; then
        echo ""
        read -p "Errors occurred. Do you want to proceed with uploading/cleaning the successful ones? (y/n) " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Aborting."
            exit 1
        fi
    else
        echo "Force mode enabled. Proceeding despite errors."
    fi
fi

if [ $NUM_SUCCESS -eq 0 ]; then
    echo "Nothing to process. Exiting."
    exit 0
fi

echo "=========================================================="
if [[ -n "$DB_PATH" ]]; then
    echo "Phase 2: Bulk Uploading"

    # Upload staging folder content to Dropbox
    echo "  -> Uploading staging files to $DB_PATH/"
    rclone copy "$STAGING_DIR" "$DB_PATH/" --progress --transfers 8

    if [ $? -eq 0 ]; then
        echo "  [OK] Upload command finished successfully."
    else
        echo "  [ERROR] rclone failed. Please check logs."
        exit 1
    fi

    echo "=========================================================="
    echo "Phase 3: Verification"

    # Check remote presence
    echo "Checking remote files..."
    REMOTE_FILES=$(rclone lsf "$DB_PATH" --files-only --include "*_LF_Data.tar")
    REMOTE_COUNT=$(echo "$REMOTE_FILES" | wc -l)

    echo "  -> Found $REMOTE_COUNT tar files in destination $DB_PATH"

    if [ "$REMOTE_COUNT" -lt "$NUM_SUCCESS" ]; then
        echo "  [WARNING] Remote count ($REMOTE_COUNT) is less than successful local packs ($NUM_SUCCESS)."
        echo "  Please inspect manually."
    else
        echo "  [OK] Remote file count matches local expectations."
    fi
else
    echo "Phase 2 & 3 Skipped (No --upload-path provided)"
fi

echo "=========================================================="
echo "Phase 4: Cleanup"

if [ "$CLEANUP_MODE" = true ]; then

    if [ "$FORCE_MODE" = false ]; then
        echo "WARNING: You are about to DELETE local LF/OutBin directories for processed runs."
        if [[ -z "$DB_PATH" ]]; then
            echo "  [NOTE] You did NOT upload these files to Dropbox. They will only exist as tarballs in '$STAGING_DIR'."
        fi
        read -p "Are you sure? (y/n) " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Cleanup aborted by user."
            exit 0
        fi
    fi

    echo "Cleaning up..."

    # 1. Remove staging dir ONLY if we uploaded it
    if [[ -n "$DB_PATH" ]]; then
        rm -rf "$STAGING_DIR"
        echo "  -> Removed staging directory (files uploaded)."
    else
        echo "  -> Keeping staging directory '$STAGING_DIR' (files NOT uploaded)."
    fi

    # 2. Remove Source OutBins for SUCCESSFUL packs/uploads
    for clean_rel_name in "${SUCCESS_LIST[@]}"; do
        outbin_path="${clean_rel_name}/LF/OutBin"
        if [ -d "$outbin_path" ]; then
            echo "  -> Deleting $outbin_path"
            rm -rf "$outbin_path"
        fi
    done
    echo "  -> Cleanup complete."

else
    echo "Cleanup skipped (use --cleanup to enable). Staging directory $STAGING_DIR preserved."
fi

echo "=========================================================="
echo "All Done."
echo "=========================================================="

