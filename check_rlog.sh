#!/bin/bash

# ---------------------------------------------------------
# Debug helper
# ---------------------------------------------------------
debug() {
    if [[ -n "$DEBUG" ]]; then
        echo "[DEBUG] $*" >&2
    fi
}

# ---------------------------------------------------------
# Show latest rlog for a base directory
# ---------------------------------------------------------
show_rlog() {
    local base_dir="$1"
    local job_label="$2"
    local map_file="$3"

    debug "show_rlog: base_dir='$base_dir', job_label='$job_label', map_file='$map_file'"

    if [[ -z "$base_dir" ]]; then
        echo "Error: Could not determine base directory for $job_label"
        return
    fi

    [[ "$base_dir" != */ ]] && base_dir="${base_dir}/"
    local rlog_dir="${base_dir}LF/Rlog"
    debug "show_rlog: rlog_dir='$rlog_dir'"

    if [[ -d "$rlog_dir" ]]; then
        local rlog_file
        rlog_file=$(ls -t "$rlog_dir"/*.rlog 2>/dev/null | head -n 1)
        debug "show_rlog: latest rlog_file='$rlog_file'"

        if [[ -n "$rlog_file" ]]; then
            local rlog_modified
            rlog_modified=$(ls -l --time-style=long-iso "$rlog_file" | awk '{print $6, $7}')

            echo ""
            echo "=========================================="
            [[ -n "$job_label" ]] && echo "Job: $job_label"
            [[ -n "$map_file" ]] && echo "Map file: $map_file"
            echo "Rlog: $rlog_file"
            echo "Last modified: $rlog_modified"
            echo "=========================================="
            tail -n 10 "$rlog_file"

            # -----------------------------------------------------
            # New Performance Analysis Logic (PBS/Cascade Version)
            # -----------------------------------------------------
            
            # 1. Check for Finished status
            local last_line_raw
            last_line_raw=$(tail -n 1 "$rlog_file")
            
            # 2. Extract Total Steps (nt)
            local total_nt
            total_nt=$(grep " nt=" "$rlog_file" | head -n 1 | awk -F= '{print $2}' | awk '{print $1}' | xargs)

            if [[ "$last_line_raw" == *"FINISHED"* ]]; then
                echo ""
                echo "✅ Job $job_label: COMPLETED SUCCESSFULLY."
                [[ -n "$total_nt" ]] && echo "Total Steps (nt): $total_nt"
                return
            fi

            # 3. Extract columns from last line for analysis
            # Rlog Columns: step(1), CPU(2), MPI(3), Mb(4), %R(5), CPU_T(6), %R(7), CUMULATIVE_CPU(8)
            local curr_step cum_time
            read -r curr_step _ _ _ _ _ _ cum_time _ <<< "$last_line_raw"

            # Sanitize inputs (remove non-numeric chars if any)
            curr_step=${curr_step%%[^0-9]*}
            cum_time=${cum_time%%[^0-9.]*}

            # 4. Get Time Left from PBS (qstat)
            # We query qstat -f to get walltime info
            local qstat_out
            qstat_out=$(qstat -f "$job_label" 2>/dev/null)
            
            local w_limit w_used w_remain
            # Format: Resource_List.walltime = 08:00:00
            w_limit=$(echo "$qstat_out" | grep "Resource_List.walltime" | awk -F= '{print $2}' | xargs)
            # Format: resources_used.walltime = 00:46:27
            w_used=$(echo "$qstat_out" | grep "resources_used.walltime" | awk -F= '{print $2}' | xargs)
            # Format: Walltime.Remaining = 12345 (seconds) - Optional, might not exist
            w_remain=$(echo "$qstat_out" | grep "Walltime.Remaining" | awk -F= '{print $2}' | xargs)

            # 5. Perform Calculation if we have data
            if [[ -n "$total_nt" && -n "$curr_step" && -n "$cum_time" ]]; then
                python3 -c "
import sys

def parse_time(t_str):
    # Handle HH:MM:SS
    try:
        if not t_str: return 0.0
        parts = list(map(int, t_str.split(':')))
        if len(parts) == 3:
            return parts[0]*3600 + parts[1]*60 + parts[2]
        elif len(parts) == 2:
            return parts[0]*60 + parts[1]
    except:
        pass
    return 0.0

try:
    nt = float('$total_nt')
    step = float('$curr_step')
    elapsed = float('$cum_time')
    
    w_limit_str = '$w_limit'
    w_used_str = '$w_used'
    w_remain_str = '$w_remain'
    
    wall_left = 0.0
    has_time = False
    
    # Priority 1: Use explicit remaining time if available (usually seconds integer)
    if w_remain_str and w_remain_str.strip().isdigit():
        wall_left = float(w_remain_str)
        has_time = True
    # Priority 2: Calculate from Limit - Used
    elif w_limit_str and w_used_str:
        lim_sec = parse_time(w_limit_str)
        used_sec = parse_time(w_used_str)
        if lim_sec > 0:
            wall_left = lim_sec - used_sec
            has_time = True
            
    if has_time and step > 0:
        sec_per_step = elapsed / step
        remain_steps = nt - step
        est_remain = remain_steps * sec_per_step
        
        est_remain_h = est_remain / 3600.0
        wall_left_h = wall_left / 3600.0
        buffer_h = (wall_left - est_remain) / 3600.0
        
        print(f'\n--- Performance Analysis ---')
        print(f'Progress:       {step/nt*100:.1f}%  ({int(step)} / {int(nt)} steps)')
        print(f'Speed:          {sec_per_step:.4f} sec/step')
        print(f'Est. Remaining: {est_remain_h:.2f} hours')
        print(f'Walltime Left:  {wall_left_h:.2f} hours')
        
        if est_remain < wall_left:
            print(f'Status:         ✅ ON TRACK (Buffer: +{buffer_h:.2f} hours)')
        else:
            print(f'Status:         ❌ INSUFFICIENT TIME (Shortfall: {abs(buffer_h):.2f} hours)')
            print(f'Action:         Consider allocating more time or nodes next time.')
    else:
        # We have log data but no qstat time data (job might have finished or queue busy)
        print(f'\nTotal Steps: {int(nt)}')
        if not has_time:
            print('⚠️  (Could not retrieve walltime from qstat to estimate completion)')

except Exception as e:
    # print(e) 
    pass
"
            else
                echo ""
                [[ -n "$total_nt" ]] && echo "Total Steps (nt): $total_nt"
                echo "⚠️  (Waiting for sufficient log data or queue info to estimate time...)"
                if [[ -n "$DEBUG" ]]; then
                    echo "[DEBUG] Missing data for calc: NT=$total_nt STEP=$curr_step TIME=$cum_time WL=$time_left_str"
                fi
            fi
        else
            echo "No .rlog files found in $rlog_dir"
        fi
    else
        echo "Directory $rlog_dir does not exist"
    fi
}

# ---------------------------------------------------------
# Parse Variable_List into key=value pairs
# ---------------------------------------------------------
extract_variable_list() {
    echo "$1" | awk '
    /Variable_List/ {
        found=1;
        gsub(/.*Variable_List = /, "");
        content=$0;
        next;
    }
    found && /^[ \t]/ {
        gsub(/^[ \t]*/, "");
        content=content $0;
        next;
    }
    found {exit}
    END {print content}
    '
}

# ---------------------------------------------------------
# Resolve base_dir for a single jobid (no [] suffix)
# ---------------------------------------------------------
resolve_base_dir_for_job() {
    local jobid="$1"
    local base_dir=""
    local map_file=""
    local sim_index=""

    debug "resolve_base_dir_for_job: jobid='$jobid'"

    local qstat_output
    qstat_output=$(qstat -w -f "$jobid" 2>/dev/null)
    if [[ -z "$qstat_output" ]]; then
        debug "qstat -w -f returned nothing for jobid='$jobid'"
        echo ""
        return
    fi

    # Extract Variable_List
    local eval_vars
    eval_vars=$(extract_variable_list "$qstat_output")
    debug "eval_vars='$eval_vars'"

    # Parse ARRAY_MAP_FILE, PBS_ARRAY_INDEX, PBS_O_WORKDIR
    local pbs_oworkdir=""
    if [[ -n "$eval_vars" ]]; then
        while IFS='=' read -r key value; do
            case "$key" in
                ARRAY_MAP_FILE)
                    map_file=$(echo "$value" | tr ',' ' ' | awk '{print $1}')
                    ;;
                PBS_ARRAY_INDEX)
                    sim_index=$(echo "$value" | grep -o '[0-9]\+' | head -n 1)
                    ;;
                PBS_O_WORKDIR)
                    pbs_oworkdir=$(echo "$value" | tr ',' ' ' | awk '{print $1}')
                    ;;
            esac
        done <<< "$(echo "$eval_vars" | tr ',' '\n')"
    fi

    debug "Parsed: map_file='$map_file', sim_index='$sim_index', PBS_O_WORKDIR='$pbs_oworkdir'"

    # If sim_index is still empty, try extracting from array_index field in qstat output
    if [[ -z "$sim_index" ]]; then
        sim_index=$(echo "$qstat_output" | grep "^    array_index = " | awk '{print $3}')
        debug "Extracted array_index from qstat main output: sim_index='$sim_index'"
    fi

    # Simulated array: ARRAY_MAP_FILE + PBS_ARRAY_INDEX
    if [[ -n "$map_file" && -n "$sim_index" && -f "$map_file" ]]; then
        echo "Detected simulated array job (Index=$sim_index)" >&2
        echo "Using map file: $map_file" >&2
        base_dir=$(sed -n "${sim_index}p" "$map_file" | xargs)
        [[ -n "$base_dir" && "$base_dir" != */ ]] && base_dir="${base_dir}/"
        debug "Simulated array base_dir='$base_dir'"
        echo "$base_dir|$map_file"
        return
    fi

    # Fallback: Output_Path
    local line
    line=$(echo "$qstat_output" | grep "Output_Path")
    debug "Output_Path line='$line'"

    if [[ -n "$line" ]]; then
        local output_path
        output_path=${line#*=}
        output_path=$(echo "$output_path" | xargs)
        output_path=${output_path#*:}
        debug "Parsed output_path='$output_path'"
        base_dir=$(dirname "$output_path")/
        debug "base_dir from Output_Path='$base_dir'"
        echo "$base_dir|$map_file"
        return
    fi

    # Fallback: PBS_O_WORKDIR (for LF-style jobs)
    if [[ -n "$pbs_oworkdir" ]]; then
        base_dir="${pbs_oworkdir}/"
        debug "base_dir from PBS_O_WORKDIR='$base_dir'"
        echo "$base_dir|$map_file"
        return
    fi

    debug "Could not resolve base_dir for jobid='$jobid'"
    echo "|$map_file"
}

# ---------------------------------------------------------
# Find matching map file in fault Logs_Submission
# ---------------------------------------------------------
find_matching_map_file() {
    local fault_dir="$1"
    local array_size="$2"

    local logs_dir="${fault_dir%/}/Logs_Submission"
    debug "find_matching_map_file: fault_dir='$fault_dir', array_size='$array_size', logs_dir='$logs_dir'"

    if [[ ! -d "$logs_dir" ]]; then
        debug "Logs_Submission not found at '$logs_dir'"
        echo ""
        return
    fi

    local candidates
    candidates=$(ls -1t "$logs_dir"/*_realisations.map* 2>/dev/null || true)
    if [[ -z "$candidates" ]]; then
        debug "No *_realisations.map* files in '$logs_dir'"
        echo ""
        return
    fi

    local best=""
    while IFS= read -r mf; do
        [[ -f "$mf" ]] || continue
        local lc
        lc=$(wc -l < "$mf")
        if [[ "$lc" -eq "$array_size" ]]; then
            debug "Map candidate '$mf' has $lc lines (matches array_size)"
            if [[ -z "$best" ]]; then
                best="$mf"
            fi
        else
            debug "Map candidate '$mf' has $lc lines (mismatch)"
        fi
    done <<< "$candidates"

    echo "$best"
}

# ---------------------------------------------------------
# Handle array container: jobid[]
# ---------------------------------------------------------
handle_array_container() {
    local raw_jobid="$1"
    local base_jobid="${raw_jobid%%\[\]*}"

    echo "Detected array container request for job: $base_jobid"

    # Get all indices
    local qstat_t
    qstat_t=$(qstat -w -t "${base_jobid}[]" 2>/dev/null)
    if [[ -z "$qstat_t" ]]; then
        echo "Error: No array jobs found for ${base_jobid}[]"
        return
    fi

    local array_indices

    array_indices=$(echo "$qstat_t" | awk '$1 ~ /\[[0-9]+\]/ {match($1, /\[([0-9]+)\]/, arr); print arr[1]}')
 
    if [[ -z "$array_indices" ]]; then
        echo "Error: Could not extract array indices for ${base_jobid}[]"
        return
    fi

    local array_size
    array_size=$(echo "$array_indices" | wc -l)
    debug "Array size for ${base_jobid}[] is $array_size"

    # Use first index to resolve base_dir → fault_dir
    local first_index
    first_index=$(echo "$array_indices" | head -n 1)
    local base_and_map
    base_and_map=$(resolve_base_dir_for_job "${base_jobid}[${first_index}]")
    local base_dir="${base_and_map%%|*}"
    local map_file_hint="${base_and_map#*|}"

    if [[ -z "$base_dir" ]]; then
        echo "Error: Could not resolve base directory for ${base_jobid}[${first_index}]"
        return
    fi

    # Fault dir = parent of base_dir
    local fault_dir
    fault_dir=$(dirname "${base_dir%/}")
    debug "Fault dir inferred as '$fault_dir' from base_dir='$base_dir'"

    # If PBS gave us a map file and it matches, use it; otherwise search Logs_Submission
    local map_file=""
    if [[ -n "$map_file_hint" && -f "$map_file_hint" ]]; then
        local lc
        lc=$(wc -l < "$map_file_hint")
        if [[ "$lc" -eq "$array_size" ]]; then
            map_file="$map_file_hint"
            debug "Using ARRAY_MAP_FILE from PBS: '$map_file'"
        else
            debug "ARRAY_MAP_FILE '$map_file_hint' has $lc lines (expected $array_size), ignoring"
        fi
    fi

    if [[ -z "$map_file" ]]; then
        map_file=$(find_matching_map_file "$fault_dir" "$array_size")
        if [[ -z "$map_file" ]]; then
            echo "Error: Could not find matching map file in $fault_dir/Logs_Submission for array size $array_size"
            return
        fi
        debug "Using map file from Logs_Submission: '$map_file'"
    fi

    echo "Using map file: $map_file"
    echo ""

    # Iterate through each index and show rlog
    local idx
    while read -r idx; do
        [[ -z "$idx" ]] && continue
        local dir
        dir=$(sed -n "${idx}p" "$map_file" | xargs)
        [[ -n "$dir" && "$dir" != */ ]] && dir="${dir}/"
        show_rlog "$dir" "${base_jobid}[$idx]" "$map_file"
    done <<< "$array_indices"
}

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "Usage: $0 <job_id>"
    echo "  job_id can be:"
    echo "    1234567           standard job"
    echo "    1234567[5]        array element"
    echo "    1234567[]         array container (all elements)"
    exit 0
fi

jobid="$1"
debug "Initial jobid='$jobid'"

if [[ -z "$jobid" ]]; then
    echo "Usage: $0 <job_id>"
    exit 1
fi

# Strip server suffix
if [[ "$jobid" == *.* ]]; then
    jobid="${jobid%%.*}"
fi
debug "Normalized jobid='$jobid'"

# Array container: 1234567[]
if [[ "$jobid" =~ ^([0-9]+)\[\]$ ]]; then
    base_jobid="${BASH_REMATCH[1]}" 
    handle_array_container "$base_jobid" 
    exit 0
fi

# Array element: 1234567[5]
if [[ "$jobid" =~ ^([0-9]+)\[([0-9]+)\]$ ]]; then
    base_jobid="${BASH_REMATCH[1]}"
    array_index="${BASH_REMATCH[2]}"
    debug "Array element: base_jobid='$base_jobid', index='$array_index'"

    # Resolve base_dir + map_file via normal job resolver
    base_and_map=$(resolve_base_dir_for_job "$jobid")
    base_dir="${base_and_map%%|*}"
    map_file_hint="${base_and_map#*|}"

    show_rlog "$base_dir" "$jobid" "$map_file_hint"
    exit 0
fi

# Standard job
base_and_map=$(resolve_base_dir_for_job "$jobid")
base_dir="${base_and_map%%|*}"
map_file_hint="${base_and_map#*|}"

show_rlog "$base_dir" "$jobid" "$map_file_hint"
