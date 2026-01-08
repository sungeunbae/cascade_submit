#!/bin/bash

# Function to display rlog for a given base directory
show_rlog() {
    local base_dir="$1"
    local job_label="$2"  # Optional label for the job (e.g., "2043229[5]")
    local map_file="$3"   # Optional map file path
    
    if [[ -z "$base_dir" ]]; then
        echo "Error: Could not determine base directory for $job_label"
        return
    fi
    
    rlog_dir="${base_dir}LF/Rlog"
    
    # Check if Rlog directory exists
    if [ -d "$rlog_dir" ]; then
        rlog_file=$(ls -t "$rlog_dir"/*.rlog 2>/dev/null | head -n 1)
        if [ -n "$rlog_file" ]; then
            # Get last modified date of rlog file
            rlog_modified=$(ls -l --time-style=long-iso "$rlog_file" | awk '{print $6, $7}')
            
            echo ""
            echo "=========================================="
            if [[ -n "$job_label" ]]; then
                echo "Job: $job_label"
            fi
            if [[ -n "$map_file" ]]; then
                echo "Map file: $map_file"
            fi
            echo "Rlog: $rlog_file"
            echo "Last modified: $rlog_modified"
            echo "=========================================="
            tail -n 10 "$rlog_file"
        else
            echo "No .rlog files found in $rlog_dir"
        fi
    else
        echo "Directory $rlog_dir does not exist"
    fi
}

# Display help if requested
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    cat << 'EOF'
check_rlog.sh - Display the latest rlog file for PBS jobs

USAGE:
    check_rlog.sh <job_id>
    check_rlog.sh --help | -h

DESCRIPTION:
    This script retrieves and displays the last 10 lines of the most recent
    rlog file for a given PBS job. It supports standard jobs, specific array
    jobs, and can display all jobs in a job array.

    For job arrays, the script automatically resolves the correct map file
    by matching the number of jobs in the array with available map files in
    the Logs_Submission directory.

ARGUMENTS:
    job_id              PBS job ID in one of the following formats:
                          - Standard job:       2043229 (or 2043229.pbsserver03)
                          - Specific array job: 2043229[5] (or 2043229[5].pbsserver03)
                          - Array container:    2043229[]

    --help, -h          Display this help message

EXAMPLES:
    # Display rlog for a standard job (short or full ID)
    check_rlog.sh 2043229
    check_rlog.sh 2043229.pbsserver03

    # Display rlog for a specific job in an array
    check_rlog.sh 2043229[5]
    check_rlog.sh 2043229[5].pbsserver03

    # Display rlogs for all jobs in an array
    check_rlog.sh 2043229[]

OUTPUT:
    For each job, the script displays:
      - Job ID
      - Map file path (for array jobs)
      - Rlog file path
      - Last modified date of the rlog file
      - Last 10 lines of the rlog file

EOF
    exit 0
fi

jobid="$1"

# Strip job server suffix (e.g., 2533374.pbsserver03 -> 2533374)
# This handles cases where users copy the full job ID from qstat.
if [[ "$jobid" == *.* ]]; then
    # We strip everything after the first dot. 
    # If it's an array job with a suffix like 2533374[1].pbsserver03, it becomes 2533374[1].
    jobid="${jobid%%.*}"
fi

if [[ -z "$jobid" ]]; then
    echo "Usage: $0 <job_id>"
    echo "  Examples:"
    echo "    $0 2043229          # Standard job (short ID)"
    echo "    $0 2043229.pbssrv   # Standard job (full ID)"
    echo "    $0 2043229[5]       # Specific array job"
    echo "    $0 2043229[]        # All jobs in array"
    echo ""
    echo "Use --help for more information"
    exit 1
fi

# Check if this is an array container request (empty brackets)
if [[ "$jobid" =~ ^([0-9]+)\[\]$ ]]; then
    base_jobid="${BASH_REMATCH[1]}"
    echo "Detected array container request for job: $base_jobid"
    
    # Get all array indices from qstat
    array_indices=$(qstat -t "${base_jobid}[]" 2>/dev/null | awk '$1 ~ /\[[0-9]+\]\./ {match($1, /\[([0-9]+)\]/, arr); print arr[1]}')
    
    if [[ -z "$array_indices" ]]; then
        echo "Error: No array jobs found for ${base_jobid}[]"
        exit 0
    fi
    
    # Get the map file from the first job
    first_index=$(echo "$array_indices" | head -n 1)
    
    # Extract ARRAY_MAP_FILE from qstat -f
    map_file=$(qstat -f "${base_jobid}[${first_index}]" 2>/dev/null | awk '
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
    END {
        n = split(content, vars, ",");
        for (i=1; i<=n; i++) {
            if (vars[i] ~ /^ARRAY_MAP_FILE=/) {
                sub(/^ARRAY_MAP_FILE=/, "", vars[i]);
                print vars[i];
            }
        }
    }
    ' | xargs | cut -d' ' -f1)
    
    if [[ -z "$map_file" || ! -f "$map_file" ]]; then
        echo "Error: Could not find ARRAY_MAP_FILE for job array"
        exit 0
    fi
    
    echo "Using map file: $map_file"
    echo ""
    
    # Iterate through each array index and show its rlog
    for idx in $array_indices; do
        base_dir=$(sed -n "${idx}p" "$map_file" | xargs)
        [[ -n "$base_dir" && "$base_dir" != */ ]] && base_dir="${base_dir}/"
        show_rlog "$base_dir" "${base_jobid}[$idx]" "$map_file"
    done
    
    exit 0
fi

# Try to handle specific job array indices
base_dir=""
if [[ "$jobid" =~ \[([0-9]+)\] ]]; then
    array_index="${BASH_REMATCH[1]}"
    echo "Detected job array index: $array_index"

    # Extract base job ID (e.g., "2043229" from "2043229[1]")
    base_jobid="${jobid%%\[*}"
    
    # Count the number of jobs in the array using qstat -t
    # Note: grep -c always outputs a number, so don't use || echo "0" as it creates duplicate output
    array_job_count=$(qstat -t "${base_jobid}[]" 2>/dev/null | grep -c "\[${base_jobid}\]\[[0-9]\+\]" || true)
    
    if [[ -z "$array_job_count" || "$array_job_count" -eq 0 ]]; then
        # Fallback: count lines that have array indices
        array_job_count=$(qstat -t "${base_jobid}[]" 2>/dev/null | awk '$1 ~ /\[[0-9]+\]\./ {count++} END {print count+0}')
    fi
    
    echo "Array job count from qstat: $array_job_count"

    # Extract ARRAY_MAP_FILE from qstat -f (which can be multi-line in Variable_List)
    map_file=$(qstat -f "${base_jobid}[]" 2>/dev/null | awk '
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
    END {
        n = split(content, vars, ",");
        for (i=1; i<=n; i++) {
            if (vars[i] ~ /^ARRAY_MAP_FILE=/) {
                sub(/^ARRAY_MAP_FILE=/, "", vars[i]);
                print vars[i];
            }
        }
    }
    ' | xargs | cut -d' ' -f1)

    # If we found a map file path, use it to locate the Logs_Submission directory
    if [[ -n "$map_file" ]]; then
        logs_dir=$(dirname "$map_file")
        echo "Logs directory: $logs_dir"
        
        # Find the correct map file by matching job count with line count
        matching_map_files=()
        if [[ "$array_job_count" -gt 0 && -d "$logs_dir" ]]; then
            echo "Searching for map file with $array_job_count lines in $logs_dir..."
            
            for candidate in "$logs_dir"/*.map; do
                if [[ -f "$candidate" ]]; then
                    line_count=$(wc -l < "$candidate")
                    if [[ "$line_count" -eq "$array_job_count" ]]; then
                        matching_map_files+=("$candidate")
                        echo "Found matching map file: $candidate (${line_count} lines)"
                    fi
                fi
            done
        fi
        
        # Handle multiple matching map files
        correct_map_file=""
        if [[ "${#matching_map_files[@]}" -eq 1 ]]; then
            # Exactly one match - use it
            correct_map_file="${matching_map_files[0]}"
        elif [[ "${#matching_map_files[@]}" -gt 1 ]]; then
            # Multiple matches - check if they're all equivalent
            echo "Found ${#matching_map_files[@]} map files with $array_job_count lines. Checking equivalence..."
            
            all_equivalent=true
            first_file="${matching_map_files[0]}"
            
            for ((i=1; i<${#matching_map_files[@]}; i++)); do
                if ! cmp -s "$first_file" "${matching_map_files[$i]}"; then
                    all_equivalent=false
                    break
                fi
            done
            
            if [[ "$all_equivalent" == true ]]; then
                echo "All matching map files are equivalent. Using: $first_file"
                correct_map_file="$first_file"
            else
                echo "ERROR: Multiple non-equivalent map files found with $array_job_count lines:"
                for mf in "${matching_map_files[@]}"; do
                    echo "  - $(basename "$mf")"
                done
                echo "Cannot determine the correct map file automatically."
                echo "Script cannot continue for job $jobid"
                # End script but don't exit SSH session
                return 0 2>/dev/null || exit 0
            fi
        fi
        
        # Use the correct map file if found, otherwise fall back to the original
        if [[ -n "$correct_map_file" ]]; then
            map_file="$correct_map_file"
        else
            echo "Warning: Could not find map file with $array_job_count lines. Using: $map_file"
        fi
    fi

    if [[ -n "$map_file" && -f "$map_file" ]]; then
        echo "Using map file: $map_file"
        base_dir=$(sed -n "${array_index}p" "$map_file" | xargs)
        [[ -n "$base_dir" && "$base_dir" != */ ]] && base_dir="${base_dir}/"
    else
        echo "Warning: Could not find or read ARRAY_MAP_FILE for job array."
    fi
fi

# Fallback to standard Output_Path logic if base_dir not found yet
if [[ -z "$base_dir" ]]; then
    # Get qstat output once for variable extraction
    qstat_output=$(qstat -f "$jobid" 2>/dev/null)
    
    if [[ -n "$qstat_output" ]]; then
        # Check if this standard job is simulating an array (via ARRAY_MAP_FILE and PBS_ARRAY_INDEX)
        # Extract variables from Variable_List (can be multi-line)
        eval_vars=$(echo "$qstat_output" | awk '
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
        END {
            n = split(content, vars, ",");
            for (i=1; i<=n; i++) {
                if (vars[i] ~ /^ARRAY_MAP_FILE=/ || vars[i] ~ /^PBS_ARRAY_INDEX=/) {
                    print vars[i];
                }
            }
        }
        ')
        
        if [[ -n "$eval_vars" ]]; then
            # Parse variables safely
            while IFS='=' read -r key value; do
                case "$key" in
                    ARRAY_MAP_FILE) 
                        # Clean up value - take only until first comma, space, or trash
                        map_file=$(echo "$value" | tr ',' ' ' | awk '{print $1}')
                        ;;
                    PBS_ARRAY_INDEX) 
                        # Clean up value - take only the numeric part
                        sim_index=$(echo "$value" | grep -o '[0-9]\+' | head -n 1)
                        ;;
                esac
            done <<< "$eval_vars"
        fi

        if [[ -n "$map_file" && -n "$sim_index" && -f "$map_file" ]]; then
            echo "Detected simulated job array variables (Index: $sim_index)."
            echo "Using map file: $map_file"
            base_dir=$(sed -n "${sim_index}p" "$map_file" | xargs)
            [[ -n "$base_dir" && "$base_dir" != */ ]] && base_dir="${base_dir}/"
        fi

        # Final fallback: Look for Output_Path if base_dir still not determined
        if [[ -z "$base_dir" ]]; then
            line=$(echo "$qstat_output" | grep "Output_Path")
            if [[ -n "$line" ]]; then
                # Remove everything up to '='
                output_path=${line#*=}
                # Trim leading/trailing whitespace
                output_path=$(echo "$output_path" | xargs)
                # Remove hostname prefix before colon
                output_path=${output_path#*:}
                # Get directory path (strip filename)
                base_dir=$(dirname "$output_path")/
            fi
        fi
    fi
fi

# Display the rlog using the helper function
show_rlog "$base_dir" "$jobid" "$map_file"
