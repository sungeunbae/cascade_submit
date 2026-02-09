#!/bin/bash

# Script to summarize progress of CyberShake simulations
# Usage: 
#   ./progress_summary.sh              # Check all scenarios
#   ./progress_summary.sh <scenario>   # Check specific scenario

# Base directories
RUNS_DIR="/uoc/project/uoc40001/scratch/baes/Cybershake/v25p11/Runs"
SOURCES_DIR="/uoc/project/uoc40001/scratch/baes/Cybershake/v25p11/Data/Sources"

# Function to display help message
show_help() {
    cat << EOF
Usage: $0 [OPTIONS] [SCENARIO]

Summarize progress of CyberShake simulations by comparing expected outputs
(based on .srf files) with completed outputs (OutBin directories containing files other than e3d.par).

OPTIONS:
    -h, --help          Show this help message and exit

ARGUMENTS:
    SCENARIO            Optional. Name of a specific scenario to check (e.g., CBalleny)
                        If omitted, all scenarios in the Runs directory will be checked

EXAMPLES:
    # Check progress for all scenarios
    $0

    # Check progress for a specific scenario
    $0 CBalleny

    # Show this help message
    $0 --help

OUTPUT:
    For each scenario, displays:
    - Scenario name
    - Number of completed runs / Total expected runs
    - Percentage complete

DIRECTORY STRUCTURE:
    Expected outputs:   ${SOURCES_DIR}/<scenario>/Srf/*.srf
    Completed outputs:  ${RUNS_DIR}/<scenario>/<scenario>_REL*/LF/OutBin/* (excluding e3d.par)

EOF
    exit 0
}

# Function to check progress for a single scenario
# Returns "expected_count completed_count" via echo for accumulation
# Args: scenario_name [return_counts]
#   return_counts: optional, if "1" then return counts to stdout
check_scenario() {
    local scenario=$1
    local return_counts=${2:-0}  # Default to 0 (don't return counts)
    local runs_scenario_dir="${RUNS_DIR}/${scenario}"
    local srf_dir="${SOURCES_DIR}/${scenario}/Srf"
    
    # Check if directories exist
    if [[ ! -d "$runs_scenario_dir" ]]; then
        echo "ERROR: Runs directory not found for scenario $scenario: $runs_scenario_dir" >&2
        [[ $return_counts -eq 1 ]] && echo "0 0"
        return 1
    fi
    
    if [[ ! -d "$srf_dir" ]]; then
        echo "ERROR: Srf directory not found for scenario $scenario: $srf_dir" >&2
        [[ $return_counts -eq 1 ]] && echo "0 0"
        return 1
    fi
    
    # Track failed realizations
    local failed_outbin_rels=()
    local failed_rlog_rels=()
    local expected_count=0
    local completed_count=0
    
    # Process each expected realization based on .srf files
    while read -r srf_path; do
        [[ -z "$srf_path" ]] && continue
        ((expected_count++))
        local rel_name=$(basename "$srf_path" .srf)
        local rel_dir="${runs_scenario_dir}/${rel_name}"
        local outbin_dir="${rel_dir}/LF/OutBin"
        local rlog_dir="${rel_dir}/LF/Rlog"
        
        local has_outbin=0
        if [[ -d "$outbin_dir" ]] && [[ $(find "$outbin_dir" -type f ! -name "e3d.par" 2>/dev/null | wc -l) -gt 0 ]]; then
            ((completed_count++))
            has_outbin=1
        fi

        if [[ $has_outbin -eq 0 ]]; then
            failed_outbin_rels+=("$rel_name")
            # Check for missing/empty Rlog (counts as missing if directory doesn't exist or is empty)
            if [[ ! -d "$rlog_dir" ]] || [[ $(find "$rlog_dir" -type f 2>/dev/null | wc -l) -eq 0 ]]; then
                failed_rlog_rels+=("$rel_name")
            fi
        fi
    done < <(find "$srf_dir" -maxdepth 1 -name "${scenario}.srf" -o -name "${scenario}_REL*.srf" 2>/dev/null)
    
    if [[ $expected_count -eq 0 ]]; then
        echo "WARNING: No .srf files found for scenario $scenario in $srf_dir" >&2
        [[ $return_counts -eq 1 ]] && echo "0 0"
        return 1
    fi
    
    # Calculate percentage
    local percentage=0
    if [[ $expected_count -gt 0 ]]; then
        percentage=$(awk "BEGIN {printf \"%.1f\", ($completed_count / $expected_count) * 100}")
    fi
    
    # Display results (to stderr so it doesn't interfere with count return)
    printf "%-20s  Completed: %3d / %3d  (%6.1f%%)\n" "$scenario" "$completed_count" "$expected_count" "$percentage" >&2
    
    # If single scenario mode and there are failures, list them
    if [[ $return_counts -eq 0 ]]; then
        if [[ ${#failed_outbin_rels[@]} -gt 0 ]]; then
            echo "  Failed (Missing OutBin Data): ${failed_outbin_rels[*]}" >&2
        fi
        if [[ ${#failed_rlog_rels[@]} -gt 0 ]]; then
            echo "  Failed (Missing Rlogs):       ${failed_rlog_rels[*]}" >&2
        fi
    fi

    # Return counts and failed rels for accumulation (to stdout) only if requested
    if [[ $return_counts -eq 1 ]]; then
        local outbin_str=$(IFS=,; echo "${failed_outbin_rels[*]}")
        local rlog_str=$(IFS=,; echo "${failed_rlog_rels[*]}")
        echo "$expected_count $completed_count $outbin_str $rlog_str"
    fi
    return 0
}

# Main script logic
main() {
    # Check for help flag
    if [[ "$1" == "-h" || "$1" == "--help" ]]; then
        show_help
    fi
    
    if [[ $# -eq 0 ]]; then
        # Mode 1: No input - check all scenarios
        echo "Checking progress for all scenarios..."
        echo "========================================"
        
        # Initialize cumulative counters
        local total_expected=0
        local total_completed=0
        local outbin_failures=()
        local rlog_failures=()
        
        # Loop through all directories in RUNS_DIR
        for dir in "$RUNS_DIR"/*; do
            if [[ -d "$dir" ]]; then
                scenario=$(basename "$dir")
                # Capture the returned info
                info=$(check_scenario "$scenario" 1)
                # Parse expected, completed counts, and failed realizations
                expected=$(echo "$info" | awk '{print $1}')
                completed=$(echo "$info" | awk '{print $2}')
                outbin_rels_str=$(echo "$info" | awk '{print $3}')
                rlog_rels_str=$(echo "$info" | awk '{print $4}')
                
                # Accumulate totals
                ((total_expected += expected))
                ((total_completed += completed))
                
                # Track failed scenarios and their specific failed realizations
                if [[ $completed -lt $expected ]]; then
                    # Replace commas with spaces for single-line lists
                    local outbin_f=$(echo "$outbin_rels_str" | tr ',' ' ')
                    local rlog_f=$(echo "$rlog_rels_str" | tr ',' ' ')
                    
                    if [[ -n "$outbin_f" ]]; then
                        outbin_failures+=( "$scenario: $outbin_f" )
                    fi
                    if [[ -n "$rlog_f" ]]; then
                        rlog_failures+=( "$scenario: $rlog_f" )
                    fi
                fi
            fi
        done
        
        # Display cumulative totals
        echo "========================================"
        local total_percentage=0
        if [[ $total_expected -gt 0 ]]; then
            total_percentage=$(awk "BEGIN {printf \"%.1f\", ($total_completed / $total_expected) * 100}")
        fi
        printf "\n%-20s  Completed: %3d / %3d  (%6.1f%%)\n" "TOTAL (All Scenarios)" "$total_completed" "$total_expected" "$total_percentage"
        
        # Display failures missing OutBin data
        if [[ ${#outbin_failures[@]} -gt 0 ]]; then
            echo -e "\nFailed (Missing OutBin Data):"
            for f in "${outbin_failures[@]}"; do
                echo "  - $f"
            done
        fi
        
        # Display failures missing Rlogs
        if [[ ${#rlog_failures[@]} -gt 0 ]]; then
            echo -e "\nFailed (Missing Rlogs):"
            for f in "${rlog_failures[@]}"; do
                echo "  - $f"
            done
        fi
    elif [[ $# -eq 1 ]]; then
        # Mode 2: Single scenario input
        scenario=$1
        echo "Checking progress for scenario: $scenario"
        echo "========================================"
        check_scenario "$scenario"
    else
        echo "Usage: $0 [scenario_name]"
        echo "  No arguments: Check all scenarios"
        echo "  One argument: Check specific scenario"
        exit 1
    fi
}

# Run main function
main "$@"
