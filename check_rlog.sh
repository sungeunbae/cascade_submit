#!/bin/bash

jobid="$1"

# Grab the full Output_Path line
line=$(qstat -w -f "$jobid" | grep "Output_Path")

# Remove everything up to '='
output_path=${line#*=}

# Trim leading/trailing whitespace
output_path=$(echo "$output_path" | xargs)

# Remove hostname prefix before colon
output_path=${output_path#*:}

# Get directory path (strip filename)
base_dir=$(dirname "$output_path")/

# Define the Rlog directory
rlog_dir="${base_dir}LF/Rlog"

# Debug print
echo "Raw line: $line"
echo "Output_Path: $output_path"
echo "Base dir: $base_dir"
echo "Rlog dir: $rlog_dir"

# Check if Rlog directory exists
if [ -d "$rlog_dir" ]; then
    rlog_file=$(ls -t "$rlog_dir"/*.rlog 2>/dev/null | head -n 1)
    if [ -n "$rlog_file" ]; then
        echo "Showing last 10 lines of: $rlog_file"
        tail -n 10 "$rlog_file"
    else
        echo "No .rlog files found in $rlog_dir"
    fi
else
    echo "Directory $rlog_dir does not exist"
fi

