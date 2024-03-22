#!/bin/bash
set -ex # Enable debugging and exit on error

# Define arrays of start and end years for future and historical periods
# Adjust these arrays as per your requirement
start_futures=(2071 2076 2081 2086 2091)
end_futures=(2081 2086 2091  2096 2101)
start_hists=(1981 1986 1991 1996 2001)
end_hists=(1991 1996 2001 2006 2011)

# Path to the Python script
script_path="test_code_for_all_test.py"

# Loop over the arrays
for i in "${!start_futures[@]}"; do
    # Make a temporary copy of the Python script
    tmp_script="tmp_${script_path}"
    cp "${script_path}" "${tmp_script}"

    # Use sed to replace the years in the Python script
    sed -i "s/start_future = .*/start_future = ${start_futures[$i]}/" "${tmp_script}"
    sed -i "s/end_future = .*/end_future = ${end_futures[$i]}/" "${tmp_script}"
    sed -i "s/start_hist = .*/start_hist = ${start_hists[$i]}/" "${tmp_script}"
    sed -i "s/end_hist = .*/end_hist = ${end_hists[$i]}/" "${tmp_script}"

    # Run the modified Python script
    python "${tmp_script}"

    # Remove the temporary script
    rm "${tmp_script}"
done
