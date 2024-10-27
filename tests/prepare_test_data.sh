#!/bin/bash


#set -x
# Get the directory of the currently running script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Create all tox environments
cd $SCRIPT_DIR/..
tox --notest

env_list=$(tox --listenvs | grep -v ROOT)

for env in $env_list; do
    echo "Running script in tox environment: $env"
    # Run the script inside a subshell with the virtual environment activated
    (
        # Activate the virtual environment for the current tox environment
        cd $SCRIPT_DIR/..
        source .tox/$env/bin/activate
        
        cd $SCRIPT_DIR/joblib/_test_data
        bash ./generate_data.sh 
    )
done
