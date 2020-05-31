#!/usr/bin/env python
import atexit
import end_to_end_lib
import os
import signal
import subprocess

# Launch a server cluster with default setting (1 master + 3 chunk servers)
# Execute a client process that creates multiple different files in parallel

# Called when script exiting to prevent dangling processes
def handle_processes_cleanup(procs):
    end_to_end_lib.kill_all_processes(procs)

def test_main():
    # Create a designated folder for this test
    test_case_name = "file_creation_test"
    end_to_end_lib.setup_test_directory(test_case_name)
    # Launch the cluster 
    config_filename = test_case_name + "/" + "config.yaml"
    log_directory = test_case_name + "/" + "logs"
    server_procs = end_to_end_lib.start_master_and_chunk_servers(
                       config_filename, log_directory)

    # Launch client process
    client_proc = subprocess.Popen(["tests/end_to_end/file_creation_client"])
    
    # Register cleanup callback
    atexit.register(handle_processes_cleanup, server_procs + [client_proc])
   
    # We expect client to finish successfully
    client_proc.communicate()
    assert client_proc.returncode == 0

    # Cleanup server processes
    end_to_end_lib.kill_all_processes(server_procs)

if __name__ == "__main__":
    test_main()
