#!/usr/bin/env python
import atexit
import end_to_end_lib
import os
import signal
import subprocess

# Purposely introduce an error during file creation by only launching the
# master server first, after that launch the chunk servers and make sure that
# the retry is successful. This makes sure that the MasterMetadataService roll
# back the file metadata if the file metadata creation succeeded but it failed
# to find chunk server to create the chunk

# Called when script exiting to prevent dangling processes
def handle_processes_cleanup(procs):
    end_to_end_lib.kill_all_processes(procs)

def test_main():
    # Create a designated folder for this test
    test_case_name = "file_creation_retry_test"
    end_to_end_lib.setup_test_directory(test_case_name)
    # Launch the cluster 
    config_filename = test_case_name + "/" + "config.yaml"
    log_directory = test_case_name + "/" + "logs"
    # Only start the master
    master_proc = end_to_end_lib.start_master_and_chunk_servers(
                      config_filename, log_directory, start_chunk = False)

    # Launch client process, this process should fail as there is no chunk
    # server so no chunk allocation done for the first chunk
    client_proc = subprocess.Popen(["gfs_client_main", 
                                    "--config_path=" + config_filename,
                                    "--filename=/foo",
                                    "--mode=create"])
    client_proc.communicate()
    assert client_proc.returncode != 0

    # Launch chunk servers, make it a full cluster
    chunk_procs = end_to_end_lib.start_master_and_chunk_servers(
                      config_filename, log_directory, start_master = False)

    # Launch client to create file again, this time should be successful as
    # the cluster is up
    client_proc = subprocess.Popen(["gfs_client_main", 
                                    "--config_path=" + config_filename,
                                    "--filename=/foo",
                                    "--mode=create"])
    client_proc.communicate()
    assert client_proc.returncode == 0

    # Register cleanup callback
    atexit.register(handle_processes_cleanup, master_proc + chunk_procs)
    
    # Cleanup server processes
    end_to_end_lib.kill_all_processes(master_proc + chunk_procs)

if __name__ == "__main__":
    test_main()
