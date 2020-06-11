#!/usr/bin/env python
import atexit
import end_to_end_lib
import os
import signal
import subprocess

# Launch a server cluster with default setting (1 master + 3 chunk servers)
# Execute a client process first in writer mode that creates and write to
# multiple different files in parallel, and then launch the client again in
# reader mode to read the files in parallel and verify the content is correct

# Called when script exiting to prevent dangling processes
def handle_processes_cleanup(procs):
    end_to_end_lib.kill_all_processes(procs)

def test_main():
    # Create a designated folder for this test
    test_case_name = "file_simple_write_read_test"
    end_to_end_lib.setup_test_directory(test_case_name)
    # Launch the cluster 
    config_filename = test_case_name + "/" + "config.yaml"
    log_directory = test_case_name + "/" + "logs"
    server_procs = end_to_end_lib.start_master_and_chunk_servers(
                      config_filename, log_directory)

    # Launch client process as writer
    writer_client = subprocess.Popen(
        ["tests/end_to_end/file_simple_write_read_client", "--is_writer"])

    # We expect client to finish successfully
    writer_client.communicate()
    assert writer_client.returncode == 0

    # Launch client process as reader
    reader_client = subprocess.Popen(
        ["tests/end_to_end/file_simple_write_read_client"])

    # Register cleanup callback
    atexit.register(handle_processes_cleanup, 
                    server_procs + [writer_client,reader_client])

    # We expect client to finish successfully
    reader_client.communicate()
    assert reader_client.returncode == 0

    # Cleanup server processes
    end_to_end_lib.kill_all_processes(server_procs)

if __name__ == "__main__":
    test_main()
