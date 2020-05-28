#!/usr/bin/env python
import end_to_end_lib
import os
import signal
import subprocess

def test_main():
    test_case_name = "file_creation_test"
    end_to_end_lib.setup_test_directory(test_case_name)
    config_filename = test_case_name + "/" + "config.yaml"
    config_data = end_to_end_lib.generate_config_and_config_file(
                      config_filename)
    log_directory = test_case_name + "/" + "logs"
    server_procs = end_to_end_lib.start_master_and_chunk_servers(
                       config_filename, config_data, log_directory)

    client_proc = subprocess.Popen(["tests/end_to_end/file_creation_client"])
    client_proc.communicate()

    assert client_proc.returncode == 0

    end_to_end_lib.kill_all_processes(server_procs)

    pass

if __name__ == "__main__":
    test_main()
