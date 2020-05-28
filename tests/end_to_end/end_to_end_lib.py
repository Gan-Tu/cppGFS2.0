import os
import signal
import socket
import subprocess

# This is a shared python module that many end-to-end tests will use. It 
# provides utility function that a distributed end-to-end test can take 
# use to automatically generate configure file and launch a GFS cluster,
# which includes a number of master servers and chunk servers. 
#
# Example: First create a directory designated for your test
#     test_case_name = "test_case"
#     end_to_end_lib.setup_test_directory(test_case_name)
#
# Then specify a config file name and log directory (note here is the 
# system log for the servers, not the operational log) as:
#     config_filename = test_case_name + "/" + "config.yaml"    
#     log_directory = test_case_name + "/" + "logs"
#
# The config file will be used to launch the servers and client and the log
# directory stores teh logs for running the servers, which comes handy for
# debugging.
# 
# Then you call start_master_and_chunk_servers function to launch the server
# cluster:
#    server_procs = end_to_end_lib.start_master_and_chunk_servers(
#                       config_filename, log_directory)
#
# This function launches the master and chunk servers, and returns the process
# handle so you can later kill them for cleanup. The default setting is 1 
# master + 3 chunk servers, and you can configure all fields in the config
# file via this function (see details for configurable params in the function
# below). 
#
# Once you have finished the above steps, you can start lanching your client
# binaries and test its end-to-end behavior.  
#
# When you test a new end-to-end test or deal with a failure that shows up, 
# typically you have run:
#     bazel test -c dbg ...
# The "-c dbg" is for debugging build and is optional. You may find it hard
# to locate the generated data and files in bazel as they got generated in
# sand box (by default). To see the files in sandbox, you have to add 
# --sandbox_debug option in the above command, and then search under the 
# bazel build directory (most likely under your ~/.cache). 
#
# Alternatively, you could debug your test locally, and to do this, you have
# to lauch the script in the correct path as the dependencies on files are 
# expressed in relative path. To do so, first cd into the bazel-bin dir:
#    cd bazel-bin
# Then launch your test script in that directory (note that you may have to
# specify the absolute path of your script, as bazel-bin is a soft-link so
# "../" does point to a different directory of your cppGFS2.0 root)

master_server_name_prefix = "master_server_"
chunk_server_name_prefix = "chunk_server_"
# Used to prevent duplicated port number assignment
used_port_number = []

# Kill a given process
def kill_process(proc):
    if not proc:
        return
    if proc.poll() is None:
        try:
            os.kill(proc.pid,signal.SIGTERM)
        except OSError:
            pass

# Kill all processes in a given list
def kill_all_processes(procs):
    for proc in procs:
        kill_process(proc)

# Pathname to master server's binary
def master_server_binary():
    return "src/server/master_server/run_master_server_main"

# Pathname to chunk server's binary
def chunk_server_binary():
    return "src/server/chunk_server/run_chunk_server_main"

# Create the test directory for a test case. If the test directory already 
# exists, a FileExistsError is raised and this either means there is a 
# duplicated test case name in our test suite, or one has called this function
# twice in the same test. 
def setup_test_directory(test_case_name):
    os.makedirs(test_case_name, exist_ok=True)

# Helper function to generate name for master server for a given server id
def master_server_name(id):
    return master_server_name_prefix + "0" + str(id)

# Helper function to generate name for chunk server for a given server id
def chunk_server_name(id):
    return chunk_server_name_prefix + "0" + str(id)

# Helper function to get a free port number so that servers can use to listen
# to. Note that this doesn't guarantee absolute correctness as there can be 
# race condition such as a returned port got grabed by another process 
# before used by the test. However, we consider this chance to be slim
# for this small project. We do though, prevent the same port from being 
# selected mutliple times by storing the currently selected ports
def get_avaialble_port():
    global used_port_number
    while True:
        # See https://stackoverflow.com/questions/2838244/get-open-tcp-port-in-python/2838309#2838309
        # and https://stackoverflow.com/questions/1365265/on-localhost-how-do-i-pick-a-free-port-number  
        # for some explanations
        # Note: there is a nicer way to write this in Python3:
        # https://www.scivision.dev/get-available-port-for-shell-script/
        new_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        new_socket.bind(("",0))
        port = new_socket.getsockname()[1]
        new_socket.close()
        # Continue if this port has been selected before, otherwise return
        if not port in used_port_number:
            break

    # Add to the used port selection
    used_port_number.append(port)
    return port

# Provide a template of config.yml for test script to generate a config file.
# You can use this function to quickly configure the following fields:
# 1) The number of master servers (default = 1)
# 2) The number of chunk servers (default = 3)
# 3) The block size (default = 64MB), and minimum disk space (default = 100 MB)
# 4) The timeout for grpc (default = 10s), lease (default = 60s), heartbeat (
#    default = 30s) and client_cache (default = 10m)
# This function automatically assign ports that are available on the current 
# machine and automatically generates server names using the "master_server_" +
# {server_id} and "chunk_server_" + {server_id} as name schemes for the servers
# Furthermore, all IP addresses from the dns_lookup_table is 0.0.0.0 as most
# end-to-end tests are expected to run on the same machine
# 
# This function also returns a config object in json (as a directory), which
# will be used later when launching the servers. This is more of an artifact 
# due to the limitation of importing yaml to Bazel sandbox (running out of 
# time to figure this out)
def generate_config_and_config_file(config_filename, num_of_master_server, 
    num_of_chunk_server, block_size_mb, min_free_disk_space_mb,
    grpc_timeout_s, lease_timeout_s, heartbeat_timeout_s,
    client_cache_timeout_m):
 
    config_data = {}
    config_data["version"] = "1.0"
    # Config the servers and network sections
    config_data["servers"] = {}
    config_data["servers"]["master_servers"] = []
    config_data["servers"]["chunk_servers"] = []
    config_data["network"] = {}
    config_data["network"]["dns_lookup_table"] = {}
    
    for i in range(1, num_of_master_server+1):
        server_name = master_server_name(i)
        config_data["servers"]["master_servers"].append(server_name)
        config_data["network"][server_name] = {"hostname" : server_name,
            "port" : get_avaialble_port()}
        config_data["network"]["dns_lookup_table"][server_name] = "0.0.0.0"

    for i in range(1, num_of_chunk_server+1):
        server_name = chunk_server_name(i)
        config_data["servers"]["chunk_servers"].append(server_name)
        config_data["network"][server_name] = {"hostname" : server_name, 
            "port" : get_avaialble_port()}
        config_data["network"]["dns_lookup_table"][server_name] = "0.0.0.0"
  
    # Generate disk config
    config_data["disk"] = {"block_size_mb" : block_size_mb,
                           "min_free_disk_space_mb" : 
                               min_free_disk_space_mb}
    # Generate timeout config
    config_data["timeout"] = {"grpc" : str(grpc_timeout_s) + "s",
        "lease" : str(lease_timeout_s) + "s", 
        "heartbeat" : str(heartbeat_timeout_s) + "s",
        "client_cache" : str(client_cache_timeout_m) + "m"}
 
    # TODO(Xi): Ideally we should just dump the dictionary into text using
    # the yaml module. But I ran into some issues in Bazel setting it up
    # If time allows (most likely not), will refactor this
    indent = " "
    config_file_content = "version: 1.0\n"
    # Generate the servers section
    config_file_content += "servers:\n"
    config_file_content += indent + "master_servers:\n"
    for i in range(1, num_of_master_server+1):
        server_name = master_server_name(i)
        config_file_content += indent + indent + "- " + server_name + "\n"

    config_file_content += indent + "chunk_servers:\n"
    for i in range(1, num_of_chunk_server+1):
        server_name = chunk_server_name(i)
        config_file_content += indent + indent + "- " + server_name + "\n"

    # Generate the network section
    config_file_content += "network:\n"
    for i in range(1, num_of_master_server+1):
        server_name = master_server_name(i)
        config_file_content += indent + server_name + ":\n"
        config_file_content += indent + indent + "hostname: " + server_name \
                                   + "\n"
        config_file_content += indent + indent + "port: " \
            + str(config_data["network"][server_name]["port"]) + "\n"

    for i in range(1, num_of_chunk_server+1):
        server_name = chunk_server_name(i)
        config_file_content += indent + server_name + ":\n"
        config_file_content += indent + indent + "hostname: " + server_name \
                                   + "\n"
        config_file_content += indent + indent + "port: " \
            + str(config_data["network"][server_name]["port"]) + "\n"
    
    # Generate the dns_lookup_table
    config_file_content += indent + "dns_lookup_table:\n"
    for i in range(1, num_of_master_server+1):
        server_name = master_server_name(i)
        config_file_content += indent + indent + server_name + ": 0.0.0.0\n"
    for i in range(1, num_of_chunk_server+1):
        server_name = chunk_server_name(i)
        config_file_content += indent + indent + server_name + ": 0.0.0.0\n"

    # Generate a few configuration params
    config_file_content += "disk:\n"
    config_file_content += indent + "block_size_mb: " + str(block_size_mb) \
                               + "\n"
    config_file_content += indent + "min_free_disk_space_mb: " \
                               + str(min_free_disk_space_mb) + "\n"

    config_file_content += "timeout:\n"
    config_file_content += indent + "grpc: " + str(grpc_timeout_s) + "s\n"
    config_file_content += indent + "lease: " + str(lease_timeout_s) + "s\n"
    config_file_content += indent + "heartbeat: " + str(heartbeat_timeout_s) \
                               + "s\n"
    config_file_content += indent + "client_cache: " \
                               + str(client_cache_timeout_m) + "m\n"

    # Write to the config file
    with open(config_filename, "w") as config_file:
        config_file.write(config_file_content)

    # Return the configuration data
    return config_data

# Start a cluster which is composed by a number of master and chunk servers
# as specified by the configurable params. Optionally this function takes a path
# to the directory to store the logs. Return the processes in a list.
def start_master_and_chunk_servers(config_filename, log_directory = None,
        num_of_master_server = 1, num_of_chunk_server = 3, block_size_mb = 64,
        min_free_disk_space_mb = 100, grpc_timeout_s = 10, lease_timeout_s = 60,
        heartbeat_timeout_s = 30, client_cache_timeout_m = 10):
    # Generate the config file and config data
    config_data = generate_config_and_config_file(config_filename, 
                      num_of_master_server, num_of_chunk_server, block_size_mb,
                      min_free_disk_space_mb, grpc_timeout_s, lease_timeout_s,
                      heartbeat_timeout_s, client_cache_timeout_m)

    master_and_chunk_server_procs = []
    for master_server_name in config_data["servers"]["master_servers"]:
        # Specify the command for master node
        command = [master_server_binary(), "--config_path=%s"%config_filename,
                       "--master_name=%s"%master_server_name]
        master_proc = None
        if log_directory != None:
            os.makedirs(log_directory, exist_ok=True)
            log_file = open(log_directory + "/" + master_server_name \
                                + ".txt", "w+")
            master_proc = subprocess.Popen(command, stderr=log_file)
        else:
            master_proc = subprocess.Popen(command)
        
        master_and_chunk_server_procs.append(master_proc)
    
    # TODO(Xi): Add chunk server's launching once it is shaped up
    return master_and_chunk_server_procs 
