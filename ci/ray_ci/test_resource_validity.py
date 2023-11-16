import subprocess
import sys
import time

import ray

ray.init()


exit_with_error = False

WAIT_TIMEOUT = 600


def verify(result, expected, op="equal"):
    if type(result) in [int, float]:
        if op == "lt":
            return result < expected
        elif op == "gt":
            return result > expected
        else:
            return result == expected

    else:
        return str(result) == str(expected)


def get_all_nodes():
    all_nodes = []
    nodes = ray.nodes()
    print("Nodes from Ray Command: " + str(nodes))

    for node in nodes:
        all_nodes.append(node["NodeName"])

    print("All nodes' IPs: " + str(all_nodes))

    return all_nodes


def run_ssh_command(host, command):
    ssh = subprocess.Popen(
        [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-i",
            "~/ray-bootstrap-key.pem",
            host,
            command,
        ],
        shell=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
    )

    # Read the standard output and error of the subprocess
    return ssh.communicate()[0].splitlines()[0].decode("utf-8")


def get_resource_by_node(node):
    resource = {}
    resource["CPU"] = run_ssh_command(node, "lscpu|awk '/^CPU\\(s\\):/{print $2}'")
    resource["Memory"] = run_ssh_command(
        node, "lsmem -b | awk '/^Total online memory:/{print $4}'"
    )

    print(node + str(resource))
    return resource


if __name__ == "__main__":
    expected_total_cpu, expected_total_memory, nodes_num = (
        int(sys.argv[1]),
        int(sys.argv[2]),
        int(sys.argv[3]),
    )

    nodes = get_all_nodes()
    total_start_time = time.time()
    while len(nodes) < nodes_num and time.time() - total_start_time < WAIT_TIMEOUT:
        time.sleep(20)
        nodes = get_all_nodes()

    total_cpu = 0.0
    total_memory = 0.0
    for node in nodes:
        resource = get_resource_by_node(node)
        total_cpu += float(resource["CPU"])
        total_memory += float(resource["Memory"])

    print(total_cpu)
    print(total_memory)

    exit_with_error = not (
        verify(total_cpu, expected_total_cpu)
        and verify(total_memory, expected_total_memory)
    )

    if exit_with_error:
        print(
            "The resource is not match the setting! Failed in Resource Validity check!"
        )
        sys.exit(1)
