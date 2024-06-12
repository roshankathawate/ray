import copy
import os
import re

import pytest
from threading import RLock
from unittest.mock import MagicMock, patch

from python.ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_SETTING_UP,
    STATUS_UNINITIALIZED,
    STATUS_UP_TO_DATE,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.autoscaler._private.vsphere.cluster_operator_client import (
    DEFAULT_WORKER_NODE_TYPE,
    ClusterOperatorClient,
    VMNodeStatus,
)


_CLUSTER_NAME = "test"
_PROVIDER_CONFIG = {
    "vsphere_config": {
        "namespace": "test",
        "ca_cert": "",
        "api_server": "10.10.10.10",
    }
}

_CLUSTER_RESPONSE = {
    "spec": {
        "desired_workers": [
            "test-cluster-worker-444d0450-0d67-4180-9bd9-d43ee02b5186",
            "test-cluster-worker-bed015eb-1290-4e77-a70d-4e2778d627a0",
            "test-cluster-worker-fe8a67a8-65b1-4634-b067-536112f25110",
        ],
        "worker_node": {
            "idle_timeout_minutes": 5,
            "max_workers": 5,
            "min_workers": 3,
            "node_config_name": "ray-nodeconfig",
        },
    },
    "status": {
        "cluster_state": "healthy",
        "current_workers": {
            "test-cluster-worker-444d0450-0d67-4180-9bd9-d43ee02b5186": {
                "vm_status": "initialized"
            },
            "test-cluster-worker-bed015eb-1290-4e77-a70d-4e2778d627a0": {
                "vm_status": "initialized"
            },
            "test-cluster-worker-fe8a67a8-65b1-4634-b067-536112f25110": {
                "vm_status": "initialized"
            },
        },
        "head_node_status": {
            "conditions": [],
            "ip": "172.26.2.88",
            "ray_status": "running",
            "vm_status": "running",
        },
    },
}


def mock_cluster_operator():
    def __init__(self, cluster_name: str, provider_config: dict):
        self.cluster_name = cluster_name
        self.tag_cache = {}
        self.supervisor_cluster_config = provider_config["vsphere_config"]
        self.namespace = self.supervisor_cluster_config["namespace"]
        self.max_worker_nodes = None
        self.min_worker_nodes = None
        os.environ["SERVICE_ACCOUNT_TOKEN"] = "test_account"
        self.k8s_api_client = MagicMock()

    with patch.object(ClusterOperatorClient, "__init__", __init__):
        operator = ClusterOperatorClient(_CLUSTER_NAME, _PROVIDER_CONFIG)
    return copy.deepcopy(operator)


def test_list_vms():
    """Should return nodes as per tags provided in the tag_filters."""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    head_node_tags = {TAG_RAY_NODE_KIND: NODE_KIND_HEAD}
    head_node_tags[TAG_RAY_LAUNCH_CONFIG] = "launch_hash"
    head_node_tags[TAG_RAY_NODE_NAME] = "ray-{}-head".format(operator.cluster_name)
    head_node_tags[TAG_RAY_NODE_STATUS] = STATUS_UNINITIALIZED
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=_CLUSTER_RESPONSE)
    # make sure function provides head node
    nodes, tag_cache = operator.list_vms(head_node_tags)
    # make sure min and max number of worker nodes are set up
    assert operator.min_worker_nodes == 3
    assert operator.max_worker_nodes == 5
    assert len(nodes) == 1
    head_node = f"{operator.cluster_name}-head"
    assert nodes[0] == head_node
    assert head_node in tag_cache.keys()
    assert tag_cache[head_node][TAG_RAY_NODE_NAME] == head_node
    assert tag_cache[head_node][TAG_RAY_NODE_STATUS] == STATUS_UP_TO_DATE
    assert tag_cache[head_node][TAG_RAY_NODE_KIND] == NODE_KIND_HEAD
    assert tag_cache[head_node][TAG_RAY_USER_NODE_TYPE] == "ray.head.default"
    # In case of wrong node kind no node should be returned
    node_tags = head_node_tags.copy()
    node_tags[TAG_RAY_NODE_KIND] = "wrong_node"
    nodes, tag_cache = operator.list_vms(node_tags)
    assert len(nodes) == 0
    assert len(tag_cache) == 0

    # Make sure status updated properly
    cluster_response = _CLUSTER_RESPONSE.copy()
    cluster_response["status"]["head_node_status"][
        "vm_status"
    ] = VMNodeStatus.INITIALIZED.value
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    # make sure the function provides head node only
    nodes, tag_cache = operator.list_vms(head_node_tags)
    assert nodes[0] == head_node
    assert head_node in tag_cache.keys()
    assert tag_cache[head_node][TAG_RAY_NODE_NAME] == head_node
    assert tag_cache[head_node][TAG_RAY_NODE_STATUS] == STATUS_SETTING_UP

    cluster_response["status"]["head_node_status"][
        "vm_status"
    ] = VMNodeStatus.FAIL.value
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    # make sure the function provides head node only
    nodes, tag_cache = operator.list_vms(head_node_tags)
    assert nodes[0] == head_node
    assert head_node in tag_cache.keys()
    assert tag_cache[head_node][TAG_RAY_NODE_NAME] == head_node
    assert tag_cache[head_node][TAG_RAY_NODE_STATUS] == STATUS_UNINITIALIZED

    # make sure the function returns only worker nodes
    worker_node_tags = {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
    cluster_response = _CLUSTER_RESPONSE.copy()
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    nodes, tag_cache = operator.list_vms(worker_node_tags)
    assert len(nodes) == 3
    for node in nodes:
        assert tag_cache[node][TAG_RAY_NODE_NAME] == node
        assert tag_cache[node][TAG_RAY_NODE_STATUS] == STATUS_SETTING_UP
        assert tag_cache[node][TAG_RAY_NODE_KIND] == NODE_KIND_WORKER
        assert tag_cache[node][TAG_RAY_USER_NODE_TYPE] == DEFAULT_WORKER_NODE_TYPE

    # In case of no tag_filters provided return head node as well as worker nodes
    tag_filters = {}
    cluster_response = _CLUSTER_RESPONSE.copy()
    cluster_response["status"]["head_node_status"][
        "vm_status"
    ] = VMNodeStatus.RUNNING.value
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    # make sure the function provides head node as well all worker nodes.
    nodes, tag_cache = operator.list_vms(tag_filters)
    assert len(nodes) == 4
    assert head_node in tag_cache.keys()
    assert tag_cache[head_node][TAG_RAY_NODE_NAME] == head_node
    assert tag_cache[head_node][TAG_RAY_NODE_STATUS] == STATUS_UP_TO_DATE
    for node in nodes:
        if node == head_node:
            continue
        assert tag_cache[node][TAG_RAY_NODE_NAME] == node
        assert tag_cache[node][TAG_RAY_NODE_STATUS] == STATUS_SETTING_UP
        assert tag_cache[node][TAG_RAY_NODE_KIND] == NODE_KIND_WORKER
        assert tag_cache[node][TAG_RAY_USER_NODE_TYPE] == DEFAULT_WORKER_NODE_TYPE
    # make sure the function returns nodes which are not yet initialised but are in
    # desired workers list
    cluster_response = _CLUSTER_RESPONSE.copy()
    del cluster_response["status"]["current_workers"][
        "test-cluster-worker-444d0450-0d67-4180-9bd9-d43ee02b5186"
    ]
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    nodes, tag_cache = operator.list_vms(tag_filters)
    desired_worker = "test-cluster-worker-444d0450-0d67-4180-9bd9-d43ee02b5186"
    assert len(nodes) == 4
    assert tag_cache[desired_worker][TAG_RAY_NODE_NAME] == desired_worker
    assert tag_cache[desired_worker][TAG_RAY_NODE_STATUS] == STATUS_SETTING_UP
    assert tag_cache[desired_worker][TAG_RAY_NODE_KIND] == NODE_KIND_WORKER
    assert tag_cache[desired_worker][TAG_RAY_USER_NODE_TYPE] == DEFAULT_WORKER_NODE_TYPE


def test_is_vm_power_on():
    """Should return True only if VM is in Running state"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.RUNNING.value}
    )
    assert operator.is_vm_power_on("test_vm") is True
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.INITIALIZED.value}
    )
    assert operator.is_vm_power_on("test_vm") is False


def test_is_vm_creating():
    """Should return True only if VM is in INITIALIZED state"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.RUNNING.value}
    )
    assert operator.is_vm_creating("test_vm") is False
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.INITIALIZED.value}
    )
    assert operator.is_vm_creating("test_vm") is True


def test_get_vm_external_ip():
    """Should return IP if it is a valid one otherwise return None"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.RUNNING.value, "ip": "10.10.10.10"}
    )
    ip = operator.get_vm_external_ip("test_vm")
    assert ip == "10.10.10.10"
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.RUNNING.value, "ip": ""}
    )
    ip = operator.get_vm_external_ip("test_vm")
    assert ip is None
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.FAIL.value, "ip": ""}
    )
    ip = operator.get_vm_external_ip("test_vm")
    assert ip is None


def test_delete_node():
    """Should call _patch with updated desired worker nodes"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    cluster_response = _CLUSTER_RESPONSE.copy()
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    operator._patch = MagicMock()
    operator.delete_node("test-cluster-worker-fe8a67a8-65b1-4634-b067-536112f25110")
    operator._patch.assert_called_once()


def test_create_nodes():
    """Should call _patch to create worker nodes"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    cluster_response = _CLUSTER_RESPONSE.copy()
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    # Make sure API is not getting called to create a head node as it should be
    # created by the operator.
    head_node = f"{operator.cluster_name}-head"
    tags = {TAG_RAY_NODE_NAME: head_node}
    operator._patch = MagicMock()
    created_nodes = operator.create_nodes(tags, 1)
    assert len(created_nodes) == 1
    assert created_nodes[head_node] == head_node
    operator._patch.assert_not_called()
    # if nodes to be creaed is zero, _patch should not be called
    created_nodes = operator.create_nodes(tags, 0)
    assert len(created_nodes) == 0
    operator._patch.assert_not_called()
    # Make sure desired number of workers get created
    tags = {TAG_RAY_NODE_NAME: "worker"}
    operator.max_worker_nodes = 5
    created_nodes = operator.create_nodes(tags, 2)
    operator._patch.assert_called()
    assert len(created_nodes) == 2
    # Make sure the function creates worker nodes if no desired workers present
    cluster_response = _CLUSTER_RESPONSE.copy()
    cluster_response["spec"]["desired_workers"] = []
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    created_nodes = operator.create_nodes(tags, 5)
    operator._patch.assert_called()
    assert len(created_nodes) == 5
    # Make sure not to overprovision the worker nodes
    cluster_response = _CLUSTER_RESPONSE.copy()
    cluster_response["spec"]["desired_workers"] = ["test-1", "test-2", "test-3"]
    # # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    created_nodes = operator.create_nodes(tags, 3)
    operator._patch.assert_not_called
    assert len(created_nodes) == 0


def test__get_node():
    """Should provide a node as per given node_id"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    cluster_response = _CLUSTER_RESPONSE.copy()
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    # Get head node
    head_node = f"{operator.cluster_name}-head"
    node = operator._get_node(head_node)
    assert node["vm_status"] == VMNodeStatus.RUNNING.value
    assert node["ip"] == "172.26.2.88"
    # Get worker node from the current workers
    cluster_response["status"]["current_workers"] = {
        "test-cluster-worker-444d0450-0d67-4180-9bd9-d43ee02b5186": {
            "vm_status": "initialized"
        }
    }
    worker_node = "test-cluster-worker-444d0450-0d67-4180-9bd9-d43ee02b5186"
    node = operator._get_node(worker_node)
    assert node["vm_status"] == VMNodeStatus.INITIALIZED.value
    # get worker from the desired workers
    cluster_response["spec"]["desired_workers"] = ["test-vm1"]
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    worker_node = "test-vm1"
    node = operator._get_node(worker_node)
    assert node["vm_status"] == VMNodeStatus.INITIALIZED.value
    # Should not return any node
    cluster_response = _CLUSTER_RESPONSE.copy()
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    worker_node = "test-vm4"
    assert operator._get_node(worker_node) == {}


def test_safe_to_scale():
    """Should not scale until current state matches desired state"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    operator.max_worker_nodes = 5
    cluster_response = _CLUSTER_RESPONSE.copy()
    # Case where not all workers are up and running
    cluster_response["status"]["current_workers"] = {
        "test-vm1": {"vm_status": "running"}
    }
    cluster_response["spec"]["desired_workers"] = ["test-vm1", "test-vm2"]
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    assert operator.safe_to_scale() is False
    # Case where workers are waiting to be deleted
    cluster_response["status"]["current_workers"] = {
        "test-vm1": {"vm_status": "running"},
        "test-vm2": {"vm_status": "running"},
    }
    cluster_response["spec"]["desired_workers"] = ["test-vm1"]
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    assert operator.safe_to_scale() is False
    # Case where current state is a desired state
    operator._patch = MagicMock()
    cluster_response["spec"]["desired_workers"] = ["test-vm1", "test-vm2"]
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    assert operator.safe_to_scale() is True

def test__create_node_name():
    """Should create node name for head and worker nodes"""
    operator = mock_cluster_operator()
    # test for head node name
    pattern = re.compile("^(test-h-[a-z0-9]{5})$")
    node_name = operator._create_node_name("ray-head-node")
    assert pattern.match(node_name) is not None
    # test for worker node name
    pattern = re.compile("^(test-w-[a-z0-9]{5})$")
    node_name = operator._create_node_name("ray-worker-node")
    print(node_name)
    assert pattern.match(node_name) is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
