import json
import logging
import os
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from threading import RLock
from typing import Any, Dict, List, Optional

import requests

from ray.autoscaler._private.vsphere.utils import is_ipv4
from ray.autoscaler.tags import NODE_KIND_HEAD, NODE_KIND_WORKER, TAG_RAY_NODE_NAME

# Design:

# Each modification the autoscaler wants to make is posted to the API server desired
# state (e.g. if the autoscaler wants to scale up, it adds VM name  to the desired
# worker list it wants to scale, if it wants to scale down it removes the name from
# the list).

# VMRay CRD version
VMRAY_CRD_VER = os.getenv("VMRAY_CRD_VER", "v1alpha1")

# VirtualMachine CRD version
VM_CRD_VER = os.getenv("VM_CRD_VER", "v1")

SERVICE_ACCOUNT_TOKEN = os.getenv("SERVICE_ACCOUNT_TOKEN")

logger = logging.getLogger(__name__)


def url_from_resource(api_server: str, namespace: str, path: str) -> str:
    """Convert resource path to REST URL for Kubernetes API server.

    Args:
        namespace: The K8s namespace of the resource
        path: The part of the resource path that starts with the resource type.
            Supported resource types are "vms" and "rayclusters".
    """
    if path.startswith("virtualmachine"):
        api_group = "/api/" + VM_CRD_VER
    elif path.startswith("vmraycluster"):
        api_group = "/apis/vmray.broadcom.com/" + VMRAY_CRD_VER
    else:
        raise NotImplementedError("Tried to access unknown entity at {}".format(path))
    return api_server + api_group + "/namespaces/" + namespace + "/" + path


class VMNodeStatus(Enum):
    INITIALIZED = "initialized"
    RUNNING = "running"
    FAIL = "failure"


class IKubernetesHttpApiClient(ABC):
    """
    An interface for a Kubernetes HTTP API client.

    This interface could be used to mock the Kubernetes API client in tests.
    """

    @abstractmethod
    def get(self, path: str) -> Dict[str, Any]:
        """Wrapper for REST GET of resource with proper headers."""
        pass

    @abstractmethod
    def patch(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Wrapper for REST PATCH of resource with proper headers."""
        pass


class KubernetesHttpApiClient(IKubernetesHttpApiClient):
    def __init__(self, namespace: str, ca_cert: str, api_server: str):
        self._ca_cert = ca_cert
        self._api_server = api_server
        self._namespace = namespace

        token = os.getenv("SERVICE_ACCOUNT_TOKEN")

        headers = {
            "Authorization": "Bearer " + token,
        }
        self._headers = headers
        self._verify = self._ca_cert

    def get(self, path: str) -> Dict[str, Any]:
        """Wrapper for REST GET of resource with proper headers.

        Args:
            path: The part of the resource path that starts with the resource type.

        Returns:
            The JSON response of the GET request.

        Raises:
            HTTPError: If the GET request fails.
        """
        url = url_from_resource(
            api_server=self._api_server,
            namespace=self._namespace,
            path=path,
        )
        result = requests.get(url, headers=self._headers, verify=self._verify)
        if not result.status_code == 200:
            result.raise_for_status()
        return result.json()

    def patch(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Wrapper for REST PATCH of resource with proper headers

        Args:
            path: The part of the resource path that starts with the resource type.
            payload: The JSON patch payload.

        Returns:
            The JSON response of the PATCH request.

        Raises:
            HTTPError: If the PATCH request fails.
        """
        url = url_from_resource(
            api_server=self._api_server,
            namespace=self._namespace,
            path=path,
        )
        result = requests.patch(
            url,
            json.dumps(payload),
            headers={**self._headers, "Content-type": "application/json-patch+json"},
            verify=self._verify,
        )
        if not result.status_code == 200:
            result.raise_for_status()
        return result.json()


class ClusterOperatorClient(IKubernetesHttpApiClient):
    def __init__(self, provider_config: Dict[str, Any]):
        logger.info("Creating Cluster Operator Client.")
        self.cluster_name = provider_config["cluster_name"]
        self.supervisor_cluster_config = provider_config["supervisor_cluster"]
        self.namespace = self.supervisor_cluster_config["user_namespace"]
        assert (
            os.getenv("SERVICE_ACCOUNT_TOKEN", None) is not None
        ), "To use vSphereNodeProvider, must set SERVICE_ACCOUNT_TOKEN env variable."

        self.k8s_api_client = IKubernetesHttpApiClient(
            self.namespace,
            self.supervisor_cluster_config.get("ca_cert"),
            self.supervisor_cluster_config.get("api_server"),
        )
        self.lock = RLock()

    def list_vms(self, tag_filters: Dict[str, str]) -> list:
        """Queries K8s for VMs in the RayCluster and filter them as per
        tags provided in the tag_filters.
        """
        with self.lock:
            logger.debug(f"Tag filter is {tag_filters}")
            nodes = []  # list of VM IDs
            vmray_cluster_status = self._get("vmrayclusters/status")
            if NODE_KIND_HEAD in tag_filters.values():
                head_node_status = vmray_cluster_status.get("head_node_status", None)
                # head node is found
                if head_node_status:
                    nodes.append(head_node_status.get("name"))
            if NODE_KIND_WORKER in tag_filters.values():
                current_workers = vmray_cluster_status.get("current_workers", None)
                # worker nodes found
                for worker in current_workers:
                    nodes.append(worker.get("name"))
            logger.debug(f"Non terminated nodes are {nodes}")
            return nodes

    def is_vm_power_on(self, nodeId: str) -> bool:
        """Check current vm list. If its state is Running then return
        true else false."""
        with self.lock:
            node = self._get_node(nodeId)
            if node:
                return node.get("vm_status") == VMNodeStatus.RUNNING.value
            return False

    def is_vm_creating(self, nodeId: str) -> bool:
        """Check current vm list. If its state is INITIALIZED then return
        true else false."""
        with self.lock:
            node = self._get_node(nodeId)
            if node:
                return node.get("vm_status") == VMNodeStatus.INITIALIZED.value
            return False

    def set_node_tags(self, tags: Dict[str, str]) -> None:
        """
        Not required
        """
        pass

    def get_vm_external_ip(self, nodeId: str) -> Optional[str]:
        """Check current worker list and get the external ip."""
        with self.lock:
            node = self._get_node(nodeId)
            if node and node.get("vm_status") == VMNodeStatus.RUNNING.value:
                ip = node.get("ip")
                if is_ipv4(ip):
                    return ip
            logger.warning(f"External IPv4 address of VM {nodeId} is not available")
            return None

    def delete_node(self, nodeId: str) -> None:
        """Remove name of the vm from the desired worker list and patch
        the VmRayCluster CR"""
        with self.lock:
            vmray_cluster_spec = self._get("vmrayclusters/spec")
            # get desired workers
            current_desired_workers = set(vmray_cluster_spec.get("desired_workers"))
            new_desired_workers = current_desired_workers.copy()
            # remove the node from the desired workers list
            new_desired_workers.discard(nodeId)
            if len(new_desired_workers) < len(current_desired_workers):
                logger.info("Deleting VM {}".format(nodeId))
                path = "vmraycluster"
                payload = {
                    "path": "spec/VMRayClusterSpec",
                    "value": {"desired_workers": new_desired_workers},
                }
                self._patch(path, payload)

    def create_nodes(
        self,
        node_config: Dict[str, Any],
        tags: dict[str, str],
        to_be_launched_node_count: int,
    ) -> Optional[Dict[str, Any]]:
        """Ask cluster operator to create worker VMs"""
        with self.lock:
            if to_be_launched_node_count > 0:
                new_desired_workers = []
                # The nodes are named as follows:
                # <cluster-name>-head-<uuid> for the head node
                # <cluster-name>-worker-<uuid> for the worker nodes
                new_vm_names = [
                    "{}_{}_{}".format(
                        self.cluster_name, tags[TAG_RAY_NODE_NAME], str(uuid.uuid4())
                    )
                    for _ in range(to_be_launched_node_count)
                ]
                logger.info("Creating new VMs {new_vm_names}")
                vmray_cluster_spec = self._get("vmrayclusters/spec")
                # get desired workers
                current_workers = vmray_cluster_spec.get("desired_workers")
                # append new VM names with existing one
                new_desired_workers.extend(
                    current_workers,
                )
                new_desired_workers.extend(new_vm_names)
                path = "vmraycluster"
                payload = {
                    "path": "spec/VMRayClusterSpec",
                    "value": {"desired_workers": new_desired_workers},
                }
                self._patch(path, payload)

    def _get_node(self, nodeId: str) -> Any:
        vmray_cluster_status = self._get("vmrayclusters/status")
        head_node_status = vmray_cluster_status.get("head_node_status", None)
        # head node is found
        if head_node_status and head_node_status.get("name") == nodeId:
            return head_node_status
        current_workers = vmray_cluster_status.get("current_workers", None)
        # worker nodes found
        for worker in current_workers:
            if worker.get("name") == nodeId:
                return worker
        logger.warning(f"VM {nodeId} is not found")

        return None

    def _get(self, path: str) -> Dict[str, Any]:
        """Wrapper for REST GET of resource with proper headers."""
        return self.k8s_api_client.get(path)

    def _patch(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Wrapper for REST PATCH of resource with proper headers."""
        return self.k8s_api_client.patch(path, payload)
