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
from ray.autoscaler.tags import NODE_KIND_HEAD, NODE_KIND_WORKER, \
TAG_RAY_NODE_NAME, TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_STATUS, \
STATUS_UP_TO_DATE, STATUS_SETTING_UP,STATUS_UNINITIALIZED

# Design:

# Each modification the autoscaler wants to make is posted to the API server desired
# state (e.g. if the autoscaler wants to scale up, it adds VM name  to the desired
# worker list it wants to scale, if it wants to scale down it removes the name from
# the list).

# VMRay CRD version
VMRAY_CRD_VER = os.getenv("VMRAY_CRD_VER", "v1alpha1")

SERVICE_ACCOUNT_TOKEN = os.getenv("SVC_ACCOUNT_TOKEN")

logger = logging.getLogger(__name__)


def url_from_resource(api_server: str, namespace: str, path: str) -> str:
    """Convert resource path to REST URL for Kubernetes API server.

    Args:
        namespace: The K8s namespace of the resource
        path: The part of the resource path that starts with the resource type.
            Supported resource types are "vms" and "rayclusters".
    """
    if path.startswith("vmrayclusters"):
        api_group = "/apis/vmray.broadcom.com/" + VMRAY_CRD_VER
    else:
        raise NotImplementedError("Tried to access unknown entity at {}".format(path))
    return "https://" + api_server + api_group + "/namespaces/" + namespace + "/" + path


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

        token = SERVICE_ACCOUNT_TOKEN

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
        result = requests.get(url, headers=self._headers, verify=False)
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
            verify=False,
        )
        if not result.status_code == 200:
            result.raise_for_status()
        return result.json()


class ClusterOperatorClient(KubernetesHttpApiClient):
    def __init__(self,cluster_name:str, provider_config: Dict[str, Any]):
        self.cluster_name = cluster_name
        self.supervisor_cluster_config = provider_config["vsphere_config"]
        self.namespace = self.supervisor_cluster_config["namespace"]
        assert (
            SERVICE_ACCOUNT_TOKEN is not None
        ), "To use vSphereNodeProvider, must set SVC_ACCOUNT_TOKEN env variable."

        self.k8s_api_client = KubernetesHttpApiClient(
            self.namespace,
            self.supervisor_cluster_config.get("ca_cert"),
            self.supervisor_cluster_config.get("api_server"),
        )
        self.lock = RLock()

    def list_vms(self, tag_filters: Dict[str, str]) -> list:
        """Queries K8s for VMs in the RayCluster and filter them as per
        tags provided in the tag_filters.
        """
        tag_cache = {}
        with self.lock:
            filters = tag_filters.copy()
            if TAG_RAY_CLUSTER_NAME not in tag_filters:
                filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name
            nodes = []  # list of VM IDs
            logger.info(f"tag filters are {tag_filters}")
            vmray_cluster_response = None
            try:
                vmray_cluster_response = self._get_cluster_response()
            except requests.exceptions.HTTPError as e:
                # If HTTP 404 received means the cluster is not yet created.
                if e.response.status_code == 404:
                    return nodes
                raise e
            vmray_cluster_status = vmray_cluster_response.get("status")
            if not vmray_cluster_status:
                return nodes
            if NODE_KIND_HEAD in tag_filters.values() or not tag_filters:
                logger.info(f"Getting head node")
                head_node_status = vmray_cluster_status.get("head_node_status", None)
                # head node is found
                if head_node_status:
                    nodeId = self.cluster_name+"-head"
                    nodes.append(nodeId)
                    new_filters = filters.copy()
                    # Setting head node status
                    status = head_node_status.get("vm_status", None)
                    if status and status == VMNodeStatus.RUNNING.value:
                        new_filters[TAG_RAY_NODE_STATUS] = STATUS_UP_TO_DATE
                    elif status and status == VMNodeStatus.INITIALIZED.value:
                        new_filters[TAG_RAY_NODE_STATUS] = STATUS_SETTING_UP
                    else:
                        new_filters[TAG_RAY_NODE_STATUS] = STATUS_UNINITIALIZED
                    tag_cache[nodeId] = new_filters
            if NODE_KIND_WORKER in tag_filters.values() or not tag_filters:
                logger.info(f"Getting worker nodes")
                current_workers = vmray_cluster_status.get("current_workers", {})
                # worker nodes found
                for worker in current_workers.keys():
                    nodes.append(worker)
                    new_filters = filters.copy()
                    # setting worker node status
                    status = worker.get("vm_status", None)
                    if status and status == VMNodeStatus.RUNNING.value:
                        new_filters[TAG_RAY_NODE_STATUS] = STATUS_UP_TO_DATE
                    elif status and status == VMNodeStatus.INITIALIZED.value:
                        new_filters[TAG_RAY_NODE_STATUS] = STATUS_SETTING_UP
                    else:
                        new_filters[TAG_RAY_NODE_STATUS] = STATUS_UNINITIALIZED
                    tag_cache[worker] = new_filters
                # List VMs from the desired workers' list
                vmray_cluster_spec = vmray_cluster_response.get("spec")
                desired_workers = vmray_cluster_spec.get("desired_workers", [])
                for worker in desired_workers:
                    if worker in current_workers.keys():
                        continue
                    nodes.append(worker)
                    new_filters = filters.copy()
                    new_filters[TAG_RAY_NODE_STATUS] = STATUS_SETTING_UP
                    tag_cache[worker] = new_filters

            logger.info(f"Non terminated nodes are {nodes}")
            return nodes, tag_cache

    def is_vm_power_on(self, nodeId: str) -> bool:
        """Check current vm list. If its state is Running then return
        true else false."""
        with self.lock:
            node = self._get_node(nodeId)
            if node:
                logger.info(f"{nodeId}: {node}")
                return node.get("vm_status") == VMNodeStatus.RUNNING.value
            logger.info(f"VM {nodeId} not found")
            return False

    def is_vm_creating(self, nodeId: str) -> bool:
        """Check current vm list. If its state is INITIALIZED then return
        true else false."""
        with self.lock:
            node = self._get_node(nodeId)
            if node:
                logger.info(f"{nodeId}: {node}")
                return node.get("vm_status") == VMNodeStatus.INITIALIZED.value
            logger.info(f"VM {nodeId} is not in initialized status")
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
                ip = node.get("ip", None)
                if ip and is_ipv4(ip):
                    logger.info(f"external IP is for {nodeId} is {ip}")
                    return ip
            logger.warning(f"External IPv4 address of VM {nodeId} is not available")
            return None

    def delete_node(self, nodeId: str) -> None:
        """Remove name of the vm from the desired worker list and patch
        the VmRayCluster CR"""
        with self.lock:
            vmray_cluster_response = self._get_cluster_response()
            vmray_cluster_spec = vmray_cluster_response.get("spec")
            # get desired workers
            current_desired_workers = set(vmray_cluster_spec.get("desired_workers"))
            logger.info("Current desired VMs {current_desired_workers}")
            new_desired_workers = current_desired_workers.copy()
            # remove the node from the desired workers list
            new_desired_workers.discard(nodeId)
            new_desired_workers = list(new_desired_workers)
            logger.info("New desired VMs {new_desired_workers}")
            if len(new_desired_workers) < len(current_desired_workers):
                logger.info("Deleting VM {}".format(nodeId))
                path = f"vmrayclusters/{self.cluster_name}"
                payload = [{
                        "op": "replace",
                        "path": "/spec/desired_workers",
                        "value": new_desired_workers,  
                    }]
                self._patch(path, payload)

    def create_nodes(
        self,
        node_config: Dict[str, Any],
        tags: Dict[str, str],
        to_be_launched_node_count: int,
    ) -> Optional[Dict[str, Any]]:
        """Ask cluster operator to create worker VMs"""
        logger.info(f"Creating {to_be_launched_node_count} nodes.")
        created_nodes_dict={}
        with self.lock:
            if to_be_launched_node_count > 0:
                new_desired_workers = []
                new_vm_names = []
                # The nodes are named as follows:
                # <cluster-name>-head for the head node
                # <cluster-name>-worker-<uuid> for the worker nodes
                if "head" in tags[TAG_RAY_NODE_NAME]:
                    new_vm_names = [f"{self.cluster_name}-head"]
                else:
                    new_vm_names = [
                        f"{self.cluster_name}-worker-{str(uuid.uuid4())}"
                        for _ in range(to_be_launched_node_count)
                    ]
                logger.info(f"Creating new VMs {new_vm_names}")
                # Cluster is not exist and need to create new Head node
                if "head" in tags[TAG_RAY_NODE_NAME]:
                    created_nodes_dict[f"{self.cluster_name}-head"] = f"{self.cluster_name}-head"
                else:
                    vmray_cluster_reponse = self._get_cluster_response()
                    vmray_cluster_spec = vmray_cluster_reponse.get("spec")
                    # get desired workers
                    current_desired_workers = vmray_cluster_spec.get("desired_workers", None)
                    logger.info(f"Current desired state: {current_desired_workers}")
                    # append new VM names with existing one
                    if current_desired_workers:
                        new_desired_workers.extend(
                            current_desired_workers
                        )
                    new_desired_workers.extend(new_vm_names)
                    logger.info(f"Adding VMs to a desired state: {new_vm_names}")
                    path = f"vmrayclusters/{self.cluster_name}"
                    payload = [{
                        "op": "replace",
                        "path": "/spec/desired_workers",
                        "value": new_desired_workers,  
                    }]
                    self._patch(path, payload)
                    for vm in new_vm_names:
                        created_nodes_dict[vm] = vm
                return created_nodes_dict
    
    def _get_cluster_response(self):
        with self.lock:
            return self._get(f"vmrayclusters/{self.cluster_name}")

    def _get_node(self, nodeId: str) -> Any:
        vmray_cluster_response = self._get_cluster_response()
        vmray_cluster_status = vmray_cluster_response.get("status")
        if not vmray_cluster_status:
            return None
        head_node_status = vmray_cluster_status.get("head_node_status", None)
        # head node is found
        if head_node_status and nodeId == self.cluster_name+"-head":
            return head_node_status
        current_workers = vmray_cluster_status.get("current_workers", None)
        # worker nodes found
        for worker in current_workers.keys():
            if worker == nodeId:
                return current_workers.get(worker)
        # If worker not found in the current worker then it might be getting created
        # and not yet ready. So check if it is in the desired workers list.
        vmray_cluster_spec = vmray_cluster_response.get("spec")
        desired_workers = vmray_cluster_spec.get("desired_workers", [])
        #TODO: Make a function to get the node from current or desired workers.
        for worker in desired_workers:
            if worker == nodeId:
                # set vm_status as VM in the desired workers' list will not 
                # have vm_status field.
                node = {"vm_status": VMNodeStatus.INITIALIZED.value}
                return node
        logger.warning(f"VM {nodeId} not found")

        return None

    def _get(self, path: str) -> Dict[str, Any]:
        """Wrapper for REST GET of resource with proper headers."""
        return self.k8s_api_client.get(path)

    def _patch(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Wrapper for REST PATCH of resource with proper headers."""
        return self.k8s_api_client.patch(path, payload)
    
