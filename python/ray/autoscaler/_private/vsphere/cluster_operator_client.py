import json
import logging
import os
import random
import string
from abc import ABC, abstractmethod
from enum import Enum
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple
from kubernetes import client, config

import requests

from ray.autoscaler._private.vsphere.utils import is_ipv4
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_SETTING_UP,
    STATUS_UNINITIALIZED,
    STATUS_UP_TO_DATE,
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)
import yaml

# Design:

# Each modification the autoscaler wants to make is posted to the API server desired
# state (e.g. if the autoscaler wants to scale up, it adds VM name to the desired
# worker list it wants to scale, if it wants to scale down it removes the name from
# the list).

# VMRay CRD version
VMRAY_CRD_VER = os.getenv("VMRAY_CRD_VER", "v1alpha1")
VMRAY_GROUP = "vmray.broadcom.com"
VMRAYCLUSTER_PLURAL= "vmrayclusters"
VMNODE_CONFIG_PLURAL = "vmraynodeconfigs"

SERVICE_ACCOUNT_TOKEN = os.getenv("SVC_ACCOUNT_TOKEN", None)

DEFAULT_HEAD_NODE_TYPE = "ray.head.default"
DEFAULT_WORKER_NODE_TYPE = "worker"


logger = logging.getLogger(__name__)
cur_path = os.path.dirname(__file__)

class VMNodeStatus(Enum):
    INITIALIZED = "initialized"
    RUNNING = "running"
    FAIL = "failure"

class KubernetesHttpApiClient(object):
    def __init__(self, ca_cert: str, api_server: str):
        token = SERVICE_ACCOUNT_TOKEN
        # If SERVICE_ACCOUNT_TOKEN not present, use local
        # ~/.kube/config file. Active context will be used.
        # This is usefull when Ray CLI are used and local autoscaler needs
        # communicate with the k8s API server
        # If the token is present then use that for communication.
        if not token:
            self.client = client.ApiClient(config.load_kube_config())
        else:
            configuration = client.Configuration()
            configuration.api_key["authorization"] = token
            configuration.api_key_prefix['authorization'] = 'Bearer'
            configuration.host = f'https://{api_server}'
            if ca_cert:
                configuration.ssl_ca_cert = ca_cert
            else:
                configuration.verify_ssl = False
                self.client = client.ApiClient(configuration)
        # Use customObjectsApi to access custom resources
        self.customObjectApi = client.CustomObjectsApi(self.client)

class ClusterOperatorClient(KubernetesHttpApiClient):
    def __init__(self, cluster_name: str, provider_config: Dict[str, Any]):
        self.cluster_name = cluster_name
        self.supervisor_cluster_config = provider_config["vsphere_config"]
        self.max_worker_nodes = None
        self.min_worker_nodes = None
        self.namespace = self.supervisor_cluster_config["namespace"]
        self.k8s_api_client = KubernetesHttpApiClient(
            self.supervisor_cluster_config.get("ca_cert"),
            self.supervisor_cluster_config.get("api_server"),
        )
        self.lock = RLock()

    def list_vms(self, tag_filters: Dict[str, str]) -> Tuple[list, dict]:
        """Queries K8s for VMs in the RayCluster and filter them as per
        tags provided in the tag_filters.
        """
        logger.info(f"Getting nodes using tags \n{tag_filters}")
        tag_cache = {}
        with self.lock:
            filters = tag_filters.copy()
            if TAG_RAY_CLUSTER_NAME not in tag_filters:
                filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name
            nodes = []  
            vmray_cluster_response = None
            vmray_cluster_response = self._get_cluster_response()
            if not vmray_cluster_response:
                return nodes, tag_cache
                
            vmray_cluster_status = vmray_cluster_response.get("status", {})
            if not vmray_cluster_status:
                return nodes, tag_cache
            
            #Check for a head node
            if NODE_KIND_HEAD in tag_filters.values() or not tag_filters:
                head_node_status = vmray_cluster_status.get("head_node_status", {})
                # head node found
                if head_node_status:
                    node_id = f"{self.cluster_name}-h-" + "1" #TODO: Change this
                    nodes.append(node_id)
                    
                    # Setting head node status
                    status = head_node_status.get("vm_status", None)
                    tags = self._set_tags(node_id, NODE_KIND_HEAD, filters[TAG_RAY_USER_NODE_TYPE],
                                   status, filters)
                    
                    tag_cache[node_id] = tags
            # Check current worker nodes 
            if NODE_KIND_WORKER in tag_filters.values() or not tag_filters:
                current_workers = vmray_cluster_status.get("current_workers", {})
                # worker nodes found
                for worker in current_workers.keys():
                    nodes.append(worker)
                    # setting worker node status
                    status = current_workers[worker].get("vm_status", None)
                    tags = self._set_tags(node_id, NODE_KIND_WORKER, filters[TAG_RAY_USER_NODE_TYPE],
                                   status, filters)
                    
                    tag_cache[node_id] = tags
                # List VMs from the desired workers' list
                vmray_cluster_spec = vmray_cluster_response.get("spec", {})
                desired_workers = vmray_cluster_spec.get("desired_workers", [])
                for worker in desired_workers:
                    if worker in current_workers.keys():
                        continue
                    nodes.append(worker)
                    tags = self._set_tags(worker, NODE_KIND_WORKER, filters[TAG_RAY_USER_NODE_TYPE],
                                   STATUS_SETTING_UP, filters)
                    tag_cache[worker] = tags

            logger.info(f"Non terminated nodes are {nodes}")
            logger.info(f"Tags for nodes are: {tag_cache}")
            return nodes, tag_cache

    def is_vm_power_on(self, node_id: str) -> bool:
        """Check current vm list. If its state is Running then return
        true else false."""
        with self.lock:
            node = self._get_node(node_id)
            if node:
                return node.get("vm_status", None) == VMNodeStatus.RUNNING.value
            logger.info(f"VM {node_id} not found")
            return False

    def is_vm_creating(self, node_id: str) -> bool:
        """Check current vm list. If its state is INITIALIZED then return
        true else false."""
        with self.lock:
            node = self._get_node(node_id)
            if node:
                return node.get("vm_status", None) == VMNodeStatus.INITIALIZED.value
            logger.info(f"VM {node_id} is not yet initialized")
            return False

    def set_node_tags(self, tags: Dict[str, str]) -> None:
        """
        Not required
        """
        pass

    def get_vm_external_ip(self, node_id: str) -> Optional[str]:
        """Check current worker list and get the external ip."""
        with self.lock:
            node = self._get_node(node_id)
            if node and node.get("vm_status", None) == VMNodeStatus.RUNNING.value:
                ip = node.get("ip", None)
                if ip and is_ipv4(ip):
                    return ip
            logger.info(f"External IPv4 address of VM {node_id} is not available")
            return None

    def delete_node(self, node_id: str) -> None:
        """Remove name of the vm from the desired worker list and patch
        the VmRayCluster CR"""
        with self.lock:
            vmray_cluster_response = self._get_cluster_response()
            vmray_cluster_spec = vmray_cluster_response.get("spec", {})
            # get desired workers
            current_desired_workers = set(vmray_cluster_spec.get("desired_workers", []))
            logger.info(f"Current desired VMs {current_desired_workers}")
            new_desired_workers = current_desired_workers.copy()
            # remove the node from the desired workers list
            new_desired_workers.discard(node_id)
            new_desired_workers = list(new_desired_workers)
            logger.info(f"New desired VMs {new_desired_workers}")
            # Make sure node was present and deleted from the desired workers list
            if len(new_desired_workers) < len(current_desired_workers):
                logger.info(f"Deleting VM {node_id}")
                path = f"vmrayclusters/{self.cluster_name}"
                payload = [
                    {
                        "op": "replace",
                        "path": "/spec/desired_workers",
                        "value": new_desired_workers,
                    }
                ]
                self.k8s_api_client.customObjectApi.patch_namespaced_custom_object(VMRAY_GROUP,VMRAY_CRD_VER,
                                                                                       self.namespace,VMRAYCLUSTER_PLURAL,
                                                                                       self.cluster_name, payload,
                                                                                       async_req=True)

    def create_nodes(
        self,
        tags: Dict[str, str],
        to_be_launched_node_count: int,
        node_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Ask cluster operator to create worker VMs"""
        logger.info(f"Creating {to_be_launched_node_count} nodes.")
        logger.info(f"Creating nodes with tags: {tags}")
        logger.info(f"Creating nodes with config: {node_config}")
        created_nodes_dict = {}
        with self.lock:
            if to_be_launched_node_count > 0:
                new_desired_workers = []
                new_vm_names = [
                        self._create_node_name(tags[TAG_RAY_NODE_NAME])
                        for _ in range(to_be_launched_node_count)
                    ]
                # First create VMNodeConfig CR if not created
                node_config_name = tags[TAG_RAY_USER_NODE_TYPE]
                if not self._is_node_config_available(node_config_name):
                    self._create_node_config(node_config_name)
                #Create a head node
                if "head" in tags[TAG_RAY_NODE_NAME]:
                     # head node will be created as a part of VMRayCluster CR
                    self._create_vmraycluster(node_config_name)
                else:
                    # Once VMRayCluster CR is created, update it to create worker 
                    # nodes.
                    vmray_cluster_response = self._get_cluster_response()
                    vmray_cluster_spec = vmray_cluster_response.get("spec", {})
                    logger.info(f"Cluster response: {vmray_cluster_response}")
                    # get desired workers
                    desired_workers = vmray_cluster_spec.get("desired_workers", [])
                    # If workers are present in both the list then it shows stable
                    # state for the cluster.
                    # Append new VM names with existing one
                    if desired_workers:
                        new_desired_workers.extend(desired_workers)
                    new_desired_workers.extend(new_vm_names)
                    if len(new_desired_workers) > self.max_worker_nodes:
                        logger.warning(
                            "Autoscaler attempted to create more than max_workers VMs."
                        )
                        return created_nodes_dict
                    logger.info(f"Adding VMs to desired VMs list: {new_vm_names}")
                    payload = [
                        {
                            "op": "replace",
                            "path": "/spec/desired_workers",
                            "value": new_desired_workers,
                        }
                    ]
                    self.k8s_api_client.customObjectApi.patch_namespaced_custom_object(VMRAY_GROUP,VMRAY_CRD_VER, 
                                                                                       self.namespace,VMRAYCLUSTER_PLURAL,
                                                                                       self.cluster_name, payload,
                                                                                       async_req=True)
                for vm in new_vm_names:
                    created_nodes_dict[vm] = vm
            return created_nodes_dict
    
    def _get_cluster_response(self):
        with self.lock:
            response = None
            try:
                response = self.k8s_api_client.customObjectApi.\
                get_namespaced_custom_object(VMRAY_GROUP,VMRAY_CRD_VER, self.namespace,VMRAYCLUSTER_PLURAL, self.cluster_name)
                return response
            except client.exceptions.ApiException as e:
                # If HTTP 404 received means the cluster is not yet created.
                if e.status == 404:
                    logger.warning(f"{self.cluster_name} not available. Creating new one.")
                    return response
                raise e

    def _get_node(self, node_id: str) -> Any:
        vmray_cluster_response = self._get_cluster_response()
        vmray_cluster_status = vmray_cluster_response.get("status", {})
        if not vmray_cluster_status:
            return {}
        head_node_status = vmray_cluster_status.get("head_node_status", {})
        current_workers = vmray_cluster_status.get("current_workers", {})
        # head node is found
        if head_node_status and node_id == self.cluster_name + "-h-1":
            return head_node_status
        # worker nodes found
        for worker in current_workers.keys():
            if worker == node_id:
                return current_workers.get(worker)
        # If worker not found in the current worker then it might be getting created
        # and not yet ready. So check if it is in the desired workers list.
        vmray_cluster_spec = vmray_cluster_response.get("spec", {})
        desired_workers = vmray_cluster_spec.get("desired_workers", [])
        for worker in desired_workers:
            if worker == node_id:
                # set vm_status as VM in the desired workers' list will not
                # have vm_status field.
                node = {"vm_status": VMNodeStatus.INITIALIZED.value}
                return node
        logger.info(f"VM {node_id} not found")

        return {}

    def safe_to_scale(self):
        """
        It is safe to scale as long as total number of workers(desired + current)
        do not exceeds cluster level max_workers.
        This function should handle cases:
        1. If there are workers in the desired_workers list but not in the
        current_workers list that means few workers are not yet up and running.
        2. If there are workers in the current_workers list but not in a
        desired_workers list indicates workers are not yet deleted completely
        and we should wait.
        3. If workers are present in both the list shows stable state for the cluster.
        """
        vmray_cluster_response = self._get_cluster_response()
        vmray_cluster_status = vmray_cluster_response.get("status", {})
        if not vmray_cluster_status:
            return False
        current_workers = vmray_cluster_status.get("current_workers", {})
        vmray_cluster_spec = vmray_cluster_response.get("spec", {})
        desired_workers = vmray_cluster_spec.get("desired_workers", [])
        logger.info(
            f"Checking is it safe to scale:\n"
            f"Current workers: {current_workers.keys()} \n and \n"
            f"Desired workers: {desired_workers}"
        )
        # Do not scale until reaches desired state
        if len(desired_workers) != len(current_workers):
            return False
        # Wait until all nodes are in a Running state
        for worker in current_workers.values():
            if worker.get("vm_status", None) != VMNodeStatus.RUNNING.value:
                return False

        return True

    def _create_node_name(self, node_name_tag):
        """Create name for a Ray node"""
        # The nodes are named as follows:
        # <cluster-name>-h-<random alphanumeric string> for the head node
        # <cluster-name>-w-<<random alphanumeric string>> for the worker nodes
        random_str = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(8)
        )
        if "head" in node_name_tag:
            return f"{self.cluster_name}-h-" + "1"
        return f"{self.cluster_name}-w-" + random_str
    
    def _is_node_config_available(self, node_type: str):
        """Check if node config for a given node type is available or not"""
        node_configs = self.k8s_api_client.customObjectApi.list_namespaced_custom_object(VMRAY_GROUP, VMRAY_CRD_VER, self.namespace, VMNODE_CONFIG_PLURAL)
        for node_config in node_configs:
            # NodeConfig name is same as the node_type provided in the cluster config
            if node_config["NodeConfigName"] == node_type:
                return True
        return False
    
    def _create_node_config(self, node_config_name):
        """Create a node config"""
        node_config = None
        nodeconfig_path = os.path.join(cur_path, "crd/vmray_v1alpha1_vmraynodeconfig.yaml")
        with open(nodeconfig_path,"r") as file:
            node_config = yaml.safe_load(file)
            node_config["metadata"]["name"] = node_config_name
            node_config["metadata"]["namespace"] = self.namespace
            logger.info(f"Creating nodeconfig \n{node_config}")
        self.k8s_api_client.customObjectApi.create_namespaced_custom_object(VMRAY_GROUP, VMRAY_CRD_VER, self.namespace, VMNODE_CONFIG_PLURAL, node_config)

    def _create_vmraycluster(self, head_node_config_name):
        ray_cluster_config = None
        raycluster_config_path = os.path.join(cur_path, "crd/vmray_v1alpha1_vmraycluster.yaml")
        with open(raycluster_config_path, "r") as file:
            ray_cluster_config = yaml.safe_load(file)
            ray_cluster_config["metadata"]["name"] = "ray-cluster-1"
            ray_cluster_config["metadata"]["namespace"] = self.namespace
            ray_cluster_config["spec"]["api_server"]["location"] = self.supervisor_cluster_config.get("api_server")
            ray_cluster_config["spec"]["head_node"]["node_config_name"] = head_node_config_name
        logger.info(f"Creating VmRayCluster \n{ray_cluster_config}")
        self.k8s_api_client.customObjectApi.create_namespaced_custom_object(VMRAY_GROUP, VMRAY_CRD_VER, self.namespace, VMRAYCLUSTER_PLURAL, ray_cluster_config)
    
    def _set_tags(self, node_id, node_kind, node_user_type, node_status, tags):
        new_tags = tags.copy()
        if NODE_KIND_HEAD == node_kind or not new_tags:
            if node_status == VMNodeStatus.RUNNING.value:
                new_tags[TAG_RAY_NODE_STATUS] = STATUS_UP_TO_DATE
            elif node_status == VMNodeStatus.INITIALIZED.value:
                new_tags[TAG_RAY_NODE_STATUS] = STATUS_SETTING_UP
            else:
                new_tags[TAG_RAY_NODE_STATUS] = STATUS_UNINITIALIZED

            new_tags[TAG_RAY_NODE_NAME] = node_id
            new_tags[TAG_RAY_NODE_KIND] = node_kind
            new_tags[TAG_RAY_USER_NODE_TYPE] = node_user_type
            return new_tags
    
    def _set_min_max_worker_nodes(self):
        vmray_cluster_response = self._get_cluster_response()
        if not self.max_worker_nodes:
            vmray_cluster_spec = vmray_cluster_response.get("spec", {})
            worker_node_config = vmray_cluster_spec.get("worker_node", {})
            # If min_workers and max_workers are not provided then default to 0
            self.min_worker_nodes = worker_node_config.get("min_workers", 0)
            self.max_worker_nodes = worker_node_config.get(
                "max_workers", self.min_worker_nodes
            )
            logger.info(
                f"Min and max workers set to {self.min_worker_nodes}"
                f"and {self.max_worker_nodes} respectively."
            )


