import logging
import threading
from typing import Any, Dict

from ray.autoscaler._private.vsphere.cluster_operator_client import (
    ClusterOperatorClient,
)
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME

logger = logging.getLogger(__name__)


class VmRayNodeProvider(NodeProvider):
    max_terminate_nodes = 1000

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        # self.frozen_vm_scheduler = None
        # self.vsphere_config = provider_config["vsphere_config"]
        # self.vsphere_credentials = provider_config["vsphere_config"]["credentials"]

        # The below cache will be a map, whose key is the Ray node and the value will
        # be a list of vSphere tags one that node. The reason for this cache is to
        # avoid calling the vSphere API again when the autoscaler tries to read the tags
        # The cache will be filled when a Ray node is created and tagged.
        self.tag_cache = {}
        self.tag_cache_lock = threading.Lock()
        self.client = ClusterOperatorClient(self.provider_config)

    # def ensure_frozen_vm_status(self, frozen_vm_name):
    #     """
    #     This function will help check if the frozen VM with the specific name is
    #     existing and in the frozen state. If the frozen VM is existing and off, this
    #     function will also help to power on the frozen VM and wait until it is frozen.
    #     """
    #     self.client.power_on_vm(frozen_vm_name)

    #     # Make sure it is frozen status
    #     return self.get_pyvmomi_sdk_provider().wait_until_vm_is_frozen(frozen_vm_name)

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config

    def non_terminated_nodes(self, tag_filters):
        nodes, tag_cache = self.client.list_vms(self.cluster_name, tag_filters)
        with self.tag_cache_lock:
            self.tag_cache.update(tag_cache)
        return nodes

    def is_running(self, node_id):
        return self.client.is_vm_power_on(node_id)

    def is_terminated(self, node_id):
        if self.client.is_vm_power_on(node_id):
            return False
        else:
            # If the node is not powered on but has the creating tag, then it could
            # be under reconfiguration, such as plugging the GPU. In this case we
            # should consider the node is not terminated, it will be turned on later
            return not self.client.is_vm_creating(node_id)

    def node_tags(self, node_id):
        with self.tag_cache_lock:
            return self.tag_cache[node_id]

    def external_ip(self, node_id):
        return self.client.get_vm_external_ip(node_id)

    def internal_ip(self, node_id):
        # Currently vSphere VMs do not show an internal IP. So we just return the
        # external IP
        return self.client.get_vm_external_ip(node_id)

    def set_node_tags(self, node_id, tags):
        # This method gets called from the Ray and it passes
        # node_id which needs to be vm.vm and not vm.name
        self.client.set_node_tags(node_id, tags)

    def create_node(self, node_config, tags, count) -> Dict[str, Any]:
        """Creates instances.

        Returns dict mapping instance id to VM object for the created
        instances.
        """
        filters = tags.copy()
        if TAG_RAY_CLUSTER_NAME not in tags:
            filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name

        to_be_launched_node_count = count

        logger.info(f"Create {count} node with tags : {filters}")

        created_nodes_dict = {}
        if to_be_launched_node_count > 0:
            created_nodes_dict = self.client.create_nodes(
                node_config, filters, to_be_launched_node_count
            )

        return created_nodes_dict

    def terminate_node(self, node_id):
        if node_id is None:
            return

        self.client.delete_vm_by_id(node_id)

        with self.tag_cache_lock:
            if node_id in self.tag_cache:
                self.tag_cache.pop(node_id)

    def terminate_nodes(self, node_ids):
        if not node_ids:
            return

        for node_id in node_ids:
            self.terminate_node(node_id)
