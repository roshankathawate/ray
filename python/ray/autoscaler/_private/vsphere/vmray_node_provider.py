import logging
from typing import Any, Dict

from ray.autoscaler._private.vsphere.cluster_operator_client import (
    ClusterOperatorClient,
)
from ray.autoscaler.node_provider import NodeProvider

logger = logging.getLogger(__name__)


class VmRayNodeProvider(NodeProvider):
    max_terminate_nodes = 1000

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.client = ClusterOperatorClient(self.provider_config)

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config

    def non_terminated_nodes(self, tag_filters):
        return self.client.list_vms(tag_filters)

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
        to_be_launched_node_count = count

        logger.info(f"Create {count} node with tags : {tags}")

        created_nodes_dict = {}
        if to_be_launched_node_count > 0:
            created_nodes_dict = self.client.create_nodes(
                node_config, tags, to_be_launched_node_count
            )

        return created_nodes_dict

    def terminate_node(self, node_id):
        if node_id is None:
            return

        self.client.delete_node(node_id)

    def terminate_nodes(self, node_ids):
        if not node_ids:
            return

        for node_id in node_ids:
            self.client.delete_node(node_id)
