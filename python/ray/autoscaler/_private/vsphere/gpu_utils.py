import logging

from pyVmomi import vim

from ray.autoscaler._private.vsphere.sdk_provider import ClientType, get_sdk_provider

logger = logging.getLogger(__name__)


def is_this_gpu_avail_on_this_host(host, gpu):
    for hardware in host.config.assignableHardwareBinding:
        if gpu.pciId in hardware.instanceId and hardware.vm is not None:
            return False
    return True


def is_any_gpu_avail_on_this_host(host, gpus):
    # No VM bind to any GPU card on this host
    if not host.config.assignableHardwareBinding:
        return True

    for gpu in gpus:
        # Find one avaialable GPU card on this host
        if is_this_gpu_avail_on_this_host(host, gpu):
            return True

    # Find no GPU card on this host
    return False


def get_supported_gpus_on_this_host(host):
    gpus = []
    # Does this host has GPU card
    if host.config.graphicsInfo is None:
        return gpus

    for gpu in host.config.graphicsInfo:
        if "nvidia" in gpu.vendorName.lower():
            gpus.append(gpu)

    return gpus


def get_gpu_frozen_vm_list(frozen_vm_pool_name):
    pyvmomi_sdk_provider = get_sdk_provider(ClientType.PYVMOMI_SDK)
    frozen_vm_pool = pyvmomi_sdk_provider.get_pyvmomi_obj_by_name(
        [vim.ResourcePool], frozen_vm_pool_name
    )
    gpu_frozen_vms = []
    if not frozen_vm_pool.vm:
        return gpu_frozen_vms

    host_gpu_avail_status = {}
    for vm in frozen_vm_pool.vm:
        host = vm.runtime.host
        if host.name in host_gpu_avail_status:
            if host_gpu_avail_status[host.name]:
                gpu_frozen_vms.append(vm)
            continue

        gpus = get_supported_gpus_on_this_host(host)

        if not is_any_gpu_avail_on_this_host(host, gpus):
            host_gpu_avail_status[host.name] = False
            continue
        else:
            host_gpu_avail_status[host.name] = True

        gpu_frozen_vms.append(vm)

    return gpu_frozen_vms


def is_any_gpu_avail_for_this_vm(vm):
    gpus = get_supported_gpus_on_this_host(vm.runtime.host)
    return is_any_gpu_avail_on_this_host(vm.runtime.host, gpus)
