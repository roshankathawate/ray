import logging

from pyVim.task import WaitForTask
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


def add_gpus_to_vm(vm_name: str, gpu_ids: list):
    """
    This function helps to add a list of gpu to a VM by PCI passthrough. Steps:
    1. Power off the VM if it is not at the off state.
    2. Construct a reconfigure spec and reconfigure the VM.
    3. Power on the VM.
    """
    pyvmomi_sdk_provider = get_sdk_provider(ClientType.PYVMOMI_SDK)
    vm_obj = pyvmomi_sdk_provider.get_pyvmomi_obj_by_name([vim.VirtualMachine], vm_name)
    # The VM is supposed to be at powered on status after instant clone.
    # We need to power it off.
    if vm_obj.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
        WaitForTask(vm_obj.PowerOffVM_Task())

    config_spec = vim.vm.ConfigSpec()

    # The below 2 advanced configs are needed for a VM to have a passthru PCI device
    config_spec.extraConfig = [
        vim.option.OptionValue(key="pciPassthru.64bitMMIOSizeGB", value="64"),
        vim.option.OptionValue(key="pciPassthru.use64bitMMIO", value="TRUE"),
    ]

    # PCI passthru device requires the memory to be hard reserved.
    config_spec.memoryReservationLockedToMax = True

    # add the GPUs into the reconfigure spec.
    config_spec.deviceChange = []

    # get the VM's plugable PCI devices
    pci_passthroughs = vm_obj.environmentBrowser.QueryConfigTarget(
        host=None
    ).pciPassthrough

    # The key is the id, such as '0000:3b:00.0'
    # The value is an instance of struct vim.vm.PciPassthroughInfo, please google it.
    id_to_pci_passthru_info = {item.pciDevice.id: item for item in pci_passthroughs}

    # The reason for this magic number -100 is following this page
    # https://gist.github.com/wiggin15/319b5e828c42af3aed40
    # The explanation can be found here:
    # https://vdc-download.vmware.com/vmwb-repository/dcr-public/
    # 790263bc-bd30-48f1-af12-ed36055d718b/e5f17bfc-ecba-40bf-a04f-376bbb11e811/
    # vim.vm.device.VirtualDevice.html
    key = -100
    for gpu_id in gpu_ids:
        pci_passthru_info = id_to_pci_passthru_info[gpu_id]
        backing = vim.VirtualPCIPassthroughDeviceBackingInfo(
            # This hex trick is what we must do to construct a backing info.
            # https://gist.github.com/wiggin15/319b5e828c42af3aed40
            # Otherwise the VM cannot be powered on.
            deviceId=hex(pci_passthru_info.pciDevice.deviceId % 2**16).lstrip("0x"),
            id=gpu_id,
            systemId=pci_passthru_info.systemId,
            vendorId=pci_passthru_info.pciDevice.vendorId,
            deviceName=pci_passthru_info.pciDevice.deviceName,
        )

        gpu = vim.VirtualPCIPassthrough(key=key, backing=backing)

        device_change = vim.vm.device.VirtualDeviceSpec(operation="add", device=gpu)

        config_spec.deviceChange.append(device_change)
        key += 1

    WaitForTask(vm_obj.ReconfigVM_Task(spec=config_spec))
    WaitForTask(vm_obj.PowerOnVM_Task())
