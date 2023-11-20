import atexit
import ssl
import sys
from datetime import datetime, timezone

from pyVim.connect import Disconnect, SmartStubAdapter, VimSessionOrientedStub
from pyVmomi import vim


def get_service_instance(server, port, user, pwd):
    credentials = VimSessionOrientedStub.makeUserLoginMethod(user, pwd)
    smart_stub = SmartStubAdapter(
        host=server,
        port=port,
        sslContext=ssl._create_unverified_context(),
        connectionPoolTimeout=120,
    )
    session_stub = VimSessionOrientedStub(smart_stub, credentials)
    si = vim.ServiceInstance("ServiceInstance", session_stub)
    if not si:
        print("Could not connect to the specified host")
        sys.exit(1)

    atexit.register(Disconnect, si)

    return si


def delete_vm(vm):
    vm_name = vm.summary.config.name
    if vm.runtime.powerState == "poweredOn":
        print(f"Poweroff virtual machine {vm_name}")
        vm.PowerOffVM_Task()
    print(f"Deleted virtual machine {vm_name}")
    vm.Destroy_Task()


def delete_vms_in_resourcepool(server, port, user, pwd, resource_pool_name):
    si = get_service_instance(server, port, user, pwd)

    # Get the root folder of the inventory
    root_folder = si.content.rootFolder

    # Create a container view for all resource pools
    container = si.content.viewManager.CreateContainerView(
        root_folder, [vim.ResourcePool], True
    )

    # Loop through the resource pools and find the one with the given name
    for rp in container.view:
        if rp.name == resource_pool_name:
            # Get the list of virtual machines in the resource pool
            vms = rp.vm
            # Print the name and power state of each virtual machine
            for vm in vms:
                delete_vm(vm)
            # Break the loop
            break
    else:
        # If no resource pool with the given name is found, print a message
        print("No resource pool with the name {} found".format(resource_pool_name))


def delete_outdated_vms(server, port, user, pwd, prefix, keep_days):
    si = get_service_instance(server, port, user, pwd)
    content = si.RetrieveContent()
    obj_view = content.viewManager.CreateContainerView(
        content.rootFolder, [vim.VirtualMachine], True
    )
    vm_list = obj_view.view
    obj_view.Destroy()
    for vm in vm_list:
        if (
            vm.name.startswith(prefix)
            and (datetime.now(timezone.utc) - vm.config.createDate).days > keep_days
        ):
            delete_vm(vm)


if __name__ == "__main__":
    server, port, user, pwd, task_name = (
        sys.argv[1],
        int(sys.argv[2]),
        sys.argv[3],
        sys.argv[4],
        sys.argv[5],
    )
    if task_name == "D_VM_RP":
        print("Deleting VMs in a resource pool")
        resource_pool_name = sys.argv[6]
        delete_vms_in_resourcepool(server, port, user, pwd, resource_pool_name)
    elif task_name == "D_VM_OUTDATED":
        print("Deleting outdated VMs.")
        prefix = sys.argv[6]
        keep_days = int(sys.argv[7])
        delete_outdated_vms(server, port, user, pwd, prefix, keep_days)
    else:
        print("Not support tasks: " + task_name)
