import atexit
import ssl

from pyVim.connect import Disconnect, SmartStubAdapter, VimSessionOrientedStub
from pyVmomi import vim

from ray.autoscaler._private.vsphere.utils import Constants


class PyvmomiSdkProvider:
    def __init__(
        self,
        server,
        user,
        password,
        session_type: Constants.SessionType,
        port: int = 443,
    ):
        # Instance variables
        self.server = server
        self.user = user
        self.password = password
        self.session_type = session_type
        self.port = port

        # Instance parameters
        if self.session_type == Constants.SessionType.UNVERIFIED:
            context_obj = ssl._create_unverified_context()
        self.timeout = 0

        # Connect using a session oriented connection
        # Ref. https://github.com/vmware/pyvmomi/issues/347
        self.pyvmomi_sdk_client = None
        credentials = VimSessionOrientedStub.makeUserLoginMethod(user, password)
        smart_stub = SmartStubAdapter(
            host=server,
            port=port,
            sslContext=context_obj,
            connectionPoolTimeout=self.timeout,
        )
        self.session_stub = VimSessionOrientedStub(smart_stub, credentials)
        self.pyvmomi_sdk_client = vim.ServiceInstance(
            "ServiceInstance", self.session_stub
        )

        if not self.pyvmomi_sdk_client:
            raise ValueError("Could not connect to the specified host")
        atexit.register(Disconnect, self.pyvmomi_sdk_client)

    def get_pyvmomi_obj_by_moid(self, vimtype, moid):
        obj = None
        if self.pyvmomi_sdk_client is None:
            raise ValueError("Must init pyvmomi_sdk_client first.")

        container = self.pyvmomi_sdk_client.content.viewManager.CreateContainerView(
            self.pyvmomi_sdk_client.content.rootFolder, vimtype, True
        )

        for c in container.view:
            if moid:
                if moid in str(c):
                    obj = c
                    break
            else:
                obj = c
                break
        if not obj:
            raise RuntimeError(
                f"Unexpected: cannot find vSphere object {vimtype} with moid: {moid}"
            )
        return obj

    def get_pyvmomi_obj_by_name(self, vimtype, name):
        """
        This function finds the vSphere object by the object name and the object type.
        The object type can be "VM", "Host", "Datastore", etc.
        The object name is a unique name under the vCenter server.
        To check all such object information, you can go to the managed object board
        page of your vCenter Server, such as: https://<your_vc_ip/mob
        """
        obj = None
        if self.pyvmomi_sdk_client is None:
            raise ValueError("Must init pyvmomi_sdk_client first.")

        container = self.pyvmomi_sdk_client.content.viewManager.CreateContainerView(
            self.pyvmomi_sdk_client.content.rootFolder, vimtype, True
        )

        for c in container.view:
            if name:
                if c.name == name:
                    obj = c
                    break
            else:
                obj = c
                break
        if not obj:
            raise RuntimeError(
                f"Unexpected: cannot find vSphere object {vimtype} with name: {name}"
            )
        return obj
