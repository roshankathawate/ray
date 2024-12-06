# Launching Ray Clusters on vSphere

This guide details the steps needed to launch a Ray cluster in a vSphere environment.

Ray integration with vSphere is offered as a vSphere WCP Supervisor Service.

## Prepare the vSphere environment 

If you don't already have a vSphere deployment, you can learn more about it by reading the [vSphere documentation](https://docs.vmware.com/en/VMware-vSphere/index.html). The vSphere Ray cluster launcher requires vSphere version 8.0 or later, along with the following prerequisites for creating Ray clusters.

* [A vSphere cluster](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-vcenter-esxi-management/GUID-F7818000-26E3-4E2A-93D2-FCDCE7114508.html) and [resource pools](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-resource-management/GUID-60077B40-66FF-4625-934A-641703ED7601.html) to host VMs composing Ray Clusters.
* A network port group (either for a [standard switch](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-networking/GUID-E198C88A-F82C-4FF3-96C9-E3DF0056AD0C.html) or [distributed switch](https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-networking/GUID-375B45C7-684C-4C51-BA3C-70E48DFABF04.html)) or an [NSX segment](https://docs.vmware.com/en/VMware-NSX/4.1/administration/GUID-316E5027-E588-455C-88AD-A7DA930A4F0B.html). VMs connected to this network should be able to obtain IP address via DHCP.
* A datastore that can be accessed by all the hosts in the vSphere cluster.
* vSphere Cluster with WCP deployed. Supervisor Cluster should be up and running in WCP environment,

Another way to prepare the vSphere environment is with VMware Cloud Foundation (VCF). VCF is a unified software-defined datacenter (SDDC) platform that seamlessly integrates vSphere, vSAN, and NSX into a natively integrated stack, delivering enterprise-ready cloud infrastructure for both private and public cloud environments. If you are using VCF, you can refer to the VCF documentation to  [create workload domains](https://docs.vmware.com/en/VMware-Cloud-Foundation/5.0/vcf-admin/GUID-3A478CF8-AFF8-43D9-9635-4E40A0E372AD.html) for running Ray Clusters. A VCF workload domain comprises one or more vSphere clusters, shared storage like vSAN, and a software-defined network managed by NSX. You can also [create NSX Edge Clusters using VCF](https://docs.vmware.com/en/VMware-Cloud-Foundation/5.0/vcf-admin/GUID-D17D0274-7764-43BD-8252-D9333CA7415A.html) and create segment for Ray VMs network.

* VI admin registers Ray Supervisor Service definition bundle into the Supervisor Services catalog, using either the vCenter web UI or CLI (e.g. dcli).
* VI Admin enables Ray Supervisor service on an existing Supervisor cluster (or enables Supervisor first on a vSphere cluster & then enables Ray Supervisor Service on it). 
* Enabling Ray Supervisor leads to a Ray operator pod starting up in it's own special namespace (e.g. `ray-system` namespace running a pod of ray-operator, that namespace is part of the supervisor service spec/config bundle), and leads to creation/registration of k8s CRDs in that Supervisor cluster. At this step, a Supervisor cluster is now capable of hosting an actual Ray cluster.

## Setting up Ray cluster using Kubectl
* DevOps then sees the available CRD, locate their assigned "work area" namespace (e.g. say `my-ml-project-namespace1`) and then uses k8s API (e.g. kubectl) to instantiates a Supervisor Service managed resource (e.g. say a custom resource object of `RayCluster` CRD) in their namespace. This leads to creation of Ray head & worker node VMs (by the RayService operator/controller pod in `ray-system` ns). Those VMs are vm-operator API `VirtualMachine` custom resource managed VMs and they are also part of the `my-ml-project-namespace1` namespace.

## Setting up Ray cluster using Aria Automation
* To enable Ray cluster creation via Aria Automation template approach, VI admin registers a cloud template in the Aria Automation Service Broker catalog. DevOps then accesses Service Broker catalog item UI to trigger deployment of a Ray cluster (effectively wiring up & exposing the Ray Supervisor Service CRDs through Aria Automation UX). 

## Setting up the cluster using Ray CLI

The Ray cluster launcher is part of the `ray` CLI. Use the CLI to start, stop and attach to a running ray cluster using commands such as `ray up`, `ray down` and `ray attach`. You can use pip to install the ray CLI with cluster launcher support. Follow [the Ray installation documentation](installation) for more detailed instructions. 

```bash
# install ray
pip install -U ray[default]
```

* To start a vSphere Ray cluster, it is required to configure the client environment.
Users have to perform kubectl vsphere login to the Supervisor cluster where the ray-on-vcf service is installed with devops user they have configured.

```bash
kubectl vsphere login  --server=<SUPERVISOR_CLUSTER_IP> --insecure-skip-tls-verify --vsphere-username <DEVOPS_USER_NAME> --tanzu-kubernetes-cluster-namespace <SUPERVISOR_USER_NAMEPACE>
```
Once the vsphere login to the Supervisor cluster server is successful, you should be ready to launch your cluster using the cluster launcher. The provided [cluster config file](https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/vsphere/example-full.yaml) will create a small cluster with a head node configured to autoscale to up to two workers.

Test that it works by running the following commands from your local machine:

```bash
# Download the example-full.yaml
wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/vsphere/example-full.yaml

# Login to the vSphere Supervisor cluster using credentials and kubernetes cluster namespace.
kubectl vsphere login  --server=<SUPERVISOR_CLUSTER_IP> --insecure-skip-tls-verify --vsphere-username <DEVOPS_USER_NAME> --tanzu-kubernetes-cluster-namespace <SUPERVISOR_USER_NAMEPACE>
```

# vi example-full.yaml

# Create or update the cluster.
ray up example-full.yaml

# Get a remote screen on the head node.
ray attach example-full.yaml

# Try running a Ray program.
python -c 'import ray; ray.init()'
exit

# Tear down the cluster.
ray down example-full.yaml
```

Congrats, you have started a Ray cluster on vSphere!

## Configure vSAN File Service as persistent storage for Ray AI Libraries

Starting in Ray 2.7, Ray AI Libraries (Train and Tune) will require users to provide a cloud storage or NFS path when running distributed training or tuning jobs. In a vSphere environment with a vSAN datastore, you can utilize the vSAN File Service feature to employ vSAN as a shared persistent storage. You can refer to [this vSAN File Service document](https://docs.vmware.com/en/VMware-vSphere/8.0/vsan-administration/GUID-CA9CF043-9434-454E-86E7-DCA9AD9B0C09.html) to create and configure NFS file shares supported by vSAN. The general steps are as follows:

1. Enable vSAN File Service and configure it with domain information and IP address pools.
2. Create a vSAN file share with NFS as the protocol.
3. View the file share information to get NFS export path.

Once a file share is created, you can mount it into the head and worker node and use the mount path as the `storage_path` for the `RunConfig` parameter in Ray Train and Tune. Please refer to [this example YAML](https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/vsphere/example-vsan-file-service.yaml) as a template on how to mount and configure the path. You will need to modify the NFS export path in the `initialization_commands` list and bind the mounted path within the Ray container. In this example, you will need to put `/mnt/shared_stos
rage/experiment_results` as the `storage_path` for `RunConfig`.
