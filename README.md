[![Documentation Status](https://readthedocs.org/projects/jupyterhub-forwardbasespawner/badge/?version=latest)](https://jupyterhub-forwardbasespawner.readthedocs.io/en/latest/?badge=latest)

# ForwardBaseSpawner

The ForwardBaseSpawner is a base class, which can be used by any Spawner that creats the singleuser notebook server on a remote system. The ForwardBaseSpawner itself cannot start anything. The actual logic of starting / stopping must be implemented in a subclass.
  
## Overview  
  
The JupyterHub ForwardBaseSpawner offers a variety of useful functions, to enable and manage the communication between JupyterHub and a singleuser notebook servers, which runs on a remote machine. It covers the following functionalities:

- Manage ssh port forwarding process from JupyterHub to singleuser notebook server
- Manage ssh port forwarding process from singleuser notebook server to JupyterHub
- Manage [Kubernetes Service](https://kubernetes.io/docs/concepts/services-networking/service/) Resource to make singleuser notebook server reachable for JupyterHub
- Adds cancel function. Allows the user to cancel an ongoing spawn process
- Start process of remote singleuser notebook servers may send their current progress to SpawnEventsAPIHandler, which shows it to the user
- SetupTunnelAPIHandler allows to define the ssh jump node between JupyterHub and singleuser notebook server during the spawn process
- ListServersAPIHandler returns all running UserID-servername combinations. Gives remote systems the chance to compare their running servers with the JupyterHub ones
- SSHNodeRestartedAPIHandler enables a recreation of all port forwarding process to a specific jump node. Required when a jump node was restarted
  
For more information and the usage have a look at the [JupyterHub OutpostSpawner](https://jupyterhub-outpostspawner.readthedocs.io) documentation. It's a subclass of the ForwardBaseSpawner and enables a central JupyterHub to start singleuser notebook-server on multiple remote systems.  
You can find a few examples and configurations over there.

## Requirements  
  
At least one JupyterHub running on a Kubernetes Cluster (recommended is the use of [Zero2JupyterHub](https://z2jh.jupyter.org/en/stable/)). 
A subclass of the ForwardBaseSpawner which implements the actual start/stop logic for singleuser notebook servers.
