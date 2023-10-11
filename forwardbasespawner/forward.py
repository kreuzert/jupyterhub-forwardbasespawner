import asyncio
import copy
import inspect
import os
import re
import string
import subprocess
import time
import traceback
from datetime import datetime
from functools import lru_cache
from urllib.parse import urlparse

import escapism
from async_generator import aclosing
from jupyterhub.spawner import Spawner
from jupyterhub.utils import maybe_future
from jupyterhub.utils import random_port
from jupyterhub.utils import url_path_join
from kubernetes import client
from kubernetes import config
from tornado import web
from traitlets import Any
from traitlets import Bool
from traitlets import Callable
from traitlets import default
from traitlets import Dict
from traitlets import Integer
from traitlets import Unicode
from traitlets import Union


@lru_cache
def get_name(key):
    """Load value from the k8s ConfigMap given a key."""

    path = f"/usr/local/etc/jupyterhub/config/{key}"
    if os.path.exists(path):
        with open(path) as f:
            return f.read()
    else:
        raise Exception(f"{path} not found!")


class ForwardBaseSpawner(Spawner):
    """
    This class contains all configurables to create a
    port forwarding process to a remotely started JupyterHub.

    It is meant to be used within a Kubernetes Cluster
    with the python kubernetes API.
    """

    # Remote jupyterhub-singleuser servers might require a ssh port forward
    # to be reachable by jupyterhub. This dict will contain this information
    # ssh -i <key> -L <local_host>:<local_port>:<remote_host>:<remote_port> <user>@<node>
    #
    # Subclasses' _start() function should return this
    port_forward_info = {}
    port_forwarded = 0

    # When restarting JupyterHub, we might have to recreate the ssh tunnel.
    # This boolean is used in poll(), to check if it's the first function call
    # during the startup phase of JupyterHub. If that's the case, the ssh tunnels
    # might have to be restarted.
    call_during_startup = True

    # This is used to prevent multiple requests during the stop procedure.
    already_stopped = False
    already_post_stop_hooked = False

    # Keep track if an event with failed=False was yielded
    _cancel_event_yielded = False

    # Store events for max 24h.
    latest_events = []
    events = {}
    yield_wait_seconds = 1

    extra_labels = Union(
        [Dict(default_value={}), Callable()],
        help="""
        An optional hook function, or dict, that you can implement to add
        extra labels to the service created when using port-forward.
        Will also be forwarded to the outpost service (see self.custom_misc_disable_default)
        
        This maybe a coroutine.
        
        Example::

            def extra_labels(spawner):
                labels = {
                    "hub.jupyter.org/username": spawner.user.name,
                    "hub.jupyter.org/servername": spawner.name,
                    "sidecar.istio.io/inject": "false"
                }
                return labels
            
            c.OutpostSpawner.extra_labels = extra_labels
        """,
    ).tag(config=True)

    ssh_recreate_at_start = Union(
        [Callable(), Bool()],
        default_value=True,
        help="""
        Whether ssh tunnels should be recreated at JupyterHub start or not.
        If you have outsourced the port forwarding to an extra pod, you can
        set this to false. This also means, that running JupyterLabs are not
        affected by JupyterHub restarts.
        
        This maybe a coroutine.
        """,
    ).tag(config=True)

    ssh_during_startup = Union(
        [Callable(), Bool()],
        default_value=False,
        help="""
        An optional hook function, or boolean, that you can implement to
        decide whether a ssh port forwarding process should be run after
        the POST request to the JupyterHub outpost service.
        
        Common Use Case: 
        singleuser service was started remotely and is not accessible by
        JupyterHub (e.g. it's running on a different K8s Cluster), but you
        know exactly where it is (e.g. the service address).

        Example::

            def ssh_during_startup(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return True
                return False

            c.OutpostSpawner.ssh_during_startup = ssh_during_startup

        """,
    ).tag(config=True)

    ssh_key = Union(
        [Callable(), Unicode()],
        allow_none=True,
        default_value="/home/jovyan/.ssh/id_rsa",
        help="""
        An optional hook function, or string, that you can implement to
        set the ssh privatekey used for ssh port forwarding.

        This maybe a coroutine.

        Example::

            def ssh_key(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "/mnt/private_keys/a"
                return "/mnt/private_keys/b"

            c.OutpostSpawner.ssh_key = ssh_key

        """,
    ).tag(config=True)

    ssh_remote_key = Union(
        [Callable(), Unicode()],
        allow_none=True,
        default_value="/home/jovyan/.ssh/id_rsa_remote",
        help="""
        An optional hook function, or string, that you can implement to
        set the ssh privatekey used for ssh port forwarding remote.

        This maybe a coroutine.

        Example::

            def ssh_remote_key(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "/mnt/private_keys/a"
                return "/mnt/private_keys/b"

            c.OutpostSpawner.ssh_remote_key = ssh_remote_key

        """,
    ).tag(config=True)

    ssh_username = Union(
        [Callable(), Unicode()],
        default_value="jupyterhuboutpost",
        help="""
        An optional hook function, or string, that you can implement to
        set the ssh username used for ssh port forwarding.

        This maybe a coroutine.

        Example::

            def ssh_username(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "jupyterhuboutpost"
                return "ubuntu"

            c.OutpostSpawner.ssh_username = ssh_username

        """,
    ).tag(config=True)

    ssh_remote_username = Union(
        [Callable(), Unicode()],
        default_value="jupyterhuboutpost",
        help="""
        An optional hook function, or string, that you can implement to
        set the ssh username used for ssh port forwarding remote.

        This maybe a coroutine.

        Example::

            def ssh_username(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "jupyterhuboutpost"
                return "ubuntu"

            c.OutpostSpawner.ssh_remote_username = ssh_username

        """,
    ).tag(config=True)

    ssh_node = Union(
        [Callable(), Unicode()],
        allow_none=True,
        default_value=None,
        help="""
        An optional hook function, or string, that you can implement to
        set the ssh node used for ssh port forwarding.

        This maybe a coroutine.

        Example::

            def ssh_node(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "outpost.namespace.svc"
                else:
                    return "<public_ip>"

            c.OutpostSpawner.ssh_node = ssh_node

        """,
    ).tag(config=True)

    ssh_remote_node = Union(
        [Callable(), Unicode()],
        allow_none=True,
        default_value=None,
        help="""
        An optional hook function, or string, that you can implement to
        set the ssh node used for ssh port forwarding remote.

        This maybe a coroutine.

        Example::

            def ssh_node(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "outpost.namespace.svc"
                else:
                    return "<public_ip>"

            c.OutpostSpawner.ssh_remote_node = ssh_node

        """,
    ).tag(config=True)

    ssh_port = Union(
        [Callable(), Integer(), Unicode()],
        default_value=22,
        help="""
        An optional hook function, or string, that you can implement to
        set the ssh port used for ssh port forwarding.

        This maybe a coroutine.

        Example::

            def ssh_port(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return 22
                else:
                    return 2222

            c.OutpostSpawner.ssh_port = ssh_port

        """,
    ).tag(config=True)

    ssh_remote_port = Union(
        [Callable(), Integer(), Unicode()],
        default_value=22,
        help="""
        An optional hook function, or string, that you can implement to
        set the ssh port used for ssh port forwarding remote.

        This maybe a coroutine.

        Example::

            def ssh_port(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return 22
                else:
                    return 2222

            c.OutpostSpawner.ssh_remote_port = ssh_port

        """,
    ).tag(config=True)

    ssh_custom_forward_remote = Any(
        help="""
        An optional hook function that you can implement to create your own
        ssh port forwarding from remote system to hub. 
        """
    ).tag(config=True)

    ssh_custom_forward_remote_remove = Any(
        help="""
        An optional hook function that you can implement to remove your own
        ssh port forwarding from remote system to hub.
        """
    ).tag(config=True)

    ssh_create_remote_forward = Any(
        default_value=False,
        help="""
        Whether a port forwarding process from a remote system to the hub is 
        required or not. The remote system must be prepared properly to support
        this feature. 
        
        Must be a boolean or a callable function
        """,
    ).tag(config=True)

    async def get_ssh_create_remote_forward(self):
        if callable(self.ssh_create_remote_forward):
            ssh_create_remote_forward = self.ssh_create_remote_forward(
                self, self.port_forward_info.get("remote", {})
            )
            if inspect.isawaitable(ssh_create_remote_forward):
                ssh_create_remote_forward = await ssh_create_remote_forward
        else:
            ssh_create_remote_forward = self.ssh_create_remote_forward
        return ssh_create_remote_forward

    ssh_custom_forward = Any(
        help="""
        An optional hook function that you can implement to create your own
        ssh port forwarding. This can be used to use an external pod
        for the port forwarding. 
        
        Example::

            from tornado.httpclient import HTTPRequest
            def ssh_custom_forward(spawner, port_forward_info):
                url = "..."
                headers = {
                    ...
                }
                req = HTTPRequest(
                    url=url,
                    method="POST",
                    headers=headers,
                    body=json.dumps(port_forward_info),                    
                )
                await spawner.send_request(
                    req, action="setuptunnel"
                )

            c.OutpostSpawner.ssh_custom_forward = ssh_custom_forward

        """
    ).tag(config=True)

    ssh_custom_forward_remove = Any(
        help="""
        An optional hook function that you can implement to remove your own
        ssh port forwarding. This can be used to use an external pod
        for the port forwarding. 
        
        Example::

            from tornado.httpclient import HTTPRequest
            def ssh_custom_forward_remove(spawner, port_forward_info):
                url = "..."
                headers = {
                    ...
                }
                req = HTTPRequest(
                    url=url,
                    method="DELETE",
                    headers=headers,
                    body=json.dumps(port_forward_info),                    
                )
                await spawner.send_request(
                    req, action="removetunnel"
                )

            c.OutpostSpawner.ssh_custom_forward_remove = ssh_custom_forward_remove

        """
    ).tag(config=True)

    ssh_custom_svc = Any(
        help="""
        An optional hook function that you can implement to create a customized
        kubernetes svc. 
        
        Example::

            def ssh_custom_svc(spawner, port_forward_info):
                ...
                return spawner.pod_name, spawner.port

            c.OutpostSpawner.ssh_custom_svc = ssh_custom_svc

        """
    ).tag(config=True)

    ssh_custom_svc_remove = Any(
        help="""
        An optional hook function that you can implement to remove a customized
        kubernetes svc. 
        
        Example::

            def ssh_custom_svc_remove(spawner, port_forward_info):
                ...
                return spawner.pod_name, spawner.port

            c.OutpostSpawner.ssh_custom_svc_remove = ssh_custom_svc_remove

        """
    ).tag(config=True)

    ssh_forward_options = Union(
        [Dict(default_value={}), Callable()],
        help="""
        An optional hook, or dict, to configure the ssh commands used in the
        spawner.ssh_default_forward function. The default configuration parameters
        (see below) can be overriden.
        
        Default::

            ssh_forward_options_all = {
                "ServerAliveInterval": "15",
                "StrictHostKeyChecking": "accept-new",
                "ControlMaster": "auto",
                "ControlPersist": "yes",
                "Port": str(ssh_port),
                "ControlPath": f"/tmp/control_{ssh_address_or_host}",
                "IdentityFile": ssh_pkey,
            }        
        
        """,
    ).tag(config=True)

    async def get_ssh_forward_options(self):
        if callable(self.ssh_forward_options):
            ssh_forward_options = self.ssh_forward_options(self, self.port_forward_info)
            if inspect.isawaitable(ssh_forward_options):
                ssh_forward_options = await ssh_forward_options
        else:
            ssh_forward_options = self.ssh_forward_options
        return ssh_forward_options

    ssh_forward_remote_options = Union(
        [Dict(default_value={}), Callable()],
        help="""
        An optional hook, or dict, to configure the ssh commands used in the
        spawner.ssh_default_forward function. The default configuration parameters
        (see below) can be overriden.
        
        Default::

            ssh_forward_remote_options_all = {
                "ServerAliveInterval": "15",
                "StrictHostKeyChecking": "accept-new",
                "ControlMaster": "auto",
                "ControlPersist": "yes",
                "Port": str(ssh_port),
                "ControlPath": f"/tmp/control_{ssh_address_or_host}",
                "IdentityFile": ssh_pkey,
            }        
        
        """,
    ).tag(config=True)

    async def get_ssh_forward_remote_options(self):
        if callable(self.ssh_forward_remote_options):
            ssh_forward_remote_options = self.ssh_forward_remote_options(
                self, self.port_forward_info.get("remote", {})
            )
            if inspect.isawaitable(ssh_forward_remote_options):
                ssh_forward_remote_options = await ssh_forward_remote_options
        else:
            ssh_forward_remote_options = self.ssh_forward_remote_options
        return ssh_forward_remote_options

    def run_pre_spawn_hook(self):
        if self.already_stopped:
            raise Exception("Server is in the process of stopping, please wait.")
        """Run the pre_spawn_hook if defined"""
        if self.pre_spawn_hook:
            return self.pre_spawn_hook(self)

    def run_post_stop_hook(self):
        if self.already_post_stop_hooked:
            return
        self.already_post_stop_hooked = True

        """Run the post_stop_hook if defined"""
        if self.post_stop_hook is not None:
            try:
                return self.post_stop_hook(self)
            except Exception:
                self.log.exception("post_stop_hook failed with exception: %s", self)

    def get_env(self):
        """Get customized environment variables

        Returns:
          env (dict): Used in communication with outpost service.
        """
        env = super().get_env()

        env["JUPYTERHUB_API_URL"] = self.public_api_url.rstrip("/")
        env[
            "JUPYTERHUB_ACTIVITY_URL"
        ] = f"{env['JUPYTERHUB_API_URL']}/users/{self.user.name}/activity"

        # Add URL to manage ssh tunnels
        url_parts = ["users", "setuptunnel", self.user.escaped_name]
        if self.name:
            url_parts.append(self.name)
        env[
            "JUPYTERHUB_SETUPTUNNEL_URL"
        ] = f"{env['JUPYTERHUB_API_URL']}/{url_path_join(*url_parts)}"

        url_parts = ["users", "progress", "events", self.user.escaped_name]
        if self.name:
            url_parts.append(self.name)
        env[
            "JUPYTERHUB_EVENTS_URL"
        ] = f"{env['JUPYTERHUB_API_URL']}/{url_path_join(*url_parts)}"

        if self.internal_ssl:
            proto = "https://"
        else:
            proto = "http://"
        env[
            "JUPYTERHUB_SERVICE_URL"
        ] = f"{proto}0.0.0.0:{self.port}/user/{self.user.name}/{self.name}/"

        return env

    async def get_extra_labels(self):
        """Get extra labels

        Returns:
          extra_labels (dict): Used in custom_misc and in default svc.
                               Labels are used in svc and remote pod.
        """
        if callable(self.extra_labels):
            extra_labels = await maybe_future(self.extra_labels(self))
        else:
            extra_labels = self.extra_labels

        return extra_labels

    def get_state(self):
        """get the current state"""
        state = super().get_state()
        state["port_forward_info"] = self.port_forward_info
        state["port"] = self.port
        if self.events:
            if type(self.events) != dict:
                self.events = {}
            self.events["latest"] = self.latest_events
            # Clear logs older than 24h or empty logs
            events_keys = copy.deepcopy(list(self.events.keys()))
            for key in events_keys:
                value = self.events.get(key, None)
                if value and len(value) > 0 and value[0]:
                    stime = self._get_event_time(value[0])
                    dtime = datetime.strptime(stime, "%Y_%m_%d %H:%M:%S")
                    now = datetime.now()
                    delta = now - dtime
                    if delta.days:
                        del self.events[key]
                else:  # empty logs
                    del self.events[key]
            state["events"] = self.events
        return state

    def load_state(self, state):
        """load state from the database"""
        super().load_state(state)
        if "port_forward_info" in state:
            self.port_forward_info = state["port_forward_info"]
        if "events" in state:
            self.events = state["events"]
            if "latest" in self.events:
                self.latest_events = self.events["latest"]
        if "port" in state:
            self.port = state["port"]

    def clear_state(self):
        """clear any state (called after shutdown)"""
        super().clear_state()
        self._start_future = None
        self._start_future_response = None
        self.port_forward_info = {}
        self.already_stopped = False
        self.already_post_stop_hooked = False
        self._cancel_event_yielded = False

    async def _generate_progress(self):
        """Private wrapper of progress generator

        This method is always an async generator and will always yield at least one event.
        """
        if not self._spawn_pending:
            self.log.warning(
                "Spawn not pending, can't generate progress for %s", self._log_name
            )
            return

        # yield {"progress": 0, "message": "Server requested"}

        async with aclosing(self.progress()) as progress:
            async for event in progress:
                yield event

    filter_events = Callable(
        allow_none=True,
        default_value=None,
        help="""
        Different JupyterHub single-user servers may send different events.
        This filter allows you to unify all events. Should always return a dict.
        If the dict should not be shown return an empty dict.
                
        Example::

            def custom_filter_events(spawner, event):
                event["html_message"] = event.get("message", "No message available")
                return event

            c.EventOutpostSpawner.filter_events = custom_filter_events
        """,
    ).tag(config=True)

    def run_filter_events(self, event):
        if self.filter_events:
            event = self.filter_events(self, event)
        return event

    cancelling_event = Union(
        [Dict(), Callable()],
        default_value={
            "failed": False,
            "ready": False,
            "progress": 99,
            "message": "",
            "html_message": "JupyterLab is cancelling the start.",
        },
        help="""
        Event shown when singleuser server was cancelled.
        Can be a function or a dict.
        
        This may be a coroutine.
        
        Example::

            from datetime import datetime
            async def cancel_click_event(spawner):
                now = datetime.now().strftime("%Y_%m_%d %H:%M:%S.%f")[:-3]
                return {
                    "failed": False,
                    "ready": False,
                    "progress": 99,
                    "message": "",
                    "html_message": f"<details><summary>{now}: Cancelling start ...</summary>We're stopping the start process.</details>",
                }
        
            c.EventOutpostSpawner.cancelling_event = cancel_click_event
        """,
    ).tag(config=True)

    async def get_cancelling_event(self):
        """Get cancelling event.
        This event will be shown while cancelling/stopping the server

        Returns:
          cancelling_event (dict)
        """
        if callable(self.cancelling_event):
            cancelling_event = await maybe_future(self.cancelling_event(self))
        else:
            cancelling_event = self.cancelling_event
        return cancelling_event

    stop_event = Union(
        [Dict(), Callable()],
        default_value={
            "failed": True,
            "ready": False,
            "progress": 100,
            "message": "",
            "html_message": "JupyterLab was stopped.",
        },
        help="""
        Event shown when single-user server was stopped.
        """,
    ).tag(config=True)

    async def get_stop_event(self):
        if callable(self.stop_event):
            stop_event = await maybe_future(self.stop_event(self))
        else:
            stop_event = self.stop_event
        return stop_event

    def _get_event_time(self, event):
        # Regex for date time
        pattern = re.compile(
            r"([0-9]+(_[0-9]+)+).*[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,3})?"
        )
        message = event["html_message"]
        match = re.search(pattern, message)
        return match.group()

    async def get_ssh_recreate_at_start(self):
        """Get ssh_recreate_at_start

        Returns:
          ssh_recreate_at_start (bool): Restart ssh tunnels if hub was restarted
        """
        if callable(self.ssh_recreate_at_start):
            ssh_recreate_at_start = await maybe_future(self.ssh_recreate_at_start(self))
        else:
            ssh_recreate_at_start = self.ssh_recreate_at_start
        return ssh_recreate_at_start

    async def get_ssh_port(self):
        """Get ssh port

        Returns:
          ssh_port (int): Used in ssh forward command. Default is 22
        """
        if callable(self.ssh_port):
            ssh_port = await maybe_future(self.ssh_port(self, self.port_forward_info))
        else:
            ssh_port = self.port_forward_info.get("ssh_port", self.ssh_port)
        return ssh_port

    async def get_ssh_remote_port(self):
        """Get ssh port

        Returns:
          ssh_port (int): Used in ssh forward command. Default is 22
        """
        if callable(self.ssh_remote_port):
            ssh_remote_port = await maybe_future(
                self.ssh_remote_port(self, self.port_forward_info.get("remote", {}))
            )
        else:
            ssh_remote_port = self.port_forward_info.get("remote", {}).get(
                "ssh_port", self.ssh_remote_port
            )
        return ssh_remote_port

    async def get_ssh_username(self):
        """Get ssh username

        Returns:
          ssh_user (string): Used in ssh forward command. Default ist "jupyterhuboutpost"
        """
        if callable(self.ssh_username):
            ssh_user = await maybe_future(
                self.ssh_username(self, self.port_forward_info)
            )
        else:
            ssh_user = self.port_forward_info.get("ssh_username", self.ssh_username)
        return ssh_user

    async def get_ssh_remote_username(self):
        """Get ssh username

        Returns:
          ssh_remote_username (string): Used in ssh forward command. Default ist "None"
        """
        if callable(self.ssh_remote_username):
            ssh_remote_username = await maybe_future(
                self.ssh_remote_username(self, self.port_forward_info.get("remote", {}))
            )
        else:
            ssh_remote_username = self.port_forward_info.get("remote", {}).get(
                "ssh_username", self.ssh_remote_username
            )
        return ssh_remote_username

    async def get_ssh_key(self):
        """Get ssh key

        Returns:
          ssh_key (string): Path to ssh privatekey used in ssh forward command"""
        if callable(self.ssh_key):
            ssh_key = await maybe_future(self.ssh_key(self, self.port_forward_info))
        else:
            ssh_key = self.port_forward_info.get("ssh_key", self.ssh_key)
        return ssh_key

    async def get_ssh_remote_key(self):
        """Get ssh remote key

        Returns:
          ssh_remote_key (string): Path to ssh privatekey used in ssh forward remote command
        """
        if callable(self.ssh_remote_key):
            ssh_remote_key = await maybe_future(
                self.ssh_remote_key(self, self.port_forward_info.get("remote", {}))
            )
        else:
            ssh_remote_key = self.port_forward_info.get("remote", {}).get(
                "ssh_key", self.ssh_remote_key
            )
        return ssh_remote_key

    def get_ssh_during_startup(self):
        """Get ssh enabled

        Returns:
          ssh_during_startup (bool): Create ssh port forwarding after successful POST request
                              to outpost service, if true

        """
        if callable(self.ssh_during_startup):
            ssh_during_startup = self.ssh_during_startup(self)
        else:
            ssh_during_startup = self.ssh_during_startup
        return ssh_during_startup

    async def get_ssh_node(self):
        """Get ssh node

        Returns:
          ssh_node (string): Used in ssh port forwading command
        """

        if callable(self.ssh_node):
            ssh_node = await maybe_future(self.ssh_node(self, self.port_forward_info))
        else:
            ssh_node = self.port_forward_info.get("ssh_node", self.ssh_node)
        return ssh_node

    async def get_ssh_remote_node(self):
        """Get ssh node

        Returns:
          ssh_remote_node (string): Used in ssh port forwading remote command
        """

        if callable(self.ssh_remote_node):
            ssh_remote_node = await maybe_future(
                self.ssh_node(self, self.port_forward_info.get("remote", {}))
            )
        else:
            ssh_remote_node = self.port_forward_info.get("remote", {}).get(
                "ssh_node", self.ssh_remote_node
            )
        return ssh_remote_node

    async def run_ssh_forward(self, create_svc=True):
        """Run the custom_create_port_forward if defined, otherwise run the default one"""
        try:
            if self.ssh_custom_forward:
                port_forward = self.ssh_custom_forward(self, self.port_forward_info)
                if inspect.isawaitable(port_forward):
                    await port_forward
            else:
                await self.ssh_default_forward()
        except Exception as e:
            raise web.HTTPError(
                419,
                log_message=f"Cannot start ssh tunnel for {self.name}: {str(e)}",
                reason=traceback.format_exc(),
            )

        if create_svc:
            try:
                if self.ssh_custom_svc:
                    ssh_custom_svc = self.ssh_custom_svc(self, self.port_forward_info)
                    if inspect.isawaitable(ssh_custom_svc):
                        ssh_custom_svc = await ssh_custom_svc
                    return ssh_custom_svc
                else:
                    return await self.ssh_default_svc()
            except Exception as e:
                raise web.HTTPError(
                    419,
                    log_message=f"Cannot create svc for {self._log_name}: {str(e)}",
                    reason=traceback.format_exc(),
                )

    async def get_forward_cmd(self, extra_args=["-f", "-N", "-n"]):
        """Get base options for ssh port forwarding

        Returns:
          (string, string, list): (ssh_user, ssh_node, base_cmd) to be used in ssh
                                  port forwarding cmd like:
                                  <base_cmd> -L0.0.0.0:port:address:port <ssh_user>@<ssh_node>

        """
        ssh_port = await self.get_ssh_port()
        ssh_username = await self.get_ssh_username()
        ssh_address_or_host = await self.get_ssh_node()
        ssh_pkey = await self.get_ssh_key()

        ssh_forward_options_all = {
            "ServerAliveInterval": "15",
            "StrictHostKeyChecking": "accept-new",
            "ControlMaster": "auto",
            "ControlPersist": "yes",
            "Port": str(ssh_port),
            "ControlPath": f"/tmp/control_{ssh_address_or_host}",
            "IdentityFile": ssh_pkey,
        }

        custom_forward_options = await self.get_ssh_forward_options()
        ssh_forward_options_all.update(custom_forward_options)
        ssh_forward_options_all.update(
            self.port_forward_info.get("ssh_forward_options", {})
        )

        cmd = ["ssh"]
        cmd.extend(extra_args)
        for key, value in ssh_forward_options_all.items():
            cmd.append(f"-o{key}={value}")
        return ssh_username, ssh_address_or_host, cmd

    async def get_forward_remote_cmd(self, extra_args=["-f", "-N", "-n"]):
        """Get base options for ssh port forwarding

        Returns:
          (string, string, list): (ssh_user, ssh_node, base_cmd) to be used in ssh
                                  remote port forwarding cmd like:
                                  <base_cmd> <ssh_user>@<ssh_node> [start|stop|status]

        """
        ssh_port = await self.get_ssh_remote_port()
        ssh_username = await self.get_ssh_remote_username()
        ssh_address_or_host = await self.get_ssh_remote_node()
        ssh_pkey = await self.get_ssh_remote_key()

        ssh_forward_options_all = {
            "ServerAliveInterval": "15",
            "StrictHostKeyChecking": "accept-new",
            "ControlMaster": "auto",
            "ControlPersist": "yes",
            "Port": str(ssh_port),
            "ControlPath": f"/tmp/control_remote_{ssh_address_or_host}",
            "IdentityFile": ssh_pkey,
        }

        custom_forward_remote_options = await self.get_ssh_forward_remote_options()
        ssh_forward_options_all.update(custom_forward_remote_options)
        ssh_forward_options_all.update(
            self.port_forward_info.get("remote", {}).get("ssh_forward_options", {})
        )

        cmd = ["ssh"]
        cmd.extend(extra_args)
        for key, value in ssh_forward_options_all.items():
            cmd.append(f"-o{key}={value}")
        return ssh_username, ssh_address_or_host, cmd

    def subprocess_cmd(self, cmd, timeout=3):
        """Execute bash cmd via subprocess.Popen as user 1000

        Returns:
          returncode (int): returncode of cmd
        """

        def set_uid():
            try:
                os.setuid(1000)
            except:
                pass

        self.log.info(f"SSH cmd: {' '.join(cmd)}")
        p = subprocess.Popen(
            cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, preexec_fn=set_uid
        )
        try:
            out, err = p.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as e:
            p.kill()
            raise e
        return p.returncode, out, err

    def split_service_address(self, service_address):
        service_address_port = service_address.removeprefix("https://").removeprefix(
            "http://"
        )
        service_address_short, port = service_address_port.split(":")
        return service_address_short, port

    async def ssh_default_forward_remove(self):
        """Default function to remove previously created port forward."""
        service_address, service_port = self.split_service_address(
            self.port_forward_info.get("service")
        )
        user, node, cmd = await self.get_forward_cmd()
        cancel_cmd = cmd.copy()
        cancel_cmd.extend(
            [
                "-O",
                "cancel",
                f"-L0.0.0.0:{self.port}:{service_address}:{service_port}",
                f"{user}@{node}",
            ]
        )
        self.subprocess_cmd(cancel_cmd)

    async def ssh_default_forward(self):
        """Default function to create port forward.
        Forwards 0.0.0.0:{self.port} to {service_address}:{service_port} within
        the hub container. Uses ssh multiplex feature to reduce open connections

        Returns:
          None
        """
        # check if ssh multiplex connection is up
        user, node, cmd = await self.get_forward_cmd()
        check_cmd = cmd.copy()
        check_cmd.extend(["-O", "check", f"{user}@{node}"])
        returncode, out, err = self.subprocess_cmd(check_cmd)

        if returncode != 0:
            # Create multiplex connection
            connect_cmd = cmd.copy()
            connect_cmd.append(f"{user}@{node}")

            # First creation always runs in a timeout. Expect this and check
            # the success with check_cmd again
            try:
                returncode, out, err = self.subprocess_cmd(connect_cmd, timeout=1)
            except subprocess.TimeoutExpired as e:
                returncode, out, err = self.subprocess_cmd(check_cmd)

            if returncode != 0:
                raise Exception(
                    f"Could not create ssh connection ({connect_cmd}) (Returncode: {returncode} != 0). Stdout: {out}. Stderr: {err}"
                )

        service_address, service_port = self.split_service_address(
            self.port_forward_info.get("service")
        )
        create_cmd = cmd.copy()
        create_cmd.extend(
            [
                "-O",
                "forward",
                f"-L0.0.0.0:{self.port}:{service_address}:{service_port}",
                f"{user}@{node}",
            ]
        )

        returncode, out, err = self.subprocess_cmd(create_cmd)
        if returncode != 0:
            # Maybe there's an old forward still running for this
            cancel_cmd = cmd.copy()
            cancel_cmd.extend(
                [
                    "-O",
                    "cancel",
                    f"-L0.0.0.0:{self.port}:{service_address}:{service_port}",
                    f"{user}@{node}",
                ]
            )
            self.subprocess_cmd(cancel_cmd)

            returncode, out, err = self.subprocess_cmd(create_cmd)
            if returncode != 0:
                raise Exception(
                    f"Could not forward port ({create_cmd}) (Returncode: {returncode} != 0). Stdout: {out}. Stderr: {err}"
                )

    async def ssh_default_forward_remote_remove(self):
        """Default function to remove previously created remote port forward."""
        service_address, service_port = self.split_service_address(
            self.port_forward_info.get("service")
        )
        user, node, cmd = await self.get_forward_remote_cmd()
        stop_cmd = cmd.copy()
        stop_cmd.extend([f"{user}@{node}", "stop"])
        self.subprocess_cmd(stop_cmd)

    async def ssh_default_forward_remote(self):
        """Default function to create port forward.
        Forwards 0.0.0.0:{self.port} to {service_address}:{service_port} within
        the hub container. Uses ssh multiplex feature to reduce open connections

        Returns:
          None
        """
        # check if ssh multiplex connection is up
        user, node, cmd = await self.get_forward_remote_cmd()
        check_cmd = cmd.copy()
        check_cmd.extend(["-O", "check", f"{user}@{node}"])
        returncode, out, err = self.subprocess_cmd(check_cmd)

        if returncode != 0:
            # Create multiplex connection
            connect_cmd = cmd.copy()
            connect_cmd.append(f"{user}@{node}")

            # First creation always runs in a timeout. Expect this and check
            # the success with check_cmd again
            try:
                returncode, out, err = self.subprocess_cmd(connect_cmd, timeout=1)
            except subprocess.TimeoutExpired as e:
                returncode, out, err = self.subprocess_cmd(check_cmd)

            if returncode != 0:
                raise Exception(
                    f"Could not create remote ssh connection ({connect_cmd}) (Returncode: {returncode} != 0). Stdout: {out}. Stderr: {err}"
                )

        start_cmd.extend([f"{user}@{node}", "start"])
        returncode, out, err = self.subprocess_cmd(start_cmd)
        if returncode != 217:
            raise Exception(
                f"Could not create remote forward port ({start_cmd}) (Returncode: {returncode} != 0). Stdout: {out}. Stderr: {err}"
            )

    def _k8s_get_client_core(self):
        """Get python kubernetes API client"""
        config.load_incluster_config()
        return client.CoreV1Api()

    async def ssh_default_svc(self):
        """Create Kubernetes Service.
        Selector: the hub container itself
        Port + targetPort: self.port

        Removes existing services with the same name, to create a new one.

        Returns:
          (string, int): (self.svc_name, self.port)
        """

        v1 = self._k8s_get_client_core()

        hub_svc = v1.read_namespaced_service(
            name=get_name("hub"), namespace=os.environ.get("POD_NAMESPACE")
        )
        hub_selector = hub_svc.to_dict()["spec"]["selector"]

        labels = hub_selector.copy()
        labels["component"] = "singleuser-server"
        extra_labels = await self.get_extra_labels()
        labels.update(extra_labels)

        service_manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "labels": labels,
                "name": self.svc_name,
                "resourceversion": "v1",
            },
            "spec": {
                "ports": [
                    {
                        "name": "http",
                        "port": self.port,
                        "protocol": "TCP",
                        "targetPort": self.port,
                    }
                ],
                "selector": hub_selector,
            },
        }
        try:
            v1.create_namespaced_service(
                body=service_manifest, namespace=self.namespace
            )
        except client.exceptions.ApiException as e:
            status_code = getattr(e, "status", 500)
            if status_code == 409:
                v1.delete_namespaced_service(
                    name=self.svc_name, namespace=self.namespace
                )
                v1.create_namespaced_service(
                    body=service_manifest, namespace=self.namespace
                )
            else:
                raise e
        return self.svc_name, self.port

    async def ssh_default_svc_remove(self):
        """Remove Kubernetes Service
        Used parameters: self.svc_name and self.namespace

        Returns:
          None
        """
        v1 = self._k8s_get_client_core()
        name = self.svc_name
        v1.delete_namespaced_service(name=name, namespace=self.namespace)

    async def run_ssh_forward_remove(self):
        """Run the custom_create_port_forward if defined, else run the default one"""
        try:
            if self.ssh_custom_forward_remove:
                port_forward_stop = self.ssh_custom_forward_remove(
                    self, self.port_forward_info
                )
                if inspect.isawaitable(port_forward_stop):
                    await port_forward_stop
            else:
                await self.ssh_default_forward_remove()
        except:
            self.log.exception("Could not cancel port forwarding")
        try:
            if self.ssh_custom_svc_remove:
                ssh_custom_svc_remove = self.ssh_custom_svc_remove(
                    self, self.port_forward_info
                )
                if inspect.isawaitable(ssh_custom_svc_remove):
                    ssh_custom_svc_remove = await ssh_custom_svc_remove
            else:
                await self.ssh_default_svc_remove()
        except:
            self.log.exception("Could not delete port forwarding svc")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._start_future = None
        self._start_future_response = None
        self.svc_name = self._expand_user_properties(self.svc_name_template)
        self.dns_name = self.dns_name_template.format(
            namespace=self.namespace, name=self.svc_name
        )

    public_api_url = Unicode(
        help="""
        Singleuser servers started remotely may have to use a different api_url than
        the default internal one. This will overwrite `JUPYTERHUB_API_URL` in env.
        Default value is the default internal `JUPYTERHUB_API_URL`
        """,
    ).tag(config=True)

    @default("public_api_url")
    def _public_api_url_default(self):
        if self.hub_connect_url is not None:
            hub_api_url = url_path_join(
                self.hub_connect_url, urlparse(self.hub.api_url).path
            )
        else:
            hub_api_url = self.hub.api_url
        return hub_api_url

    dns_name_template = Unicode(
        "{name}.{namespace}.svc.cluster.local",
        config=True,
        help="""
        Template to use to form the dns name for the pod.
        """,
    )

    svc_name_template = Unicode(
        "jupyter-{username}--{servername}",
        config=True,
        help="""
        Template to use to form the name of user's pods.

        `{username}`, `{userid}`, `{servername}`, `{hubnamespace}`,
        `{unescaped_username}`, and `{unescaped_servername}` will be expanded if
        found within strings of this configuration. The username and servername
        come escaped to follow the `DNS label standard
        <https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names>`__.

        Trailing `-` characters are stripped for safe handling of empty server names (user default servers).

        This must be unique within the namespace the pods are being spawned
        in, so if you are running multiple jupyterhubs spawning in the
        same namespace, consider setting this to be something more unique.

        """,
    )

    namespace = Unicode(
        config=True,
        help="""
        Kubernetes namespace to create services in.
        Default::

          ns_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
          if os.path.exists(ns_path):
              with open(ns_path) as f:
                  return f.read().strip()
          return "default"
        """,
    )

    @default("namespace")
    def _namespace_default(self):
        """
        Set namespace default to current namespace if running in a k8s cluster

        If not in a k8s cluster with service accounts enabled, default to
        `default`
        """
        ns_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
        if os.path.exists(ns_path):
            with open(ns_path) as f:
                return f.read().strip()
        return "default"

    def _expand_user_properties(self, template):
        # Make sure username and servername match the restrictions for DNS labels
        # Note: '-' is not in safe_chars, as it is being used as escape character
        safe_chars = set(string.ascii_lowercase + string.digits)

        raw_servername = self.name or ""
        safe_servername = escapism.escape(
            raw_servername, safe=safe_chars, escape_char="-"
        ).lower()

        hub_namespace = self._namespace_default()
        if hub_namespace == "default":
            hub_namespace = "user"

        legacy_escaped_username = "".join(
            [s if s in safe_chars else "-" for s in self.user.name.lower()]
        )
        safe_username = escapism.escape(
            self.user.name, safe=safe_chars, escape_char="-"
        ).lower()
        rendered = template.format(
            userid=self.user.id,
            username=safe_username,
            unescaped_username=self.user.name,
            legacy_escape_username=legacy_escaped_username,
            servername=safe_servername,
            unescaped_servername=raw_servername,
            hubnamespace=hub_namespace,
        )
        # strip trailing - delimiter in case of empty servername.
        # k8s object names cannot have trailing -
        return rendered.rstrip("-")

    def start(self):
        # Wrapper around self._start
        # Can be used to cancel start progress while waiting for it's response

        self.call_during_startup = False

        async def call_subclass_start(self):
            if self.port == 0:
                self.port = random_port()

            create_ssh_remote_forward = await self.get_ssh_create_remote_forward()
            if create_ssh_remote_forward:
                try:
                    if self.ssh_custom_forward_remote:
                        port_forward_remote = self.ssh_custom_forward_remote(
                            self, self.ssh_custom_forward_remote
                        )
                        if inspect.isawaitable(port_forward_remote):
                            await port_forward_remote
                    else:
                        await self.ssh_default_forward_remote()
                except Exception as e:
                    raise web.HTTPError(
                        419,
                        log_message=f"Cannot start remote ssh tunnel for {self._log_name}: {str(e)}",
                    )

            self._start_future = asyncio.ensure_future(self._start())
            try:
                resp = await self._start_future
            except Exception as e:
                status_code = getattr(e, "status_code", 500)
                reason = getattr(e, "reason", traceback.format_exc()).replace(
                    "\n", "<br>"
                )
                log_message = getattr(e, "log_message", "")
                now = datetime.now().strftime("%Y_%m_%d %H:%M:%S.%f")[:-3]
                self.stop_event = {
                    "failed": True,
                    "ready": False,
                    "progress": 100,
                    "message": "",
                    "html_message": f"<details><summary>{now}: JupyterLab start failed ({status_code}). {log_message}</summary>{reason}</details>",
                }
                self.latest_events.append(self.stop_event)
                # Wait up to 5 times yield_wait_seconds, before sending stop event to frontend
                stopwait = time.monotonic() + 5 * self.yield_wait_seconds
                while time.monotonic() < stopwait:
                    if self._cancel_event_yielded:
                        break
                    await asyncio.sleep(2 * self.yield_wait_seconds)
                raise e
            resp_json = {"service": resp}

            """
            There are 3 possible scenarios for remote singleuser servers:
            1. Reachable by JupyterHub (e.g. outpost service running on same cluster)
            2. Port forwarding required, and we know the service_address (e.g. outpost service running on remote cluster)
            3. Port forwarding required, but we don't know the service_address yet (e.g. start on a batch system)
            """
            if self.internal_ssl:
                proto = "https://"
            else:
                proto = "http://"
            port = self.port
            ssh_during_startup = self.get_ssh_during_startup()
            if ssh_during_startup:
                # Case 2: Create port forwarding to service_address given by outpost service.

                # Store port_forward_info, required for port forward removal
                self.port_forward_info = resp_json
                svc_name, port = await maybe_future(self.run_ssh_forward())
                ret = f"{proto}{svc_name}:{port}"
            else:
                if not resp_json.get("service", ""):
                    # Case 3: service_address not known yet.
                    # Wait for service at default address. The singleuser server itself
                    # has to call the SetupTunnel API with it's actual location.
                    # This will trigger the delayed port forwarding.
                    ret = f"{proto}{self.svc_name}:{self.port}"
                else:
                    # Case 1: No port forward required, just connect to given service_address
                    service_address, port = self.split_service_address(
                        resp_json.get("service")
                    )
                    ret = f"{proto}{service_address}:{port}"

            # Port may have changed in port forwarding or by remote outpost service.
            self.port = int(port)
            self.log.info(f"Expect JupyterLab at {ret}")
            return ret

        self._start_future_response = asyncio.ensure_future(call_subclass_start(self))
        return self._start_future_response

    async def _start(self):
        raise NotImplementedError("Override in subclass. Must be a coroutine.")

    async def poll(self):
        status = await self._poll()

        if self.call_during_startup:
            self.call_during_startup = False
            ssh_recreate_at_start = await self.get_ssh_recreate_at_start()

            if status != None:
                await self.stop(cancel=True)
                self.run_post_stop_hook()
                return status
            elif ssh_recreate_at_start:
                try:
                    await self.run_ssh_forward(create_svc=False)
                except:
                    self.log.exception(
                        "Could not recreate ssh tunnel during startup. Stop server"
                    )
                    self.call_during_startup = False
                    await self.stop(cancel=True)
                    self.run_post_stop_hook()
                    return 0

        return status

    async def _poll(self):
        raise NotImplementedError("Override in subclass. Must be a coroutine.")

    async def _stop(self):
        raise NotImplementedError("Override in subclass. Must be a coroutine.")

    async def stop(self, now=False, cancel=False, event=None, **kwargs):
        if self.already_stopped:
            # We've already sent a request to the outpost.
            # There's no need to do it again.
            return

        # Prevent multiple requests to the outpost
        self.already_stopped = True

        if cancel:
            # If self._start is still running we cancel it here
            await self.cancel_start_function()

        try:
            await self._stop(now=now, **kwargs)
        finally:
            if event:
                if callable(event):
                    event = await maybe_future(event)
                self.latest_events.append(event)

        # We've implemented a cancel feature, which allows us to call
        # Spawner.stop(cancel=True) and stop the spawn process.
        # Used by api_setup_tunnel.py.
        if cancel:
            await self.cancel()

        if self.port_forward_info:
            await self.run_ssh_forward_remove()

    async def cancel_start_function(self):
        # cancel self._start, if it's running
        for future in [self._start_future_response, self._start_future]:
            if future and type(future) is asyncio.Task:
                self.log.warning(f"Start future status: {future._state}")
                if future._state in ["PENDING"]:
                    try:
                        future.cancel()
                        await maybe_future(future)
                    except asyncio.CancelledError:
                        pass
            else:
                self.log.debug(f"{future} not cancelled.")

    async def cancel(self):
        try:
            # If this function was, it was called directly in self.stop
            # and not via user.stop. So we want to cleanup the user object
            # as well. It will throw an exception, but we expect the asyncio task
            # to be cancelled, because we've cancelled it ourself.
            await self.user.stop(self.name)
        except asyncio.CancelledError:
            pass

        if type(self._spawn_future) is asyncio.Task:
            if self._spawn_future._state in ["PENDING"]:
                try:
                    self._spawn_future.cancel()
                    await maybe_future(self._spawn_future)
                except asyncio.CancelledError:
                    pass
