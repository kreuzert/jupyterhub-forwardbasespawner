import asyncio
import copy
import inspect
import os
import re
import string
import subprocess
import time
import traceback
import uuid
from datetime import datetime
from datetime import timedelta
from functools import lru_cache
from urllib.parse import urlparse

import escapism
from async_generator import aclosing
from jupyterhub.spawner import Spawner
from jupyterhub.utils import AnyTimeoutError
from jupyterhub.utils import maybe_future
from jupyterhub.utils import random_port
from jupyterhub.utils import url_path_join
from kubernetes import client
from kubernetes import config
from tornado import gen
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

    # Used in api_notifications to check, if the UNICORE notification
    # is for the current start attempt.
    start_id = ""

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

    # Other elements might want to wait for the cancellation to finish
    _cancel_wait_event = None

    # Store events for last start_attempt
    events = []
    yield_wait_seconds = 1

    extra_labels = Union(
        [Dict(default_value={}), Callable()],
        help="""
        An optional hook function, or dict, you can implement to add
        extra labels to the service created when using port-forwarding.
        Will also be forwarded to the Outpost service (see self.custom_misc_disable_default)
        
        This may be a coroutine.
        
        Example::

            def extra_labels(spawner):
                labels = {
                    "hub.jupyter.org/username": spawner.user.name,
                    "hub.jupyter.org/servername": spawner.name,
                    "sidecar.istio.io/inject": "false"
                }
                return labels
            
            c.ForwardBaseSpawner.extra_labels = extra_labels
        """,
    ).tag(config=True)

    custom_port = Union(
        [Integer(), Callable()],
        default_value=0,
        help="""
        An optional hook function, or dict, you can implement to define
        a port depending on the spawner object.
        
        Example::
        
            from jupyterhub.utils import random_potr
            def custom_port(spawner, user_options):
                if user_options.get("system", "") == "A":
                    return 8080
                return random_port()
            
            c.OutpostSpawner.custom_port = custom_port
        """,
    ).tag(config=True)

    @property
    def port(self):
        """Get the port used for the singleuser server

        Returns:
          port (int): port of the newly created singleuser server
        """
        if callable(self.custom_port):
            port = self.custom_port(self, self.user_options)
        elif self.custom_port:
            port = self.custom_port
        else:
            port = random_port()
            self.custom_port = port
        return port

    ssh_recreate_at_start = Union(
        [Callable(), Bool()],
        default_value=False,
        help="""
        Whether ssh tunnels should be recreated when JupyterHub starts or not.
        If you have outsourced the port forwarding to an extra pod, you can
        set this to false. Outsourcing also means, that connections to running
        JupyterLabs are not affected by JupyterHub restarts.
        
        This may be a coroutine.
        """,
    ).tag(config=True)

    ssh_during_startup = Union(
        [Callable(), Bool()],
        default_value=False,
        help="""
        An optional hook function, or boolean, you can implement to
        decide whether a ssh port forwarding process should be run after
        the POST request to the JupyterHub Outpost service.
        
        Common Use Case: 
        singleuser service was started remotely and is not accessible by
        JupyterHub (e.g. it's running on a different K8s Cluster), but you
        know exactly where it is (e.g. the service address).

        Example::

            def ssh_during_startup(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return True
                return False

            c.ForwardBaseSpawner.ssh_during_startup = ssh_during_startup

        """,
    ).tag(config=True)

    ssh_key = Union(
        [Callable(), Unicode()],
        allow_none=True,
        default_value="/home/jovyan/.ssh/id_rsa",
        help="""
        An optional hook function, or string, you can implement to
        set the ssh privatekey used for ssh port forwarding.

        This may be a coroutine.

        Example::

            def ssh_key(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "/mnt/private_keys/a"
                return "/mnt/private_keys/b"

            c.ForwardBaseSpawner.ssh_key = ssh_key

        """,
    ).tag(config=True)

    ssh_remote_key = Union(
        [Callable(), Unicode()],
        allow_none=True,
        default_value="/home/jovyan/.ssh/id_rsa_remote",
        help="""
        An optional hook function, or string, you can implement to
        set the ssh privatekey used for ssh port forwarding remote.

        This may be a coroutine.

        Example::

            def ssh_remote_key(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "/mnt/private_keys/a"
                return "/mnt/private_keys/b"

            c.ForwardBaseSpawner.ssh_remote_key = ssh_remote_key

        """,
    ).tag(config=True)

    ssh_username = Union(
        [Callable(), Unicode()],
        default_value="jhuboutpost",
        help="""
        An optional hook function, or string, you can implement to
        set the ssh username used for ssh port forwarding.

        This may be a coroutine.

        Example::

            def ssh_username(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "jhuboutpost"
                return "ubuntu"

            c.ForwardBaseSpawner.ssh_username = ssh_username

        """,
    ).tag(config=True)

    ssh_remote_username = Union(
        [Callable(), Unicode()],
        default_value="jhuboutpost",
        help="""
        An optional hook function, or string, you can implement to
        set the ssh username used for ssh port forwarding remote.

        This may be a coroutine.

        Example::

            def ssh_username(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "jhuboutpost"
                return "ubuntu"

            c.ForwardBaseSpawner.ssh_remote_username = ssh_username

        """,
    ).tag(config=True)

    ssh_node = Union(
        [Callable(), Unicode()],
        allow_none=True,
        default_value=None,
        help="""
        An optional hook function, or string, you can implement to
        set the ssh node used for ssh port forwarding.

        This may be a coroutine.

        Example::

            def ssh_node(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "outpost.namespace.svc"
                else:
                    return "<public_ip>"

            c.ForwardBaseSpawner.ssh_node = ssh_node

        """,
    ).tag(config=True)

    svc_create = Union(
        [Callable(), Bool()],
        allow_none=True,
        default_value=True,
        help="""
        An optional hook function, or boolean, you can implement to
        disable the svc creation.

        This may be a coroutine.

        Example::

            async def svc_create(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return False
                else:
                    return True

            c.ForwardBaseSpawner.svc_create = svc_create

        """,
    ).tag(config=True)

    ssh_node_mapping = Callable(
        allow_none=True,
        default_value=None,
        help="""
        An optional hook function, you can implement to
        set the map the given ssh node to a different avlue.

        This may be a coroutine.

        Example::

            def ssh_node_mapping(spawner, ssh_node):
                if ssh_node == "<internal_hostname>":
                    return "<external_dns_name>"
                return ssh_node

            c.ForwardBaseSpawner.ssh_node_mapping = ssh_node_mapping

        """,
    ).tag(config=True)

    ssh_remote_node = Union(
        [Callable(), Unicode()],
        allow_none=True,
        default_value=None,
        help="""
        An optional hook function, or string, you can implement to
        set the ssh node used for ssh port forwarding remote.

        This may be a coroutine.

        Example::

            def ssh_node(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return "outpost.namespace.svc"
                else:
                    return "<public_ip>"

            c.ForwardBaseSpawner.ssh_remote_node = ssh_node

        """,
    ).tag(config=True)

    ssh_port = Union(
        [Callable(), Integer(), Unicode()],
        default_value=22,
        help="""
        An optional hook function, or string, you can implement to
        set the ssh port used for ssh port forwarding.

        This may be a coroutine.

        Example::

            def ssh_port(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return 22
                else:
                    return 2222

            c.ForwardBaseSpawner.ssh_port = ssh_port

        """,
    ).tag(config=True)

    ssh_remote_port = Union(
        [Callable(), Integer(), Unicode()],
        default_value=22,
        help="""
        An optional hook function, or string, you can implement to
        set the ssh port used for ssh port forwarding remote.

        This may be a coroutine.

        Example::

            def ssh_port(spawner):
                if spawner.user_options.get("system", "") == "A":
                    return 22
                else:
                    return 2222

            c.ForwardBaseSpawner.ssh_remote_port = ssh_port

        """,
    ).tag(config=True)

    ssh_custom_forward_remote = Any(
        help="""
        An optional hook function you can implement to create your own
        ssh port forwarding from remote system to hub. 
        """
    ).tag(config=True)

    ssh_custom_forward_remote_remove = Any(
        help="""
        An optional hook function you can implement to remove your own
        ssh port forwarding from remote system to hub.
        """
    ).tag(config=True)

    pre_poll_hook = Any(
        default_value=False,
        help="""
        Hook which allows to run a function before calling poll.
        Useful when you already know the answer of the upcoming poll
        call (e.g. information in auth_state is missing).
        
        Used return values of the pre_poll_hook:
        Return True: Unknown status. Call self._poll()
        Return False: Unknown status. Do not call self._poll(). Server continues as running.
        Return Integer: That's the exit code. Do not call self._poll()
        Return None: Server still running. Do not call self._poll()
        
        Callable function, may be a coroutine.
        """,
    ).tag(config=True)

    async def run_pre_poll_hook(self):
        if self.pre_poll_hook:
            ret = self.pre_poll_hook(self)
            if inspect.isawaitable(ret):
                ret = await ret
            self.log.debug(f"{self._log_name} - Pre poll hook return: {ret}")
            return ret
        else:
            return True

    update_expected_path = Any(
        default_value=False,
        help="""
        Hook which allows to update the return value of Spawner.start().
        
        Callable function, may be a coroutine.
        """,
    ).tag(config=True)

    async def run_update_expected_path(self, default_start_return_value):
        if self.update_expected_path:
            expected_path = self.update_expected_path(self, default_start_return_value)
            if inspect.isawaitable(expected_path):
                expected_path = await expected_path
            return expected_path
        else:
            return default_start_return_value

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
        An optional hook function you can implement to create your own
        ssh port forwarding called in the start function. This can be
        used to use an external pod for the port forwarding instead of 
        having JupyterHub handle it.
        
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

            c.ForwardBaseSpawner.ssh_custom_forward = ssh_custom_forward

        """
    ).tag(config=True)

    ssh_custom_forward_remove = Any(
        help="""
        An optional hook function you can implement to remove your own
        ssh port forwarding called in the stop function. This can be 
        used to use an external pod for the port forwarding instead of 
        having JupyterHub handle it.

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

            c.ForwardBaseSpawner.ssh_custom_forward_remove = ssh_custom_forward_remove

        """
    ).tag(config=True)

    ssh_custom_svc = Any(
        help="""
        An optional hook function you can implement to create a customized
        kubernetes svc called in the start function. 
        
        Example::

            def ssh_custom_svc(spawner, port_forward_info):
                ...
                return spawner.pod_name, spawner.port

            c.ForwardBaseSpawner.ssh_custom_svc = ssh_custom_svc

        """
    ).tag(config=True)

    ssh_custom_svc_remove = Any(
        help="""
        An optional hook function you can implement to remove a customized
        kubernetes svc called in the stop function. 
        
        Example::

            def ssh_custom_svc_remove(spawner, port_forward_info):
                ...
                return spawner.pod_name, spawner.port

            c.ForwardBaseSpawner.ssh_custom_svc_remove = ssh_custom_svc_remove

        """
    ).tag(config=True)

    ssh_forward_options = Union(
        [Dict(default_value={}), Callable()],
        help="""
        An optional hook, or dict, to configure the ssh commands used in the
        spawner.ssh_default_forward function. The default configuration parameters
        (see below) can be overridden.
        
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
                "StrictHostKeyChecking": "accept-new",
                "Port": str(ssh_port),
                "ControlPath": f"/tmp/control_{ssh_address_or_host}",
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

        self.events = []
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
            except Exception as e:
                self.log.exception(
                    f"{self._log_name} - post_stop_hook failed with exception"
                )

    def get_env(self):
        """Get customized environment variables

        Returns:
          env (dict): Used in communication with Outpost service.
        """
        env = super().get_env()

        env["JUPYTERHUB_API_URL"] = self.get_public_api_url().rstrip("/")
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
        state["port_forward_info"] = copy.deepcopy(self.port_forward_info)
        state["custom_port"] = self.port
        state["start_id"] = self.start_id
        return state

    def load_state(self, state):
        """load state from the database"""
        super().load_state(state)
        if "port_forward_info" in state:
            self.port_forward_info = copy.deepcopy(state["port_forward_info"])
        if "custom_port" in state:
            self.custom_port = state["custom_port"]
        if "start_id" in state:
            self.start_id = state["start_id"]

    def clear_state(self):
        """clear any state (called after shutdown)"""
        super().clear_state()
        self._start_future = None
        self._start_future_response = None
        self.port_forward_info = {}
        self.already_stopped = False
        self.already_post_stop_hooked = False
        self.start_id = ""
        self._cancel_event_yielded = False

    show_first_default_event = Any(
        default_value=True,
        help="""
        Hook to define if the default event at 0% should be shown.
        
        Can be a boolean or a callable function.
        This may be a coroutine.
        """,
    ).tag(config=True)

    async def get_show_first_default_event(self):
        if callable(self.show_first_default_event):
            show_first_default_event = await maybe_future(
                self.show_first_default_event(self)
            )
        else:
            show_first_default_event = self.show_first_default_event
        return show_first_default_event

    async def _generate_progress(self):
        """Private wrapper of progress generator

        This method is always an async generator and will always yield at least one event.
        """
        if not self._spawn_pending:
            self.log.warning(
                f"{self._log_name} - Spawn not pending, can't generate progress."
            )
            return

        show_first_default_event = await self.get_show_first_default_event()
        if show_first_default_event:
            yield {"progress": 0, "message": "Server requested"}

        async with aclosing(self.progress()) as progress:
            async for event in progress:
                yield event

    async def progress(self):
        spawn_future = self._spawn_future
        next_event = 0

        break_while_loop = False
        while True:
            # Ensure we always capture events following the start_future
            # signal has fired.
            if spawn_future.done():
                break_while_loop = True

            len_events = len(self.events)
            if next_event < len_events:
                for i in range(next_event, len_events):
                    yield self.events[i]
                    if (
                        len(self.events) > i
                        and self.events[i].get("failed", False) == True
                    ):
                        self._cancel_event_yielded = True
                        break_while_loop = True
                next_event = len_events

            if break_while_loop:
                break
            await asyncio.sleep(self.yield_wait_seconds)

    filter_events = Callable(
        allow_none=True,
        default_value=None,
        help="""
        Different JupyterHub single-user servers may send different events.
        This filter allows you to unify all events. Should always return a dict.
        If the dict should not be shown, return an empty dict.
                
        Example::

            def custom_filter_events(spawner, event):
                event["html_message"] = event.get("message", "No message available")
                return event

            c.ForwardBaseSpawner.filter_events = custom_filter_events
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
        Event shown when a singleuser server was cancelled.
        Can be a function or a dict.
        
        This may be a coroutine.
        
        Example::

            from datetime import datetime
            async def cancel_click_event(spawner):
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                return {
                    "failed": False,
                    "ready": False,
                    "progress": 99,
                    "message": "",
                    "html_message": f"<details><summary>{now}: Cancelling start ...</summary>We're stopping the start process.</details>",
                }
        
            c.ForwardBaseSpawner.cancelling_event = cancel_click_event
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
            r"([0-9]+(-[0-9]+)+).*[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,3})?"
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
          ssh_user (string): Used in ssh forward command. Default is "jhuboutpost"
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
          ssh_remote_username (string): Used in ssh forward command. Default is "None"
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
                              to Outpost service, if true

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
        return await self.get_ssh_node_mapping(ssh_node)

    async def get_ssh_node_mapping(self, ssh_node):
        """Get ssh node

        Returns:
          ssh_node_mapping (string): Used in ssh port forwading command
        """

        if callable(self.ssh_node_mapping):
            ssh_node_mapping = await maybe_future(self.ssh_node_mapping(self, ssh_node))
        else:
            ssh_node_mapping = ssh_node
        return ssh_node_mapping

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

    async def get_svc_create(self):
        """Get ssh username

        Returns:
          svc_create (bool): Whether a service should be created. Default is True
        """
        if callable(self.svc_create):
            svc_create = await maybe_future(self.svc_create(self))
        else:
            svc_create = self.svc_create
        return svc_create

    async def run_ssh_forward(self, create_svc=True):
        """Run the custom_create_port_forward if defined, otherwise run the default one"""
        try:
            if self.ssh_custom_forward:
                port_forward = self.ssh_custom_forward(self, self.port_forward_info)
                if inspect.isawaitable(port_forward):
                    ret = await port_forward
            else:
                ret = await self.ssh_default_forward()
        except Exception as e:
            try:
                ret = await self.run_ssh_forward_remove()
            except:
                self.log.exception(
                    f"{self._log_name} - Could not remove ssh forward processes"
                )
            raise web.HTTPError(
                419,
                log_message=f"Cannot start ssh tunnel for {self.name}: {str(e)}",
                reason=traceback.format_exc(),
            )
        # If the custom_forward function already creates a service
        # the following won't be necessary.
        # If it's a hub restart and the hub managed svc still
        # exists, it's only not necessary to recreat it.
        class_create_svc = await self.get_svc_create()
        if class_create_svc and create_svc:
            try:
                if self.ssh_custom_svc:
                    ssh_custom_svc = self.ssh_custom_svc(self, self.port_forward_info)
                    if inspect.isawaitable(ssh_custom_svc):
                        ssh_custom_svc = await ssh_custom_svc
                    ret = ssh_custom_svc
                else:
                    ret = await self.ssh_default_svc()
            except Exception as e:
                try:
                    self.run_ssh_forward_remove()
                except:
                    self.log.exception(
                        f"{self._log_name} - Could not remove ssh forward processes"
                    )
                raise web.HTTPError(
                    419,
                    log_message=f"Cannot create svc for {self._log_name}: {str(e)}",
                    reason=traceback.format_exc(),
                )
        return ret

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

    async def get_forward_remote_cmd(self, extra_args=["-n"]):
        """Get base options for ssh remote port forwarding

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
            "StrictHostKeyChecking": "accept-new",
            "Port": str(ssh_port),
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

    async def subprocess_cmd(self, cmd, timeout=3):
        """Execute bash cmd via subprocess.Popen as user 1000

        Returns:
          returncode (int): returncode of cmd
        """

        def set_uid():
            try:
                os.setuid(1000)
            except:
                pass

        self.log.info(f"{self._log_name} - ssh cmd: \n{' '.join(cmd)}")
        p = await asyncio.create_subprocess_shell(
            " ".join(cmd),
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            preexec_fn=set_uid,
        )

        try:
            comm_future = p.communicate()
            out, err = await gen.with_timeout(timedelta(seconds=timeout), comm_future)
        except Exception as e:
            if isinstance(e, AnyTimeoutError):
                self.log.warning(
                    f"{self._log_name} - subprocess cmd timeout ({timeout}) for {' '.join(cmd)}"
                )
            else:
                self.log.exception(
                    f"{self._log_name} - Unexpected error for {' '.join(cmd)}"
                )
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
        await self.subprocess_cmd(cancel_cmd)

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
        returncode, out, err = await self.subprocess_cmd(check_cmd)

        if returncode != 0:
            # Create multiplex connection
            connect_cmd = cmd.copy()
            connect_cmd.append(f"{user}@{node}")

            # First creation always runs in a timeout. Expect this and check
            # the success with check_cmd again
            try:
                returncode, out, err = await self.subprocess_cmd(connect_cmd, timeout=1)
            except AnyTimeoutError as e:
                returncode, out, err = await self.subprocess_cmd(check_cmd)

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

        returncode, out, err = await self.subprocess_cmd(create_cmd)
        if returncode != 0:
            self.log.warning(
                f"{self._log_name} - Could not forward port ({create_cmd}) (Returncode: {returncode} != 0). Stdout: {out}. Stderr: {err}"
            )
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
            returncode, out, err = await self.subprocess_cmd(cancel_cmd)
            self.log.warning(
                f"{self._log_name} - Could not remote previous port forwarding ({cancel_cmd}) (Returncode: {returncode} != 0). Stdout: {out}. Stderr: {err}"
            )

            returncode, out, err = await self.subprocess_cmd(create_cmd)
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
        await self.subprocess_cmd(stop_cmd)

    async def ssh_default_forward_remote(self):
        """Default function to create remote port forward.
        Forwards 0.0.0.0:<custom_port> to JupyterHub on an external system.
        This allows a JupyterLab without internet connection running
        on an external system. It will reach its JupyterHub via this
        port forward process.

        Returns:
          None
        """
        user, node, cmd = await self.get_forward_remote_cmd()
        start_cmd = cmd.copy()
        start_cmd.extend([f"{user}@{node}", "start"])
        try:
            returncode, out, err = await self.subprocess_cmd(start_cmd)
        except subprocess.TimeoutExpired as e:
            self.log.info(
                f"{self._log_name} - Start cmd timeout. Check if it's running with status."
            )
            status_cmd = cmd.copy()
            status_cmd.extend([f"{user}@{node}", "status"])
            returncode, out, err = await self.subprocess_cmd(status_cmd)
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
        return f"{self.svc_name}.{self.namespace}.svc", self.port

    async def ssh_default_svc_remove(self):
        """Remove Kubernetes Service
        Used parameters: self.svc_name and self.namespace

        Returns:
          None
        """
        v1 = self._k8s_get_client_core()
        name = self.svc_name
        try:
            v1.delete_namespaced_service(name=name, namespace=self.namespace)
        except Exception as e:
            if getattr(e, "reason", "") == "Not Found":
                pass
            else:
                raise e

    async def run_ssh_forward_remove(self, delete_svc=True):
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
            self.log.exception(f"{self._log_name} - Could not cancel port forwarding")

        # If the hub does not manage the k8s svc, we don't have to
        # delete them.
        class_create_svc = await self.get_svc_create()
        if class_create_svc and delete_svc:
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
                self.log.warning(
                    f"{self._log_name} - Could not delete port forwarding svc"
                )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._start_future = None
        self._start_future_response = None
        self.svc_name = self._expand_user_properties(self.svc_name_template)
        self.dns_name = self.dns_name_template.format(
            namespace=self.namespace, name=self.svc_name
        )

    public_api_url = Any(
        help="""
        Singleuser servers started remotely may have to use a different api_url than
        the default internal one. This will overwrite `JUPYTERHUB_API_URL` in env.
        Default value is the default internal `JUPYTERHUB_API_URL`
        """,
    ).tag(config=True)

    def get_public_api_url(self):
        if callable(self.public_api_url):
            public_api_url = self.public_api_url(self)
        elif self.public_api_url:
            public_api_url = self.public_api_url
        else:
            if self.hub_connect_url is not None:
                public_api_url = url_path_join(
                    self.hub_connect_url, urlparse(self.hub.api_url).path
                )
            else:
                public_api_url = self.hub.api_url
        return public_api_url

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
        Set namespace default to current namespace if running in a k8s cluster.

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

    async def wait_for_stop_event(self):
        # Wait up to 5 times yield_wait_seconds, before sending stop event to frontend
        stopwait = time.monotonic() + 5 * self.yield_wait_seconds
        while time.monotonic() < stopwait:
            if self._cancel_event_yielded:
                break
            await asyncio.sleep(2 * self.yield_wait_seconds)
        return

    def start(self):
        # Wrapper around self._start
        # Can be used to cancel start progress while waiting for it's response

        self.call_during_startup = False
        if not getattr(self, "start_id", ""):
            self.start_id = uuid.uuid4().hex

        async def call_subclass_start(self):
            if self.port == 0:
                self.custom_port = random_port()

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
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                self.stop_event = {
                    "failed": True,
                    "ready": False,
                    "progress": 100,
                    "message": "",
                    "html_message": f"<details><summary>{now}: JupyterLab start failed ({status_code} - {str(e)}). {log_message}</summary>{reason}</details>",
                }
                self.events.append(self.stop_event)
                await self.wait_for_stop_event()
                raise e

            self.log.info(f"{self._log_name} - Start response: {resp}")
            resp_json = {"service": resp}

            """
            There are 3 possible scenarios for remote singleuser servers:
            1. Reachable by JupyterHub (e.g. Outpost service running on same cluster)
            2. Port forwarding required, and we know the service_address (e.g. Outpost service running on remote cluster)
            3. Port forwarding required, but we don't know the service_address yet (e.g. start on a batch system)
            """
            if self.internal_ssl:
                proto = "https://"
            else:
                proto = "http://"
            port = self.port
            ssh_during_startup = self.get_ssh_during_startup()
            if ssh_during_startup:
                # Case 2: Create port forwarding to service_address given by Outpost service.

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

            # Port may have changed in port forwarding or by remote Outpost service.
            self.custom_port = int(port)
            ret = await self.run_update_expected_path(ret)
            self.log.info(f"{self._log_name} - Expect JupyterLab at {ret}")
            return ret

        self._start_future_response = asyncio.ensure_future(call_subclass_start(self))
        return self._start_future_response

    async def _start(self):
        raise NotImplementedError("Override in subclass. Must be a coroutine.")

    async def poll(self):
        if self.already_stopped:
            # avoid loop with stop
            return 0

        pre_poll_value = await self.run_pre_poll_hook()
        if type(pre_poll_value) == bool and pre_poll_value:
            # Return True: Unknown status. Call self._poll()
            status = await self._poll()
        elif type(pre_poll_value) == bool and not pre_poll_value:
            # Return False: Unknown status. Do not call self._poll(). Server continues as running.
            status = None
        else:
            # Return Integer: That's the exit code. Do not call self._poll()
            # Return None: Server still running. Do not call self._poll()
            status = pre_poll_value

        if self.call_during_startup:
            self.call_during_startup = False
            ssh_recreate_at_start = await self.get_ssh_recreate_at_start()

            if status != None:
                try:
                    await self.stop(cancel=True)
                except:
                    self.log.exception(f"{self._log_name} - Could not stop")
                try:
                    self.run_post_stop_hook()
                except:
                    self.log.exception(
                        f"{self._log_name} - Could not run post stop hook"
                    )
                return status
            elif ssh_recreate_at_start:
                try:
                    await self.run_ssh_forward(create_svc=False)
                except:
                    self.log.exception(
                        f"{self._log_name} - Could not recreate ssh tunnel during startup. Stop server"
                    )
                    self.call_during_startup = False
                    await self.stop(cancel=True)
                    self.run_post_stop_hook()
                    return 0
        else:
            # If the remote running service is no longer running, we
            # call the stop function anyway, to ensure everything
            # is teared down correctly. Thanks to the "self.already_stopped"
            # flag, it won't be called twice
            if status != None:
                await self.stop(now=True, cancel=True, event=None)

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
            self._cancel_wait_event = asyncio.Event()
            # If self._start is still running we cancel it here
            cancelling_event = await self.get_cancelling_event()
            self.events.append(cancelling_event)
            await self.cancel_start_function()
        try:
            await self._stop(now=now, **kwargs)
        except AnyTimeoutError:
            self.log.exception(f"{self._log_name} - timeout")
        except:
            self.log.exception(f"{self._log_name} - Could not stop")
        finally:
            if not event:
                event = await self.get_stop_event()
            elif callable(event):
                event = await maybe_future(event(self))
            self.events.append(event)
            await self.wait_for_stop_event()

        # We've implemented a cancel feature, which allows us to call
        # Spawner.stop(cancel=True) and stop the spawn process.
        # Used by api_setup_tunnel.py.
        if cancel:
            try:
                await self.cancel()
            except:
                self.log.exception(f"{self._log_name} - Start function cancelled")

        if self.port_forward_info:
            try:
                future = self.run_ssh_forward_remove()
                await gen.with_timeout(timedelta(seconds=10), future)
            except AnyTimeoutError:
                self.log.exception(f"{self._log_name} - timeout")

    async def cancel_start_function(self):
        # cancel self._start, if it's running
        for future in [self._start_future, self._start_future_response]:
            if future and type(future) is asyncio.Task:
                if future._state in ["PENDING"]:
                    try:
                        future.cancel()
                    except asyncio.CancelledError:
                        pass
                    except:
                        self.log.exception(
                            f"{self._log_name} - Unexpected error during cancelling."
                        )

    async def cancel(self):
        try:
            # If this function was called, it was called directly in self.stop
            # and not via user.stop. So we want to cleanup the user object
            # as well. It will throw an exception, but we expect the asyncio task
            # to be cancelled, because we've cancelled it ourself.
            await self.user.stop(self.name)
        except asyncio.CancelledError:
            pass
        except:
            self.log.exception(
                f"{self._log_name} - Unexpected error during cancelling."
            )

        if type(self._spawn_future) is asyncio.Task:
            if self._spawn_future._state in ["PENDING"]:
                try:
                    self._spawn_future.cancel()
                except asyncio.CancelledError:
                    pass
        if self._cancel_wait_event:
            self._cancel_wait_event.set()
