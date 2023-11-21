import json

from jupyterhub import orm
from jupyterhub.apihandlers import APIHandler
from jupyterhub.apihandlers import default_handlers
from jupyterhub.utils import token_authenticated

from .utils import check_custom_scopes


class SSHNodeRestartedAPIHandler(APIHandler):
    """
    Whenever a node, which is used as proxy to reach the user's
    notebook server, was restarted JupyterHub has to recreate
    all port-forwarding process to this node.

    It's the nodes responsibility to inform JupyterHub that it
    was restarted.
    """

    required_scopes = ["custom:sshnode:restart"]

    def check_xsrf_cookie(self):
        pass

    async def recreate_processes(self):
        query = (
            self.db.query(orm.Spawner)
            .filter(orm.Spawner.server != None)
            .order_by(orm.Spawner.user_id.asc())
        )
        servers = [
            (x.user_id, x.name)
            for x in query
            if x.state.get("port_forward_info", {}).get("ssh_node", "") == ssh_node
        ]
        for x in servers:
            try:
                await self.app.users[x[0]].spawners[x[1]].run_ssh_forward_remove(
                    delete_svc=False
                )
            except:
                self.log.warning(
                    f"{x[0]}:{x[1]} - Could not remove port_forward before recreating it"
                )
            try:
                await self.app.users[x[0]].spawners[x[1]].run_ssh_forward(
                    create_svc=False
                )
            except:
                self.log.exception(f"{x[0]}:{x[1]} - Could not recreate port_forward")

    @token_authenticated
    async def get(self, ssh_node):
        check_custom_scopes(self)
        asyncio.create_task(self.recreate_processes())
        self.set_status(202)
        return


default_handlers.append((r"/api/recreateforward/([^/]+)", SSHNodeRestartedAPIHandler))
