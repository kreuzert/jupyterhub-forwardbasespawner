import datetime
import inspect
import json

from jupyterhub.apihandlers import default_handlers
from jupyterhub.apihandlers.base import APIHandler
from jupyterhub.scopes import needs_scope
from tornado import web


class SetupTunnelAPIHandler(APIHandler):
    @needs_scope("access:servers")
    async def post(self, user_name, server_name=""):
        self.set_header("Cache-Control", "no-cache")
        if server_name is None:
            server_name = ""
        user = self.find_user(user_name)
        if user is None:
            # no such user
            raise web.HTTPError(404)
        if server_name not in user.spawners:
            # user has no such server
            raise web.HTTPError(404)
        body = self.request.body.decode("utf8")
        try:
            json_body = json.loads(body) if body else {}
        except:
            self.set_status(400)
            self.log.exception(
                f"{user_name}:{server_name} - Could not load body into json. Body: {body}"
            )
            return

        user = self.find_user(user_name)
        spawner = user.spawners[server_name]

        if spawner._stop_pending:
            self.log.debug(
                f"{spawner._log_name} - APICall: SetupTunnel - but spawner is already stopping.",
                extra={
                    "log_name": spawner._log_name,
                    "user": user_name,
                    "action": "setuptunnel",
                    "event": json_body,
                },
            )
            self.set_header("Content-Type", "text/plain")
            self.write("Bad Request.")
            self.set_status(400)
            return

        if json_body:
            self.log.debug(
                f"{spawner._log_name} - APICall: SetupTunnel",
                extra={
                    "log_name": spawner._log_name,
                    "user": user_name,
                    "action": "setuptunnel",
                    "event": json_body,
                },
            )
            try:
                spawner.port_forward_info.update(json_body)
                spawner.orm_spawner.state = spawner.get_state()
                self.db.commit()
                await spawner.run_ssh_forward()
            except Exception as e:
                now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                failed_event = {
                    "progress": 100,
                    "failed": True,
                    "html_message": f"<details><summary>{now}: Could not setup tunnel</summary>{str(e)}</details>",
                }
                self.log.exception(
                    f"{spawner._log_name} - Could not setup tunnel for {user_name}:{server_name}",
                    extra={
                        "log_name": spawner._log_name,
                        "user": user_name,
                        "action": "tunnelfailed",
                        "event": failed_event,
                    },
                )
                stop = spawner.stop(cancel=True, event=failed_event)
                if inspect.isawaitable(stop):
                    await stop
                self.set_header("Content-Type", "text/plain")
                self.set_status(400)
                return
            else:
                self.set_header("Content-Type", "text/plain")
                self.set_status(204)
                return
        else:
            self.set_header("Content-Type", "text/plain")
            self.write("Bad Request.")
            self.set_status(400)
            return


default_handlers.append((r"/api/users/setuptunnel/([^/]+)", SetupTunnelAPIHandler))
default_handlers.append(
    (r"/api/users/setuptunnel/([^/]+)/([^/]+)", SetupTunnelAPIHandler)
)
