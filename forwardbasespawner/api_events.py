import asyncio
import datetime
import json

from jupyterhub.apihandlers import default_handlers
from jupyterhub.apihandlers.base import APIHandler
from jupyterhub.scopes import needs_scope
from tornado import web


class SpawnEventsAPIHandler(APIHandler):
    def check_xsrf_cookie(self):
        pass

    @needs_scope("read:servers")
    async def get(self, user_name, server_name=""):
        user = self.find_user(user_name)
        if user is None:
            # no such user
            raise web.HTTPError(404)
        if server_name not in user.spawners:
            # user has no such server
            raise web.HTTPError(404)
        spawner = user.spawners[server_name]
        data = {
            "events": spawner.events,
            "active": spawner.active,
            "ready": spawner.ready,
        }
        self.write(json.dumps(data))

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
            event = json.loads(body) if body else {}
        except:
            self.set_status(400)
            self.log.exception(
                f"{user_name}:{server_name} - Could not load body into json. Body: {body}"
            )
            return

        user = self.find_user(user_name)
        spawner = user.spawners[server_name]
        uuidcode = server_name

        # Do not do anything if stop or cancel is already pending
        if spawner.pending == "stop" or spawner.already_stopped:
            self.set_status(204)
            return

        if event and event.get("failed", False):
            self.log.debug(
                f"{spawner._log_name} - APICall: SpawnUpdate - {event.get('html_message')}",
                extra={
                    "uuidcode": uuidcode,
                    "log_name": f"{spawner._log_name}",
                    "user": user_name,
                    "action": "failed",
                    "event": event,
                },
            )
            spawner.last_event = event
            if spawner.pending:
                spawn_future = spawner._spawn_future
                if spawn_future:
                    spawn_future.cancel()
                    # Wait (up to 1s, same budget as the previous fixed sleep)
                    # for the cancelled spawn to settle. spawner._spawn_pending
                    # is cleared by the spawn done-callback (jupyterhub
                    # handlers/base.py _clear_spawn_future), and
                    # stop_single_user() raises RuntimeError while
                    # spawner.pending is set.
                    loop = asyncio.get_running_loop()
                    deadline = loop.time() + 1
                    while not spawn_future.done() and loop.time() < deadline:
                        await asyncio.sleep(0.05)
                    if spawn_future.done():
                        # One more yield so the done-callback runs first.
                        await asyncio.sleep(0.05)
                if spawner.pending:
                    # Did not settle within the budget. Log instead of raising
                    # a RuntimeError into the request; a normal stop will pick
                    # it up later.
                    self.log.warning(
                        f"{spawner._log_name} - spawn still pending "
                        f"{spawner.pending!r} after cancel, skipping stop"
                    )
                else:
                    await self.stop_single_user(user, server_name)
            else:
                # include notify, so that a server that died is noticed immediately
                status = await spawner.poll_and_notify()
                if status is None:
                    await self.stop_single_user(user, server_name)
            self.set_header("Content-Type", "text/plain")
            self.set_status(204)
            return


        try:
            event = spawner.run_filter_events(event)
        except:
            self.log.exception(f"{spawner._log_name} - Could not filter exception")
            event = {}
        else:
            if event is None:
                event = {}

        if not event or spawner._stop_pending:
            self.set_header("Content-Type", "text/plain")
            self.write("Bad Request")
            self.set_status(400)
            return
        else:
            # Add timestamp
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            if event.get("html_message", event.get("message", "")).startswith(
                "<details><summary>"
            ):
                event[
                    "html_message"
                ] = f"<details><summary>{now}: {event.get('html_message', event.get('message', ''))[len('<details><summary>'):]}"
            elif not event.get("html_message", ""):
                event["html_message"] = event.get("message", "")

            self.log.debug(
                f"{spawner._log_name} - APICall: SpawnUpdate - {event.get('html_message')}",
                extra={
                    "uuidcode": uuidcode,
                    "log_name": f"{spawner._log_name}",
                    "user": user_name,
                    "action": "spawnupdate",
                    "event": event.get("html_message", event.get("message", event)),
                },
            )
            spawner = user.spawners[server_name]
            if hasattr(spawner, "events") and type(spawner.events) == list:
                spawner.events.append(event)
            self.set_header("Content-Type", "text/plain")
            self.set_status(204)
            return


default_handlers.append((r"/api/users/progress/events/([^/]+)", SpawnEventsAPIHandler))
default_handlers.append(
    (r"/api/users/progress/events/([^/]+)/([^/]+)", SpawnEventsAPIHandler)
)
