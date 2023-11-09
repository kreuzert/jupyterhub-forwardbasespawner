import json

from jupyterhub import orm
from jupyterhub.apihandlers import APIHandler
from jupyterhub.apihandlers import default_handlers
from jupyterhub.utils import token_authenticated

from .utils import check_custom_scopes


class ListServersAPIHandler(APIHandler):
    required_scopes = ["custom:servers:list"]

    def check_xsrf_cookie(self):
        pass

    @token_authenticated
    async def get(self):
        check_custom_scopes(self)
        query = (
            self.db.query(orm.Spawner)
            .filter(orm.Spawner.server != None)
            .order_by(orm.Spawner.user_id.asc())
        )
        names = [f"{x.user_id}_{x.name}" for x in query]
        self.set_status(200)
        self.write(json.dumps(names))


default_handlers.append((r"/api/servers", ListServersAPIHandler))
