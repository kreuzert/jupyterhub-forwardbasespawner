from tornado.web import HTTPError


def check_custom_scopes(handler):
    allowed = False
    for scope in handler.required_scopes:
        if handler.has_scope(scope):
            allowed = True
            continue
    if not allowed:
        raise HTTPError(
            403,
            f"Not allowed with scopes {list(handler.parsed_scopes.keys())}. At least one of {handler.required_scopes} is required",
        )
