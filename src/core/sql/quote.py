def _quote_ident(name: str) -> str:
    # minimal identifier quoting
    return '"' + name.replace('"', '""') + '"'
