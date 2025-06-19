import logging

class ConnectionStringError(Exception):
    """Custom exception for invalid or incomplete SQL connection strings."""
    pass

def parse_conn_str(conn_str: str):
    """
    Parses a SQL connection string into components needed by pytds.connect().

    Args:
        conn_str (str): The SQL connection string.

    Returns:
        server_tuple: (host: str, port: int)
        database: str
        user: str
        password: str

    Raises:
        ConnectionStringError: If any required component is missing.
    
    Example:
        server_tuple, database, user, password = parse_conn_str(
            "Server=myserver,1433;Database=erp;User Id=admin;Password=secret"
        )
    """
    parts = {
        k.strip().lower(): v.strip()
        for kv in conn_str.split(";") if "=" in kv
        for k, v in [kv.split("=", 1)]
    }

    # Server and port
    raw_server = parts.get("server", "")
    if not raw_server:
        logging.error("Missing 'Server' in SQL connection string.")
        raise ConnectionStringError("Missing 'Server' in SQL connection string.")

    if "," in raw_server:
        host, port_str = raw_server.split(",", 1)
    else:
        host = raw_server
        port_str = parts.get("port", "1433")

    # Credentials and database
    database = parts.get("database", "")
    user = parts.get("user id") or parts.get("uid") or ""
    password = parts.get("password") or parts.get("pwd") or ""

    # Validation for all critical fields
    missing = []
    if not host: missing.append("server/host")
    if not database: missing.append("database")
    if not user: missing.append("user id/uid")
    if not password: missing.append("password/pwd")
    if missing:
        logging.error(f"Missing required fields in connection string: {missing}")
        raise ConnectionStringError(f"Missing required fields in connection string: {missing}")

    try:
        port = int(port_str)
    except ValueError:
        logging.warning("Invalid port '%s'; defaulting to 1433", port_str)
        port = 1433

    server_tuple = (host, port)
    # Only log non-sensitive connection components
    logging.debug(
        "Parsed connection: host=%s, port=%s, database=%s, user=%s [password hidden]",
        host, port, database, user
    )

    return server_tuple, database, user, password
