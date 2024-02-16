from cassandra.cluster import Session


def create_keyspace(keyspace: str, clazz: str, replication_factor: 1, session: Session) -> None:
    """
    Creates a keyspace if it doesn't exist
    :param keyspace: The keyspace
    :param clazz: The CQL class
    :param replication_factor: The replication factor
    :param session: The CQL session
    """
    session.execute(
        f"""CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH REPLICATION = {{'class': '{clazz}', 'replication_factor': {replication_factor}}}
        """
    )


def create_table(tablename: str, keyspace: str, columns: dict, session: Session) -> None:
    """
    Creates table if it doesn't exist
    :param tablename: The name of the table
    :param keyspace: The keyspace
    :param columns: The columns for the table
    :param session: The CQL session
    """
    column_definitions = ', '.join([f'{name} {type_}' for name, type_ in columns.items()])
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{tablename} ({column_definitions})
        """
    )
