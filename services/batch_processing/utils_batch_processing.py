import time
from config_batch_processing import (
    cassandra_connection_attempts,
    cassandra_connection_attempts_delay,
    cassandra_ip,
    cassandra_keyspace,
    cassandra_password,
    cassandra_port,
    cassandra_username,
)
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


def create_cassandra_session():
    attempts = 0
    while attempts < cassandra_connection_attempts:
        try:
            auth_provider = PlainTextAuthProvider(
                username=cassandra_username, password=cassandra_password
            )
            cluster = Cluster(
                [cassandra_ip], port=cassandra_port, auth_provider=auth_provider
            )
            session = cluster.connect(cassandra_keyspace)
        except Exception as e:
            print(f"Attempt {attempts + 1} failed: {e}")
            attempts += 1
            time.sleep(cassandra_connection_attempts_delay)
    else:
        print(
            f"Failed to connect to Cassandra after {cassandra_connection_attempts} attempts"
        )
        exit(1)
    return session


def query_cassandra(session, query):
    rows = session.execute(query)
    return rows
