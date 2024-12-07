import time
from cassandra.cluster import Cluster
from config_batch_processing import cassandra_ip, cassandra_port, cassandra_keyspace
from cassandra.auth import PlainTextAuthProvider


if __name__ == "__main__":

    def query_cassandra():
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster([cassandra_ip], port=cassandra_port, auth_provider=auth_provider)
        session = cluster.connect(cassandra_keyspace)

        tables = ["dataset1", "dataset2", "dataset3"]
        for table in tables:
            query = f"SELECT * FROM {table} LIMIT 10"
            rows = session.execute(query)
            print(f"Data from {table}:")
            for row in rows:
                print(row)

        cluster.shutdown()

    while True:
        query_cassandra()
        time.sleep(300)  # Sleep for 5 minutes
