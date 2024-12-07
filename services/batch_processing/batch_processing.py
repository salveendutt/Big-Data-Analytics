import time
from utils_batch_processing import create_cassandra_session, query_cassandra

if __name__ == "__main__":
    tables = ["dataset1", "dataset2", "dataset3"]
    # TODO: Remove 'LIMIT 10' from the query, this is just for testing
    query = lambda table: f"SELECT * FROM {table} LIMIT 10"
    while True:
        session = create_cassandra_session()
        for table in tables:
            rows = query_cassandra(session, table, query(table))
            print(f"Query result for {table}: {rows}")
            # TODO: Perform batch processing on the results, we need to create 'views' and then put them into some other DB that is not setup yet
        session.shutdown()
        time.sleep(300)
