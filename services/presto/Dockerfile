FROM prestodb/presto:0.290

# Install Cassandra plugin
RUN mkdir -p /usr/lib/presto/plugin/cassandra \
    && curl -L https://repo1.maven.org/maven2/com/facebook/presto/presto-cassandra/0.290/presto-cassandra-0.290.jar -o /usr/lib/presto/plugin/cassandra/presto-cassandra-0.290.jar
