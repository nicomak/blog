import uuid
from cassandra.cluster import Cluster
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from datetime import datetime
from project.settings import CASSANDRA_NODES

KEYSPACE = 'posts_db'

# Set up the connection with the IPs of the nodes and the keyspace
connection.setup(CASSANDRA_NODES, KEYSPACE)
session = connection.get_session()
session.set_keyspace(KEYSPACE)

class PostModel(Model):
    __table_name__ = 'posts'

    username	= columns.Text(primary_key=True, required=True)
    creation    = columns.TimeUUID(primary_key=True, required=True, default=uuid.uuid1)
    content	= columns.Text(required=True)

    @classmethod
    def count(cls):
        return cls.objects.count()

    @classmethod
    def select_user_latest(cls, username, n=100):
        return cls.objects(username=username).limit(n)

    @classmethod
    def select_user_latest_with_date(cls, username, n=100):
        return session.execute("""
                               SELECT username, creation, dateOf(creation) as creation_date, content
                               FROM posts
                               WHERE username=%s
                               ORDER BY creation DESC
                               """,
                               [username])

    @classmethod
    def create_post(cls, username, content):
        return cls.create(username=username, content=content)
