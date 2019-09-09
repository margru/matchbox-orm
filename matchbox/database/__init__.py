from google.cloud import firestore

from matchbox.database import error

_DEFAULT_DATABASE = "(default)"


class Database:
    def __init__(self):
        self._conn = None
        self._project = None
        self._database = None
        self._root_path = None

    # Required scopes:
    # LINK: https://googleapis.dev/python/firestore/latest/client.html#google.cloud.firestore_v1.client.Client.SCOPE
    #  - https://www.googleapis.com/auth/cloud-platform
    #  - https://www.googleapis.com/auth/datastore
    def initialization(self, cert_path, project=None, database=None, root_path=None):

        kwargs = {}
        if project is not None:
            kwargs["project"] = project
        if database is not None:
            kwargs["database"] = database
        self._conn = firestore.Client.from_service_account_json(cert_path, **kwargs)

        self._project = project
        self._database = database
        self._root_path = root_path.strip("/") if root_path is not None else ""

    @property
    def conn(self):
        if self._conn is None:
            raise error.DBDoesNotinitialized(
                'Connection to db must be initialized'
            )
        return self._conn

    @property
    def root_path(self):
        return self._root_path

    def collection(self, name):
        if self.root_path:
            name = "/".join([self._root_path, name.strip("/")])
        return self.conn.collection(name)


db = Database()


def db_initialization(cert_path, project=None, database=_DEFAULT_DATABASE, root_path=None):
    global db

    db.initialization(cert_path, project, database, root_path)
