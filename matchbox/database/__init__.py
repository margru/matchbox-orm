import firebase_admin
from firebase_admin import firestore

from matchbox.database import error

_DEFAULT_DATABASE = "(default)"


class Database:
    def __init__(self):
        self._conn = None
        self._project = None
        self._database = None

    def initialization(self, cert_path, project=None, database=_DEFAULT_DATABASE):
        try:
            firebase_admin.get_app()
        except ValueError:
            cred = firebase_admin.credentials.Certificate(cert_path)
            firebase_admin.initialize_app(cred)

        self._conn = firestore.client(project=project, database=database)
        self._project = project
        self._database = database

    @property
    def conn(self):
        if self._conn is None:
            raise error.DBDoesNotinitialized(
                'Connection to db must be initialized'
            )
        return self._conn


db = Database()


def db_initialization(cert_path, project=None, database=_DEFAULT_DATABASE):
    global db

    db.initialization(cert_path, project, database)
