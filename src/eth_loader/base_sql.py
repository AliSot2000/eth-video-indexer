import os
from sqlite3 import Connection, Cursor
from typing import Union


class BaseSQliteDB:
    db_path: Union[str, None]

    sq_con: Union[Connection, None]
    sq_cur: Union[Cursor, None]

    def __init__(self, db_path: str):
        """
        Base class for sqlite3 database access.

        :param db_path: path to db
        """
        self.db_path = None
        self.sq_con = None
        self.sq_cur = None

        self.connect(db_path)


    def connect(self, db_path: str):
        """
        Connect to the database, set the path of the object and create a cursor
        """
        self.db_path = os.path.abspath(db_path)

        self.sq_con = Connection(self.db_path)
        self.sq_cur = self.sq_con.cursor()

    def debug_execute(self, stmt: str):
        """
        Function executes statement in database and in case of an exception prints the offending statement.

        :param stmt: Statement to execute

        :return:
        """
        try:
            self.sq_cur.execute(stmt)
        except Exception as e:
            print(f"Failed to execute:\n{stmt}")
            raise e

    def cleanup(self):
        """
        Actions performed:
        - Commit the changes
        - Close the connection
        - clear the variables
        """
        self.sq_con.commit()
        self.sq_con.close()

        self.db_path = None

        self.sq_con = None
        self.sq_cur = None