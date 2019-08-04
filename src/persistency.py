import os
import sqlite3

from datetime import datetime


# noinspection SqlNoDataSourceInspection,SqlResolve
class ResumeCopySQL:
    def __init__(self, start_id, dest_id, folder_id, app_dir):
        self.start_id = start_id
        self.dest_id = dest_id
        self.folder_id = folder_id
        self.app_dir = app_dir

        self._db_name = "{}-{}-{}.sqlite3".format(self.start_id, self.dest_id, self.folder_id)
        self.sqlite_path = os.path.join(self.app_dir, self._db_name)

        if not os.path.isdir(self.app_dir):
            os.makedirs(self.app_dir)

        if not os.path.exists(self.sqlite_path):
            # we create an empty file
            open(self.sqlite_path, 'a').close()

            sql_create_query = """CREATE TABLE file_archive (
                                  start_id           TEXT      UNIQUE
                                                               NOT NULL,
                                  dest_id            TEXT      NOT NULL,
                                  start_modifiedTime TIMESTAMP NOT NULL,
                                  parents            TEXT      NOT NULL);"""

            tmp_con = sqlite3.connect(self.sqlite_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            tmp_cur = tmp_con.cursor()
            tmp_cur.execute(sql_create_query)
            tmp_con.commit()
            tmp_cur.close()
            tmp_con.close()

        self._con = sqlite3.connect(self.sqlite_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        # self._con.row_factory = sqlite3.Row
        self._cur = self._con.cursor()

    def resume(self):
        resume_sql = "SELECT * from 'file_archive'"
        self._cur.execute(resume_sql)

        for x in self._cur.fetchall():
            yield x

    def add_mapping(self, start_id, dest_id, dt, parents, replace=False):
        if replace:
            insert_sql = "REPLACE INTO 'file_archive' (start_id, dest_id, start_modifiedTime, parents) VALUES (?,?,?,?)"
        else:
            insert_sql = "INSERT INTO 'file_archive' (start_id, dest_id, start_modifiedTime, parents) VALUES (?,?,?,?)"
        self._cur.execute(insert_sql, (start_id, dest_id, dt, parents))
        self._con.commit()

    def reset_mappings(self):
        delete_sql = "DELETE FROM 'file_archive'"
        self._cur.execute(delete_sql)
        self._con.commit()

    @staticmethod
    def lock_update():
        pass
