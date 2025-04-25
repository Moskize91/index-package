import os

from index_package.sqlite3_pool import SQLite3Pool
from ..types import Extension
from ..events.core import EventsDatabase
from .extension import FileExtension
from .model import Model, Scope


class FileKnowledgeBase:
  def __init__(self, db_path: str):
    self._db: SQLite3Pool = SQLite3Pool("file", db_path)
    self._events_db: EventsDatabase = EventsDatabase()
    self._model: Model = Model()

  def create_extension(self) -> Extension:
    return FileExtension(self._db, self._events_db, self._model)

  def scopes(self) -> list[tuple[str, str]]:
    with self._db.connect() as (cursor, _):
      return [
        (scope.name, scope.path)
        for scope in self._model.scopes(cursor)
      ]

  def put_scope(self, name: str, path: str):
    with self._db.connect() as (cursor, conn):
      self._model.put_scope(cursor, Scope(name=name, path=path))
      conn.commit()

  def remove_scope(self, name):
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        self._model.remove_scope(cursor, name)
        files = list(self._model.files(cursor, name))
        for file in files:
          source_id = f"{name}/{file.path}"
          ext_name = os.path.splitext(file.path)[1]
          self._events_db.report_removed(cursor, source_id, ext_name, file.mtime)
        self._model.remove_files(cursor, name)
        conn.commit()

      except Exception as e:
        conn.rollback()
        raise e