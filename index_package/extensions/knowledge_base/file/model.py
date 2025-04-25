from dataclasses import dataclass
from sqlite3 import Cursor
from typing import Generator
from index_package.sqlite3_pool import register_table_creators
from ..events import create_events_tables


@dataclass
class Scope:
  name: str
  path: str

@dataclass
class File:
  scope: str
  path: str
  mtime: float
  children: list[str] | None

  @property
  def is_dir(self) -> bool:
    return self.children is not None

class Model:
  def scopes(self, cursor: Cursor) -> list[Scope]:
    cursor.execute("SELECT name, path FROM scopes ORDER BY name")
    rows = cursor.fetchall()
    return [Scope(name=row[0], path=row[1]) for row in rows]

  def scope(self, cursor: Cursor, name: str) -> Scope | None:
    cursor.execute(
      "SELECT name, path FROM scopes WHERE name = ?",
      (name,),
      )
    row = cursor.fetchone()
    if row is None:
      return None
    return Scope(name=row[0], path=row[1])

  def put_scope(self, cursor: Cursor, scope: Scope):
    cursor.execute(
      "SELECT path FROM scopes WHERE name = ?",
      (scope.name,)
    )
    row = cursor.fetchone()
    if row is None:
      cursor.execute(
        "INSERT INTO scopes (name, path) VALUES (?, ?)",
        (scope.name, scope.path),
      )
    elif row[0] != scope.path:
      cursor.execute(
        "UPDATE scopes SET path = ? WHERE name = ?",
        (scope.path, scope.name),
      )


  def remove_scope(self, cursor: Cursor, name: str):
    cursor.execute(
      "DELETE FROM scopes WHERE name = ?",
      (name,),
    )

  def file(self, cursor: Cursor, scope: str, path: str) -> File | None:
    cursor.execute(
      "SELECT mtime, children FROM files WHERE scope = ? AND path = ?",
      (scope, path),
    )
    row = cursor.fetchone()
    if row is None:
      return None

    mtime: float = row[0]
    children = self._decode_children(row[1])

    return File(scope=scope, path=path, mtime=mtime, children=children)

  def files(self, cursor: Cursor, scope: str) -> Generator[File, None, None]:
    cursor.execute(
      "SELECT path, mtime, children FROM files WHERE scope = ?",
      (scope,),
    )
    for row in cursor.fetchall():
      path: str = row[0]
      mtime: float = row[1]
      children = self._decode_children(row[2])
      yield File(scope, path, mtime, children)

  def insert_file(self, cursor: Cursor, file: File):
    children = self._encode_children(file.children)
    cursor.execute(
      "INSERT INTO files (scope, path, mtime, children) VALUES (?, ?, ?, ?)",
      (file.scope, file.path, file.mtime, children),
    )

  def update_file(self, cursor: Cursor, file: File):
    children = self._encode_children(file.children)
    cursor.execute(
      "UPDATE files SET mtime = ?, children = ? WHERE scope = ? AND path = ?",
      (file.mtime, children, file.scope, file.path),
    )

  def remove_file(self, cursor: Cursor, scope: str, path: str):
    cursor.execute(
      "DELETE FROM files WHERE scope = ? AND path = ?",
      (scope, path),
    )

  def remove_files(self, cursor: Cursor, scope: str):
    cursor.execute(
      "DELETE FROM files WHERE scope = ?",
      (scope,),
    )

  def _encode_children(self, children: list[str] | None):
    if children is None:
      return None
    else:
      return "/".join(children)

  def _decode_children(self, encoded: str | None) -> list[str] | None:
    if encoded is None:
      return None
    else:
      return encoded.split("/")

def _create_tables(cursor: Cursor):
  create_events_tables(cursor)
  cursor.execute("""
    CREATE TABLE files (
      id INTEGER PRIMARY KEY,
      scope TEXT NOT NULL,
      path TEXT NOT NULL,
      mtime REAL NOT NULL,
      children TEXT
    )
  """)
  cursor.execute("""
    CREATE TABLE scopes (
      name TEXT PRIMARY KEY,
      path TEXT NOT NULL
    )
  """)
  cursor.execute("""
    CREATE UNIQUE INDEX idx_files ON files (scope, path)
  """)

register_table_creators("file", _create_tables)