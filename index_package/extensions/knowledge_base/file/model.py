from dataclasses import dataclass
from sqlite3 import Cursor
from index_package.sqlite3_pool import register_table_creators, SQLite3Pool


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
  def __init__(self, db_path: str):
    self._db: SQLite3Pool = SQLite3Pool("scanner", db_path)

  @property
  def db(self) -> SQLite3Pool:
    return self._db

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

  def file(self, cursor: Cursor, scope: str, path: str) -> File | None:
    cursor.execute(
      "SELECT mtime, children FROM files WHERE scope = ? AND path = ?",
      (scope, path),
    )
    row = cursor.fetchone()
    if row is None:
      return None

    mtime, encoded_children = row
    children: list[str] | None = None
    if encoded_children is not None:
      children = encoded_children.split("/")

    return File(scope=scope, path=path, mtime=mtime, children=children)

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

  def _encode_children(self, children: list[str] | None):
    if children is None:
      return None
    else:
      return "/".join(children)

def _create_tables(cursor: Cursor):
  cursor.execute('''
    CREATE TABLE files (
      id INTEGER PRIMARY KEY,
      scope TEXT NOT NULL,
      path TEXT NOT NULL,
      mtime REAL NOT NULL,
      children TEXT
    )
  ''')
  cursor.execute('''
    CREATE TABLE scopes (
      name TEXT PRIMARY KEY,
      path TEXT NOT NULL
    )
  ''')
  cursor.execute("""
    CREATE UNIQUE INDEX idx_files ON files (scope, path)
  """)

register_table_creators("knowledge_base", _create_tables)