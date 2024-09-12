import os
import sqlite3

from dataclasses import dataclass
from typing import Optional

from .events import EventKind, EventSearcher, EventTarget

@dataclass
class _File:
  path: str
  mtime: float
  children: Optional[list[str]]

class Scanner:
  def __init__(self, db_path: str, sources: dict[str, str]) -> None:
    self._db_path: str = db_path
    self._sources: dict[str, str] = sources
    self._did_sync_scopes = False

  def scan(self) -> EventSearcher:
    conn = self._connect()
    cursor = conn.cursor()

    for scope, scan_path in self._sources.items():
      self._scan_scope(conn, cursor, scope, scan_path)

    return EventSearcher(conn, cursor)

  def scan_scope(self, scope: str) -> EventSearcher:
    scan_path = self._sources.get(scope, None)
    if scan_path is None:
      raise ValueError(f"unregistered scope: {scope}")

    conn = self._connect()
    cursor = conn.cursor()
    self._scan_scope(conn, cursor, scope, scan_path)

    return EventSearcher(conn, cursor)

  def _scan_scope(
    self,
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    scope: str,
    scan_path: str,
  ):
    next_relative_paths: list[str] = ["/"]

    if not self._did_sync_scopes:
      self._sync_scopes(conn, cursor)
      self._did_sync_scopes = True

    while len(next_relative_paths) > 0:
      relative_path = next_relative_paths.pop()
      children = self._scan_and_report(conn, cursor, scope, scan_path, relative_path)
      if children is not None:
        for child in children:
          next_relative_path = os.path.join(relative_path, child)
          next_relative_paths.insert(0, next_relative_path)

  def _connect(self) -> sqlite3.Connection:
    is_first_time = not os.path.exists(self._db_path)
    conn = sqlite3.connect(self._db_path)

    if is_first_time:
      cursor = conn.cursor()
      cursor.execute('''
        CREATE TABLE files (
          id TEXT PRIMARY KEY,
          mtime REAL NOT NULL,
          scope TEXT NOT NULL,
          children TEXT
        )
      ''')
      cursor.execute('''
        CREATE TABLE events (
          id INTEGER PRIMARY KEY,
          kind INTEGER NOT NULL,
          target INTEGER NOT NULL,
          path TEXT NOT NULL,
          scope TEXT NOT NULL,
          mtime REAL NOT NULL
        )
      ''')
      # TODO: 需要存储 path，以便 sources 变化时能读取到上一次的数据
      cursor.execute('''
        CREATE TABLE scopes (
          name TEXT PRIMARY KEY
        )
      ''')
      conn.commit()
      cursor.close()

    return conn

  def _sync_scopes(
    self,
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
  ):
    cursor.execute("SELECT name FROM scopes")
    origin_scopes: set[str] = set()

    for row in cursor.fetchall():
      origin_scopes.add(row[0])

    try:
      cursor.execute("BEGIN TRANSACTION")
      for scope in self._sources.keys():
        if scope in origin_scopes:
          origin_scopes.remove(scope)
        else:
          cursor.execute("INSERT INTO scopes (name) VALUES (?)", (scope,))

      for to_remove_scope in origin_scopes:
        # TODO: 将所有被删除的数据生成 events
        cursor.execute("DELETE FROM files WHERE scope = ?", (to_remove_scope,))

      conn.commit()

    except Exception as e:
      conn.rollback()
      raise e

  def _scan_and_report(
    self,
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    scope: str,
    scan_path: str,
    relative_path: str
  ) -> Optional[list[str]]:

    abs_path = os.path.join(scan_path, f".{relative_path}")
    abs_path = os.path.abspath(abs_path)
    old_file = self._select_file(cursor, scope, relative_path)
    new_file: Optional[_File] = None
    mtime_never_change = False

    if os.path.exists(abs_path):
      mtime = os.path.getmtime(abs_path)
      children: Optional[list[str]] = None

      if old_file is not None and old_file.mtime == mtime:
        children = old_file.children
        mtime_never_change = True
      elif os.path.isdir(abs_path):
        children = os.listdir(abs_path)

      new_file = _File(relative_path, mtime, children)

    elif old_file is None:
      return

    if not mtime_never_change:
      try:
        cursor.execute("BEGIN TRANSACTION")
        self._commit_file_self_events(cursor, scope, old_file, new_file)
        self._commit_children_events(cursor, scope, old_file, new_file)
        conn.commit()
      except Exception as e:
        conn.rollback()
        raise e

    if new_file is None:
      return None

    if new_file.children is None:
      return None

    return new_file.children

  def _commit_file_self_events(
    self,
    cursor: sqlite3.Cursor,
    scope: str,
    old_file: Optional[_File],
    new_file: Optional[_File]
  ):
    if new_file is not None:
      new_path = new_file.path
      new_mtime = new_file.mtime
      new_children, new_target = self._file_inserted_children_and_target(new_file)

      if old_file is None:
        file_id = self._file_id(scope, new_path)
        cursor.execute(
          "INSERT INTO files (id, mtime, scope, children) VALUES (?, ?, ?, ?)",
          (file_id, new_mtime, scope, new_children),
        )
        cursor.execute(
          "INSERT INTO events (kind, target, path, scope, mtime) VALUES (?, ?, ?, ?, ?)",
          (EventKind.Added.value, new_target.value, new_path, scope, new_mtime),
        )
      else:
        file_id = self._file_id(scope, new_path)
        cursor.execute(
          "UPDATE files SET mtime = ?, children = ? WHERE id = ?",
          (new_mtime, new_children, file_id),
        )
        cursor.execute(
          "INSERT INTO events (kind, target, path, scope, mtime) VALUES (?, ?, ?, ?, ?)",
          (EventKind.Updated.value, new_target.value, new_path, scope, new_mtime),
        )
    elif old_file is not None:
      old_path = old_file.path
      old_mtime = old_file.mtime
      old_target = EventTarget.File if old_file.children is None else EventTarget.Directory
      file_id = self._file_id(scope, old_path)

      cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))
      cursor.execute(
        "INSERT INTO events (kind, target, path, scope, mtime) VALUES (?, ?, ?, ?, ?)",
        (EventKind.Removed.value, old_target.value, old_path, scope, old_mtime),
      )
      if old_file.children is not None:
        self._handle_removed_folder(cursor, scope, old_file)

  def _commit_children_events(
    self,
    cursor: sqlite3.Cursor,
    scope: str,
    old_file: Optional[_File],
    new_file: Optional[_File]):

    if old_file is None:
      return
    if old_file.children is None:
      return

    to_remove = set(old_file.children)

    if new_file is not None and new_file.children is not None:
      for child in new_file.children:
        if child in to_remove:
          to_remove.remove(child)

    for removed_file in to_remove:
      child_path = os.path.join(old_file.path, removed_file)
      child_file = self._select_file(cursor, scope, child_path)

      if child_file is None:
        continue

      target: EventTarget = EventTarget.File
      file_id = self._file_id(scope, child_path)

      if child_file.children is not None:
        target = EventTarget.Directory
        self._handle_removed_folder(cursor, scope, child_file)

      cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))
      cursor.execute(
        "INSERT INTO events (kind, target, path, scope, mtime) VALUES (?, ?, ?, ?, ?)",
        (EventKind.Removed.value, target.value, child_path, scope, child_file.mtime),
      )

  def _handle_removed_folder(self, cursor: sqlite3.Cursor, scope: str, folder: _File):
    assert folder.children is not None

    for child in folder.children:
      path = os.path.join(folder.path, child)
      file = self._select_file(cursor, scope, path)
      if file is None:
        continue

      target: EventTarget = EventTarget.File
      if file.children is not None:
        target = EventTarget.Directory
        self._handle_removed_folder(cursor, scope, file)

      cursor.execute("DELETE FROM files WHERE id = ?", (file.path,))
      cursor.execute(
        "INSERT INTO events (kind, target, path, scope, mtime) VALUES (?, ?, ?, ?, ?)",
        (EventKind.Removed.value, target.value, file.path, scope, file.mtime),
      )

  def _select_file(self, cursor: sqlite3.Cursor, scope: str, relative_path: str) -> Optional[_File]:
    file_id = self._file_id(scope, relative_path)
    cursor.execute("SELECT mtime, children FROM files WHERE id = ?", (file_id,))
    row = cursor.fetchone()
    if row is None:
      return None
    mtime, children_str = row
    children: Optional[list[str]] = None

    if children_str is not None:
      # "/" is disabled in unix file system, so it's safe to use it as separator
      children = children_str.split("/")

    return _File(relative_path, mtime, children)

  def _file_inserted_children_and_target(self, file: _File) -> tuple[Optional[str], EventTarget]:
    children: Optional[str] = None
    target: EventTarget = EventTarget.File

    if file.children is not None:
      children = "/".join(file.children)
      target = EventTarget.Directory

    return children, target

  def _file_id(self, scope: str, relative_path: str) -> str:
    return f"{scope}:{relative_path}"