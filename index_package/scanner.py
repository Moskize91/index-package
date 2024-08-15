import os
import sqlite3

from typing import Optional
from dataclasses import dataclass
from .events import EventKind, EventSearcher, EventTarget

@dataclass
class _Folder:
  path: str
  mtime: float
  children: Optional[list[str]]

def scan(scan_path: str, db_path: str) -> EventSearcher:
  conn = _connect(db_path)
  cursor = conn.cursor()
  next_relative_paths: list[str] = ["/"]
  while len(next_relative_paths) > 0:
    relative_path = next_relative_paths.pop()
    children = _scan_and_report(conn, cursor, scan_path, relative_path)
    if children is not None:
      for child in children:
        next_relative_path = os.path.join(relative_path, child)
        next_relative_paths.insert(0, next_relative_path)
  return EventSearcher(conn, cursor)

def _connect(db_path: str) -> sqlite3.Connection:
  is_first_time = not os.path.exists(db_path)
  conn = sqlite3.connect(db_path)
  os.path.getmtime(db_path)

  if is_first_time:
    cursor = conn.cursor()
    cursor.execute('''
      CREATE TABLE files (
        path TEXT PRIMARY KEY,
        mtime REAL NOT NULL,
        children TEXT
      )
    ''')
    cursor.execute('''
      CREATE TABLE events (
        id INTEGER PRIMARY KEY,
        kind INTEGER NOT NULL,
        target INTEGER NOT NULL,
        path TEXT NOT NULL,
        mtime REAL NOT NULL
      )
    ''')
    conn.commit()
    cursor.close()

  return conn

def _scan_and_report(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    scan_path: str,
    relative_path: str,
) -> Optional[list[str]]:
  abs_path = os.path.join(scan_path, f".{relative_path}")
  old_folder = _select_folder(cursor, abs_path, relative_path)
  new_folder: Optional[_Folder] = None
  need_update_db = True

  if os.path.exists(abs_path):
    mtime = os.path.getmtime(abs_path)
    children: Optional[list[str]] = None

    if old_folder is not None and old_folder.mtime == mtime:
      children = old_folder.children
      need_update_db = False
    elif os.path.isdir(abs_path):
      children = os.listdir(abs_path)
    new_folder = _Folder(relative_path, mtime, children)

  elif old_folder is None:
    return

  if need_update_db:
    try:
      conn.execute("BEGIN TRANSACTION")
      if new_folder is not None:
        new_path = new_folder.path
        new_mtime = new_folder.mtime
        new_children: Optional[str] = None
        new_target: EventTarget = EventTarget.File

        if new_folder.children is not None:
          new_children = "/".join(new_folder.children)
          new_target = EventTarget.Directory

        if old_folder is None:
          conn.execute(
            "INSERT INTO files (path, mtime, children) VALUES (?, ?, ?)",
            (new_path, new_mtime, new_children),
          )
          conn.execute(
            "INSERT INTO events (kind, target, path, mtime) VALUES (?, ?, ?, ?)",
            (EventKind.Added.value, new_target.value, new_path, new_mtime),
          )
        else:
          conn.execute(
            "UPDATE files SET mtime = ?, children = ? WHERE path = ?",
            (new_mtime, new_children, new_path),
          )
          conn.execute(
            "INSERT INTO events (kind, target, path, mtime) VALUES (?, ?, ?, ?)",
            (EventKind.Updated.value, new_target.value, new_path, new_mtime),
          )
      elif old_folder is not None:
        old_path = old_folder.path
        old_mtime = old_folder.mtime
        old_target = EventTarget.File if old_folder.children is None else EventTarget.Directory

        conn.execute("DELETE FROM files WHERE path = ?", (old_path,))
        conn.execute(
          "INSERT INTO events (kind, target, path, mtime) VALUES (?, ?, ?, ?)",
          (EventKind.Removed.value, old_target.value, old_path, old_mtime),
        )
      conn.commit()

    except Exception as e:
      print(e)
      conn.rollback()
      raise e

    if new_folder is None:
      return None

    if new_folder.children is None:
      return None

    return new_folder.children

def _select_folder(cursor: sqlite3.Cursor, abs_path: str, relative_path: str) -> Optional[_Folder]:
  cursor.execute("SELECT mtime, children FROM files WHERE path = ?", (abs_path,))
  row = cursor.fetchone()
  if row is None:
    return None
  mtime, children_str = row
  children: Optional[list[str]] = None

  if children_str is not None:
    # "/" is disabled in unix file system, so it's safe to use it as separator
    children = children_str.split("/")

  return _Folder(relative_path, mtime, children)