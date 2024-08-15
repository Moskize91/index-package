import os
import sqlite3

from typing import Optional
from dataclasses import dataclass
from enum import Enum
from index_package.watcher import Watcher

@dataclass
class _Folder:
  path: str
  mtime: float
  children: Optional[list[str]]

class EventKind(Enum):
    Added = 0
    Updated = 1
    Removed = 2

@dataclass
class Event:
  id: int
  kind: EventKind
  path: str
  mtime: float

def scan(scan_path: str, db_path: str) -> Watcher:
  conn = _connect(db_path)
  cursor = conn.cursor()
  next_scan_dirs: list[str] = ["/"]
  while len(next_scan_dirs) > 0:
    scan_dir = next_scan_dirs.pop()
    scan_sub_path = os.path.join(scan_path, f".{scan_dir}")
    children = _scan_and_report(conn, cursor, scan_sub_path)
    if children is not None:
      for child in children:
        next_scan_dir = os.path.join(scan_dir, child)
        next_scan_dirs.insert(0, next_scan_dir)
  return Watcher(conn, cursor)

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
    path: str,
) -> Optional[list[str]]:

  old_folder = _select_folder(cursor, path)
  new_folder: Optional[_Folder] = None
  need_update_db = True

  if os.path.exists(path):
    mtime = os.path.getmtime(path)
    children: Optional[list[str]] = None

    if old_folder is not None and old_folder.mtime == mtime:
      children = old_folder.children
      need_update_db = False
    elif os.path.isdir(path):
      children = os.listdir(path)
    new_folder = _Folder(path, mtime, children)

  elif old_folder is None:
    return

  if need_update_db:
    try:
      conn.execute("BEGIN TRANSACTION")
      if new_folder is not None:
        new_path = new_folder.path
        new_mtime = new_folder.mtime
        new_children: Optional[str] = None
        if new_folder.children is not None:
          new_children = "/".join(new_folder.children)

        if old_folder is None:
          conn.execute(
            "INSERT INTO files (path, mtime, children) VALUES (?, ?, ?)",
            (new_path, new_mtime, new_children),
          )
          conn.execute(
            "INSERT INTO events (kind, path, mtime) VALUES (?, ?, ?)",
            (EventKind.Added, new_path, new_mtime),
          )
        else:
          conn.execute(
            "UPDATE files SET mtime = ?, children = ? WHERE path = ?",
            (new_mtime, new_children, new_path),
          )
          conn.execute(
            "INSERT INTO events (kind, path, mtime) VALUES (?, ?, ?)",
            (EventKind.Updated, new_path, new_mtime),
          )
      elif old_folder is not None:
        old_path = old_folder.path
        old_mtime = old_folder.mtime
        conn.execute("DELETE FROM files WHERE path = ?", (old_path,))
        conn.execute(
          "INSERT INTO events (kind, path, mtime) VALUES (?, ?, ?)",
          (EventKind.Removed, old_path, old_mtime),
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

def _select_folder(cursor: sqlite3.Cursor, path: str) -> Optional[_Folder]:
  cursor.execute("SELECT mtime, children FROM files WHERE path = ?", (path,))
  row = cursor.fetchone()
  if row is None:
    return None
  mtime, children_str = row
  children: Optional[list[str]] = None

  if children_str is not None:
    # "/" is disabled in unix file system, so it's safe to use it as separator
    children = children_str.split("/")

  return _Folder(path, mtime, children)