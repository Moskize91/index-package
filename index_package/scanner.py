import os
import sqlite3

from typing import Optional
from dataclasses import dataclass
from .events import EventKind, EventSearcher, EventTarget

@dataclass
class _File:
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
    relative_path: str) -> Optional[list[str]]:

  abs_path = os.path.join(scan_path, f".{relative_path}")
  old_file = _select_file(cursor, relative_path)
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
      conn.execute("BEGIN TRANSACTION")
      _commit_file_self_events(cursor, old_file, new_file)
      _commit_children_events(cursor, old_file, new_file)
      conn.commit()
    except Exception as e:
      print(e)
      conn.rollback()
      raise e

  if new_file is None:
    return None

  if new_file.children is None:
    return None

  return new_file.children

def _commit_file_self_events(
  cursor: sqlite3.Cursor,
  old_file: Optional[_File],
  new_file: Optional[_File]):

  if new_file is not None:
    new_path = new_file.path
    new_mtime = new_file.mtime
    new_children, new_target = file_inserted_children_and_target(new_file)

    if old_file is None:
      cursor.execute(
        "INSERT INTO files (path, mtime, children) VALUES (?, ?, ?)",
        (new_path, new_mtime, new_children),
      )
      cursor.execute(
        "INSERT INTO events (kind, target, path, mtime) VALUES (?, ?, ?, ?)",
        (EventKind.Added.value, new_target.value, new_path, new_mtime),
      )
    else:
      cursor.execute(
        "UPDATE files SET mtime = ?, children = ? WHERE path = ?",
        (new_mtime, new_children, new_path),
      )
      cursor.execute(
        "INSERT INTO events (kind, target, path, mtime) VALUES (?, ?, ?, ?)",
        (EventKind.Updated.value, new_target.value, new_path, new_mtime),
      )
  elif old_file is not None:
    old_path = old_file.path
    old_mtime = old_file.mtime
    old_target = EventTarget.File if old_file.children is None else EventTarget.Directory

    cursor.execute("DELETE FROM files WHERE path = ?", (old_path,))
    cursor.execute(
      "INSERT INTO events (kind, target, path, mtime) VALUES (?, ?, ?, ?)",
      (EventKind.Removed.value, old_target.value, old_path, old_mtime),
    )

  if old_file is not None and old_file.children is not None:
    _handle_removed_folder(old_file)

def _commit_children_events(
  cursor: sqlite3.Cursor,
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
    child_file = _select_file(cursor, child_path)

    if child_file is None:
      continue

    target: EventTarget = EventTarget.File

    if child_file.children is not None:
      target = EventTarget.Directory
      _handle_removed_folder(child_file)

    cursor.execute("DELETE FROM files WHERE path = ?", (child_path,))
    cursor.execute(
      "INSERT INTO events (kind, target, path, mtime) VALUES (?, ?, ?, ?)",
      (EventKind.Removed.value, target.value, child_path, child_file.mtime),
    )

def _select_file(cursor: sqlite3.Cursor, relative_path: str) -> Optional[_File]:
  cursor.execute("SELECT mtime, children FROM files WHERE path = ?", (relative_path,))
  row = cursor.fetchone()
  if row is None:
    return None
  mtime, children_str = row
  children: Optional[list[str]] = None

  if children_str is not None:
    # "/" is disabled in unix file system, so it's safe to use it as separator
    children = children_str.split("/")

  return _File(relative_path, mtime, children)

def _handle_removed_folder(folder: _File):
  # TODO:
  pass

def file_inserted_children_and_target(file: _File) -> tuple[Optional[str], EventTarget]:
  children: Optional[str] = None
  target: EventTarget = EventTarget.File

  if file.children is not None:
    children = "/".join(file.children)
    target = EventTarget.Directory

  return children, target