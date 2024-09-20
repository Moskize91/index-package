from sqlite3 import Connection, Cursor
from enum import Enum
from typing import Generator

class EventKind(Enum):
  Added = 0
  Updated = 1
  Removed = 2

class EventTarget(Enum):
  File = 0
  Directory = 1

def scan_events(conn: Connection) -> Generator[int, None, None]:
  cursor = conn.cursor()
  try:
    cursor.execute("SELECT id FROM events ORDER BY id")
    for row in cursor.fetchmany(size=45):
      yield row[0]
  finally:
    cursor.close()

def record_added_event(cursor: Cursor, target: EventTarget, path: str, scope: str, mtime: float):
  cursor.execute(
    "INSERT INTO events (kind, target, path, scope, mtime) VALUES (?, ?, ?, ?, ?)",
    (EventKind.Added.value, target.value, path, scope, mtime),
  )

def record_updated_event(cursor: Cursor, target: EventTarget, path: str, scope: str, mtime: float):
  cursor.execute(
    "INSERT INTO events (kind, target, path, scope, mtime) VALUES (?, ?, ?, ?, ?)",
    (EventKind.Updated.value, target.value, path, scope, mtime),
  )

def record_removed_event(cursor: Cursor, target: EventTarget, path: str, scope: str, mtime: float):
  cursor.execute(
    "INSERT INTO events (kind, target, path, scope, mtime) VALUES (?, ?, ?, ?, ?)",
    (EventKind.Removed.value, target.value, path, scope, mtime),
  )