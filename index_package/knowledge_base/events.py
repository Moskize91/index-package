from sqlite3 import Cursor
from enum import Enum
from ..sqlite3_pool import register_table_creators, SQLite3Pool


class EventKind(Enum):
  Added = 0
  Updated = 1
  Removed = 2

class Model:
  def __init__(self, db_path: str):
    self._db = SQLite3Pool("knowledge_base", db_path)

  def report_added(self, cursor: Cursor, kb_id: str, source_id: str, mtime: float):
    cursor.execute(
      "SELECT kind, mtime FROM events WHERE kb_id = ? AND source_id = ?",
      (kb_id, source_id),
    )
    row = cursor.fetchone()

    if row is None:
      cursor.execute(
        "INSERT INTO events (kb_id, source_id, kind, mtime) VALUES (?, ?, ?, ?)",
        (kb_id, source_id, EventKind.Added.value, mtime),
      )
    else:
      self._report_exists_row(cursor, row, mtime)

  def report_updated(self, cursor: Cursor, kb_id: str, source_id: str, mtime: float):
    cursor.execute(
      "SELECT kind, mtime FROM events WHERE kb_id = ? AND source_id = ?",
      (kb_id, source_id),
    )
    row = cursor.fetchone()

    if row is None:
      cursor.execute(
        "INSERT INTO events (kb_id, source_id, kind, mtime) VALUES (?, ?, ?, ?)",
        (kb_id, source_id, EventKind.Updated.value, mtime),
      )
    else:
      self._report_exists_row(cursor, row, mtime)

  def report_removed(self, cursor: Cursor, kb_id: str, source_id: str, mtime: float):
    cursor.execute(
      "SELECT kind, mtime FROM events WHERE kb_id = ? AND source_id = ?",
      (kb_id, source_id),
    )
    row = cursor.fetchone()

    if row is None:
      cursor.execute(
        "INSERT INTO events (kb_id, source_id, kind, mtime) VALUES (?, ?, ?, ?)",
        (kb_id, source_id, EventKind.Removed.value, mtime),
      )
    else:
      self._report_not_exists_row(cursor, row, mtime)

  def _report_exists_row(self, cursor: Cursor, origin_row: any, new_mtime: float):
    kb_id: str = origin_row[0]
    source_id: str = origin_row[1]
    kind = EventKind(origin_row[2])
    origin_mtime: float = origin_row[3]

    if kind == EventKind.Removed:
      if new_mtime == origin_mtime:
        cursor.execute(
          "DELETE FROM events WHERE kb_id = ? AND source_id = ?",
          (kb_id, source_id),
        )
      else:
        cursor.execute(
          "UPDATE events SET kind = ?, mtime = ? WHERE kb_id = ? AND source_id = ?",
          (EventKind.Updated.value, new_mtime, kb_id, source_id),
        )
    elif new_mtime != origin_mtime:
      cursor.execute(
        "UPDATE events SET mtime = ? WHERE kb_id = ? AND source_id = ?",
        (new_mtime, kb_id, source_id),
      )

  def _report_not_exists_row(self, cursor: Cursor, origin_row: any, new_time: float):
    kb_id: str = origin_row[0]
    source_id: str = origin_row[1]
    kind = EventKind(origin_row[2])
    origin_mtime: float = origin_row[3]

    if kind == EventKind.Added:
      cursor.execute(
        "DELETE FROM events WHERE kb_id = ? AND source_id = ?",
        (kb_id, source_id),
      )
    elif kind == EventKind.Updated:
      cursor.execute(
        "UPDATE events SET kind = ?, mtime = ? WHERE kb_id = ? AND source_id = ?",
        (EventKind.Removed.value, new_time, kb_id, source_id),
      )
    elif kind == EventKind.Removed and new_time != origin_mtime:
      cursor.execute(
        "UPDATE events SET mtime = ? WHERE kb_id = ? AND source_id = ?",
        (new_time, kb_id, source_id),
      )

def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE events (
      id INTEGER PRIMARY KEY,
      kb_id TEXT NOT NULL,
      source_id TEXT NOT NULL,
      kind INTEGER NOT NULL,
      mtime REAL NOT NULL
    )
  """)
  cursor.execute("""
    CREATE INDEX idx_events ON files (kb_id, source_id, kind)
  """)

register_table_creators("knowledge_base", _create_tables)