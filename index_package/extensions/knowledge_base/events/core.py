from time import time
from sqlite3 import Cursor
from ..types import Event, EventKind


class EventsDatabase:
  def oldest_event(self, cursor: Cursor, kind: EventKind | None) -> Event | None:
    if kind is None:
      cursor.execute(
        "SELECT id, kind, ext_name, mtime, created_at FROM events ORDER BY created_at LIMIT 1",
      )
    else:
      cursor.execute(
        "SELECT id, ext_name, mtime, created_at FROM events WHERE kind = ? ORDER BY created_at LIMIT 1",
        (kind.value,),
      )
    row = cursor.fetchone()
    if row is None:
      return None

    return Event(
      id=row[0],
      kind=kind,
      ext_name=row[1],
      mtime=row[2],
      created_at=row[3],
    )

  def remove_event(self, cursor: Cursor, source_id: str):
    cursor.execute(
      "DELETE FROM events WHERE id = ?",
      (source_id,),
    )

  def report_added(self, cursor: Cursor, source_id: str, ext_name: str, mtime: float):
    cursor.execute(
      "SELECT kind, mtime FROM events WHERE id = ?",
      (source_id,),
    )
    row = cursor.fetchone()

    if row is None:
      cursor.execute(
        "INSERT INTO events (id, kind, ext_name, mtime, created_at) VALUES (?, ?, ?, ?, ?)",
        (source_id, EventKind.Added.value, ext_name, mtime, time()),
      )
    else:
      self._report_exists_row(cursor, row, mtime)

  def report_updated(self, cursor: Cursor, source_id: str, ext_name: str, mtime: float):
    cursor.execute(
      "SELECT kind, mtime FROM events WHERE id = ?",
      (source_id,),
    )
    row = cursor.fetchone()

    if row is None:
      cursor.execute(
        "INSERT INTO events (id, kind, ext_name, mtime, created_at) VALUES (?, ?, ?, ?, ?)",
        (source_id, EventKind.Updated.value, ext_name, mtime, time()),
      )
    else:
      self._report_exists_row(cursor, row, mtime)

  def report_removed(self, cursor: Cursor, source_id: str, ext_name: str, mtime: float):
    cursor.execute(
      "SELECT kind, mtime FROM events WHERE id = ?",
      (source_id,),
    )
    row = cursor.fetchone()

    if row is None:
      cursor.execute(
        "INSERT INTO events (id, kind, ext_name, mtime, created_at) VALUES (?, ?, ?, ?, ?)",
        (source_id, EventKind.Removed.value, ext_name, mtime, time()),
      )
    else:
      self._report_not_exists_row(cursor, row, mtime)

  def _report_exists_row(self, cursor: Cursor, origin_row: any, new_mtime: float):
    source_id: str = origin_row[0]
    kind = EventKind(origin_row[1])
    origin_mtime: float = origin_row[2]

    if kind == EventKind.Removed:
      if new_mtime == origin_mtime:
        cursor.execute(
          "DELETE FROM events WHERE id = ?",
          (source_id,),
        )
      else:
        cursor.execute(
          "UPDATE events SET kind = ?, mtime = ? WHERE id = ?",
          (EventKind.Updated.value, new_mtime, source_id),
        )
    elif new_mtime != origin_mtime:
      cursor.execute(
        "UPDATE events SET mtime = ? WHERE id = ?",
        (new_mtime, source_id),
      )

  def _report_not_exists_row(self, cursor: Cursor, origin_row: any, new_time: float):
    source_id: str = origin_row[0]
    kind = EventKind(origin_row[1])
    origin_mtime: float = origin_row[2]

    if kind == EventKind.Added:
      cursor.execute(
        "DELETE FROM events WHERE id = ?",
        (source_id,),
      )
    elif kind == EventKind.Updated:
      cursor.execute(
        "UPDATE events SET kind = ?, mtime = ? WHERE id = ?",
        (EventKind.Removed.value, new_time, source_id),
      )
    elif kind == EventKind.Removed and new_time != origin_mtime:
      cursor.execute(
        "UPDATE events SET mtime = ? WHERE id = ?",
        (new_time, source_id),
      )

def create_events_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE events (
      id TEXT PRIMARY KEY,
      kind INTEGER NOT NULL,
      ext_name TEXT NOT NULL,
      mtime REAL NOT NULL,
      created_at REAL NOT NULL
    )
  """)
  cursor.execute("""
    CREATE INDEX idx_events ON events (id, created_at)
  """)
  cursor.execute("""
    CREATE INDEX idx_kind_events ON events (kind, id, created_at)
  """)