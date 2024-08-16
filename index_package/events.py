from enum import Enum
import sqlite3

from dataclasses import dataclass
from typing import Optional

class EventTarget(Enum):
  File = 0
  Directory = 1

class EventKind(Enum):
    Added = 0
    Updated = 1
    Removed = 2

@dataclass
class Event:
  id: int
  kind: EventKind
  target: EventTarget
  path: str
  mtime: float

class EventSearcher:
  def __init__(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    self._conn: sqlite3.Connection = conn
    self._cursor: sqlite3.Cursor = cursor
    self._events: Optional[list[Event]] = None
    self._did_completed = False

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    if self._events is not None:
      latest_id = self._events[-1].id
      self._cursor.execute("DELETE FROM events WHERE id < ?", (latest_id,))
      self._events = None

    self._cursor.close()
    self._conn.commit()
    self._conn.close()
    return False

  def __iter__(self):
    return self

  def __next__(self) -> Event:
    if self._did_completed:
      raise StopIteration

    if self._events is None:
      self._events = self._fetch_events_group()
      if self._events is None:
        self._did_completed = True
        raise StopIteration

    event = self._events.pop()

    if len(self._events) == 0:
      self._events = None
      self._cursor.execute("DELETE FROM events WHERE id <= ?", (event.id,))
      self._conn.commit()

    return event

  def _fetch_events_group(self) -> Optional[list[Event]]:
    group_size = 45
    row = self._cursor.execute(
      "SELECT id, kind, target, path, mtime FROM events ORDER BY id LIMIT ?", (group_size,),
    )
    rows = row.fetchall()
    if len(rows) == 0:
      return None

    events: list[Event] = []
    for row in rows:
      events.append(Event(
        id=row[0],
        kind=EventKind(row[1]),
        target=EventTarget(row[2]),
        path=row[3],
        mtime=row[4],
      ))
    events.reverse()
    return events