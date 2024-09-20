from abc import ABC, abstractmethod
from sqlite3 import Cursor, Connection
from typing import Optional

from .event_parser import EventTarget
from .events import record_removed_event

class Scope(ABC):

  @property
  @abstractmethod
  def scopes(self) -> list[str]:
    pass

  @abstractmethod
  def scope_path(self, scope: str) -> Optional[str]:
    pass

class ScopeManager(Scope):
  def __init__(self, conn: Connection):
    cursor = conn.cursor()
    try:
      self._sources = self._fill_sources(cursor)
    finally:
      cursor.close()

  @property
  def scopes(self) -> list[str]:
    return list(self._sources.keys())

  def scope_path(self, scope: str) -> Optional[str]:
    return self._sources.get(scope, None)

  def commit_sources(self, conn: Connection, sources: dict[str, str]):
    cursor = conn.cursor()
    try:
      cursor.execute("BEGIN TRANSACTION")
      origin_sources = self._fill_sources(cursor)
      removed_scopes: list[str] = []

      for name, path in sources.items():
        origin_path = origin_sources.get(name, None)
        if origin_path is None:
          cursor.execute("INSERT INTO scopes (name, path) VALUES (?, ?)", (name, path))
        elif origin_path != path:
          cursor.execute("UPDATE scopes SET path = ? WHERE name = ?", (path, name))
          origin_sources.pop(name)

      for name in origin_sources.keys():
        cursor.execute("DELETE FROM scopes WHERE name = ?", (name,))
        removed_scopes.append(name)

      for scope_name in removed_scopes:
        self._record_events_about_scope_removed(cursor, scope_name)

      self._sources = sources
      conn.commit()

    except Exception as e:
      conn.rollback()
      raise e

    finally:
      cursor.close()

  def _fill_sources(self, cursor: Cursor) -> dict[str, str]:
    cursor.execute("SELECT name, path FROM scopes")
    sources: dict[str, str] = {}
    for name, path in cursor.fetchall():
      sources[name] = path
    return sources

  def _record_events_about_scope_removed(self, cursor: Cursor, scope: str):
    cursor.execute("SELECT path, mtime, children FROM files WHERE scope = ?", (scope,))

    while True:
      rows = cursor.fetchmany(size=100)
      if len(rows) == 0:
        break
      for row in rows:
        path, mtime, children = row
        target = EventTarget.File if children is None else EventTarget.Directory
        record_removed_event(cursor, target, path, scope, mtime)

    cursor.execute("DELETE FROM files WHERE scope = ?", (scope,))