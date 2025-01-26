import os

from typing import cast
from dataclasses import dataclass
from sqlite3 import Cursor, Connection
from index_package.sqlite3_pool import SQLite3Pool

from ..types import Context, Event, EventKind
from ..events import EventsDatabase
from .model import File, Scope, Model


@dataclass
class _Context:
  ctx: Context
  cursor: Cursor
  conn: Connection

class FileExtension:
  def __init__(self, db_path: str):
    self._db: SQLite3Pool = SQLite3Pool("file", db_path)
    self._events_db: EventsDatabase = EventsDatabase()
    self._model: Model = Model()

  @property
  def id(self) -> str:
    return "file"

  def file_path(self, source_id: str) -> str | None:
    parts = source_id.split("/", 1)
    if len(parts) != 2:
      raise ValueError(f"Invalid source_id: {source_id}")

    with self._db.connect() as (cursor, _):
      scope_name, path = parts
      scope = self._model.scope(cursor, scope_name)
      if scope is None:
        return None
      return self._model.file(cursor, scope_name, path)

  def oldest_event(self, kind: EventKind | None) -> Event | None:
    with self._db.connect() as (cursor, _):
      return self._events_db.oldest_event(cursor, kind)

  def remove_event(self, event: Event):
    with self._db.connect() as (cursor, conn):
      self._events_db.remove_event(cursor, event.id)
      conn.commit()

  def scan(self, context: Context):
    with self._db.connect() as (cursor, conn):
      ctx = _Context(ctx=context, cursor=cursor, conn=conn)
      for scope in self._model.scopes(cursor):
        self._scan_scope(ctx, scope)

  def _scan_scope(self, context: _Context, scope: Scope):
    next_relative_paths: list[str] = [os.path.sep]
    while len(next_relative_paths) > 0:
      context.ctx.assert_continue()
      relative_path = next_relative_paths.pop()
      children = self._scan_dir(context, scope, relative_path)
      if children is not None:
        for child in children:
          next_relative_path = os.path.join(relative_path, child)
          next_relative_paths.insert(0, next_relative_path)

  def _scan_dir(self, context: _Context, scope: Scope, relative_path: str):
    abs_path = os.path.join(scope.path, f".{relative_path}")
    abs_path = os.path.abspath(abs_path)
    old_file = self._model.file(context.cursor, scope.name, relative_path)
    new_file: File | None = None
    file_never_change = False

    if os.path.exists(abs_path):
      is_dir = os.path.isdir(abs_path)
      new_file, file_never_change = self._build_new_file(
        scope, abs_path, relative_path, is_dir, old_file,
      )
    elif old_file is None:
      return None

    if not file_never_change:
      self._commit_file_updation(
        context, scope, old_file, new_file,
      )

    if new_file is None:
      return None
    if new_file.children is None:
      return None
    if new_file.is_dir and self._ignore_dir(abs_path):
      return None

    return new_file.children

  def _build_new_file(self, scope: Scope, abs_path: str, relative_path: str, is_dir: bool, old_file: File):
    mtime = os.path.getmtime(abs_path)
    children: list[str] | None = None
    file_never_change = False

    if old_file is not None and \
       old_file.mtime == mtime and \
       is_dir == old_file.is_dir:
      children = old_file.children
      file_never_change = True

    elif is_dir:
      children = sorted(os.listdir(abs_path))

    new_file = File(scope, relative_path, mtime, children)
    return new_file, file_never_change

  def _commit_file_updation(self, context: _Context, scope: Scope, old_file: File | None, new_file: File | None):
    cursor = context.cursor
    conn = context.conn
    try:
      cursor.execute("BEGIN TRANSACTION")
      self._commit_file_self_events(context, scope, old_file, new_file)
      if old_file is not None and old_file.is_dir:
        self._commit_release_children(context, scope, old_file, new_file)
      conn.commit()

    except Exception as e:
      conn.rollback()
      raise e

  def _commit_file_self_events(
    self,
    context: _Context,
    scope: Scope,
    old_file: File | None,
    new_file: File | None
  ):
    if new_file is not None:
      ext_name = os.path.splitext(new_file.path)[1]
      if old_file is None:
        self._model.insert_file(context.cursor, new_file)
        if new_file.is_dir:
          source_id = f"{scope.name}/{new_file.path}"
          self._events_db.report_added(context.cursor, source_id, ext_name, new_file.mtime)
      else:
        source_id = f"{scope.name}/{new_file.path}"
        self._model.update_file(context.cursor, new_file)
        if old_file.is_dir and not new_file.is_dir:
          self._events_db.report_added(context.cursor, source_id, ext_name, new_file.mtime)
        elif not old_file.is_dir and not new_file.is_dir:
          self._events_db.report_updated(context.cursor, source_id, ext_name, new_file.mtime)
        elif not old_file.is_dir and new_file.is_dir:
          self._events_db.report_removed(context.cursor, source_id, ext_name, old_file.mtime)

    elif old_file is not None:
      ext_name = os.path.splitext(old_file.path)[1]
      self._model.remove_file(context.cursor, scope.name, old_file.path)
      if old_file.is_dir:
        self._handle_removed_folder(context, old_file)
      else:
        source_id = f"{scope.name}/{old_file.path}"
        self._events_db.report_removed(context.cursor, source_id, ext_name, old_file.mtime)

  def _commit_release_children(self, context: _Context, scope: Scope, old_file: File, new_file: File | None):
    to_remove = set(cast(list[str], old_file.children))
    if new_file is not None and new_file.children is not None:
      for child in new_file.children:
        if child in to_remove:
          to_remove.remove(child)
    for removed_file in to_remove:
      child_path = os.path.join(old_file.path, removed_file)
      child_file = self._model.file(context.cursor, scope.name, child_path)
      if child_file is None:
        continue

      self._handle_removed_folder(context, child_file)
      if child_file.is_dir:
        self._handle_removed_folder(context.cursor, child_file)
      else:
        source_id = f"{scope.name}/{child_file.path}"
        ext_name = os.path.splitext(child_file.path)[1]
        self._events_db.report_removed(context.cursor, source_id, ext_name, child_file.mtime)

  def _ignore_dir(self, path: str) -> bool:
    # iBook will save epub as a directory
    _, file_extension = os.path.splitext(path)
    return file_extension.lower() == ".epub"

  def _handle_removed_folder(self, context: _Context, folder: File):
    assert folder.children is not None
    for child in folder.children:
      path = os.path.join(folder.path, child)
      file = self._model.file(context.cursor, folder.scope, path)
      if file is not None:
        self._model.remove_file(context.cursor, file.scope, file.path)
        if file.is_dir:
          self._handle_removed_folder(context, file)
        else:
          source_id = f"{file.scope}/{file.path}"
          ext_name = os.path.splitext(file.path)[1]
          self._events_db.report_removed(context.cursor, source_id, ext_name, file.mtime)
