from __future__ import annotations
from typing import Protocol
from ..sqlite3_pool import SQLite3Pool


class KnowledgeBaseExtension(Protocol):
  @property
  def id(self) -> str:
    ...

  def file_path(self, source_id: str) -> str | None:
    ...

  def scan(self, context: KnowledgeBaseScanningContext) -> None:
    ...

class KnowledgeBaseScanningContext(Protocol):
  def assert_continue(self) -> None:
    ...

class KnowledgeBaseEventsDatabase(Protocol):
  @property
  def db(self) -> SQLite3Pool:
    ...

  def report_added_source(self, id: str, ext_name: str, mtime: float):
    ...

  def report_updated_source(self, id: str, mtime: float):
    ...

  def report_removed_source(self, id: str):
    ...