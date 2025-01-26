from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

@dataclass
class Event:
  id: str
  kind: EventKind
  ext_name: str
  mtime: float
  created_at: float

class EventKind(Enum):
  Added = 0
  Updated = 1
  Removed = 2

class Context(Protocol):
  def assert_continue(self) -> None:
    ...

class Extension(Protocol):
  @property
  def id(self) -> str:
    ...

  def file_path(self, source_id: str) -> str | None:
    ...

  def scan(self, context: Context) -> None:
    ...

  def oldest_event(self, kind: EventKind | None) -> Event | None:
    ...

  def remove_event(self, event: Event) -> None:
    ...
