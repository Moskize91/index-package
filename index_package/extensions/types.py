from __future__ import annotations
from typing import Protocol


class KnowledgeBaseExtension(Protocol):
  @property
  def id(self) -> str:
    ...

  def scan(self, context: KnowledgeBaseScanningContext) -> None:
    ...

class KnowledgeBaseScanningContext(Protocol):
  def assert_continue(self) -> None:
    ...