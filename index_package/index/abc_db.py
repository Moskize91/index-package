from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class IndexItem:
  id: str
  document: str
  metadata: dict
  rank: float

class IndexDB(ABC):
  def __init__(self, name: str) -> None:
    super().__init__()
    self.name: str = name

  @abstractmethod
  def save_index(self, id: str, document: str, metadata: dict) -> None:
    pass

  @abstractmethod
  def remove_index(self, prefix_id: str) -> None:
    pass

  @abstractmethod
  def query(self, keywords: list[str], results_limit: int) -> list[list[IndexItem]]:
    pass