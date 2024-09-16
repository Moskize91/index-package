from dataclasses import dataclass
from .fts5_db import FTS5DB
from .vector_db import VectorDB
from ..segmentation import Segment

@dataclass
class IndexNode:
  id: str
  metadata: dict
  rank: float
  segments: list[tuple[int, int]]

class IndexDB:
  def __init__(self, fts5_db: FTS5DB, vector_db: VectorDB):
    self._fts5_db: FTS5DB = fts5_db
    self._vector_db: VectorDB = vector_db

  def save(self, node_id: str, segments: list[Segment], metadata: dict):
    self._fts5_db.save(node_id, segments, metadata)
    self._vector_db.save(node_id, segments, metadata)

  def remove(self, node_id: str):
    self._fts5_db.remove(node_id)
    self._vector_db.remove(node_id)