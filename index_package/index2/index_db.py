from dataclasses import dataclass
from enum import Enum

from .types import IndexNode, IndexNodeMatching
from .fts5_db import FTS5DB
from .vector_db import VectorDB
from ..segmentation import Segment

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

  def query(self, query: str, results_limit: int) -> list[IndexNode]:
    matched_node_ids: set[str] = set()
    matched_nodes: list[IndexNode] = []
    generator = self._fts5_db.query(
      query,
      matching=IndexNodeMatching.Matched,
      is_or_condition=False,
    )
    for node in generator:
      matched_node_ids.add(node.id)
      matched_nodes.append(node)
      if len(matched_nodes) >= results_limit:
        matched_nodes.sort(key=lambda node: -node.rank)
        return matched_nodes

    matched_nodes.sort(key=lambda node: -node.rank)
    part_matched_nodes: list[IndexNode] = []
    generator = self._fts5_db.query(
      query,
      matching=IndexNodeMatching.MatchedPartial,
      is_or_condition=True,
    )
    for node in generator:
      matched_node_ids.add(node.id)
      part_matched_nodes.append(node)
      if len(matched_nodes) >= results_limit:
        part_matched_nodes.sort(key=lambda node: -node.rank)
        return matched_nodes + part_matched_nodes

    part_matched_nodes.sort(key=lambda node: -node.rank)
    similarity_nodes: list[IndexNode] = []
    nodes = self._vector_db.query(
      query,
      matching=IndexNodeMatching.Similarity,
      results_limit=results_limit,
    )
    for node in nodes:
      if not node.id in matched_node_ids:
        similarity_nodes.append(node)

    similarity_nodes.sort(key=lambda node: node.rank)
    return matched_nodes + part_matched_nodes + similarity_nodes