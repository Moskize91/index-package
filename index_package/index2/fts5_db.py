import os
import re
import json
import sqlite3

from typing import Callable, Generator
from dataclasses import dataclass
from ..segmentation import Segment

_Segment = tuple[int, int, list[str]]
_INVALID_TOKENS = set(["", "NEAR", "AND", "OR", "NOT"])

@dataclass
class FTS5Node:
  id: str
  metadata: dict
  rank: float
  segments: list[tuple[int, int]]

class FTS5DB:
  def __init__(self, db_path: str):
    self._conn: sqlite3.Connection = self._connect(
      db_path=db_path
    )

  def _connect(self, db_path: str) -> sqlite3.Connection:
    is_first_time = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path)

    if is_first_time:
      cursor = conn.cursor()
      # unicode61 remove_diacritics 2 means: diacritics are correctly removed from all Latin characters.
      # to see: https://www.sqlite.org/fts5.html
      cursor.execute("""
        CREATE VIRTUAL TABLE contents USING fts5(
          content,
          tokenize = "unicode61 remove_diacritics 2"
        );
      """)
      cursor.execute("""
        CREATE TABLE nodes (
          node_id TEXT PRIMARY KEY,
          type TEXT,
          metadata TEXT NOT NULL,
          segments TEXT NOT NULL,
          content_id INTEGER NOT NULL
        )
      """)
      cursor.execute("""
        CREATE INDEX idx_nodes ON nodes (content_id)
      """)
      conn.commit()
      cursor.close()

    return conn

  def query(
    self,
    query_text: str,
    is_or_condition: bool = False,
    rank_relationship: Callable[[str, int], float] = lambda _1, _2: 1.0,
  ) -> Generator[FTS5Node, None, None]:

    query_tokens = self._split_tokens(query_text)
    if len(query_tokens) == 0:
      return

    cursor = self._conn.cursor()

    try:
      splitter = " OR " if is_or_condition else " AND "
      query = splitter.join(query_tokens)
      query = f"\"content\": {query}"
      fields = "N.node_id, C.content, N.metadata, N.segments"
      sql = f"SELECT {fields} from contents C INNER JOIN nodes N ON C.rowid = N.content_id WHERE C.content MATCH ?"
      cursor.execute(sql, (query,))

      while True:
        rows = cursor.fetchmany(size=25)
        if len(rows) == 0:
          break
        for row in rows:
          node_id, content, metadata_json, encoded_segments = row
          metadata: dict = json.loads(metadata_json)
          segments = self._decode_segment(content, encoded_segments)
          rank = self._calculate_rank(query_tokens, segments, rank_relationship)
          node = FTS5Node(
            id=node_id,
            metadata=metadata,
            rank=rank,
            segments=[(s[0], s[1]) for s in segments],
          )
          yield node
    finally:
      cursor.close()

  def save(self, node_id: str, segments: list[Segment], metadata: dict):
    encoded_segments, tokens = self._encode_segments(segments)
    if len(encoded_segments) == 0:
      return

    try:
      document = " ".join(tokens)
      cursor = self._conn.cursor()
      cursor.execute("BEGIN TRANSACTION")
      cursor.execute("INSERT INTO contents (content) VALUES (?)", (document,))
      content_id = cursor.lastrowid
      type = metadata.get("type", None)
      metadata_json = json.dumps(metadata)
      cursor.execute(
        "INSERT INTO nodes (node_id, type, metadata, segments, content_id) VALUES (?, ?, ?, ?, ?)",
        (node_id, type, metadata_json, encoded_segments, content_id),
      )
      self._conn.commit()
      cursor.close()

    except Exception as e:
      self._conn.rollback()
      raise e

  def remove(self, node_id: str):
    try:
      cursor = self._conn.cursor()
      cursor.execute("BEGIN TRANSACTION")
      cursor.execute("SELECT content_id FROM nodes WHERE node_id = ?", (node_id,))
      row = cursor.fetchone()
      if row is not None:
        content_id = row[0]
        cursor.execute("DELETE FROM contents WHERE rowid = ?", (content_id,))
        cursor.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))
        self._conn.commit()
      cursor.close()

    except Exception as e:
      self._conn.rollback()
      raise e

  def _calculate_rank(
    self,
    query_tokens: list[str],
    segments: list[_Segment],
    rank_relationship: Callable[[str, int], float],
  ) -> float:
    return 0.0

  def _encode_segments(self, segments: list[Segment]) -> tuple[str, list[str]]:
    encoded: list[str] = []
    tokens: list[str] = []

    for s in segments:
      segment_tokens = self._split_tokens(s.text)
      if len(segment_tokens) == 0:
        continue
      encoded.append(f"{len(segment_tokens)}:{s.start}-{s.end}")
      for k in segment_tokens:
        tokens.append(k)

    return ",".join(encoded), tokens

  def _decode_segment(self, content: str, segments: str) -> list[_Segment]:
    decoded: list[tuple[int, int, list[str]]] = []
    tokens = content.split(" ")
    offset: int = 0

    for segment in segments.split(","):
      parts = segment.split(":", 1)
      token_count, position = parts
      token_count = int(token_count)
      start, end = position.split("-")
      start = int(start)
      end = int(end)
      segment_tokens = tokens[offset:offset + token_count]
      offset += token_count
      decoded.append((start, end, segment_tokens))

    return decoded

  def _split_tokens(self, text: str) -> list[str]:
    text = re.sub(r"[-+:!\"'\{\},\.]", " ", text)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f ]+", " ", text)
    tokens: list[str] = []

    for token in text.split(" "):
      token = token.lower()
      if not token in _INVALID_TOKENS:
        tokens.append(token)

    return tokens