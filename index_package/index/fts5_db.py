import os
import re
import json
import sqlite3

from .abc_db import IndexDB, IndexItem

class FTS5DB(IndexDB):
  def __init__(self, index_dir_path: str):
    super().__init__("fts5")
    self._conn: sqlite3.Connection = self._connect(
      db_path=os.path.abspath(os.path.join(index_dir_path, "full_text.sqlite3"))
    )
    self._cursor: sqlite3.Cursor = self._conn.cursor()

  def _connect(self, db_path: str) -> sqlite3.Connection:
    is_first_time = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path)

    if is_first_time:
      cursor = conn.cursor()
      # unicode61 remove_diacritics 2 means: diacritics are correctly removed from all Latin characters.
      # to see: https://www.sqlite.org/fts5.html
      cursor.execute("""
        CREATE VIRTUAL TABLE full_text USING fts5(
          content,
          tokenize = "unicode61 remove_diacritics 2"
        );
      """)
      cursor.execute("""
        CREATE TABLE items (
          id INTEGER PRIMARY KEY,
          parent_id TEXT NOT NULL,
          child_id TEXT NOT NULL,
          metadata TEXT NOT NULL,
          type TEXT,
          idx_rowid INTEGER NOT NULL
        )
      """)
      cursor.execute("""
        CREATE INDEX idx_items ON items (parent_id, child_id)
      """)
      cursor.execute("""
        CREATE INDEX idx_inverted_items ON items (idx_rowid)
      """)
      conn.commit()
      cursor.close()

    return conn

  def save_index(self, id: str, document: str, metadata: dict):
    parent_id, child_id = id.split("/", 1)
    try:
      self._cursor.execute("BEGIN TRANSACTION")
      self._cursor.execute("INSERT INTO full_text (content) VALUES (?)", (document,))
      rowid = self._cursor.lastrowid
      type = metadata.get("type", None)
      self._cursor.execute(
        "INSERT INTO items (parent_id, child_id, metadata, type, idx_rowid) VALUES (?, ?, ?, ?, ?)",
        (parent_id, child_id, json.dumps(metadata), type, rowid),
      )
      self._conn.commit()

    except Exception as e:
      self._conn.rollback()
      raise e

  def remove_index(self, prefix_id: str) -> None:
    try:
      self._cursor.execute("BEGIN TRANSACTION")
      group_size: int = 256

      while True:
        self._cursor.execute(
          "SELECT idx_rowid, child_id FROM items WHERE parent_id = ? LIMIT = ? ORDER BY child_id DESC",
          (prefix_id, group_size),
        )
        rows = [(row[0], row[1]) for row in self._cursor.fetchall()]
        for row in rows:
          rowid, child_id = row
          self._cursor.execute("DELETE FROM full_text WHERE rowid = ?", (rowid,))
          self._cursor.execute("DELETE FROM items WHERE idx_rowid = ? AND child_id = ?", (rowid, child_id))
        if len(rows) < group_size:
          break

      self._conn.commit()

    except Exception as e:
      self._conn.rollback()
      raise e

  def query(self, keywords: list[str], results_limit: int) -> list[list[IndexItem]]:
    return [self._query_keyword(keyword, results_limit) for keyword in keywords]

  def _query_keyword(self, keyword: str, results_limit: int) -> list[IndexItem]:
    keyword = re.sub(r"[-+:!\"'\{\},\.]", " ", keyword)
    keyword = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f ]+", " ", keyword)
    keywords: list[str] = []

    for cell in keyword.split(" "):
      if cell != "":
        keywords.append(cell)

    if len(keywords) == 0:
      return []

    index_items: list[IndexItem] = []
    query = " + ".join(keywords)
    query = f"\"content\": {query}"
    fields = "I.parent_id, I.child_id, F.content, I.metadata"
    sql = f"SELECT {fields} from full_text F INNER JOIN items I ON F.rowid = I.idx_rowid WHERE F.content MATCH ? LIMIT ?"
    self._cursor.execute(sql, (query, results_limit))

    for parent_id, child_id, content, metadata_json in self._cursor.fetchall():
      id = f"{parent_id}/{child_id}"
      metadata = json.loads(metadata_json)
      index_items.append(IndexItem(
        id=id,
        document=content,
        metadata=metadata,
        rank=0.0,
      ))

    return index_items