import os
import sqlite3

from typing import cast, Optional, Union
from chromadb import PersistentClient
from chromadb.api import ClientAPI
from chromadb.api.types import EmbeddingFunction, IncludeEnum, Embeddable, Metadata, Document
from ..parser import PdfParser, PdfPage, PdfPageUpdatedEvent
from ..scanner import Event, EventKind, EventTarget
from ..utils import hash_sha512

class Index:
  def __init__(
    self,
    parser: PdfParser,
    db_path: str,
    scope_map: dict[str, str],
    embedding_function: Optional[EmbeddingFunction[Embeddable]],
  ):
    self._parser: PdfParser = parser
    self._scope_map: dict[str, str] = scope_map
    self._conn: sqlite3.Connection = self._connect(os.path.join(db_path, "files.db"))
    self._cursor: sqlite3.Cursor = self._conn.cursor()
    self._chromadb: ClientAPI = PersistentClient(
      path=os.path.join(db_path, "chromadb"),
    )
    self._pages_db = self._chromadb.get_or_create_collection(
      name="pages",
      embedding_function=embedding_function,
    )

  def query(self, texts: Union[str, list[str]], results_limit: int = 10) -> list[dict]:
    result = self._pages_db.query(
      query_texts=texts,
      n_results=results_limit,
    )
    results: list[dict] = []
    documents = cast(list[Metadata], result.get("documents", []))
    metadatas = cast(list[Metadata], result.get("metadatas", []))
    distances = cast(list[Metadata], result.get("distances", []))

    for i in range(len(documents)):
      results.append({
        "document": documents[i],
        "metadata": metadatas[i],
        "distance": distances[i],
      })
    return results

  def handle_event(self, scope: str, event: Event):
    if event.target == EventTarget.Directory:
      return

    logic_path = event.path
    scope_path = self._scope_map.get(scope, None)

    _, ext_name = os.path.splitext(logic_path)
    if ext_name.lower() != ".pdf":
      return

    if scope_path is None:
      return

    hash: Optional[str] = None
    origin_hash: Optional[str] = None
    path = os.path.join(scope_path, f".{logic_path}")
    file_id = self._encode_file_id(scope, logic_path)

    self._cursor.execute("SELECT hash FROM files WHERE id = ?", (file_id,))
    row = self._cursor.fetchone()

    if row is not None:
      origin_hash = row[0]

    if event.kind != EventKind.Removed:
      hash = hash_sha512(path)
      if origin_hash is None:
        self._cursor.execute("INSERT INTO files (id, type, hash) VALUES (?, ?, ?)", (file_id, "pdf", hash))
        self._conn.commit()
      elif hash != origin_hash:
        self._cursor.execute("UPDATE files SET hash = ? WHERE id = ?", (hash, file_id))
        self._conn.commit()

    elif origin_hash is not None:
      self._cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))
      self._conn.commit()

    if hash == origin_hash:
      return

    if hash is not None:
      self._cursor.execute("SELECT COUNT(*) FROM files WHERE hash = ?", (hash,))
      num_rows = self._cursor.fetchone()[0]
      if num_rows == 1:
        parser_event = self._parser.add_file(hash, path)
        self._handle_parser_event(hash, parser_event)

    if origin_hash is not None:
      self._cursor.execute("SELECT * FROM files WHERE hash = ? LIMIT 1", (origin_hash,))
      if self._cursor.fetchone() is None:
        parser_event = self._parser.remove_file(origin_hash)
        self._handle_parser_event(origin_hash, parser_event)

  def _handle_parser_event(self, pdf_hash: str, event: PdfPageUpdatedEvent):
    for hash in event.added_page_hashes:
      page = self._parser.page(hash)
      if page is not None:
        self._add_page_to_index(pdf_hash, page)

    for hash in event.removed_page_hashes:
      self._remove_page_from_index(hash)

  def _add_page_to_index(self, pdf_hash: str, page: PdfPage):
    documents: list[Document] = []
    metadatas: list[Metadata] = []

    for i, annotation in enumerate(page.annotations):
      if annotation.content is not None:
        documents.append(annotation.content)
        metadatas.append({
          "kind": "annotation.content",
          "index": i,
        })
      if annotation.extracted_text is not None:
        documents.append(annotation.extracted_text)
        metadatas.append({
          "kind": "annotation.extracted",
          "index": i,
        })

    ids: list[str] = []
    for i in range(len(documents)):
      ids.append(f"{page.hash}:{i}")

    ids.append(page.hash)
    documents.append(page.snapshot)
    metadatas.append({
      "kind": "page",
      "pdf_hash": pdf_hash,
      "index": page.index,
      "children_count": len(ids) - 1,
    })

    self._pages_db.add(
      ids=ids,
      documents=documents,
      metadatas=metadatas,
    )

  def _remove_page_from_index(self, page_hash: str):
    result = self._pages_db.get(
      ids=[page_hash],
      include=[IncludeEnum.metadatas],
    )
    child_count: int = -1
    for metadata in cast(list[Metadata], result.get("metadatas", [])):
      child_count = cast(int, metadata.get("children_count", -1))
      break

    if child_count != -1:
      to_remove_ids: list[str] = [page_hash]
      for i in range(child_count):
        to_remove_ids.append(f"{page_hash}:{i}")
      self._pages_db.delete(ids=to_remove_ids)

  def _connect(self, db_path: str) -> sqlite3.Connection:
    is_first_time = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path)
    os.path.getmtime(db_path)

    if is_first_time:
      cursor = conn.cursor()
      cursor.execute("""
        CREATE TABLE files (
          id TEXT PRIMARY KEY,
          type TEXT NOT NULL,
          hash TEXT NOT NULL
        )
      """)
      cursor.execute("""
        CREATE INDEX idx_files ON files (hash)
      """)
      conn.commit()
      cursor.close()

    return conn

  def _encode_file_id(self, scope: str, logic_path: str) -> str:
    return f"{scope}:{logic_path}"