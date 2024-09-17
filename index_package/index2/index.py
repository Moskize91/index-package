from __future__ import annotations

import os
import io
import sqlite3

from typing import Optional
from index_package.parser.pdf import PdfPage

from .fts5_db import FTS5DB
from .vector_db import VectorDB
from .index_db import IndexDB
from .types import IndexNode
from ..parser import PdfParser
from ..scanner import Event, EventKind, EventTarget
from ..segmentation import Segment, Segmentation
from ..progress import Progress
from ..utils import hash_sha512, ensure_parent_dir, is_empty_string

class Index:
  def __init__(
    self,
    index_dir_path: str,
    pdf_parser: PdfParser,
    segmentation: Segmentation,
    fts5_db: FTS5DB,
    vector_db: VectorDB,
    sources: dict[str, str],
  ):
    self._pdf_parser: PdfParser = pdf_parser
    self._segmentation: Segmentation = segmentation
    self._index_proxy: _IndexProxy = _IndexProxy(fts5_db, vector_db, segmentation)
    self._sources: dict[str, str] = sources
    self._conn: sqlite3.Connection = self._connect(
      ensure_parent_dir(os.path.join(index_dir_path, "index.sqlite3"))
    )
    self._cursor: sqlite3.Cursor = self._conn.cursor()

  def _connect(self, db_path: str) -> sqlite3.Connection:
    is_first_time = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path)
    os.path.getmtime(db_path)

    if is_first_time:
      cursor = conn.cursor()
      cursor.execute("""
        CREATE TABLE files (
          id INTEGER PRIMARY KEY,
          type TEXT NOT NULL,
          scope TEXT NOT NULL,
          path TEXT NOT NULL,
          hash TEXT NOT NULL
        )
      """)
      cursor.execute("""
        CREATE TABLE pages (
          id INTEGER PRIMARY KEY,
          pdf_hash TEXT NOT NULL,
          index INTEGER NOT NULL,
          hash TEXT NOT NULL
        )
      """)
      cursor.execute("""
        CREATE INDEX idx_files ON files (hash)
      """)
      cursor.execute("""
        CREATE INDEX idx_pages ON files (hash)
      """)
      cursor.execute("""
        CREATE INDEX idx_parent_pages ON files (pdf_hash, index)
      """)
      conn.commit()
      cursor.close()

    return conn

  def get_paths(self, file_hash: str) -> list[str]:
    self._cursor.execute("SELECT scope, path FROM files WHERE hash = ?", (file_hash,))
    paths: list[str] = []

    for row in self._cursor.fetchall():
      scope, path = row
      scope_path = self._sources.get(scope, None)
      if scope_path is not None:
        path = os.path.join(scope_path, f".{path}")
        path = os.path.abspath(path)
        paths.append(path)

    return paths

  def query(
    self,
    query_text: str,
    results_limit: Optional[int] = None,
    to_keywords: bool = True) -> list[IndexNode]:

    if results_limit is None:
      results_limit = 10

    if to_keywords:
      keywords = self._segmentation.to_keywords(query_text)
      query_text = " ".join(keywords)

    if is_empty_string(query_text):
      return []

    return self._index_proxy.query(query_text, results_limit)

  def handle_event(self, event: Event, progress: Optional[Progress] = None):
    path = self._filter_and_get_abspath(event)
    if path is None:
      return

    self._cursor.execute("SELECT id, hash FROM files WHERE scope = ? AND path = ?", (event.scope, event.path,))
    row = self._cursor.fetchone()
    new_hash: Optional[str] = None
    origin: Optional[tuple[int, str]] = None
    did_update = False

    if row is not None:
      id, hash = row
      origin = (id, hash)

    if event.kind != EventKind.Removed:
      new_hash = hash_sha512(path)
      if origin is None:
        self._cursor.execute(
          "INSERT INTO files (type, scope, path, hash) VALUES (?, ?, ?, ?)",
          ("pdf", event.scope, event.path, new_hash),
        )
        self._conn.commit()
        did_update = True
      else:
        origin_id, origin_hash = origin
        if new_hash != origin_hash:
          self._cursor.execute("UPDATE files SET hash = ? WHERE id = ?", (new_hash, origin_id,))
          self._conn.commit()
          did_update = True

    elif origin is not None:
      origin_id, _ = origin
      self._cursor.execute("DELETE FROM files WHERE id = ?", (origin_id,))
      self._conn.commit()
      did_update = True

    if not did_update:
      return

    if new_hash is not None:
      self._cursor.execute("SELECT COUNT(*) FROM files WHERE hash = ?", (new_hash,))
      num_rows = self._cursor.fetchone()[0]
      if num_rows == 1:
        self._handle_found_pdf_hash(new_hash, path, progress)

    if origin is not None:
      _, origin_hash = origin
      self._cursor.execute("SELECT * FROM files WHERE hash = ? LIMIT 1", (origin_hash,))
      if self._cursor.fetchone() is None:
        self._handle_lost_pdf_hash(origin_hash)

  def _filter_and_get_abspath(self, event: Event) -> Optional[str]:
    if event.target == EventTarget.Directory:
      return

    scope_path = self._sources.get(event.scope, None)
    if scope_path is None:
      return

    _, ext_name = os.path.splitext(event.path)
    if ext_name.lower() != ".pdf":
      return

    path = os.path.join(scope_path, f".{event.path}")
    path = os.path.abspath(path)

    return path

  def _handle_found_pdf_hash(self, hash: str, path: str, progress: Optional[Progress]):
    pdf = self._pdf_parser.pdf(hash, path, progress)
    for page in pdf.pages:
      self._cursor.execute(
        "INSERT INTO pages (pdf_hash, index, hash) VALUES (?, ?, ?)",
        (hash, page.index, page.hash),
      )
    self._conn.commit()
    self._index_proxy.save(hash, "pdf", self._pdf_meta_to_document(pdf.meta))

    for page in pdf.pages:
      self._cursor.execute("SELECT * FROM pages WHERE hash = ? LIMIT 1", (page.hash,))
      if self._cursor.fetchone() is None:
        self._handle_found_page_hash(page)
      if progress is not None:
        progress.on_complete_index_pdf_page(page.index, len(pdf.pages))

  def _handle_lost_pdf_hash(self, hash: str):
    self._cursor.execute(
      "SELECT hash FROM pages WHERE pdf_hash = ? ORDER BY index", (hash,),
    )
    page_hashes: list[str] = []
    for row in self._cursor.fetchall():
      page_hashes.append(row[0])

    self._cursor.execute("DELETE FROM pages WHERE pdf_hash = ?", (hash,))
    self._conn.commit()
    self._index_proxy.remove(hash)

    for page_hash in page_hashes:
      self._cursor.execute("SELECT COUNT(*) FROM pages WHERE hash = ?", (page_hash,))
      num_rows = self._cursor.fetchone()[0]
      if num_rows == 1:
        page = self._pdf_parser.page_with_hash(page_hash)
        if page is not None:
          self._handle_lost_page_hash(page)

    self._pdf_parser.fire_file_removed(hash)

  def _pdf_meta_to_document(self, meta: dict) -> str:
    buffer = io.StringIO()
    keys = list(meta.keys())
    keys.sort()
    for key in keys:
      buffer.write(key)
      buffer.write(": ")
      buffer.write(meta[key])
      buffer.write("\n")
    return buffer.getvalue()

  def _handle_found_page_hash(self, page: PdfPage):
    self._index_proxy.save(
      id=page.hash,
      type="pdf.page",
      text=page.snapshot,
    )
    for index, annotation in enumerate(page.annotations):
      if annotation.content is not None:
        self._index_proxy.save(
          id=f"{page.hash}/anno/{index}/content",
          type="pdf.page.anno.content",
          text=annotation.content,
        )
      if annotation.extracted_text is not None:
        self._index_proxy.save(
          id=f"{page.hash}/anno/{index}/extracted",
          type="pdf.page.anno.extracted",
          text=annotation.extracted_text,
        )

  def _handle_lost_page_hash(self, page: PdfPage):
    for index in range(len(page.annotations)):
      self._index_proxy.remove(f"{page.hash}/anno/{index}/content")
      self._index_proxy.remove(f"{page.hash}/anno/{index}/extracted")
    self._index_proxy.remove(page.hash)

class _IndexProxy:
  def __init__(self,
    fts5_db: FTS5DB,
    vector_db: VectorDB,
    segmentation: Segmentation,
  ):
    self._index_db: IndexDB = IndexDB(fts5_db, vector_db)
    self._segmentation: Segmentation = segmentation

  def query(self, query: str, results_limit: int) -> list[IndexNode]:
    return self._index_db.query(query, results_limit)

  def save(self, id: str, type: str, text: str, properties: Optional[dict] = None):
    segments: list[Segment] = []
    for segment in self._segmentation.split(text):
      if not is_empty_string(segment.text):
        continue
      segments.append(segment)
    if properties is None:
      properties = { "type": type }
    else:
      properties.copy()
      properties["type"] = type
    self._index_db.save(id, segments, properties)

  def remove(self, id: str):
    self._index_db.remove(id)