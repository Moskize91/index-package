import os
import io
import math
import sqlite3

from typing import Optional

from .abc_db import IndexDB, IndexItem
from .types import PdfQueryKind, PdfQueryItem
from ..parser import PdfParser
from ..scanner import Event, EventKind, EventTarget
from ..segmentation import Segmentation
from ..progress import Progress
from ..utils import hash_sha512, ensure_parent_dir

class Index:
  def __init__(
    self,
    index_dir_path: str,
    pdf_parser: PdfParser,
    segmentation: Segmentation,
    databases: list[IndexDB],
    sources: dict[str, str],
  ):
    self._pdf_parser: PdfParser = pdf_parser
    self._segmentation: Segmentation = segmentation
    self._databases: list[IndexDB] = databases
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
        CREATE INDEX idx_files ON files (hash)
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
        self._handle_found_hash(new_hash, path, progress)

    if origin is not None:
      _, origin_hash = origin
      self._cursor.execute("SELECT * FROM files WHERE hash = ? LIMIT 1", (origin_hash,))
      if self._cursor.fetchone() is None:
        self._handle_lost_hash(origin_hash)

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

  def _handle_found_hash(self, hash: str, path: str, progress: Optional[Progress]):
    pdf = self._pdf_parser.pdf(hash, path, progress)
    writer = _WritePdf2Index(
      hash=hash,
      segmentation=self._segmentation,
      databases=self._databases,
    )
    writer.write(
      type="pdf",
      text=self._pdf_meta_to_document(pdf.meta),
    )
    for i, page in enumerate(pdf.pages):
      writer.write(
        type="pdf.page",
        text=page.snapshot,
        properties={
          "page_index": page.index,
        },
      )
      for index, annotation in enumerate(page.annotations):
        if annotation.content is not None:
          writer.write(
            type="pdf.page.anno.content",
            text=annotation.content,
            properties={
              "page_index": page.index,
              "anno_index": index,
            },
          )
        if annotation.extracted_text is not None:
          writer.write(
            type="pdf.page.anno.extracted",
            text=annotation.extracted_text,
            properties={
              "page_index": page.index,
              "anno_index": index,
            },
          )
      if progress is not None:
        progress.on_complete_index_pdf_page(i, len(pdf.pages))

  def _handle_lost_hash(self, hash: str):
    for database in self._databases:
      database.remove_index(hash)

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

  def query(self, query_text: str, results_limit: Optional[int] = None) -> dict[str, list[PdfQueryItem]]:
    if results_limit is None:
      results_limit = 10

    keywords = self._segmentation.to_keywords(query_text)
    target_results: dict[str, list[PdfQueryItem]] = {}

    if len(keywords) == 0:
      for database in self._databases:
        target_results[database.name] = []
      return target_results

    for database in self._databases:
      query_results = database.query(keywords, results_limit)
      query_results = self._flatten_results(query_results)

      min_rank: float = float("inf")
      max_rank: float = float("-inf")

      for result in query_results:
        # 通常来说，向量数据库表示的差异（distance）以指数形式扩散
        # 后续需要算 rank 的平均值的百分比分布，此处取对数可以平滑这个变化
        rank = abs(result.rank)
        rank = math.log(rank + 1.0)
        result.rank = rank
        if min_rank > rank:
          min_rank = rank
        if max_rank < rank:
          max_rank = rank

      rank_diff = max_rank - min_rank

      if rank_diff == 0.0:
        for result in query_results:
          result.rank = 0.0
      else:
        for result in query_results:
          result.rank = (result.rank - min_rank) / rank_diff

      sub_results: list[PdfQueryItem] = []
      target_results[database.name] = sub_results

      for item in query_results:
        target_result = self._parse_item(item)
        if target_result is not None:
          sub_results.append(target_result)

    return target_results

  def _flatten_results(self, results: list[list[IndexItem]]) -> list[IndexItem]:
    items: list[IndexItem] = []
    for result in results:
      items.extend(result)
    items.sort(key=lambda item: item.rank)
    return items

  def _parse_item(self, item: IndexItem) -> Optional[PdfQueryItem]:
    type = item.metadata["type"]
    page_index: int = 0
    anno_index: int = 0

    if type == "pdf":
      kind = PdfQueryKind.pdf
    elif type == "pdf.page":
      kind = PdfQueryKind.page
      page_index = int(item.metadata["page_index"])
    elif type == "pdf.page.anno.content":
      kind = PdfQueryKind.anno_content
      page_index = int(item.metadata["page_index"])
      anno_index = int(item.metadata["anno_index"])
    elif type == "pdf.page.anno.extracted":
      kind = PdfQueryKind.anno_extracted
      page_index = int(item.metadata["page_index"])
      anno_index = int(item.metadata["anno_index"])
    else:
      return None

    pdf_hash, _ = item.id.split("/", 1)
    seg_start = item.metadata["seg_start"]
    seg_end = item.metadata["seg_end"]

    return PdfQueryItem(
      kind=kind,
      pdf_hash=pdf_hash,
      page_index=page_index,
      anno_index=anno_index,
      segment_start=seg_start,
      segment_end=seg_end,
      rank=item.rank,
    )

class _WritePdf2Index:
  def __init__(self,
    hash: str,
    segmentation: Segmentation,
    databases: list[IndexDB],
  ):
    self._segmentation: Segmentation = segmentation
    self._databases: list[IndexDB] = databases
    self._hash: str = hash
    self._index: int = 0

  def write(self, type: str, text: str, properties: dict = {}):
    for segment in self._segmentation.split(text):
      id = self._gen_id()
      metadata = properties.copy()
      metadata["type"] = type
      metadata["seg_start"] = segment.start
      metadata["seg_end"] = segment.end
      for database in self._databases:
        database.save_index(id, segment.text, metadata)

  def _gen_id(self) -> str:
    id = f"{self._hash}/{self._index}"
    self._index += 1
    return id