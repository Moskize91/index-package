import os
import sqlite3

from typing import cast, Optional
from numpy import ndarray
from sentence_transformers import SentenceTransformer
from chromadb import PersistentClient
from chromadb.api import ClientAPI
from chromadb.api.types import EmbeddingFunction, IncludeEnum, ID, Documents, Embeddings, Embeddable, Metadata, Document

from .types import PdfVectorResult, PdfQueryKind
from ..parser import PdfParser, PdfPage, PdfPageUpdatedEvent
from ..scanner import Event, EventKind, EventTarget
from ..progress import Progress
from ..utils import hash_sha512, ensure_parent_dir

class _EmbeddingFunction(EmbeddingFunction):
  def __init__(self, model_id: str):
    self._model_id: str = model_id
    self._model: Optional[SentenceTransformer] = None

  def __call__(self, input: Documents) -> Embeddings:
    if self._model is None:
      self._model = SentenceTransformer(self._model_id)
    result = self._model.encode(input)
    if not isinstance(result, ndarray):
      raise ValueError("Model output is not a numpy array")
    return result.tolist()

class VectorIndex:
  def __init__(
    self,
    parser: PdfParser,
    root_dir_path: str,
    scope_map: dict[str, str],
    embedding_model_id: str,
  ):
    files_db_path = ensure_parent_dir(os.path.join(root_dir_path, "files.sqlite3"))
    self._parser: PdfParser = parser
    self._scope_map: dict[str, str] = scope_map
    self._conn: sqlite3.Connection = self._connect(files_db_path)
    self._cursor: sqlite3.Cursor = self._conn.cursor()
    self._chromadb: ClientAPI = PersistentClient(
      path=os.path.join(root_dir_path, "chromadb"),
    )
    self._pages_db = self._chromadb.get_or_create_collection(
      name="pages",
      embedding_function=_EmbeddingFunction(embedding_model_id),
    )

  def query(self, texts: list[str], results_limit: Optional[int] = None) -> list[list[PdfVectorResult]]:
    if results_limit is None:
      results_limit = 10
    result = self._pages_db.query(
      query_texts=texts,
      n_results=results_limit,
    )
    ids = cast(list[list[ID]], result.get("ids", []))
    documents = cast(list[list[Document]], result.get("documents", []))
    metadatas = cast(list[list[Metadata]], result.get("metadatas", []))
    distances = cast(list[list[float]], result.get("distances", []))

    results: list[list[PdfVectorResult]] = []
    for i in range(len(documents)):
      j_results: list[PdfVectorResult] = []
      results.append(j_results)
      sub_ids = ids[i]
      sub_documents = documents[i]
      sub_metadatas = metadatas[i]
      sub_distances = distances[i]
      for j in range(len(sub_ids)):
        j_result = self._to_vector_result(
          id=sub_ids[j],
          document=sub_documents[j],
          metadata=sub_metadatas[j],
          distance=sub_distances[j],
        )
        if j_result is not None:
          j_results.append(j_result)

    return results

  def hash_to_files(self, hash: str) -> list[str]:
    self._cursor.execute("SELECT id FROM files WHERE hash = ?", (hash,))
    files: list[str] = []

    for row in self._cursor.fetchall():
      scope, path = self._decode_file_id(row[0])
      scope_path = self._scope_map.get(scope, None)
      if scope_path is not None:
        files.append(os.path.abspath(os.path.join(scope_path, f".{path}")))

    return files

  def handle_event(self, event: Event, progress: Progress = Progress()):
    if event.target == EventTarget.Directory:
      return

    logic_path = event.path
    scope_path = self._scope_map.get(event.scope, None)

    _, ext_name = os.path.splitext(logic_path)
    if ext_name.lower() != ".pdf":
      return

    if scope_path is None:
      return

    hash: Optional[str] = None
    origin_hash: Optional[str] = None
    path = os.path.join(scope_path, f".{logic_path}")
    file_id = self._encode_file_id(event.scope, logic_path)

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
        parser_event = self._parser.add_file(hash, path, progress)
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
          "pdf_hash": pdf_hash,
        })
      if annotation.extracted_text is not None:
        documents.append(annotation.extracted_text)
        metadatas.append({
          "kind": "annotation.extracted",
          "index": i,
          "pdf_hash": pdf_hash,
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

  def _to_vector_result(self, id: str, document: str, metadata: Metadata, distance: float) -> Optional[PdfVectorResult]:
    kind = metadata.get("kind", None)
    result: Optional[PdfVectorResult] = None
    if kind == "page":
      result = PdfVectorResult(
        kind=PdfQueryKind.page,
        page_hash=id,
        pdf_hash=cast(str, metadata["pdf_hash"]),
        index=int(cast(str, metadata["index"])),
        text=document,
        distance=distance,
      )
    elif kind == "annotation.content":
      result = PdfVectorResult(
        kind=PdfQueryKind.annotation_content,
        page_hash=id.split(":")[0],
        pdf_hash=cast(str, metadata["pdf_hash"]),
        index=int(cast(str, metadata["index"])),
        text=document,
        distance=distance,
      )
    elif kind == "annotation.extracted":
      result = PdfVectorResult(
        kind=PdfQueryKind.annotation_extracted,
        page_hash=id.split(":")[0],
        pdf_hash=cast(str, metadata["pdf_hash"]),
        index=int(cast(str, metadata["index"])),
        text=document,
        distance=distance,
      )
    return result

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

  def _decode_file_id(self, id: str) -> tuple[str, str]:
    parts = id.split(":", 1)
    return parts[0], parts[1]