from __future__ import annotations

from typing import Optional
from dataclasses import dataclass

from ..progress import Progress
from .trimmer import trim_nodes, QueryItem
from ..scanner import Event
from ..parser import PdfParser
from ..segmentation import Segmentation
from ..index import Index, VectorDB, FTS5DB

@dataclass
class QueryResult:
  items: list[QueryItem]
  keywords: list[str]

# sqlite3 can only be used in the same thread it was created
class ServiceInThread:
  def __init__(
    self,
    sources: dict[str, str],
    embedding_model_id: str,
    pdf_parser_cache_path: str,
    pdf_parser_temp_path: str,
    fts5_db_path: str,
    vector_dir_path: str,
    index_dir_path: str,
  ):
    self._sources: dict[str, str] = sources
    self._pdf_parser: PdfParser = PdfParser(
      cache_dir_path=pdf_parser_cache_path,
      temp_dir_path=pdf_parser_temp_path,
    )
    self._index: Index = Index(
      pdf_parser=self._pdf_parser,
      fts5_db=FTS5DB(db_path=fts5_db_path),
      vector_db=VectorDB(
        embedding_model_id=embedding_model_id,
        distance_space="l2",
        index_dir_path=vector_dir_path,
      ),
      index_dir_path=index_dir_path,
      segmentation=Segmentation(),
      sources=sources,
    )

  def query(self, text: str, results_limit: Optional[int]) -> QueryResult:
    nodes, keywords = self._index.query(text, results_limit)
    trimmed_nodes = trim_nodes(self._index, self._pdf_parser, nodes)
    return QueryResult(trimmed_nodes, keywords)

  def page_content(self, pdf_hash: str, page_index: int) -> str:
    pdf = self._pdf_parser.pdf_or_none(pdf_hash)
    if pdf is None:
      return ""
    return pdf.pages[page_index].snapshot

  def handle_event(self, event: Event, progress: Optional[Progress] = None):
    self._index.handle_event(event, progress)

  def close(self):
    # TODO:
    pass