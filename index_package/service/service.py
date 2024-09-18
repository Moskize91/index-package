import os

from typing import Optional
from dataclasses import dataclass
from .trimmer import trim_nodes, QueryItem
from ..scanner import Scanner
from ..parser import PdfParser
from ..segmentation import Segmentation
from ..index import Index, VectorDB, FTS5DB
from ..progress import Progress, ProgressListeners
from ..utils import ensure_dir, ensure_parent_dir

@dataclass
class QueryResult:
  items: list[QueryItem]
  keywords: list[str]

class Service:
  def __init__(
    self,
    workspace_path: str,
    embedding_model_id: str,
    sources: dict[str, str],
  ):
    self._sources: dict[str, str] = sources.copy()
    self._scanner: Scanner = Scanner(
      db_path=ensure_parent_dir(
        os.path.abspath(os.path.join(workspace_path, "scanner.sqlite3"))
      ),
      sources=self._sources,
    )
    self._pdf_parser: PdfParser = PdfParser(
      cache_dir_path=ensure_dir(
        os.path.abspath(os.path.join(workspace_path, "parser", "pdf_cache")),
      ),
      temp_dir_path=ensure_dir(
        os.path.abspath(os.path.join(workspace_path, "temp")),
      ),
    )
    self._index: Index = Index(
      pdf_parser=self._pdf_parser,
      fts5_db=FTS5DB(
        db_path=ensure_parent_dir(
          os.path.abspath(os.path.join(workspace_path, "index_fts5.sqlite3"))
        )
      ),
      vector_db=VectorDB(
        embedding_model_id=embedding_model_id,
        distance_space="l2",
        index_dir_path=ensure_dir(
          os.path.abspath(os.path.join(workspace_path, "vector_db")),
        ),
      ),
      index_dir_path=ensure_dir(
        os.path.abspath(os.path.join(workspace_path, "indexes")),
      ),
      segmentation=Segmentation(),
      sources=self._sources,
    )

  def scan(self, progress_listeners: ProgressListeners = ProgressListeners()):
    progress = Progress(progress_listeners)
    with self._scanner.scan() as events:
      progress.start_scan(events.count)
      for event in events:
        path = os.path.join(self._sources[event.scope], f".{event.path}")
        path = os.path.abspath(path)
        progress.start_handle_file(path)
        self._index.handle_event(event, progress)
        progress.complete_handle_file(path)

  def query(self, text: str, results_limit: Optional[int]) -> QueryResult:
    nodes, keywords = self._index.query(text, results_limit)
    trimmed_nodes = trim_nodes(self._index, self._pdf_parser, nodes)
    return QueryResult(trimmed_nodes, keywords)

  def page_content(self, pdf_hash: str, page_index: int) -> str:
    pdf = self._pdf_parser.pdf_or_none(pdf_hash)
    if pdf is None:
      return ""
    return pdf.pages[page_index].snapshot