import os

from typing import Optional, Callable
from dataclasses import dataclass
from .trimmer import trim_nodes, QueryItem
from ..scanner import Event, Scanner
from ..parser import PdfParser
from ..segmentation import Segmentation
from ..index import Index, VectorDB, FTS5DB
from ..progress import Progress, ProgressListeners
from ..utils import ensure_dir, ensure_parent_dir, TasksPool, TasksPoolResultState

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

  def scan(
      self,
      progress_listeners: ProgressListeners = ProgressListeners(),
      on_receive_interrupter: Callable[[Callable[[], None]], None] = lambda _: None,
  ):
    progress = Progress(progress_listeners)
    pool = TasksPool[Event](
      max_workers=4,
      print_error=True,
      on_handle=lambda e: self._handle_scan_event(e, progress),
    )
    with self._scanner.scan() as events:
      progress.start_scan(events.count)
      if on_receive_interrupter is not None:
        on_receive_interrupter(pool.interrupt)
      for event in events:
        success = pool.push(event)
        if not success:
          break
    state = pool.complete()
    if state == TasksPoolResultState.RaisedException:
      raise RuntimeError("scan failed with Exception")

  def _handle_scan_event(self, event: Event, progress: Progress):
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