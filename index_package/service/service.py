import os

from typing import Optional, Union
from ..scanner import Scanner
from ..parser import PdfParser
from ..segmentation import Segmentation
from ..index import Index, VectorDB, PdfQueryItem
from ..progress import Progress, ProgressListeners
from ..utils import ensure_dir, ensure_parent_dir

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
    self._index = Index(
      pdf_parser=PdfParser(
        cache_dir_path=ensure_dir(
          os.path.abspath(os.path.join(workspace_path, "parser", "pdf_cache")),
        ),
        temp_dir_path=ensure_dir(
          os.path.abspath(os.path.join(workspace_path, "temp")),
        ),
      ),
      index_dir_path=ensure_dir(
        os.path.abspath(os.path.join(workspace_path, "indexes")),
      ),
      segmentation=Segmentation(),
      sources=self._sources,
      databases=[VectorDB(
        embedding_model_id=embedding_model_id,
        index_dir_path=ensure_dir(
          os.path.abspath(os.path.join(workspace_path, "vector_db")),
        ),
      )],
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

  def query(self, texts: Union[str, list[str]], results_limit: Optional[int]) -> list[PdfQueryItem]:
    if isinstance(texts, list):
      text: str = " ".join(texts)
    else:
      text: str = texts

    return self._index.query(text, results_limit)["vector"]
