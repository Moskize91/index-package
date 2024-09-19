import os

from typing import Optional
from .service_in_thread import ServiceInThread, QueryResult
from .scan_job import ServiceScanJob
from ..index import VectorDB
from ..segmentation.segmentation import Segmentation
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
    self._service_in_thread: Optional[ServiceInThread] = None
    self._scan_db_path: str = ensure_parent_dir(
      os.path.abspath(os.path.join(workspace_path, "scanner.sqlite3"))
    )
    self._pdf_parser_cache_path = ensure_dir(
      os.path.abspath(os.path.join(workspace_path, "parser", "pdf_cache")),
    )
    self._pdf_parser_temp_path = ensure_dir(
      os.path.abspath(os.path.join(workspace_path, "temp")),
    )
    self._fts5_db_path = ensure_parent_dir(
      os.path.abspath(os.path.join(workspace_path, "index_fts5.sqlite3"))
    )
    self._index_dir_path = ensure_dir(
      os.path.abspath(os.path.join(workspace_path, "indexes")),
    )
    self._segmentation: Segmentation = Segmentation()
    self._vector_db=VectorDB(
      embedding_model_id=embedding_model_id,
      distance_space="l2",
      index_dir_path=ensure_dir(
        os.path.abspath(os.path.join(workspace_path, "vector_db")),
      ),
    )

  def query(self, text: str, results_limit: Optional[int]) -> QueryResult:
    return self._get_service_in_thread().query(text, results_limit)

  def page_content(self, pdf_hash: str, page_index: int) -> str:
    return self._get_service_in_thread().page_content(pdf_hash, page_index)

  def scan_job(self, max_workers: int = 1, progress_listeners: Optional[ProgressListeners] = None) -> ServiceScanJob:
    progress: Optional[Progress] = None
    if progress_listeners is not None:
      progress = Progress(progress_listeners)

    return ServiceScanJob(
      sources=self._sources,
      max_workers=max_workers,
      scan_db_path=self._scan_db_path,
      progress=progress,
      create_service=lambda: self._create_service_in_thread(),
    )

  def _get_service_in_thread(self) -> ServiceInThread:
    if self._service_in_thread is None:
      self._service_in_thread = self._create_service_in_thread()
    return self._service_in_thread

  def _create_service_in_thread(self) -> ServiceInThread:
    return ServiceInThread(
      sources=self._sources,
      pdf_parser_cache_path=self._pdf_parser_cache_path,
      pdf_parser_temp_path=self._pdf_parser_temp_path,
      fts5_db_path=self._fts5_db_path,
      index_dir_path=self._index_dir_path,
      segmentation=self._segmentation,
      vector_db=self._vector_db,
    )