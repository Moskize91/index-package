from __future__ import annotations

import os

from typing import Optional
from .service_in_thread import ServiceInThread, QueryResult
from .scan_job import ServiceScanJob
from ..progress import Progress, ProgressListeners
from ..utils import ensure_dir, ensure_parent_dir

class Service:
  def __init__(
    self,
    workspace_path: str,
    embedding_model_id: str,
    sources: dict[str, str],
  ):
    self._embedding_model_id: str = embedding_model_id
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
    self._vector_dir_path = ensure_dir(
      os.path.abspath(os.path.join(workspace_path, "vector_db")),
    )
    self._index_dir_path = ensure_dir(
      os.path.abspath(os.path.join(workspace_path, "indexes")),
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
      embedding_model_id=self._embedding_model_id,
      pdf_parser_cache_path=self._pdf_parser_cache_path,
      pdf_parser_temp_path=self._pdf_parser_temp_path,
      fts5_db_path=self._fts5_db_path,
      vector_dir_path=self._vector_dir_path,
      index_dir_path=self._index_dir_path,
    )