import os

from typing import cast, Optional, Callable
from .service_in_thread import ServiceInThread
from ..scanner import EventParser, Scanner
from ..progress import Progress
from ..utils import TasksPool, TasksPoolResultState

_JobContext = tuple[ServiceInThread, EventParser]

class ServiceScanJob:
  def __init__(
    self,
    scan_db_path: str,
    max_workers: int,
    sources: dict[str, str],
    create_service: Callable[[], ServiceInThread],
    progress: Optional[Progress],
  ):
    self._job_contexts: list[Optional[_JobContext]] = [None for _ in range(max_workers)]
    self._sources: dict[str, str] = sources
    self._create_service: Callable[[], ServiceInThread] = create_service
    self._scan_db_path: str = scan_db_path
    self._progress: Optional[Progress] = progress
    self._scanner: Scanner = Scanner(
      db_path=scan_db_path,
      sources=self._sources,
    )
    self._pool: TasksPool[int] = TasksPool[int](
      max_workers=max_workers,
      print_error=True,
      on_init=self._init_context,
      on_dispose=lambda i, _: self._dispose_context(i),
      on_handle=lambda id, i: self._handle_event(id, i),
    )

  def start(self):
    self._pool.start()

    if self._progress is not None:
      count = self._scanner.events_count
      self._progress.start_scan(count)

    for event_id in self._scanner.scan():
      success = self._pool.push(event_id)
      if not success:
        break

    state = self._pool.complete()
    if state == TasksPoolResultState.RaisedException:
      raise RuntimeError("scan failed with Exception")

  def interrupt(self):
    self._pool.interrupt()

  def _init_context(self, index: int):
    self._job_contexts[index] = (
      self._create_service(),
      self._scanner.event_parser(),
    )

  def _dispose_context(self, index: int):
    service, parser = cast(_JobContext, self._job_contexts[index])
    service.close()
    parser.close()

  def _handle_event(self, event_id: int, index: int):
    service, parser = cast(_JobContext, self._job_contexts[index])
    event = parser.parse(event_id)

    path = os.path.join(self._sources[event.scope], f".{event.path}")
    path = os.path.abspath(path)

    if self._progress is not None:
      self._progress.start_handle_file(path)

    service.handle_event(event, self._progress)

    if self._progress is not None:
      self._progress.complete_handle_file(path)

