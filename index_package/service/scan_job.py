import os

from typing import cast, Optional, Callable
from .service_in_thread import ServiceInThread
from ..scanner import Event, Scanner
from ..progress import Progress
from ..utils import TasksPool, TasksPoolResultState

class ServiceScanJob:
  def __init__(
    self,
    scan_db_path: str,
    max_workers: int,
    sources: dict[str, str],
    create_service: Callable[[], ServiceInThread],
    progress: Optional[Progress],
  ):
    self._services: list[Optional[ServiceInThread]] = [None for _ in range(max_workers)]
    self._sources: dict[str, str] = sources
    self._scan_db_path: str = scan_db_path
    self._progress: Optional[Progress] = progress
    self._scanner: Scanner = Scanner(
      db_path=scan_db_path,
      sources=self._sources,
    )
    self._pool: TasksPool[Event] = TasksPool[Event](
      max_workers=max_workers,
      print_error=True,
      on_init=lambda i: self._init_service(i, create_service),
      on_dispose=lambda i, _: self._close_service(i),
      on_handle=lambda e, i: self._handle_event(e, i),
    )

  def start(self):
    with self._scanner.scan() as events:
      self._pool.start()
      if self._progress is not None:
        self._progress.start_scan(events.count)
      for event in events:
        success = self._pool.push(event)
        if not success:
          break
      state = self._pool.complete()
      if state == TasksPoolResultState.RaisedException:
        raise RuntimeError("scan failed with Exception")

  def interrupt(self):
    self._pool.interrupt()

  def _init_service(self, index: int, create_service: Callable[[], ServiceInThread]):
    self._services[index] = create_service()

  def _close_service(self, index: int):
    cast(ServiceInThread, self._services[index]).close()

  def _handle_event(self, event: Event, index: int):
    service = cast(ServiceInThread, self._services[index])
    path = os.path.join(self._sources[event.scope], f".{event.path}")
    path = os.path.abspath(path)
    if self._progress is not None:
      self._progress.start_handle_file(path)

    service.handle_event(event, self._progress)

    if self._progress is not None:
      self._progress.complete_handle_file(path)

