import signal
import time
import sys

from typing import Optional
from index_package import Service, ServiceScanJob

class SignalHandler:
  def __init__(self, service: Service):
    self._service: Service = service
    self._scan_job: Optional[ServiceScanJob] = None
    self._first_interrupted_at: Optional[float] = None

  def watch(self, scan_job: ServiceScanJob):
    if self._scan_job is not None:
      raise Exception("SignalHandler already watching a scan job")
    self._scan_job = scan_job
    signal.signal(signal.SIGINT, self._on_sigint)

  def _on_sigint(self, sig, frame):
    if self._scan_job is None:
      return

    if self._first_interrupted_at is None:
      print("\nInterrupting...")
      self._first_interrupted_at = time.time()
      self._scan_job.interrupt()
    else:
      duration_seconds = time.time() - self._first_interrupted_at
      limit_seconds = 5.0

      if duration_seconds > limit_seconds:
        print("\nForce stopping...")
        print("It may corrupt the data structure of the database")
        self._service.freeze_database()
        sys.exit(1)
      else:
        str_seconds = "{:.2f}".format(limit_seconds - duration_seconds)
        print(f"\nForce stopping... (press again to force stop after {str_seconds}s)")

  def stop_watch(self):
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    self._scan_job = None