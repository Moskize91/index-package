import signal
import time
import sys
import threading

from typing import Optional
from index_package import Service, ServiceScanJob

class SignalHandler:
  def __init__(self, service: Service):
    self._service: Service = service
    self._scan_job: Optional[ServiceScanJob] = None
    self._first_interrupted_at: Optional[float] = None
    self._is_inputting: bool = False
    self._lock: threading.Lock = threading.Lock()
    signal.signal(signal.SIGINT, self._on_sigint)

  def clean_state(self):
    with self._lock:
      self._first_interrupted_at = None

  def bind_scan_job(self, scan_job: ServiceScanJob):
    with self._lock:
      if self._scan_job is not None:
        raise Exception("SignalHandler already watching a scan job")
      self._scan_job = scan_job

  def unbind_scan_job(self):
    with self._lock:
      self._scan_job = None

  def mark_start_input(self):
    with self._lock:
      self._is_inputting = True

  def mark_complete_input(self):
    with self._lock:
      self._is_inputting = False

  def _on_sigint(self, sig, frame):
    with self._lock:
      if self._is_inputting:
        if self._first_interrupted_at is None:
          print("\nPress Ctrl+C again to exit")
          self._first_interrupted_at = time.time()
        else:
          print("\nExiting...")
          sys.exit(130)
      else:
        limit_seconds = 12.0
        if self._scan_job is not None and \
          self._first_interrupted_at is None:
          print("\nInterrupting...")
          self._first_interrupted_at = time.time()
          self._scan_job.interrupt()

        elif self._first_interrupted_at is None:
          print(f"\nCannot Interrupt this command (or press again to force stop after {limit_seconds}s)")
          self._first_interrupted_at = time.time()

        else:
          duration_seconds = time.time() - self._first_interrupted_at
          if duration_seconds <= limit_seconds:
            str_seconds = "{:.2f}".format(limit_seconds - duration_seconds)
            print(f"\nForce stopping... (press again to force stop after {str_seconds}s)")
          else:
            print("\nForce stopping...")
            print("It may corrupt the data structure of the database")
            self._service.freeze_database()
            sys.exit(1)