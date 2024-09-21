import os
import json
import shutil
import yaml

from dataclasses import dataclass
from typing import Any
from tqdm import tqdm
from index_package import Service, ProgressListeners
from .args import CommandArgs, CommandPurge, CommandClear, CommandQuery, CommandScan
from .display import show_items
from .signal_handler import SignalHandler

class App:
  def __init__(self, package_path: str):
    package, workspace_path = self._package_and_path(package_path)
    self.package_path: str = package_path
    self._workspace_path: str = os.path.abspath(os.path.join(workspace_path, "workspace"))
    self._sources: dict[str, str] = package.get("sources", {})
    self._service: Service = Service(
      workspace_path=self._workspace_path,
      embedding_model_id=package["embedding"],
    )
    self.signal_handler: SignalHandler = SignalHandler(self._service)

  # @return is_interrupted
  def run(self, command: CommandArgs) -> bool:
    is_interrupted = False

    if isinstance(command, CommandClear):
      os.system("cls" if os.name == "nt" else "clear")
      print("\033[0;0H")

    elif isinstance(command, CommandPurge):
      if os.path.exists(self._workspace_path):
        shutil.rmtree(self._workspace_path)

    elif isinstance(command, CommandScan):
      listeners = _create_progress_listeners()
      scan_job = self._service.scan_job(progress_listeners=listeners)
      self.signal_handler.bind_scan_job(scan_job)
      try:
        success = scan_job.start(self._sources)
        if not success:
          print("\nComplete Interrupted.")
          is_interrupted = True
      finally:
        self.signal_handler.unbind_scan_job()

    elif isinstance(command, CommandQuery):
      text = command.text
      if text.strip() == "":
        print("Text not provided")
      else:
        query_result = self._service.query(
          text=text,
          results_limit=command.limit,
        )
        show_items(query_result)

    else:
      raise Exception(f"Invalid command {command}")

    return is_interrupted

  def _package_and_path(self, package_path: str) -> tuple[dict, str]:
    package_path = os.path.join(os.getcwd(), package_path)
    package_path = os.path.abspath(package_path)

    if not os.path.exists(package_path):
      raise Exception(f"Path {package_path} not found")

    if os.path.isdir(package_path):
      did_found = False
      for ext_name in ("json", "yaml", "yml"):
        file_path = os.path.join(package_path, f"package.{ext_name}")
        if os.path.exists(file_path):
          package_path = file_path
          did_found = True
          break

      if not did_found:
        raise Exception(f"package.json not found in {package_path}")

    _, ext_name = os.path.splitext(package_path)

    if ext_name == ".json":
      with open(package_path, "r") as file:
        package: dict = json.load(file)
    elif ext_name == ".yaml" or ext_name == ".yml":
      with open(package_path, "r") as file:
        package: dict = yaml.safe_load(file)
    else:
      raise Exception(f"Invalid file type {ext_name}")

    return package, os.path.dirname(package_path)

@dataclass
class _ProgressContext:
  count: int = 0
  files_count: int = 0
  progress_bar: Any = None

def _create_progress_listeners() -> ProgressListeners:
  context = _ProgressContext()

  def on_start_scan(count: int):
    print(f"Scanning {count} Files...")
    context.count = count

  def on_start_handle_file(path: str):
    print(f"[{context.files_count + 1}/{context.count}] Handling File {path}")

  def on_complete_handle_file(_: str):
    context.files_count += 1
    close_progress_if_exists()

  def on_complete_handle_pdf_page(page_index: int, total_pages: int):
    if context.progress_bar is None:
      context.progress_bar = tqdm(total=total_pages, desc=f"Parse PDF: {total_pages} pages", position=1)
    context.progress_bar.update(1)
    if page_index == total_pages - 1:
      close_progress_if_exists()

  def on_complete_index_pdf_page(page_index: int, total_pages: int):
    if context.progress_bar is None:
      context.progress_bar = tqdm(total=total_pages, desc=f"Index PDF {total_pages}: pages", position=1)
    context.progress_bar.update(1)
    if page_index == total_pages - 1:
      close_progress_if_exists()

  def close_progress_if_exists():
    if context.progress_bar is not None:
      context.progress_bar.close()
      context.progress_bar = None

  return ProgressListeners(
    on_start_scan=on_start_scan,
    on_start_handle_file=on_start_handle_file,
    on_complete_handle_pdf_page=on_complete_handle_pdf_page,
    on_complete_index_pdf_page=on_complete_index_pdf_page,
    on_complete_handle_file=on_complete_handle_file,
  )