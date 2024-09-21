import os
import sys
import json
import shutil
import yaml

from dataclasses import dataclass
from typing import Any, Optional
from tqdm import tqdm
from index_package import Service, ProgressListeners
from .args import Args, CommandPurge, CommandQuery, CommandScan
from .display import show_items
from .signal_handler import SignalHandler

def main():
  command, package = Args().parse_args()
  package, workspace_path = _package_and_path(package)
  workspace_path = os.path.join(workspace_path, "workspace")
  exit_code: int = 0

  if isinstance(command, CommandPurge):
    if os.path.exists(workspace_path):
      shutil.rmtree(workspace_path)
  else:
    embedding: str = package["embedding"]
    sources: Optional[dict] = package["sources"]
    if sources is None:
      sources = {}

    service = Service(
      workspace_path=workspace_path,
      embedding_model_id=embedding,
    )
    signal_handler = SignalHandler(service)

    if isinstance(command, CommandScan):
      listeners = _create_progress_listeners()
      scan_job = service.scan_job(progress_listeners=listeners)
      signal_handler.watch(scan_job)
      success = scan_job.start(sources)

      if not success:
        print("\nComplete Interrupted.")
        exit_code = 130

      signal_handler.stop_watch()

    elif isinstance(command, CommandQuery):
      text = command.text
      if len(text) == 0:
        print("Text not provided")
      else:
        if text is None:
          raise Exception("You can search by providing text")
        query_result = service.query(
          text=text,
          results_limit=command.limit,
        )
        show_items(query_result)

  sys.exit(exit_code)

def _package_and_path(package_path: str) -> tuple[dict, str]:
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

if __name__ == "__main__":
  main()