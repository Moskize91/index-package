import os
import json
import argparse
import shutil

from dataclasses import dataclass
from typing import Any
from tqdm import tqdm
from index_package import Service, ProgressListeners
from command.display import show_items

def main():
  parser = argparse.ArgumentParser(
    prog="Index Package",
    description="scan your files & save into index database",
  )
  parser.add_argument("text", nargs='*', type=str, default="")
  parser.add_argument(
    "-p", "--package",
    default=".",
    help="workspace directory path (default: current directory)",
    required=False,
    type=str,
  )
  parser.add_argument(
    "--scan",
    default=False,
    help="scan all directories",
    required=False,
    type=bool,
    action=argparse.BooleanOptionalAction,
  )
  parser.add_argument(
    "--purge",
    default=False,
    help="purge all index data",
    required=False,
    type=bool,
    action=argparse.BooleanOptionalAction,
  )
  parser.add_argument(
    "--limit",
    default=None,
    help="workspace directory path (default: current directory)",
    required=False,
    type=int,
  )
  args = parser.parse_args()
  package, workspace_path = _package_and_path(args.package)
  workspace_path = os.path.join(workspace_path, "workspace")

  if args.purge == True:
    if args.scan == True:
      raise Exception("Cannot scan and purge at the same time")
    if os.path.exists(workspace_path):
      shutil.rmtree(workspace_path)
  else:
    embedding: str = package["embedding"]
    sources: dict = package["sources"]
    service = Service(
      workspace_path=workspace_path,
      embedding_model_id=embedding,
      sources=sources,
    )
    if args.scan == True:
      listeners = _create_progress_listeners()
      service.scan(listeners)
    else:
      text = args.text
      if len(text) == 0:
        print("Text not provided")
      else:
        if text is None:
          raise Exception("You can search by providing text")

        items = service.query(
          texts=text,
          results_limit=args.limit,
        )
        show_items(text, service, items)

def _package_and_path(package_path: str) -> tuple[dict, str]:
  package_path = os.path.join(os.getcwd(), package_path)
  package_path = os.path.abspath(package_path)

  if not os.path.exists(package_path):
    raise Exception(f"Path {package_path} not found")

  if os.path.isdir(package_path):
    json_path = os.path.join(package_path, "package.json")
    if os.path.exists(json_path):
      package_path = json_path
    else:
      raise Exception(f"package.json not found in {package_path}")

  _, ext_name = os.path.splitext(package_path)
  if ext_name != ".json":
    raise Exception(f"Invalid file type {ext_name}")

  with open(package_path, "r") as file:
    package: dict = json.load(file)

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
    print(f"[{context.files_count}/{context.count}] Handling File {path}")

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