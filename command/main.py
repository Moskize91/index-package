import os
import json
import argparse
import shutil

from dataclasses import dataclass
from typing import Any
from tqdm import tqdm
from index_package import Service, ProgressListeners
from command.display import show_results

def main():
  parser = argparse.ArgumentParser(
    prog="Index Package",
    description="scan your files & save into index database",
  )
  parser.add_argument(
    "command",
    type=str,
    choices=["scan", "query", "purge"],
  )
  parser.add_argument(
    "-p", "--package",
    default=".",
    help="workspace directory path (default: current directory)",
    required=False,
    type=str,
  )
  parser.add_argument(
    "-t", "--text",
    help="workspace directory path (default: current directory)",
    required=False,
    type=str,
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

  if args.command == "purge":
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
    if args.command == "scan":
      listeners = _create_progress_listeners()
      service.scan(listeners)

    elif args.command == "query":
      text = args.text
      results_limit = args.limit
      if text is None:
        raise Exception("Text not provided")

      results = service.query(
        texts=[text],
        results_limit=results_limit,
      )
      show_results(service, results)

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
    if context.progress_bar is not None:
      context.progress_bar.close()
      context.progress_bar = None
      print("\n")

  def on_complete_handle_pdf_page(_: int, total_pages: int):
    if context.progress_bar is None:
      context.progress_bar = tqdm(total=total_pages, desc=f"PDF {total_pages} pages", position=1)
    context.progress_bar.update(1)

  return ProgressListeners(
    on_start_scan=on_start_scan,
    on_start_handle_file=on_start_handle_file,
    on_complete_handle_pdf_page=on_complete_handle_pdf_page,
    on_complete_handle_file=on_complete_handle_file,
  )

if __name__ == "__main__":
  main()