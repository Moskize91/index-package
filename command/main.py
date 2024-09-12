import os
import json
import argparse

from index_package import Service

def main():
  parser = argparse.ArgumentParser(
    prog="Index Package",
    description="scan your files & save into index database",
  )
  parser.add_argument(
    "command",
    type=str,
    choices=["scan", "query"],
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
  embedding: str = package["embedding"]
  sources: dict = package["sources"]
  service = Service(
    workspace_path=workspace_path,
    embedding_model_id=embedding,
    sources=sources,
  )
  if args.command == "scan":
    service.scan()

  elif args.command == "query":
    text = args.text
    results_limit = args.limit
    if text is None:
      raise Exception("Text not provided")

    results = service.query(
      texts=[text],
      results_limit=results_limit,
    )
    print(results[0])

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

if __name__ == "__main__":
  main()