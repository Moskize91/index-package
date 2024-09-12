import os
import argparse

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
    "-t", "--target",
    default=".",
    help="workspace directory path (default: current directory)",
    required=False,
    type=str,
  )
  args = parser.parse_args()
  _package_and_path(args.target)
  print(args.command, args.target)

def _package_and_path(target: str):
  target_path = os.path.join(os.getcwd(), target)
  target_path = os.path.abspath(target_path)

  print(target_path)

if __name__ == "__main__":
  main()