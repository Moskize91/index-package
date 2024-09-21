import sys

from .app import App
from .args import Args

def main():
  command, package_path = Args().parse_args()
  app = App(package_path)
  is_interrupted = app.run(command)

  if is_interrupted:
    sys.exit(130)
  else:
    sys.exit(1)

if __name__ == "__main__":
  main()