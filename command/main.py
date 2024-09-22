import sys

from .app import App
from .args import Args, CommandPurge, CommandStart

def main():
  args = Args()
  command, package_path = args.parse_args()
  app = App(package_path)

  if isinstance(command, CommandStart):
    is_interrupted = _start_service_loop(args, app)
  else:
    is_interrupted = app.run(command)

  if is_interrupted:
    sys.exit(130)
  else:
    sys.exit(0)

def _start_service_loop(args: Args, app: App) -> bool:
  print("Please press your commands.\n")
  is_interrupted: bool = False

  while True:
    app.signal_handler.mark_start_input()
    command_line: str = input("> ")
    print("")
    app.signal_handler.mark_complete_input()
    command, package_path = args.parse_args(command_line)

    if package_path != ".":
      print("warn: cannot change -p or --package while service is running")

    if isinstance(command, CommandStart) or \
        isinstance(command, CommandPurge):

      print(f"warn: cannot run \"{command_line}\" while service is running")
      continue

    is_interrupted = app.run(command)
    app.signal_handler.clean_state()

    if is_interrupted:
      break

  return is_interrupted

if __name__ == "__main__":
  main()