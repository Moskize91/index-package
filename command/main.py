import sys

from .app import App
from .args import Args, CommandPurge, CommandStart

def main():
  args = Args()
  command, package_path = args.parse_args()
  app = App(package_path)
  app.signal_handler.start_watch()

  if isinstance(command, CommandStart):
    is_interrupted = _start_service_loop(args, app)
  else:
    is_interrupted = app.run(command)

  if is_interrupted:
    sys.exit(130)
  else:
    sys.exit(1)

def _start_service_loop(args: Args, app: App) -> bool:
  print("Please press your commands.\n")
  is_last_command_is_interrupt: bool = False
  is_interrupted: bool = False

  while True:
    try:
      app.signal_handler.stop_watch()
      command_line = input("> ")
      app.signal_handler.start_watch()

      print("")
      is_last_command_is_interrupt = False
      command, package_path = args.parse_args(command_line)

      if app.package_path != package_path:
        print("warn: cannot change -p or --package while service is running")

      if isinstance(command, CommandStart) or \
         isinstance(command, CommandPurge):

        print(f"warn: cannot run \"{command_line}\" while service is running")
        continue

      is_interrupted = app.run(command)
      if is_interrupted:
        break

    except KeyboardInterrupt:
      if is_last_command_is_interrupt:
        print("\nExiting...")
        sys.exit(130)
      else:
        is_last_command_is_interrupt = True
        app.signal_handler.start_watch()
        print("\nPress Ctrl+C again to exit")

  return is_interrupted

if __name__ == "__main__":
  main()