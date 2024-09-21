import argparse

from typing import Union, Optional
from dataclasses import dataclass

@dataclass
class CommandPurge:
  pass

@dataclass
class CommandScan:
  pass

@dataclass
class CommandQuery:
  text: str
  limit: Optional[int]

CommandArgs = Union[CommandPurge, CommandScan, CommandQuery]

class Args:
  def __init__(self):
    self._parser = argparse.ArgumentParser(
      prog="Index Package",
      description="scan your files & save into index database",
    )
    self._parser.add_argument("arguments", nargs="+", type=str, default="")
    self._parser.add_argument(
      "-p", "--package",
      default=".",
      help="workspace directory path (default: current directory)",
      required=False,
      type=str,
    )
    self._parser.add_argument(
      "--scan",
      default=False,
      help="scan all directories",
      required=False,
      type=bool,
      action=argparse.BooleanOptionalAction,
    )
    self._parser.add_argument(
      "--purge",
      default=False,
      help="purge all index data",
      required=False,
      type=bool,
      action=argparse.BooleanOptionalAction,
    )
    self._parser.add_argument(
      "--limit",
      default=None,
      help="workspace directory path (default: current directory)",
      required=False,
      type=int,
    )

  # @return args, package_path
  def parse_args(self) -> tuple[CommandArgs, str]:
    args = self._parser.parse_args()
    arguments: list[str] = args.arguments
    command = arguments[0].lower()

    if command in ("query", "purge", "scan"):
      arguments = arguments[1:]
    else:
      command = "query"

    return self._parse_args_command(args, command, arguments), args.package

  def _parse_args_command(self, args, command: str, arguments: list[str]) -> CommandArgs:
    if command == "purge":
      return CommandPurge()

    elif command == "scan":
      return CommandScan()

    else:
      return CommandQuery(
        text=" ".join(arguments),
        limit=args.limit,
      )