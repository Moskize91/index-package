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
    self._parser.add_argument("text", nargs='*', type=str, default="")
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

    if args.purge and args.scan:
      raise Exception("Cannot scan and purge at the same time")

    if args.purge:
      return CommandPurge(), args.package
    elif args.scan:
      return CommandScan(), args.package
    else:
      command = CommandQuery(
        text=" ".join(args.text),
        limit=args.limit,
      )
      return command, args.package