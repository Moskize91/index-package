import os

from index_package.extensions.types import KnowledgeBaseScanningContext

class FileExtension:
  def __init__(self, path: str):
    self._root_path: str = path

  @property
  def id(self) -> str:
    return "file"

  def scan(self, context: KnowledgeBaseScanningContext):
    self._scan_dir(context, ".")

  def _scan_dir(self, context: KnowledgeBaseScanningContext, relative_path: str):
    abs_path = os.path.join(self._root_path, f".{relative_path}")
    abs_path = os.path.abspath(abs_path)
    old_file = self._select_file(cursor, scope, relative_path)